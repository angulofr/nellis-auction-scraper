#!/usr/bin/env python3
"""
Nellis Auction Data Lake V2 Compactor & Precomputation Engine

Eliminates read-time global `DISTINCT ON` scans across R2 partitions by:
  1. Deduplicating `fact_auctions` and `fact_bids` once at ETL/compaction time.
  2. Splitting `fact_auctions` into strictly disjoint partitions:
       - `v2/fact_auctions/hot/active_and_recent.parquet` (open + last N days)
       - `v2/fact_auctions/cold/year=YYYY/month=MM/data.parquet` (sorted by location, condition, is_closed, closed_at DESC)
  3. Persisting enriched product fingerprints (`amazon_image_id`, `title_signature`, `search_text_norm`)
     directly on `v2/dim_products/products_catalog.parquet`.
  4. Materializing `v2/agg/agg_product_price_stats.parquet` and `v2/agg/agg_lake_stats.parquet`
     for O(1) UI deal matching, live cards, and telemetry without 500k-row Python loops.
  5. Publishing `v2/manifest.json` atomically last with per-file SHA-256 checksums for ETag syncing.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from src.r2_loader import (
    R2_BUCKET_NAME,
    V2_MANIFEST_KEY,
    build_facts_projection_sql,
    configure_duckdb_s3,
    enforce_dim_products_schema,
    enforce_fact_auctions_schema,
    enforce_fact_bids_schema,
    get_r2_s3_client,
    is_r2_configured,
)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _load_manifest_from_r2(s3_client, bucket: str) -> dict | None:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=V2_MANIFEST_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None


def _load_manifest_local(lake_dir: str) -> dict | None:
    p = Path(lake_dir) / "v2" / "manifest.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _stage_v2_outputs(con: duckdb.DuckDBPyConnection, stage_dir: Path, has_bids: bool, hot_days: int = 3) -> dict:
    """
    Materialize sorted, deduplicated v2 Parquet partitions, aggregates, and file inventory inside `stage_dir`.
    Assumes `_src_facts`, `_src_dim`, and (if `has_bids`) `_src_bids` temp tables exist in `con`.
    """
    v2_root = stage_dir / "v2"
    hot_dir = v2_root / "fact_auctions" / "hot"
    cold_dir = v2_root / "fact_auctions" / "cold"
    bids_dir = v2_root / "fact_bids"
    dim_dir = v2_root / "dim_products"
    agg_dir = v2_root / "agg"
    for d in (hot_dir, cold_dir, bids_dir, dim_dir, agg_dir):
        d.mkdir(parents=True, exist_ok=True)

    # 1. Enriched dim_products sorted for dictionary compression & prefix pruning
    dim_file = dim_dir / "products_catalog.parquet"
    dim_sql = str(dim_file).replace("\\", "/")
    con.execute(f"""
        COPY (
            SELECT * FROM _src_dim
            ORDER BY category_primary, product_id
        ) TO '{dim_sql}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """).fetchall()

    # 2. Disjoint hot (open or closed within hot_days) vs cold monthly partitions
    cutoff_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=int(hot_days))
    cutoff_iso = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    hot_file = hot_dir / "active_and_recent.parquet"
    hot_sql = str(hot_file).replace("\\", "/")
    con.execute(f"""
        COPY (
            SELECT *
            FROM _src_facts
            WHERE closed_at IS NULL
               OR CAST(closed_at AS TIMESTAMPTZ) >= TIMESTAMPTZ '{cutoff_iso}'
            ORDER BY closed_at DESC NULLS FIRST, location, condition, auction_id
        ) TO '{hot_sql}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """).fetchall()

    cold_months = con.execute(f"""
        SELECT DISTINCT
            CAST(EXTRACT(year FROM closed_at) AS INTEGER) AS y,
            CAST(EXTRACT(month FROM closed_at) AS INTEGER) AS m
        FROM _src_facts
        WHERE closed_at IS NOT NULL
          AND CAST(closed_at AS TIMESTAMPTZ) < TIMESTAMPTZ '{cutoff_iso}'
        ORDER BY y, m;
    """).fetchall()

    cold_rel_files = []
    for y_val, m_val in cold_months:
        y_str = f"{int(y_val):04d}"
        m_str = f"{int(m_val):02d}"
        part_dir = cold_dir / f"year={y_str}" / f"month={m_str}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_p = part_dir / "data.parquet"
        out_sql = str(out_p).replace("\\", "/")
        con.execute(f"""
            COPY (
                SELECT *
                FROM _src_facts
                WHERE closed_at IS NOT NULL
                  AND CAST(closed_at AS TIMESTAMPTZ) < TIMESTAMPTZ '{cutoff_iso}'
                  AND EXTRACT(year FROM closed_at) = {int(y_val)}
                  AND EXTRACT(month FROM closed_at) = {int(m_val)}
                ORDER BY location, condition, COALESCE(is_closed, FALSE) DESC, closed_at DESC, auction_id
            ) TO '{out_sql}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """).fetchall()
        cold_rel_files.append(f"v2/fact_auctions/cold/year={y_str}/month={m_str}/data.parquet")

    # 3. Partitioned & deduplicated fact_bids
    bid_rel_files = []
    if has_bids:
        bid_months = con.execute("""
            SELECT DISTINCT
                CAST(EXTRACT(year FROM scraped_date) AS INTEGER) AS y,
                CAST(EXTRACT(month FROM scraped_date) AS INTEGER) AS m
            FROM _src_bids
            WHERE scraped_date IS NOT NULL
            ORDER BY y, m;
        """).fetchall()
        for y_val, m_val in bid_months:
            y_str = f"{int(y_val):04d}"
            m_str = f"{int(m_val):02d}"
            b_part_dir = bids_dir / f"year={y_str}" / f"month={m_str}"
            b_part_dir.mkdir(parents=True, exist_ok=True)
            out_b = b_part_dir / "data.parquet"
            out_b_sql = str(out_b).replace("\\", "/")
            con.execute(f"""
                COPY (
                    SELECT *
                    FROM _src_bids
                    WHERE scraped_date IS NOT NULL
                      AND EXTRACT(year FROM scraped_date) = {int(y_val)}
                      AND EXTRACT(month FROM scraped_date) = {int(m_val)}
                    ORDER BY time_bucket, auction_id, bid_seq
                ) TO '{out_b_sql}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
            """).fetchall()
            bid_rel_files.append(f"v2/fact_bids/year={y_str}/month={m_str}/data.parquet")

    # 4. Precomputed aggregate price stats & lake telemetry
    agg_stats_file = agg_dir / "agg_product_price_stats.parquet"
    agg_stats_sql = str(agg_stats_file).replace("\\", "/")
    con.execute(f"""
        COPY (
            WITH cand AS (
                SELECT
                    f.auction_id,
                    f.product_id,
                    COALESCE(NULLIF(p.amazon_image_id, ''), NULLIF(p.title_signature, ''), f.product_id) AS product_key,
                    COALESCE(p.amazon_image_id, '') AS amazon_image_id,
                    COALESCE(p.title_signature, '') AS title_signature,
                    CASE
                        WHEN LOWER(COALESCE(f.condition, '') || ' ' || COALESCE(f.damage_notes, '')) ~ '(incomplete|damaged|missing|broken|1/5|2/5|parts)'
                            THEN 'Incomplete / Damaged / Parts'
                        WHEN LOWER(COALESCE(f.condition, '')) ~ '(used|fair|3/5)'
                            THEN 'Used'
                        WHEN LOWER(COALESCE(f.condition, '')) ~ '(like new|open box|4/5|very good)'
                            THEN 'Open Box / Like New'
                        ELSE 'New'
                    END AS cond_tier,
                    f.retail_price,
                    f.final_price,
                    f.discount_ratio,
                    f.bid_count,
                    f.closed_at,
                    (
                        COALESCE(f.is_closed, TRUE) = TRUE
                        AND COALESCE(f.market_status, 'sold') <> 'active'
                        AND f.final_price > 0
                        AND f.bid_count > 0
                    ) AS is_settled_sale
                FROM _src_facts f
                LEFT JOIN _src_dim p ON f.product_id = p.product_id
                WHERE f.closed_at IS NOT NULL
                  AND f.closed_at <= now()
                  AND COALESCE(f.market_status, 'sold') NOT IN (
                      'relisted', 'canceled', 'cancelled', 'void', 'unsold', 'archived_unverified'
                  )
            )
            SELECT
                product_key,
                MAX(amazon_image_id) AS amazon_image_id,
                MAX(title_signature) AS title_signature,
                cond_tier,
                COUNT(*) FILTER (WHERE is_settled_sale) AS settled_count,
                ROUND(MIN(final_price) FILTER (WHERE is_settled_sale), 2) AS settled_min_price,
                ROUND(QUANTILE_CONT(final_price, 0.20) FILTER (WHERE is_settled_sale), 2) AS settled_p20_price,
                ROUND(AVG(final_price) FILTER (WHERE is_settled_sale), 2) AS settled_avg_price,
                ROUND(MEDIAN(final_price) FILTER (WHERE is_settled_sale), 2) AS settled_median_price,
                ROUND(
                    AVG(CASE WHEN retail_price > 0 THEN (1.0 - discount_ratio) * 100.0 END)
                    FILTER (WHERE is_settled_sale),
                    1
                ) AS settled_avg_disc_pct,
                ROUND(MAX(retail_price) FILTER (WHERE is_settled_sale), 2) AS settled_max_retail,
                MAX(closed_at) FILTER (WHERE is_settled_sale) AS settled_last_at,
                COUNT(*) AS all_closed_count,
                ARRAY_TO_STRING(
                    LIST(CAST(auction_id AS VARCHAR) ORDER BY is_settled_sale DESC, final_price ASC)[:20],
                    ','
                ) AS sample_auction_ids
            FROM cand
            GROUP BY product_key, cond_tier
            ORDER BY settled_count DESC, all_closed_count DESC
        ) TO '{agg_stats_sql}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """).fetchall()

    counts_row = con.execute("""
        SELECT
            COUNT(*) AS total_lots,
            COUNT(*) FILTER (WHERE COALESCE(is_closed, FALSE) = TRUE) AS settled_lots,
            COUNT(*) FILTER (WHERE COALESCE(bids_captured, FALSE) = TRUE) AS lots_with_bids
        FROM _src_facts;
    """).fetchone()
    total_bids = int(con.execute("SELECT COUNT(*) FROM _src_bids;").fetchone()[0]) if has_bids else 0
    unique_prods = int(con.execute("SELECT COUNT(*) FROM _src_dim;").fetchone()[0])

    agg_stats_overview = agg_dir / "agg_lake_stats.parquet"
    agg_overview_sql = str(agg_stats_overview).replace("\\", "/")
    built_at_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    con.execute(f"""
        COPY (
            SELECT
                {int(counts_row[0])}::BIGINT AS total_lots,
                {int(counts_row[1])}::BIGINT AS settled_lots,
                {int(counts_row[2])}::BIGINT AS lots_with_bids,
                {int(total_bids)}::BIGINT AS total_bids,
                {int(unique_prods)}::BIGINT AS unique_products,
                '{built_at_utc}'::VARCHAR AS built_at_utc
        ) TO '{agg_overview_sql}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """).fetchall()

    # Inventory all generated parquet files with sha256 + byte size
    all_rel_files = (
        ["v2/fact_auctions/hot/active_and_recent.parquet"]
        + cold_rel_files
        + bid_rel_files
        + [
            "v2/dim_products/products_catalog.parquet",
            "v2/agg/agg_product_price_stats.parquet",
            "v2/agg/agg_lake_stats.parquet",
        ]
    )
    files_meta = {}
    digest = hashlib.sha256()
    for rel in sorted(all_rel_files):
        fp = stage_dir / rel
        sz = fp.stat().st_size
        sh = _sha256_file(fp)
        files_meta[rel] = {"size": sz, "sha256": sh}
        digest.update(f"{rel}:{sz}:{sh}|".encode("utf-8"))

    manifest = {
        "schema_version": 2,
        "built_at_utc": built_at_utc,
        "etag_version": digest.hexdigest()[:24],
        "counts": {
            "fact_auctions": int(counts_row[0]),
            "settled_lots": int(counts_row[1]),
            "lots_with_bids": int(counts_row[2]),
            "fact_bids": int(total_bids),
            "dim_products": int(unique_prods),
        },
        "hot_files": ["v2/fact_auctions/hot/active_and_recent.parquet"],
        "cold_files": cold_rel_files,
        "bid_files": bid_rel_files,
        "dim_file": "v2/dim_products/products_catalog.parquet",
        "agg_stats_file": "v2/agg/agg_product_price_stats.parquet",
        "agg_overview_file": "v2/agg/agg_lake_stats.parquet",
        "files": files_meta,
    }
    return manifest


def _publish_staged_v2(
    stage_dir: Path,
    manifest: dict,
    use_r2: bool,
    lake_dir: str,
    s3_client=None,
) -> dict:
    """Publish staged v2 Parquet files & atomic manifest to Cloudflare R2 or local lake_dir."""
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")

    if use_r2 and (s3_client is not None or is_r2_configured()):
        s3 = s3_client or get_r2_s3_client()
        old_manifest = _load_manifest_from_r2(s3, R2_BUCKET_NAME) or {}
        old_files = old_manifest.get("files") or {}

        uploaded = 0
        skipped = 0
        for rel, fmeta in manifest["files"].items():
            prev = old_files.get(rel)
            if prev and prev.get("sha256") == fmeta["sha256"] and prev.get("size") == fmeta["size"]:
                skipped += 1
                continue
            fp = stage_dir / rel
            s3.put_object(Bucket=R2_BUCKET_NAME, Key=rel, Body=fp.read_bytes())
            uploaded += 1

        # Remove stale cold/bid files no longer referenced in new manifest
        stale_keys = set(old_files.keys()) - set(manifest["files"].keys())
        for stale_k in sorted(stale_keys):
            try:
                s3.delete_object(Bucket=R2_BUCKET_NAME, Key=stale_k)
            except Exception:
                pass

        # Write manifest last so readers only flip once every data file is live
        s3.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=V2_MANIFEST_KEY,
            Body=manifest_bytes,
            ContentType="application/json",
        )
        print(f"[✓] Published v2 manifest to s3://{R2_BUCKET_NAME}/{V2_MANIFEST_KEY} (uploaded={uploaded}, unchanged={skipped})")
        return manifest

    dest_v2 = Path(lake_dir) / "v2"
    tmp_prev = _load_manifest_local(lake_dir) or {}
    old_files = set((tmp_prev.get("files") or {}).keys())
    new_files = set(manifest["files"].keys())

    for rel in new_files:
        src_p = stage_dir / rel
        dst_p = Path(lake_dir) / rel
        dst_p.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_p, dst_p)

    for stale_rel in old_files - new_files:
        stale_p = Path(lake_dir) / stale_rel
        if stale_p.exists():
            stale_p.unlink()

    manifest_path = dest_v2 / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_tmp = dest_v2 / "manifest.json.tmp"
    manifest_tmp.write_bytes(manifest_bytes)
    os.replace(manifest_tmp, manifest_path)
    print(f"[✓] Published v2 manifest locally -> {manifest_path}")
    return manifest


def compact_v1_to_v2(
    use_r2: Optional[bool] = None,
    lake_dir: str = "data_lake",
    hot_days: int = 3,
    s3_client=None,
) -> dict:
    """
    Full compaction migrating v1 legacy partitions (and any newer v2 hot rows) into
    cleanly deduplicated, sorted v2 cold monthly + hot partitions and precomputed aggregates.
    """
    eff_r2 = is_r2_configured() if use_r2 is None else (use_r2 and is_r2_configured())
    con = duckdb.connect()
    if eff_r2:
        configure_duckdb_s3(con)
        v1_facts_glob = f"s3://{R2_BUCKET_NAME}/fact_auctions/*/*/*.parquet"
        v1_bids_glob = f"s3://{R2_BUCKET_NAME}/fact_bids/*/*/*.parquet"
        v1_dim_path = f"s3://{R2_BUCKET_NAME}/dim_products/products_catalog.parquet"
        v2_hot_path = f"s3://{R2_BUCKET_NAME}/v2/fact_auctions/hot/active_and_recent.parquet"
    else:
        base = lake_dir.replace("\\", "/").rstrip("/")
        v1_facts_glob = f"{base}/fact_auctions/*/*/*.parquet"
        v1_bids_glob = f"{base}/fact_bids/*/*/*.parquet"
        v1_dim_path = f"{base}/dim_products/products_catalog.parquet"
        v2_hot_path = f"{base}/v2/fact_auctions/hot/active_and_recent.parquet"

    with tempfile.TemporaryDirectory(prefix="nellis_compact_") as tmp_dir_str:
        stage_dir = Path(tmp_dir_str)
        # Merge v1 + any v2 hot partition so nothing written incrementally is ever lost
        sources = [v1_facts_glob]
        try:
            con.execute(f"SELECT COUNT(*) FROM read_parquet('{v2_hot_path}')").fetchone()
            sources.append(v2_hot_path)
        except Exception:
            pass

        if len(sources) == 1:
            proj_sql = build_facts_projection_sql(con, f"'{sources[0]}'", deduplicate=True)
            con.execute(f"CREATE OR REPLACE TEMP TABLE _src_facts AS {proj_sql};")
        else:
            p_v1 = build_facts_projection_sql(con, f"'{sources[0]}'", deduplicate=False)
            p_v2 = build_facts_projection_sql(con, f"'{sources[1]}'", deduplicate=False)
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _src_facts AS
                SELECT DISTINCT ON (auction_id) *
                FROM (
                    {p_v1}
                    UNION ALL BY NAME
                    {p_v2}
                )
                ORDER BY auction_id,
                         COALESCE(bids_captured, FALSE) DESC,
                         COALESCE(is_closed, FALSE) DESC,
                         scraped_date DESC,
                         final_price DESC,
                         bid_count DESC;
            """)

        dim_df = con.execute(f"SELECT * FROM read_parquet('{v1_dim_path}');").df()
        dim_df = enforce_dim_products_schema(dim_df).drop_duplicates(subset=["product_id"], keep="first")
        con.register("_raw_dim_df", dim_df)
        con.execute("CREATE OR REPLACE TEMP TABLE _src_dim AS SELECT * FROM _raw_dim_df;")

        has_bids = False
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _src_bids AS
                SELECT DISTINCT ON (auction_id, bid_seq) *
                FROM read_parquet('{v1_bids_glob}', union_by_name=true)
                ORDER BY auction_id, bid_seq, scraped_date DESC, bid_amount DESC;
            """)
            has_bids = int(con.execute("SELECT COUNT(*) FROM _src_bids;").fetchone()[0]) > 0
        except Exception:
            has_bids = False

        manifest = _stage_v2_outputs(con, stage_dir, has_bids=has_bids, hot_days=hot_days)
        con.close()
        return _publish_staged_v2(stage_dir, manifest, use_r2=eff_r2, lake_dir=lake_dir, s3_client=s3_client)


def update_v2_incremental(
    new_fact_records: list | None = None,
    new_dim_records: list | None = None,
    new_bid_records: list | None = None,
    use_r2: Optional[bool] = None,
    lake_dir: str = "data_lake",
    hot_days: int = 3,
    s3_client=None,
) -> dict:
    """
    Fast nightly/settlement incremental v2 update.
    Only touches `hot`, `dim_products`, affected cold months, and precomputed aggregates.
    Falls back to `compact_v1_to_v2` automatically if `v2/manifest.json` does not exist yet.
    """
    eff_r2 = (
        (s3_client is not None or is_r2_configured())
        if use_r2 is None
        else (use_r2 and (s3_client is not None or is_r2_configured()))
    )
    s3 = (s3_client or get_r2_s3_client()) if eff_r2 else None
    manifest_prev = _load_manifest_from_r2(s3, R2_BUCKET_NAME) if eff_r2 else _load_manifest_local(lake_dir)
    if not manifest_prev:
        return compact_v1_to_v2(use_r2=eff_r2, lake_dir=lake_dir, hot_days=hot_days, s3_client=s3_client)

    con = duckdb.connect()
    if eff_r2:
        configure_duckdb_s3(con)
        v2_base = f"s3://{R2_BUCKET_NAME}"
    else:
        v2_base = lake_dir.replace("\\", "/").rstrip("/")

    hot_uri = f"{v2_base}/v2/fact_auctions/hot/active_and_recent.parquet"
    cold_glob = f"{v2_base}/v2/fact_auctions/cold/*/*/*.parquet"
    dim_uri = f"{v2_base}/v2/dim_products/products_catalog.parquet"
    bids_glob = f"{v2_base}/v2/fact_bids/*/*/*.parquet"

    existing_cold = manifest_prev.get("cold_files") or []
    fact_sources = [f"SELECT * FROM read_parquet('{hot_uri}', union_by_name=true)"]
    if existing_cold:
        fact_sources.append(f"SELECT * FROM read_parquet('{cold_glob}', union_by_name=true)")

    if new_fact_records:
        df_new_f = enforce_fact_auctions_schema(pd.DataFrame(new_fact_records))
        con.register("_delta_facts_df", df_new_f)
        fact_sources.insert(0, "SELECT * FROM _delta_facts_df")

    combined_facts_sql = " UNION ALL BY NAME ".join(fact_sources)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _src_facts AS
        SELECT DISTINCT ON (auction_id) *
        FROM ({combined_facts_sql})
        ORDER BY auction_id,
                 COALESCE(bids_captured, FALSE) DESC,
                 COALESCE(is_closed, FALSE) DESC,
                 scraped_date DESC,
                 final_price DESC,
                 bid_count DESC;
    """)

    if new_dim_records:
        old_dim = enforce_dim_products_schema(con.execute(f"SELECT * FROM read_parquet('{dim_uri}');").df())
        new_dim = enforce_dim_products_schema(pd.DataFrame(new_dim_records))
        # Normalize date column uniformly before groupby min across new + old slices
        merged_dim = pd.concat([new_dim, old_dim], ignore_index=True)
        merged_dim["first_seen_date"] = pd.to_datetime(merged_dim["first_seen_date"], errors="coerce").dt.date
        first_seen_min = (
            merged_dim.groupby("product_id")["first_seen_date"]
            .min()
            .to_dict()
        )
        merged_dim = merged_dim.drop_duplicates(subset=["product_id"], keep="first").copy()
        merged_dim["first_seen_date"] = merged_dim["product_id"].map(first_seen_min)
        merged_dim = enforce_dim_products_schema(merged_dim)
        con.register("_raw_dim_df", merged_dim)
        con.execute("CREATE OR REPLACE TEMP TABLE _src_dim AS SELECT * FROM _raw_dim_df;")
    else:
        con.execute(f"CREATE OR REPLACE TEMP TABLE _src_dim AS SELECT * FROM read_parquet('{dim_uri}');")

    bid_sources = []
    if manifest_prev.get("bid_files"):
        bid_sources.append(f"SELECT * FROM read_parquet('{bids_glob}', union_by_name=true)")
    if new_bid_records:
        df_new_b = enforce_fact_bids_schema(pd.DataFrame(new_bid_records))
        con.register("_delta_bids_df", df_new_b)
        bid_sources.insert(0, "SELECT * FROM _delta_bids_df")

    has_bids = False
    if bid_sources:
        combined_bids_sql = " UNION ALL BY NAME ".join(bid_sources)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _src_bids AS
            SELECT DISTINCT ON (auction_id, bid_seq) *
            FROM ({combined_bids_sql})
            ORDER BY auction_id, bid_seq, scraped_date DESC, bid_amount DESC;
        """)
        has_bids = int(con.execute("SELECT COUNT(*) FROM _src_bids;").fetchone()[0]) > 0

    with tempfile.TemporaryDirectory(prefix="nellis_v2_inc_") as tmp_dir_str:
        stage_dir = Path(tmp_dir_str)
        manifest = _stage_v2_outputs(con, stage_dir, has_bids=has_bids, hot_days=hot_days)
        con.close()
        return _publish_staged_v2(stage_dir, manifest, use_r2=eff_r2, lake_dir=lake_dir, s3_client=s3_client)


def main():
    parser = argparse.ArgumentParser(description="Compact Nellis Data Lake v1 partitions into fast sorted v2 store.")
    parser.add_argument("--local", action="store_true", help="Compact local ./data_lake instead of R2")
    parser.add_argument("--lake-dir", default="data_lake", help="Local data lake directory (default: data_lake)")
    parser.add_argument("--hot-days", type=int, default=3, help="Days of recent/active closes kept in hot partition")
    args = parser.parse_args()

    use_r2 = False if args.local else None
    manifest = compact_v1_to_v2(use_r2=use_r2, lake_dir=args.lake_dir, hot_days=args.hot_days)
    print(json.dumps(manifest["counts"], indent=2))


if __name__ == "__main__":
    main()

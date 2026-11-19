#!/usr/bin/env python3
"""
Nellis Auction ETag-Synced Lake Cache & DuckDB View Configurator

Eliminates repeated S3 range requests inside Streamlit Community Cloud / CLI by:
  1. Checking compact `v2/manifest.json` (~1 KB) against local cache directory.
  2. Incrementally downloading only changed Parquet partitions (SHA-256 diffed).
  3. Configuring DuckDB over local deduplicated, sorted Parquet + pre-materialized
     `dim_products` (with optional BM25 FTS index), `agg_product_price_stats`, and `agg_lake_stats`.
  4. Automatically falling back to legacy `v1` S3/local glob views if `v2` is not yet built.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Optional

import duckdb

from src.r2_loader import (
    R2_ACCESS_KEY_ID,
    R2_BUCKET_NAME,
    R2_ENDPOINT_URL,
    R2_SECRET_ACCESS_KEY,
    V2_MANIFEST_KEY,
    build_facts_projection_sql,
    configure_duckdb_s3,
    get_r2_s3_client,
)


def _resolve_r2_creds(r2_creds: Optional[dict] = None) -> dict:
    src = r2_creds or {}
    return {
        "access_key_id": src.get("access_key_id") or os.environ.get("R2_ACCESS_KEY_ID") or R2_ACCESS_KEY_ID,
        "secret_access_key": src.get("secret_access_key") or os.environ.get("R2_SECRET_ACCESS_KEY") or R2_SECRET_ACCESS_KEY,
        "endpoint_url": src.get("endpoint_url") or os.environ.get("R2_ENDPOINT_URL") or R2_ENDPOINT_URL,
        "bucket_name": src.get("bucket_name") or os.environ.get("R2_BUCKET_NAME") or R2_BUCKET_NAME,
    }


def _has_r2_creds(creds: dict) -> bool:
    return bool(creds.get("access_key_id") and creds.get("secret_access_key") and creds.get("endpoint_url") and creds.get("bucket_name"))


def _get_cache_dir(bucket_name: str, endpoint_url: str) -> Path:
    custom = os.environ.get("NELLIS_CACHE_DIR", "").strip()
    if custom:
        return Path(custom)
    tag = hashlib.sha256(f"{endpoint_url}|{bucket_name}".encode("utf-8")).hexdigest()[:12]
    return Path(tempfile.gettempdir()) / f"nellis_v2_cache_{tag}"


def fetch_v2_manifest_etag(
    use_local: bool = False,
    lake_dir: str = "data_lake",
    r2_creds: Optional[dict] = None,
    s3_client=None,
) -> str:
    """Fetch lightweight v2/manifest.json etag_version (~1 KB), or 'v1' if v2 is not deployed."""
    creds = _resolve_r2_creds(r2_creds)
    if (not use_local) and _has_r2_creds(creds):
        try:
            s3 = s3_client or get_r2_s3_client(
                endpoint_url=creds["endpoint_url"],
                access_key_id=creds["access_key_id"],
                secret_access_key=creds["secret_access_key"],
            )
            obj = s3.get_object(Bucket=creds["bucket_name"], Key=V2_MANIFEST_KEY)
            m = json.loads(obj["Body"].read().decode("utf-8"))
            return str(m.get("etag_version") or "v2")
        except Exception:
            return "v1"

    m_path = Path(lake_dir) / "v2" / "manifest.json"
    if not m_path.exists():
        return "v1"
    try:
        m = json.loads(m_path.read_text(encoding="utf-8"))
        return str(m.get("etag_version") or "v2")
    except Exception:
        return "v1"


def sync_v2_cache(
    use_local: bool = False,
    lake_dir: str = "data_lake",
    r2_creds: Optional[dict] = None,
    s3_client=None,
) -> dict:
    """
    Sync v2 compacted Parquet & aggregates into local disk cache if using R2, or resolve local v2 paths.
    Returns dict with keys: `mode` ('v2'|'v1'), `v2_root` (str), `manifest` (dict).
    """
    creds = _resolve_r2_creds(r2_creds)
    if (not use_local) and _has_r2_creds(creds):
        try:
            s3 = s3_client or get_r2_s3_client(
                endpoint_url=creds["endpoint_url"],
                access_key_id=creds["access_key_id"],
                secret_access_key=creds["secret_access_key"],
            )
            obj = s3.get_object(Bucket=creds["bucket_name"], Key=V2_MANIFEST_KEY)
            remote_manifest = json.loads(obj["Body"].read().decode("utf-8"))
        except Exception:
            return {"mode": "v1", "v2_root": "", "manifest": {}}

        cache_dir = _get_cache_dir(creds["bucket_name"], creds["endpoint_url"])
        cache_dir.mkdir(parents=True, exist_ok=True)
        local_manifest_path = cache_dir / "v2" / "manifest.json"
        local_manifest = {}
        if local_manifest_path.exists():
            try:
                local_manifest = json.loads(local_manifest_path.read_text(encoding="utf-8"))
            except Exception:
                local_manifest = {}

        old_files = local_manifest.get("files") or {}
        new_files = remote_manifest.get("files") or {}

        for rel, fmeta in new_files.items():
            dst = cache_dir / rel
            prev = old_files.get(rel)
            if (
                dst.exists()
                and prev
                and prev.get("sha256") == fmeta.get("sha256")
                and prev.get("size") == fmeta.get("size")
            ):
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            tmp_dst = dst.with_suffix(".parquet.tmp")
            obj_f = s3.get_object(Bucket=creds["bucket_name"], Key=rel)
            tmp_dst.write_bytes(obj_f["Body"].read())
            os.replace(tmp_dst, dst)

        for stale_rel in set(old_files.keys()) - set(new_files.keys()):
            stale_p = cache_dir / stale_rel
            if stale_p.exists():
                try:
                    stale_p.unlink()
                except OSError:
                    pass

        local_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_m = local_manifest_path.with_suffix(".json.tmp")
        tmp_m.write_text(json.dumps(remote_manifest), encoding="utf-8")
        os.replace(tmp_m, local_manifest_path)
        return {"mode": "v2", "v2_root": str(cache_dir), "manifest": remote_manifest}

    local_m_path = Path(lake_dir) / "v2" / "manifest.json"
    if not local_m_path.exists():
        return {"mode": "v1", "v2_root": "", "manifest": {}}
    try:
        m = json.loads(local_m_path.read_text(encoding="utf-8"))
        # Verify referenced files actually exist locally
        for rel in (m.get("hot_files") or []):
            if not (Path(lake_dir) / rel).exists():
                return {"mode": "v1", "v2_root": "", "manifest": {}}
        return {"mode": "v2", "v2_root": str(Path(lake_dir)), "manifest": m}
    except Exception:
        return {"mode": "v1", "v2_root": "", "manifest": {}}


def create_empty_fallback_views(con: duckdb.DuckDBPyConnection, include_facts: bool = True):
    """Create typed zero-row views when lake tables do not exist yet."""
    if include_facts:
        con.execute("""
            CREATE OR REPLACE VIEW fact_auctions AS
            SELECT
                CAST(NULL AS BIGINT) AS auction_id,
                CAST(NULL AS VARCHAR) AS product_id,
                CAST(NULL AS VARCHAR) AS location,
                CAST(NULL AS VARCHAR) AS condition,
                CAST(NULL AS VARCHAR) AS completeness,
                CAST(NULL AS VARCHAR) AS functional_status,
                CAST(NULL AS VARCHAR) AS damage_notes,
                CAST(NULL AS DOUBLE) AS retail_price,
                CAST(NULL AS DOUBLE) AS final_price,
                CAST(NULL AS DOUBLE) AS discount_ratio,
                CAST(NULL AS INTEGER) AS bid_count,
                CAST(NULL AS INTEGER) AS unique_bidders,
                CAST(NULL AS TIMESTAMPTZ) AS closed_at,
                CAST(NULL AS TINYINT) AS close_day_of_week,
                CAST(NULL AS TINYINT) AS close_hour,
                CAST(NULL AS DATE) AS scraped_date,
                CAST(NULL AS VARCHAR) AS photo_url,
                CAST(NULL AS BOOLEAN) AS is_closed,
                CAST('unverified' AS VARCHAR) AS market_status,
                CAST(NULL AS TIMESTAMPTZ) AS open_at,
                CAST(NULL AS TIMESTAMPTZ) AS initial_closed_at,
                CAST(NULL AS BOOLEAN) AS was_extended,
                CAST(NULL AS INTEGER) AS extension_seconds,
                CAST(NULL AS DOUBLE) AS first_bid_price,
                CAST(NULL AS DOUBLE) AS price_at_1h_left,
                CAST(NULL AS DOUBLE) AS price_at_5m_left,
                CAST(NULL AS DOUBLE) AS price_at_1m_left,
                CAST(NULL AS INTEGER) AS bids_last_5m,
                CAST(NULL AS INTEGER) AS bids_last_60s,
                CAST(NULL AS DOUBLE) AS winning_bid_sec_before_close,
                CAST(FALSE AS BOOLEAN) AS bids_captured
            WHERE FALSE;
        """)
    con.execute("""
        CREATE OR REPLACE VIEW fact_bids AS
        SELECT
            CAST(NULL AS BIGINT) AS auction_id,
            CAST(NULL AS VARCHAR) AS product_id,
            CAST(NULL AS SMALLINT) AS bid_seq,
            CAST(NULL AS VARCHAR) AS bidder_label,
            CAST(NULL AS SMALLINT) AS bidder_num,
            CAST(NULL AS DOUBLE) AS bid_amount,
            CAST(NULL AS DOUBLE) AS bid_increment,
            CAST(NULL AS VARCHAR) AS bid_status,
            CAST(NULL AS BOOLEAN) AS is_winning_bid,
            CAST(NULL AS TIMESTAMPTZ) AS bid_time,
            CAST(NULL AS TINYINT) AS bid_hour,
            CAST(NULL AS DOUBLE) AS seconds_before_close,
            CAST(NULL AS DOUBLE) AS seconds_before_initial_close,
            CAST(NULL AS VARCHAR) AS time_bucket,
            CAST(NULL AS DOUBLE) AS auction_final_price,
            CAST(NULL AS DOUBLE) AS pct_of_final_price,
            CAST(NULL AS DATE) AS scraped_date
        WHERE FALSE;
    """)


def _create_empty_dim_view(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE OR REPLACE VIEW dim_products AS
        SELECT
            CAST(NULL AS VARCHAR) AS product_id,
            CAST(NULL AS VARCHAR) AS title,
            CAST(NULL AS VARCHAR) AS brand,
            CAST(NULL AS VARCHAR) AS category_primary,
            CAST(NULL AS VARCHAR) AS category_secondary,
            CAST(NULL AS DOUBLE) AS base_retail_price,
            CAST(NULL AS VARCHAR) AS description,
            CAST(NULL AS DATE) AS first_seen_date,
            CAST(NULL AS VARCHAR) AS photo_url,
            CAST(NULL AS VARCHAR) AS amazon_image_id,
            CAST(NULL AS VARCHAR) AS title_signature,
            CAST(NULL AS VARCHAR) AS search_text_norm
        WHERE FALSE;
    """)


def _create_empty_agg_views(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE OR REPLACE VIEW agg_product_price_stats AS
        SELECT
            CAST(NULL AS VARCHAR) AS product_key,
            CAST(NULL AS VARCHAR) AS amazon_image_id,
            CAST(NULL AS VARCHAR) AS title_signature,
            CAST(NULL AS VARCHAR) AS cond_tier,
            CAST(0 AS BIGINT) AS settled_count,
            CAST(NULL AS DOUBLE) AS settled_min_price,
            CAST(NULL AS DOUBLE) AS settled_p20_price,
            CAST(NULL AS DOUBLE) AS settled_avg_price,
            CAST(NULL AS DOUBLE) AS settled_median_price,
            CAST(NULL AS DOUBLE) AS settled_avg_disc_pct,
            CAST(NULL AS DOUBLE) AS settled_max_retail,
            CAST(NULL AS TIMESTAMPTZ) AS settled_last_at,
            CAST(0 AS BIGINT) AS all_closed_count,
            CAST('' AS VARCHAR) AS sample_auction_ids
        WHERE FALSE;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW agg_lake_stats AS
        SELECT
            CAST(0 AS BIGINT) AS total_lots,
            CAST(0 AS BIGINT) AS settled_lots,
            CAST(0 AS BIGINT) AS lots_with_bids,
            CAST(0 AS BIGINT) AS total_bids,
            CAST(0 AS BIGINT) AS unique_products,
            CAST('' AS VARCHAR) AS built_at_utc
        WHERE FALSE;
    """)


def configure_duckdb_lake_views(
    con: duckdb.DuckDBPyConnection,
    use_local: bool = False,
    lake_dir: str = "data_lake",
    r2_creds: Optional[dict] = None,
    enable_fts: bool = True,
    s3_client=None,
) -> dict:
    """
    Configure DuckDB runtime settings and mount either:
      - Fast local-cached `v2` non-overlapping Parquet views + materialized `dim_products` (with optional FTS)
        and `agg_product_price_stats` / `agg_lake_stats`, OR
      - Legacy `v1` deduplicated Parquet glob views when `v2/manifest.json` does not exist yet.
    """
    mem_limit = os.environ.get("NELLIS_DUCKDB_MEM", "650MB")
    try:
        con.execute(f"SET memory_limit = '{mem_limit}';")
        con.execute("SET threads = 2;")
    except Exception:
        pass

    sync_info = sync_v2_cache(use_local=use_local, lake_dir=lake_dir, r2_creds=r2_creds, s3_client=s3_client)
    if sync_info["mode"] == "v2":
        root = Path(sync_info["v2_root"])
        manifest = sync_info["manifest"]

        hot_paths = [str(root / rel).replace("\\", "/") for rel in (manifest.get("hot_files") or []) if (root / rel).exists()]
        cold_paths = [str(root / rel).replace("\\", "/") for rel in (manifest.get("cold_files") or []) if (root / rel).exists()]
        all_fact_paths = hot_paths + cold_paths

        if all_fact_paths:
            paths_literal = "[" + ", ".join(f"'{p}'" for p in all_fact_paths) + "]"
            con.execute(
                f"CREATE OR REPLACE VIEW fact_auctions AS "
                f"SELECT * FROM read_parquet({paths_literal}, union_by_name=true);"
            )
        else:
            create_empty_fallback_views(con, include_facts=True)

        if hot_paths:
            hot_literal = "[" + ", ".join(f"'{p}'" for p in hot_paths) + "]"
            con.execute(
                f"CREATE OR REPLACE VIEW fact_auctions_hot AS "
                f"SELECT * FROM read_parquet({hot_literal}, union_by_name=true);"
            )
        else:
            con.execute("CREATE OR REPLACE VIEW fact_auctions_hot AS SELECT * FROM fact_auctions WHERE FALSE;")

        bid_paths = [str(root / rel).replace("\\", "/") for rel in (manifest.get("bid_files") or []) if (root / rel).exists()]
        if bid_paths:
            bids_literal = "[" + ", ".join(f"'{p}'" for p in bid_paths) + "]"
            con.execute(
                f"CREATE OR REPLACE VIEW fact_bids AS "
                f"SELECT * FROM read_parquet({bids_literal}, union_by_name=true);"
            )
        else:
            create_empty_fallback_views(con, include_facts=False)

        dim_rel = manifest.get("dim_file") or "v2/dim_products/products_catalog.parquet"
        dim_path = str(root / dim_rel).replace("\\", "/")
        has_fts = False
        if (root / dim_rel).exists():
            con.execute(f"CREATE OR REPLACE TABLE dim_products AS SELECT * FROM read_parquet('{dim_path}');")
            if enable_fts:
                try:
                    cnt = int(con.execute("SELECT COUNT(*) FROM dim_products;").fetchone()[0])
                    if cnt > 0:
                        con.execute("INSTALL fts; LOAD fts;")
                        con.execute(
                            "PRAGMA create_fts_index('dim_products', 'product_id', "
                            "'title', 'brand', 'category_primary', 'category_secondary', 'search_text_norm', "
                            "overwrite=1);"
                        )
                        has_fts = True
                except Exception:
                    has_fts = False
        else:
            _create_empty_dim_view(con)

        agg_stats_rel = manifest.get("agg_stats_file") or "v2/agg/agg_product_price_stats.parquet"
        if (root / agg_stats_rel).exists():
            agg_stats_path = str(root / agg_stats_rel).replace("\\", "/")
            con.execute(f"CREATE OR REPLACE TABLE agg_product_price_stats AS SELECT * FROM read_parquet('{agg_stats_path}');")
        else:
            _create_empty_agg_views(con)

        agg_ov_rel = manifest.get("agg_overview_file") or "v2/agg/agg_lake_stats.parquet"
        if (root / agg_ov_rel).exists():
            agg_ov_path = str(root / agg_ov_rel).replace("\\", "/")
            con.execute(f"CREATE OR REPLACE TABLE agg_lake_stats AS SELECT * FROM read_parquet('{agg_ov_path}');")
        else:
            con.execute("""
                CREATE OR REPLACE VIEW agg_lake_stats AS
                SELECT
                    CAST(0 AS BIGINT) AS total_lots,
                    CAST(0 AS BIGINT) AS settled_lots,
                    CAST(0 AS BIGINT) AS lots_with_bids,
                    CAST(0 AS BIGINT) AS total_bids,
                    CAST(0 AS BIGINT) AS unique_products,
                    CAST('' AS VARCHAR) AS built_at_utc
                WHERE FALSE;
            """)

        return {
            "mode": "v2",
            "has_v2": True,
            "has_fts": has_fts,
            "etag": str(manifest.get("etag_version") or "v2"),
            "counts": manifest.get("counts") or {},
        }

    # --- Legacy v1 Fallback Mode ---
    creds = _resolve_r2_creds(r2_creds)
    if (not use_local) and _has_r2_creds(creds):
        try:
            configure_duckdb_s3(
                con,
                endpoint_url=creds["endpoint_url"],
                access_key_id=creds["access_key_id"],
                secret_access_key=creds["secret_access_key"],
            )
            bucket = creds["bucket_name"]
            facts_glob = f"s3://{bucket}/fact_auctions/*/*/*.parquet"
            bids_glob = f"s3://{bucket}/fact_bids/*/*/*.parquet"
            dim_glob = f"s3://{bucket}/dim_products/products_catalog.parquet"
        except Exception:
            base = lake_dir.replace("\\", "/").rstrip("/")
            facts_glob = f"{base}/fact_auctions/*/*/*.parquet"
            bids_glob = f"{base}/fact_bids/*/*/*.parquet"
            dim_glob = f"{base}/dim_products/products_catalog.parquet"
    else:
        base = lake_dir.replace("\\", "/").rstrip("/")
        facts_glob = f"{base}/fact_auctions/*/*/*.parquet"
        bids_glob = f"{base}/fact_bids/*/*/*.parquet"
        dim_glob = f"{base}/dim_products/products_catalog.parquet"

    try:
        proj_sql = build_facts_projection_sql(con, f"'{facts_glob}'", deduplicate=True)
        con.execute(f"CREATE OR REPLACE VIEW fact_auctions AS {proj_sql};")
    except Exception:
        create_empty_fallback_views(con, include_facts=True)

    con.execute("CREATE OR REPLACE VIEW fact_auctions_hot AS SELECT * FROM fact_auctions;")

    try:
        con.execute(f"""
            CREATE OR REPLACE VIEW fact_bids AS
            SELECT DISTINCT ON (auction_id, bid_seq) *
            FROM read_parquet('{bids_glob}', union_by_name=true)
            ORDER BY auction_id, bid_seq, scraped_date DESC;
        """)
    except Exception:
        create_empty_fallback_views(con, include_facts=False)

    try:
        con.execute(f"CREATE OR REPLACE VIEW dim_products AS SELECT * FROM read_parquet('{dim_glob}');")
    except Exception:
        _create_empty_dim_view(con)

    _create_empty_agg_views(con)
    return {"mode": "v1", "has_v2": False, "has_fts": False, "etag": "v1", "counts": {}}

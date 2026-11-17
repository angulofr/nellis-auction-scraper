#!/usr/bin/env python3
"""
Nellis Auction Cloudflare R2 / Parquet Loader Module
Handles:
  1. Hive-partitioned writing of daily fact_auctions Parquet files
  2. Hive-partitioned writing of daily fact_bids Parquet files (bid-by-bid curves)
  3. Merging & upserting deduplicated dim_products product catalog
  4. Automatic S3/R2 authentication with local ./data_lake fallback
"""

import datetime
import os
import duckdb
import pandas as pd

# Environment configuration
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL", "https://965a5411101d1521c551d99d9421a145.r2.cloudflarestorage.com")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "nellis-auction-lake")


def get_r2_host():
    return R2_ENDPOINT_URL.replace("https://", "").replace("http://", "").strip("/")


def is_r2_configured() -> bool:
    """Check if Cloudflare R2 credentials are present and non-empty."""
    return bool(R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_ENDPOINT_URL and R2_BUCKET_NAME)


def get_configured_duckdb_conn():
    """Create a DuckDB connection pre-configured globally with Cloudflare R2 S3/httpfs settings."""
    con = duckdb.connect()
    if is_r2_configured():
        con.execute("INSTALL httpfs; LOAD httpfs;")
        host = get_r2_host()
        con.execute(f"""
            SET GLOBAL s3_endpoint = '{host}';
            SET GLOBAL s3_access_key_id = '{R2_ACCESS_KEY_ID}';
            SET GLOBAL s3_secret_access_key = '{R2_SECRET_ACCESS_KEY}';
            SET GLOBAL s3_url_style = 'path';
            SET GLOBAL s3_region = 'auto';
            SET GLOBAL s3_use_ssl = true;
        """)
    return con


def enforce_fact_auctions_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce consistent column set and Parquet types across all fact_auctions writers."""
    df = df.copy()
    df["auction_id"] = df["auction_id"].astype("int64")
    df["product_id"] = df["product_id"].fillna("").astype("string")
    df["location"] = df["location"].fillna("").astype("string")
    df["condition"] = df["condition"].fillna("").astype("string")
    df["completeness"] = df["completeness"].fillna("").astype("string")
    df["functional_status"] = df["functional_status"].fillna("").astype("string")
    df["damage_notes"] = df["damage_notes"].fillna("").astype("string")
    df["retail_price"] = pd.to_numeric(df["retail_price"], errors="coerce").astype("float64")
    df["final_price"] = pd.to_numeric(df["final_price"], errors="coerce").astype("float64")
    df["discount_ratio"] = pd.to_numeric(df["discount_ratio"], errors="coerce").astype("float64")
    df["bid_count"] = pd.to_numeric(df["bid_count"], errors="coerce").fillna(0).astype("int32")
    df["unique_bidders"] = pd.to_numeric(df["unique_bidders"], errors="coerce").fillna(0).astype("int32")
    df["closed_at"] = pd.to_datetime(df["closed_at"], format="ISO8601", utc=True, errors="coerce")
    df["close_day_of_week"] = pd.to_numeric(df["close_day_of_week"], errors="coerce").astype("Int8")
    df["close_hour"] = pd.to_numeric(df["close_hour"], errors="coerce").astype("Int8")
    df["scraped_date"] = pd.to_datetime(df["scraped_date"]).dt.date
    df["photo_url"] = (
        df["photo_url"].fillna("").astype("string")
        if "photo_url" in df.columns
        else pd.Series([""] * len(df), dtype="string")
    )
    df["is_closed"] = (
        df["is_closed"].fillna(False).astype("bool")
        if "is_closed" in df.columns
        else pd.Series([False] * len(df), dtype="bool")
    )
    df["market_status"] = (
        df["market_status"].fillna("active").astype("string")
        if "market_status" in df.columns
        else pd.Series(["active"] * len(df), dtype="string")
    )

    for dt_col in ("open_at", "initial_closed_at"):
        if dt_col in df.columns:
            df[dt_col] = pd.to_datetime(df[dt_col], format="ISO8601", utc=True, errors="coerce")
        else:
            df[dt_col] = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")

    df["was_extended"] = (
        df["was_extended"].fillna(False).astype("bool")
        if "was_extended" in df.columns
        else pd.Series([False] * len(df), dtype="bool")
    )
    for int_col in ("extension_seconds", "bids_last_5m", "bids_last_60s"):
        df[int_col] = (
            pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype("int32")
            if int_col in df.columns
            else pd.Series([0] * len(df), dtype="int32")
        )
    for flt_col in ("first_bid_price", "price_at_1h_left", "price_at_5m_left", "price_at_1m_left", "winning_bid_sec_before_close"):
        df[flt_col] = (
            pd.to_numeric(df[flt_col], errors="coerce").astype("float64")
            if flt_col in df.columns
            else pd.Series([float("nan")] * len(df), dtype="float64")
        )
    df["bids_captured"] = (
        df["bids_captured"].fillna(False).astype("bool")
        if "bids_captured" in df.columns
        else pd.Series([False] * len(df), dtype="bool")
    )
    return df


def enforce_fact_bids_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce consistent analytics-ready Parquet types for bid-level event rows."""
    df = df.copy()
    df["auction_id"] = df["auction_id"].astype("int64")
    df["product_id"] = df["product_id"].fillna("").astype("string")
    df["bid_seq"] = pd.to_numeric(df["bid_seq"], errors="coerce").fillna(0).astype("int16")
    df["bidder_label"] = df["bidder_label"].fillna("").astype("string")
    df["bidder_num"] = pd.to_numeric(df["bidder_num"], errors="coerce").fillna(0).astype("int16")
    df["bid_amount"] = pd.to_numeric(df["bid_amount"], errors="coerce").astype("float64")
    df["bid_increment"] = pd.to_numeric(df["bid_increment"], errors="coerce").fillna(0.0).astype("float64")
    df["bid_status"] = df["bid_status"].fillna("").astype("string")
    df["is_winning_bid"] = df["is_winning_bid"].fillna(False).astype("bool")
    df["bid_time"] = pd.to_datetime(df["bid_time"], format="ISO8601", utc=True, errors="coerce")
    df["bid_hour"] = pd.to_numeric(df["bid_hour"], errors="coerce").astype("Int8")
    df["seconds_before_close"] = pd.to_numeric(df["seconds_before_close"], errors="coerce").astype("float64")
    df["seconds_before_initial_close"] = pd.to_numeric(df["seconds_before_initial_close"], errors="coerce").astype("float64")
    df["time_bucket"] = df["time_bucket"].fillna("").astype("string")
    df["auction_final_price"] = pd.to_numeric(df["auction_final_price"], errors="coerce").astype("float64")
    df["pct_of_final_price"] = pd.to_numeric(df["pct_of_final_price"], errors="coerce").astype("float64")
    df["scraped_date"] = pd.to_datetime(df["scraped_date"]).dt.date
    return df


def write_fact_auctions(fact_records: list, scrape_date: str = None, use_r2: bool = True) -> str:
    """
    Write daily fact_auctions records to Hive-partitioned Parquet.
    Target path: fact_auctions/year=YYYY/month=MM/auctions_YYYY-MM-DD.parquet
    """
    if not fact_records:
        print("[!] No fact records to write.")
        return ""

    if not scrape_date:
        scrape_date = datetime.datetime.now().strftime("%Y-%m-%d")

    dt = datetime.datetime.strptime(scrape_date, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")

    df = enforce_fact_auctions_schema(pd.DataFrame(fact_records))
    con = get_configured_duckdb_conn()

    if use_r2 and is_r2_configured():
        target_path = f"s3://{R2_BUCKET_NAME}/fact_auctions/year={year}/month={month}/auctions_{scrape_date}.parquet"
    else:
        local_dir = os.path.join("data_lake", "fact_auctions", f"year={year}", f"month={month}")
        os.makedirs(local_dir, exist_ok=True)
        target_path = os.path.join(local_dir, f"auctions_{scrape_date}.parquet").replace("\\", "/")

    con.register("daily_facts_view", df)

    partition_exists = False
    try:
        con.execute(f"SELECT COUNT(*) FROM '{target_path}'").fetchone()
        partition_exists = True
    except Exception:
        partition_exists = False

    if partition_exists:
        print(f"[*] Merging {len(df)} fact records into existing partition: {target_path}...")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE merged_facts AS
            WITH combined AS (
                SELECT * FROM daily_facts_view
                UNION ALL BY NAME
                SELECT * FROM '{target_path}'
            )
            SELECT DISTINCT ON (auction_id) *
            FROM combined
            ORDER BY auction_id,
                     COALESCE(bids_captured, FALSE) DESC,
                     COALESCE(is_closed, FALSE) DESC,
                     final_price DESC,
                     bid_count DESC;
        """).fetchall()
        con.execute(f"COPY merged_facts TO '{target_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');").fetchall()
        total_rows = con.execute("SELECT COUNT(*) FROM merged_facts").fetchone()[0]
        print(f"[✓] Successfully merged fact_auctions Parquet ({total_rows:,} total rows)")
    else:
        print(f"[*] Writing {len(df)} fact records to: {target_path}...")
        con.execute(f"COPY daily_facts_view TO '{target_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');").fetchall()
        print(f"[✓] Successfully wrote fact_auctions Parquet ({len(df):,} rows)")

    con.close()
    return target_path


def write_fact_bids(bid_records: list, partition_date: str = None, use_r2: bool = True) -> str:
    """
    Write individual chronological bid-history events to Hive-partitioned Parquet.
    Target path: fact_bids/year=YYYY/month=MM/bids_YYYY-MM-DD.parquet
    """
    if not bid_records:
        return ""

    if not partition_date:
        partition_date = datetime.datetime.now().strftime("%Y-%m-%d")

    dt = datetime.datetime.strptime(partition_date, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")

    df = enforce_fact_bids_schema(pd.DataFrame(bid_records))
    con = get_configured_duckdb_conn()

    if use_r2 and is_r2_configured():
        target_path = f"s3://{R2_BUCKET_NAME}/fact_bids/year={year}/month={month}/bids_{partition_date}.parquet"
    else:
        local_dir = os.path.join("data_lake", "fact_bids", f"year={year}", f"month={month}")
        os.makedirs(local_dir, exist_ok=True)
        target_path = os.path.join(local_dir, f"bids_{partition_date}.parquet").replace("\\", "/")

    con.register("incoming_bids_view", df)
    partition_exists = False
    try:
        con.execute(f"SELECT COUNT(*) FROM '{target_path}'").fetchone()
        partition_exists = True
    except Exception:
        partition_exists = False

    if partition_exists:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE merged_bids AS
            WITH combined AS (
                SELECT * FROM incoming_bids_view
                UNION ALL BY NAME
                SELECT * FROM '{target_path}'
            )
            SELECT DISTINCT ON (auction_id, bid_seq) *
            FROM combined
            ORDER BY auction_id, bid_seq, bid_amount DESC;
        """).fetchall()
        con.execute(f"COPY merged_bids TO '{target_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');").fetchall()
        total_rows = con.execute("SELECT COUNT(*) FROM merged_bids").fetchone()[0]
        print(f"[✓] Merged fact_bids Parquet -> {target_path} ({total_rows:,} bid rows)")
    else:
        con.execute(f"COPY incoming_bids_view TO '{target_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');").fetchall()
        print(f"[✓] Wrote fact_bids Parquet -> {target_path} ({len(df):,} bid rows)")

    con.close()
    return target_path


def merge_dim_products(new_products: list, use_r2: bool = True) -> str:
    """
    Merge newly scraped products into the deduplicated dim_products catalog.
    Preserves original first_seen_date for existing products.
    """
    if not new_products:
        print("[!] No products to merge.")
        return ""

    df_new = pd.DataFrame(new_products)
    df_new["product_id"] = df_new["product_id"].astype("string")
    df_new["title"] = df_new["title"].astype("string")
    df_new["brand"] = df_new["brand"].astype("string")
    df_new["category_primary"] = df_new["category_primary"].astype("string")
    df_new["category_secondary"] = df_new["category_secondary"].astype("string")
    df_new["base_retail_price"] = df_new["base_retail_price"].astype("float64")
    df_new["description"] = df_new["description"].astype("string")
    df_new["first_seen_date"] = pd.to_datetime(df_new["first_seen_date"]).dt.date
    if "photo_url" in df_new.columns:
        df_new["photo_url"] = df_new["photo_url"].fillna("").astype("string")
    else:
        df_new["photo_url"] = ""

    con = get_configured_duckdb_conn()

    if use_r2 and is_r2_configured():
        catalog_path = f"s3://{R2_BUCKET_NAME}/dim_products/products_catalog.parquet"
    else:
        local_dir = os.path.join("data_lake", "dim_products")
        os.makedirs(local_dir, exist_ok=True)
        catalog_path = os.path.join(local_dir, "products_catalog.parquet").replace("\\", "/")

    con.register("new_products_view", df_new)

    catalog_exists = False
    try:
        con.execute(f"SELECT COUNT(*) FROM '{catalog_path}'").fetchone()
        catalog_exists = True
    except Exception:
        catalog_exists = False

    if catalog_exists:
        print(f"[*] Merging {len(df_new)} incoming products with existing catalog at: {catalog_path}...")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE merged_catalog AS
            WITH combined AS (
                SELECT 
                    COALESCE(n.product_id, o.product_id) AS product_id,
                    COALESCE(NULLIF(n.title, ''), o.title) AS title,
                    COALESCE(NULLIF(n.brand, ''), o.brand) AS brand,
                    COALESCE(NULLIF(n.category_primary, ''), o.category_primary) AS category_primary,
                    COALESCE(NULLIF(n.category_secondary, ''), o.category_secondary) AS category_secondary,
                    COALESCE(NULLIF(n.base_retail_price, 0.0), o.base_retail_price) AS base_retail_price,
                    COALESCE(NULLIF(n.description, ''), o.description) AS description,
                    COALESCE(o.first_seen_date, n.first_seen_date) AS first_seen_date,
                    COALESCE(NULLIF(n.photo_url, ''), o.photo_url) AS photo_url
                FROM '{catalog_path}' o
                FULL OUTER JOIN new_products_view n ON o.product_id = n.product_id
            )
            SELECT DISTINCT ON (product_id) * FROM combined;
        """).fetchall()
        con.execute(f"COPY merged_catalog TO '{catalog_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');").fetchall()
        row_count = con.execute("SELECT COUNT(*) FROM merged_catalog").fetchone()[0]
        print(f"[✓] Product catalog updated. Total unique products: {row_count:,}")
    else:
        print(f"[*] Initializing new product catalog at: {catalog_path}...")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE init_catalog AS
            SELECT DISTINCT ON (product_id) * FROM new_products_view;
        """).fetchall()
        con.execute(f"COPY init_catalog TO '{catalog_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');").fetchall()
        row_count = con.execute("SELECT COUNT(*) FROM init_catalog").fetchone()[0]
        print(f"[✓] Product catalog initialized. Total unique products: {row_count:,}")

    con.close()
    return catalog_path

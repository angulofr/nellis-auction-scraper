#!/usr/bin/env python3
"""
Nellis Auction Cloudflare R2 / Parquet Loader Module
Handles:
  1. Hive-partitioned writing of daily fact_auctions Parquet files
  2. Merging & upserting deduplicated dim_products product catalog
  3. Automatic S3/R2 authentication with local ./data_lake fallback
"""

import datetime
import os
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Environment configuration
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL", "https://965a5411101d1521c551d99d9421a145.r2.cloudflarestorage.com")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "nellis-auction-lake")

# Extract host from endpoint URL for DuckDB S3 endpoint
def get_r2_host():
    url = R2_ENDPOINT_URL.replace("https://", "").replace("http://", "").strip("/")
    return url


def is_r2_configured() -> bool:
    """Check if Cloudflare R2 credentials are present and non-empty."""
    return bool(R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_ENDPOINT_URL and R2_BUCKET_NAME)


def get_configured_duckdb_conn():
    """Create a DuckDB connection pre-configured with Cloudflare R2 S3/httpfs settings."""
    con = duckdb.connect()
    if is_r2_configured():
        con.execute("INSTALL httpfs; LOAD httpfs;")
        host = get_r2_host()
        con.execute(f"""
            SET s3_endpoint = '{host}';
            SET s3_access_key_id = '{R2_ACCESS_KEY_ID}';
            SET s3_secret_access_key = '{R2_SECRET_ACCESS_KEY}';
            SET s3_url_style = 'path';
            SET s3_region = 'auto';
            SET s3_use_ssl = true;
        """)
    return con


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

    df = pd.DataFrame(fact_records)

    # Convert types for strict schema conformance
    df["auction_id"] = df["auction_id"].astype("int64")
    df["product_id"] = df["product_id"].astype("string")
    df["location"] = df["location"].astype("string")
    df["condition"] = df["condition"].astype("string")
    df["completeness"] = df["completeness"].astype("string")
    df["functional_status"] = df["functional_status"].astype("string")
    df["damage_notes"] = df["damage_notes"].astype("string")
    df["retail_price"] = df["retail_price"].astype("float64")
    df["final_price"] = df["final_price"].astype("float64")
    df["discount_ratio"] = df["discount_ratio"].astype("float64")
    df["bid_count"] = df["bid_count"].astype("int32")
    df["unique_bidders"] = df["unique_bidders"].astype("int32")
    df["closed_at"] = pd.to_datetime(df["closed_at"], format="ISO8601", errors="coerce")
    df["close_day_of_week"] = df["close_day_of_week"].astype("Int8")
    df["close_hour"] = df["close_hour"].astype("Int8")
    df["scraped_date"] = pd.to_datetime(df["scraped_date"]).dt.date
    if "photo_url" in df.columns:
        df["photo_url"] = df["photo_url"].fillna("").astype("string")
    else:
        df["photo_url"] = ""

    con = get_configured_duckdb_conn()

    if use_r2 and is_r2_configured():
        target_path = f"s3://{R2_BUCKET_NAME}/fact_auctions/year={year}/month={month}/auctions_{scrape_date}.parquet"
    else:
        local_dir = os.path.join("data_lake", "fact_auctions", f"year={year}", f"month={month}")
        os.makedirs(local_dir, exist_ok=True)
        target_path = os.path.join(local_dir, f"auctions_{scrape_date}.parquet").replace("\\", "/")

    con.register("daily_facts_view", df)

    # Check if partition already exists so we can upsert and preserve earlier closed lots
    partition_exists = False
    try:
        con.execute(f"SELECT COUNT(*) FROM '{target_path}'")
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
            ORDER BY auction_id, final_price DESC, bid_count DESC;
        """)
        con.execute(f"""
            COPY merged_facts TO '{target_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """)
        total_rows = con.execute("SELECT COUNT(*) FROM merged_facts").fetchone()[0]
        print(f"[✓] Successfully merged fact_auctions Parquet ({total_rows:,} total rows)")
    else:
        print(f"[*] Writing {len(df)} fact records to: {target_path}...")
        con.execute(f"""
            COPY daily_facts_view TO '{target_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """)
        print(f"[✓] Successfully wrote fact_auctions Parquet ({len(df):,} rows)")

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

    # Check if catalog exists
    catalog_exists = False
    try:
        con.execute(f"SELECT COUNT(*) FROM '{catalog_path}'")
        catalog_exists = True
    except Exception:
        catalog_exists = False

    if catalog_exists:
        print(f"[*] Merging {len(df_new)} incoming products with existing catalog at: {catalog_path}...")
        # Union with deduplication on product_id (keep existing first_seen_date, update non-empty fields)
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
        """)
        con.execute(f"""
            COPY merged_catalog TO '{catalog_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """)
        row_count = con.execute("SELECT COUNT(*) FROM merged_catalog").fetchone()[0]
        print(f"[✓] Product catalog updated. Total unique products: {row_count:,}")
    else:
        print(f"[*] Initializing new product catalog at: {catalog_path}...")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE init_catalog AS
            SELECT DISTINCT ON (product_id) * FROM new_products_view;
        """)
        con.execute(f"""
            COPY init_catalog TO '{catalog_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """)
        row_count = con.execute("SELECT COUNT(*) FROM init_catalog").fetchone()[0]
        print(f"[✓] Product catalog initialized. Total unique products: {row_count:,}")

    con.close()
    return catalog_path

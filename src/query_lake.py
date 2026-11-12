#!/usr/bin/env python3
"""
DuckDB Cloudflare R2 Query Lake Helper
Executes sub-second analytical queries against partitioned Parquet files stored in Cloudflare R2.
"""

import argparse
import os
import sys
import duckdb
import pandas as pd

R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "8716bfa9053104690f8b939b7a8218d0")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "41b6f9dbb1ff938f1a057d4fb3874344f4ff3071a5e3dd6d387d8163e0893ff8")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL", "https://965a5411101d1521c551d99d9421a145.r2.cloudflarestorage.com")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "nellis-auction-lake")


def get_r2_host():
    return R2_ENDPOINT_URL.replace("https://", "").replace("http://", "").strip("/")


def get_duckdb_lake_conn(use_local: bool = False):
    """Initialize DuckDB with httpfs S3/R2 credentials or local fallback."""
    con = duckdb.connect()
    if not use_local and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY:
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
        facts_pattern = f"s3://{R2_BUCKET_NAME}/fact_auctions/*/*/*.parquet"
        dim_catalog = f"s3://{R2_BUCKET_NAME}/dim_products/products_catalog.parquet"
    else:
        facts_pattern = "data_lake/fact_auctions/*/*/*.parquet"
        dim_catalog = "data_lake/dim_products/products_catalog.parquet"

    # Create convenient views so queries can just use 'fact_auctions' and 'dim_products'
    con.execute(f"CREATE OR REPLACE VIEW fact_auctions AS SELECT * FROM read_parquet('{facts_pattern}');")
    con.execute(f"CREATE OR REPLACE VIEW dim_products AS SELECT * FROM read_parquet('{dim_catalog}');")
    return con


# Pre-defined analytical queries
PRESET_QUERIES = {
    "category-spread": {
        "title": "Category & Brand Value Spreads (Lowest Median Discount Ratio)",
        "sql": """
            SELECT 
                COALESCE(NULLIF(p.category_primary, ''), 'Uncategorized') AS category,
                COALESCE(NULLIF(p.brand, ''), 'Generic') AS brand,
                COUNT(f.auction_id) AS total_lots_sold,
                ROUND(MEDIAN(f.discount_ratio), 4) AS median_discount_ratio,
                ROUND(AVG(f.retail_price), 2) AS avg_retail_price,
                ROUND(AVG(f.final_price), 2) AS avg_winning_bid,
                ROUND(AVG(f.bid_count), 1) AS avg_bids
            FROM fact_auctions f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE f.discount_ratio IS NOT NULL AND f.retail_price >= 20.0
            GROUP BY category, brand
            HAVING COUNT(f.auction_id) >= 5
            ORDER BY median_discount_ratio ASC
            LIMIT 25;
        """
    },
    "condition-arbitrage": {
        "title": "Condition Risk Arbitrage (Brand New vs. Used vs. Damaged / Untested)",
        "sql": """
            SELECT 
                f.condition,
                f.completeness,
                f.functional_status,
                COUNT(f.auction_id) AS lot_count,
                ROUND(AVG(f.retail_price), 2) AS avg_retail,
                ROUND(AVG(f.final_price), 2) AS avg_winning_bid,
                ROUND(AVG(f.discount_ratio), 4) AS avg_discount_ratio,
                ROUND(MEDIAN(f.discount_ratio), 4) AS median_discount_ratio,
                ROUND(AVG(f.bid_count), 1) AS avg_bids
            FROM fact_auctions f
            WHERE f.discount_ratio IS NOT NULL
            GROUP BY f.condition, f.completeness, f.functional_status
            ORDER BY lot_count DESC
            LIMIT 20;
        """
    },
    "temporal": {
        "title": "Temporal Sweet Spots (Competition Heatmap by Day & Hour MST)",
        "sql": """
            SELECT 
                f.close_day_of_week,
                CASE f.close_day_of_week
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                    ELSE 'Unknown'
                END AS day_name,
                f.close_hour,
                COUNT(f.auction_id) AS items_closing,
                ROUND(AVG(f.discount_ratio), 4) AS avg_discount_ratio,
                ROUND(MEDIAN(f.discount_ratio), 4) AS median_discount_ratio,
                ROUND(AVG(f.bid_count), 1) AS avg_bids_per_item
            FROM fact_auctions f
            WHERE f.close_day_of_week IS NOT NULL AND f.close_hour IS NOT NULL
            GROUP BY f.close_day_of_week, day_name, f.close_hour
            ORDER BY f.close_day_of_week ASC, f.close_hour ASC;
        """
    },
    "sleepers": {
        "title": "Sleeper / Low-Bid High-Value Hunting (Retail > $150, Bids < 10, Discount Ratio < 0.25)",
        "sql": """
            SELECT 
                f.auction_id,
                p.title,
                f.location,
                f.condition,
                f.retail_price,
                f.final_price,
                ROUND(f.discount_ratio, 4) AS discount_ratio,
                f.bid_count,
                f.damage_notes,
                f.closed_at
            FROM fact_auctions f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE f.retail_price >= 150.0
              AND f.bid_count <= 10
              AND f.discount_ratio <= 0.25
              AND f.condition IN ('Brand New', 'Like New', 'Open Box')
            ORDER BY f.retail_price DESC
            LIMIT 30;
        """
    }
}


def print_df_pretty(df: pd.DataFrame, title: str):
    """Print query results in a clean formatted table."""
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)
    if df.empty:
        print("No matching rows found.")
        return

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", 50)
    print(df.to_string(index=False))
    print(f"\n[+] Total rows returned: {len(df)}")


def main():
    parser = argparse.ArgumentParser(description="Query the Nellis Auction Data Lake on Cloudflare R2 using DuckDB.")
    parser.add_argument("--query", "-q", choices=list(PRESET_QUERIES.keys()), help="Run one of the 4 core preset analytical queries")
    parser.add_argument("--sql", "-s", type=str, help="Execute a custom SQL statement against fact_auctions & dim_products")
    parser.add_argument("--local", action="store_true", help="Query local ./data_lake Parquet files instead of Cloudflare R2")
    parser.add_argument("--interactive", "-i", action="store_true", help="Launch interactive DuckDB SQL prompt over R2")
    args = parser.parse_args()

    target = "local ./data_lake" if args.local else f"Cloudflare R2 (s3://{R2_BUCKET_NAME})"
    print(f"[*] Connecting DuckDB to: {target}...")

    con = get_duckdb_lake_conn(use_local=args.local)

    if args.interactive:
        print("\nInteractive DuckDB Data Lake Prompt. Tables available: 'fact_auctions', 'dim_products'")
        print("Type 'exit' or press Ctrl+C to quit.\n")
        while True:
            try:
                user_sql = input("duckdb-r2> ")
                if not user_sql.strip():
                    continue
                if user_sql.strip().lower() in ["exit", "quit", "q"]:
                    break
                df = con.execute(user_sql).df()
                print_df_pretty(df, "Query Results")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"[!] Error: {e}")
        con.close()
        return

    if args.query:
        preset = PRESET_QUERIES[args.query]
        df = con.execute(preset["sql"]).df()
        print_df_pretty(df, preset["title"])
        con.close()
        return

    if args.sql:
        df = con.execute(args.sql).df()
        print_df_pretty(df, "Custom SQL Query")
        con.close()
        return

    # If no arguments provided, run a quick overview summary
    print("\nNo specific query passed. Running high-level Lake Summary:\n")
    summary_sql = """
        SELECT 
            (SELECT COUNT(*) FROM fact_auctions) AS total_auction_facts,
            (SELECT COUNT(*) FROM dim_products) AS total_unique_products,
            (SELECT ROUND(SUM(retail_price), 2) FROM fact_auctions) AS total_retail_volume_usd,
            (SELECT ROUND(AVG(discount_ratio), 3) FROM fact_auctions WHERE discount_ratio IS NOT NULL) AS lake_avg_discount_ratio;
    """
    df_summary = con.execute(summary_sql).df()
    print_df_pretty(df_summary, "Nellis Auction Lake Statistics")

    print("\nTip: Run with pre-defined analytical models:")
    print("  python -m src.query_lake --query category-spread")
    print("  python -m src.query_lake --query condition-arbitrage")
    print("  python -m src.query_lake --query temporal")
    print("  python -m src.query_lake --query sleepers")
    print("  python -m src.query_lake --sql \"SELECT * FROM fact_auctions LIMIT 5\"")
    con.close()


if __name__ == "__main__":
    main()

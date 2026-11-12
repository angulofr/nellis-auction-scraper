#!/usr/bin/env python3
"""
Nellis Auction Automated Ingestion & Pipeline Runner
Fetches active and finished auctions for Phoenix & Mesa, transforms into Star Schema,
and loads Parquet files directly into Cloudflare R2 Data Lake (or local fallback).
"""

import argparse
import datetime
import http.cookiejar
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.transform import transform_records
from src.r2_loader import write_fact_auctions, merge_dim_products, is_r2_configured

BASE_URL = "https://www.nellisauction.com"
PHOENIX_LOCATION_ID = 2  # Shopping location ID for Phoenix, AZ (Phoenix & Mesa warehouses)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
MST_TZ = datetime.timezone(datetime.timedelta(hours=-7))


def get_authenticated_session():
    """Create opener and set the Phoenix shopping location cookie."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

    post_data = urllib.parse.urlencode({
        "shoppingLocationId": PHOENIX_LOCATION_ID,
        "referrer": "/"
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{BASE_URL}/change-shopping-location",
        data=post_data,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded"
        }
    )
    try:
        opener.open(req, timeout=15)
    except Exception as e:
        print(f"[!] Warning during cookie initialization: {e}")

    return opener


def unflatten_val(val, raw, memo=None):
    """Recursively resolve React Router / Turbo-Stream serialized indexes."""
    if memo is None:
        memo = {}

    if isinstance(val, int):
        if val < 0:
            return None
        if val in memo:
            return memo[val]
        if val >= len(raw):
            return None

        raw_val = raw[val]
        if isinstance(raw_val, (dict, list)):
            res = {} if isinstance(raw_val, dict) else []
            memo[val] = res
            if isinstance(raw_val, dict):
                for k, v in raw_val.items():
                    rk = raw[int(k[1:])] if k.startswith("_") and k[1:].isdigit() else k
                    res[rk] = unflatten_val(v, raw, memo)
            else:
                if len(raw_val) == 2 and raw_val[0] == "D":
                    return raw_val[1]
                for item in raw_val:
                    res.append(unflatten_val(item, raw, memo))
            return res
        else:
            memo[val] = raw_val
            return raw_val

    elif isinstance(val, dict):
        res = {}
        for k, v in val.items():
            rk = raw[int(k[1:])] if k.startswith("_") and k[1:].isdigit() else k
            res[rk] = unflatten_val(v, raw, memo)
        return res

    elif isinstance(val, list):
        if len(val) == 2 and val[0] == "D":
            return val[1]
        return [unflatten_val(x, raw, memo) for x in val]

    return val


def fetch_page(opener, location_name: str, page_num: int, page_size: int = 120, max_retries: int = 3):
    """Fetch a single page of items for a specific location."""
    params = [
        ("Location Name", location_name),
        ("_p1", f"s:{page_size},n:{page_num}")
    ]
    qs = urllib.parse.urlencode(params)
    url = f"{BASE_URL}/search.data?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    for attempt in range(max_retries):
        try:
            with opener.open(req, timeout=20) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            total_count = 0
            for i, item in enumerate(data):
                if item == "searchResultsCount" and i + 1 < len(data):
                    total_count = data[i + 1] if isinstance(data[i + 1], int) else 0
                    break

            products = []
            for i, item in enumerate(data):
                if item == "products" and i + 1 < len(data):
                    p_indices = data[i + 1]
                    if isinstance(p_indices, list):
                        for idx in p_indices:
                            prod = unflatten_val(idx, data)
                            if prod and isinstance(prod, dict):
                                products.append(prod)
                    break

            return total_count, products

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"[!] Error fetching page {page_num} for {location_name}: {e}")
                return 0, []


def scrape_all_locations(opener, locations: list, limit_per_loc: int = None, page_size: int = 120) -> list:
    """Scrape active auction catalog across specified locations."""
    all_items = []
    start_time = time.time()

    for loc in locations:
        print(f"\n[*] Querying listings for location: {loc}...")
        total_available, first_page = fetch_page(opener, loc, page_num=0, page_size=page_size)

        if not first_page and total_available == 0:
            print(f"[!] No items found for {loc}.")
            continue

        print(f"[+] Total items available in {loc}: {total_available:,}")
        target = total_available if limit_per_loc is None else min(total_available, limit_per_loc)
        total_pages = (target + page_size - 1) // page_size

        for item in first_page[:target]:
            item["location_name"] = loc
            all_items.append(item)

        print(f"  -> Page 1/{total_pages}: captured {len(first_page[:target])} items")

        for page in range(1, total_pages):
            if limit_per_loc and len([x for x in all_items if x.get("location_name") == loc]) >= limit_per_loc:
                break

            time.sleep(0.4)
            _, items = fetch_page(opener, loc, page_num=page, page_size=page_size)
            if not items:
                break

            for item in items:
                item["location_name"] = loc
                all_items.append(item)

            loc_count = len([x for x in all_items if x.get("location_name") == loc])
            pct = (loc_count / target) * 100 if target else 0
            if (page + 1) % 10 == 0 or (page + 1) == total_pages:
                print(f"  -> Page {page + 1}/{total_pages}: {loc_count:,}/{target:,} ({pct:.1f}%)")

        loc_total = len([x for x in all_items if x.get("location_name") == loc])
        print(f"[✓] Finished {loc}: {loc_total:,} items captured.")

    elapsed = time.time() - start_time
    print(f"\n[+] Total scraped across all locations: {len(all_items):,} items in {elapsed:.1f}s")
    return all_items


def main():
    parser = argparse.ArgumentParser(description="Nellis Auction Data Pipeline Runner (Scrape, Transform, Parquet, R2).")
    parser.add_argument("--locations", default="Phoenix,Mesa", help="Comma-separated warehouse locations (default: Phoenix,Mesa)")
    parser.add_argument("--limit", type=int, default=None, help="Item limit per location for testing (default: all)")
    parser.add_argument("--date", type=str, default=None, help="Scrape partition date YYYY-MM-DD (default: today)")
    parser.add_argument("--upload-r2", action="store_true", default=True, help="Upload Parquet files to Cloudflare R2 (default: True)")
    parser.add_argument("--local-only", action="store_true", help="Force saving to local ./data_lake instead of Cloudflare R2")
    args = parser.parse_args()

    locations = [loc.strip() for loc in args.locations.split(",") if loc.strip()]
    scrape_date = args.date or datetime.datetime.now(MST_TZ).strftime("%Y-%m-%d")
    use_r2 = False if args.local_only else is_r2_configured()

    print("=" * 70)
    print("Nellis Auction Data Pipeline: Ingestion & Parquet R2 Loader")
    print(f"Partition Date   : {scrape_date}")
    print(f"Target Locations : {', '.join(locations)}")
    print(f"Destination      : {'Cloudflare R2 (S3)' if use_r2 else 'Local Data Lake (./data_lake)'}")
    if args.limit:
        print(f"Item Limit       : {args.limit} per location")
    print("=" * 70)

    opener = get_authenticated_session()
    raw_items = scrape_all_locations(opener, locations, limit_per_loc=args.limit)

    if not raw_items:
        print("[!] No items captured. Exiting.")
        sys.exit(0)

    print("\n[*] Transforming raw records into Star Schema...")
    dim_products, fact_auctions = transform_records(raw_items, scrape_date=scrape_date)
    print(f"[+] Unique Products Catalog: {len(dim_products):,} records")
    print(f"[+] Daily Fact Auctions    : {len(fact_auctions):,} records")

    print("\n[*] Loading Parquet files into Data Lake...")
    fact_path = write_fact_auctions(fact_auctions, scrape_date=scrape_date, use_r2=use_r2)
    catalog_path = merge_dim_products(dim_products, use_r2=use_r2)

    print("\n" + "=" * 70)
    print("[✓] Pipeline Run Completed Successfully!")
    print(f"  • Fact Parquet    : {fact_path}")
    print(f"  • Product Catalog : {catalog_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()

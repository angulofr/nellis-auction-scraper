#!/usr/bin/env python3
"""
Nellis Auction Scraper & Finalizer for Phoenix & Mesa, AZ
Scrapes auction items from nellisauction.com and stores them in a local SQLite database.

Core Workflow:
  1. Discovery (Morning / Afternoon):
     Run `python nellis_scraper.py` to ingest all items closing today & upcoming days.
  2. Finalization (Night ~10:00 PM AZ time):
     Run `python nellis_scraper.py --finalize` to query every item that reached its close
     time and update it with the official winning bid, final bid count, and sold status.
"""

import argparse
import datetime
import http.cookiejar
import json
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://www.nellisauction.com"
PHOENIX_LOCATION_ID = 2  # Shopping location ID for Phoenix, AZ (covers Phoenix & Mesa warehouses)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Arizona is UTC-7 year-round (no DST)
MST_TZ = datetime.timezone(datetime.timedelta(hours=-7))


def setup_database(db_path: str) -> sqlite3.Connection:
    """Initialize SQLite database with comprehensive schema for analytics."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS auction_items (
            id INTEGER PRIMARY KEY,
            inventory_number TEXT UNIQUE,
            title TEXT NOT NULL,
            description TEXT,
            bid_total INTEGER DEFAULT 0,
            winning_bid_amount REAL DEFAULT 0.0,
            estimated_retail_price REAL DEFAULT 0.0,
            discount_pct REAL DEFAULT 0.0,
            closing_date TEXT,
            close_time_ms INTEGER,
            open_date TEXT,
            location_name TEXT,
            location_city TEXT,
            location_address TEXT,
            market_status TEXT,
            is_closed INTEGER DEFAULT 0,
            was_extended INTEGER DEFAULT 0,
            not_returnable INTEGER DEFAULT 0,
            buyer_premium_pct REAL DEFAULT 15.0,
            condition_type TEXT,
            condition_rating INTEGER,
            is_functional TEXT,
            damage_type TEXT,
            missing_parts TEXT,
            in_package TEXT,
            assembly_required TEXT,
            inspector_notes TEXT,
            brand TEXT,
            category TEXT,
            subcategory TEXT,
            color TEXT,
            size TEXT,
            dimensions TEXT,
            weight TEXT,
            catalog_description TEXT,
            features_json TEXT,
            item_url TEXT,
            photo_url TEXT,
            photo_count INTEGER DEFAULT 0,
            all_photos_json TEXT,
            last_updated TEXT
        )
    """)
    # Performance indexes for analytics and finalization queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_inventory_number ON auction_items(inventory_number)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_location ON auction_items(location_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_closing_date ON auction_items(close_time_ms)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_status ON auction_items(market_status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_is_closed ON auction_items(is_closed)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_condition_type ON auction_items(condition_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_condition_rating ON auction_items(condition_rating)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_damage_type ON auction_items(damage_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_is_functional ON auction_items(is_functional)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_missing_parts ON auction_items(missing_parts)")
    conn.commit()
    return conn


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


def format_timestamp(ts_ms: int) -> str:
    """Format millisecond epoch timestamp into Arizona local time ISO 8601."""
    if not ts_ms:
        return ""
    try:
        dt = datetime.datetime.fromtimestamp(ts_ms / 1000.0, tz=MST_TZ)
        return dt.strftime("%Y-%m-%d %H:%M:%S MST")
    except Exception:
        return ""


def build_description_summary(grade: dict, notes: str, catalog_desc: str = None, features: list = None) -> str:
    """Construct a readable, multi-line summary of condition, notes, and product info."""
    parts = []

    if grade:
        cond = grade.get("conditionType", {})
        cond_desc = cond.get("description") if isinstance(cond, dict) else cond
        func = grade.get("functionalType", {})
        func_desc = func.get("description") if isinstance(func, dict) else func
        dmg = grade.get("damageType", {})
        dmg_desc = dmg.get("description") if isinstance(dmg, dict) else dmg
        parts_type = grade.get("missingPartsType", {})
        parts_desc = parts_type.get("description") if isinstance(parts_type, dict) else parts_type
        pkg_type = grade.get("packageType", {})
        pkg_desc = pkg_type.get("description") if isinstance(pkg_type, dict) else pkg_type
        asm_type = grade.get("assemblyType", {})
        asm_desc = asm_type.get("description") if isinstance(asm_type, dict) else asm_type
        rating = grade.get("rating")

        grade_summary = []
        if cond_desc:
            grade_summary.append(f"Condition: {cond_desc}")
        if func_desc:
            grade_summary.append(f"Functional: {func_desc}")
        if dmg_desc and dmg_desc.lower() != "none":
            grade_summary.append(f"Damage: {dmg_desc}")
        if parts_desc and parts_desc.lower() != "no":
            grade_summary.append(f"Missing Parts: {parts_desc}")
        if pkg_desc:
            grade_summary.append(f"In Package: {pkg_desc}")
        if asm_desc and asm_desc.lower() == "yes":
            grade_summary.append("Assembly Required: Yes")
        if rating:
            grade_summary.append(f"Rating: {rating}/5")

        if grade_summary:
            parts.append(" | ".join(grade_summary))

    if notes and notes.strip():
        parts.append(f"Inspector Notes: {notes.strip()}")

    if catalog_desc and catalog_desc.strip():
        parts.append(f"Product Description: {catalog_desc.strip()}")

    if features and len(features) > 0:
        clean_features = [f.strip() for f in features if f and f.strip()]
        if clean_features:
            parts.append("Features:\n• " + "\n• ".join(clean_features[:5]))

    return "\n\n".join(parts) if parts else "No description provided."


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


def fetch_item_detail(opener, item_id: int):
    """Fetch individual item page details for extended attributes."""
    url = f"{BASE_URL}/p/item/{item_id}.data"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with opener.open(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        for i, item in enumerate(data):
            if item == "product" and i + 1 < len(data):
                prod = unflatten_val(data[i + 1], data)
                if prod and isinstance(prod, dict):
                    descs = prod.get("descriptions", [])
                    catalog_desc = descs[0] if descs else ""
                    features = prod.get("features", [])
                    return {
                        "brand": prod.get("brand") or "",
                        "category": prod.get("taxonomyLevel1") or "",
                        "subcategory": prod.get("taxonomyLevel2") or "",
                        "color": prod.get("color") or "",
                        "size": prod.get("size") or "",
                        "dimensions": prod.get("dimensions") or "",
                        "weight": prod.get("weight") or "",
                        "catalog_description": catalog_desc,
                        "features": features,
                        "is_closed": 1 if prod.get("isClosed") else 0,
                        "market_status": prod.get("marketStatus") or "open",
                        "current_price": float(prod.get("currentPrice") or 0.0),
                        "bid_count": int(prod.get("bidCount") or 0),
                        "close_time_ms": prod.get("closeTime") or 0,
                        "was_extended": 1 if prod.get("projectExtended") else 0
                    }
    except Exception:
        pass
    return {}


def save_items_to_db(conn: sqlite3.Connection, items: list, location_name: str, fetch_details: bool = False, opener = None):
    """Save batch of items to SQLite database using UPSERT with all attributes."""
    cursor = conn.cursor()
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

    records = []
    for item in items:
        item_id = item.get("id")
        inv_num = item.get("inventoryNumber") or f"ID_{item_id}"
        title = item.get("title") or "Untitled"
        retail_price = float(item.get("retailPrice") or 0.0)
        bid_total = int(item.get("bidCount") or 0)
        winning_bid = float(item.get("currentPrice") or 0.0)

        # Discount percentage
        discount_pct = 0.0
        if retail_price > 0 and winning_bid >= 0:
            discount_pct = round(((retail_price - winning_bid) / retail_price) * 100.0, 1)

        close_time_ms = item.get("closeTime") or 0
        closing_date_str = format_timestamp(close_time_ms)
        open_time_ms = item.get("openTime") or 0
        open_date_str = format_timestamp(open_time_ms)

        notes = item.get("notes") or ""
        grade = item.get("grade") or {}

        # Parse detailed grade attributes
        rating = grade.get("rating") if isinstance(grade, dict) else None
        cond_type = grade.get("conditionType", {}).get("description") if isinstance(grade.get("conditionType"), dict) else None
        damage_type = grade.get("damageType", {}).get("description") if isinstance(grade.get("damageType"), dict) else None
        func_type = grade.get("functionalType", {}).get("description") if isinstance(grade.get("functionalType"), dict) else None
        parts_type = grade.get("missingPartsType", {}).get("description") if isinstance(grade.get("missingPartsType"), dict) else None
        pkg_type = grade.get("packageType", {}).get("description") if isinstance(grade.get("packageType"), dict) else None
        asm_type = grade.get("assemblyType", {}).get("description") if isinstance(grade.get("assemblyType"), dict) else None

        # Fetch extra product details if requested
        detail_data = {}
        if fetch_details and opener:
            detail_data = fetch_item_detail(opener, item_id)

        catalog_desc = detail_data.get("catalog_description", "")
        features_list = detail_data.get("features", [])
        features_json = json.dumps(features_list) if features_list else None
        brand = detail_data.get("brand", "")
        category = detail_data.get("category", "")
        subcategory = detail_data.get("subcategory", "")
        color = detail_data.get("color", "")
        size = detail_data.get("size", "")
        dimensions = detail_data.get("dimensions", "")
        weight = detail_data.get("weight", "")

        description = build_description_summary(grade, notes, catalog_desc, features_list)

        loc = item.get("location") or {}
        loc_name = loc.get("name") or location_name
        loc_city = loc.get("city") or location_name
        loc_address = loc.get("address") or ""
        market_status = item.get("marketStatus") or "open"
        is_closed = 1 if item.get("isClosed") else 0
        was_extended = 1 if item.get("projectExtended") else 0
        not_returnable = 1 if item.get("notReturnable") else 0
        buyer_premium = float(item.get("buyerPremium") or 15.0)

        # Build direct URL
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", title).strip("-")[:60]
        item_url = f"{BASE_URL}/p/{slug}/{item_id}"

        # Photos
        photos = item.get("photos") or []
        photo_count = len(photos)
        all_photo_urls = [p.get("url") for p in photos if isinstance(p, dict) and p.get("url")]
        photo_url = all_photo_urls[0] if all_photo_urls else ""
        all_photos_json = json.dumps(all_photo_urls) if all_photo_urls else None

        records.append((
            item_id,
            inv_num,
            title,
            description,
            bid_total,
            winning_bid,
            retail_price,
            discount_pct,
            closing_date_str,
            close_time_ms,
            open_date_str,
            loc_name,
            loc_city,
            loc_address,
            market_status,
            is_closed,
            was_extended,
            not_returnable,
            buyer_premium,
            cond_type,
            rating,
            func_type,
            damage_type,
            parts_type,
            pkg_type,
            asm_type,
            notes,
            brand,
            category,
            subcategory,
            color,
            size,
            dimensions,
            weight,
            catalog_desc,
            features_json,
            item_url,
            photo_url,
            photo_count,
            all_photos_json,
            now_str
        ))

    cursor.executemany("""
        INSERT INTO auction_items (
            id, inventory_number, title, description, bid_total, winning_bid_amount,
            estimated_retail_price, discount_pct, closing_date, close_time_ms, open_date,
            location_name, location_city, location_address, market_status, is_closed,
            was_extended, not_returnable, buyer_premium_pct, condition_type, condition_rating,
            is_functional, damage_type, missing_parts, in_package, assembly_required,
            inspector_notes, brand, category, subcategory, color, size, dimensions, weight,
            catalog_description, features_json, item_url, photo_url, photo_count,
            all_photos_json, last_updated
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(inventory_number) DO UPDATE SET
            title = excluded.title,
            description = excluded.description,
            bid_total = excluded.bid_total,
            winning_bid_amount = excluded.winning_bid_amount,
            estimated_retail_price = excluded.estimated_retail_price,
            discount_pct = excluded.discount_pct,
            closing_date = excluded.closing_date,
            close_time_ms = excluded.close_time_ms,
            location_name = excluded.location_name,
            market_status = excluded.market_status,
            is_closed = excluded.is_closed,
            was_extended = excluded.was_extended,
            not_returnable = excluded.not_returnable,
            buyer_premium_pct = excluded.buyer_premium_pct,
            condition_type = excluded.condition_type,
            condition_rating = excluded.condition_rating,
            is_functional = excluded.is_functional,
            damage_type = excluded.damage_type,
            missing_parts = excluded.missing_parts,
            in_package = excluded.in_package,
            assembly_required = excluded.assembly_required,
            inspector_notes = excluded.inspector_notes,
            brand = excluded.brand,
            category = excluded.category,
            subcategory = excluded.subcategory,
            color = excluded.color,
            size = excluded.size,
            dimensions = excluded.dimensions,
            weight = excluded.weight,
            catalog_description = excluded.catalog_description,
            features_json = excluded.features_json,
            photo_count = excluded.photo_count,
            all_photos_json = excluded.all_photos_json,
            last_updated = excluded.last_updated
    """, records)

    conn.commit()
    return len(records)


def finalize_closed_items(conn: sqlite3.Connection, opener, workers: int = 10, limit: int = None):
    """
    Query items in the database that have reached or passed their closing time,
    fetch their direct endpoints concurrently, and save official final prices and bid counts.
    """
    now_ms = int(time.time() * 1000)
    cursor = conn.cursor()

    query = """
        SELECT id, inventory_number, title, estimated_retail_price
        FROM auction_items
        WHERE is_closed = 0 AND close_time_ms <= ?
        ORDER BY close_time_ms ASC
    """
    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query, (now_ms,))
    pending_items = cursor.fetchall()

    if not pending_items:
        print("[+] No pending closed items need finalization right now.")
        return 0

    print(f"\n[*] Finalizing {len(pending_items)} items that have finished closing...")
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    updated_count = 0

    def check_single_item(item_info):
        item_id, inv_num, title, retail = item_info
        url = f"{BASE_URL}/p/item/{item_id}.data"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with opener.open(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            for i, item in enumerate(data):
                if item == "product" and i + 1 < len(data):
                    prod = unflatten_val(data[i + 1], data)
                    if prod and isinstance(prod, dict):
                        is_closed = 1 if prod.get("isClosed") else 0
                        status = prod.get("marketStatus") or "closed"
                        price = float(prod.get("currentPrice") or 0.0)
                        bids = int(prod.get("bidCount") or 0)
                        close_ms = prod.get("closeTime") or 0
                        extended = 1 if prod.get("projectExtended") else 0
                        closing_date = format_timestamp(close_ms)

                        disc = 0.0
                        if retail > 0 and price >= 0:
                            disc = round(((retail - price) / retail) * 100.0, 1)

                        return (item_id, price, bids, is_closed, status, closing_date, close_ms, extended, disc, None)
        except Exception as e:
            return (item_id, None, None, None, None, None, None, None, None, str(e))
        return (item_id, None, None, None, None, None, None, None, None, "No product data")

    results_to_update = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(check_single_item, item): item for item in pending_items}
        for idx, future in enumerate(as_completed(futures)):
            res = future.result()
            if res[1] is not None:
                results_to_update.append(res)
            if (idx + 1) % 25 == 0 or (idx + 1) == len(pending_items):
                print(f"  -> Checked {idx + 1}/{len(pending_items)} items...")

    for (item_id, price, bids, is_closed, status, closing_date, close_ms, extended, disc, _) in results_to_update:
        cursor.execute("""
            UPDATE auction_items SET
                winning_bid_amount = ?,
                bid_total = ?,
                is_closed = ?,
                market_status = ?,
                closing_date = ?,
                close_time_ms = ?,
                was_extended = ?,
                discount_pct = ?,
                last_updated = ?
            WHERE id = ?
        """, (price, bids, is_closed, status, closing_date, close_ms, extended, disc, now_str, item_id))
        updated_count += 1

    conn.commit()
    print(f"[✓] Finalized and updated {updated_count}/{len(pending_items)} items.")
    return updated_count


def export_to_csv(conn: sqlite3.Connection, csv_path: str):
    """Export all database attributes to a comprehensive CSV file."""
    import csv
    cursor = conn.cursor()
    cursor.execute("""
        SELECT closing_date, title, description, bid_total, winning_bid_amount,
               estimated_retail_price, discount_pct, inventory_number, location_name,
               market_status, condition_type, condition_rating, is_functional,
               damage_type, missing_parts, in_package, assembly_required,
               inspector_notes, not_returnable, was_extended, photo_count, item_url
        FROM auction_items
        ORDER BY close_time_ms ASC
    """)
    rows = cursor.fetchall()
    headers = [
        "closing_date", "title", "description", "bid_total", "winning_bid_amount",
        "estimated_retail_price", "discount_pct", "inventory_number", "location_name",
        "market_status", "condition_type", "condition_rating", "is_functional",
        "damage_type", "missing_parts", "in_package", "assembly_required",
        "inspector_notes", "not_returnable", "was_extended", "photo_count", "item_url"
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"[+] Exported {len(rows)} items to {csv_path}")


def main():
    parser = argparse.ArgumentParser(description="Nellis Auction Scraper & Finalizer for Phoenix & Mesa.")
    parser.add_argument("--locations", default="Phoenix,Mesa", help="Comma-separated warehouse locations (default: Phoenix,Mesa)")
    parser.add_argument("--db", default="nellis_auctions.db", help="Path to SQLite database file (default: nellis_auctions.db)")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of items to scrape per location")
    parser.add_argument("--page-size", type=int, default=120, help="Items per page request (default: 120)")
    parser.add_argument("--fetch-details", action="store_true", help="Also fetch full manufacturer descriptions, brand, and category")
    parser.add_argument("--finalize", action="store_true", help="Only finalize past closing time items in the database (record final winning bid, total bids, status)")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent workers for detail/finalizing requests (default: 10)")
    parser.add_argument("--export-csv", type=str, default=None, help="Optional CSV file path to export to")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    conn = setup_database(db_path)
    opener = get_authenticated_session()

    print("=" * 65)
    print("Nellis Auction Scraper & Finalizer (Phoenix & Mesa, AZ)")
    print(f"Database File    : {db_path}")

    # Mode 1: Finalize closed items only
    if args.finalize:
        print("Mode             : Finalizing past closed items")
        print("=" * 65)
        finalize_closed_items(conn, opener, workers=args.workers, limit=args.limit)
        if args.export_csv:
            export_to_csv(conn, args.export_csv)
        conn.close()
        return

    # Mode 2: Catalog Scrape
    locations = [loc.strip() for loc in args.locations.split(",") if loc.strip()]
    print(f"Target Locations : {', '.join(locations)}")
    if args.limit:
        print(f"Item Limit       : {args.limit} per location")
    print(f"Fetch Details    : {'Yes' if args.fetch_details else 'No (Fast Batch Mode with Condition & Notes)'}")
    print("=" * 65)

    total_scraped_all = 0
    start_time = time.time()

    for loc in locations:
        print(f"\n[*] Querying listings for location: {loc}...")
        total_available, first_page_items = fetch_page(opener, loc, page_num=0, page_size=args.page_size)

        if not first_page_items and total_available == 0:
            print(f"[!] No items found for {loc}.")
            continue

        print(f"[+] Total available items in {loc}: {total_available}")

        target_items = total_available if args.limit is None else min(total_available, args.limit)
        total_pages = (target_items + args.page_size - 1) // args.page_size

        saved_count = save_items_to_db(conn, first_page_items[:target_items], loc, args.fetch_details, opener)
        loc_scraped = saved_count
        print(f"  -> Page 1/{total_pages}: saved {saved_count} items (Running total: {loc_scraped}/{target_items})")

        for page in range(1, total_pages):
            if args.limit and loc_scraped >= args.limit:
                break

            time.sleep(0.5)  # Respectful pacing between page fetches
            _, items = fetch_page(opener, loc, page_num=page, page_size=args.page_size)

            if not items:
                print(f"[!] End of items reached at page {page + 1}")
                break

            remaining = target_items - loc_scraped
            items_to_save = items[:remaining]
            saved = save_items_to_db(conn, items_to_save, loc, args.fetch_details, opener)
            loc_scraped += saved

            pct = (loc_scraped / target_items) * 100 if target_items else 0
            print(f"  -> Page {page + 1}/{total_pages}: saved {saved} items ({loc_scraped}/{target_items} - {pct:.1f}%)")

        total_scraped_all += loc_scraped
        print(f"[+] Finished {loc}: {loc_scraped} items stored.")

    elapsed = time.time() - start_time
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM auction_items")
    db_total = cursor.fetchone()[0]

    print("\n" + "=" * 65)
    print(f"[✓] Catalog scrape complete in {elapsed:.1f} seconds!")
    print(f"[✓] Total items processed this run : {total_scraped_all}")
    print(f"[✓] Total items in database table  : {db_total}")
    print("=" * 65)

    if args.export_csv:
        export_to_csv(conn, args.export_csv)

    conn.close()


if __name__ == "__main__":
    main()

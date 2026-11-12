#!/usr/bin/env python3
"""
Nellis Auction Data Transformation Module
Normalizes raw scraped auction records into a Star Schema:
  - dim_products: Deduplicated product catalog with deterministic SHA-256 keys
  - fact_auctions: Lean, numeric-heavy table for daily completed lots
"""

import datetime
import hashlib
import re
from typing import Dict, List, Tuple

# Arizona is UTC-7 year-round (no DST)
MST_TZ = datetime.timezone(datetime.timedelta(hours=-7))


def normalize_title(title: str) -> str:
    """Normalize product title for deterministic hashing and deduplication."""
    if not title:
        return ""
    # Remove leading/trailing inspector tags like ** FACTORY SEALED **, (broken corner), etc.
    t = re.sub(r"^\*+.*?\*+", "", title)
    t = re.sub(r"^\(.*?\)", "", t)
    # Lowercase, collapse spaces, remove special non-alphanumeric punctuation
    t = re.sub(r"[^\w\s]", " ", t.lower())
    t = re.sub(r"\s+", " ", t).strip()
    return t


def generate_product_id(title: str, retail_price: float) -> str:
    """
    Generate deterministic SHA-256 product ID hash from normalized title and retail price.
    Returns 32-character hex digest.
    """
    norm_title = normalize_title(title)
    norm_price = f"{round(float(retail_price or 0.0), 2):.2f}"
    raw_key = f"{norm_title}|{norm_price}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:32]


def standardize_condition(cond_type: str, damage_type: str, rating: int) -> str:
    """Standardize condition string into canonical category."""
    c = (cond_type or "").strip().lower()
    d = (damage_type or "").strip().lower()

    if "new" in c:
        return "Brand New"
    if "open" in c:
        return "Open Box"
    if "damage" in d or "major" in d or (rating and rating <= 2):
        return "Damaged"
    if "used" in c:
        return "Used"
    if "untested" in c or "unknown" in c:
        return "Untested"
    return "Like New" if rating and rating >= 4 else "Used"


def standardize_completeness(missing_parts: str) -> str:
    """Standardize completeness status."""
    mp = (missing_parts or "").strip().lower()
    if mp in ["no", "none", "complete"]:
        return "Complete"
    if "partial" in mp:
        return "Partial Set"
    if "yes" in mp or "missing" in mp:
        return "Missing Parts"
    return "Unknown"


def standardize_functionality(func_type: str) -> str:
    """Standardize functional status."""
    f = (func_type or "").strip().lower()
    if f in ["yes", "functional", "tested - works"]:
        return "Tested - Works"
    if f in ["no", "not functional"]:
        return "Not Functional"
    if "power" in f:
        return "Powers On"
    return "Untested"


def transform_records(raw_items: List[Dict], scrape_date: str = None) -> Tuple[List[Dict], List[Dict]]:
    """
    Transform raw auction item dictionaries into:
      1. dim_products (deduplicated product catalog records)
      2. fact_auctions (daily auction transactional records)
    """
    if not scrape_date:
        scrape_date = datetime.datetime.now(MST_TZ).strftime("%Y-%m-%d")

    dim_products_map = {}
    fact_auctions = []

    for item in raw_items:
        # Extract base identifiers
        auction_id = int(item.get("id"))
        title = item.get("title") or "Untitled"
        retail_price = round(float(item.get("retailPrice") or item.get("estimated_retail_price") or 0.0), 2)
        final_price = round(float(item.get("currentPrice") or item.get("winning_bid_amount") or 0.0), 2)

        # Generate deterministic product ID
        product_id = generate_product_id(title, retail_price)

        # Timestamps and temporal analysis
        close_ms = item.get("closeTime") or item.get("close_time_ms") or 0
        if close_ms:
            close_dt_mst = datetime.datetime.fromtimestamp(close_ms / 1000.0, tz=MST_TZ)
            close_day_of_week = (close_dt_mst.weekday() + 1) % 7  # 0=Sunday through 6=Saturday
            close_hour = close_dt_mst.hour
            closed_at_iso = close_dt_mst.isoformat()
        else:
            closed_at_iso = None
            close_day_of_week = None
            close_hour = None

        # Computed discount ratio (final_price / retail_price)
        if retail_price > 0 and final_price >= 0:
            discount_ratio = round(final_price / retail_price, 4)
        else:
            discount_ratio = None

        # Condition & inspection attributes
        grade = item.get("grade") if isinstance(item.get("grade"), dict) else {}
        cond_raw = item.get("condition_type") or grade.get("conditionType", {}).get("description")
        damage_raw = item.get("damage_type") or grade.get("damageType", {}).get("description")
        func_raw = item.get("is_functional") or grade.get("functionalType", {}).get("description")
        missing_raw = item.get("missing_parts") or grade.get("missingPartsType", {}).get("description")
        rating = item.get("condition_rating") or grade.get("rating")
        notes = (item.get("inspector_notes") or item.get("notes") or "").strip()

        condition = standardize_condition(cond_raw, damage_raw, rating)
        completeness = standardize_completeness(missing_raw)
        functional_status = standardize_functionality(func_raw)

        # Warehouse location
        loc = item.get("location") if isinstance(item.get("location"), dict) else {}
        location_name = item.get("location_name") or loc.get("name") or "Phoenix"

        # Unique bidders estimation from bid count
        bid_count = int(item.get("bidCount") or item.get("bid_total") or 0)
        # Unique bidders: estimated as min(bid_count, max(1, int(bid_count * 0.65)))
        unique_bidders = item.get("unique_bidders") or (min(bid_count, max(1, int(bid_count * 0.65))) if bid_count > 0 else 0)

        # 1. Fact Table Record
        fact_record = {
            "auction_id": auction_id,
            "product_id": product_id,
            "location": location_name,
            "condition": condition,
            "completeness": completeness,
            "functional_status": functional_status,
            "damage_notes": notes,
            "retail_price": retail_price,
            "final_price": final_price,
            "discount_ratio": discount_ratio,
            "bid_count": bid_count,
            "unique_bidders": unique_bidders,
            "closed_at": closed_at_iso,
            "close_day_of_week": close_day_of_week,
            "close_hour": close_hour,
            "scraped_date": scrape_date,
            "photo_url": item.get("photo_url") or ""
        }
        fact_auctions.append(fact_record)

        # 2. Dim Table Record (Deduplicated)
        if product_id not in dim_products_map:
            dim_products_map[product_id] = {
                "product_id": product_id,
                "title": title,
                "brand": item.get("brand") or "",
                "category_primary": item.get("category") or item.get("taxonomyLevel1") or "",
                "category_secondary": item.get("subcategory") or item.get("taxonomyLevel2") or "",
                "base_retail_price": retail_price,
                "description": item.get("catalog_description") or item.get("description") or "",
                "first_seen_date": scrape_date,
                "photo_url": item.get("photo_url") or ""
            }

    return list(dim_products_map.values()), fact_auctions

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


INCOMPLETE_DAMAGED_REGEX = re.compile(
    r"(?:"
    r"missing\s+(?:\d+\s+)?(?:box|boxes|piece|pieces|part|parts|hardware|screws|bolts|legs|cushion|cushions|shelf|shelves|door|doors|panel|panels|rail|rails|cable|cables|cord|remote|battery|batteries|charger|lid|cover|tray|drawer|wheel|wheels|item|items|set)"
    r"|(?:box|boxes|part|parts|piece|pieces|bag|bags|crate|crates|parcel|parcels|carton|cartons)\s*#?\d+\s*(?:of|or|/)\s*\d+(?:\s*only)?"
    r"|\bonly\s+\d+\s*(?:of|/)\s*\d+\b"
    r"|\b\d+\s*(?:of|/)\s*\d+\s+boxes?\s+only\b"
    r"|\bonly\s+(?:box|boxes|part|parts|piece|pieces)\s*#?\d+\b"
    r"|\b(?:box|boxes|case|manual|remote|frame|stand|base|lid|cover|cushion|cushions|door|shelf|drawer|bag|bags|crate|crates|parcel|parcels|carton|cartons|panel|panels|hardware|parts?|pieces?)\s*(?:#?\d+\s+)?only\b"
    r"|\bfor\s+parts\b"
    r"|\bpartial\s+set\b"
    r"|\bincomplete(?:\s+set)?\b"
    r"|\bnot\s+complete\b"
    r"|\bno\s+(?:charger|battery|batteries|remote|cord|cable|power\s+supply|adapter|hardware|legs|cushions|mattress|stand|lid)\b"
    r"|\b(?:major\s+damage|heavy\s+damage|damaged\s+(?:screen|box|frame|leg|corner|glass|panel|item|top|side)|cracked|shattered|broken|defective|malfunction|as[\s-]is|salvage|scrap)\b"
    r"|\b(?:does\s+not\s+(?:work|turn\s+on|power\s+on)|won'?t\s+turn\s+on|needs\s+repair)\b"
    r")",
    re.IGNORECASE,
)


def is_incomplete_or_damaged(title: str = "", notes: str = "", damage_type: str = "", missing_parts: str = "", func_type: str = "", rating=None) -> bool:
    """Detect whether an item is incomplete, missing boxes/pieces, parts-only, or damaged."""
    d = str(damage_type or "").strip().lower()
    mp = str(missing_parts or "").strip().lower()
    f = str(func_type or "").strip().lower()

    if any(k in d for k in ["major", "moderate", "minor", "damage", "broken", "cracked", "yes"]):
        return True
    if any(k in mp for k in ["yes", "partial", "missing"]):
        return True
    if f in ["no", "not functional", "broken", "does not work"]:
        return True
    try:
        if rating is not None and int(rating) <= 2:
            return True
    except (ValueError, TypeError):
        pass

    combined_text = f"{title or ''} {notes or ''}"
    return bool(INCOMPLETE_DAMAGED_REGEX.search(combined_text))


def standardize_condition(cond_type: str, damage_type: str, rating: int, missing_parts: str = "", func_type: str = "", title: str = "", notes: str = "") -> str:
    """Standardize condition string into canonical category, excluding incomplete/damaged items from Brand New."""
    if is_incomplete_or_damaged(title=title, notes=notes, damage_type=damage_type, missing_parts=missing_parts, func_type=func_type, rating=rating):
        return "Incomplete / Damaged / Parts"

    c = (cond_type or "").strip().lower()

    if "new" in c:
        return "Brand New"
    if "open" in c:
        return "Open Box"
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


CATEGORY_RULES = [
    ("Electronics", "Speakers & Audio", ["speaker", "soundbar", "subwoofer", "earbud", "headphone", "airpod", "bluetooth audio", "turntable", "amplifier", "receiver", "microphone", "karaoke", "bose", "sonos", "jbl", "beats", "skullcandy"]),
    ("Electronics", "Monitors & TVs", ["monitor", "television", "smart tv", "oled", "qled", "4k tv", "projector", "tv mount", "tv stand", "roku", "fire tv", "apple tv"]),
    ("Electronics", "Computers & Tablets", ["laptop", "macbook", "chromebook", "desktop", "ipad", "tablet", "keyboard", "mouse", "webcam", "router", "mesh wifi", "ssd", "hard drive", "motherboard", "gpu", "graphics card", "docking station", "usb-c hub", "printer", "scanner"]),
    ("Electronics", "Cameras & Drones", ["camera", "dash cam", "dashcam", "webcam", "gopro", "drone", "gimbal", "tripod", "ring light", "security camera", "doorbell camera", "nvr", "trail camera"]),
    ("Electronics", "Gaming & VR", ["playstation", "ps5", "ps4", "xbox", "nintendo", "switch", "gaming chair", "gaming desk", "controller", "vr headset", "oculus", "meta quest", "headset"]),
    ("Electronics", "Phones & Smartwatches", ["iphone", "samsung galaxy", "pixel", "smartwatch", "apple watch", "garmin", "fitbit", "charger", "power bank", "magsafe", "phone case", "screen protector"]),
    ("Tools & Hardware", "Power Tools", ["drill", "impact driver", "circular saw", "miter saw", "jigsaw", "reciprocating saw", "angle grinder", "sander", "nailer", "nail gun", "rotary tool", "oscillating", "dewalt", "milwaukee", "makita", "ryobi", "bosch", "craftsman"]),
    ("Tools & Hardware", "Hand Tools & Storage", ["wrench", "socket set", "screwdriver", "pliers", "toolbox", "tool chest", "tool bag", "ladder", "step stool", "pressure washer", "air compressor", "generator", "multimeter", "laser level", "stud finder"]),
    ("Home & Kitchen", "Kitchen Appliances", ["air fryer", "espresso", "coffee maker", "keurig", "nespresso", "blender", "vitamix", "ninja", "mixer", "kitchenaid", "toaster", "microwave", "instant pot", "slow cooker", "rice cooker", "food processor", "juicer", "ice maker", "mini fridge", "wine cooler"]),
    ("Home & Kitchen", "Cookware & Dining", ["cookware", "skillet", "frying pan", "dutch oven", "knife set", "cutlery", "flatware", "dinnerware", "bakeware", "thermos", "tumbler", "stanley", "yeti", "water bottle"]),
    ("Home & Kitchen", "Vacuums & Floor Care", ["vacuum", "roomba", "roborock", "dyson", "shark", "bissell", "carpet cleaner", "steam mop", "robot vacuum", "air purifier", "humidifier", "dehumidifier", "space heater", "tower fan", "ceiling fan"]),
    ("Home & Kitchen", "Furniture & Decor", ["sofa", "couch", "recliner", "bookshelf", "nightstand", "dresser", "dining table", "coffee table", "desk", "office chair", "bar stool", "mattress", "bed frame", "rug", "mirror", "lamp", "chandelier", "curtain", "blinds"]),
    ("Home & Kitchen", "Bedding & Bath", ["comforter", "duvet", "sheet set", "pillow", "memory foam", "towel", "shower head", "bidet", "bathroom vanity", "faucet", "bath mat"]),
    ("Patio & Garden", "Outdoor & Grill", ["grill", "bbq", "smoker", "blackstone", "traeger", "patio", "gazebo", "pergola", "fire pit", "lawn mower", "string trimmer", "leaf blower", "chainsaw", "hose", "sprinkler", "planter", "solar light"]),
    ("Sports & Outdoors", "Fitness & Exercise", ["treadmill", "exercise bike", "peloton", "dumbbell", "kettlebell", "barbell", "weight bench", "yoga mat", "massage gun", "theragun", "pull up bar", "rowing machine", "elliptical"]),
    ("Sports & Outdoors", "Camping & Recreation", ["tent", "sleeping bag", "cooler", "kayak", "paddleboard", "bicycle", "e-bike", "ebike", "scooter", "helmet", "backpack", "pickleball", "golf", "fishing"]),
    ("Automotive", "Parts & Accessories", ["car seat cover", "floor mat", "jump starter", "tire inflator", "car stereo", "carplay", "brake pad", "headlight", "led light bar", "tonneau cover", "roof rack", "obd2", "car cover", "dash mat"]),
    ("Toys & Games", "Toys & LEGO", ["lego", "nerf", "barbie", "hot wheels", "board game", "puzzle", "action figure", "plush", "rc car", "remote control", "playset", "dollhouse", "trampoline"]),
    ("Baby & Kids", "Baby Gear", ["stroller", "car seat", "crib", "bassinet", "high chair", "baby monitor", "diaper", "bottle warmer", "playpen", "baby gate"]),
    ("Beauty & Health", "Personal Care", ["hair dryer", "curling iron", "flat iron", "straightener", "electric toothbrush", "waterpik", "shaver", "razor", "trimmer", "skincare", "serum", "perfume", "cologne", "blood pressure", "thermometer"]),
    ("Pet Supplies", "Pet Care", ["dog bed", "dog crate", "cat tree", "litter box", "pet feeder", "dog food", "leash", "harness", "aquarium", "dog toy", "pet gate", "bark collar"]),
    ("Clothing & Shoes", "Apparel & Accessories", ["jacket", "hoodie", "sneaker", "boots", "shoes", "sandals", "jeans", "leggings", "backpack", "handbag", "wallet", "sunglasses", "luggage", "suitcase"]),
    ("Office & School", "Office Supplies", ["shredder", "whiteboard", "label maker", "standing desk", "ergonomic", "calculator", "binder", "filing cabinet", "desk lamp", "toner", "ink cartridge"]),
]


def classify_product_category(title: str, existing_primary: str = "", existing_secondary: str = "") -> Tuple[str, str]:
    """
    Resolve Nellis-style two-level category (primary, secondary).
    Uses existing taxonomy fields if present; otherwise applies deterministic keyword classification ($0 cost).
    """
    p_clean = str(existing_primary or "").strip()
    s_clean = str(existing_secondary or "").strip()
    if p_clean and p_clean.lower() not in ("nan", "none", "uncategorized"):
        return p_clean, (s_clean if s_clean and s_clean.lower() not in ("nan", "none") else "General")

    t_low = f" {str(title or '').lower()} "
    for primary, secondary, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in t_low:
                return primary, secondary

    return "Home & General", "Miscellaneous"


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

        condition = standardize_condition(
            cond_raw, damage_raw, rating,
            missing_parts=missing_raw, func_type=func_raw, title=title, notes=notes
        )
        completeness = standardize_completeness(missing_raw)
        functional_status = standardize_functionality(func_raw)

        # Warehouse location
        loc = item.get("location") if isinstance(item.get("location"), dict) else {}
        location_name = item.get("location_name") or loc.get("name") or "Phoenix"

        # Unique bidders estimation from bid count
        bid_count = int(item.get("bidCount") or item.get("bid_total") or 0)
        # Unique bidders: estimated as min(bid_count, max(1, int(bid_count * 0.65)))
        unique_bidders = item.get("unique_bidders") or (min(bid_count, max(1, int(bid_count * 0.65))) if bid_count > 0 else 0)

        # Photo URL extraction (supports both SQLite photo_url and raw Nellis photos array)
        photos_arr = item.get("photos") if isinstance(item.get("photos"), list) else []
        first_photo = photos_arr[0].get("url", "") if photos_arr and isinstance(photos_arr[0], dict) else ""
        photo_url = item.get("photo_url") or first_photo or ""

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
            "photo_url": photo_url
        }
        fact_auctions.append(fact_record)

        # 2. Dim Table Record (Deduplicated)
        if product_id not in dim_products_map:
            raw_cat_primary = item.get("category") or item.get("taxonomyLevel1") or ""
            raw_cat_secondary = item.get("subcategory") or item.get("taxonomyLevel2") or ""
            cat_primary, cat_secondary = classify_product_category(title, raw_cat_primary, raw_cat_secondary)
            dim_products_map[product_id] = {
                "product_id": product_id,
                "title": title,
                "brand": item.get("brand") or "",
                "category_primary": cat_primary,
                "category_secondary": cat_secondary,
                "base_retail_price": retail_price,
                "description": item.get("catalog_description") or item.get("description") or "",
                "first_seen_date": scrape_date,
                "photo_url": photo_url
            }

    return list(dim_products_map.values()), fact_auctions

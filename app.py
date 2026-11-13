#!/usr/bin/env python3
"""
Nellis Auction Data Lake & Deal Finder App
Cloud-ready Streamlit application queryable from phone, tablet, or desktop.
"""

import ast
import difflib
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
import duckdb
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import importlib
import src.transform as _transform_mod
if not hasattr(_transform_mod, "is_incomplete_or_damaged"):
    importlib.reload(_transform_mod)
from src.transform import CATEGORY_RULES, classify_product_category, is_incomplete_or_damaged

# Page setup
st.set_page_config(
    page_title="Nellis Auction Sniper",
    page_icon="🔨",
    layout="wide",
    initial_sidebar_state="expanded"
)

def get_secret(key, default=""):
    try:
        return st.secrets.get(key, os.environ.get(key, default))
    except Exception:
        return os.environ.get(key, default)


# Cloudflare R2 Secrets (Reads from Streamlit Cloud secrets or Environment Variables)
R2_ACCESS_KEY_ID = get_secret("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = get_secret("R2_SECRET_ACCESS_KEY", "")
R2_ENDPOINT_URL = get_secret("R2_ENDPOINT_URL", "https://965a5411101d1521c551d99d9421a145.r2.cloudflarestorage.com")
R2_BUCKET_NAME = get_secret("R2_BUCKET_NAME", "nellis-auction-lake")

STOP_WORDS = {
    "with", "for", "and", "the", "in", "to", "of", "by", "on", "at", "from",
    "compatible", "pack", "set", "new", "sealed", "factory", "inch", "ft"
}


@st.cache_resource
def get_duckdb_con(_v=2):
    """Create and cache persistent DuckDB connection with Cloudflare R2 httpfs."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    host = R2_ENDPOINT_URL.replace("https://", "").replace("http://", "").strip("/")
    con.execute(f"""
        SET GLOBAL s3_endpoint = '{host}';
        SET GLOBAL s3_access_key_id = '{R2_ACCESS_KEY_ID}';
        SET GLOBAL s3_secret_access_key = '{R2_SECRET_ACCESS_KEY}';
        SET GLOBAL s3_url_style = 'path';
        SET GLOBAL s3_region = 'auto';
        SET GLOBAL s3_use_ssl = true;
    """)
    con.execute(f"CREATE OR REPLACE VIEW fact_auctions AS SELECT * FROM read_parquet('s3://{R2_BUCKET_NAME}/fact_auctions/*/*/*.parquet');")
    con.execute(f"CREATE OR REPLACE VIEW dim_products AS SELECT * FROM read_parquet('s3://{R2_BUCKET_NAME}/dim_products/products_catalog.parquet');")
    return con


con = get_duckdb_con()


def run_duckdb_query(sql: str) -> pd.DataFrame:
    """Execute a DuckDB query on a dedicated cursor so concurrent reruns never collide."""
    cur = con.cursor()
    try:
        return cur.execute(sql).df()
    finally:
        cur.close()


@st.cache_data(ttl=300, show_spinner=False)
def run_cached_duckdb_query(sql: str) -> pd.DataFrame:
    """Cached DuckDB query (5m TTL) so pagination & modal clicks don't re-scan R2."""
    return run_duckdb_query(sql)


# --- PRODUCT MATCHING & NORMALIZATION ENGINE ($0 COST) ---

def extract_amazon_image_id(photo_url: str) -> str:
    """Extract canonical Amazon product image ID from URL (covers ~97.3% of Nellis catalog)."""
    if not photo_url:
        return ""
    m = re.search(r'/images/I/([A-Za-z0-9\+%\-_]+?)(?:\._[^\./]+)?\.(?:jpg|jpeg|png|webp|gif)', str(photo_url), re.I)
    return m.group(1) if m else ""


def clean_title_text(title: str) -> str:
    """Strip warehouse inspector prefixes like **FACTORY SEALED** or [4 in 1]."""
    if not title:
        return ""
    t = str(title)
    t = re.sub(r'^\s*\*+[^*]+\*+\s*', '', t)
    t = re.sub(r'^\s*\[[^\]]+\]\s*', '', t)
    t = re.sub(r'^\s*\([^)]+\)\s*', '', t)
    return t.strip()


def normalize_title_tokens(title: str) -> list:
    """Tokenize and normalize product title for fuzzy signature matching."""
    t = clean_title_text(title).lower()
    t = re.sub(r'[^\w\s]', ' ', t)
    tokens = []
    for w in t.split():
        if w in STOP_WORDS or len(w) <= 1:
            continue
        if len(w) > 4 and w.endswith('s') and not w.endswith('ss'):
            w = w[:-1]
        tokens.append(w)
    return tokens


def get_title_signature(title: str) -> str:
    """Return first 5 normalized core tokens as a fast lookup key."""
    tokens = normalize_title_tokens(title)
    return " ".join(tokens[:5])


def extract_search_keywords(title: str) -> str:
    """Extract clean 5-word search query for live Nellis duplicate lookup."""
    t = clean_title_text(title)
    t = re.sub(r'[^\w\s\-]', ' ', t)
    words = [w for w in t.split() if len(w) > 1]
    return " ".join(words[:6])


def format_clean_condition(raw_cond, title: str = "", notes: str = "") -> str:
    """Convert raw grade dict or string into clean display label, flagging incomplete/damaged items."""
    if isinstance(raw_cond, dict):
        c_type = raw_cond.get('conditionType', {})
        d_type = raw_cond.get('damageType', {})
        m_type = raw_cond.get('missingPartsType', {})
        f_type = raw_cond.get('functionalType', {})
        c_desc = c_type.get('description', '') if isinstance(c_type, dict) else ''
        d_desc = d_type.get('description', '') if isinstance(d_type, dict) else ''
        m_desc = m_type.get('description', '') if isinstance(m_type, dict) else ''
        f_desc = f_type.get('description', '') if isinstance(f_type, dict) else ''
        rat = raw_cond.get('rating')

        if is_incomplete_or_damaged(title=title, notes=notes, damage_type=d_desc, missing_parts=m_desc, func_type=f_desc, rating=rat):
            return f"Incomplete / Damaged / Parts ({rat}/5)" if rat else "Incomplete / Damaged / Parts"
        if c_desc in ['New', 'Brand New'] or rat == 5:
            return "Brand New (5/5)"
        elif c_desc:
            return f"{c_desc} ({rat}/5)" if rat else c_desc
        elif rat:
            return f"Rated {rat}/5"
        return "Standard"

    s = str(raw_cond or "Standard").strip()
    if s.startswith("{"):
        try:
            g = ast.literal_eval(s)
            return format_clean_condition(g, title=title, notes=notes)
        except Exception:
            pass

    if is_incomplete_or_damaged(title=title, notes=notes):
        m_r = re.search(r"(\d)/5", s)
        return f"Incomplete / Damaged / Parts ({m_r.group(1)}/5)" if m_r else "Incomplete / Damaged / Parts"
    return s


def get_condition_tier(cond_str: str, title: str = "", notes: str = "") -> str:
    """Standardize condition into like-for-like comparison tiers."""
    c = format_clean_condition(cond_str, title=title, notes=notes).lower()
    if any(k in c for k in ["incomplete", "damaged", "missing", "broken", "1/5", "2/5", "parts"]):
        return "Incomplete / Damaged / Parts"
    if any(k in c for k in ["used", "fair", "3/5"]):
        return "Used"
    if any(k in c for k in ["like new", "open box", "4/5", "very good"]):
        return "Open Box / Like New"
    return "New"


def is_same_product(title_a, msrp_a, photo_a, title_b, msrp_b, photo_b) -> bool:
    """
    Determine if two lots represent the exact same underlying product.
    Combines Amazon Image ID fingerprint, MSRP guardrail (±28%), and normalized token overlap.
    """
    try:
        ma, mb = float(msrp_a or 0), float(msrp_b or 0)
        if ma > 0 and mb > 0:
            ratio = mb / ma
            if ratio < 0.72 or ratio > 1.38:
                return False
    except Exception:
        pass

    img_a = extract_amazon_image_id(photo_a)
    img_b = extract_amazon_image_id(photo_b)
    if img_a and img_b and img_a == img_b:
        return True

    tok_a = normalize_title_tokens(title_a)[:8]
    tok_b = normalize_title_tokens(title_b)[:8]
    if not tok_a or not tok_b:
        return False
    if tok_a[:5] == tok_b[:5]:
        return True

    set_a, set_b = set(tok_a), set(tok_b)
    overlap = len(set_a & set_b) / float(max(len(set_a), len(set_b)))
    return overlap >= 0.75


# --- CACHED HISTORICAL PRICE INDEX FROM CLOUDFLARE R2 ---

@st.cache_data(ttl=600)
def load_historical_price_index():
    """
    Load closed auctions from Cloudflare R2 Data Lake and build fast O(1) lookup indexes
    by Amazon Image ID and Normalized Title Signature.
    """
    try:
        hist_df = run_duckdb_query("""
            SELECT 
                f.auction_id,
                p.title,
                f.location,
                f.condition,
                f.retail_price,
                f.final_price,
                f.bid_count,
                f.damage_notes,
                f.closed_at,
                f.photo_url
            FROM fact_auctions f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE f.closed_at <= now() AND f.final_price > 0
        """)
    except Exception as e:
        print(f"Warning loading historical index: {e}")
        return {"by_img": {}, "by_sig": {}}

    by_img = {}
    by_sig = {}

    for row in hist_df.to_dict("records"):
        purl = row.get("photo_url") or ""
        title = row.get("title") or ""
        notes = row.get("damage_notes") or ""
        img_id = extract_amazon_image_id(purl)
        sig = get_title_signature(title)
        row["cond_tier"] = get_condition_tier(row.get("condition"), title=title, notes=notes)
        row["clean_cond"] = format_clean_condition(row.get("condition"), title=title, notes=notes)

        if img_id:
            by_img.setdefault(img_id, []).append(row)
        if sig:
            by_sig.setdefault(sig, []).append(row)

    return {"by_img": by_img, "by_sig": by_sig}


def find_historical_matches(item_row: dict, hist_index: dict, same_condition_only: bool = True) -> list:
    """Find all closed historical lots matching the given item."""
    item_id = int(item_row.get("auction_id") or 0)
    title = item_row.get("title") or ""
    msrp = float(item_row.get("retail_price") or 0)
    photo = item_row.get("photo_url") or ""
    target_tier = get_condition_tier(item_row.get("condition"))

    img_id = extract_amazon_image_id(photo)
    sig = get_title_signature(title)

    candidates = []
    if img_id and img_id in hist_index.get("by_img", {}):
        candidates.extend(hist_index["by_img"][img_id])
    if sig and sig in hist_index.get("by_sig", {}):
        candidates.extend(hist_index["by_sig"][sig])

    matches = []
    seen_ids = set()
    for cand in candidates:
        cid = int(cand.get("auction_id") or 0)
        if cid == item_id or cid in seen_ids:
            continue
        if same_condition_only and cand.get("cond_tier") != target_tier:
            continue
        if is_same_product(title, msrp, photo, cand.get("title"), cand.get("retail_price"), cand.get("photo_url")):
            seen_ids.add(cid)
            matches.append(cand)

    # Sort by winning price (lowest to highest)
    matches.sort(key=lambda x: (float(x.get("final_price") or 0), str(x.get("closed_at") or "")))
    return matches


# --- LIVE NELLIS API FETCHERS ---

def unflatten_val(idx, data):
    """Decode Turbo-Stream JSON graph indices."""
    if not isinstance(idx, int) or idx < 0 or idx >= len(data):
        return None
    val = data[idx]
    if isinstance(val, (int, float, str, bool)) or val is None:
        return val
    if isinstance(val, list):
        if len(val) == 2 and val[0] == 'D' and isinstance(val[1], (int, float)):
            return val[1]
        return [unflatten_val(i, data) for i in val]
    if isinstance(val, dict):
        res = {}
        for k, v in val.items():
            key = k
            if k.startswith("_"):
                try:
                    k_idx = int(k[1:])
                    if 0 <= k_idx < len(data):
                        key = str(data[k_idx])
                except ValueError:
                    pass
            res[key] = unflatten_val(v, data)
        return res
    return val


def parse_nellis_products_from_stream(data: list) -> list:
    """Extract normalized product dictionaries from a Nellis search.data response array."""
    now_ms = int(time.time() * 1000)
    items = []
    for i, el in enumerate(data):
        if el == 'products' and i + 1 < len(data):
            if not isinstance(data[i + 1], list):
                continue
            for idx in data[i + 1]:
                p = unflatten_val(idx, data)
                if not p or not isinstance(p, dict) or p.get('isClosed', False):
                    continue

                raw_close = p.get('closeTime')
                if isinstance(raw_close, list) and raw_close:
                    raw_close = raw_close[0]
                try:
                    close_ms = int(raw_close)
                except (ValueError, TypeError):
                    continue

                diff_sec = (close_ms - now_ms) / 1000.0
                if diff_sec <= -30:
                    # Skip already ended lots
                    continue

                loc_dict = p.get('location') if isinstance(p.get('location'), dict) else {}
                loc_name = loc_dict.get('name', 'Phoenix')
                retail = float(p.get('retailPrice') or 0.0)
                bid = float(p.get('currentPrice') or 0.0)
                discount_pct = ((retail - bid) / retail * 100.0) if retail > 0 and bid <= retail else 0.0

                photos = p.get('photos') or []
                photo_url = photos[0].get('url', '') if photos and isinstance(photos[0], dict) else ''
                title_str = p.get('title', 'Unknown Item')
                notes_str = p.get('notes') or ''
                condition_str = format_clean_condition(p.get('grade'), title=title_str, notes=notes_str)
                cat_primary, cat_secondary = classify_product_category(
                    title_str,
                    p.get('taxonomyLevel1') or p.get('category') or "",
                    p.get('taxonomyLevel2') or p.get('subcategory') or "",
                )

                items.append({
                    'auction_id': p.get('id'),
                    'title': title_str,
                    'category_primary': cat_primary,
                    'category_secondary': cat_secondary,
                    'location': loc_name,
                    'condition': condition_str,
                    'retail_price': retail,
                    'final_price': bid,
                    'discount_pct': round(discount_pct, 1),
                    'bid_count': p.get('bidCount', 0),
                    'damage_notes': p.get('notes') or '',
                    'diff_sec': diff_sec,
                    'close_ms': close_ms,
                    'photo_url': photo_url,
                    'closed_at': pd.to_datetime(close_ms, unit='ms', utc=True).tz_convert('America/Phoenix')
                })
            break
    return items


SEARCH_SYNONYMS = {
    "shoes": {"sneaker", "sneakers", "shoe", "shoes", "boot", "boots", "loafer", "loafers", "sandal", "sandals", "heel", "heels", "footwear", "slipper", "slippers"},
    "shoe": {"sneaker", "sneakers", "shoe", "shoes", "boot", "boots", "loafer", "loafers", "sandal", "sandals", "heel", "heels", "footwear", "slipper", "slippers"},
    "sneakers": {"sneaker", "sneakers", "shoe", "shoes", "footwear"},
    "tv": {"tv", "television", "oled", "qled", "smart tv"},
    "couch": {"couch", "sofa", "sectional", "loveseat"},
    "sofa": {"couch", "sofa", "sectional", "loveseat"},
    "fridge": {"fridge", "refrigerator", "freezer", "cooler"},
    "laptop": {"laptop", "notebook", "macbook", "chromebook"},
    "earbuds": {"earbuds", "earbud", "headphone", "headphones", "airpods", "earphones"},
    "headphones": {"headphone", "headphones", "earbuds", "earbud", "headset"},
}


def smart_fuzzy_search_score(query: str, title: str, notes: str = "") -> float:
    """
    Zero-cost typo-tolerant multi-token fuzzy search scorer with retail synonym expansion.
    Returns 1.0 when no query; otherwise returns 0.0..1.0 based on token coverage & edit similarity.
    """
    q = str(query or "").strip().lower()
    if not q:
        return 1.0

    q_tokens = [w for w in re.findall(r"[a-z0-9]+", q) if len(w) >= 2]
    if not q_tokens:
        return 1.0

    haystack_raw = f"{title or ''} {notes or ''}".lower()
    if q in haystack_raw:
        return 1.0

    h_tokens = re.findall(r"[a-z0-9]+", haystack_raw)
    if not h_tokens:
        return 0.0
    h_set = set(h_tokens)

    matched_score = 0.0
    for qt in q_tokens:
        if qt in haystack_raw:
            matched_score += 1.0
            continue

        # Check retail synonym equivalence (e.g., "shoes" -> "sneakers")
        syns = SEARCH_SYNONYMS.get(qt)
        if syns and (syns & h_set):
            matched_score += 0.95
            continue

        # Typo-tolerant similarity check against each word in title/notes
        best_sim = 0.0
        for ht in h_tokens:
            if abs(len(ht) - len(qt)) > max(3, len(qt) // 2):
                continue
            sim = difflib.SequenceMatcher(None, qt, ht).ratio()
            if sim > best_sim:
                best_sim = sim
            if best_sim >= 0.92:
                break

        threshold = 0.72 if len(qt) >= 4 else 0.78
        if best_sim >= threshold:
            matched_score += best_sim

    return matched_score / len(q_tokens)


def build_fuzzy_sql_where(query: str) -> str:
    """
    Build a typo-tolerant DuckDB SQL WHERE clause using 2-3 char prefix/suffix anchors
    so misspelled queries still pull candidate rows before Python fuzzy scoring.
    """
    q_tokens = [w for w in re.findall(r"[a-z0-9]+", str(query or "").lower()) if len(w) >= 2]
    if not q_tokens:
        return ""

    token_clauses = []
    for qt in q_tokens[:4]:
        safe_qt = qt.replace("'", "''")
        if len(qt) <= 3:
            token_clauses.append(f"(p.title ILIKE '%{safe_qt}%' OR f.damage_notes ILIKE '%{safe_qt}%')")
        else:
            p3 = safe_qt[:3]
            s3 = safe_qt[-3:]
            token_clauses.append(
                f"(p.title ILIKE '%{p3}%' OR p.title ILIKE '%{s3}%' OR f.damage_notes ILIKE '%{p3}%')"
            )
    return "(" + " OR ".join(token_clauses) + ")"


@st.cache_data(ttl=15)
def fetch_live_nellis_lots(search_kw: str = "", loc_filter: str = "All", sort_by: str = "⏳ Closing Soonest (Sniper)", page_size: int = 120, max_pages: int = 5):
    """
    Fetch real-time active lots directly from Nellis Auction's API (up to 600 lots across 5 pages).
    Supports live keyword search (with typo fallback), warehouse location filter, and sorting.
    """
    try:
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
        req_loc = urllib.request.Request(
            'https://www.nellisauction.com/change-shopping-location',
            data=b'shoppingLocationId=2',
            headers={'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/x-www-form-urlencoded'}
        )
        opener.open(req_loc, timeout=10)

        nellis_sort = {
            "⏳ Closing Soonest (Sniper)": "time_remaining_asc",
            "Lowest Final Bid ($)": "current_price_asc",
            "Highest Retail MSRP ($)": "retail_price_desc",
            "Highest Discount (%)": "time_remaining_asc"
        }.get(sort_by, "time_remaining_asc")

        def _fetch_query(kw_str: str, pages: int, direct_match: bool = False):
            collected = []
            seen = set()
            for page_num in range(0, pages):
                params = [("sortBy", nellis_sort), ("_p1", f"s:{page_size},n:{page_num}")]
                if kw_str and kw_str.strip():
                    params.append(("query", kw_str.strip()))
                if loc_filter in ["Phoenix", "Mesa"]:
                    params.append(("Location Name", loc_filter))

                qs = urllib.parse.urlencode(params)
                url = f'https://www.nellisauction.com/search.data?{qs}'
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with opener.open(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode())

                page_items = parse_nellis_products_from_stream(data)
                if not page_items:
                    break
                new_added = 0
                for it in page_items:
                    aid = it.get("auction_id")
                    if aid not in seen:
                        seen.add(aid)
                        it["_direct_nellis_match"] = direct_match
                        collected.append(it)
                        new_added += 1
                if new_added == 0 or len(page_items) < page_size:
                    break
            return collected

        items = _fetch_query(search_kw, max_pages, direct_match=bool(search_kw and search_kw.strip()))

        # If search query had a typo or multi-word constraint that returned very few hits from Nellis,
        # also fetch a broader live pool so our typo-tolerant fuzzy matcher can surface relevant items.
        if search_kw and search_kw.strip() and len(items) < 25:
            q_tokens = [w for w in re.findall(r"[a-z0-9]+", search_kw.strip().lower()) if len(w) >= 3]
            seen_ids = {it.get("auction_id") for it in items}
            if len(q_tokens) > 1:
                longest_tok = max(q_tokens, key=len)
                for extra in _fetch_query(longest_tok, 2, direct_match=False):
                    if extra.get("auction_id") not in seen_ids:
                        seen_ids.add(extra.get("auction_id"))
                        items.append(extra)
            if len(items) < 25:
                for extra in _fetch_query("", 3, direct_match=False):
                    if extra.get("auction_id") not in seen_ids:
                        seen_ids.add(extra.get("auction_id"))
                        items.append(extra)

        return pd.DataFrame(items)
    except Exception as e:
        print(f"Error fetching live sniper items: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=15)
def search_live_active_duplicates(title: str, retail_price: float, photo_url: str) -> list:
    """
    Query Nellis Live Search in real-time (~250ms) for all currently active copies
    of a specific item across Phoenix & Mesa, sorted by lowest current bid.
    """
    kw = extract_search_keywords(title)
    if not kw:
        return []
    try:
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
        req_loc = urllib.request.Request(
            'https://www.nellisauction.com/change-shopping-location',
            data=b'shoppingLocationId=2',
            headers={'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/x-www-form-urlencoded'}
        )
        opener.open(req_loc, timeout=8)

        qs = urllib.parse.urlencode({
            "query": kw,
            "sortBy": "current_price_asc",
            "_p1": "s:40,n:0"
        })
        url = f"https://www.nellisauction.com/search.data?{qs}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with opener.open(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())

        candidates = parse_nellis_products_from_stream(data)
        matches = []
        for cand in candidates:
            if cand.get("diff_sec", 0) <= 0:
                continue
            if is_same_product(title, retail_price, photo_url, cand.get("title"), cand.get("retail_price"), cand.get("photo_url")):
                matches.append(cand)

        matches.sort(key=lambda x: (float(x.get("final_price") or 0), float(x.get("diff_sec") or 0)))
        return matches
    except Exception as e:
        print(f"Error searching live duplicates: {e}")
        return []


# --- POPUP MODAL OVERLAY FOR PRICE HISTORY & CHEAPER ACTIVE COPIES ---

@st.dialog("🔍 Price History & Active Copies", width="large")
def show_item_history_dialog(item_row: dict):
    """Compact, mobile-first modal popup displaying active copies and past winning price history."""
    item_id = int(item_row.get("auction_id") or 0)
    title = str(item_row.get("title") or "Unknown Item")
    cur_bid = float(item_row.get("final_price") or 0)
    msrp = float(item_row.get("retail_price") or 0)
    cond_display = format_clean_condition(item_row.get("condition"))
    cond_tier = get_condition_tier(cond_display)
    photo = str(item_row.get("photo_url") or "").strip()
    notes_str = str(item_row.get("damage_notes") or "").strip()
    if notes_str.lower() == "nan":
        notes_str = ""
    item_url = f"https://www.nellisauction.com/p/item/{item_id}"
    safe_title = title.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
    img_src = photo if photo and photo.lower() != "nan" else "https://placehold.co/120x120?text=No+Photo"

    cat_p = item_row.get("category_primary") or ""
    cat_s = item_row.get("category_secondary") or ""
    if not cat_p:
        cat_p, cat_s = classify_product_category(title)

    # Compact side-by-side header (stays horizontal on both mobile and desktop)
    st.markdown(
        f"""
        <div style="display:flex; gap:12px; align-items:center; background:#0f172a; border:1px solid #334155; border-radius:12px; padding:10px 12px; margin-bottom:10px;">
            <div style="flex:0 0 72px; width:72px; height:72px; background:#ffffff; border-radius:8px; display:flex; align-items:center; justify-content:center; padding:4px;">
                <img src="{img_src}" style="max-width:100%; max-height:100%; object-fit:contain;" alt="Item"/>
            </div>
            <div style="flex:1; min-width:0;">
                <div style="font-size:14px; font-weight:700; line-height:1.3; margin-bottom:4px;">
                    <a href="{item_url}" target="_blank" style="color:#f8fafc; text-decoration:none;">{safe_title} ↗</a>
                </div>
                <div style="font-size:12px; color:#cbd5e1; line-height:1.4;">
                    <b>Bid:</b> ${cur_bid:,.2f} &nbsp;·&nbsp; <b>MSRP:</b> ${msrp:,.2f} &nbsp;·&nbsp; <b>{cond_display}</b>
                </div>
                <div style="font-size:11px; color:#94a3b8; margin-top:2px;">
                    🏷️ {cat_p} → {cat_s} &nbsp;·&nbsp; Lot #{item_id}
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if notes_str:
        st.warning(f"⚠️ **Notes:** {notes_str}")

    same_cond_only = st.checkbox(
        f"Compare only '{cond_tier}' condition",
        value=True,
        key=f"dlg_cond_toggle_{item_id}",
    )

    # Instant O(1) lookup from cached indexes (0ms network wait when opening modal)
    open_radar_index = load_open_radar_index()
    cached_active = find_open_radar_matches(item_row, open_radar_index)
    live_key = f"_dlg_live_copies_{item_id}"
    live_copies = st.session_state.get(live_key)
    source_active = live_copies if live_copies is not None else cached_active
    other_active = [
        c for c in source_active
        if int(c.get("auction_id") or 0) != item_id
        and (not same_cond_only or get_condition_tier(c.get("condition")) == cond_tier)
    ]
    hist_index = load_historical_price_index()
    past_matches = find_historical_matches(item_row, hist_index, same_condition_only=same_cond_only)

    # Quick Tab Switcher inside modal for clean mobile reading
    tab_hist, tab_active = st.tabs([
        f"🏆 Past Sales ({len(past_matches)})",
        f"🟢 Active Copies ({len(other_active)})",
    ])

    with tab_hist:
        if not past_matches:
            st.info(f"No past closed lots recorded yet in **{cond_tier if same_cond_only else 'any'}** condition.")
        else:
            prices = [float(m.get("final_price") or 0) for m in past_matches]
            min_won = min(prices)
            avg_won = sum(prices) / len(prices)
            max_won = max(prices)

            if cur_bid <= min_won:
                st.success(f"🟢 **Great Price:** Current ${cur_bid:,.2f} is at/below all-time low (${min_won:,.2f})!")
            elif cur_bid <= avg_won:
                st.info(f"🟢 **Good Price:** Below historical average of **${avg_won:,.2f}**.")
            else:
                st.warning(f"🔴 **Above Avg:** Current ${cur_bid:,.2f} is above historical avg **${avg_won:,.2f}**.")

            st.markdown(
                f"""
                <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(110px, 1fr)); gap:8px; margin-bottom:12px;">
                    <div class="nellis-stat-box"><div class="nellis-stat-label">Cheapest</div><div class="nellis-stat-val">${min_won:,.2f}</div></div>
                    <div class="nellis-stat-box"><div class="nellis-stat-label">Average</div><div class="nellis-stat-val">${avg_won:,.2f}</div></div>
                    <div class="nellis-stat-box"><div class="nellis-stat-label">Highest</div><div class="nellis-stat-val">${max_won:,.2f}</div></div>
                    <div class="nellis-stat-box"><div class="nellis-stat-label">Total Sold</div><div class="nellis-stat-val">{len(past_matches)}</div></div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            rows_html = []
            for rank, past in enumerate(past_matches[:8], start=1):
                p_id = past.get("auction_id")
                p_url = f"https://www.nellisauction.com/p/item/{p_id}"
                p_price = float(past.get("final_price") or 0)
                p_msrp = float(past.get("retail_price") or msrp or 0)
                p_disc = ((p_msrp - p_price) / p_msrp * 100.0) if p_msrp > 0 else 0
                p_date = str(past.get("closed_at") or "")[:10]
                p_loc = past.get("location") or "AZ"
                p_cond = past.get("clean_cond") or format_clean_condition(past.get("condition"))
                p_bids = past.get("bid_count") or 0
                rows_html.append(
                    f"""
                    <div style="border:1px solid #334155; background:#0f172a; border-radius:10px; padding:9px 12px; margin-bottom:8px;">
                        <div style="display:flex; justify-content:space-between; align-items:center; gap:8px;">
                            <div style="font-size:14px; font-weight:800; color:#f8fafc;">
                                #{rank} &nbsp; ${p_price:,.2f} <span style="color:#4ade80; font-size:12px;">({p_disc:.0f}% off)</span>
                            </div>
                            <a href="{p_url}" target="_blank" style="background:#334155; color:#ffffff; text-decoration:none; font-size:12px; font-weight:700; padding:4px 10px; border-radius:6px; white-space:nowrap;">View ↗</a>
                        </div>
                        <div style="font-size:12px; color:#94a3b8; margin-top:4px;">
                            📅 {p_date} &nbsp;·&nbsp; 📍 {p_loc} &nbsp;·&nbsp; {p_cond} &nbsp;·&nbsp; {p_bids} bids
                        </div>
                    </div>
                    """
                )
            st.markdown("".join(rows_html), unsafe_allow_html=True)

    with tab_active:
        if st.button("🔄 Live-Check Nellis Now", key=f"dlg_live_btn_{item_id}"):
            with st.spinner("Querying Nellis live catalog..."):
                st.session_state[live_key] = search_live_active_duplicates(title, msrp, photo)
                st.rerun(scope="fragment")

        if not other_active:
            st.info(f"No other open copies in **{cond_tier if same_cond_only else 'any'}** condition right now.")
        else:
            cheaper_copies = [c for c in other_active if float(c.get("final_price") or 0) < cur_bid]
            if cheaper_copies:
                lowest_active = float(cheaper_copies[0].get("final_price") or 0)
                savings = cur_bid - lowest_active
                st.success(f"💡 **Found {len(cheaper_copies)} cheaper active {'copy' if len(cheaper_copies)==1 else 'copies'}!** Lowest is **${lowest_active:,.2f}** (${savings:,.2f} cheaper).")
            else:
                st.caption(f"Found {len(other_active)} other open {'copy' if len(other_active)==1 else 'copies'} (this lot at ${cur_bid:,.2f} is currently the cheapest):")

            active_html = []
            for idx, copy in enumerate(other_active[:8], start=1):
                c_id = copy.get("auction_id")
                c_url = f"https://www.nellisauction.com/p/item/{c_id}"
                c_price = float(copy.get("final_price") or 0)
                c_msrp = float(copy.get("retail_price") or msrp or 0)
                c_disc = float(copy.get("discount_pct") or (((c_msrp - c_price) / c_msrp * 100.0) if c_msrp > 0 else 0.0))
                c_loc = copy.get("location") or "AZ"
                c_cond = format_clean_condition(copy.get("condition"))
                c_bids = copy.get("bid_count", 0)
                if copy.get("diff_sec") is not None:
                    c_sec = float(copy.get("diff_sec") or 0)
                    c_time_str = f"{max(1, int(c_sec // 60))}m left" if c_sec < 3600 else f"{int(c_sec // 3600)}h {int((c_sec % 3600) // 60)}m left"
                else:
                    c_time_str = f"Closes {str(copy.get('closed_at') or '')[:16]}"
                cheaper_badge = ' <span style="background:#14532d; color:#86efac; font-size:10.5px; padding:2px 6px; border-radius:4px;">CHEAPER</span>' if c_price < cur_bid else ""
                active_html.append(
                    f"""
                    <div style="border:1px solid #334155; background:#0f172a; border-radius:10px; padding:9px 12px; margin-bottom:8px;">
                        <div style="display:flex; justify-content:space-between; align-items:center; gap:8px;">
                            <div style="font-size:14px; font-weight:800; color:#f8fafc;">
                                #{idx} &nbsp; ${c_price:,.2f} <span style="color:#4ade80; font-size:12px;">({c_disc:.0f}% off)</span>{cheaper_badge}
                            </div>
                            <a href="{c_url}" target="_blank" style="background:#b91c1c; color:#ffffff; text-decoration:none; font-size:12px; font-weight:700; padding:4px 10px; border-radius:6px; white-space:nowrap;">Bid ↗</a>
                        </div>
                        <div style="font-size:12px; color:#94a3b8; margin-top:4px;">
                            📍 {c_loc} &nbsp;·&nbsp; ⏳ {c_time_str} &nbsp;·&nbsp; {c_cond} &nbsp;·&nbsp; {c_bids} bids
                        </div>
                    </div>
                    """
                )
            st.markdown("".join(active_html), unsafe_allow_html=True)


def find_open_radar_matches(item_row: dict, open_index: dict) -> list:
    """Find currently open active lots matching a past closed deal ('Back Tonight' Radar)."""
    title = item_row.get("title") or ""
    msrp = float(item_row.get("retail_price") or 0)
    photo = item_row.get("photo_url") or ""
    img_id = extract_amazon_image_id(photo)
    sig = get_title_signature(title)

    candidates = []
    if img_id and img_id in open_index.get("by_img", {}):
        candidates.extend(open_index["by_img"][img_id])
    if sig and sig in open_index.get("by_sig", {}):
        candidates.extend(open_index["by_sig"][sig])

    matches = []
    seen_ids = set()
    for cand in candidates:
        cid = int(cand.get("auction_id") or 0)
        if cid in seen_ids:
            continue
        if is_same_product(title, msrp, photo, cand.get("title"), cand.get("retail_price"), cand.get("photo_url")):
            seen_ids.add(cid)
            matches.append(cand)
    matches.sort(key=lambda x: float(x.get("final_price") or 0))
    return matches


@st.cache_data(ttl=300)
def load_open_radar_index():
    """Build O(1) index of currently open lots from R2 + Live API for 'Back Tonight' Radar."""
    open_rows = []
    try:
        lake_open = run_duckdb_query("""
            SELECT f.auction_id, p.title, f.location, f.condition, f.retail_price,
                   f.final_price, f.bid_count, f.closed_at, f.photo_url
            FROM fact_auctions f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE f.closed_at > now()
            LIMIT 3000;
        """)
        if not lake_open.empty:
            open_rows.extend(lake_open.to_dict("records"))
    except Exception:
        pass

    try:
        live_open = fetch_live_nellis_lots(search_kw="", loc_filter="All", page_size=120, max_pages=3)
        if not live_open.empty:
            seen = {int(r.get("auction_id") or 0) for r in open_rows}
            for r in live_open.to_dict("records"):
                aid = int(r.get("auction_id") or 0)
                if aid not in seen and float(r.get("diff_sec") or 0) > 0:
                    seen.add(aid)
                    open_rows.append(r)
    except Exception:
        pass

    by_img = {}
    by_sig = {}
    for row in open_rows:
        img_id = extract_amazon_image_id(row.get("photo_url") or "")
        sig = get_title_signature(row.get("title") or "")
        if img_id:
            by_img.setdefault(img_id, []).append(row)
        if sig:
            by_sig.setdefault(sig, []).append(row)
    return {"by_img": by_img, "by_sig": by_sig}


def inject_nellis_card_css():
    """Inject shared high-contrast Nellis-style card CSS for 4-column desktop & mobile grid."""
    st.markdown(
        """
        <style>
        .nellis-card-img-wrap {
            width: 100%;
            height: 170px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: #ffffff;
            border-radius: 8px;
            overflow: hidden;
            margin-bottom: 8px;
            padding: 6px;
        }
        .nellis-card-img-wrap img {
            max-height: 158px;
            max-width: 100%;
            object-fit: contain;
        }
        .nellis-cat-pill {
            display: inline-block;
            font-size: 11px;
            font-weight: 600;
            color: #93c5fd;
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 999px;
            padding: 2px 8px;
            margin-bottom: 6px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 100%;
        }
        .nellis-card-title {
            font-size: 14px;
            font-weight: 700;
            line-height: 1.35;
            height: 38px;
            overflow: hidden;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            margin-bottom: 8px;
        }
        .nellis-card-title a {
            text-decoration: none;
        }
        div[data-testid="stColumn"] div[data-testid="stVerticalBlockBorderWrapper"] {
            border: 2px solid #94a3b8 !important;
            border-radius: 14px !important;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.14) !important;
            padding: 12px !important;
            margin-bottom: 18px !important;
        }
        .nellis-stat-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            margin-bottom: 8px;
        }
        .nellis-stat-box {
            background: #f8fafc;
            border: 1px solid #cbd5e1;
            border-radius: 10px;
            padding: 8px 8px;
            text-align: center;
        }
        .nellis-stat-label {
            font-size: 10px;
            font-weight: 800;
            letter-spacing: 0.5px;
            text-transform: uppercase;
            color: #475569;
            margin-bottom: 3px;
        }
        .nellis-stat-val {
            font-size: 15px;
            font-weight: 800;
            color: #0f172a;
            line-height: 1.2;
        }
        .nellis-disc-badge {
            font-size: 11px;
            color: #15803d;
            font-weight: 800;
        }
        .nellis-meta-strip {
            display: grid;
            grid-template-columns: 1fr 0.65fr 1fr;
            gap: 4px;
            background: #f1f5f9;
            border: 1px solid #cbd5e1;
            border-radius: 8px;
            padding: 7px 4px;
            margin-bottom: 8px;
            text-align: center;
        }
        .nellis-meta-item-label {
            font-size: 9.5px;
            font-weight: 800;
            text-transform: uppercase;
            color: #475569;
            margin-bottom: 2px;
        }
        .nellis-meta-item-val {
            font-size: 13px;
            font-weight: 800;
            color: #0f172a;
        }
        .nellis-cond-row {
            text-align: center;
            margin-bottom: 8px;
        }
        .nellis-stars {
            font-size: 14px;
            letter-spacing: 1px;
            margin-bottom: 4px;
        }
        .nellis-tag-pill {
            display: inline-block;
            font-size: 10.5px;
            font-weight: 600;
            padding: 2px 8px;
            border-radius: 999px;
            margin: 2px 2px;
            background: #451a03;
            color: #fde68a;
            border: 1px solid #92400e;
        }
        .nellis-tag-pill.new {
            background: #052e16;
            color: #86efac;
            border-color: #166534;
        }
        .nellis-place-bid-btn {
            display: block;
            width: 100%;
            text-align: center;
            background: #b91c1c;
            color: #ffffff !important;
            font-weight: 800;
            font-size: 13px;
            letter-spacing: 0.4px;
            text-transform: uppercase;
            padding: 8px 10px;
            border-radius: 8px;
            text-decoration: none !important;
            margin-bottom: 6px;
        }
        .nellis-place-bid-btn:hover {
            background: #dc2626;
        }
        .nellis-place-bid-btn.archive {
            background: #334155;
        }
        @media (max-width: 768px) {
            .nellis-card-img-wrap { height: 210px; }
            .nellis-card-img-wrap img { max-height: 196px; }
            .nellis-card-title { height: auto; -webkit-line-clamp: 3; font-size: 15px; }
            .nellis-stat-val { font-size: 17px; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


st.markdown('<div id="nellis-top-anchor"></div>', unsafe_allow_html=True)


# --- SIDEBAR CONTROLS ---
view_mode = st.sidebar.radio("Navigation", ["🎯 Deal Finder", "🏆 Best Past Deals", "📊 Analytics Hub", "💻 SQL Studio"])


# --- TAB 1: DEAL FINDER ---
if view_mode == "🎯 Deal Finder":
    # Build sorted list of primary categories for Left Sidebar Filter
    PRIMARY_CATEGORIES = ["All Categories"] + sorted({p for p, _, _ in CATEGORY_RULES} | {"Home & General"})

    # Left Sidebar Filter Navigation (Directly below Navigation)
    st.sidebar.markdown("---")
    st.sidebar.subheader("🎛️ Filter Listings")
    search_kw = st.sidebar.text_input("🔍 Search Keyword", placeholder="e.g. DeWalt, Dyson, Lego, Speaker...")
    time_filter = st.sidebar.selectbox(
        "⏳ Time Remaining / Status",
        [
            "🔥 Ending in < 5 mins (Live)",
            "⚡ Ending in < 15 mins (Live)",
            "⏳ Ending in < 30 mins (Live)",
            "🕒 Ending in < 1 hour (Live)",
            "📅 Ending Today (< 24 hrs)",
            "All Active Lots",
            "🗄️ Closed / Historical Archive",
        ]
    )
    is_archive_mode = (time_filter == "🗄️ Closed / Historical Archive")

    cat_filter = "All Categories"
    loc_filter = "All"
    cond_filter = st.sidebar.selectbox("✨ Condition", ["All", "Brand New", "Like New", "Open Box", "Used", "Incomplete / Damaged / Parts"])
    sort_options = (
        [
            "🕒 Most Recently Closed",
            "Highest Discount (%)",
            "Lowest Winning Bid ($)",
            "Highest Retail MSRP ($)",
        ]
        if is_archive_mode
        else [
            "⏳ Closing Soonest (Sniper)",
            "🔮 Predicted Best Steal (Lowest Hist Avg)",
            "🔥 Highest Hist Avg % Off",
            "Highest Discount (%)",
            "Lowest Final Bid ($)",
            "Highest Retail MSRP ($)",
        ]
    )
    sort_by = st.sidebar.selectbox("↕️ Sort By", sort_options)

    predict_cheap_only = False
    min_hist_sales = 2
    min_hist_disc = 85
    max_hist_avg = 25
    if not is_archive_mode:
        predict_cheap_only = st.sidebar.checkbox(
            "🔮 Likely Cheap Tonight (Historical Steals)",
            value=False,
            key="predict_cheap_only",
            help="Show active items that previously sold for cheap in the same condition tier.",
        )
        if predict_cheap_only:
            min_hist_sales = st.sidebar.number_input("Min Past Sales", min_value=1, max_value=50, value=2, step=1, key="min_hist_sales")
            min_hist_disc = st.sidebar.number_input("Min Historical Avg % Off", min_value=50, max_value=99, value=85, step=5, key="min_hist_disc")
            max_hist_avg = st.sidebar.number_input("Max Historical Avg Price ($)", min_value=1, max_value=5000, value=25, step=5, key="max_hist_avg")

    with st.sidebar.expander("⚙️ More Filters", expanded=False):
        cat_filter = st.selectbox("🏷️ Category", PRIMARY_CATEGORIES)
        loc_filter = st.selectbox("📍 Warehouse", ["All", "Phoenix", "Mesa"])
        min_retail = st.number_input(
            "Minimum Retail MSRP ($)",
            min_value=0,
            max_value=50000,
            value=0,
            step=10
        )
        max_bid = st.number_input(
            "Maximum Current / Winning Bid ($)",
            min_value=1,
            max_value=50000,
            value=1000,
            step=25
        )
        page_size_opt = st.selectbox("📄 Items Per Page", [40, 60, 100], index=0, key="df_page_size")

    # Active filter summary bar + floating always-accessible Refresh button
    kw_label = f" · 🔍 '{search_kw.strip()}'" if search_kw.strip() else ""
    cat_label = f" · 🏷️ {cat_filter}" if cat_filter != "All Categories" else ""
    pred_label = f" · 🔮 Likely Cheap (≥{min_hist_sales} sales, Avg ≤${max_hist_avg}, ≥{min_hist_disc}% off)" if predict_cheap_only else ""
    st.caption(f"**Active Filter:** {time_filter}{cat_label} · 📍 {loc_filter} · {cond_filter} · MSRP ≥ ${min_retail} · Bid ≤ ${max_bid}{kw_label}{pred_label}")

    st.markdown('<span id="nellis-refresh-anchor"></span>', unsafe_allow_html=True)
    if st.button("🔄 Refresh Live Data", key="refresh_live_btn", type="primary"):
        fetch_live_nellis_lots.clear()
        search_live_active_duplicates.clear()
        st.rerun()

    # Inject clean card styling for uniform 4-column desktop grid + floating Refresh FAB
    st.markdown(
        """
        <style>
        .st-key-refresh_live_btn,
        div[data-testid="stElementContainer"]:has(#nellis-refresh-anchor) + div[data-testid="stElementContainer"] {
            position: fixed !important;
            top: 64px !important;
            right: 18px !important;
            bottom: auto !important;
            left: auto !important;
            z-index: 9999 !important;
            width: auto !important;
        }
        .st-key-refresh_live_btn button,
        div[data-testid="stElementContainer"]:has(#nellis-refresh-anchor) + div[data-testid="stElementContainer"] button {
            border-radius: 999px !important;
            padding: 0.55rem 1.25rem !important;
            font-weight: 700 !important;
            box-shadow: 0 6px 20px rgba(0, 0, 0, 0.45) !important;
        }
        .nellis-card-img-wrap {
            width: 100%;
            height: 170px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: #ffffff;
            border-radius: 8px;
            overflow: hidden;
            margin-bottom: 8px;
            padding: 6px;
        }
        .nellis-card-img-wrap img {
            max-height: 158px;
            max-width: 100%;
            object-fit: contain;
        }
        .nellis-cat-pill {
            display: inline-block;
            font-size: 11px;
            font-weight: 600;
            color: #93c5fd;
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 999px;
            padding: 2px 8px;
            margin-bottom: 6px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 100%;
        }
        .nellis-card-title {
            font-size: 14px;
            font-weight: 700;
            line-height: 1.35;
            height: 38px;
            overflow: hidden;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            margin-bottom: 8px;
        }
        .nellis-card-title a {
            text-decoration: none;
        }
        /* Distinct Card Separation for both Desktop Grid & Stacked Mobile */
        div[data-testid="stColumn"] div[data-testid="stVerticalBlockBorderWrapper"] {
            border: 2px solid #94a3b8 !important;
            border-radius: 14px !important;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.14) !important;
            padding: 12px !important;
            margin-bottom: 18px !important;
        }
        .nellis-stat-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            margin-bottom: 8px;
        }
        .nellis-stat-box {
            background: #f8fafc;
            border: 1px solid #cbd5e1;
            border-radius: 10px;
            padding: 8px 8px;
            text-align: center;
        }
        .nellis-stat-label {
            font-size: 10px;
            font-weight: 800;
            letter-spacing: 0.5px;
            text-transform: uppercase;
            color: #475569;
            margin-bottom: 3px;
        }
        .nellis-stat-val {
            font-size: 15px;
            font-weight: 800;
            color: #0f172a;
            line-height: 1.2;
        }
        .nellis-disc-badge {
            font-size: 11px;
            color: #15803d;
            font-weight: 800;
        }
        .nellis-meta-strip {
            display: grid;
            grid-template-columns: 1fr 0.65fr 1fr;
            gap: 4px;
            background: #f1f5f9;
            border: 1px solid #cbd5e1;
            border-radius: 8px;
            padding: 7px 4px;
            margin-bottom: 8px;
            text-align: center;
        }
        .nellis-meta-item-label {
            font-size: 9.5px;
            font-weight: 800;
            text-transform: uppercase;
            color: #475569;
            margin-bottom: 2px;
        }
        .nellis-meta-item-val {
            font-size: 13px;
            font-weight: 800;
            color: #0f172a;
        }
        .nellis-cond-row {
            text-align: center;
            margin-bottom: 8px;
        }
        .nellis-stars {
            font-size: 14px;
            letter-spacing: 1px;
            margin-bottom: 4px;
        }
        .nellis-tag-pill {
            display: inline-block;
            font-size: 10.5px;
            font-weight: 600;
            padding: 2px 8px;
            border-radius: 999px;
            margin: 2px 2px;
            background: #451a03;
            color: #fde68a;
            border: 1px solid #92400e;
        }
        .nellis-tag-pill.new {
            background: #052e16;
            color: #86efac;
            border-color: #166534;
        }
        .nellis-place-bid-btn {
            display: block;
            width: 100%;
            text-align: center;
            background: #b91c1c;
            color: #ffffff !important;
            font-weight: 800;
            font-size: 13px;
            letter-spacing: 0.4px;
            text-transform: uppercase;
            padding: 8px 10px;
            border-radius: 8px;
            text-decoration: none !important;
            margin-bottom: 6px;
        }
        .nellis-place-bid-btn:hover {
            background: #dc2626;
        }
        .nellis-place-bid-btn.archive {
            background: #334155;
        }
        @media (max-width: 768px) {
            .nellis-card-img-wrap {
                height: 210px;
            }
            .nellis-card-img-wrap img {
                max-height: 196px;
            }
            .nellis-card-title {
                height: auto;
                -webkit-line-clamp: 3;
                font-size: 15px;
            }
            .nellis-stat-val {
                font-size: 17px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    def enrich_categories(df_in: pd.DataFrame) -> pd.DataFrame:
        if df_in.empty:
            return df_in
        if "category_primary" not in df_in.columns:
            df_in["category_primary"] = ""
        if "category_secondary" not in df_in.columns:
            df_in["category_secondary"] = ""

        cats = [
            classify_product_category(t, cp, cs)
            for t, cp, cs in zip(
                df_in["title"].fillna(""),
                df_in["category_primary"].fillna(""),
                df_in["category_secondary"].fillna(""),
            )
        ]
        df_in["category_primary"] = [c[0] for c in cats]
        df_in["category_secondary"] = [c[1] for c in cats]
        return df_in

    results_df = pd.DataFrame()
    filtered_all = pd.DataFrame()
    live_df = pd.DataFrame()

    if is_archive_mode:
        # Query Closed / Historical Archive directly from Cloudflare R2 Data Lake
        with st.spinner("Searching historical closed auctions in Data Lake..."):
            where_parts = [
                "f.closed_at <= now()",
                f"f.retail_price >= {min_retail}",
                f"f.final_price <= {max_bid}",
            ]
            if loc_filter != "All":
                where_parts.append(f"f.location = '{loc_filter}'")
            if cond_filter != "All" and cond_filter != "Incomplete / Damaged / Parts":
                where_parts.append(f"f.condition ILIKE '%{cond_filter}%'")
            if search_kw.strip():
                fuzzy_sql = build_fuzzy_sql_where(search_kw)
                if fuzzy_sql:
                    where_parts.append(fuzzy_sql)

            where_sql = " AND ".join(where_parts)
            sort_sql = {
                "🕒 Most Recently Closed": "f.closed_at DESC",
                "Highest Discount (%)": "f.discount_ratio ASC",
                "Lowest Winning Bid ($)": "f.final_price ASC",
                "Highest Retail MSRP ($)": "f.retail_price DESC",
            }.get(sort_by, "f.closed_at DESC")

            archive_query = f"""
                SELECT 
                    f.auction_id,
                    p.title,
                    p.category_primary,
                    p.category_secondary,
                    f.location,
                    f.condition,
                    f.completeness,
                    f.functional_status,
                    f.retail_price,
                    f.final_price,
                    ROUND((1.0 - f.discount_ratio) * 100.0, 1) AS discount_pct,
                    f.bid_count,
                    f.damage_notes,
                    f.closed_at,
                    f.photo_url,
                    CAST(epoch_ms(f.closed_at) AS BIGINT) AS close_ms,
                    -1.0 AS diff_sec
                FROM fact_auctions f
                JOIN dim_products p ON f.product_id = p.product_id
                WHERE {where_sql}
                ORDER BY {sort_sql}
                LIMIT 2000;
            """
            try:
                archive_df = run_cached_duckdb_query(archive_query)
                archive_df = enrich_categories(archive_df)
                if not archive_df.empty:
                    notes_col = archive_df["damage_notes"].fillna("") if "damage_notes" in archive_df.columns else [""] * len(archive_df)
                    comp_col = archive_df["completeness"].fillna("") if "completeness" in archive_df.columns else [""] * len(archive_df)
                    func_col = archive_df["functional_status"].fillna("") if "functional_status" in archive_df.columns else [""] * len(archive_df)
                    archive_df["condition"] = [
                        "Incomplete / Damaged / Parts"
                        if (str(cp).lower() in ("incomplete", "partial") or str(fn).lower() in ("non-functional", "broken"))
                        else format_clean_condition(c, t, n)
                        for c, t, n, cp, fn in zip(archive_df["condition"].fillna(""), archive_df["title"].fillna(""), notes_col, comp_col, func_col)
                    ]
                    if cond_filter != "All":
                        archive_df = archive_df[archive_df["condition"].str.contains(cond_filter, case=False, na=False)]
                if not archive_df.empty and cat_filter != "All Categories":
                    archive_df = archive_df[archive_df["category_primary"] == cat_filter]
                if not archive_df.empty and search_kw.strip():
                    scores = [
                        smart_fuzzy_search_score(search_kw, t, n)
                        for t, n in zip(archive_df["title"].fillna(""), archive_df["damage_notes"].fillna(""))
                    ]
                    archive_df = archive_df.assign(_search_score=scores)
                    archive_df = archive_df[archive_df["_search_score"] >= 0.65]
                filtered_all = archive_df
            except Exception as e:
                st.error(f"Error querying Historical Archive: {e}")
    else:
        # Fetch active lots (Always guarantees only ACTIVE lots are shown, never lots that ended last night)
        with st.spinner("Fetching active lots..."):
            live_df = fetch_live_nellis_lots(
                search_kw=search_kw,
                loc_filter=loc_filter,
                sort_by=sort_by,
                page_size=120,
                max_pages=5
            )

            # If "All Active Lots" or "Ending Today", also union any active (closed_at > now()) lots in the Data Lake
            if time_filter in ["All Active Lots", "📅 Ending Today (< 24 hrs)"]:
                where_parts = [
                    "f.closed_at > now()",
                    f"f.retail_price >= {min_retail}",
                    f"f.final_price <= {max_bid}"
                ]
                if time_filter == "📅 Ending Today (< 24 hrs)":
                    where_parts.append("f.closed_at <= now() + INTERVAL 24 HOUR")
                if loc_filter != "All":
                    where_parts.append(f"f.location = '{loc_filter}'")
                if cond_filter != "All" and cond_filter != "Incomplete / Damaged / Parts":
                    where_parts.append(f"f.condition ILIKE '%{cond_filter}%'")
                if search_kw.strip():
                    fuzzy_sql = build_fuzzy_sql_where(search_kw)
                    if fuzzy_sql:
                        where_parts.append(fuzzy_sql)

                where_sql = " AND ".join(where_parts)
                sort_sql = {
                    "⏳ Closing Soonest (Sniper)": "f.closed_at ASC",
                    "Highest Discount (%)": "f.discount_ratio ASC",
                    "Lowest Final Bid ($)": "f.final_price ASC",
                    "Highest Retail MSRP ($)": "f.retail_price DESC"
                }.get(sort_by, "f.closed_at ASC")

                lake_active_query = f"""
                    SELECT 
                        f.auction_id,
                        p.title,
                        p.category_primary,
                        p.category_secondary,
                        f.location,
                        f.condition,
                        f.completeness,
                        f.functional_status,
                        f.retail_price,
                        f.final_price,
                        ROUND((1.0 - f.discount_ratio) * 100.0, 1) AS discount_pct,
                        f.bid_count,
                        f.damage_notes,
                        f.closed_at,
                        f.photo_url,
                        CAST(epoch_ms(f.closed_at) AS BIGINT) AS close_ms
                    FROM fact_auctions f
                    JOIN dim_products p ON f.product_id = p.product_id
                    WHERE {where_sql}
                    ORDER BY {sort_sql}
                    LIMIT 2000;
                """
                try:
                    lake_active_df = run_cached_duckdb_query(lake_active_query)
                    if not lake_active_df.empty:
                        now_ms_val = int(time.time() * 1000)
                        lake_active_df = lake_active_df.copy()
                        lake_active_df["diff_sec"] = (lake_active_df["close_ms"].astype("float64") - now_ms_val) / 1000.0
                        if live_df.empty:
                            live_df = lake_active_df
                        else:
                            live_df = pd.concat([live_df, lake_active_df], ignore_index=True)
                            live_df = live_df.drop_duplicates(subset=["auction_id"], keep="first")
                except Exception:
                    pass

        if not live_df.empty:
            live_df = enrich_categories(live_df)
            notes_col = live_df["damage_notes"].fillna("") if "damage_notes" in live_df.columns else [""] * len(live_df)
            comp_col = live_df["completeness"].fillna("") if "completeness" in live_df.columns else [""] * len(live_df)
            func_col = live_df["functional_status"].fillna("") if "functional_status" in live_df.columns else [""] * len(live_df)
            live_df["condition"] = [
                "Incomplete / Damaged / Parts"
                if (str(cp).lower() in ("incomplete", "partial") or str(fn).lower() in ("non-functional", "broken"))
                else format_clean_condition(c, t, n)
                for c, t, n, cp, fn in zip(live_df["condition"].fillna(""), live_df["title"].fillna(""), notes_col, comp_col, func_col)
            ]
            # Strict filter: Only currently active lots (diff_sec > 0)
            filtered = live_df[
                (live_df['diff_sec'] > 0) &
                (live_df['retail_price'] >= min_retail) &
                (live_df['final_price'] <= max_bid)
            ]
            if cat_filter != "All Categories":
                filtered = filtered[filtered['category_primary'] == cat_filter]
            if loc_filter != "All":
                filtered = filtered[filtered['location'].str.contains(loc_filter, case=False, na=False)]
            if cond_filter != "All":
                filtered = filtered[filtered['condition'].str.contains(cond_filter, case=False, na=False)]
            if search_kw.strip() and not filtered.empty:
                scores = [
                    1.0 if direct else smart_fuzzy_search_score(search_kw, t, n)
                    for direct, t, n in zip(
                        filtered.get('_direct_nellis_match', pd.Series([False] * len(filtered))).fillna(False),
                        filtered['title'].fillna(""),
                        filtered['damage_notes'].fillna("")
                    )
                ]
                filtered = filtered.assign(_search_score=scores)
                filtered = filtered[filtered['_search_score'] >= 0.65]

            max_sec_map = {
                "🔥 Ending in < 5 mins (Live)": 300,
                "⚡ Ending in < 15 mins (Live)": 900,
                "⏳ Ending in < 30 mins (Live)": 1800,
                "🕒 Ending in < 1 hour (Live)": 3600,
                "📅 Ending Today (< 24 hrs)": 86400
            }
            if time_filter in max_sec_map:
                filtered = filtered[filtered['diff_sec'] <= max_sec_map[time_filter]]

            need_hist_scoring = (
                predict_cheap_only
                or sort_by in ("🔮 Predicted Best Steal (Lowest Hist Avg)", "🔥 Highest Hist Avg % Off")
            )
            if need_hist_scoring and not filtered.empty:
                hist_index_for_pred = load_historical_price_index()
                h_counts, h_avgs, h_discs = [], [], []
                for r in filtered.to_dict("records"):
                    ms = find_historical_matches(r, hist_index_for_pred, same_condition_only=True)
                    if ms:
                        avg_p = sum(float(m.get("final_price") or 0) for m in ms) / len(ms)
                        msrp_v = float(r.get("retail_price") or 0)
                        avg_d = ((msrp_v - avg_p) / msrp_v * 100.0) if msrp_v > 0 else 0.0
                        h_counts.append(len(ms))
                        h_avgs.append(avg_p)
                        h_discs.append(avg_d)
                    else:
                        h_counts.append(0)
                        h_avgs.append(999999.0)
                        h_discs.append(-1.0)
                filtered = filtered.assign(
                    _hist_count=h_counts,
                    _hist_avg_price=h_avgs,
                    _hist_avg_disc=h_discs,
                    _has_hist=[c > 0 for c in h_counts],
                )
                if predict_cheap_only:
                    filtered = filtered[
                        (filtered["_hist_count"] >= min_hist_sales)
                        & (filtered["_hist_avg_price"] <= max_hist_avg)
                        & (filtered["_hist_avg_disc"] >= min_hist_disc)
                    ]

            if sort_by == "🔮 Predicted Best Steal (Lowest Hist Avg)" and "_hist_avg_price" in filtered.columns:
                filtered = filtered.sort_values(by=["_has_hist", "_hist_avg_price", "_hist_avg_disc"], ascending=[False, True, False])
            elif sort_by == "🔥 Highest Hist Avg % Off" and "_hist_avg_disc" in filtered.columns:
                filtered = filtered.sort_values(by=["_has_hist", "_hist_avg_disc", "_hist_avg_price"], ascending=[False, False, True])
            elif sort_by == "Highest Discount (%)":
                filtered = filtered.sort_values(by="discount_pct", ascending=False)
            elif sort_by == "Lowest Final Bid ($)":
                filtered = filtered.sort_values(by="final_price", ascending=True)
            elif sort_by == "Highest Retail MSRP ($)":
                filtered = filtered.sort_values(by="retail_price", ascending=False)
            else:
                filtered = filtered.sort_values(by="diff_sec", ascending=True)

            filtered_all = filtered

    # Pagination: configurable (default 40 items per page)
    PAGE_SIZE = page_size_opt
    filter_sig = (
        search_kw.strip(), time_filter, cat_filter, loc_filter, cond_filter,
        min_retail, max_bid, sort_by, predict_cheap_only, min_hist_sales, min_hist_disc, max_hist_avg, PAGE_SIZE,
    )
    if st.session_state.get("last_filter_sig") != filter_sig:
        st.session_state["last_filter_sig"] = filter_sig
        st.session_state["page_num"] = 1

    total_results = len(filtered_all)
    total_pages = max(1, math.ceil(total_results / PAGE_SIZE))
    current_page = min(max(1, int(st.session_state.get("page_num", 1))), total_pages)
    st.session_state["page_num"] = current_page

    start_idx = (current_page - 1) * PAGE_SIZE
    end_idx = min(start_idx + PAGE_SIZE, total_results)
    results_df = filtered_all.iloc[start_idx:end_idx] if total_results > 0 else pd.DataFrame()

    # Load cached historical index and open radar index for fast O(1) badges
    hist_index = load_historical_price_index()
    open_radar_index = load_open_radar_index()
    active_batch_records = live_df.to_dict("records") if not live_df.empty else []

    def render_pagination_bar(key_prefix: str):
        if total_results == 0:
            return
        p_col1, p_col2, p_col3 = st.columns([1.5, 3, 1.5])
        with p_col1:
            if st.button("⬅️ Previous Page", key=f"{key_prefix}_prev", disabled=(current_page <= 1), use_container_width=True):
                st.session_state["page_num"] = current_page - 1
                st.session_state["_scroll_to_top"] = True
                st.rerun()
        with p_col2:
            st.markdown(
                f"<div style='text-align:center; padding-top:6px; font-weight:600;'>"
                f"Page {current_page} of {total_pages} &nbsp;·&nbsp; Showing {start_idx + 1}–{end_idx} of {total_results:,} lots"
                f"</div>",
                unsafe_allow_html=True,
            )
        with p_col3:
            if st.button("Next Page ➡️", key=f"{key_prefix}_next", disabled=(current_page >= total_pages), use_container_width=True):
                st.session_state["page_num"] = current_page + 1
                st.session_state["_scroll_to_top"] = True
                st.rerun()

    mode_noun = "closed historical lots" if is_archive_mode else "active lots"
    if total_results > 0:
        st.write(f"**Found {total_results:,} {mode_noun}** (showing {start_idx + 1}–{end_idx}):")
        if total_pages > 1:
            render_pagination_bar("top")
    else:
        st.write(f"**Showing 0 {mode_noun}:**")

    if results_df.empty:
        st.info("No items match your filter criteria right now. Adjust the filters in the left sidebar to widen your search.")
    else:
        records = results_df.to_dict("records")
        for row_start in range(0, len(records), 4):
            grid_cols = st.columns(4)
            for col_idx, row_dict in enumerate(records[row_start:row_start + 4]):
                auction_id = int(row_dict['auction_id'])
                item_url = f"https://www.nellisauction.com/p/item/{auction_id}"
                cur_bid = float(row_dict.get('final_price') or 0)
                msrp = float(row_dict.get('retail_price') or 0)
                cond_display = format_clean_condition(row_dict.get('condition'))
                cond_tier = get_condition_tier(cond_display)
                cat_primary = row_dict.get('category_primary') or 'Home & General'
                cat_secondary = row_dict.get('category_secondary') or 'Miscellaneous'

                # Calculate initial time badge
                if is_archive_mode:
                    time_badge = f"🏁 Closed: {str(row_dict.get('closed_at'))[:16]}"
                else:
                    try:
                        diff_sec = float(row_dict.get('diff_sec') or 0)
                        if diff_sec <= 0:
                            time_badge = "🔴 **POTENTIALLY CLOSED**"
                        elif diff_sec < 180:
                            mins = int(diff_sec // 60)
                            secs = int(diff_sec % 60)
                            time_badge = f"🔥 **{mins}m {secs:02d}s left (SNIPE!)**"
                        elif diff_sec < 600:
                            mins = int(diff_sec // 60)
                            secs = int(diff_sec % 60)
                            time_badge = f"⚡ **{mins}m {secs:02d}s left**"
                        elif diff_sec < 3600:
                            mins = int(diff_sec // 60)
                            time_badge = f"⏳ **{mins}m left**"
                        else:
                            hours = int(diff_sec // 3600)
                            mins = int((diff_sec % 3600) // 60)
                            time_badge = f"🕒 **{hours}h {mins}m left**"
                    except Exception:
                        time_badge = f"Closes: {str(row_dict.get('closed_at'))[:16]}"

                # 1. Lookup Historical Closed Matches (Like-for-Like Condition) & Active Copies (O(1))
                past_matches = find_historical_matches(row_dict, hist_index, same_condition_only=True)
                open_matches = [
                    c for c in find_open_radar_matches(row_dict, open_radar_index)
                    if int(c.get("auction_id") or 0) != auction_id
                ]

                # 2. Check for cheaper active copies in O(1) open_matches (when not in archive mode)
                cheaper_in_batch = []
                if not is_archive_mode and open_matches:
                    cheaper_in_batch = [
                        c for c in open_matches
                        if float(c.get("final_price") or 0) < cur_bid
                        and get_condition_tier(c.get("condition")) == cond_tier
                    ]

                with grid_cols[col_idx]:
                    with st.container(border=True):
                        # 1. Uniform Product Image
                        photo = row_dict.get("photo_url")
                        img_src = str(photo).strip() if photo and str(photo) != "nan" and str(photo).strip() else "https://placehold.co/300x200?text=No+Photo"
                        st.markdown(
                            f'<a href="{item_url}" target="_blank"><div class="nellis-card-img-wrap"><img src="{img_src}" alt="Item"/></div></a>',
                            unsafe_allow_html=True,
                        )

                        # 2. Category Breadcrumb Pill
                        st.markdown(
                            f'<div class="nellis-cat-pill" title="{cat_primary} → {cat_secondary}">🏷️ {cat_primary} → {cat_secondary}</div>',
                            unsafe_allow_html=True,
                        )

                        # 3. Title
                        safe_title = str(row_dict.get('title') or 'Untitled').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                        st.markdown(
                            f'<div class="nellis-card-title" title="{safe_title}"><a href="{item_url}" target="_blank">{safe_title}</a></div>',
                            unsafe_allow_html=True,
                        )

                        # 4. 2-Column Nellis-style Stat Boxes: TIME LEFT | CURRENT PRICE
                        bid_label = "WINNING PRICE" if is_archive_mode else "CURRENT PRICE"
                        disc_pct_val = float(row_dict.get('discount_pct') or 0)
                        if is_archive_mode:
                            time_cell_html = f'<div style="font-size:12px; font-weight:700; color:#cbd5e1;">🏁 {str(row_dict.get("closed_at"))[:16]}</div>'
                        else:
                            close_epoch_ms = int(row_dict.get('close_ms') or 0)
                            time_cell_html = (
                                f'<span class="nellis-countdown-badge" data-close-ms="{close_epoch_ms}" '
                                f'style="display:inline-block; font-weight:700; padding:2px 6px; border-radius:6px; font-size:12px; '
                                f'background-color:#450a0a; color:#fca5a5; border:1px solid #ef4444;">{time_badge}</span>'
                            )

                        st.markdown(
                            f"""
                            <div class="nellis-stat-grid">
                                <div class="nellis-stat-box">
                                    <div class="nellis-stat-label">{"CLOSED" if is_archive_mode else "TIME LEFT"}</div>
                                    <div class="nellis-stat-val">{time_cell_html}</div>
                                </div>
                                <div class="nellis-stat-box">
                                    <div class="nellis-stat-label">{bid_label}</div>
                                    <div class="nellis-stat-val">${cur_bid:,.2f} <span class="nellis-disc-badge">({disc_pct_val:.0f}% off)</span></div>
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                        # 5. 3-Column Metadata Strip: EST. RETAIL | BIDS | LOCATION
                        st.markdown(
                            f"""
                            <div class="nellis-meta-strip">
                                <div>
                                    <div class="nellis-meta-item-label">EST. RETAIL</div>
                                    <div class="nellis-meta-item-val">${msrp:,.2f}</div>
                                </div>
                                <div>
                                    <div class="nellis-meta-item-label">BIDS</div>
                                    <div class="nellis-meta-item-val">{int(row_dict.get('bid_count') or 0)}</div>
                                </div>
                                <div>
                                    <div class="nellis-meta-item-label">LOCATION</div>
                                    <div class="nellis-meta-item-val">📍 {row_dict['location']}</div>
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                        # 6. Star Rating + Condition Pill Tags
                        m_rating = re.search(r"(\d)/5", cond_display)
                        if m_rating:
                            stars_n = int(m_rating.group(1))
                        elif cond_tier == "New":
                            stars_n = 5
                        elif cond_tier == "Open Box / Like New":
                            stars_n = 4
                        elif cond_tier == "Used":
                            stars_n = 3
                        else:
                            stars_n = 2
                        stars_html = f'<span style="color:#facc15;">{"★" * stars_n}</span><span style="color:#475569;">{"☆" * (5 - stars_n)}</span>'

                        tag_cls = "nellis-tag-pill new" if cond_tier == "New" else "nellis-tag-pill"
                        tags = [cond_display.split("(")[0].strip().lower()]
                        notes = row_dict.get('damage_notes')
                        notes_str = str(notes).strip() if notes and str(notes) != "nan" else ""
                        if "missing" in notes_str.lower():
                            tags.append("missing parts")
                        if "damage" in notes_str.lower() or "broken" in notes_str.lower():
                            tags.append("damaged")
                        tags_html = "".join(f'<span class="{tag_cls}">{t}</span>' for t in tags[:3])

                        st.markdown(
                            f'<div class="nellis-cond-row"><div class="nellis-stars">{stars_html}</div>{tags_html}</div>',
                            unsafe_allow_html=True,
                        )

                        if notes_str:
                            safe_notes = notes_str.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                            st.markdown(
                                f'<div style="font-size:11.5px; color:#fbbf24; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; margin-bottom:4px;" title="{safe_notes}">⚠️ {safe_notes}</div>',
                                unsafe_allow_html=True,
                            )

                        # 7. Price History & Cheaper Active Copies Summary
                        if past_matches:
                            p_prices = [float(m.get("final_price") or 0) for m in past_matches]
                            min_won = min(p_prices)
                            avg_won = sum(p_prices) / len(p_prices)
                            hist_avg_disc = ((msrp - avg_won) / msrp * 100.0) if msrp > 0 else 0.0
                            if cur_bid <= min_won:
                                verdict = "🟢 **Below All-Time Low!**"
                            elif cur_bid <= avg_won:
                                verdict = "🟢 **Below Avg Win**"
                            else:
                                verdict = f"🔴 **Above Avg (${avg_won:.0f})**"
                            if not is_archive_mode and len(past_matches) >= min_hist_sales and avg_won <= max_hist_avg and hist_avg_disc >= min_hist_disc:
                                st.markdown(f"🔮 **:green[Likely Cheap Tonight: Avg ${avg_won:.2f} ({hist_avg_disc:.0f}% off · {len(past_matches)} sales)]**")
                            st.caption(f"🏆 **Min Won ({cond_tier}):** ${min_won:.2f} · Avg ${avg_won:.0f} ({len(past_matches)}) · {verdict}")
                        else:
                            st.caption(f"📊 No past '{cond_tier}' sales yet")

                        if cheaper_in_batch:
                            cheapest_open = min(float(c.get("final_price") or 0) for c in cheaper_in_batch)
                            st.markdown(f"💡 **:green[{len(cheaper_in_batch)} Cheaper Open (${cheapest_open:.2f})!]**")

                        # 8. Nellis-style Place Bid Button + History & Active Modal Button
                        btn_cls = "nellis-place-bid-btn archive" if is_archive_mode else "nellis-place-bid-btn"
                        action_label = "VIEW CLOSED LOT ↗" if is_archive_mode else "PLACE BID ↗"
                        st.markdown(
                            f'<a class="{btn_cls}" href="{item_url}" target="_blank">{action_label}</a>',
                            unsafe_allow_html=True,
                        )
                        btn_label = f"🔍 History ({len(past_matches)}) & Active ({len(open_matches)})"
                        if st.button(btn_label, key=f"btn_hist_{auction_id}", use_container_width=True):
                            show_item_history_dialog(row_dict)

        if total_pages > 1:
            st.markdown("---")
            render_pagination_bar("bottom")

        # Inject real-time ticking countdown JavaScript (only needed in active mode)
        if not is_archive_mode:
            components.html(
                """
                <script>
                (function() {
                    function tickCountdowns() {
                        try {
                            const parentDoc = window.parent.document;
                            if (!parentDoc) return;
                            const badges = parentDoc.querySelectorAll('.nellis-countdown-badge[data-close-ms]');
                            const now = Date.now();
                            
                            badges.forEach(badge => {
                                const closeMs = parseInt(badge.getAttribute('data-close-ms'));
                                if (!closeMs || isNaN(closeMs)) return;
                                
                                const diff = closeMs - now;
                                const totalSec = Math.floor(diff / 1000);
                                
                                if (totalSec <= 0) {
                                    badge.innerHTML = "🔴 <b>POTENTIALLY CLOSED</b> <span style='font-size:10px; opacity:0.8;'>(anti-snipe)</span>";
                                    badge.style.backgroundColor = "#334155";
                                    badge.style.color = "#cbd5e1";
                                    badge.style.borderColor = "#64748b";
                                } else {
                                    const mins = Math.floor(totalSec / 60);
                                    const secs = totalSec % 60;
                                    const secStr = secs < 10 ? '0' + secs : secs;
                                    
                                    if (totalSec < 180) {
                                        badge.innerHTML = "🔥 <b>" + mins + "m " + secStr + "s left (SNIPE!)</b>";
                                        badge.style.backgroundColor = "#450a0a";
                                        badge.style.color = "#fca5a5";
                                        badge.style.borderColor = "#ef4444";
                                    } else if (totalSec < 600) {
                                        badge.innerHTML = "⚡ <b>" + mins + "m " + secStr + "s left</b>";
                                        badge.style.backgroundColor = "#451a03";
                                        badge.style.color = "#fcd34d";
                                        badge.style.borderColor = "#f59e0b";
                                    } else if (totalSec < 3600) {
                                        badge.innerHTML = "⏳ <b>" + mins + "m left</b>";
                                        badge.style.backgroundColor = "#172554";
                                        badge.style.color = "#93c5fd";
                                        badge.style.borderColor = "#3b82f6";
                                    } else {
                                        const hours = Math.floor(totalSec / 3600);
                                        const remMins = Math.floor((totalSec % 3600) / 60);
                                        badge.innerHTML = "🕒 <b>" + hours + "h " + remMins + "m left</b>";
                                        badge.style.backgroundColor = "#0f172a";
                                        badge.style.color = "#cbd5e1";
                                        badge.style.borderColor = "#334155";
                                    }
                                }
                            });
                        } catch (e) {
                            // ignore sandbox warnings
                        }
                    }
                    
                    tickCountdowns();
                    if (window.parent && !window.parent.__nellisTimer) {
                        window.parent.__nellisTimer = setInterval(tickCountdowns, 1000);
                    } else if (!window.__nellisTimer) {
                        window.__nellisTimer = setInterval(tickCountdowns, 1000);
                    }
                })();
                </script>
                """,
                height=0,
                width=0
            )


# --- TAB 1B: BEST PAST DEALS (DAYTIME SCOUTING) ---
elif view_mode == "🏆 Best Past Deals":
    import datetime as _dt

    PRIMARY_CATEGORIES = ["All Categories"] + sorted({p for p, _, _ in CATEGORY_RULES} | {"Home & General"})

    st.sidebar.markdown("---")
    st.sidebar.subheader("🏆 Past Deals Filters")
    pd_kw = st.sidebar.text_input("🔍 Search Keyword", placeholder="e.g. Dyson, Monitor, Air Fryer...", key="pd_kw")
    pd_date_preset = st.sidebar.selectbox(
        "📅 Auction Date Window",
        [
            "Last Night (24h)",
            "Last 3 Days",
            "Last 7 Days",
            "Last 30 Days",
            "All Time",
            "Custom Date Range",
        ],
        index=2,
        key="pd_date_preset",
    )

    today_az = _dt.date.today()
    pd_start_date, pd_end_date = None, None
    if pd_date_preset == "Custom Date Range":
        d_col1, d_col2 = st.sidebar.columns(2)
        with d_col1:
            pd_start_date = st.date_input("From", value=today_az - _dt.timedelta(days=7), key="pd_start")
        with d_col2:
            pd_end_date = st.date_input("To", value=today_az, key="pd_end")

    pd_min_disc = st.sidebar.number_input(
        "🔥 Minimum Discount (% Off)",
        min_value=0,
        max_value=99,
        value=90,
        step=5,
        key="pd_min_disc",
    )
    pd_cond = st.sidebar.selectbox("✨ Condition", ["All", "Brand New", "Like New", "Open Box", "Used", "Incomplete / Damaged / Parts"], index=1, key="pd_cond")
    pd_sort = st.sidebar.selectbox(
        "↕️ Sort By",
        [
            "Highest % Off",
            "Biggest $ Savings",
            "Lowest Winning Bid ($)",
            "Most Recently Closed",
            "Highest Retail MSRP ($)",
        ],
        key="pd_sort",
    )

    with st.sidebar.expander("⚙️ More Filters", expanded=False):
        pd_sleeper_only = st.checkbox(
            "😴 Low-Competition Sleepers (Bids ≤ 3 & MSRP ≥ $100)",
            value=False,
            key="pd_sleeper_only",
        )
        pd_cat = st.selectbox("🏷️ Category", PRIMARY_CATEGORIES, key="pd_cat")
        pd_loc = st.selectbox("📍 Warehouse", ["All", "Phoenix", "Mesa"], key="pd_loc")
        pd_min_retail = st.number_input("Minimum Retail MSRP ($)", min_value=0, max_value=50000, value=0, step=10, key="pd_min_retail")
        pd_max_bid = st.number_input("Maximum Winning Bid ($)", min_value=1, max_value=50000, value=1000, step=25, key="pd_max_bid")
        pd_page_size_opt = st.selectbox("📄 Deals Per Page", [40, 60, 100], index=0, key="pd_page_size")

    inject_nellis_card_css()

    # Build DuckDB query for closed historical deals
    where_parts = [
        "f.closed_at <= now()",
        "f.final_price > 0",
        "f.retail_price > 0",
        f"f.retail_price >= {pd_min_retail}",
        f"f.final_price <= {pd_max_bid}",
        f"ROUND((1.0 - f.discount_ratio) * 100.0, 1) >= {pd_min_disc}",
    ]
    if pd_date_preset == "Last Night (24h)":
        where_parts.append("f.closed_at >= now() - INTERVAL 24 HOUR")
    elif pd_date_preset == "Last 3 Days":
        where_parts.append("f.closed_at >= now() - INTERVAL 3 DAY")
    elif pd_date_preset == "Last 7 Days":
        where_parts.append("f.closed_at >= now() - INTERVAL 7 DAY")
    elif pd_date_preset == "Last 30 Days":
        where_parts.append("f.closed_at >= now() - INTERVAL 30 DAY")
    elif pd_date_preset == "Custom Date Range" and pd_start_date and pd_end_date:
        where_parts.append(f"CAST(f.closed_at AS DATE) >= DATE '{pd_start_date.isoformat()}'")
        where_parts.append(f"CAST(f.closed_at AS DATE) <= DATE '{pd_end_date.isoformat()}'")

    if pd_sleeper_only:
        where_parts.append("f.bid_count <= 3 AND f.retail_price >= 100")
    if pd_loc != "All":
        where_parts.append(f"f.location = '{pd_loc}'")
    if pd_cond != "All" and pd_cond != "Incomplete / Damaged / Parts":
        where_parts.append(f"f.condition ILIKE '%{pd_cond}%'")
    if pd_kw.strip():
        fuzzy_sql = build_fuzzy_sql_where(pd_kw)
        if fuzzy_sql:
            where_parts.append(fuzzy_sql)

    where_sql = " AND ".join(where_parts)
    sort_sql = {
        "Highest % Off": "f.discount_ratio ASC, f.retail_price DESC",
        "Biggest $ Savings": "(f.retail_price - f.final_price) DESC",
        "Lowest Winning Bid ($)": "f.final_price ASC",
        "Most Recently Closed": "f.closed_at DESC",
        "Highest Retail MSRP ($)": "f.retail_price DESC",
    }.get(pd_sort, "f.discount_ratio ASC")

    past_deals_sql = f"""
        SELECT
            f.auction_id,
            p.title,
            p.category_primary,
            p.category_secondary,
            f.location,
            f.condition,
            f.completeness,
            f.functional_status,
            f.retail_price,
            f.final_price,
            ROUND(f.retail_price - f.final_price, 2) AS dollar_savings,
            ROUND((1.0 - f.discount_ratio) * 100.0, 1) AS discount_pct,
            f.bid_count,
            f.damage_notes,
            f.closed_at,
            f.photo_url
        FROM fact_auctions f
        JOIN dim_products p ON f.product_id = p.product_id
        WHERE {where_sql}
        ORDER BY {sort_sql}
        LIMIT 2000;
    """

    pd_df = pd.DataFrame()
    with st.spinner("Scanning past winning deals in Data Lake..."):
        try:
            pd_df = run_cached_duckdb_query(past_deals_sql)
        except Exception as e:
            st.error(f"Error querying Best Past Deals: {e}")

    if not pd_df.empty:
        if "category_primary" not in pd_df.columns:
            pd_df["category_primary"] = ""
        if "category_secondary" not in pd_df.columns:
            pd_df["category_secondary"] = ""
        cats = [
            classify_product_category(t, cp, cs)
            for t, cp, cs in zip(pd_df["title"].fillna(""), pd_df["category_primary"].fillna(""), pd_df["category_secondary"].fillna(""))
        ]
        pd_df["category_primary"] = [c[0] for c in cats]
        pd_df["category_secondary"] = [c[1] for c in cats]

        notes_col = pd_df["damage_notes"].fillna("") if "damage_notes" in pd_df.columns else [""] * len(pd_df)
        comp_col = pd_df["completeness"].fillna("") if "completeness" in pd_df.columns else [""] * len(pd_df)
        func_col = pd_df["functional_status"].fillna("") if "functional_status" in pd_df.columns else [""] * len(pd_df)
        pd_df["condition"] = [
            "Incomplete / Damaged / Parts"
            if (str(cp).lower() in ("incomplete", "partial") or str(fn).lower() in ("non-functional", "broken"))
            else format_clean_condition(c, t, n)
            for c, t, n, cp, fn in zip(pd_df["condition"].fillna(""), pd_df["title"].fillna(""), notes_col, comp_col, func_col)
        ]
        if pd_cond != "All":
            pd_df = pd_df[pd_df["condition"].str.contains(pd_cond, case=False, na=False)]

        if pd_cat != "All Categories":
            pd_df = pd_df[pd_df["category_primary"] == pd_cat]
        if pd_kw.strip() and not pd_df.empty:
            scores = [
                smart_fuzzy_search_score(pd_kw, t, n)
                for t, n in zip(pd_df["title"].fillna(""), pd_df["damage_notes"].fillna(""))
            ]
            pd_df = pd_df.assign(_search_score=scores)
            pd_df = pd_df[pd_df["_search_score"] >= 0.65]

    st.caption(
        f"**Past Deals Filter:** 📅 {pd_date_preset} · 🔥 ≥ {pd_min_disc}% off"
        + (f" · 😴 Sleepers Only" if pd_sleeper_only else "")
        + (f" · 🏷️ {pd_cat}" if pd_cat != "All Categories" else "")
        + (f" · 🔍 '{pd_kw.strip()}'" if pd_kw.strip() else "")
    )

    if pd_df.empty:
        st.info("No past closed deals match your criteria. Try widening the Date Window or lowering Minimum Discount (% Off).")
    else:
        # IDEA 3: "Target Bid" Cheat Sheet Banner
        median_win = float(pd_df["final_price"].median())
        steal_p20 = float(pd_df["final_price"].quantile(0.20))
        median_disc = float(pd_df["discount_pct"].median())
        avg_bids = float(pd_df["bid_count"].mean())
        max_savings = float(pd_df["dollar_savings"].max())

        st.markdown(
            f"""
            <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(145px, 1fr)); gap:8px; margin-bottom:10px;">
                <div class="nellis-stat-box">
                    <div class="nellis-stat-label">Matching Lots</div>
                    <div class="nellis-stat-val">{len(pd_df):,}</div>
                    <div style="font-size:11px; color:#475569; font-weight:600;">Avg {avg_bids:.1f} bids</div>
                </div>
                <div class="nellis-stat-box">
                    <div class="nellis-stat-label">Median Win</div>
                    <div class="nellis-stat-val">${median_win:,.2f}</div>
                    <div style="font-size:11px; color:#15803d; font-weight:700;">{median_disc:.1f}% off</div>
                </div>
                <div class="nellis-stat-box">
                    <div class="nellis-stat-label">🎯 Target Steal (P20)</div>
                    <div class="nellis-stat-val">${steal_p20:,.2f}</div>
                    <div style="font-size:11px; color:#475569; font-weight:600;">Walk-away target</div>
                </div>
                <div class="nellis-stat-box">
                    <div class="nellis-stat-label">💰 Top Savings</div>
                    <div class="nellis-stat-val">${max_savings:,.2f}</div>
                    <div style="font-size:11px; color:#475569; font-weight:600;">Single-lot high</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # IDEA 5: "Condition Arbitrage" Quick Comparison Strip
        pd_df["_cond_tier"] = [get_condition_tier(c) for c in pd_df["condition"]]
        tier_summary = (
            pd_df.groupby("_cond_tier", as_index=False)
            .agg(
                lots=("auction_id", "count"),
                avg_win=("final_price", "mean"),
                med_disc=("discount_pct", "median"),
            )
            .sort_values("avg_win", ascending=True)
        )
        if len(tier_summary) > 1:
            with st.expander("⚖️ Condition Arbitrage Breakdown (Avg Win by Condition)", expanded=False):
                t_cols = st.columns(len(tier_summary))
                for idx_t, (_, trow) in enumerate(tier_summary.iterrows()):
                    t_cols[idx_t].metric(
                        f"{trow['_cond_tier']} ({int(trow['lots'])})",
                        f"${trow['avg_win']:,.2f} avg",
                        f"{trow['med_disc']:.0f}% off",
                    )

        # Load Open Radar Index (Idea 1: "Back Tonight" Radar)
        open_radar_index = load_open_radar_index()
        hist_index = load_historical_price_index()

        # Configurable pagination (default 40 per page)
        PD_PAGE_SIZE = pd_page_size_opt
        pd_sig = (pd_kw.strip(), pd_date_preset, str(pd_start_date), str(pd_end_date), pd_min_disc, pd_sleeper_only, pd_cat, pd_loc, pd_cond, pd_min_retail, pd_max_bid, pd_sort, PD_PAGE_SIZE)
        if st.session_state.get("pd_last_sig") != pd_sig:
            st.session_state["pd_last_sig"] = pd_sig
            st.session_state["pd_page_num"] = 1

        pd_total = len(pd_df)
        pd_total_pages = max(1, math.ceil(pd_total / PD_PAGE_SIZE))
        pd_curr_page = min(max(1, int(st.session_state.get("pd_page_num", 1))), pd_total_pages)
        st.session_state["pd_page_num"] = pd_curr_page
        pd_start_i = (pd_curr_page - 1) * PD_PAGE_SIZE
        pd_end_i = min(pd_start_i + PD_PAGE_SIZE, pd_total)
        pd_page_df = pd_df.iloc[pd_start_i:pd_end_i]

        def render_pd_pagination(key_prefix: str):
            if pd_total_pages <= 1:
                return
            p1, p2, p3 = st.columns([1.5, 3, 1.5])
            with p1:
                if st.button("⬅️ Previous Page", key=f"{key_prefix}_prev", disabled=(pd_curr_page <= 1), use_container_width=True):
                    st.session_state["pd_page_num"] = pd_curr_page - 1
                    st.session_state["_scroll_to_top"] = True
                    st.rerun()
            with p2:
                st.markdown(
                    f"<div style='text-align:center; padding-top:6px; font-weight:600;'>"
                    f"Page {pd_curr_page} of {pd_total_pages} &nbsp;·&nbsp; Showing {pd_start_i + 1}–{pd_end_i} of {pd_total:,} deals"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with p3:
                if st.button("Next Page ➡️", key=f"{key_prefix}_next", disabled=(pd_curr_page >= pd_total_pages), use_container_width=True):
                    st.session_state["pd_page_num"] = pd_curr_page + 1
                    st.session_state["_scroll_to_top"] = True
                    st.rerun()

        render_pd_pagination("pd_top")

        records = pd_page_df.to_dict("records")
        for row_start in range(0, len(records), 4):
            grid_cols = st.columns(4)
            for col_idx, row_dict in enumerate(records[row_start:row_start + 4]):
                auction_id = int(row_dict["auction_id"])
                item_url = f"https://www.nellisauction.com/p/item/{auction_id}"
                win_bid = float(row_dict.get("final_price") or 0)
                msrp = float(row_dict.get("retail_price") or 0)
                disc_pct_val = float(row_dict.get("discount_pct") or 0)
                savings_val = float(row_dict.get("dollar_savings") or max(0.0, msrp - win_bid))
                cond_display = format_clean_condition(row_dict.get("condition"))
                cond_tier = get_condition_tier(cond_display)
                cat_primary = row_dict.get("category_primary") or "Home & General"
                cat_secondary = row_dict.get("category_secondary") or "Miscellaneous"

                # Idea 1: Check if an identical item is open right now!
                open_matches = find_open_radar_matches(row_dict, open_radar_index)
                past_matches = find_historical_matches(row_dict, hist_index, same_condition_only=True)

                with grid_cols[col_idx]:
                    with st.container(border=True):
                        photo = row_dict.get("photo_url")
                        img_src = str(photo).strip() if photo and str(photo) != "nan" and str(photo).strip() else "https://placehold.co/300x200?text=No+Photo"
                        st.markdown(
                            f'<a href="{item_url}" target="_blank"><div class="nellis-card-img-wrap"><img src="{img_src}" alt="Item"/></div></a>',
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            f'<div class="nellis-cat-pill" title="{cat_primary} → {cat_secondary}">🏷️ {cat_primary} → {cat_secondary}</div>',
                            unsafe_allow_html=True,
                        )
                        safe_title = str(row_dict.get("title") or "Untitled").replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                        st.markdown(
                            f'<div class="nellis-card-title" title="{safe_title}"><a href="{item_url}" target="_blank">{safe_title}</a></div>',
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            f"""
                            <div class="nellis-stat-grid">
                                <div class="nellis-stat-box">
                                    <div class="nellis-stat-label">CLOSED DATE</div>
                                    <div class="nellis-stat-val" style="font-size:12.5px;">🏁 {str(row_dict.get("closed_at"))[:16]}</div>
                                </div>
                                <div class="nellis-stat-box">
                                    <div class="nellis-stat-label">WINNING PRICE</div>
                                    <div class="nellis-stat-val">${win_bid:,.2f} <span class="nellis-disc-badge">({disc_pct_val:.0f}% off)</span></div>
                                </div>
                            </div>
                            <div class="nellis-meta-strip">
                                <div><div class="nellis-meta-item-label">EST. RETAIL</div><div class="nellis-meta-item-val">${msrp:,.2f}</div></div>
                                <div><div class="nellis-meta-item-label">BIDS</div><div class="nellis-meta-item-val">{int(row_dict.get('bid_count') or 0)}</div></div>
                                <div><div class="nellis-meta-item-label">SAVED</div><div class="nellis-meta-item-val" style="color:#15803d;">${savings_val:,.0f}</div></div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                        # Star rating + condition tags + location
                        m_rating = re.search(r"(\d)/5", cond_display)
                        stars_n = int(m_rating.group(1)) if m_rating else (5 if cond_tier == "New" else (4 if cond_tier == "Open Box / Like New" else (3 if cond_tier == "Used" else 2)))
                        stars_html = f'<span style="color:#facc15;">{"★" * stars_n}</span><span style="color:#475569;">{"☆" * (5 - stars_n)}</span>'
                        tag_cls = "nellis-tag-pill new" if cond_tier == "New" else "nellis-tag-pill"
                        st.markdown(
                            f'<div class="nellis-cond-row"><div class="nellis-stars">{stars_html}</div>'
                            f'<span class="{tag_cls}">{cond_display.split("(")[0].strip().lower()}</span> '
                            f'<span class="nellis-tag-pill">📍 {row_dict["location"]}</span></div>',
                            unsafe_allow_html=True,
                        )

                        if open_matches:
                            cheapest_now = float(open_matches[0].get("final_price") or 0)
                            open_url = f"https://www.nellisauction.com/p/item/{int(open_matches[0]['auction_id'])}"
                            st.markdown(f"🟢 **:green[Back Open Tonight!]** ([From ${cheapest_now:.2f} ↗]({open_url}))")

                        st.markdown(
                            f'<a class="nellis-place-bid-btn archive" href="{item_url}" target="_blank">VIEW CLOSED LOT ↗</a>',
                            unsafe_allow_html=True,
                        )
                        if st.button(f"🔍 History ({len(past_matches)}) & Active ({len(open_matches)})", key=f"pd_btn_hist_{auction_id}", use_container_width=True):
                            show_item_history_dialog(row_dict)

        render_pd_pagination("pd_bot")


# --- TAB 2: ANALYTICS HUB ---
elif view_mode == "📊 Analytics Hub":
    st.title("📊 Nellis Auction Analytical Models")
    st.caption("Sub-second SQL analytics powered by DuckDB columnar processing over Parquet")

    model_choice = st.selectbox(
        "Choose Analytical Model",
        [
            "1. Condition Risk Arbitrage (Brand New vs. Used vs. Damaged)",
            "2. Category & Brand Value Spreads (Lowest Median Discount Ratio)",
            "3. Temporal Sweet Spots (Arizona Day & Hour Competition Heatmap)",
            "4. Sleeper Lot Hunter (High Retail, Low Bids, Heavy Discount)"
        ]
    )

    if "1. Condition" in model_choice:
        sql = """
            SELECT 
                f.condition,
                f.completeness,
                f.functional_status,
                COUNT(f.auction_id) AS lot_count,
                ROUND(AVG(f.retail_price), 2) AS avg_retail,
                ROUND(AVG(f.final_price), 2) AS avg_winning_bid,
                ROUND(MEDIAN(f.discount_ratio), 4) AS median_discount_ratio,
                ROUND((1.0 - MEDIAN(f.discount_ratio)) * 100.0, 1) AS median_discount_pct,
                ROUND(AVG(f.bid_count), 1) AS avg_bids
            FROM fact_auctions f
            WHERE f.discount_ratio IS NOT NULL
            GROUP BY f.condition, f.completeness, f.functional_status
            ORDER BY lot_count DESC
            LIMIT 25;
        """
        st.subheader("Condition Risk Arbitrage")
        st.caption("Compares discount ratios across functional and cosmetic condition tiers to quantify risk-adjusted bidding.")
        df = run_duckdb_query(sql)
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            st.bar_chart(df.set_index("condition")["median_discount_pct"])

    elif "2. Category" in model_choice:
        sql = """
            SELECT 
                COALESCE(NULLIF(p.category_primary, ''), 'Uncategorized') AS category,
                COALESCE(NULLIF(p.brand, ''), 'Generic') AS brand,
                COUNT(f.auction_id) AS lots_sold,
                ROUND(MEDIAN(f.discount_ratio), 4) AS median_ratio,
                ROUND((1.0 - MEDIAN(f.discount_ratio)) * 100.0, 1) AS median_discount_pct,
                ROUND(AVG(f.retail_price), 2) AS avg_retail,
                ROUND(AVG(f.final_price), 2) AS avg_winning_bid
            FROM fact_auctions f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE f.discount_ratio IS NOT NULL AND f.retail_price >= 25.0
            GROUP BY category, brand
            HAVING COUNT(f.auction_id) >= 5
            ORDER BY median_ratio ASC
            LIMIT 30;
        """
        st.subheader("Category & Brand Liquidation Spreads")
        st.caption("Identifies segments with the highest historical liquidation spreads (lowest winning price vs retail).")
        df = run_duckdb_query(sql)
        st.dataframe(df, use_container_width=True)

    elif "3. Temporal" in model_choice:
        sql = """
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
                ROUND(MEDIAN(f.discount_ratio), 4) AS median_discount_ratio,
                ROUND(AVG(f.bid_count), 1) AS avg_bids_per_item
            FROM fact_auctions f
            WHERE f.close_day_of_week IS NOT NULL AND f.close_hour IS NOT NULL
            GROUP BY f.close_day_of_week, day_name, f.close_hour
            ORDER BY f.close_day_of_week ASC, f.close_hour ASC;
        """
        st.subheader("Temporal Sweet Spots (Arizona MST)")
        st.caption("Tracks bidding competition and discount depth by day of week and closing hour.")
        df = run_duckdb_query(sql)
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            st.line_chart(df.set_index("close_hour")[["avg_bids_per_item"]])

    elif "4. Sleeper" in model_choice:
        sql = """
            SELECT 
                f.auction_id,
                p.title,
                f.location,
                f.condition,
                f.retail_price,
                f.final_price,
                ROUND((1.0 - f.discount_ratio) * 100.0, 1) AS discount_pct,
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
            LIMIT 40;
        """
        st.subheader("Sleeper Lot Hunter")
        st.caption("High-MSRP items ($150+) that closed with <= 10 bids and >= 75% discount.")
        df = run_duckdb_query(sql)
        st.dataframe(df, use_container_width=True)


# --- TAB 3: SQL STUDIO ---
elif view_mode == "💻 SQL Studio":
    st.title("💻 Interactive DuckDB SQL Studio")
    st.caption("Ask questions in plain English (powered by Gemini Free Tier) or run custom SQL directly against `fact_auctions` and `dim_products` on Cloudflare R2.")

    SCHEMA_PROMPT = """Tables in DuckDB:
1. fact_auctions f:
   - auction_id (BIGINT)
   - product_id (VARCHAR, joins to dim_products.product_id)
   - location (VARCHAR: 'Phoenix' or 'Mesa')
   - condition (VARCHAR: 'Brand New', 'Like New', 'Open Box', 'Used', 'Incomplete / Damaged / Parts', 'Untested')
   - completeness (VARCHAR: 'Complete', 'Incomplete', 'Partial')
   - functional_status (VARCHAR: 'Functional', 'Tested - Works', 'Non-Functional')
   - damage_notes (VARCHAR)
   - retail_price (DOUBLE)
   - final_price (DOUBLE)
   - discount_ratio (DOUBLE = final_price / retail_price; discount % off = ROUND((1.0 - f.discount_ratio) * 100, 1))
   - bid_count (INTEGER)
   - unique_bidders (INTEGER)
   - closed_at (TIMESTAMPTZ, Arizona MST)
   - close_day_of_week (TINYINT: 0=Sunday..6=Saturday)
   - close_hour (TINYINT: 0..23)
   - scraped_date (DATE)
   - photo_url (VARCHAR)
2. dim_products p:
   - product_id (VARCHAR)
   - title (VARCHAR)
   - category_primary (VARCHAR)
   - category_secondary (VARCHAR)
   - brand (VARCHAR)"""

    def validate_readonly_sql(sql_text: str) -> str:
        s = re.sub(r"^```(?:sql)?\s*|\s*```$", "", (sql_text or "").strip(), flags=re.IGNORECASE | re.MULTILINE).strip()
        if not re.match(r"^(SELECT|WITH)\b", s, re.IGNORECASE):
            raise ValueError("Generated query must start with SELECT or WITH.")
        if re.search(r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|ATTACH|DETACH|COPY|TRUNCATE|PRAGMA)\b", s, re.IGNORECASE):
            raise ValueError("Only read-only SELECT queries are permitted.")
        return s

    def generate_sql_from_english(question: str, api_key: str) -> str:
        prompt = (
            "You are a DuckDB SQL expert for a Nellis Auction database.\n"
            f"{SCHEMA_PROMPT}\n\n"
            "Rules:\n"
            "- Output ONLY a single valid DuckDB SELECT (or WITH ... SELECT) SQL query. No markdown, no explanations.\n"
            "- Always JOIN dim_products p ON f.product_id = p.product_id when using title, brand, or category.\n"
            "- Closed/past auctions satisfy f.closed_at <= now(). 'Last night' means f.closed_at >= now() - INTERVAL 24 HOUR AND f.closed_at <= now().\n"
            "- 'New' condition means f.condition = 'Brand New'. Use ILIKE '%...%' for keyword matching on p.title.\n"
            "- Include LIMIT 200 on row-level queries (omit LIMIT on single-row aggregates like COUNT/MIN/AVG).\n\n"
            f"User question: {question}"
        )
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={urllib.parse.quote(api_key)}"
        payload = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.0, "maxOutputTokens": 600},
        }).encode("utf-8")
        req = urllib.request.Request(endpoint, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        text = body["candidates"][0]["content"]["parts"][0]["text"]
        return validate_readonly_sql(text)

    default_sql = """SELECT 
    p.title,
    f.condition,
    f.retail_price,
    f.final_price,
    ROUND((1.0 - f.discount_ratio) * 100, 1) AS discount_pct,
    f.bid_count,
    f.location
FROM fact_auctions f
JOIN dim_products p ON f.product_id = p.product_id
WHERE f.condition = 'Brand New'
  AND f.retail_price >= 200
ORDER BY f.discount_ratio ASC
LIMIT 25;"""

    if "sql_editor_text" not in st.session_state:
        st.session_state["sql_editor_text"] = default_sql
    if "ai_sql_question" not in st.session_state:
        st.session_state["ai_sql_question"] = ""

    st.subheader("✨ Ask Anything About Nellis Auctions")
    gemini_key = get_secret("GEMINI_API_KEY", "").strip()
    if not gemini_key:
        st.info("Add `GEMINI_API_KEY` (free from [Google AI Studio](https://aistudio.google.com/)) to Streamlit Secrets, or paste one below for this session.")
        gemini_key = st.text_input("Temporary Gemini API Key", type="password", key="temp_gemini_key").strip()

    ex_cols = st.columns(3)
    examples = [
        ("0-Bid Brand New Last Night", "How many Brand New items from last night ended up with 0 bids?"),
        ("Cheapest New USB-C Drive", "What's the cheapest final price seen for a Brand New usb-c drive?"),
        ("Top Categories by % Off", "Top 10 categories by average discount % on Brand New items over $100 retail"),
    ]
    for idx_ex, (label_ex, ex_text) in enumerate(examples):
        if ex_cols[idx_ex].button(f"💡 {label_ex}", key=f"sql_ex_{idx_ex}", use_container_width=True):
            st.session_state["ai_sql_question"] = ex_text

    ai_question = st.text_input(
        "Your Question",
        key="ai_sql_question",
        placeholder="e.g. How many Brand New items from last night ended up with 0 bids?",
    )

    run_ai_clicked = st.button(
        "✨ Ask AI & Show Answer",
        type="primary",
        disabled=(not gemini_key or not ai_question.strip()),
        use_container_width=True,
    )

    if run_ai_clicked:
        with st.spinner("Translating question to SQL & querying Data Lake..."):
            try:
                generated_sql = generate_sql_from_english(ai_question.strip(), gemini_key)
                st.session_state["sql_editor_text"] = generated_sql
                safe_sql = validate_readonly_sql(generated_sql)
                st.session_state["sql_last_df"] = run_duckdb_query(safe_sql)
                st.session_state["sql_last_err"] = ""
            except Exception as e:
                st.session_state["sql_last_err"] = str(e)

    # Render results immediately below the question box (no scrolling past SQL editor)
    if st.session_state.get("sql_last_err"):
        st.error(f"Query Error: {st.session_state['sql_last_err']}")
    elif isinstance(st.session_state.get("sql_last_df"), pd.DataFrame):
        res_df = st.session_state["sql_last_df"]
        if res_df.empty:
            st.info("Query ran cleanly, but returned 0 matching rows.")
        elif len(res_df) == 1 and len(res_df.columns) <= 4:
            m_cols = st.columns(len(res_df.columns))
            for idx_c, col_name in enumerate(res_df.columns):
                val = res_df.iloc[0][col_name]
                disp_val = f"{val:,.2f}" if isinstance(val, float) else (f"{val:,}" if isinstance(val, int) else str(val))
                m_cols[idx_c].metric(col_name.replace("_", " ").title(), disp_val)
            st.dataframe(res_df, use_container_width=True, hide_index=True)
        else:
            st.success(f"Found {len(res_df):,} rows.")
            display_df = res_df.copy()
            col_cfg = {}
            if "auction_id" in display_df.columns and "item_url" not in display_df.columns:
                display_df.insert(0, "item_url", display_df["auction_id"].apply(lambda x: f"https://www.nellisauction.com/p/item/{int(x)}" if pd.notna(x) else ""))
                col_cfg["item_url"] = st.column_config.LinkColumn("Nellis Link", display_text="View ↗")
            st.dataframe(display_df, use_container_width=True, hide_index=True, column_config=col_cfg)

    with st.expander("🛠️ View / Edit SQL Query", expanded=False):
        custom_sql = st.text_area("SQL Query Editor", key="sql_editor_text", height=160)
        if st.button("▶ Execute Custom SQL"):
            with st.spinner("Executing query on DuckDB..."):
                try:
                    safe_sql = validate_readonly_sql(custom_sql)
                    st.session_state["sql_last_df"] = run_duckdb_query(safe_sql)
                    st.session_state["sql_last_err"] = ""
                    st.rerun()
                except Exception as e:
                    st.session_state["sql_last_err"] = str(e)
                    st.rerun()

    with st.expander("📋 Table & Column Reference", expanded=False):
        st.code(SCHEMA_PROMPT, language="text")


if st.session_state.pop("_scroll_to_top", False):
    scroll_nonce = int(st.session_state.get("_scroll_nonce", 0)) + 1
    st.session_state["_scroll_nonce"] = scroll_nonce
    components.html(
        f"""
        <!-- scroll_nonce: {scroll_nonce} -->
        <script>
        (function() {{
            function scrollTopNow() {{
                try {{
                    const p = window.parent;
                    const doc = p.document;
                    const anchor = doc.getElementById('nellis-top-anchor');
                    if (anchor && anchor.scrollIntoView) {{
                        anchor.scrollIntoView({{ behavior: 'instant', block: 'start' }});
                    }}
                    const selectors = [
                        'section.stMain',
                        'section.main',
                        '[data-testid="stAppViewContainer"]',
                        '[data-testid="stMain"]',
                        '.main'
                    ];
                    selectors.forEach(sel => {{
                        const el = doc.querySelector(sel);
                        if (el) el.scrollTop = 0;
                    }});
                    if (doc.documentElement) doc.documentElement.scrollTop = 0;
                    if (doc.body) doc.body.scrollTop = 0;
                    p.scrollTo(0, 0);
                }} catch (e) {{}}
            }}
            scrollTopNow();
            setTimeout(scrollTopNow, 100);
            setTimeout(scrollTopNow, 300);
            setTimeout(scrollTopNow, 600);
        }})();
        </script>
        """,
        height=0,
    )

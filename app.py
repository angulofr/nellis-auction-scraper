#!/usr/bin/env python3
"""
Nellis Auction Data Lake & Deal Finder App
Cloud-ready Streamlit application queryable from phone, tablet, or desktop.
"""

from __future__ import annotations

import ast
import datetime as _dt
import difflib
import inspect as _inspect
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
from src.settlement import fetch_closed_lots_batch, is_verified_sold_result

# Page setup
st.set_page_config(
    page_title="Nellis Auction Sniper",
    page_icon="🔨",
    layout="wide",
    initial_sidebar_state="auto"
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
NELLIS_API_BASE = os.environ.get("NELLIS_API_BASE", "https://www.nellisauction.com").rstrip("/")

STOP_WORDS = {
    "with", "for", "and", "the", "in", "to", "of", "by", "on", "at", "from",
    "compatible", "pack", "set", "new", "sealed", "factory", "inch", "ft"
}


def _create_empty_fact_auctions_view(con):
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


def _create_empty_fact_bids_view(con):
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


def _create_empty_dim_products_view(con):
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
            CAST(NULL AS VARCHAR) AS photo_url
        WHERE FALSE;
    """)


@st.cache_resource
def get_duckdb_con(_v=5):
    """Create and cache persistent DuckDB connection with Cloudflare R2, local lake, or empty fallback views."""
    con = duckdb.connect()
    use_local = os.environ.get("NELLIS_USE_LOCAL_LAKE") == "1"
    lake_dir = os.environ.get("NELLIS_LAKE_DIR", "data_lake").replace("\\", "/").rstrip("/")
    r2_key = get_secret("R2_ACCESS_KEY_ID", R2_ACCESS_KEY_ID)
    r2_secret = get_secret("R2_SECRET_ACCESS_KEY", R2_SECRET_ACCESS_KEY)
    r2_endpoint = get_secret("R2_ENDPOINT_URL", R2_ENDPOINT_URL)
    r2_bucket = get_secret("R2_BUCKET_NAME", R2_BUCKET_NAME)

    if (not use_local) and r2_key and r2_secret:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            host = r2_endpoint.replace("https://", "").replace("http://", "").strip("/")
            con.execute(f"""
                SET GLOBAL s3_endpoint = '{host}';
                SET GLOBAL s3_access_key_id = '{r2_key}';
                SET GLOBAL s3_secret_access_key = '{r2_secret}';
                SET GLOBAL s3_url_style = 'path';
                SET GLOBAL s3_region = 'auto';
                SET GLOBAL s3_use_ssl = true;
            """)
            facts_glob = f"s3://{r2_bucket}/fact_auctions/*/*/*.parquet"
            bids_glob = f"s3://{r2_bucket}/fact_bids/*/*/*.parquet"
            dim_glob = f"s3://{r2_bucket}/dim_products/products_catalog.parquet"
        except Exception:
            facts_glob = f"{lake_dir}/fact_auctions/*/*/*.parquet"
            bids_glob = f"{lake_dir}/fact_bids/*/*/*.parquet"
            dim_glob = f"{lake_dir}/dim_products/products_catalog.parquet"
    else:
        facts_glob = f"{lake_dir}/fact_auctions/*/*/*.parquet"
        bids_glob = f"{lake_dir}/fact_bids/*/*/*.parquet"
        dim_glob = f"{lake_dir}/dim_products/products_catalog.parquet"

    optional_fact_cols = [
        ("is_closed", "CAST(NULL AS BOOLEAN)"),
        ("market_status", "CAST('unverified' AS VARCHAR)"),
        ("open_at", "CAST(NULL AS TIMESTAMPTZ)"),
        ("initial_closed_at", "CAST(NULL AS TIMESTAMPTZ)"),
        ("was_extended", "CAST(NULL AS BOOLEAN)"),
        ("extension_seconds", "CAST(NULL AS INTEGER)"),
        ("first_bid_price", "CAST(NULL AS DOUBLE)"),
        ("price_at_1h_left", "CAST(NULL AS DOUBLE)"),
        ("price_at_5m_left", "CAST(NULL AS DOUBLE)"),
        ("price_at_1m_left", "CAST(NULL AS DOUBLE)"),
        ("bids_last_5m", "CAST(NULL AS INTEGER)"),
        ("bids_last_60s", "CAST(NULL AS INTEGER)"),
        ("winning_bid_sec_before_close", "CAST(NULL AS DOUBLE)"),
        ("bids_captured", "CAST(FALSE AS BOOLEAN)"),
    ]

    try:
        desc_rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{facts_glob}', union_by_name=true);").fetchall()
        raw_cols = {str(r[0]).lower() for r in desc_rows}
        extra_cols = [f"{expr} AS {col}" for col, expr in optional_fact_cols if col not in raw_cols]
        extra_sql = (", " + ", ".join(extra_cols)) if extra_cols else ""
        bids_order = "COALESCE(bids_captured, FALSE) DESC, " if "bids_captured" in raw_cols else ""
        settled_order = "COALESCE(is_closed, FALSE) DESC, " if "is_closed" in raw_cols else ""
        con.execute(f"""
            CREATE OR REPLACE VIEW fact_auctions AS
            SELECT DISTINCT ON (auction_id) *{extra_sql}
            FROM read_parquet('{facts_glob}', union_by_name=true)
            ORDER BY auction_id, {bids_order}{settled_order}scraped_date DESC, final_price DESC, bid_count DESC;
        """)
    except Exception:
        _create_empty_fact_auctions_view(con)

    try:
        con.execute(f"""
            CREATE OR REPLACE VIEW fact_bids AS
            SELECT DISTINCT ON (auction_id, bid_seq) *
            FROM read_parquet('{bids_glob}', union_by_name=true)
            ORDER BY auction_id, bid_seq, scraped_date DESC;
        """)
    except Exception:
        _create_empty_fact_bids_view(con)

    try:
        con.execute(f"CREATE OR REPLACE VIEW dim_products AS SELECT * FROM read_parquet('{dim_glob}');")
    except Exception:
        _create_empty_dim_products_view(con)

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


@st.cache_data(ttl=300, show_spinner=False)
def has_settled_history_rows() -> bool:
    """True once R2 holds at least 500 settled post-close lots."""
    try:
        cnt = int(run_duckdb_query("SELECT COUNT(*) FROM fact_auctions WHERE is_closed = TRUE;").iloc[0, 0])
        return cnt >= 500
    except Exception:
        return False


def get_settled_sales_where_sql(alias: str = "f") -> str:
    """SQL predicate restricting historical winning prices to actual closed sales."""
    parts = [
        f"{alias}.closed_at <= now()",
        f"{alias}.final_price > 0",
        f"{alias}.bid_count > 0",
        f"COALESCE({alias}.market_status, 'sold') NOT IN ('relisted', 'canceled', 'cancelled', 'void', 'unsold', 'active', 'archived_unverified')",
    ]
    if has_settled_history_rows():
        parts.append(f"{alias}.is_closed = TRUE")
    else:
        parts.append(f"COALESCE({alias}.is_closed, TRUE) = TRUE")
    return " AND ".join(parts)


def get_archive_candidates_where_sql(alias: str = "f") -> str:
    """SQL predicate covering ALL closed historical lots (settled + unverified pre-close snapshots with initial market_status='active')."""
    return " AND ".join([
        f"{alias}.closed_at <= now()",
        f"COALESCE({alias}.market_status, 'sold') NOT IN ('relisted', 'canceled', 'cancelled', 'void', 'unsold', 'archived_unverified')",
        f"(COALESCE({alias}.is_closed, FALSE) = FALSE OR (COALESCE({alias}.market_status, 'sold') <> 'active' AND {alias}.final_price > 0 AND {alias}.bid_count > 0))",
    ])


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
        cand_where = get_archive_candidates_where_sql("f")
        hist_df = run_duckdb_query(f"""
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
                f.photo_url,
                f.is_closed,
                f.market_status
            FROM fact_auctions f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE {cand_where}
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


def parse_nellis_products_with_meta(data: list, include_just_ended_sec: int = -30) -> tuple:
    """
    Extract normalized product dicts from a Nellis search.data response array.
    Returns (items, raw_product_count, max_diff_sec_on_page) so pagination never stops
    early just because closed/filtered lots were skipped on page 0.
    """
    now_ms = int(time.time() * 1000)
    items = []
    raw_count = 0
    max_diff_sec = None

    for i, el in enumerate(data):
        if el == 'products' and i + 1 < len(data):
            if not isinstance(data[i + 1], list):
                continue
            raw_ids = data[i + 1]
            raw_count = len(raw_ids)
            for idx in raw_ids:
                p = unflatten_val(idx, data)
                if not p or not isinstance(p, dict):
                    continue

                raw_close = p.get('closeTime')
                if isinstance(raw_close, list) and raw_close:
                    raw_close = raw_close[0]
                try:
                    close_ms = int(raw_close)
                except (ValueError, TypeError):
                    continue

                diff_sec = (close_ms - now_ms) / 1000.0
                if max_diff_sec is None or diff_sec > max_diff_sec:
                    max_diff_sec = diff_sec

                if p.get('isClosed', False) or diff_sec <= include_just_ended_sec:
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
    return items, raw_count, max_diff_sec


def parse_nellis_products_from_stream(data: list) -> list:
    """Backward-compatible wrapper returning normalized active items from a search.data stream."""
    items, _, _ = parse_nellis_products_with_meta(data)
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
    return "(" + " AND ".join(token_clauses) + ")"


@st.cache_data(ttl=15)
def fetch_live_nellis_lots(
    search_kw: str = "",
    loc_filter: str = "All",
    sort_by: str = "Closing Soonest",
    page_size: int = 240,
    max_pages: int = 10,
    target_window_sec: int = 0,
):
    """
    Fetch real-time active lots directly from Nellis Auction's API.
    When target_window_sec > 0 (e.g., < 15 mins / < 30 mins / < 1 hour), always walks
    chronologically by time_remaining_asc past page-0 closed clutter until the entire
    requested time window is covered, then lets the UI re-sort in Pandas.
    """
    try:
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
        req_loc = urllib.request.Request(
            f'{NELLIS_API_BASE}/change-shopping-location',
            data=b'shoppingLocationId=2',
            headers={'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/x-www-form-urlencoded'}
        )
        opener.open(req_loc, timeout=10)

        # Short live windows MUST walk chronologically so sorting by Discount/Bid/MSRP still sees the full window
        if target_window_sec > 0:
            nellis_sort = "time_remaining_asc"
            eff_max_pages = max(max_pages, 25 if target_window_sec >= 1800 else 18)
        else:
            nellis_sort = {
                "Closing Soonest": "time_remaining_asc",
                "⏳ Closing Soonest (Sniper)": "time_remaining_asc",
                "Lowest Current Bid ($)": "current_price_asc",
                "Lowest Final Bid ($)": "current_price_asc",
                "Highest Retail MSRP ($)": "retail_price_desc",
                "Highest Discount (%)": "time_remaining_asc",
            }.get(sort_by, "time_remaining_asc")
            eff_max_pages = max_pages

        eff_page_size = min(max(int(page_size), 40), 240)

        def _fetch_query(kw_str: str, pages: int, direct_match: bool = False):
            collected = []
            seen = set()
            consecutive_empty_new = 0
            for page_num in range(0, pages):
                params = [("sortBy", nellis_sort), ("_p1", f"s:{eff_page_size},n:{page_num}")]
                if kw_str and kw_str.strip():
                    params.append(("query", kw_str.strip()))
                if loc_filter in ["Phoenix", "Mesa"]:
                    params.append(("Location Name", loc_filter))

                qs = urllib.parse.urlencode(params)
                url = f'{NELLIS_API_BASE}/search.data?{qs}'
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with opener.open(req, timeout=12) as resp:
                    data = json.loads(resp.read().decode())

                page_items, raw_count, max_diff_sec = parse_nellis_products_with_meta(
                    data, include_just_ended_sec=0
                )
                if raw_count == 0:
                    break

                new_added = 0
                for it in page_items:
                    aid = it.get("auction_id")
                    if aid not in seen:
                        seen.add(aid)
                        it["_direct_nellis_match"] = direct_match
                        collected.append(it)
                        new_added += 1

                # Stop early once chronological walk clearly passes target window (+90s safety buffer)
                if (
                    target_window_sec > 0
                    and nellis_sort == "time_remaining_asc"
                    and max_diff_sec is not None
                    and max_diff_sec >= target_window_sec + 90
                ):
                    break

                if raw_count < eff_page_size:
                    break

                if new_added == 0 and (not page_items or len(seen) > 0):
                    # Page 0/1 can contain 100% already-closed items; keep advancing unless repeated stalled pages after items found
                    if len(collected) > 0:
                        consecutive_empty_new += 1
                        if consecutive_empty_new >= 2:
                            break
                else:
                    consecutive_empty_new = 0
            return collected

        items = _fetch_query(search_kw, eff_max_pages, direct_match=bool(search_kw and search_kw.strip()))

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
                for extra in _fetch_query("", 4, direct_match=False):
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
            f'{NELLIS_API_BASE}/change-shopping-location',
            data=b'shoppingLocationId=2',
            headers={'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/x-www-form-urlencoded'}
        )
        opener.open(req_loc, timeout=8)

        qs = urllib.parse.urlencode({
            "query": kw,
            "sortBy": "current_price_asc",
            "_p1": "s:40,n:0"
        })
        url = f"{NELLIS_API_BASE}/search.data?{qs}"
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

@st.cache_data(ttl=1800, show_spinner=False)
def verify_closed_lots_cached(aids_tuple: tuple) -> dict:
    """Cache live post-close Nellis .data verification per batch of past auction IDs."""
    return fetch_closed_lots_batch(list(aids_tuple), workers=24)


def _format_az_date(val) -> str:
    """Format closed timestamp/epoch cleanly as YYYY-MM-DD in Arizona MST."""
    if val is None or str(val) in ("NaT", "nan", ""):
        return ""
    try:
        if isinstance(val, (int, float)) and val > 1e11:
            dt = pd.to_datetime(int(val), unit="ms", utc=True).tz_convert("America/Phoenix")
        else:
            dt = pd.to_datetime(val, utc=True, errors="coerce")
            if pd.notna(dt):
                dt = dt.tz_convert("America/Phoenix")
        if pd.notna(dt):
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    return str(val)[:10]


def verify_past_matches_live(past_matches: list, max_verify: int = 30, preloaded_map: dict = None) -> list:
    """
    Verify any unverified (is_closed != True) historical match directly against Nellis's
    post-close endpoint so modal and card stats show actual WON FOR prices instead of pre-close snapshots.
    """
    if not past_matches:
        return []

    verified_known = [m for m in past_matches if m.get("is_closed") is True]
    unverified_pool = sorted(
        [m for m in past_matches if m.get("is_closed") is not True],
        key=lambda x: (-int(x.get("bid_count") or 0), -float(x.get("final_price") or 0)),
    )[:max_verify]
    candidate_list = verified_known + unverified_pool

    if preloaded_map is not None:
        live_map = preloaded_map
    else:
        to_check = tuple(sorted({int(m["auction_id"]) for m in unverified_pool if int(m.get("auction_id") or 0) > 0}))
        live_map = verify_closed_lots_cached(to_check) if to_check else {}

    cleaned = []
    for m in candidate_list:
        aid = int(m.get("auction_id") or 0)
        row = dict(m)
        if row.get("is_closed") is True:
            mkt = str(row.get("market_status") or "sold").lower()
            if mkt in ("relisted", "canceled", "cancelled", "void", "unsold", "active", "archived_unverified") or int(row.get("bid_count") or 0) <= 0:
                continue
            row["_verified_badge"] = "Verified Won"
            cleaned.append(row)
            continue

        res = live_map.get(aid)
        if res and not res.get("not_found"):
            if not is_verified_sold_result(res):
                # Exclude active, 0-bid, relisted, or canceled lots from Past Sales
                continue
            row["final_price"] = float(res["final_price"])
            row["bid_count"] = int(res["bid_count"])
            if float(res.get("retail_price") or 0) > 0:
                row["retail_price"] = float(res["retail_price"])
            if res.get("close_ms"):
                row["closed_at"] = res["close_ms"]
            row["is_closed"] = True
            row["market_status"] = res.get("market_status") or "sold"
            row["_verified_badge"] = "Live Verified"
            cleaned.append(row)
        # Do not let unverified pre-close snapshots or purged/rate-limited unknowns skew Min Won / Avg prices

    cleaned.sort(key=lambda x: (float(x.get("final_price") or 0), str(x.get("closed_at") or "")))
    return cleaned


def batch_verify_rows_history(
    rows: list,
    hist_index: dict,
    same_condition_only: bool = True,
    max_verify_per_item: int = 12,
    max_total_live_checks: int = 240,
) -> dict:
    """
    Batch-verify unverified historical matches across a page/candidate list in one concurrent sweep
    so card Min Won / Avg / Likely Cheap Tonight stats match the Price History modal identically.
    """
    raw_by_aid = {}
    unverified_ids = []
    seen_unv = set()

    for r in rows:
        aid = int(r.get("auction_id") or 0)
        raw_ms = find_historical_matches(r, hist_index, same_condition_only=same_condition_only)
        raw_by_aid[aid] = raw_ms
        unverified_cand = sorted(
            [m for m in raw_ms if not (m.get("is_closed") is True)],
            key=lambda x: (-int(x.get("bid_count") or 0), -float(x.get("final_price") or 0)),
        )
        for m in unverified_cand[:max_verify_per_item]:
            m_id = int(m.get("auction_id") or 0)
            if m_id > 0 and m_id not in seen_unv:
                seen_unv.add(m_id)
                unverified_ids.append(m_id)

    live_map = {}
    if unverified_ids:
        target_ids = tuple(sorted(unverified_ids[:max_total_live_checks]))
        live_map = verify_closed_lots_cached(target_ids)

    return {
        aid: verify_past_matches_live(ms, max_verify=max_verify_per_item, preloaded_map=live_map)
        for aid, ms in raw_by_aid.items()
    }


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
    raw_past_matches = find_historical_matches(item_row, hist_index, same_condition_only=same_cond_only)
    past_matches = verify_past_matches_live(raw_past_matches)

    # Quick Tab Switcher inside modal for clean mobile reading
    tab_hist, tab_active = st.tabs([
        f"🏆 Past Sales ({len(past_matches)})",
        f"🟢 Active Copies ({len(other_active)})",
    ])

    with tab_hist:
        if not past_matches:
            st.info(f"No verified past closed sales found in **{cond_tier if same_cond_only else 'any'}** condition.")
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
                p_date = _format_az_date(past.get("closed_at"))
                p_loc = past.get("location") or "AZ"
                p_cond = past.get("clean_cond") or format_clean_condition(past.get("condition"))
                p_bids = past.get("bid_count") or 0
                p_badge = past.get("_verified_badge") or "Verified"
                badge_color = "#4ade80" if "Verified" in p_badge and "Unverified" not in p_badge else "#fbbf24"
                rows_html.append(
                    f"""
                    <div style="border:1px solid #334155; background:#0f172a; border-radius:10px; padding:9px 12px; margin-bottom:8px;">
                        <div style="display:flex; justify-content:space-between; align-items:center; gap:8px;">
                            <div style="font-size:14px; font-weight:800; color:#f8fafc;">
                                #{rank} &nbsp; ${p_price:,.2f} <span style="color:#4ade80; font-size:12px;">({p_disc:.0f}% off)</span>
                                <span style="color:{badge_color}; font-size:11px; font-weight:600; margin-left:6px;">· {p_badge}</span>
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


def validate_readonly_sql(sql_text: str) -> str:
    """Ensure SQL Studio queries are strictly single-statement read-only SELECT / WITH queries."""
    s = re.sub(r"^```(?:sql)?\s*|\s*```$", "", (sql_text or "").strip(), flags=re.IGNORECASE | re.MULTILINE).strip()
    s = re.sub(r";+\s*$", "", s).strip()
    if ";" in s:
        raise ValueError("Only a single read-only SELECT statement is permitted.")
    if not re.match(r"^(SELECT|WITH)\b", s, re.IGNORECASE):
        raise ValueError("Generated query must start with SELECT or WITH.")
    if re.search(
        r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|ATTACH|DETACH|COPY|TRUNCATE|PRAGMA|EXPORT|IMPORT|CALL|INSTALL|LOAD)\b",
        s,
        re.IGNORECASE,
    ):
        raise ValueError("Only read-only SELECT queries are permitted.")
    return s


def enrich_categories(df_in: pd.DataFrame) -> pd.DataFrame:
    """Populate normalized `category_primary` and `category_secondary` columns on a DataFrame."""
    if df_in.empty:
        return df_in
    df_out = df_in.copy()
    if "category_primary" not in df_out.columns:
        df_out["category_primary"] = ""
    if "category_secondary" not in df_out.columns:
        df_out["category_secondary"] = ""

    cats = [
        classify_product_category(t, cp, cs)
        for t, cp, cs in zip(
            df_out["title"].fillna(""),
            df_out["category_primary"].fillna(""),
            df_out["category_secondary"].fillna(""),
        )
    ]
    df_out["category_primary"] = [c[0] for c in cats]
    df_out["category_secondary"] = [c[1] for c in cats]
    return df_out


def normalize_df_conditions(df_in: pd.DataFrame) -> pd.DataFrame:
    """Normalize condition strings consistently across Live Deals and Past Archive DataFrames."""
    if df_in.empty:
        return df_in
    df_out = df_in.copy()
    notes_col = df_out["damage_notes"].fillna("") if "damage_notes" in df_out.columns else [""] * len(df_out)
    comp_col = df_out["completeness"].fillna("") if "completeness" in df_out.columns else [""] * len(df_out)
    func_col = df_out["functional_status"].fillna("") if "functional_status" in df_out.columns else [""] * len(df_out)
    df_out["condition"] = [
        "Incomplete / Damaged / Parts"
        if (str(cp).lower() in ("incomplete", "partial") or str(fn).lower() in ("non-functional", "broken"))
        else format_clean_condition(c, t, n)
        for c, t, n, cp, fn in zip(df_out["condition"].fillna(""), df_out["title"].fillna(""), notes_col, comp_col, func_col)
    ]
    return df_out


def inject_nellis_card_css():
    """Inject shared card, filter-bar, slim number-stepper, and responsive Refresh FAB CSS."""
    st.markdown(
        """
        <style>
        /* Floating Refresh FAB: top-right on desktop, thumb-safe bottom-right above Manage app on mobile */
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
        @media (max-width: 768px) {
            .st-key-refresh_live_btn,
            div[data-testid="stElementContainer"]:has(#nellis-refresh-anchor) + div[data-testid="stElementContainer"] {
                top: auto !important;
                bottom: 58px !important;
                right: 14px !important;
            }
        }
        /* Keep 2-column number inputs inside sidebar and popover readable without clipping digits */
        [data-testid="stSidebar"] [data-testid="stNumberInputContainer"] button,
        [data-testid="stPopoverBody"] [data-testid="stNumberInputContainer"] button {
            padding-left: 4px !important;
            padding-right: 4px !important;
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


def _safe_int(val, default: int = 0) -> int:
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return default
        return int(float(val))
    except Exception:
        return default


def _safe_float(val, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        f = float(val)
        return default if math.isnan(f) else f
    except Exception:
        return default


def _supports_kw(fn, kw: str) -> bool:
    try:
        return kw in _inspect.signature(fn).parameters
    except Exception:
        return False


def _stretch_kwargs(fn) -> dict:
    return {"width": "stretch"} if _supports_kw(fn, "width") else {"use_container_width": True}


def _st_columns(spec, vertical_alignment: str = "top"):
    if vertical_alignment != "top" and _supports_kw(st.columns, "vertical_alignment"):
        return st.columns(spec, vertical_alignment=vertical_alignment)
    return st.columns(spec)


def _open_popover_or_expander(label: str):
    if hasattr(st, "popover"):
        return st.popover(label, **_stretch_kwargs(st.popover))
    return st.expander(label, expanded=False)


def _render_quick_pills(label: str, options: list[str], key: str, on_change):
    if hasattr(st, "pills") and _supports_kw(st.pills, "selection_mode"):
        st.pills(
            label,
            options,
            selection_mode="multi",
            label_visibility="collapsed",
            key=key,
            on_change=on_change,
        )
    else:
        st.multiselect(
            label,
            options,
            label_visibility="collapsed",
            placeholder="⚡ Quick filters...",
            key=key,
            on_change=on_change,
        )


def render_pagination_bar(
    key_prefix: str,
    page_state_key: str,
    current_page: int,
    total_pages: int,
    start_idx: int,
    end_idx: int,
    total_results: int,
    noun: str = "lots",
):
    """Render standardized Previous / Next pagination strip with top-scroll behavior."""
    if total_results == 0 or total_pages <= 1:
        return
    p_col1, p_col2, p_col3 = st.columns([1.5, 3, 1.5])
    with p_col1:
        if st.button("⬅️ Previous Page", key=f"{key_prefix}_prev", disabled=(current_page <= 1), **_stretch_kwargs(st.button)):
            st.session_state[page_state_key] = current_page - 1
            st.session_state["_scroll_to_top"] = True
            st.rerun()
    with p_col2:
        st.markdown(
            f"<div style='text-align:center; padding-top:6px; font-weight:600;'>"
            f"Page {current_page} of {total_pages} &nbsp;·&nbsp; Showing {start_idx + 1}–{end_idx} of {total_results:,} {noun}"
            f"</div>",
            unsafe_allow_html=True,
        )
    with p_col3:
        if st.button("Next Page ➡️", key=f"{key_prefix}_next", disabled=(current_page >= total_pages), **_stretch_kwargs(st.button)):
            st.session_state[page_state_key] = current_page + 1
            st.session_state["_scroll_to_top"] = True
            st.rerun()


def _format_live_countdown_badge(diff_sec_raw, closed_at_val) -> str:
    try:
        diff_sec = float(diff_sec_raw or 0)
        if diff_sec <= 0:
            return "🔴 <b>POTENTIALLY CLOSED</b>"
        if diff_sec < 180:
            return f"🔥 <b>{int(diff_sec // 60)}m {int(diff_sec % 60):02d}s left (SNIPE!)</b>"
        if diff_sec < 600:
            return f"⚡ <b>{int(diff_sec // 60)}m {int(diff_sec % 60):02d}s left</b>"
        if diff_sec < 3600:
            return f"⏳ <b>{int(diff_sec // 60)}m left</b>"
        hours = int(diff_sec // 3600)
        mins = int((diff_sec % 3600) // 60)
        return f"🕒 <b>{hours}h {mins}m left</b>"
    except Exception:
        return f"Closes: {str(closed_at_val)[:16]}"


def _render_countdown_script():
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
                        const totalSec = Math.floor((closeMs - now) / 1000);
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
                } catch (e) {}
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
        width=0,
    )


def _render_auction_card_grid(
    records: list[dict],
    is_archive_mode: bool,
    hist_index: dict,
    open_radar_index: dict,
    btn_key_prefix: str = "btn_hist",
    min_hist_sales: int = 2,
    min_hist_disc: int = 85,
    max_hist_avg: int = 25,
):
    """Render responsive 4-column card grid for Live Deals or unified Past Archive."""
    verified_page_hist = batch_verify_rows_history(records, hist_index, same_condition_only=True)
    for row_start in range(0, len(records), 4):
        grid_cols = st.columns(4)
        for col_idx, row_dict in enumerate(records[row_start:row_start + 4]):
            auction_id = _safe_int(row_dict.get("auction_id"))
            item_url = f"https://www.nellisauction.com/p/item/{auction_id}"
            cur_bid = _safe_float(row_dict.get("final_price"))
            msrp = _safe_float(row_dict.get("retail_price"))
            disc_pct_val = _safe_float(row_dict.get("discount_pct"))
            savings_val = _safe_float(row_dict.get("dollar_savings"), max(0.0, msrp - cur_bid))
            cond_display = format_clean_condition(row_dict.get("condition"))
            cond_tier = get_condition_tier(cond_display)
            cat_primary = row_dict.get("category_primary") or "Home & General"
            cat_secondary = row_dict.get("category_secondary") or "Miscellaneous"

            past_matches = verified_page_hist.get(auction_id, [])
            open_matches = [
                c for c in find_open_radar_matches(row_dict, open_radar_index)
                if _safe_int(c.get("auction_id")) != auction_id
            ]
            cheaper_in_batch = []
            if not is_archive_mode and open_matches:
                cheaper_in_batch = [
                    c for c in open_matches
                    if _safe_float(c.get("final_price")) < cur_bid
                    and get_condition_tier(c.get("condition")) == cond_tier
                ]

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
                    safe_title = str(row_dict.get("title") or "Untitled").replace('"', "&quot;").replace("<", "&lt;").replace(">", "&gt;")
                    st.markdown(
                        f'<div class="nellis-card-title" title="{safe_title}"><a href="{item_url}" target="_blank">{safe_title}</a></div>',
                        unsafe_allow_html=True,
                    )

                    bid_label = "WINNING PRICE" if is_archive_mode else "CURRENT PRICE"
                    if is_archive_mode:
                        arch_badge = row_dict.get("_arch_status_badge") or "✅ Verified Won"
                        time_cell_html = (
                            f'<div style="font-size:11.5px; font-weight:700; color:#475569;">🏁 {str(row_dict.get("closed_at"))[:16]}</div>'
                            f'<div style="font-size:10px; font-weight:700; color:#1d4ed8; margin-top:2px;">{arch_badge}</div>'
                        )
                    else:
                        close_epoch_ms = _safe_int(row_dict.get("close_ms"))
                        time_badge = _format_live_countdown_badge(row_dict.get("diff_sec"), row_dict.get("closed_at"))
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

                    third_col_html = (
                        f'<div><div class="nellis-meta-item-label">SAVED</div><div class="nellis-meta-item-val" style="color:#15803d;">${savings_val:,.0f}</div></div>'
                        if is_archive_mode
                        else f'<div><div class="nellis-meta-item-label">LOCATION</div><div class="nellis-meta-item-val">📍 {row_dict.get("location") or "Unknown"}</div></div>'
                    )
                    st.markdown(
                        f"""
                        <div class="nellis-meta-strip">
                            <div><div class="nellis-meta-item-label">EST. RETAIL</div><div class="nellis-meta-item-val">${msrp:,.2f}</div></div>
                            <div><div class="nellis-meta-item-label">BIDS</div><div class="nellis-meta-item-val">{_safe_int(row_dict.get("bid_count"))}</div></div>
                            {third_col_html}
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

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
                    notes = row_dict.get("damage_notes")
                    notes_str = str(notes).strip() if notes and str(notes) != "nan" else ""
                    if "missing" in notes_str.lower():
                        tags.append("missing parts")
                    if "damage" in notes_str.lower() or "broken" in notes_str.lower():
                        tags.append("damaged")
                    tags_html = "".join(f'<span class="{tag_cls}">{t}</span>' for t in tags[:3])
                    if is_archive_mode:
                        tags_html += f' <span class="nellis-tag-pill">📍 {row_dict.get("location") or "Unknown"}</span>'

                    st.markdown(
                        f'<div class="nellis-cond-row"><div class="nellis-stars">{stars_html}</div>{tags_html}</div>',
                        unsafe_allow_html=True,
                    )

                    if notes_str:
                        safe_notes = notes_str.replace('"', "&quot;").replace("<", "&lt;").replace(">", "&gt;")
                        st.markdown(
                            f'<div style="font-size:11.5px; color:#fbbf24; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; margin-bottom:4px;" title="{safe_notes}">⚠️ {safe_notes}</div>',
                            unsafe_allow_html=True,
                        )

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
                            verdict = f"🔴 **Above Avg (${avg_won:.2f})**"
                        if (
                            not is_archive_mode
                            and cur_bid <= avg_won
                            and len(past_matches) >= min_hist_sales
                            and avg_won <= max_hist_avg
                            and hist_avg_disc >= min_hist_disc
                        ):
                            sale_noun = "sale" if len(past_matches) == 1 else "sales"
                            st.markdown(f"🔮 **:green[Likely Cheap Tonight: Avg ${avg_won:.2f} ({hist_avg_disc:.0f}% off · {len(past_matches)} {sale_noun})]**")
                        st.caption(f"🏆 **Min Won ({cond_tier}):** ${min_won:.2f} · Avg ${avg_won:.2f} ({len(past_matches)}) · {verdict}")
                    else:
                        st.caption(f"📊 No past '{cond_tier}' sales yet")

                    if is_archive_mode and open_matches:
                        cheapest_now = float(open_matches[0].get("final_price") or 0)
                        open_url = f"https://www.nellisauction.com/p/item/{int(open_matches[0]['auction_id'])}"
                        st.markdown(f"🟢 **:green[Back Open Tonight!]** ([From ${cheapest_now:.2f} ↗]({open_url}))")
                    elif cheaper_in_batch:
                        cheapest_open = min(float(c.get("final_price") or 0) for c in cheaper_in_batch)
                        st.markdown(f"💡 **:green[{len(cheaper_in_batch)} Cheaper Open (${cheapest_open:.2f})!]**")

                    btn_cls = "nellis-place-bid-btn archive" if is_archive_mode else "nellis-place-bid-btn"
                    action_label = "VIEW CLOSED LOT ↗" if is_archive_mode else "PLACE BID ↗"
                    st.markdown(
                        f'<a class="{btn_cls}" href="{item_url}" target="_blank">{action_label}</a>',
                        unsafe_allow_html=True,
                    )
                    if st.button(f"🔍 History ({len(past_matches)}) & Active ({len(open_matches)})", key=f"{btn_key_prefix}_{auction_id}", **_stretch_kwargs(st.button)):
                        show_item_history_dialog(row_dict)


# --- TOP NAVIGATION & TWO-WAY FILTER ENGINE ---
NAV_OPTIONS = ["🎯 Live Deals", "🗄️ Past Archive", "📊 Analytics", "💻 SQL Studio"]
PRIMARY_CATEGORIES = ["All Categories"] + sorted({p for p, _, _ in CATEGORY_RULES} | {"Home & General"})
WAREHOUSE_OPTIONS = ["All", "Phoenix", "Mesa"]
COND_OPTIONS = ["All", "Brand New", "Like New", "Open Box", "Used", "Incomplete / Damaged / Parts"]
LIVE_TIME_OPTIONS = ["< 5 mins", "< 15 mins", "< 30 mins", "< 1 hour", "< 24 hours", "All Active"]
LIVE_TIME_SEC_MAP = {
    "< 5 mins": 300,
    "< 15 mins": 900,
    "< 30 mins": 1800,
    "< 1 hour": 3600,
    "< 24 hours": 86400,
}
LIVE_SORT_OPTIONS = [
    "Closing Soonest",
    "Best Steal (Hist Avg $)",
    "Highest Hist Avg % Off",
    "Highest Discount (%)",
    "Lowest Current Bid ($)",
    "Highest Retail MSRP ($)",
]
ARCH_DATE_OPTIONS = ["All Time", "Last 24h", "Last 3 Days", "Last 7 Days", "Last 30 Days", "Custom Range"]
ARCH_SORT_OPTIONS = [
    "Most Recently Closed",
    "Highest % Off",
    "Biggest $ Savings",
    "Lowest Winning Bid ($)",
    "Highest Retail MSRP ($)",
]
LIVE_QUICK_PILLS = ["🔥 < 5m", "⚡ < 15m", "🔮 Steals Tonight", "📍 Phoenix", "📍 Mesa", "🆕 Brand New"]
ARCH_QUICK_PILLS = ["🏆 Top Steals (90%+)", "😴 Sleepers", "✅ Settled Only", "📅 Last 7 Days", "📍 Phoenix", "📍 Mesa", "🆕 Brand New"]

LIVE_DEFAULTS = {
    "live_kw": "",
    "live_time": "< 5 mins",
    "live_wh": "All",
    "live_cond": "All",
    "live_sort": "Closing Soonest",
    "live_min_retail": 0,
    "live_max_bid": 1000,
    "live_cat": "All Categories",
    "live_predict_cheap": False,
    "live_min_sales": 2,
    "live_min_disc": 85,
    "live_max_hist_avg": 25,
    "live_page_size": 40,
}


def _get_arch_defaults() -> dict:
    today_az = _dt.date.today()
    return {
        "arch_kw": "",
        "arch_date_preset": "All Time",
        "arch_start": today_az - _dt.timedelta(days=7),
        "arch_end": today_az,
        "arch_wh": "All",
        "arch_cond": "All",
        "arch_sort": "Most Recently Closed",
        "arch_min_retail": 0,
        "arch_max_bid": 1000,
        "arch_cat": "All Categories",
        "arch_min_disc": 0,
        "arch_settled_only": False,
        "arch_sleeper_only": False,
        "arch_page_size": 40,
    }


def _init_filter_state(defaults: dict):
    """Initialize canonical filter keys and sync both sidebar (`sb_*`) and main popover (`top_*`) widget mirrors."""
    for field, default_val in defaults.items():
        if field not in st.session_state:
            st.session_state[field] = default_val
        st.session_state[f"sb_{field}"] = st.session_state[field]
        st.session_state[f"top_{field}"] = st.session_state[field]


def _sync_widget(widget_key: str, canonical_key: str):
    st.session_state[canonical_key] = st.session_state[widget_key]


def _reset_filter_group(defaults: dict):
    for field, default_val in defaults.items():
        st.session_state[field] = default_val
        st.session_state[f"sb_{field}"] = default_val
        st.session_state[f"top_{field}"] = default_val


def _compute_live_pills() -> list[str]:
    out = []
    if st.session_state.get("live_time") == "< 5 mins":
        out.append("🔥 < 5m")
    elif st.session_state.get("live_time") == "< 15 mins":
        out.append("⚡ < 15m")
    if st.session_state.get("live_predict_cheap"):
        out.append("🔮 Steals Tonight")
    wh = st.session_state.get("live_wh", "All")
    if wh == "Phoenix":
        out.append("📍 Phoenix")
    elif wh == "Mesa":
        out.append("📍 Mesa")
    if st.session_state.get("live_cond") == "Brand New":
        out.append("🆕 Brand New")
    return out


def _on_live_pills_change():
    new_set = set(st.session_state.get("live_quick_pills") or [])
    old_set = set(st.session_state.get("_prev_live_quick_pills") or [])
    added = new_set - old_set
    removed = old_set - new_set

    if "🔥 < 5m" in added:
        st.session_state["live_time"] = "< 5 mins"
    elif "⚡ < 15m" in added:
        st.session_state["live_time"] = "< 15 mins"
    elif ("🔥 < 5m" in removed and st.session_state.get("live_time") == "< 5 mins") or (
        "⚡ < 15m" in removed and st.session_state.get("live_time") == "< 15 mins"
    ):
        st.session_state["live_time"] = "All Active"

    if "🔮 Steals Tonight" in added:
        st.session_state["live_predict_cheap"] = True
    elif "🔮 Steals Tonight" in removed:
        st.session_state["live_predict_cheap"] = False

    if "📍 Phoenix" in added:
        st.session_state["live_wh"] = "Phoenix"
    elif "📍 Mesa" in added:
        st.session_state["live_wh"] = "Mesa"
    elif ("📍 Phoenix" in removed and st.session_state.get("live_wh") == "Phoenix") or (
        "📍 Mesa" in removed and st.session_state.get("live_wh") == "Mesa"
    ):
        st.session_state["live_wh"] = "All"

    if "🆕 Brand New" in added:
        st.session_state["live_cond"] = "Brand New"
    elif "🆕 Brand New" in removed and st.session_state.get("live_cond") == "Brand New":
        st.session_state["live_cond"] = "All"


def _compute_arch_pills() -> list[str]:
    out = []
    if int(st.session_state.get("arch_min_disc") or 0) >= 90 and st.session_state.get("arch_settled_only"):
        out.append("🏆 Top Steals (90%+)")
    if st.session_state.get("arch_sleeper_only"):
        out.append("😴 Sleepers")
    if st.session_state.get("arch_settled_only"):
        out.append("✅ Settled Only")
    if st.session_state.get("arch_date_preset") == "Last 7 Days":
        out.append("📅 Last 7 Days")
    wh = st.session_state.get("arch_wh", "All")
    if wh == "Phoenix":
        out.append("📍 Phoenix")
    elif wh == "Mesa":
        out.append("📍 Mesa")
    if st.session_state.get("arch_cond") == "Brand New":
        out.append("🆕 Brand New")
    return out


def _on_arch_pills_change():
    new_set = set(st.session_state.get("arch_quick_pills") or [])
    old_set = set(st.session_state.get("_prev_arch_quick_pills") or [])
    added = new_set - old_set
    removed = old_set - new_set

    if "🏆 Top Steals (90%+)" in added:
        st.session_state["arch_min_disc"] = 90
        st.session_state["arch_settled_only"] = True
        st.session_state["arch_sort"] = "Highest % Off"
        if st.session_state.get("arch_date_preset") == "All Time":
            st.session_state["arch_date_preset"] = "Last 7 Days"
    elif "🏆 Top Steals (90%+)" in removed:
        st.session_state["arch_min_disc"] = 0
        if st.session_state.get("arch_sort") == "Highest % Off":
            st.session_state["arch_sort"] = "Most Recently Closed"
        if st.session_state.get("arch_date_preset") == "Last 7 Days":
            st.session_state["arch_date_preset"] = "All Time"
        st.session_state["arch_settled_only"] = bool(st.session_state.get("arch_sleeper_only"))

    if "😴 Sleepers" in added:
        st.session_state["arch_sleeper_only"] = True
        st.session_state["arch_settled_only"] = True
    elif "😴 Sleepers" in removed:
        st.session_state["arch_sleeper_only"] = False

    if "✅ Settled Only" in added:
        st.session_state["arch_settled_only"] = True
    elif "✅ Settled Only" in removed:
        st.session_state["arch_settled_only"] = False

    if "📅 Last 7 Days" in added:
        st.session_state["arch_date_preset"] = "Last 7 Days"
    elif "📅 Last 7 Days" in removed and st.session_state.get("arch_date_preset") == "Last 7 Days":
        st.session_state["arch_date_preset"] = "All Time"

    if "📍 Phoenix" in added:
        st.session_state["arch_wh"] = "Phoenix"
    elif "📍 Mesa" in added:
        st.session_state["arch_wh"] = "Mesa"
    elif ("📍 Phoenix" in removed and st.session_state.get("arch_wh") == "Phoenix") or (
        "📍 Mesa" in removed and st.session_state.get("arch_wh") == "Mesa"
    ):
        st.session_state["arch_wh"] = "All"

    if "🆕 Brand New" in added:
        st.session_state["arch_cond"] = "Brand New"
    elif "🆕 Brand New" in removed and st.session_state.get("arch_cond") == "Brand New":
        st.session_state["arch_cond"] = "All"


def _guard_nav_view():
    if st.session_state.get("nav_view") not in NAV_OPTIONS:
        st.session_state["nav_view"] = st.session_state.get("_last_nav_view", "🎯 Live Deals")
    else:
        st.session_state["_last_nav_view"] = st.session_state["nav_view"]


def render_top_nav() -> str:
    """Render top horizontal navigation segmented control (with radio fallback on Streamlit < 1.40)."""
    st.markdown('<div id="nellis-top-anchor"></div>', unsafe_allow_html=True)
    if st.session_state.get("nav_view") not in NAV_OPTIONS:
        st.session_state["nav_view"] = st.session_state.get("_last_nav_view", "🎯 Live Deals")
    if hasattr(st, "segmented_control"):
        chosen = st.segmented_control(
            "Navigation",
            NAV_OPTIONS,
            key="nav_view",
            label_visibility="collapsed",
            on_change=_guard_nav_view,
        )
    else:
        chosen = st.radio(
            "Navigation",
            NAV_OPTIONS,
            key="nav_view",
            horizontal=True,
            label_visibility="collapsed",
            on_change=_guard_nav_view,
        )
    if chosen not in NAV_OPTIONS:
        chosen = st.session_state.get("_last_nav_view", "🎯 Live Deals")
    st.session_state["_last_nav_view"] = chosen
    return chosen


def _render_live_field_group(prefix: str, include_search: bool = False, two_col: bool = True):
    """Render compact Live Deals filter controls cleanly without nested columns."""
    if include_search:
        st.text_input(
            "Search",
            placeholder="e.g. DeWalt, Dyson, Pixel, Lego...",
            key=f"{prefix}_live_kw",
            on_change=_sync_widget,
            args=(f"{prefix}_live_kw", "live_kw"),
        )
    if two_col:
        c1, c2 = st.columns(2)
        with c1:
            st.selectbox("Time Left", LIVE_TIME_OPTIONS, key=f"{prefix}_live_time", on_change=_sync_widget, args=(f"{prefix}_live_time", "live_time"))
        with c2:
            st.selectbox("Warehouse", WAREHOUSE_OPTIONS, key=f"{prefix}_live_wh", on_change=_sync_widget, args=(f"{prefix}_live_wh", "live_wh"))
    else:
        st.selectbox("Time Left", LIVE_TIME_OPTIONS, key=f"{prefix}_live_time", on_change=_sync_widget, args=(f"{prefix}_live_time", "live_time"))
        st.selectbox("Warehouse", WAREHOUSE_OPTIONS, key=f"{prefix}_live_wh", on_change=_sync_widget, args=(f"{prefix}_live_wh", "live_wh"))

    st.selectbox("Sort By", LIVE_SORT_OPTIONS, key=f"{prefix}_live_sort", on_change=_sync_widget, args=(f"{prefix}_live_sort", "live_sort"))
    st.selectbox("Condition", COND_OPTIONS, key=f"{prefix}_live_cond", on_change=_sync_widget, args=(f"{prefix}_live_cond", "live_cond"))

    if two_col:
        p1, p2 = st.columns(2)
        with p1:
            st.number_input("Min MSRP ($)", min_value=0, max_value=50000, step=10, key=f"{prefix}_live_min_retail", on_change=_sync_widget, args=(f"{prefix}_live_min_retail", "live_min_retail"))
        with p2:
            st.number_input("Max Bid ($)", min_value=1, max_value=50000, step=25, key=f"{prefix}_live_max_bid", on_change=_sync_widget, args=(f"{prefix}_live_max_bid", "live_max_bid"))
    else:
        st.number_input("Min MSRP ($)", min_value=0, max_value=50000, step=10, key=f"{prefix}_live_min_retail", on_change=_sync_widget, args=(f"{prefix}_live_min_retail", "live_min_retail"))
        st.number_input("Max Bid ($)", min_value=1, max_value=50000, step=25, key=f"{prefix}_live_max_bid", on_change=_sync_widget, args=(f"{prefix}_live_max_bid", "live_max_bid"))

    st.selectbox("Category", PRIMARY_CATEGORIES, key=f"{prefix}_live_cat", on_change=_sync_widget, args=(f"{prefix}_live_cat", "live_cat"))
    st.checkbox(
        "🔮 Likely Cheap Tonight",
        key=f"{prefix}_live_predict_cheap",
        help="Active items currently trading below their past winning average in the same condition.",
        on_change=_sync_widget,
        args=(f"{prefix}_live_predict_cheap", "live_predict_cheap"),
    )
    with st.expander("⚙️ Steal Rules & Page Size", expanded=bool(st.session_state.get("live_predict_cheap"))):
        st.number_input("Min Past Sales", min_value=1, max_value=50, step=1, key=f"{prefix}_live_min_sales", on_change=_sync_widget, args=(f"{prefix}_live_min_sales", "live_min_sales"))
        st.number_input("Min Avg % Off", min_value=50, max_value=99, step=5, key=f"{prefix}_live_min_disc", on_change=_sync_widget, args=(f"{prefix}_live_min_disc", "live_min_disc"))
        st.number_input("Max Hist Avg ($)", min_value=1, max_value=5000, step=5, key=f"{prefix}_live_max_hist_avg", on_change=_sync_widget, args=(f"{prefix}_live_max_hist_avg", "live_max_hist_avg"))
        st.selectbox("Items / Page", [40, 60, 100], key=f"{prefix}_live_page_size", on_change=_sync_widget, args=(f"{prefix}_live_page_size", "live_page_size"))


def _render_inline_search_and_popover(
    placeholder: str,
    search_label: str,
    search_key: str,
    canonical_kw_key: str,
    badge_count: int,
    render_fields_fn,
    pop_reset_key: str,
    top_reset_key: str,
    defaults: dict,
    is_dirty: bool,
):
    """Render flat 1-level inline Search + Filters popover + Reset bar (safe across all Streamlit >=1.30 versions)."""
    col_s, col_pop, col_rst = _st_columns([3.0, 1.25, 0.75], vertical_alignment="center")
    with col_s:
        st.text_input(
            search_label,
            placeholder=placeholder,
            label_visibility="collapsed",
            key=search_key,
            on_change=_sync_widget,
            args=(search_key, canonical_kw_key),
        )
    pop_label = f"🎛️ Filters ({badge_count})" if badge_count else "🎛️ Filters"
    btn_stretch = _stretch_kwargs(st.button)
    with col_pop:
        with _open_popover_or_expander(pop_label):
            render_fields_fn("top", include_search=False, two_col=False)
            st.button(
                "↺ Reset Filters",
                key=pop_reset_key,
                on_click=_reset_filter_group,
                args=(defaults,),
                disabled=not is_dirty,
                **btn_stretch,
            )
    with col_rst:
        st.button(
            "↺ Reset",
            key=top_reset_key,
            on_click=_reset_filter_group,
            args=(defaults,),
            disabled=not is_dirty,
            **btn_stretch,
        )


def render_live_filter_controls() -> dict:
    """Render synchronized sidebar and mobile-first main-page filters for Live Deals."""
    _init_filter_state(LIVE_DEFAULTS)

    badge_count = sum([
        st.session_state["live_time"] != LIVE_DEFAULTS["live_time"],
        st.session_state["live_wh"] != "All",
        st.session_state["live_cond"] != "All",
        st.session_state["live_sort"] != LIVE_DEFAULTS["live_sort"],
        st.session_state["live_cat"] != "All Categories",
        int(st.session_state["live_min_retail"]) > 0,
        int(st.session_state["live_max_bid"]) < 1000,
        bool(st.session_state["live_predict_cheap"]),
    ])
    is_dirty = badge_count > 0 or bool(str(st.session_state["live_kw"]).strip()) or int(st.session_state["live_page_size"]) != 40

    with st.sidebar:
        sh_col, sr_col = _st_columns([2.2, 1], vertical_alignment="center")
        with sh_col:
            st.subheader("🎛️ Live Filters")
        with sr_col:
            st.button(
                "↺ Reset",
                key="sb_live_reset",
                on_click=_reset_filter_group,
                args=(LIVE_DEFAULTS,),
                disabled=not is_dirty,
                **_stretch_kwargs(st.button),
            )
        _render_live_field_group("sb", include_search=True)

    _render_inline_search_and_popover(
        placeholder="🔍 Search live deals (e.g. DeWalt, Dyson, Pixel, Lego)...",
        search_label="Search Active",
        search_key="top_live_kw",
        canonical_kw_key="live_kw",
        badge_count=badge_count,
        render_fields_fn=_render_live_field_group,
        pop_reset_key="pop_live_reset",
        top_reset_key="top_live_reset",
        defaults=LIVE_DEFAULTS,
        is_dirty=is_dirty,
    )

    cur_pills = _compute_live_pills()
    st.session_state["_prev_live_quick_pills"] = cur_pills
    st.session_state["live_quick_pills"] = cur_pills
    _render_quick_pills("Quick Filters", LIVE_QUICK_PILLS, key="live_quick_pills", on_change=_on_live_pills_change)

    parts = [st.session_state["live_time"]]
    if st.session_state["live_wh"] != "All":
        parts.append(f"📍 {st.session_state['live_wh']}")
    if st.session_state["live_cond"] != "All":
        parts.append(st.session_state["live_cond"])
    if st.session_state["live_cat"] != "All Categories":
        parts.append(f"🏷️ {st.session_state['live_cat']}")
    if int(st.session_state["live_min_retail"]) > 0:
        parts.append(f"MSRP ≥ ${int(st.session_state['live_min_retail'])}")
    if int(st.session_state["live_max_bid"]) < 1000:
        parts.append(f"Bid ≤ ${int(st.session_state['live_max_bid'])}")
    if st.session_state["live_predict_cheap"]:
        parts.append(
            f"🔮 Steals (≥{int(st.session_state['live_min_sales'])} sales, "
            f"Avg ≤ ${int(st.session_state['live_max_hist_avg'])}, ≥{int(st.session_state['live_min_disc'])}% off)"
        )
    if str(st.session_state["live_kw"]).strip():
        parts.append(f"🔍 '{str(st.session_state['live_kw']).strip()}'")
    parts.append(f"↕️ {st.session_state['live_sort']}")
    st.caption(f"**Active:** {' · '.join(parts)}")

    return {
        "search_kw": str(st.session_state["live_kw"]),
        "time_filter": st.session_state["live_time"],
        "loc_filter": st.session_state["live_wh"],
        "cond_filter": st.session_state["live_cond"],
        "sort_by": st.session_state["live_sort"],
        "min_retail": int(st.session_state["live_min_retail"]),
        "max_bid": int(st.session_state["live_max_bid"]),
        "cat_filter": st.session_state["live_cat"],
        "predict_cheap_only": bool(st.session_state["live_predict_cheap"]),
        "min_hist_sales": int(st.session_state["live_min_sales"]),
        "min_hist_disc": int(st.session_state["live_min_disc"]),
        "max_hist_avg": int(st.session_state["live_max_hist_avg"]),
        "page_size": int(st.session_state["live_page_size"]),
    }


def _render_arch_field_group(prefix: str, include_search: bool = False, two_col: bool = True):
    """Render compact Past Archive filter controls cleanly without nested columns."""
    if include_search:
        st.text_input(
            "Search",
            placeholder="e.g. Pixel, Dyson, Monitor, Drill...",
            key=f"{prefix}_arch_kw",
            on_change=_sync_widget,
            args=(f"{prefix}_arch_kw", "arch_kw"),
        )
    if two_col:
        c1, c2 = st.columns(2)
        with c1:
            st.selectbox("Date Window", ARCH_DATE_OPTIONS, key=f"{prefix}_arch_date_preset", on_change=_sync_widget, args=(f"{prefix}_arch_date_preset", "arch_date_preset"))
        with c2:
            st.selectbox("Warehouse", WAREHOUSE_OPTIONS, key=f"{prefix}_arch_wh", on_change=_sync_widget, args=(f"{prefix}_arch_wh", "arch_wh"))
    else:
        st.selectbox("Date Window", ARCH_DATE_OPTIONS, key=f"{prefix}_arch_date_preset", on_change=_sync_widget, args=(f"{prefix}_arch_date_preset", "arch_date_preset"))
        st.selectbox("Warehouse", WAREHOUSE_OPTIONS, key=f"{prefix}_arch_wh", on_change=_sync_widget, args=(f"{prefix}_arch_wh", "arch_wh"))

    if st.session_state.get("arch_date_preset") == "Custom Range":
        if two_col:
            d1, d2 = st.columns(2)
            with d1:
                st.date_input("From", key=f"{prefix}_arch_start", on_change=_sync_widget, args=(f"{prefix}_arch_start", "arch_start"))
            with d2:
                st.date_input("To", key=f"{prefix}_arch_end", on_change=_sync_widget, args=(f"{prefix}_arch_end", "arch_end"))
        else:
            st.date_input("From", key=f"{prefix}_arch_start", on_change=_sync_widget, args=(f"{prefix}_arch_start", "arch_start"))
            st.date_input("To", key=f"{prefix}_arch_end", on_change=_sync_widget, args=(f"{prefix}_arch_end", "arch_end"))

    st.selectbox("Sort By", ARCH_SORT_OPTIONS, key=f"{prefix}_arch_sort", on_change=_sync_widget, args=(f"{prefix}_arch_sort", "arch_sort"))
    st.selectbox("Condition", COND_OPTIONS, key=f"{prefix}_arch_cond", on_change=_sync_widget, args=(f"{prefix}_arch_cond", "arch_cond"))

    if two_col:
        p1, p2 = st.columns(2)
        with p1:
            st.number_input("Min MSRP ($)", min_value=0, max_value=50000, step=10, key=f"{prefix}_arch_min_retail", on_change=_sync_widget, args=(f"{prefix}_arch_min_retail", "arch_min_retail"))
        with p2:
            st.number_input("Max Bid ($)", min_value=1, max_value=50000, step=25, key=f"{prefix}_arch_max_bid", on_change=_sync_widget, args=(f"{prefix}_arch_max_bid", "arch_max_bid"))
    else:
        st.number_input("Min MSRP ($)", min_value=0, max_value=50000, step=10, key=f"{prefix}_arch_min_retail", on_change=_sync_widget, args=(f"{prefix}_arch_min_retail", "arch_min_retail"))
        st.number_input("Max Bid ($)", min_value=1, max_value=50000, step=25, key=f"{prefix}_arch_max_bid", on_change=_sync_widget, args=(f"{prefix}_arch_max_bid", "arch_max_bid"))

    st.selectbox("Category", PRIMARY_CATEGORIES, key=f"{prefix}_arch_cat", on_change=_sync_widget, args=(f"{prefix}_arch_cat", "arch_cat"))

    if two_col:
        b1, b2 = st.columns(2)
        with b1:
            st.number_input("Min % Off", min_value=0, max_value=99, step=5, key=f"{prefix}_arch_min_disc", on_change=_sync_widget, args=(f"{prefix}_arch_min_disc", "arch_min_disc"))
        with b2:
            st.selectbox("Items / Page", [40, 60, 100], key=f"{prefix}_arch_page_size", on_change=_sync_widget, args=(f"{prefix}_arch_page_size", "arch_page_size"))
    else:
        st.number_input("Min % Off", min_value=0, max_value=99, step=5, key=f"{prefix}_arch_min_disc", on_change=_sync_widget, args=(f"{prefix}_arch_min_disc", "arch_min_disc"))
        st.selectbox("Items / Page", [40, 60, 100], key=f"{prefix}_arch_page_size", on_change=_sync_widget, args=(f"{prefix}_arch_page_size", "arch_page_size"))

    st.checkbox(
        "✅ Settled WON FOR Sales Only",
        key=f"{prefix}_arch_settled_only",
        help="Unchecked searches ALL closed lots in the Data Lake (visible cards are live-checked automatically).",
        on_change=_sync_widget,
        args=(f"{prefix}_arch_settled_only", "arch_settled_only"),
    )
    st.checkbox(
        "😴 Sleepers (Bids ≤ 3 & MSRP ≥ $100)",
        key=f"{prefix}_arch_sleeper_only",
        on_change=_sync_widget,
        args=(f"{prefix}_arch_sleeper_only", "arch_sleeper_only"),
    )


def render_arch_filter_controls() -> dict:
    """Render synchronized sidebar and mobile-first main-page filters for unified Past Archive."""
    arch_defaults = _get_arch_defaults()
    _init_filter_state(arch_defaults)

    badge_count = sum([
        st.session_state["arch_date_preset"] != "All Time",
        st.session_state["arch_wh"] != "All",
        st.session_state["arch_cond"] != "All",
        st.session_state["arch_sort"] != "Most Recently Closed",
        st.session_state["arch_cat"] != "All Categories",
        int(st.session_state["arch_min_retail"]) > 0,
        int(st.session_state["arch_max_bid"]) < 1000,
        int(st.session_state["arch_min_disc"]) > 0,
        bool(st.session_state["arch_settled_only"]),
        bool(st.session_state["arch_sleeper_only"]),
    ])
    is_dirty = badge_count > 0 or bool(str(st.session_state["arch_kw"]).strip()) or int(st.session_state["arch_page_size"]) != 40

    with st.sidebar:
        sh_col, sr_col = _st_columns([2.2, 1], vertical_alignment="center")
        with sh_col:
            st.subheader("🗄️ Archive Filters")
        with sr_col:
            st.button(
                "↺ Reset",
                key="sb_arch_reset",
                on_click=_reset_filter_group,
                args=(arch_defaults,),
                disabled=not is_dirty,
                **_stretch_kwargs(st.button),
            )
        _render_arch_field_group("sb", include_search=True)

    _render_inline_search_and_popover(
        placeholder="🔍 Search past auctions & steals (e.g. Pixel, Dyson, Monitor)...",
        search_label="Search Archive",
        search_key="top_arch_kw",
        canonical_kw_key="arch_kw",
        badge_count=badge_count,
        render_fields_fn=_render_arch_field_group,
        pop_reset_key="pop_arch_reset",
        top_reset_key="top_arch_reset",
        defaults=arch_defaults,
        is_dirty=is_dirty,
    )

    cur_pills = _compute_arch_pills()
    st.session_state["_prev_arch_quick_pills"] = cur_pills
    st.session_state["arch_quick_pills"] = cur_pills
    _render_quick_pills("Archive Quick Views", ARCH_QUICK_PILLS, key="arch_quick_pills", on_change=_on_arch_pills_change)

    parts = [st.session_state["arch_date_preset"]]
    if st.session_state["arch_settled_only"]:
        parts.append("✅ Settled Sales Only")
    else:
        parts.append("All Closed Lots")
    if int(st.session_state["arch_min_disc"]) > 0:
        parts.append(f"🔥 ≥ {int(st.session_state['arch_min_disc'])}% off")
    if st.session_state["arch_sleeper_only"]:
        parts.append("😴 Sleepers")
    if st.session_state["arch_wh"] != "All":
        parts.append(f"📍 {st.session_state['arch_wh']}")
    if st.session_state["arch_cond"] != "All":
        parts.append(st.session_state["arch_cond"])
    if st.session_state["arch_cat"] != "All Categories":
        parts.append(f"🏷️ {st.session_state['arch_cat']}")
    if int(st.session_state["arch_min_retail"]) > 0:
        parts.append(f"MSRP ≥ ${int(st.session_state['arch_min_retail'])}")
    if int(st.session_state["arch_max_bid"]) < 1000:
        parts.append(f"Bid ≤ ${int(st.session_state['arch_max_bid'])}")
    if str(st.session_state["arch_kw"]).strip():
        parts.append(f"🔍 '{str(st.session_state['arch_kw']).strip()}'")
    parts.append(f"↕️ {st.session_state['arch_sort']}")
    st.caption(f"**Archive:** {' · '.join(parts)}")

    return {
        "search_kw": str(st.session_state["arch_kw"]),
        "date_preset": st.session_state["arch_date_preset"],
        "start_date": st.session_state["arch_start"],
        "end_date": st.session_state["arch_end"],
        "loc_filter": st.session_state["arch_wh"],
        "cond_filter": st.session_state["arch_cond"],
        "sort_by": st.session_state["arch_sort"],
        "min_retail": int(st.session_state["arch_min_retail"]),
        "max_bid": int(st.session_state["arch_max_bid"]),
        "cat_filter": st.session_state["arch_cat"],
        "min_disc": int(st.session_state["arch_min_disc"]),
        "settled_only": bool(st.session_state["arch_settled_only"]),
        "sleeper_only": bool(st.session_state["arch_sleeper_only"]),
        "page_size": int(st.session_state["arch_page_size"]),
    }


view_mode = render_top_nav()


# --- TAB 1: LIVE DEALS (ACTIVE AUCTIONS & SNIPING) ---
if view_mode == "🎯 Live Deals":
    inject_nellis_card_css()
    f_cfg = render_live_filter_controls()
    search_kw = f_cfg["search_kw"]
    time_filter = f_cfg["time_filter"]
    loc_filter = f_cfg["loc_filter"]
    cond_filter = f_cfg["cond_filter"]
    sort_by = f_cfg["sort_by"]
    min_retail = f_cfg["min_retail"]
    max_bid = f_cfg["max_bid"]
    cat_filter = f_cfg["cat_filter"]
    predict_cheap_only = f_cfg["predict_cheap_only"]
    min_hist_sales = f_cfg["min_hist_sales"]
    min_hist_disc = f_cfg["min_hist_disc"]
    max_hist_avg = f_cfg["max_hist_avg"]
    page_size_opt = f_cfg["page_size"]

    st.markdown('<span id="nellis-refresh-anchor"></span>', unsafe_allow_html=True)
    if st.button("🔄 Refresh Live Data", key="refresh_live_btn", type="primary"):
        fetch_live_nellis_lots.clear()
        search_live_active_duplicates.clear()
        st.rerun()

    filtered_all = pd.DataFrame()
    live_df = pd.DataFrame()
    short_window_sec = LIVE_TIME_SEC_MAP.get(time_filter, 0) if time_filter != "< 24 hours" else 0

    with st.spinner("Fetching active lots..."):
        live_df = fetch_live_nellis_lots(
            search_kw=search_kw,
            loc_filter=loc_filter,
            sort_by=sort_by,
            page_size=240,
            max_pages=8,
            target_window_sec=short_window_sec,
        )

        # For "< 24 hours" or "All Active", also union any active (closed_at > now()) lots in the Data Lake
        if time_filter in ("All Active", "< 24 hours"):
            where_parts = [
                "f.closed_at > now()",
                f"f.retail_price >= {min_retail}",
                f"f.final_price <= {max_bid}",
            ]
            if time_filter == "< 24 hours":
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
                "Closing Soonest": "f.closed_at ASC",
                "Highest Discount (%)": "f.discount_ratio ASC",
                "Lowest Current Bid ($)": "f.final_price ASC",
                "Highest Retail MSRP ($)": "f.retail_price DESC",
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
        live_df = normalize_df_conditions(live_df)
        # Recompute diff_sec against live current time (never use stale cached countdowns)
        now_ms_live = int(time.time() * 1000)
        if "close_ms" in live_df.columns:
            live_df = live_df.copy()
            live_df["diff_sec"] = (pd.to_numeric(live_df["close_ms"], errors="coerce").fillna(0) - now_ms_live) / 1000.0

        # Exclude expired or about-to-expire (<10s / <20s) lots that flip to POTENTIALLY CLOSED before render
        min_sec_buffer = 10.0 if time_filter == "< 5 mins" else 20.0
        filtered = live_df[
            (live_df["diff_sec"] > min_sec_buffer)
            & (live_df["retail_price"] >= min_retail)
            & (live_df["final_price"] <= max_bid)
        ]
        if cat_filter != "All Categories":
            filtered = filtered[filtered["category_primary"] == cat_filter]
        if loc_filter != "All":
            filtered = filtered[filtered["location"].str.contains(loc_filter, case=False, na=False, regex=False)]
        if cond_filter != "All":
            filtered = filtered[filtered["condition"].str.contains(cond_filter, case=False, na=False, regex=False)]
        if search_kw.strip() and not filtered.empty:
            scores = [
                1.0 if direct else smart_fuzzy_search_score(search_kw, t, n)
                for direct, t, n in zip(
                    filtered.get("_direct_nellis_match", pd.Series([False] * len(filtered))).fillna(False),
                    filtered["title"].fillna(""),
                    filtered["damage_notes"].fillna(""),
                )
            ]
            filtered = filtered.assign(_search_score=scores)
            filtered = filtered[filtered["_search_score"] >= 0.65]

        if time_filter in LIVE_TIME_SEC_MAP:
            filtered = filtered[filtered["diff_sec"] <= LIVE_TIME_SEC_MAP[time_filter]]

        need_hist_scoring = (
            predict_cheap_only
            or sort_by in ("Best Steal (Hist Avg $)", "Highest Hist Avg % Off")
        )
        if need_hist_scoring and not filtered.empty:
            hist_index_for_pred = load_historical_price_index()
            cand_rows = filtered.to_dict("records")

            # Pre-filter candidates, then live-verify matching sales so Likely Cheap Tonight
            # never surfaces stale $1 pre-close prices or already-expensive live items.
            pre_keep_rows = []
            for r in cand_rows:
                ms = find_historical_matches(r, hist_index_for_pred, same_condition_only=True)
                if not ms:
                    continue
                avg_p = sum(float(m.get("final_price") or 0) for m in ms) / len(ms)
                msrp_v = float(r.get("retail_price") or 0)
                cur_p = float(r.get("final_price") or 0)
                avg_d = ((msrp_v - avg_p) / msrp_v * 100.0) if msrp_v > 0 else 0.0
                if (not predict_cheap_only) or (
                    len(ms) >= min_hist_sales
                    and avg_p <= max_hist_avg
                    and cur_p <= max_hist_avg
                    and avg_d >= min_hist_disc
                ):
                    pre_keep_rows.append(r)

            verified_pred_map = batch_verify_rows_history(
                pre_keep_rows,
                hist_index_for_pred,
                same_condition_only=True,
                max_verify_per_item=15,
                max_total_live_checks=280,
            )

            h_counts, h_avgs, h_discs, h_still_cheap = [], [], [], []
            for r in cand_rows:
                aid_r = int(r.get("auction_id") or 0)
                ms = verified_pred_map.get(aid_r, [])
                if ms:
                    avg_p = sum(float(m.get("final_price") or 0) for m in ms) / len(ms)
                    msrp_v = float(r.get("retail_price") or 0)
                    cur_p = float(r.get("final_price") or 0)
                    avg_d = ((msrp_v - avg_p) / msrp_v * 100.0) if msrp_v > 0 else 0.0
                    h_counts.append(len(ms))
                    h_avgs.append(avg_p)
                    h_discs.append(avg_d)
                    h_still_cheap.append(cur_p <= avg_p)
                else:
                    h_counts.append(0)
                    h_avgs.append(999999.0)
                    h_discs.append(-1.0)
                    h_still_cheap.append(False)
            filtered = filtered.assign(
                _hist_count=h_counts,
                _hist_avg_price=h_avgs,
                _hist_avg_disc=h_discs,
                _has_hist=[c > 0 for c in h_counts],
                _still_below_avg=h_still_cheap,
            )
            if predict_cheap_only:
                filtered = filtered[
                    (filtered["_hist_count"] >= min_hist_sales)
                    & (filtered["_hist_avg_price"] <= max_hist_avg)
                    & (filtered["_hist_avg_disc"] >= min_hist_disc)
                    & (filtered["final_price"] <= filtered["_hist_avg_price"])
                ]

        if sort_by == "Best Steal (Hist Avg $)" and "_hist_avg_price" in filtered.columns:
            filtered = filtered.sort_values(by=["_has_hist", "_still_below_avg", "_hist_avg_price", "_hist_avg_disc"], ascending=[False, False, True, False])
        elif sort_by == "Highest Hist Avg % Off" and "_hist_avg_disc" in filtered.columns:
            filtered = filtered.sort_values(by=["_has_hist", "_still_below_avg", "_hist_avg_disc", "_hist_avg_price"], ascending=[False, False, False, True])
        elif sort_by == "Highest Discount (%)":
            filtered = filtered.sort_values(by="discount_pct", ascending=False)
        elif sort_by == "Lowest Current Bid ($)":
            filtered = filtered.sort_values(by="final_price", ascending=True)
        elif sort_by == "Highest Retail MSRP ($)":
            filtered = filtered.sort_values(by="retail_price", ascending=False)
        else:
            filtered = filtered.sort_values(by="diff_sec", ascending=True)

        filtered_all = filtered

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

    hist_index = load_historical_price_index()
    open_radar_index = load_open_radar_index()

    if total_results > 0:
        st.write(f"**Found {total_results:,} active lots** (showing {start_idx + 1}–{end_idx}):")
        render_pagination_bar("top", "page_num", current_page, total_pages, start_idx, end_idx, total_results, noun="lots")
    else:
        st.write("**Showing 0 active lots:**")

    if results_df.empty:
        st.info("No active items match your filter criteria right now. Tap **Reset** above or widen your filters to see more auctions.")
    else:
        _render_auction_card_grid(
            results_df.to_dict("records"),
            is_archive_mode=False,
            hist_index=hist_index,
            open_radar_index=open_radar_index,
            btn_key_prefix="btn_hist",
            min_hist_sales=min_hist_sales,
            min_hist_disc=min_hist_disc,
            max_hist_avg=max_hist_avg,
        )
        if total_pages > 1:
            st.markdown("---")
            render_pagination_bar("bottom", "page_num", current_page, total_pages, start_idx, end_idx, total_results, noun="lots")
        _render_countdown_script()


# --- TAB 2: PAST ARCHIVE (UNIFIED HISTORICAL SEARCH & TOP DEALS) ---
elif view_mode == "🗄️ Past Archive":
    inject_nellis_card_css()
    a_cfg = render_arch_filter_controls()
    a_kw = a_cfg["search_kw"]
    a_date_preset = a_cfg["date_preset"]
    a_start_date = a_cfg["start_date"]
    a_end_date = a_cfg["end_date"]
    a_loc = a_cfg["loc_filter"]
    a_cond = a_cfg["cond_filter"]
    a_sort = a_cfg["sort_by"]
    a_min_retail = a_cfg["min_retail"]
    a_max_bid = a_cfg["max_bid"]
    a_cat = a_cfg["cat_filter"]
    a_min_disc = a_cfg["min_disc"]
    a_settled_only = a_cfg["settled_only"]
    a_sleeper_only = a_cfg["sleeper_only"]
    a_page_size = a_cfg["page_size"]

    base_arch_where = get_settled_sales_where_sql("f") if a_settled_only else get_archive_candidates_where_sql("f")
    where_parts = [
        base_arch_where,
        f"f.retail_price >= {a_min_retail}",
        f"f.final_price <= {a_max_bid}",
    ]
    if a_min_disc > 0:
        where_parts.append("f.retail_price > 0")
        where_parts.append(f"ROUND((1.0 - f.discount_ratio) * 100.0, 1) >= {a_min_disc}")

    if a_date_preset == "Last 24h":
        where_parts.append("f.closed_at >= now() - INTERVAL 24 HOUR")
    elif a_date_preset == "Last 3 Days":
        where_parts.append("f.closed_at >= now() - INTERVAL 3 DAY")
    elif a_date_preset == "Last 7 Days":
        where_parts.append("f.closed_at >= now() - INTERVAL 7 DAY")
    elif a_date_preset == "Last 30 Days":
        where_parts.append("f.closed_at >= now() - INTERVAL 30 DAY")
    elif a_date_preset == "Custom Range" and a_start_date and a_end_date:
        where_parts.append(f"CAST(f.closed_at AS DATE) >= DATE '{a_start_date.isoformat()}'")
        where_parts.append(f"CAST(f.closed_at AS DATE) <= DATE '{a_end_date.isoformat()}'")

    if a_sleeper_only:
        where_parts.append("f.bid_count <= 3 AND f.retail_price >= 100")
    if a_loc != "All":
        where_parts.append(f"f.location = '{a_loc}'")
    if a_cond == "Incomplete / Damaged / Parts":
        where_parts.append("(f.condition ILIKE '%Damaged%' OR f.condition ILIKE '%Incomplete%' OR f.condition ILIKE '%Parts%')")
    elif a_cond != "All":
        where_parts.append(f"f.condition ILIKE '%{a_cond}%'")
    if a_kw.strip():
        fuzzy_sql = build_fuzzy_sql_where(a_kw)
        if fuzzy_sql:
            where_parts.append(fuzzy_sql)

    where_sql = " AND ".join(where_parts)
    sort_sql = {
        "Most Recently Closed": "f.closed_at DESC",
        "Highest % Off": "f.discount_ratio ASC, f.retail_price DESC",
        "Biggest $ Savings": "(f.retail_price - f.final_price) DESC",
        "Lowest Winning Bid ($)": "f.final_price ASC",
        "Highest Retail MSRP ($)": "f.retail_price DESC",
    }.get(a_sort, "f.closed_at DESC")

    archive_sql = f"""
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
            f.photo_url,
            f.is_closed,
            f.market_status,
            COUNT(*) OVER () AS _sql_total_count,
            CAST(epoch_ms(f.closed_at) AS BIGINT) AS close_ms
        FROM fact_auctions f
        JOIN dim_products p ON f.product_id = p.product_id
        WHERE {where_sql}
        ORDER BY {sort_sql}
        LIMIT 2000;
    """

    arch_df = pd.DataFrame()
    with st.spinner("Searching closed historical auctions in Data Lake..."):
        try:
            arch_df = run_cached_duckdb_query(archive_sql)
        except Exception as e:
            st.error(f"Error querying Past Archive: {e}")

    if not arch_df.empty:
        arch_df = enrich_categories(arch_df)
        arch_df = normalize_df_conditions(arch_df)
        if a_cond != "All":
            arch_df = arch_df[arch_df["condition"].str.contains(a_cond, case=False, na=False, regex=False)]
        if a_cat != "All Categories":
            arch_df = arch_df[arch_df["category_primary"] == a_cat]
        if a_kw.strip() and not arch_df.empty:
            scores = [
                smart_fuzzy_search_score(a_kw, t, n)
                for t, n in zip(arch_df["title"].fillna(""), arch_df["damage_notes"].fillna(""))
            ]
            arch_df = arch_df.assign(_search_score=scores)
            arch_df = arch_df[arch_df["_search_score"] >= 0.65]

    if arch_df.empty:
        st.info("No past closed lots match your filters. Try tapping **Reset** or widening the Date Window / Discount threshold.")
    else:
        total_arch = len(arch_df)
        sql_total_arch = int(arch_df["_sql_total_count"].max() or 0) if "_sql_total_count" in arch_df.columns else total_arch

        # Target Bid Cheat Sheet & Condition Arbitrage: prefer verified sales when mixed archive results exist
        settled_mask = (arch_df["is_closed"] == True) & (arch_df["bid_count"].fillna(0) > 0) & (arch_df["final_price"].fillna(0) > 0)
        stats_df = arch_df[settled_mask] if (not a_settled_only and settled_mask.any()) else arch_df
        stats_scope = "verified sales" if (a_settled_only or settled_mask.any()) else "listed closes"

        median_win = float(stats_df["final_price"].median())
        steal_p20 = float(stats_df["final_price"].quantile(0.20))
        median_disc = float(stats_df["discount_pct"].median())
        avg_bids = float(stats_df["bid_count"].mean())
        max_savings = float(stats_df["dollar_savings"].max())

        lot_count_sub = (
            f"of {sql_total_arch:,} in lake · Avg {avg_bids:.1f} bids"
            if sql_total_arch > total_arch
            else f"Avg {avg_bids:.1f} bids"
        )

        st.markdown(
            f"""
            <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(145px, 1fr)); gap:8px; margin-bottom:10px;">
                <div class="nellis-stat-box">
                    <div class="nellis-stat-label">Matching Lots</div>
                    <div class="nellis-stat-val">{total_arch:,}</div>
                    <div style="font-size:11px; color:#475569; font-weight:600;">{lot_count_sub}</div>
                </div>
                <div class="nellis-stat-box">
                    <div class="nellis-stat-label">Median Win ({stats_scope})</div>
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

        cond_base_df = stats_df.copy()
        cond_base_df["_cond_tier"] = [get_condition_tier(c) for c in cond_base_df["condition"]]
        tier_summary = (
            cond_base_df.groupby("_cond_tier", as_index=False)
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

        ARCH_PAGE_SIZE = a_page_size
        arch_sig = (
            a_kw.strip(), a_date_preset, str(a_start_date), str(a_end_date),
            a_min_disc, a_settled_only, a_sleeper_only, a_cat, a_loc, a_cond,
            a_min_retail, a_max_bid, a_sort, ARCH_PAGE_SIZE,
        )
        if st.session_state.get("arch_last_sig") != arch_sig:
            st.session_state["arch_last_sig"] = arch_sig
            st.session_state["arch_page_num"] = 1

        arch_total_pages = max(1, math.ceil(total_arch / ARCH_PAGE_SIZE))
        arch_curr_page = min(max(1, int(st.session_state.get("arch_page_num", 1))), arch_total_pages)
        st.session_state["arch_page_num"] = arch_curr_page
        arch_start_i = (arch_curr_page - 1) * ARCH_PAGE_SIZE
        arch_end_i = min(arch_start_i + ARCH_PAGE_SIZE, total_arch)
        arch_page_df = arch_df.iloc[arch_start_i:arch_end_i]

        if sql_total_arch > total_arch and total_arch >= 2000:
            st.write(f"**Found {sql_total_arch:,} closed historical lots** (showing {arch_start_i + 1}–{arch_end_i} of top 2,000):")
        elif sql_total_arch > total_arch:
            st.write(f"**Found {total_arch:,} matching closed historical lots** (from top 2,000 of {sql_total_arch:,}; showing {arch_start_i + 1}–{arch_end_i}):")
        else:
            st.write(f"**Found {total_arch:,} closed historical lots** (showing {arch_start_i + 1}–{arch_end_i}):")

        render_pagination_bar("arch_top", "arch_page_num", arch_curr_page, arch_total_pages, arch_start_i, arch_end_i, total_arch, noun="lots")

        records = arch_page_df.to_dict("records")
        # Live-verify unverified visible page rows so archive results show live status without scanning all 290k rows
        unv_page_ids = tuple(sorted({
            int(r["auction_id"])
            for r in records
            if not (r.get("is_closed") is True) and int(r.get("auction_id") or 0) > 0
        }))
        page_live_map = verify_closed_lots_cached(unv_page_ids) if unv_page_ids else {}
        for r in records:
            if r.get("is_closed") is True:
                r["_arch_status_badge"] = "✅ Verified Won"
                continue
            res_l = page_live_map.get(int(r.get("auction_id") or 0))
            if res_l and not res_l.get("not_found") and res_l.get("is_closed"):
                r["final_price"] = float(res_l.get("final_price") or r.get("final_price") or 0)
                r["bid_count"] = int(res_l.get("bid_count") or r.get("bid_count") or 0)
                msrp_r = float(r.get("retail_price") or res_l.get("retail_price") or 0)
                r["dollar_savings"] = max(0.0, msrp_r - r["final_price"])
                if msrp_r > 0:
                    r["discount_pct"] = round((1.0 - r["final_price"] / msrp_r) * 100.0, 1)
                mkt_l = str(res_l.get("market_status") or "sold").lower()
                r["_arch_status_badge"] = (
                    "✅ Live Verified Won"
                    if is_verified_sold_result(res_l)
                    else f"⚠️ {mkt_l.title()} (0 bids)"
                )
            elif res_l and res_l.get("not_found"):
                r["_arch_status_badge"] = "🗄️ Purged / Archived"
            else:
                r["_arch_status_badge"] = "📸 Pre-Close Snapshot"

        open_radar_index = load_open_radar_index()
        hist_index = load_historical_price_index()
        _render_auction_card_grid(
            records,
            is_archive_mode=True,
            hist_index=hist_index,
            open_radar_index=open_radar_index,
            btn_key_prefix="arch_btn_hist",
        )
        if arch_total_pages > 1:
            st.markdown("---")
            render_pagination_bar("arch_bot", "arch_page_num", arch_curr_page, arch_total_pages, arch_start_i, arch_end_i, total_arch, noun="lots")


# --- TAB 3: ANALYTICS HUB ---
elif view_mode == "📊 Analytics":
    with st.sidebar:
        st.subheader("📊 Analytics Hub")
        st.caption("Filters apply to **🎯 Live Deals** and **🗄️ Past Archive**. Choose one of the 5 Data Lake models on the right.")
    st.title("📊 Nellis Auction Analytical Models")
    st.caption("Sub-second SQL analytics powered by DuckDB columnar processing over Parquet")

    model_choice = st.selectbox(
        "Choose Analytical Model",
        [
            "1. Condition Risk Arbitrage (Brand New vs. Used vs. Damaged)",
            "2. Category & Brand Value Spreads (Lowest Median Discount Ratio)",
            "3. Temporal Sweet Spots (Arizona Day & Hour Competition Heatmap)",
            "4. Sleeper Lot Hunter (High Retail, Low Bids, Heavy Discount)",
            "5. Sniping & Bid Timing Curve (Last 60s vs 5m vs 1h)",
        ]
    )

    settled_where = get_settled_sales_where_sql("f")
    if "1. Condition" in model_choice:
        sql = f"""
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
            WHERE {settled_where} AND f.discount_ratio IS NOT NULL
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
        sql = f"""
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
            WHERE {settled_where} AND f.discount_ratio IS NOT NULL AND f.retail_price >= 25.0
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
        sql = f"""
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
            WHERE {settled_where} AND f.close_day_of_week IS NOT NULL AND f.close_hour IS NOT NULL
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
        sql = f"""
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
            WHERE {settled_where}
              AND f.retail_price >= 150.0
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

    else:
        st.subheader("⏱️ Sniping & Bid Timing Curve")
        st.caption("How prices evolve before close, how often anti-snipe extensions trigger, and when winning bids are placed.")

        summary_sql = f"""
            SELECT
                COUNT(*) AS lots_with_bid_history,
                ROUND(100.0 * AVG(CASE WHEN f.was_extended THEN 1.0 ELSE 0.0 END), 1) AS pct_anti_snipe_extended,
                ROUND(AVG(f.price_at_1h_left), 2) AS avg_price_1h_left,
                ROUND(AVG(f.price_at_5m_left), 2) AS avg_price_5m_left,
                ROUND(AVG(f.final_price), 2) AS avg_final_price,
                ROUND(AVG(f.final_price - COALESCE(f.price_at_5m_left, 0)), 2) AS avg_last_5m_jump,
                ROUND(AVG(f.winning_bid_sec_before_close), 1) AS avg_win_bid_sec_left
            FROM fact_auctions f
            WHERE {settled_where}
              AND COALESCE(f.bids_captured, FALSE) = TRUE
              AND f.bid_count > 0;
        """
        summary_df = run_duckdb_query(summary_sql)
        if summary_df.empty or int(summary_df.iloc[0]["lots_with_bid_history"] or 0) == 0:
            st.info("Per-bid timing analytics (`fact_bids`) are still being populated. Check back shortly after the backfill or next daily scrape finishes!")
        else:
            row_s = summary_df.iloc[0]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Lots With Bid Curves", f"{int(row_s['lots_with_bid_history']):,}")
            k2.metric("Anti-Snipe Extended", f"{row_s['pct_anti_snipe_extended']}%")
            k3.metric("Avg Last-5m Jump", f"+${row_s['avg_last_5m_jump']:.2f}")
            k4.metric("Avg Win Bid Timing", f"{row_s['avg_win_bid_sec_left']:.0f}s left")

            curve_sql = f"""
                SELECT
                    b.time_bucket,
                    COUNT(*) AS total_bids,
                    COUNT(DISTINCT b.auction_id) AS auctions,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_all_bids,
                    ROUND(AVG(b.pct_of_final_price) * 100, 1) AS avg_pct_of_final_price,
                    SUM(CASE WHEN b.is_winning_bid THEN 1 ELSE 0 END) AS winning_bids_placed_here
                FROM fact_bids b
                JOIN fact_auctions f ON b.auction_id = f.auction_id
                WHERE {settled_where}
                  AND f.bid_count > 0
                GROUP BY b.time_bucket
                ORDER BY CASE b.time_bucket
                    WHEN 'Early (> 6h left)' THEN 1
                    WHEN '1h - 6h left' THEN 2
                    WHEN '15m - 1h left' THEN 3
                    WHEN '5m - 15m left' THEN 4
                    WHEN '1m - 5m left' THEN 5
                    WHEN 'Last 60s' THEN 6
                    WHEN 'Overtime (Anti-Snipe)' THEN 7
                    ELSE 8
                END;
            """
            st.markdown("##### 📊 Bid Activity & Price Progression by Time Window")
            curve_df = run_duckdb_query(curve_sql)
            st.dataframe(curve_df, use_container_width=True)

            ext_sql = f"""
                SELECT
                    CASE WHEN COALESCE(f.was_extended, FALSE) THEN 'Extended (Anti-Snipe)' ELSE 'Not Extended' END AS close_behavior,
                    COUNT(*) AS lots,
                    ROUND(AVG(f.bid_count), 1) AS avg_bids,
                    ROUND(AVG(f.unique_bidders), 1) AS avg_bidders,
                    ROUND(AVG(f.bids_last_60s), 2) AS avg_bids_last_60s,
                    ROUND(AVG(f.price_at_5m_left), 2) AS avg_price_5m_left,
                    ROUND(AVG(f.final_price), 2) AS avg_final_price
                FROM fact_auctions f
                WHERE {settled_where}
                  AND COALESCE(f.bids_captured, FALSE) = TRUE
                  AND f.bid_count > 0
                GROUP BY close_behavior
                ORDER BY lots DESC;
            """
            st.markdown("##### ⏳ Anti-Snipe Extension Impact")
            ext_df = run_duckdb_query(ext_sql)
            st.dataframe(ext_df, use_container_width=True)


# --- TAB 4: SQL STUDIO ---
elif view_mode == "💻 SQL Studio":
    with st.sidebar:
        st.subheader("💻 SQL Studio")
        st.caption("Run read-only DuckDB queries or ask Gemini in plain English. Switch tabs above to return to deal filters.")
    st.title("💻 Interactive DuckDB SQL Studio")
    st.caption("Ask questions in plain English (powered by Gemini Free Tier) or run custom SQL directly against `fact_auctions`, `fact_bids`, and `dim_products` on Cloudflare R2.")

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
   - unique_bidders (INTEGER; distinct sequential Bidder #N count inside this lot)
   - open_at (TIMESTAMPTZ, Arizona MST)
   - initial_closed_at (TIMESTAMPTZ, scheduled close before any anti-snipe extensions)
   - closed_at (TIMESTAMPTZ, actual final close in Arizona MST)
   - was_extended (BOOLEAN; TRUE if anti-snipe extended close past initial_closed_at)
   - extension_seconds (INTEGER)
   - first_bid_price (DOUBLE)
   - price_at_1h_left (DOUBLE; highest bid price at 60 min before final closed_at)
   - price_at_5m_left (DOUBLE; highest bid price at 5 min before final closed_at)
   - price_at_1m_left (DOUBLE; highest bid price at 60 sec before final closed_at)
   - bids_last_5m (INTEGER; bids placed < 300s before final closed_at)
   - bids_last_60s (INTEGER; bids placed <= 60s before final close)
   - winning_bid_sec_before_close (DOUBLE; seconds before final close when winning bid was placed)
   - bids_captured (BOOLEAN; TRUE if per-bid timing & curve columns are populated)
   - close_day_of_week (TINYINT: 0=Sunday..6=Saturday)
   - close_hour (TINYINT: 0..23)
   - scraped_date (DATE)
   - photo_url (VARCHAR)
   - is_closed (BOOLEAN; TRUE if verified post-close, FALSE if captured while still open)
   - market_status (VARCHAR: 'sold', 'relisted', 'canceled', 'active', 'unverified')
2. dim_products p:
   - product_id (VARCHAR)
   - title (VARCHAR)
   - category_primary (VARCHAR)
   - category_secondary (VARCHAR)
   - brand (VARCHAR)
3. fact_bids b (one row per chronological bid, joins to fact_auctions on auction_id and dim_products on product_id):
   - auction_id (BIGINT)
   - product_id (VARCHAR)
   - bid_seq (SMALLINT: 1 = earliest bid)
   - bidder_label (VARCHAR: e.g. 'Bidder #1'; PER-LOT anonymous label, NOT global across auctions)
   - bidder_num (SMALLINT: 1, 2, ... within that lot)
   - bid_amount (DOUBLE)
   - bid_increment (DOUBLE: jump over previous highest bid in lot)
   - bid_status (VARCHAR: 'Outbid' or 'Winning')
   - is_winning_bid (BOOLEAN)
   - bid_time (TIMESTAMPTZ)
   - bid_hour (TINYINT: 0..23 Arizona MST)
   - seconds_before_close (DOUBLE: seconds before final closed_at)
   - seconds_before_initial_close (DOUBLE: seconds before scheduled initial_closed_at; negative = placed during anti-snipe extension)
   - time_bucket (VARCHAR: 'Early (> 6h left)', '1h - 6h left', '15m - 1h left', '5m - 15m left', '1m - 5m left', 'Last 60s', 'Overtime (Anti-Snipe)')
   - auction_final_price (DOUBLE)
   - pct_of_final_price (DOUBLE: bid_amount / auction_final_price)
   - scraped_date (DATE)"""

    def generate_sql_from_english(question: str, api_key: str) -> str:
        prompt = (
            "You are a DuckDB SQL expert for a Nellis Auction database.\n"
            f"{SCHEMA_PROMPT}\n\n"
            "Rules:\n"
            "- Output ONLY a single valid DuckDB SELECT (or WITH ... SELECT) SQL query. No markdown, no explanations.\n"
            "- Always JOIN dim_products p ON f.product_id = p.product_id (or b.product_id = p.product_id) when using title, brand, or category.\n"
            "- Closed/past sold auctions satisfy f.closed_at <= now() AND COALESCE(f.is_closed, TRUE) = TRUE AND COALESCE(f.market_status, 'sold') NOT IN ('relisted', 'canceled', 'cancelled', 'void', 'unsold', 'active'). For winning-price averages, require f.bid_count > 0 AND f.final_price > 0. 'Last night' means f.closed_at >= now() - INTERVAL 24 HOUR AND f.closed_at <= now().\n"
            "- For bid timing / curve questions, filter COALESCE(f.bids_captured, FALSE) = TRUE or query fact_bids b JOIN fact_auctions f ON b.auction_id = f.auction_id. Remember bidder_label / bidder_num are PER-LOT anonymous numbers (not global account IDs across auctions).\n"
            "- 'New' condition means f.condition = 'Brand New'.\n"
            "- CRITICAL for product keyword matching: NEVER put a multi-word product phrase in one contiguous ILIKE '%a b c%'. Split into separate AND p.title ILIKE '%word%' clauses for the core product words, and handle hyphen/space variants with OR using standard single quotes (e.g., (p.title ILIKE '%usb-c%' OR p.title ILIKE '%usb c%' OR p.title ILIKE '%type-c%') AND p.title ILIKE '%cable%').\n"
            "- For aggregations (AVG, MIN, MAX), ALWAYS use ROUND(..., 2), give clean aliases (e.g. AS avg_price, AS min_price), and also include COUNT(*) AS matching_lots.\n"
            "- Include LIMIT 200 on row-level queries (omit LIMIT on single-row aggregates).\n\n"
            f"User question: {question}"
        )
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={urllib.parse.quote(api_key)}"
        payload = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.0,
                "maxOutputTokens": 2048,
                "thinkingConfig": {"thinkingBudget": 0},
            },
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

    ex_cols = st.columns(4)
    examples = [
        ("0-Bid Brand New Last Night", "How many Brand New items from last night ended up with 0 bids?"),
        ("Cheapest New USB-C Drive", "What's the cheapest final price seen for a Brand New usb-c drive?"),
        ("Top Categories by % Off", "Top 10 categories by average discount % on Brand New items over $100 retail"),
        ("Sniping in Last 60s", "What percentage of bids are placed in each time bucket before auction close?"),
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

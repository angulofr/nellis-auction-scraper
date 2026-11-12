#!/usr/bin/env python3
"""
Nellis Auction Data Lake & Deal Finder App
Cloud-ready Streamlit application queryable from phone, tablet, or desktop.
"""

import ast
import json
import os
import re
import time
import urllib.parse
import urllib.request
import duckdb
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# Page setup
st.set_page_config(
    page_title="Nellis Auction Sniper",
    page_icon="🔨",
    layout="wide",
    initial_sidebar_state="collapsed"
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
def get_duckdb_con():
    """Create and cache persistent DuckDB connection with Cloudflare R2 httpfs."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    host = R2_ENDPOINT_URL.replace("https://", "").replace("http://", "").strip("/")
    con.execute(f"""
        SET s3_endpoint = '{host}';
        SET s3_access_key_id = '{R2_ACCESS_KEY_ID}';
        SET s3_secret_access_key = '{R2_SECRET_ACCESS_KEY}';
        SET s3_url_style = 'path';
        SET s3_region = 'auto';
        SET s3_use_ssl = true;
    """)
    con.execute(f"CREATE OR REPLACE VIEW fact_auctions AS SELECT * FROM read_parquet('s3://{R2_BUCKET_NAME}/fact_auctions/*/*/*.parquet');")
    con.execute(f"CREATE OR REPLACE VIEW dim_products AS SELECT * FROM read_parquet('s3://{R2_BUCKET_NAME}/dim_products/products_catalog.parquet');")
    return con


con = get_duckdb_con()


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


def format_clean_condition(raw_cond) -> str:
    """Convert raw grade dict or string into clean display label."""
    if isinstance(raw_cond, dict):
        c_type = raw_cond.get('conditionType', {})
        c_desc = c_type.get('description', '') if isinstance(c_type, dict) else ''
        rat = raw_cond.get('rating')
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
            return format_clean_condition(g)
        except Exception:
            return "Standard"
    return s


def get_condition_tier(cond_str: str) -> str:
    """Standardize condition into like-for-like comparison tiers."""
    c = format_clean_condition(cond_str).lower()
    if any(k in c for k in ["damaged", "missing", "broken", "1/5", "2/5", "parts"]):
        return "Damaged"
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
        hist_df = con.execute("""
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
        """).df()
    except Exception as e:
        print(f"Warning loading historical index: {e}")
        return {"by_img": {}, "by_sig": {}}

    by_img = {}
    by_sig = {}

    for row in hist_df.to_dict("records"):
        purl = row.get("photo_url") or ""
        title = row.get("title") or ""
        img_id = extract_amazon_image_id(purl)
        sig = get_title_signature(title)
        row["cond_tier"] = get_condition_tier(row.get("condition"))
        row["clean_cond"] = format_clean_condition(row.get("condition"))

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
                condition_str = format_clean_condition(p.get('grade'))

                items.append({
                    'auction_id': p.get('id'),
                    'title': p.get('title', 'Unknown Item'),
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


@st.cache_data(ttl=5)
def fetch_live_nellis_lots(search_kw: str = "", loc_filter: str = "All", sort_by: str = "⏳ Closing Soonest (Sniper)", page_size: int = 120):
    """
    Fetch real-time active lots directly from Nellis Auction's API.
    Supports live keyword search, warehouse location filter, and sorting.
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

        params = [("sortBy", nellis_sort), ("_p1", f"s:{page_size},n:1")]
        if search_kw and search_kw.strip():
            params.append(("query", search_kw.strip()))
        if loc_filter in ["Phoenix", "Mesa"]:
            params.append(("Location Name", loc_filter))

        qs = urllib.parse.urlencode(params)
        url = f'https://www.nellisauction.com/search.data?{qs}'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with opener.open(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        items = parse_nellis_products_from_stream(data)
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
            "_p1": "s:40,n:1"
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

@st.dialog("🔍 Item Price History & Active Copies", width="large")
def show_item_history_dialog(item_row: dict):
    """Modal popup displaying cheaper active copies to bid on + past winning price history."""
    item_id = int(item_row.get("auction_id") or 0)
    title = item_row.get("title") or "Unknown Item"
    cur_bid = float(item_row.get("final_price") or 0)
    msrp = float(item_row.get("retail_price") or 0)
    cond_display = format_clean_condition(item_row.get("condition"))
    cond_tier = get_condition_tier(cond_display)
    photo = item_row.get("photo_url") or ""
    item_url = f"https://www.nellisauction.com/p/item/{item_id}"

    # Header summary of selected item
    h_col1, h_col2 = st.columns([1, 4])
    with h_col1:
        if photo and str(photo) != "nan":
            st.image(photo, use_container_width=True)
    with h_col2:
        st.markdown(f"**[{title}]({item_url})**")
        st.caption(f"Selected Lot ID: `{item_id}` · Condition: **{cond_display}** (`{cond_tier}`) · MSRP: **${msrp:.2f}** · Current Bid: **${cur_bid:.2f}**")
        same_cond_only = st.checkbox(
            f"Like-for-Like Condition Only (Compare only against '{cond_tier}' items)",
            value=True,
            key=f"dlg_cond_toggle_{item_id}"
        )

    st.markdown("---")

    # 1. LIVE ACTIVE COPIES SECTION
    st.subheader("🟢 Currently Open Copies on Nellis (Pick the Cheapest!)")
    with st.spinner("Checking Nellis live catalog for identical open copies..."):
        live_copies = search_live_active_duplicates(title, msrp, photo)
        other_active = [
            c for c in live_copies
            if int(c.get("auction_id") or 0) != item_id
            and (not same_cond_only or get_condition_tier(c.get("condition")) == cond_tier)
        ]

    if not other_active:
        st.info(f"No other open copies of this item in **{cond_tier}** condition were found right now.")
    else:
        cheaper_copies = [c for c in other_active if float(c.get("final_price") or 0) < cur_bid]
        if cheaper_copies:
            lowest_active = float(cheaper_copies[0].get("final_price") or 0)
            savings = cur_bid - lowest_active
            st.success(f"💡 **Found {len(cheaper_copies)} cheaper active {'copy' if len(cheaper_copies)==1 else 'copies'}!** Lowest is currently **${lowest_active:.2f}** (${savings:.2f} cheaper than this lot).")
        else:
            st.caption(f"Found {len(other_active)} other open {'copy' if len(other_active)==1 else 'copies'} (this lot at ${cur_bid:.2f} is currently the cheapest):")

        for idx, copy in enumerate(other_active[:6], start=1):
            c_id = copy.get("auction_id")
            c_url = f"https://www.nellisauction.com/p/item/{c_id}"
            c_price = float(copy.get("final_price") or 0)
            c_disc = float(copy.get("discount_pct") or 0)
            c_loc = copy.get("location") or "AZ"
            c_cond = format_clean_condition(copy.get("condition"))
            c_bids = copy.get("bid_count", 0)
            c_sec = float(copy.get("diff_sec") or 0)
            if c_sec < 3600:
                c_time_str = f"{max(1, int(c_sec // 60))}m left"
            else:
                c_time_str = f"{int(c_sec // 3600)}h {int((c_sec % 3600) // 60)}m left"

            price_tag = f"🔥 **${c_price:.2f}**" if c_price < cur_bid else f"**${c_price:.2f}**"
            a1, a2, a3 = st.columns([2.2, 3.8, 2])
            with a1:
                st.markdown(f"{idx}. {price_tag} *({c_disc:.0f}% off)*")
            with a2:
                st.markdown(f"📍 **{c_loc}** · {c_cond} · ⏳ {c_time_str} *({c_bids} bids)*")
            with a3:
                st.markdown(f"[👉 **Bid on Copy ↗**]({c_url})")

    st.markdown("---")

    # 2. PAST WINNING PRICE HISTORY SECTION
    st.subheader("🏆 Past Winning Price History (Lowest to Highest)")
    hist_index = load_historical_price_index()
    past_matches = find_historical_matches(item_row, hist_index, same_condition_only=same_cond_only)

    if not past_matches:
        st.info(f"No past closed lots recorded yet for this item in **{cond_tier if same_cond_only else 'any'}** condition.")
    else:
        prices = [float(m.get("final_price") or 0) for m in past_matches]
        min_won = min(prices)
        avg_won = sum(prices) / len(prices)
        max_won = max(prices)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Cheapest Won", f"${min_won:.2f}")
        m2.metric("Average Won", f"${avg_won:.2f}")
        m3.metric("Highest Won", f"${max_won:.2f}")
        m4.metric("Total Sold", f"{len(past_matches)}")

        st.caption(f"Showing top {min(5, len(past_matches))} lowest winning prices (click any link to inspect the closed lot on Nellis):")

        for rank, past in enumerate(past_matches[:5], start=1):
            p_id = past.get("auction_id")
            p_url = f"https://www.nellisauction.com/p/item/{p_id}"
            p_price = float(past.get("final_price") or 0)
            p_msrp = float(past.get("retail_price") or msrp or 0)
            p_disc = ((p_msrp - p_price) / p_msrp * 100.0) if p_msrp > 0 else 0
            p_date = str(past.get("closed_at") or "")[:16]
            p_loc = past.get("location") or "AZ"
            p_cond = past.get("clean_cond") or format_clean_condition(past.get("condition"))
            p_bids = past.get("bid_count") or 0

            pcol1, pcol2, pcol3 = st.columns([2.2, 3.8, 2])
            with pcol1:
                st.markdown(f"**#{rank} — ${p_price:.2f}** *({p_disc:.0f}% off)*")
            with pcol2:
                st.markdown(f"📅 {p_date} · 📍 {p_loc} · {p_cond} *({p_bids} bids)*")
            with pcol3:
                st.markdown(f"[🔗 **View on Nellis ↗**]({p_url})")


# --- SIDEBAR CONTROLS ---
st.sidebar.title("🔨 Nellis Auction Lake")
st.sidebar.caption("Phoenix & Mesa, AZ Analytics")

try:
    summary_df = con.execute("""
        SELECT 
            COUNT(*) AS total_lots,
            ROUND(SUM(retail_price), 0) AS total_retail,
            ROUND(AVG(discount_ratio), 3) AS avg_ratio
        FROM fact_auctions
    """).df()
    total_lots = summary_df["total_lots"][0]
    total_retail = summary_df["total_retail"][0]
    avg_discount = (1.0 - summary_df["avg_ratio"][0]) * 100
    st.sidebar.metric("Total Lake Lots", f"{total_lots:,}")
    st.sidebar.metric("Total Retail MSRP", f"${total_retail:,.0f}")
    st.sidebar.metric("Average Discount", f"{avg_discount:.1f}%")
except Exception:
    pass

st.sidebar.markdown("---")
view_mode = st.sidebar.radio("Navigation", ["🎯 Deal Finder", "📊 Analytics Hub", "💻 SQL Studio"])

st.sidebar.markdown("---")
st.sidebar.caption("Cloud Storage: Cloudflare R2 (S3)\nQuery Engine: DuckDB 2.0 (httpfs)\nLive Feed: Nellis Turbo-Stream API")


# --- TAB 1: DEAL FINDER ---
if view_mode == "🎯 Deal Finder":
    # Always-visible top bar with Refresh button
    top_bar_left, top_bar_right = st.columns([3.5, 1.5])
    with top_bar_right:
        if st.button("🔄 Refresh Live Data", use_container_width=True, type="primary"):
            fetch_live_nellis_lots.clear()
            search_live_active_duplicates.clear()
            st.rerun()

    # Collapsible Filter & Sort Options
    with st.expander("⚙️ Filter & Sort Options", expanded=False):
        col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2.5, 2.5])
        with col1:
            search_kw = st.text_input("🔍 Search Keyword", placeholder="e.g. DeWalt, Dyson, Lego, Monitor...")
        with col2:
            loc_filter = st.selectbox("Warehouse", ["All", "Phoenix", "Mesa"])
        with col3:
            cond_filter = st.selectbox("Condition", ["All", "Brand New", "Like New", "Open Box", "Used", "Damaged"])
        with col4:
            time_filter = st.selectbox(
                "⏳ Time Remaining",
                [
                    "🔥 Ending in < 5 mins (Live)",
                    "⚡ Ending in < 15 mins (Live)",
                    "⏳ Ending in < 30 mins (Live)",
                    "🕒 Ending in < 1 hour (Live)",
                    "📅 Ending Today (< 24 hrs)",
                    "All Active Lots"
                ]
            )
        with col5:
            sort_by = st.selectbox(
                "Sort By",
                [
                    "⏳ Closing Soonest (Sniper)",
                    "Highest Discount (%)",
                    "Lowest Final Bid ($)",
                    "Highest Retail MSRP ($)"
                ]
            )

        col_p1, col_p2 = st.columns(2)
        with col_p1:
            min_retail = st.number_input(
                "Minimum Retail MSRP ($)",
                min_value=0,
                max_value=50000,
                value=0,
                step=10
            )
        with col_p2:
            max_bid = st.number_input(
                "Maximum Current / Winning Bid ($)",
                min_value=1,
                max_value=50000,
                value=1000,
                step=25
            )

    with top_bar_left:
        kw_label = f" · 🔍 '{search_kw.strip()}'" if search_kw.strip() else ""
        st.caption(f"**Active Filter:** {time_filter} · 📍 {loc_filter} · {cond_filter} · MSRP ≥ ${min_retail} · Bid ≤ ${max_bid}{kw_label}")

    # Fetch active lots (Always guarantees only ACTIVE lots are shown, never lots that ended last night)
    with st.spinner("Fetching active lots..."):
        live_df = fetch_live_nellis_lots(
            search_kw=search_kw,
            loc_filter=loc_filter,
            sort_by=sort_by,
            page_size=120
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
            if cond_filter != "All":
                where_parts.append(f"f.condition ILIKE '%{cond_filter}%'")
            if search_kw.strip():
                kw = search_kw.strip().replace("'", "''")
                where_parts.append(f"(p.title ILIKE '%{kw}%' OR f.damage_notes ILIKE '%{kw}%')")

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
                    f.location,
                    f.condition,
                    f.retail_price,
                    f.final_price,
                    ROUND((1.0 - f.discount_ratio) * 100.0, 1) AS discount_pct,
                    f.bid_count,
                    f.damage_notes,
                    f.closed_at,
                    f.photo_url,
                    CAST(epoch_ms(f.closed_at) AS BIGINT) AS close_ms,
                    (CAST(epoch_ms(f.closed_at) AS DOUBLE) - {int(time.time()*1000)}) / 1000.0 AS diff_sec
                FROM fact_auctions f
                JOIN dim_products p ON f.product_id = p.product_id
                WHERE {where_sql}
                ORDER BY {sort_sql}
                LIMIT 100;
            """
            try:
                lake_active_df = con.execute(lake_active_query).df()
                if not lake_active_df.empty:
                    if live_df.empty:
                        live_df = lake_active_df
                    else:
                        live_df = pd.concat([live_df, lake_active_df], ignore_index=True)
                        live_df = live_df.drop_duplicates(subset=["auction_id"], keep="first")
            except Exception:
                pass

    results_df = pd.DataFrame()
    if not live_df.empty:
        # Strict filter: Only currently active lots (diff_sec > 0)
        filtered = live_df[
            (live_df['diff_sec'] > 0) &
            (live_df['retail_price'] >= min_retail) &
            (live_df['final_price'] <= max_bid)
        ]
        if loc_filter != "All":
            filtered = filtered[filtered['location'].str.contains(loc_filter, case=False, na=False)]
        if cond_filter != "All":
            filtered = filtered[filtered['condition'].str.contains(cond_filter, case=False, na=False)]
        if search_kw.strip():
            filtered = filtered[
                filtered['title'].str.contains(search_kw.strip(), case=False, na=False) |
                filtered['damage_notes'].str.contains(search_kw.strip(), case=False, na=False)
            ]

        max_sec_map = {
            "🔥 Ending in < 5 mins (Live)": 300,
            "⚡ Ending in < 15 mins (Live)": 900,
            "⏳ Ending in < 30 mins (Live)": 1800,
            "🕒 Ending in < 1 hour (Live)": 3600,
            "📅 Ending Today (< 24 hrs)": 86400
        }
        if time_filter in max_sec_map:
            filtered = filtered[filtered['diff_sec'] <= max_sec_map[time_filter]]

        if sort_by == "Highest Discount (%)":
            filtered = filtered.sort_values(by="discount_pct", ascending=False)
        elif sort_by == "Lowest Final Bid ($)":
            filtered = filtered.sort_values(by="final_price", ascending=True)
        elif sort_by == "Highest Retail MSRP ($)":
            filtered = filtered.sort_values(by="retail_price", ascending=False)
        else:
            filtered = filtered.sort_values(by="diff_sec", ascending=True)

        results_df = filtered.head(50)

    # Load cached historical index for fast O(1) price history badges
    hist_index = load_historical_price_index()
    active_batch_records = live_df.to_dict("records") if not live_df.empty else []

    st.write(f"**Showing {len(results_df)} active lots:**")

    if results_df.empty:
        st.info("No active items match your filter criteria right now. Open '⚙️ Filter & Sort Options' to expand the time window or adjust filters.")
    else:
        for idx, row in results_df.iterrows():
            row_dict = row.to_dict()
            auction_id = int(row_dict['auction_id'])
            item_url = f"https://www.nellisauction.com/p/item/{auction_id}"
            cur_bid = float(row_dict.get('final_price') or 0)
            msrp = float(row_dict.get('retail_price') or 0)
            cond_display = format_clean_condition(row_dict.get('condition'))
            cond_tier = get_condition_tier(cond_display)

            # Calculate initial time badge
            time_badge = ""
            try:
                diff_sec = float(row_dict.get('diff_sec') or 0)
                if diff_sec <= 0:
                    time_badge = "🔴 **POTENTIALLY CLOSED**"
                elif diff_sec < 180:
                    mins = int(diff_sec // 60)
                    secs = int(diff_sec % 60)
                    time_badge = f"🔥 **{mins}m {secs:02d}s left (SNIPE NOW!)**"
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

            # 1. Lookup Historical Closed Matches (Like-for-Like Condition)
            past_matches = find_historical_matches(row_dict, hist_index, same_condition_only=True)

            # 2. Check for cheaper active copies in current batch
            cheaper_in_batch = [
                c for c in active_batch_records
                if int(c.get("auction_id") or 0) != auction_id
                and float(c.get("diff_sec") or 0) > 0
                and float(c.get("final_price") or 0) < cur_bid
                and get_condition_tier(c.get("condition")) == cond_tier
                and is_same_product(
                    row_dict.get("title"), msrp, row_dict.get("photo_url"),
                    c.get("title"), c.get("retail_price"), c.get("photo_url")
                )
            ]

            with st.container():
                c_img, c_card1, c_card2, c_card3 = st.columns([1.4, 4.2, 2.6, 2.8])

                with c_img:
                    photo = row_dict.get("photo_url")
                    if photo and str(photo) != "nan" and str(photo).strip():
                        st.image(photo, use_container_width=True)
                    else:
                        st.caption("📷 No photo")

                with c_card1:
                    st.markdown(f"**[{row_dict['title']}]({item_url})**")
                    notes = row_dict.get('damage_notes')
                    if notes and str(notes) != "nan" and str(notes).strip():
                        st.caption(f"⚠️ **Notes:** {notes}")
                    st.markdown(f"[👉 **Place Bid on Nellis Auction ↗**]({item_url})")

                with c_card2:
                    cond_color = "🟢" if cond_tier == "New" else "🟡"
                    st.write(f"{cond_color} **{cond_display}** · 📍 {row_dict['location']}")
                    close_epoch_ms = int(row_dict.get('close_ms') or 0)
                    time_badge_html = f"""<div class="nellis-countdown-badge" data-close-ms="{close_epoch_ms}" style="display:inline-block; font-weight:700; padding:3px 9px; border-radius:6px; font-size:12px; margin-top:4px; margin-bottom:4px; background-color:#450a0a; color:#fca5a5; border:1px solid #ef4444;">{time_badge}</div>"""
                    st.markdown(time_badge_html, unsafe_allow_html=True)
                    st.caption(f"Bids: **{row_dict['bid_count']}** · Closes: {str(row_dict['closed_at'])[:19]}")

                with c_card3:
                    st.metric(
                        label="Current Bid",
                        value=f"${cur_bid:.2f}",
                        delta=f"{float(row_dict['discount_pct']):.0f}% off (${msrp:.0f} MSRP)"
                    )

                    # Price History & Cheaper Active Copies Summary Badge
                    if past_matches:
                        p_prices = [float(m.get("final_price") or 0) for m in past_matches]
                        min_won = min(p_prices)
                        avg_won = sum(p_prices) / len(p_prices)
                        if cur_bid <= min_won:
                            verdict = "🟢 **Below All-Time Low!**"
                        elif cur_bid <= avg_won:
                            verdict = "🟢 **Below Avg Winning Price**"
                        else:
                            verdict = f"🔴 **Above Avg (${avg_won:.0f})**"
                        st.caption(f"🏆 **Cheapest Won ({cond_tier}):** **${min_won:.2f}** · Avg ${avg_won:.0f} *({len(past_matches)} sold)*\n\n{verdict}")
                    else:
                        st.caption(f"📊 No past '{cond_tier}' sales in lake yet")

                    if cheaper_in_batch:
                        cheapest_open = min(float(c.get("final_price") or 0) for c in cheaper_in_batch)
                        st.markdown(f"💡 **:green[{len(cheaper_in_batch)} Cheaper Open (From ${cheapest_open:.2f})!]**")

                    btn_label = f"🔍 History ({len(past_matches)}) & Active Copies"
                    if st.button(btn_label, key=f"btn_hist_{auction_id}", use_container_width=True):
                        show_item_history_dialog(row_dict)

                st.markdown("---")

        # Inject real-time ticking countdown JavaScript
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
                                badge.innerHTML = "🔴 <b>POTENTIALLY CLOSED</b> <span style='font-size:10px; opacity:0.8;'>(anti-snipe check)</span>";
                                badge.style.backgroundColor = "#334155";
                                badge.style.color = "#cbd5e1";
                                badge.style.borderColor = "#64748b";
                            } else {
                                const mins = Math.floor(totalSec / 60);
                                const secs = totalSec % 60;
                                const secStr = secs < 10 ? '0' + secs : secs;
                                
                                if (totalSec < 180) {
                                    badge.innerHTML = "🔥 <b>" + mins + "m " + secStr + "s left (SNIPE NOW!)</b>";
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
        df = con.execute(sql).df()
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
        df = con.execute(sql).df()
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
        df = con.execute(sql).df()
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
        df = con.execute(sql).df()
        st.dataframe(df, use_container_width=True)


# --- TAB 3: SQL STUDIO ---
elif view_mode == "💻 SQL Studio":
    st.title("💻 Interactive DuckDB SQL Studio")
    st.caption("Run custom analytical SQL queries directly against `fact_auctions` and `dim_products` on Cloudflare R2.")

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

    custom_sql = st.text_area("SQL Query Editor", value=default_sql, height=200)

    if st.button("▶ Execute SQL Query", type="primary"):
        with st.spinner("Executing query on DuckDB..."):
            try:
                res_df = con.execute(custom_sql).df()
                st.success(f"Returned {len(res_df)} rows.")
                st.dataframe(res_df, use_container_width=True)
            except Exception as e:
                st.error(f"SQL Error: {e}")

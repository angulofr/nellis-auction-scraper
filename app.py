#!/usr/bin/env python3
"""
Nellis Auction Data Lake & Deal Finder App
Cloud-ready Streamlit application queryable from phone, tablet, or desktop.
"""

import json
import os
import time
import urllib.request
import duckdb
import pandas as pd
import streamlit as st

# Page setup
st.set_page_config(
    page_title="Nellis Auction Lake",
    page_icon="🔨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cloudflare R2 Secrets (Reads from Streamlit Cloud secrets or Environment Variables)
R2_ACCESS_KEY_ID = st.secrets.get("R2_ACCESS_KEY_ID", os.environ.get("R2_ACCESS_KEY_ID", ""))
R2_SECRET_ACCESS_KEY = st.secrets.get("R2_SECRET_ACCESS_KEY", os.environ.get("R2_SECRET_ACCESS_KEY", ""))
R2_ENDPOINT_URL = st.secrets.get("R2_ENDPOINT_URL", os.environ.get("R2_ENDPOINT_URL", "https://965a5411101d1521c551d99d9421a145.r2.cloudflarestorage.com"))
R2_BUCKET_NAME = st.secrets.get("R2_BUCKET_NAME", os.environ.get("R2_BUCKET_NAME", "nellis-auction-lake"))


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
    # Register Lake Views
    con.execute(f"CREATE OR REPLACE VIEW fact_auctions AS SELECT * FROM read_parquet('s3://{R2_BUCKET_NAME}/fact_auctions/*/*/*.parquet');")
    con.execute(f"CREATE OR REPLACE VIEW dim_products AS SELECT * FROM read_parquet('s3://{R2_BUCKET_NAME}/dim_products/products_catalog.parquet');")
    return con


con = get_duckdb_con()


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


@st.cache_data(ttl=5)
def fetch_live_nellis_sniper(page_size=120):
    """
    Fetch real-time active lots ending soonest directly from Nellis Auction's API.
    Guarantees up-to-the-second current winning bids and anti-snipe countdowns.
    """
    try:
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
        # Set Phoenix/Mesa shopping location
        req_loc = urllib.request.Request(
            'https://www.nellisauction.com/change-shopping-location',
            data=b'shoppingLocationId=2',
            headers={'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/x-www-form-urlencoded'}
        )
        opener.open(req_loc, timeout=10)

        url = f'https://www.nellisauction.com/search.data?sortBy=time_remaining_asc&_p1=s:{page_size},n:1'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with opener.open(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        now_ms = int(time.time() * 1000)
        items = []
        for i, el in enumerate(data):
            if el == 'products' and i + 1 < len(data):
                for idx in data[i + 1]:
                    p = unflatten_val(idx, data)
                    if not p or p.get('isClosed', False):
                        continue
                    
                    raw_close = p.get('closeTime')
                    if isinstance(raw_close, list) and raw_close:
                        raw_close = raw_close[0]
                    try:
                        close_ms = int(raw_close)
                    except (ValueError, TypeError):
                        continue

                    diff_sec = (close_ms - now_ms) / 1000.0
                    
                    loc_dict = p.get('location') if isinstance(p.get('location'), dict) else {}
                    loc_name = loc_dict.get('name', 'Phoenix')
                    retail = float(p.get('retailPrice') or 0.0)
                    bid = float(p.get('currentPrice') or 0.0)
                    discount_pct = ((retail - bid) / retail * 100.0) if retail > 0 and bid <= retail else 0.0
                    
                    photos = p.get('photos') or []
                    photo_url = photos[0].get('url', '') if photos and isinstance(photos[0], dict) else ''

                    # Clean condition parsing from grade object
                    grade_obj = p.get('grade')
                    condition_str = "Standard"
                    if isinstance(grade_obj, dict):
                        c_type = grade_obj.get('conditionType', {})
                        c_desc = c_type.get('description', '') if isinstance(c_type, dict) else ''
                        rating = grade_obj.get('rating')
                        if c_desc in ['New', 'Brand New'] or rating == 5:
                            condition_str = "Brand New (5/5)"
                        elif c_desc:
                            condition_str = f"{c_desc} ({rating}/5)" if rating else c_desc
                        elif rating:
                            condition_str = f"Rated {rating}/5"
                    elif isinstance(grade_obj, str) and grade_obj:
                        condition_str = grade_obj

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
                        'photo_url': photo_url,
                        'closed_at': pd.to_datetime(close_ms, unit='ms', utc=True).tz_convert('America/Phoenix')
                    })
                break
        return pd.DataFrame(items)
    except Exception as e:
        print(f"Error fetching live sniper items: {e}")
        return pd.DataFrame()


# --- SIDEBAR CONTROLS ---
st.sidebar.title("🔨 Nellis Auction Lake")
st.sidebar.caption("Phoenix & Mesa, AZ Analytics")

# Quick KPI Metrics in Sidebar
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
    st.title("🎯 Nellis Auction Deal Finder & Sniper")
    st.caption("Live & historical lots from Phoenix & Mesa warehouses with instant countdowns and real photos")

    col_top1, col_top2 = st.columns([4, 1.2])
    with col_top1:
        st.markdown("Search active lots, filter by ending time, and snipe underpriced items before the hammer closes.")
    with col_top2:
        if st.button("🔄 Refresh Live Data", use_container_width=True, type="primary"):
            st.cache_data.clear()
            st.rerun()

    col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 2])
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
                "All Active Lots (Data Lake)",
                "📅 Ending Today (< 24 hrs)"
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

    # Determine whether to use Real-Time Live API or Data Lake
    is_live_sniper = time_filter in [
        "🔥 Ending in < 5 mins (Live)",
        "⚡ Ending in < 15 mins (Live)",
        "⏳ Ending in < 30 mins (Live)",
        "🕒 Ending in < 1 hour (Live)"
    ]

    results_df = pd.DataFrame()

    if is_live_sniper:
        st.info("🟢 **Real-Time Live API Connected** — Querying Nellis Auction directly for up-to-the-second bids & anti-snipe extensions.")
        with st.spinner("Fetching live active lots from Nellis Auction..."):
            live_df = fetch_live_nellis_sniper(page_size=120)
            if not live_df.empty:
                # Apply in-memory filters
                filtered = live_df[
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
                
                # Filter by max diff_sec
                max_sec = {
                    "🔥 Ending in < 5 mins (Live)": 300,
                    "⚡ Ending in < 15 mins (Live)": 900,
                    "⏳ Ending in < 30 mins (Live)": 1800,
                    "🕒 Ending in < 1 hour (Live)": 3600
                }.get(time_filter, 3600)
                filtered = filtered[filtered['diff_sec'] <= max_sec]

                # Sorting
                if sort_by == "Highest Discount (%)":
                    filtered = filtered.sort_values(by="discount_pct", ascending=False)
                elif sort_by == "Lowest Final Bid ($)":
                    filtered = filtered.sort_values(by="final_price", ascending=True)
                elif sort_by == "Highest Retail MSRP ($)":
                    filtered = filtered.sort_values(by="retail_price", ascending=False)
                else:
                    filtered = filtered.sort_values(by="diff_sec", ascending=True)

                results_df = filtered.head(50)

    else:
        # Query Cloudflare R2 Data Lake via DuckDB
        where_parts = [f"f.retail_price >= {min_retail}", f"f.final_price <= {max_bid}"]
        if loc_filter != "All":
            where_parts.append(f"f.location = '{loc_filter}'")
        if cond_filter != "All":
            where_parts.append(f"f.condition = '{cond_filter}'")
        if search_kw.strip():
            kw = search_kw.strip().replace("'", "''")
            where_parts.append(f"(p.title ILIKE '%{kw}%' OR f.damage_notes ILIKE '%{kw}%')")

        if time_filter == "📅 Ending Today (< 24 hrs)":
            where_parts.append("f.closed_at >= now() AND f.closed_at <= now() + INTERVAL 24 HOUR")

        where_sql = " AND ".join(where_parts)
        sort_sql = {
            "⏳ Closing Soonest (Sniper)": "f.closed_at ASC",
            "Highest Discount (%)": "f.discount_ratio ASC",
            "Lowest Final Bid ($)": "f.final_price ASC",
            "Highest Retail MSRP ($)": "f.retail_price DESC"
        }.get(sort_by, "f.closed_at ASC")

        query = f"""
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
                f.photo_url
            FROM fact_auctions f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE {where_sql}
            ORDER BY {sort_sql}
            LIMIT 50;
        """

        with st.spinner("Querying Cloudflare R2 Data Lake via DuckDB..."):
            try:
                results_df = con.execute(query).df()
            except Exception as e:
                st.error(f"Error querying lake: {e}")

    # Render Results Cards with Pictures
    st.write(f"**Found {len(results_df)} matching lots:**")

    if results_df.empty:
        st.info("No items match your filter criteria. Try expanding the time filter or adjusting the price sliders.")
    else:
        for idx, row in results_df.iterrows():
            item_url = f"https://www.nellisauction.com/p/item/{row['auction_id']}"
            
            # Calculate live time remaining
            time_badge = ""
            try:
                if 'diff_sec' in row and pd.notnull(row['diff_sec']):
                    diff_sec = float(row['diff_sec'])
                elif pd.notnull(row['closed_at']):
                    closed_dt = row['closed_at']
                    now_tz = pd.Timestamp.now(tz=closed_dt.tzinfo if hasattr(closed_dt, 'tzinfo') and closed_dt.tzinfo else None)
                    diff_sec = (closed_dt - now_tz).total_seconds()
                else:
                    diff_sec = -1

                if diff_sec <= 0:
                    time_badge = "🔴 **CLOSED / SOLD**"
                elif diff_sec < 180: # < 3 mins
                    mins = int(diff_sec // 60)
                    secs = int(diff_sec % 60)
                    time_badge = f"🔥 **{mins}m {secs}s left (SNIPE NOW!)**"
                elif diff_sec < 600: # < 10 mins
                    mins = int(diff_sec // 60)
                    secs = int(diff_sec % 60)
                    time_badge = f"⚡ **{mins}m {secs}s left**"
                elif diff_sec < 3600: # < 1 hour
                    mins = int(diff_sec // 60)
                    time_badge = f"⏳ **{mins}m left**"
                else:
                    hours = int(diff_sec // 3600)
                    mins = int((diff_sec % 3600) // 60)
                    time_badge = f"🕒 **{hours}h {mins}m left**"
            except Exception:
                time_badge = f"Closes: {str(row['closed_at'])[:16]}"

            with st.container():
                c_img, c_card1, c_card2, c_card3 = st.columns([1.5, 4.5, 2.5, 2.5])
                
                with c_img:
                    photo = row.get("photo_url")
                    if photo and str(photo) != "nan" and str(photo).strip():
                        st.image(photo, use_container_width=True)
                    else:
                        st.caption("📷 No photo")

                with c_card1:
                    st.markdown(f"**[{row['title']}]({item_url})**")
                    notes = row.get('damage_notes')
                    if notes and str(notes) != "nan" and str(notes).strip():
                        st.caption(f"⚠️ **Notes:** {notes}")
                    st.markdown(f"[👉 **Place Bid on Nellis Auction**]({item_url})")

                with c_card2:
                    raw_cond = str(row.get('condition', 'Standard'))
                    cond_display = raw_cond
                    if raw_cond.startswith("{"):
                        import ast
                        try:
                            g = ast.literal_eval(raw_cond)
                            c_type = g.get('conditionType', {})
                            c_desc = c_type.get('description', '') if isinstance(c_type, dict) else ''
                            rat = g.get('rating')
                            if c_desc in ['New', 'Brand New'] or rat == 5:
                                cond_display = "Brand New (5/5)"
                            elif c_desc:
                                cond_display = f"{c_desc} ({rat}/5)" if rat else c_desc
                            elif rat:
                                cond_display = f"Rated {rat}/5"
                        except Exception:
                            cond_display = "Standard"

                    cond_color = "🟢" if any(w in cond_display.lower() for w in ["brand new", "new", "5/5"]) else "🟡"
                    st.write(f"{cond_color} **{cond_display}** · 📍 {row['location']}")
                    st.markdown(f"{time_badge}")
                    st.caption(f"Bids: **{row['bid_count']}** · Closes: {str(row['closed_at'])[:19]}")

                with c_card3:
                    st.metric(
                        label="Current Bid",
                        value=f"${float(row['final_price']):.2f}",
                        delta=f"{float(row['discount_pct']):.0f}% off (${float(row['retail_price']):.0f} MSRP)"
                    )
                st.markdown("---")


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
        st.subheader("Temporal Sweet Spots (Arizona MST Time)")
        st.caption("Identifies closing time windows where competition is lowest.")
        df = con.execute(sql).df()
        st.dataframe(df, use_container_width=True)

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
                f.damage_notes
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
        st.caption("High-value retail ($150+) lots in clean condition that sold or closed with minimal bidder competition.")
        df = con.execute(sql).df()
        st.dataframe(df, use_container_width=True)


# --- TAB 3: SQL STUDIO ---
elif view_mode == "💻 SQL Studio":
    st.title("💻 DuckDB Cloud SQL Studio")
    st.caption("Execute custom analytical SQL queries directly over Cloudflare R2 Parquet files.")

    default_sql = """SELECT 
    f.location,
    f.condition,
    COUNT(*) AS total_lots,
    ROUND(AVG(f.retail_price), 2) AS avg_retail,
    ROUND(AVG(f.final_price), 2) AS avg_final_bid,
    ROUND(AVG(f.discount_ratio), 3) AS avg_discount_ratio
FROM fact_auctions f
GROUP BY f.location, f.condition
ORDER BY total_lots DESC;"""

    user_sql = st.text_area("SQL Statement (DuckDB httpfs)", value=default_sql, height=180)

    if st.button("🚀 Run Query", type="primary"):
        try:
            with st.spinner("Executing over Cloudflare R2..."):
                query_df = con.execute(user_sql).df()
                st.success(f"Returned {len(query_df)} rows")
                st.dataframe(query_df, use_container_width=True)

                # CSV Download button
                csv_bytes = query_df.to_csv(index=False).encode("utf-8")
                st.download_button("📥 Download Results as CSV", csv_bytes, "query_results.csv", "text/csv")
        except Exception as e:
            st.error(f"SQL Error: {e}")

#!/usr/bin/env python3
"""
Nellis Auction Data Lake & Deal Finder App
Cloud-ready Streamlit application queryable from phone, tablet, or desktop.
"""

import os
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

# --- PIN PROTECTION GATE ---
# PIN is read strictly from Streamlit Cloud Secrets (never stored in public code or repository)
APP_PIN = str(st.secrets.get("APP_PIN", os.environ.get("APP_PIN", ""))).strip()

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown("<br><br>", unsafe_allow_html=True)
    _, pin_col, _ = st.columns([1, 2, 1])
    with pin_col:
        st.title("🔒 Nellis Auction Lake")
        st.caption("Phoenix & Mesa, AZ Liquidation Analytics")
        entered_pin = st.text_input("Enter PIN to access", type="password", max_chars=16, placeholder="PIN")
        if st.button("Unlock Dashboard", use_container_width=True, type="primary"):
            if APP_PIN and entered_pin.strip() == APP_PIN:
                st.session_state.authenticated = True
                st.rerun()
            elif not APP_PIN:
                st.error("PIN not configured. Please add APP_PIN in Streamlit Cloud Secrets.")
            else:
                st.error("Incorrect PIN. Please try again.")
    st.stop()

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
st.sidebar.caption("Cloud Storage: Cloudflare R2 (S3)\nQuery Engine: DuckDB 2.0 (httpfs)")
if st.sidebar.button("🔒 Lock Dashboard", use_container_width=True):
    st.session_state.authenticated = False
    st.rerun()


# --- TAB 1: DEAL FINDER ---
if view_mode == "🎯 Deal Finder":
    st.title("🎯 Nellis Auction Deal Finder & Sniper")
    st.caption("Live & completed lots from Phoenix & Mesa warehouses with instant countdowns")

    col_top1, col_top2 = st.columns([4, 1])
    with col_top1:
        st.markdown("Find liquidation deals, sort by urgency, or filter by items ending in the next few minutes.")
    with col_top2:
        if st.button("🔄 Refresh Deals", use_container_width=True):
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
                "All Active Lots",
                "🔥 Ending in < 5 mins (Sniper)",
                "⚡ Ending in < 15 mins",
                "⏳ Ending in < 30 mins",
                "🕒 Ending in < 1 hour",
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
        min_retail = st.slider("Minimum Retail MSRP ($)", min_value=0, max_value=2000, value=50, step=25)
    with col_p2:
        max_bid = st.slider("Maximum Current / Winning Bid ($)", min_value=1, max_value=1000, value=150, step=10)

    # Build SQL Query
    where_parts = [f"f.retail_price >= {min_retail}", f"f.final_price <= {max_bid}"]
    if loc_filter != "All":
        where_parts.append(f"f.location = '{loc_filter}'")
    if cond_filter != "All":
        where_parts.append(f"f.condition = '{cond_filter}'")
    if search_kw.strip():
        kw = search_kw.strip().replace("'", "''")
        where_parts.append(f"(p.title ILIKE '%{kw}%' OR f.damage_notes ILIKE '%{kw}%')")

    # Time Remaining Filter logic
    if time_filter == "🔥 Ending in < 5 mins (Sniper)":
        where_parts.append("f.closed_at >= now() AND f.closed_at <= now() + INTERVAL 5 MINUTE")
    elif time_filter == "⚡ Ending in < 15 mins":
        where_parts.append("f.closed_at >= now() AND f.closed_at <= now() + INTERVAL 15 MINUTE")
    elif time_filter == "⏳ Ending in < 30 mins":
        where_parts.append("f.closed_at >= now() AND f.closed_at <= now() + INTERVAL 30 MINUTE")
    elif time_filter == "🕒 Ending in < 1 hour":
        where_parts.append("f.closed_at >= now() AND f.closed_at <= now() + INTERVAL 1 HOUR")
    elif time_filter == "📅 Ending Today (< 24 hrs)":
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
            f.closed_at
        FROM fact_auctions f
        JOIN dim_products p ON f.product_id = p.product_id
        WHERE {where_sql}
        ORDER BY {sort_sql}
        LIMIT 50;
    """

    with st.spinner("Querying Cloudflare R2 Data Lake via DuckDB..."):
        try:
            results_df = con.execute(query).df()
            st.write(f"**Found {len(results_df)} matching lots:**")

            if results_df.empty:
                st.info("No items match your filter criteria. If looking for items ending soon, try expanding the time filter to 15m or 1h.")
            else:
                for idx, row in results_df.iterrows():
                    item_url = f"https://www.nellisauction.com/p/item/{row['auction_id']}"
                    
                    # Calculate live time remaining
                    time_badge = ""
                    try:
                        closed_dt = row['closed_at']
                        if pd.notnull(closed_dt):
                            now_tz = pd.Timestamp.now(tz=closed_dt.tzinfo if closed_dt.tzinfo else None)
                            diff_sec = (closed_dt - now_tz).total_seconds()
                            if diff_sec <= 0:
                                time_badge = "🔴 **CLOSED**"
                            elif diff_sec < 300: # < 5 mins
                                mins = int(diff_sec // 60)
                                secs = int(diff_sec % 60)
                                time_badge = f"🔥 **{mins}m {secs}s left** (SNIPE!)"
                            elif diff_sec < 900: # < 15 mins
                                mins = int(diff_sec // 60)
                                time_badge = f"⚡ **{mins}m left**"
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
                        c_card1, c_card2, c_card3 = st.columns([5, 3, 2])
                        with c_card1:
                            st.markdown(f"**[{row['title']}]({item_url})**")
                            notes = row['damage_notes']
                            if notes and str(notes) != "nan" and str(notes).strip():
                                st.caption(f"⚠️ Notes: {notes}")
                            st.markdown(f"[👉 Bid on Nellis Auction]({item_url})")
                        with c_card2:
                            cond_color = "🟢" if row['condition'] == "Brand New" else "🟡"
                            st.write(f"{cond_color} **{row['condition']}** · 📍 {row['location']}")
                            st.markdown(f"{time_badge}")
                            st.caption(f"Bids: {row['bid_count']} · Closes: {str(row['closed_at'])[:19]}")
                        with c_card3:
                            st.metric(
                                label="Current Bid",
                                value=f"${row['final_price']:.2f}",
                                delta=f"{row['discount_pct']:.0f}% off (${row['retail_price']:.0f} MSRP)"
                            )
                        st.markdown("---")
        except Exception as e:
            st.error(f"Error executing query: {e}")


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

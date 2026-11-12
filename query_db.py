#!/usr/bin/env python3
"""
Nellis Auction Database Query Tool
Easily inspect, filter, and search scraped items in nellis_auctions.db.
"""

import argparse
import os
import sqlite3
import sys

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nellis_auctions.db")


def print_table(rows, headers):
    """Format and print rows nicely in console."""
    if not rows:
        print("No items found matching your criteria.")
        return

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            s = str(val) if val is not None else ""
            widths[i] = max(widths[i], min(len(s), 60))

    # Print header
    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("-" * len(header_line))

    # Print rows
    for row in rows:
        line_parts = []
        for i, val in enumerate(row):
            s = str(val) if val is not None else ""
            if len(s) > 60:
                s = s[:57] + "..."
            line_parts.append(s.ljust(widths[i]))
        print(" | ".join(line_parts))
    print(f"\n[+] Total results: {len(rows)}")


def main():
    parser = argparse.ArgumentParser(description="Query the Nellis Auction local database.")
    parser.add_argument("--search", "-s", type=str, help="Search keywords in title or inspector notes")
    parser.add_argument("--deals", "-d", action="store_true", help="Show top discounts (retail > $100, discount > 80%)")
    parser.add_argument("--new-only", "-n", action="store_true", help="Only show Brand New, Functional, No Damage items")
    parser.add_argument("--closing-soon", "-c", action="store_true", help="Show items closing next in Phoenix/Mesa")
    parser.add_argument("--location", "-l", type=str, choices=["Phoenix", "Mesa", "all"], default="all", help="Filter by warehouse location")
    parser.add_argument("--min-retail", type=float, default=0.0, help="Minimum retail price ($)")
    parser.add_argument("--max-bid", type=float, default=None, help="Maximum winning/current bid ($)")
    parser.add_argument("--limit", type=int, default=15, help="Number of items to return (default: 15)")
    parser.add_argument("--stats", action="store_true", help="Display overview summary statistics of the database")
    parser.add_argument("--sql", type=str, help="Execute a raw custom SQL query")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"[!] Database file not found at: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if args.stats:
        print("=" * 60)
        print("Nellis Auction Database Overview")
        print("=" * 60)
        cursor.execute("SELECT count(*) FROM auction_items")
        print(f"Total Items Stored     : {cursor.fetchone()[0]:,}")

        cursor.execute("SELECT location_name, count(*) FROM auction_items GROUP BY location_name")
        for loc, count in cursor.fetchall():
            print(f"  • {loc.ljust(20)}: {count:,} items")

        cursor.execute("SELECT condition_type, count(*) FROM auction_items GROUP BY condition_type ORDER BY count(*) DESC LIMIT 5")
        print("\nConditions:")
        for cond, count in cursor.fetchall():
            c_name = cond if cond else "Unspecified"
            print(f"  • {c_name.ljust(20)}: {count:,} items")

        cursor.execute("SELECT is_closed, count(*) FROM auction_items GROUP BY is_closed")
        print("\nAuction Status:")
        for is_closed, count in cursor.fetchall():
            lbl = "Finalized / Closed" if is_closed == 1 else "Active / Open"
            print(f"  • {lbl.ljust(20)}: {count:,} items")

        cursor.execute("SELECT ROUND(SUM(estimated_retail_price), 2) FROM auction_items")
        total_retail = cursor.fetchone()[0] or 0.0
        print(f"\nTotal Estimated Value  : ${total_retail:,.2f}")
        print("=" * 60)
        conn.close()
        return

    if args.sql:
        print(f"Executing: {args.sql}\n")
        cursor.execute(args.sql)
        headers = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        print_table(rows, headers)
        conn.close()
        return

    # Build dynamic query
    conditions = []
    params = []

    if args.location and args.location.lower() != "all":
        conditions.append("location_name = ?")
        params.append(args.location)

    if args.min_retail > 0:
        conditions.append("estimated_retail_price >= ?")
        params.append(args.min_retail)

    if args.max_bid is not None:
        conditions.append("winning_bid_amount <= ?")
        params.append(args.max_bid)

    if args.new_only:
        conditions.append("condition_type = 'New' AND is_functional = 'Yes' AND damage_type = 'None' AND missing_parts = 'No'")

    if args.deals:
        conditions.append("estimated_retail_price >= 100 AND discount_pct >= 80")

    if args.search:
        conditions.append("(title LIKE ? OR inspector_notes LIKE ?)")
        kw = f"%{args.search}%"
        params.extend([kw, kw])

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    order_clause = " ORDER BY close_time_ms ASC" if args.closing_soon else " ORDER BY discount_pct DESC"

    query = f"""
        SELECT 
            id,
            title,
            condition_type,
            winning_bid_amount as current_bid,
            estimated_retail_price as retail,
            discount_pct,
            closing_date,
            location_name,
            inspector_notes
        FROM auction_items
        {where_clause}
        {order_clause}
        LIMIT {args.limit}
    """

    cursor.execute(query, params)
    headers = ["ID", "Title", "Condition", "Bid ($)", "Retail ($)", "Discount %", "Closing Date", "Location", "Notes"]
    rows = cursor.fetchall()
    print_table(rows, headers)

    conn.close()


if __name__ == "__main__":
    main()

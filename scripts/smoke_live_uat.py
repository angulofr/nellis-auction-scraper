#!/usr/bin/env python3
"""
Read-Only Live Production & Cloudflare R2 Sanity Probe (`TC-SMOKE-001` .. `TC-SMOKE-003`).
Strictly read-only (never mutates R2 partitions or live Nellis Auction state).

Usage:
  python scripts/smoke_live_uat.py --live
  python scripts/smoke_live_uat.py --r2
  python scripts/smoke_live_uat.py --live --r2
"""

import argparse
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.query_lake import PRESET_QUERIES, get_duckdb_lake_conn
from src.settlement import _build_pooled_session, build_bid_analytics, fetch_closed_lot_result


def check_live_nellis_api() -> None:
    """Verify live `/search.data` cookie flow and `/p/item/{id}.data` contract."""
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
    req_loc = urllib.request.Request(
        "https://www.nellisauction.com/change-shopping-location",
        data=b"shoppingLocationId=2",
        headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/x-www-form-urlencoded"},
    )
    with opener.open(req_loc, timeout=12) as resp:
        assert resp.status in (200, 302), f"Unexpected location init status: {resp.status}"

    qs = urllib.parse.urlencode({"sortBy": "time_remaining_asc", "_p1": "s:40,n:0"})
    req_search = urllib.request.Request(
        f"https://www.nellisauction.com/search.data?{qs}",
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with opener.open(req_search, timeout=12) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    assert isinstance(data, list) and "products" in data, "Live /search.data missing 'products' turbo-stream key"
    print(f"[✓] Live /search.data handshake OK (turbo-stream array length={len(data)})")

    sess = _build_pooled_session(4)
    try:
        for aid, expected_min_bids in ((138958727, 8), (138958730, 31)):
            res = fetch_closed_lot_result(aid, session=sess)
            assert res and res.get("is_closed") is True, f"Failed to parse closed item #{aid}"
            bid_rows, summary = build_bid_analytics(res, product_id="smoke_probe")
            assert len(bid_rows) >= expected_min_bids, f"Unexpected bidHistory count for #{aid}: {len(bid_rows)}"
            assert summary.get("bids_captured") is True
            print(
                f"[✓] Live .data #{aid}: status={res['market_status']}, price=${res['final_price']:.2f}, "
                f"bid_rows={len(bid_rows)}, unique_bidders={summary['unique_bidders']}, "
                f"was_extended={summary['was_extended']}"
            )
    finally:
        sess.close()


def check_r2_lake_readonly() -> None:
    """Verify read-only Cloudflare R2 Parquet integrity, zero duplicates, and bid-timing preset."""
    con = get_duckdb_lake_conn(use_local=False)
    try:
        dup_auctions = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT auction_id FROM fact_auctions GROUP BY auction_id HAVING COUNT(*) > 1
            );
        """).fetchone()[0]
        assert dup_auctions == 0, f"Found {dup_auctions} duplicate auction_id keys in fact_auctions view!"

        dup_bids = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT auction_id, bid_seq FROM fact_bids GROUP BY auction_id, bid_seq HAVING COUNT(*) > 1
            );
        """).fetchone()[0]
        assert dup_bids == 0, f"Found {dup_bids} duplicate (auction_id, bid_seq) keys in fact_bids view!"

        total_auctions, captured_bids = con.execute("""
            SELECT COUNT(*), SUM(CASE WHEN COALESCE(bids_captured, FALSE) THEN 1 ELSE 0 END)
            FROM fact_auctions;
        """).fetchone()
        total_bids = con.execute("SELECT COUNT(*) FROM fact_bids;").fetchone()[0]
        df_timing = con.execute(PRESET_QUERIES["bid-timing"]["sql"]).fetchdf()

        print(
            f"[✓] Read-Only R2 Lake OK: fact_auctions={total_auctions:,} (bids_captured={captured_bids:,}), "
            f"fact_bids={total_bids:,}, timing_buckets={len(df_timing)}"
        )
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(description="Read-Only QA/UAT Smoke Verification for Nellis Auction Platform")
    parser.add_argument("--live", action="store_true", help="Verify live nellisauction.com /search.data & .data contracts")
    parser.add_argument("--r2", action="store_true", help="Verify read-only Cloudflare R2 Parquet views & dedup keys")
    args = parser.parse_args()

    if not args.live and not args.r2:
        args.live = True

    if args.live:
        check_live_nellis_api()
    if args.r2:
        check_r2_lake_readonly()


if __name__ == "__main__":
    main()

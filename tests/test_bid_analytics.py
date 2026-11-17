import os
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.transform import transform_records
from src.r2_loader import (
    enforce_fact_auctions_schema,
    enforce_fact_bids_schema,
    write_fact_auctions,
    write_fact_bids,
    merge_dim_products,
)
from src.settlement import (
    parse_bid_dollar_amount,
    parse_bidder_num,
    classify_bid_time_bucket,
    build_bid_analytics,
)
from src.query_lake import get_duckdb_lake_conn, PRESET_QUERIES


class TestBidAnalyticsPipeline(unittest.TestCase):
    def test_parsers_and_buckets(self):
        self.assertEqual(parse_bid_dollar_amount("$1,234.50"), 1234.50)
        self.assertEqual(parse_bid_dollar_amount(9), 9.0)
        self.assertEqual(parse_bid_dollar_amount(None), 0.0)
        self.assertEqual(parse_bidder_num("Bidder #12"), 12)
        self.assertEqual(parse_bidder_num(""), 0)

        self.assertEqual(classify_bid_time_bucket(10.0, -5.0), "Overtime (Anti-Snipe)")
        self.assertEqual(classify_bid_time_bucket(45.0, 45.0), "Last 60s")
        self.assertEqual(classify_bid_time_bucket(180.0, 180.0), "1m - 5m left")
        self.assertEqual(classify_bid_time_bucket(600.0, 600.0), "5m - 15m left")
        self.assertEqual(classify_bid_time_bucket(2000.0, 2000.0), "15m - 1h left")
        self.assertEqual(classify_bid_time_bucket(7200.0, 7200.0), "1h - 6h left")
        self.assertEqual(classify_bid_time_bucket(30000.0, 30000.0), "Early (> 6h left)")

    def test_build_bid_analytics_proxy_bids_and_extension(self):
        # Reproduces Nellis proxy-bid timestamp behavior seen on item #138958727:
        # Bidder #5 placed a max proxy of $8 early, and later $7 outbid entries appear below it.
        init_close_ms = 1_794_542_700_000
        close_ms = init_close_ms + 17_289
        open_ms = init_close_ms - 86_400_000

        res = {
            "auction_id": 138958727,
            "open_ms": open_ms,
            "initial_close_ms": init_close_ms,
            "close_ms": close_ms,
            "final_price": 9.0,
            "bid_count": 4,
            "bid_history": [
                {"name": "Bidder #7", "amount": "$9", "time": close_ms - 30_000, "type": "Winning"},
                {"name": "Bidder #6", "amount": "$7", "time": close_ms - 120_000, "type": "Outbid"},
                {"name": "Bidder #5", "amount": "$8", "time": close_ms - 7_200_000, "type": "Outbid"},
                {"name": "Bidder #1", "amount": "$1", "time": open_ms + 60_000, "type": "Outbid"},
            ],
        }

        bid_rows, summary = build_bid_analytics(res, product_id="prod_abc", scraped_date="2026-11-12")
        self.assertEqual(len(bid_rows), 4)
        self.assertEqual([r["bid_seq"] for r in bid_rows], [1, 2, 3, 4])
        self.assertTrue(summary["was_extended"])
        self.assertEqual(summary["extension_seconds"], 17)
        self.assertEqual(summary["unique_bidders"], 4)
        self.assertEqual(summary["first_bid_price"], 1.0)
        # Running high-water price at cutoffs MUST reflect $8 proxy placed 2h before close, NOT $7
        self.assertEqual(summary["price_at_1h_left"], 8.0)
        self.assertEqual(summary["price_at_5m_left"], 8.0)
        self.assertEqual(summary["price_at_1m_left"], 8.0)
        self.assertEqual(summary["bids_last_5m"], 2)
        self.assertEqual(summary["bids_last_60s"], 1)
        self.assertAlmostEqual(summary["winning_bid_sec_before_close"], 30.0, places=2)
        self.assertTrue(summary["bids_captured"])

        win_row = bid_rows[-1]
        self.assertEqual(win_row["bidder_label"], "Bidder #7")
        self.assertEqual(win_row["bidder_num"], 7)
        self.assertTrue(win_row["is_winning_bid"])
        # Placed 12.711s before initial_close (triggering +17.289s anti-snipe to 30s left), so still 'Last 60s'
        self.assertEqual(win_row["time_bucket"], "Last 60s")
        self.assertEqual(win_row["bid_increment"], 1.0)
        self.assertEqual(win_row["pct_of_final_price"], 1.0)

        # Also verify true Overtime bid (placed AFTER scheduled initial_close_ms)
        overtime_res = {
            "auction_id": 138958799,
            "open_ms": open_ms,
            "initial_close_ms": init_close_ms,
            "close_ms": init_close_ms + 35_000,
            "final_price": 12.0,
            "bid_count": 2,
            "bid_history": [
                {"name": "Bidder #2", "amount": "$12", "time": init_close_ms + 5_000, "type": "Winning"},
                {"name": "Bidder #1", "amount": "$10", "time": init_close_ms - 10_000, "type": "Outbid"},
            ],
        }
        ot_rows, ot_summary = build_bid_analytics(overtime_res, product_id="prod_ot", scraped_date="2026-11-12")
        self.assertEqual(ot_summary["extension_seconds"], 35)
        self.assertEqual(ot_rows[0]["time_bucket"], "Last 60s")
        self.assertEqual(ot_rows[1]["time_bucket"], "Overtime (Anti-Snipe)")

    def test_build_bid_analytics_zero_bids(self):
        """TC-UNIT-021: Zero-bid / empty bidHistory settles cleanly with empty bid_rows."""
        close_ms = 1_794_542_700_000
        res_zero = {
            "auction_id": 138959999,
            "open_ms": close_ms - 86_400_000,
            "initial_close_ms": close_ms,
            "close_ms": close_ms,
            "final_price": 0.0,
            "bid_count": 0,
            "bid_history": [],
        }
        bid_rows, summary = build_bid_analytics(res_zero, product_id="prod_zero", scraped_date="2026-11-12")
        self.assertEqual(bid_rows, [])
        self.assertEqual(summary["unique_bidders"], 0)
        self.assertEqual(summary["bids_last_5m"], 0)
        self.assertEqual(summary["bids_last_60s"], 0)
        self.assertIsNone(summary["first_bid_price"])
        self.assertTrue(summary["bids_captured"])

    def test_transform_records_timing_fields(self):
        init_close_ms = 1_794_532_860_000
        raw_items = [
            {
                "id": 990001,
                "title": "DeWalt 20V Cordless Impact Driver",
                "retailPrice": 159.99,
                "currentPrice": 0.0,
                "bidCount": 0,
                "bidderCount": 0,
                "openTime": init_close_ms - 86_400_000,
                "initialCloseTime": init_close_ms,
                "closeTime": init_close_ms,
                "condition": "Brand New",
                "locationName": "Phoenix",
                "inventoryNumber": "PHX-123",
            }
        ]
        dim_records, fact_records = transform_records(raw_items, scrape_date="2026-11-12")
        self.assertEqual(len(dim_records), 1)
        self.assertEqual(len(fact_records), 1)
        fact = fact_records[0]
        self.assertFalse(fact["was_extended"])
        self.assertEqual(fact["extension_seconds"], 0)
        self.assertFalse(fact["bids_captured"])
        self.assertIsNotNone(fact["open_at"])
        self.assertIsNotNone(fact["initial_closed_at"])

    def test_duckdb_parquet_roundtrip_and_preset_queries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            prev_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                init_close_ms = 1_794_542_700_000
                close_ms = init_close_ms + 25_000
                open_ms = init_close_ms - 86_400_000
                raw_items = [
                    {
                        "id": 880001,
                        "title": "Anker USB-C Fast Charger 65W",
                        "retailPrice": 49.99,
                        "currentPrice": 12.0,
                        "bidCount": 3,
                        "bidderCount": 2,
                        "openTime": open_ms,
                        "initialCloseTime": init_close_ms,
                        "closeTime": close_ms,
                        "condition": "Brand New",
                        "locationName": "Phoenix",
                    }
                ]
                dim_records, fact_records = transform_records(raw_items, scrape_date="2026-11-12")
                merge_dim_products(dim_records, use_r2=False)

                # First write pre-settlement row (bids_captured=False) and verify empty fact_bids fallback
                write_fact_auctions(fact_records, scrape_date="2026-11-12", use_r2=False)
                con_pre = get_duckdb_lake_conn(use_local=True)
                try:
                    pre_bids_count = con_pre.execute("SELECT COUNT(*) FROM fact_bids;").fetchone()[0]
                    self.assertEqual(pre_bids_count, 0)
                finally:
                    con_pre.close()

                # Simulate post-close settlement upgrading the same auction row + writing fact_bids
                bid_rows, summary = build_bid_analytics(
                    {
                        "auction_id": 880001,
                        "open_ms": open_ms,
                        "initial_close_ms": init_close_ms,
                        "close_ms": close_ms,
                        "final_price": 15.0,
                        "bid_count": 3,
                        "bid_history": [
                            {"name": "Bidder #1", "amount": "$5", "time": init_close_ms - 7_200_000, "type": "Outbid"},
                            {"name": "Bidder #2", "amount": "$10", "time": init_close_ms - 120_000, "type": "Outbid"},
                            {"name": "Bidder #3", "amount": "$15", "time": close_ms - 15_000, "type": "Winning"},
                        ],
                    },
                    product_id=fact_records[0]["product_id"],
                    scraped_date="2026-11-12",
                )
                settled_fact = dict(fact_records[0])
                settled_fact.update(summary)
                settled_fact["final_price"] = 15.0
                settled_fact["is_closed"] = True
                settled_fact["market_status"] = "sold"

                write_fact_auctions([settled_fact], scrape_date="2026-11-12", use_r2=False)
                write_fact_bids(bid_rows, partition_date="2026-11-12", use_r2=False)

                con = get_duckdb_lake_conn(use_local=True)
                try:
                    fact_df = con.execute("SELECT auction_id, final_price, bids_captured, was_extended, price_at_5m_left FROM fact_auctions;").fetchdf()
                    self.assertEqual(len(fact_df), 1)
                    self.assertEqual(float(fact_df.iloc[0]["final_price"]), 15.0)
                    self.assertTrue(bool(fact_df.iloc[0]["bids_captured"]))
                    self.assertTrue(bool(fact_df.iloc[0]["was_extended"]))
                    self.assertEqual(float(fact_df.iloc[0]["price_at_5m_left"]), 5.0)

                    for q_name, q_meta in PRESET_QUERIES.items():
                        res_df = con.execute(q_meta["sql"]).fetchdf()
                        if q_name == "bid-timing":
                            self.assertFalse(res_df.empty)
                            self.assertEqual(int(res_df["Total Bids"].sum()), 3)
                            self.assertEqual(int(res_df["Winning Bids Placed Here"].sum()), 1)
                finally:
                    con.close()
            finally:
                os.chdir(prev_cwd)


if __name__ == "__main__":
    unittest.main()

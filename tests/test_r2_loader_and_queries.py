"""
QA-INT Pipeline, Parquet Schema, DuckDB Views, Mocked Settlement & Live Walk Tests
(`TC-INT-002` .. `TC-INT-009`, `TC-NFR-004`).
"""

import io
import json
import os
import sys
import tempfile
import time
import unittest
import urllib.parse
from pathlib import Path
from unittest import mock

import duckdb
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src import query_lake, r2_loader, settlement, transform
from tests._app_helpers import load_app_module_helpers
from tests.fixtures.nellis_fixtures import (
    build_turbo_stream_item_payload,
    build_turbo_stream_search_payload,
    seed_local_data_lake,
)


class _InMemoryS3Client:
    """Pure-Python in-memory S3 mock implementing get_paginator / get_object / put_object."""

    def __init__(self, initial_objects: dict | None = None):
        self.objects = dict(initial_objects or {})

    class _Paginator:
        def __init__(self, store):
            self.store = store

        def paginate(self, Bucket=None, Prefix=""):
            matching = [{"Key": k} for k in sorted(self.store) if k.startswith(Prefix)]
            yield {"Contents": matching}

    def get_paginator(self, op_name):
        assert op_name == "list_objects_v2"
        return self._Paginator(self.objects)

    def get_object(self, Bucket=None, Key=""):
        if Key not in self.objects:
            raise FileNotFoundError(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket=None, Key="", Body=b""):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.read()


class TestR2LoaderAndQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app_module_helpers()

    def test_mixed_legacy_and_new_partition_views(self):
        """TC-INT-002: union_by_name projects missing columns seamlessly over legacy + new partitions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lake_dir = os.path.join(tmpdir, "data_lake")
            seed_local_data_lake(lake_dir)

            prev_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                con = query_lake.get_duckdb_lake_conn(use_local=True)
                try:
                    rows = con.execute(
                        "SELECT auction_id, bids_captured, was_extended FROM fact_auctions ORDER BY auction_id;"
                    ).fetchall()
                    by_id = {r[0]: (r[1], r[2]) for r in rows}
                    self.assertIn(9001, by_id)  # legacy 17-column row
                    self.assertFalse(bool(by_id[9001][0]))  # projected bids_captured=FALSE
                    self.assertIsNone(by_id[9001][1])       # projected was_extended=NULL
                    self.assertTrue(bool(by_id[1001][0]))   # modern settled row
                finally:
                    con.close()
            finally:
                os.chdir(prev_cwd)

    def test_dim_preserve_first_seen_and_zstd_codec(self):
        """TC-INT-003 & TC-NFR-004: Preserve earliest first_seen_date, higher final_price, and ZSTD codec."""
        with tempfile.TemporaryDirectory() as tmpdir:
            prev_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                close_ms = 1_794_542_700_000
                raw_day1 = [
                    {
                        "id": 77001,
                        "title": "Bose QuietComfort Headphones",
                        "retailPrice": 349.0,
                        "currentPrice": 55.0,
                        "bidCount": 4,
                        "closeTime": close_ms,
                        "condition": "Brand New",
                        "locationName": "Phoenix",
                    }
                ]
                dim1, fact1 = transform.transform_records(raw_day1, scrape_date="2026-11-10")
                r2_loader.merge_dim_products(dim1, use_r2=False)
                fact_path = r2_loader.write_fact_auctions(fact1, scrape_date="2026-11-10", use_r2=False)

                # Same product re-scraped 2 days later with higher bid price on partition
                raw_day2 = [dict(raw_day1[0], currentPrice=92.0, bidCount=9, brand="Bose")]
                dim2, fact2 = transform.transform_records(raw_day2, scrape_date="2026-11-12")
                cat_path = r2_loader.merge_dim_products(dim2, use_r2=False)
                r2_loader.write_fact_auctions(fact2, scrape_date="2026-11-10", use_r2=False)

                con = duckdb.connect()
                try:
                    cat_row = con.execute(
                        f"SELECT first_seen_date, brand FROM read_parquet('{cat_path}');"
                    ).fetchone()
                    self.assertEqual(str(cat_row[0]), "2026-11-10")
                    self.assertEqual(cat_row[1], "Bose")

                    f_row = con.execute(
                        f"SELECT COUNT(*), MAX(final_price) FROM read_parquet('{fact_path}');"
                    ).fetchone()
                    self.assertEqual(f_row[0], 1)
                    self.assertEqual(float(f_row[1]), 92.0)

                    meta = con.execute(f"SELECT compression FROM parquet_metadata('{fact_path}');").fetchall()
                    self.assertTrue(any("ZSTD" in str(m[0]).upper() for m in meta))
                finally:
                    con.close()
            finally:
                os.chdir(prev_cwd)

    def test_settled_sales_sql_excludes_invalid_statuses(self):
        """TC-INT-004: Relisted, 0-bid, future active, and archived_unverified lots never leak into settled averages."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lake_dir = os.path.join(tmpdir, "data_lake")
            seed_local_data_lake(lake_dir)

            prev_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                con = query_lake.get_duckdb_lake_conn(use_local=True)
                try:
                    self.app.con = con
                    where_sql = self.app.get_settled_sales_where_sql("f")
                    rows = con.execute(
                        f"SELECT f.auction_id FROM fact_auctions f WHERE {where_sql} ORDER BY f.auction_id;"
                    ).fetchall()
                    ids = [r[0] for r in rows]
                    self.assertEqual(ids, [1001, 1002, 1003, 9001])
                    self.assertNotIn(1004, ids)  # relisted & 0-bid
                    self.assertNotIn(1005, ids)  # active future close
                finally:
                    con.close()
            finally:
                os.chdir(prev_cwd)

    def test_analytics_hub_and_best_past_deals_sql(self):
        """TC-INT-005: All 5 Analytics Hub models and Best Past Deals SQL agree with seeded ground truth."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lake_dir = os.path.join(tmpdir, "data_lake")
            seed_local_data_lake(lake_dir)

            prev_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                con = query_lake.get_duckdb_lake_conn(use_local=True)
                try:
                    self.app.con = con
                    settled_where = self.app.get_settled_sales_where_sql("f")

                    # Model 5 summary & curve check
                    summary_df = con.execute(
                        f"""
                        SELECT
                            COUNT(*) AS lots_with_bid_history,
                            ROUND(100.0 * AVG(CASE WHEN f.was_extended THEN 1.0 ELSE 0.0 END), 1) AS pct_ext
                        FROM fact_auctions f
                        WHERE {settled_where} AND COALESCE(f.bids_captured, FALSE) = TRUE AND f.bid_count > 0;
                        """
                    ).fetchdf()
                    self.assertEqual(int(summary_df.iloc[0]["lots_with_bid_history"]), 3)
                    self.assertAlmostEqual(float(summary_df.iloc[0]["pct_ext"]), 33.3, places=1)

                    curve_df = con.execute(query_lake.PRESET_QUERIES["bid-timing"]["sql"]).fetchdf()
                    self.assertAlmostEqual(float(curve_df["% of All Bids"].sum()), 100.0, delta=0.5)
                    self.assertIn("Last 60s", set(curve_df["Time Window Before Close"]))

                    # Best Past Deals high-discount sleeper check
                    bpd_df = con.execute(
                        f"""
                        SELECT f.auction_id
                        FROM fact_auctions f
                        JOIN dim_products p ON f.product_id = p.product_id
                        WHERE {settled_where}
                          AND ROUND((1.0 - f.discount_ratio) * 100.0, 1) >= 90
                          AND f.bid_count <= 3
                          AND f.retail_price >= 100;
                        """
                    ).fetchdf()
                    self.assertEqual(bpd_df["auction_id"].tolist(), [1003])
                finally:
                    con.close()
            finally:
                os.chdir(prev_cwd)

    def test_fetch_closed_lot_result_status_and_retry(self):
        """TC-INT-006: Handle 200 sold, 404 -> archived_unverified, transient retry, and relisted exclusion."""
        close_ms = 1_794_542_700_000
        sold_payload = build_turbo_stream_item_payload(
            {
                "id": 55501,
                "title": "Test Sold Item",
                "retailPrice": 100.0,
                "currentPrice": 22.0,
                "bidCount": 7,
                "closeTime": close_ms,
                "openTime": close_ms - 86_400_000,
                "initialCloseTime": close_ms,
                "isClosed": True,
                "marketStatus": "sold",
                "notes": "",
                "grade": {
                    "conditionType": {"description": "New"},
                    "damageType": {"description": "None"},
                    "missingPartsType": {"description": "No"},
                    "functionalType": {"description": "Yes"},
                    "rating": 5,
                },
            },
            bid_history=[{"time": close_ms - 10_000, "name": "Bidder #1", "amount": "$22", "type": "Winning"}],
        )
        relisted_payload = build_turbo_stream_item_payload(
            {
                "id": 55502,
                "title": "Relisted Zero Bids",
                "retailPrice": 100.0,
                "currentPrice": 0.0,
                "bidCount": 0,
                "closeTime": close_ms,
                "isClosed": True,
                "marketStatus": "relisted",
                "notes": "",
                "grade": {},
            },
            bid_history=[],
        )

        class _MockResp:
            def __init__(self, status_code, body=None):
                self.status_code = status_code
                self._body = body

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise RuntimeError(f"HTTP {self.status_code}")

            def json(self):
                return self._body

        sess = mock.MagicMock()
        # Attempt 1 transient error, attempt 2 succeeds
        sess.get.side_effect = [RuntimeError("timeout"), _MockResp(200, sold_payload)]
        res_sold = settlement.fetch_closed_lot_result(55501, session=sess)
        self.assertIsNotNone(res_sold)
        self.assertTrue(settlement.is_verified_sold_result(res_sold))
        self.assertEqual(res_sold["final_price"], 22.0)

        # 404 returns archived_unverified
        sess.get.side_effect = [_MockResp(404)]
        res_404 = settlement.fetch_closed_lot_result(55503, session=sess)
        self.assertEqual(res_404["market_status"], "archived_unverified")
        self.assertFalse(settlement.is_verified_sold_result(res_404))

        # Relisted returns False on is_verified_sold_result
        sess.get.side_effect = [_MockResp(200, relisted_payload)]
        res_relisted = settlement.fetch_closed_lot_result(55502, session=sess)
        self.assertFalse(settlement.is_verified_sold_result(res_relisted))

    def test_settle_r2_partitions_end_to_end_mocked_s3(self):
        """TC-INT-007: Settle partitions, emit deduplicated fact_bids once across dates, and skip future buffer."""
        now_ms = int(time.time() * 1000)
        closed_past_ms = now_ms - 3_600_000
        too_recent_ms = now_ms - 60_000  # inside buffer_minutes=5

        dim_recs, fact_recs_day1 = transform.transform_records(
            [
                {
                    "id": 30001,
                    "title": "Anker 65W USB-C Charger",
                    "retailPrice": 60.0,
                    "currentPrice": 5.0,
                    "bidCount": 2,
                    "openTime": closed_past_ms - 86_400_000,
                    "initialCloseTime": closed_past_ms,
                    "closeTime": closed_past_ms,
                    "condition": "Brand New",
                    "locationName": "Phoenix",
                    "isClosed": False,
                },
                {
                    "id": 30002,
                    "title": "Closing Very Recently",
                    "retailPrice": 50.0,
                    "currentPrice": 3.0,
                    "bidCount": 1,
                    "closeTime": too_recent_ms,
                    "condition": "Brand New",
                    "locationName": "Mesa",
                    "isClosed": False,
                },
            ],
            scrape_date="2026-11-15",
        )
        # Same auction 30001 also present on previous day's partition -> bids_emitted_aids prevents duplicate bid rows
        _, fact_recs_day0 = transform.transform_records(
            [
                {
                    "id": 30001,
                    "title": "Anker 65W USB-C Charger",
                    "retailPrice": 60.0,
                    "currentPrice": 2.0,
                    "bidCount": 1,
                    "openTime": closed_past_ms - 86_400_000,
                    "initialCloseTime": closed_past_ms,
                    "closeTime": closed_past_ms,
                    "condition": "Brand New",
                    "locationName": "Phoenix",
                    "isClosed": False,
                }
            ],
            scrape_date="2026-11-14",
        )

        df_dim = pd.DataFrame(dim_recs)
        df_day0 = r2_loader.enforce_fact_auctions_schema(pd.DataFrame(fact_recs_day0))
        df_day1 = r2_loader.enforce_fact_auctions_schema(pd.DataFrame(fact_recs_day1))

        def _to_bytes(df):
            b = io.BytesIO()
            df.to_parquet(b, index=False, compression="zstd")
            return b.getvalue()

        s3_mock = _InMemoryS3Client(
            {
                "dim_products/products_catalog.parquet": _to_bytes(df_dim),
                "fact_auctions/year=2026/month=11/auctions_2026-11-14.parquet": _to_bytes(df_day0),
                "fact_auctions/year=2026/month=11/auctions_2026-11-15.parquet": _to_bytes(df_day1),
            }
        )

        settled_api_map = {
            30001: {
                "auction_id": 30001,
                "is_closed": True,
                "market_status": "sold",
                "final_price": 18.0,
                "retail_price": 60.0,
                "bid_count": 4,
                "close_ms": closed_past_ms + 12_000,
                "initial_close_ms": closed_past_ms,
                "open_ms": closed_past_ms - 86_400_000,
                "bid_history": [
                    {"name": "Bidder #1", "amount": "$5", "time": closed_past_ms - 7_200_000, "type": "Outbid"},
                    {"name": "Bidder #2", "amount": "$18", "time": closed_past_ms - 10_000, "type": "Winning"},
                ],
                "cond_raw": "New",
                "dmg_raw": "None",
                "mp_raw": "No",
                "func_raw": "Yes",
                "rating": 5,
                "notes": "",
                "not_found": False,
            }
        }

        with mock.patch.object(r2_loader, "R2_ACCESS_KEY_ID", "test_key"), mock.patch.object(
            r2_loader, "R2_SECRET_ACCESS_KEY", "test_secret"
        ), mock.patch.object(
            r2_loader, "R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com"
        ), mock.patch.object(
            r2_loader, "R2_BUCKET_NAME", "test-bucket"
        ), mock.patch("boto3.client", return_value=s3_mock), mock.patch.object(
            settlement, "fetch_closed_lots_batch", return_value=settled_api_map
        ):
            updated = settlement.settle_r2_partitions(max_files=0, workers=4, buffer_minutes=5)

        self.assertEqual(updated, 2)  # both 2026-11-15 and 2026-11-14 copies updated
        self.assertIn("fact_bids/year=2026/month=11/bids_2026-11-15.parquet", s3_mock.objects)
        # 2026-11-14 had only 30001 which was already emitted on newest file 2026-11-15
        self.assertNotIn("fact_bids/year=2026/month=11/bids_2026-11-14.parquet", s3_mock.objects)

        res_day1 = pd.read_parquet(io.BytesIO(s3_mock.objects["fact_auctions/year=2026/month=11/auctions_2026-11-15.parquet"]))
        row_30001 = res_day1.loc[res_day1["auction_id"] == 30001].iloc[0]
        row_30002 = res_day1.loc[res_day1["auction_id"] == 30002].iloc[0]
        self.assertEqual(float(row_30001["final_price"]), 18.0)
        self.assertTrue(bool(row_30001["bids_captured"]))
        self.assertFalse(bool(row_30002["is_closed"]))  # respected 5-minute safety buffer

    def test_fetch_live_short_window_chronological_walk(self):
        """TC-INT-008 (CR-01): Short-window mode walks past closed page 0 & stops at target_window_sec + 90."""
        now_ms = int(time.time() * 1000)

        def _make_lot(aid, offset_sec):
            return {
                "id": aid,
                "title": f"Item {aid}",
                "retailPrice": 100.0,
                "currentPrice": 10.0,
                "bidCount": 2,
                "closeTime": now_ms + int(offset_sec * 1000),
                "location": {"name": "Phoenix"},
                "grade": {"conditionType": {"description": "New"}, "rating": 5},
                "photos": [],
            }

        page0 = build_turbo_stream_search_payload([_make_lot(100 + i, -60) for i in range(40)])
        page1 = build_turbo_stream_search_payload([_make_lot(200 + i, 300 + i * 5) for i in range(40)])
        page2 = build_turbo_stream_search_payload([_make_lot(300 + i, 960 + i * 5) for i in range(40)])  # max_diff_sec >= 990

        called_urls = []

        class _MockUrlResp:
            def __init__(self, data):
                self._raw = json.dumps(data).encode("utf-8")

            def read(self):
                return self._raw

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        class _MockOpener:
            def open(self, req, timeout=10):
                url = req.full_url if hasattr(req, "full_url") else str(req)
                called_urls.append(url)
                if "change-shopping-location" in url:
                    return _MockUrlResp({"ok": True})
                if "n:0" in url or "n%3A0" in url:
                    return _MockUrlResp(page0)
                if "n:1" in url or "n%3A1" in url:
                    return _MockUrlResp(page1)
                if "n:2" in url or "n%3A2" in url:
                    return _MockUrlResp(page2)
                raise AssertionError(f"Should have stopped early before requesting: {url}")

        with mock.patch("urllib.request.build_opener", return_value=_MockOpener()):
            df = self.app.fetch_live_nellis_lots(
                search_kw="",
                loc_filter="All",
                sort_by="Highest Discount (%)",
                page_size=40,
                max_pages=10,
                target_window_sec=900,
            )

        self.assertFalse(df.empty)
        search_urls = [u for u in called_urls if "search.data" in u]
        self.assertEqual(len(search_urls), 3)  # pages 0, 1, 2 then stopped
        for u in search_urls:
            self.assertIn("sortBy=time_remaining_asc", u)
        self.assertTrue((df["diff_sec"] > 0).all())

    def test_batch_verify_card_and_modal_parity(self):
        """TC-INT-009 (CR-03): Unverified relisted matches dropped; unverified genuine matches updated to live price."""
        img = "https://m.media-amazon.com/images/I/71VERIFY._AC_SL1500_.jpg"
        hist_index = {
            "by_img": {
                "71VERIFY": [
                    {
                        "auction_id": 801,
                        "title": "Test Blender",
                        "retail_price": 100.0,
                        "final_price": 1.0,
                        "is_closed": False,
                        "closed_at": "2026-11-15",
                        "photo_url": img,
                        "cond_tier": "New",
                    },
                    {
                        "auction_id": 802,
                        "title": "Test Blender",
                        "retail_price": 100.0,
                        "final_price": 5.0,
                        "is_closed": False,
                        "closed_at": "2026-11-15",
                        "photo_url": img,
                        "cond_tier": "New",
                    },
                    {
                        "auction_id": 803,
                        "title": "Test Blender",
                        "retail_price": 100.0,
                        "final_price": 10.0,
                        "is_closed": True,
                        "bid_count": 3,
                        "closed_at": "2026-11-14",
                        "photo_url": img,
                        "cond_tier": "New",
                    },
                ]
            },
            "by_sig": {},
        }
        live_map = {
            801: {"auction_id": 801, "is_closed": True, "market_status": "relisted", "final_price": 0.0, "bid_count": 0},
            802: {"auction_id": 802, "is_closed": True, "market_status": "sold", "final_price": 14.0, "bid_count": 5},
        }
        rows = [{"auction_id": 999, "title": "Test Blender", "retail_price": 100.0, "photo_url": img, "condition": "Brand New"}]
        with mock.patch.object(self.app, "verify_closed_lots_cached", return_value=live_map):
            verified_map = self.app.batch_verify_rows_history(rows, hist_index, same_condition_only=True)

        verified = verified_map[999]
        self.assertEqual([m["auction_id"] for m in verified], [803, 802])
        self.assertEqual([m["final_price"] for m in verified], [10.0, 14.0])


if __name__ == "__main__":
    unittest.main()

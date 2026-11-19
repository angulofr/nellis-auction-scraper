"""
QA/Performance Suite for Lake Compaction (v1 -> v2), ETag Incremental Sync,
Precomputed Price Aggregates, Modal SQL Lookup, and 100k+ Row Scale Latency.
"""

import hashlib
import io
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import duckdb
from streamlit.testing.v1 import AppTest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.compact_lake import compact_v1_to_v2, update_v2_incremental
from src.lake_sync import configure_duckdb_lake_views, fetch_v2_manifest_etag, sync_v2_cache
from src.transform import transform_records
from tests._app_helpers import load_app_module_helpers
from tests.fixtures.nellis_fixtures import seed_local_data_lake, start_mock_nellis_server


class _CountingS3Client:
    """In-memory S3 mock counting GetObject / PutObject / DeleteObject calls."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.get_keys: list[str] = []

    def put_object(self, Bucket=None, Key="", Body=b"", **_kwargs):
        data = Body if isinstance(Body, bytes) else Body.read()
        self.objects[Key] = data

    def get_object(self, Bucket=None, Key=""):
        self.get_keys.append(Key)
        if Key not in self.objects:
            raise FileNotFoundError(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def delete_object(self, Bucket=None, Key=""):
        self.objects.pop(Key, None)


class TestCompactionAndSpeed(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app_module_helpers()

    def test_v1_to_v2_parity_zero_duplicates_and_aggregates(self):
        """Compact v1 into disjoint sorted v2 hot/cold + verify 100% row/agg parity."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lake_dir = os.path.join(tmpdir, "data_lake")
            res = seed_local_data_lake(lake_dir)

            manifest = compact_v1_to_v2(use_r2=False, lake_dir=lake_dir, hot_days=3)
            self.assertEqual(manifest["counts"]["fact_auctions"], res["fact_count"])
            self.assertEqual(manifest["counts"]["fact_bids"], res["bid_count"])

            con = duckdb.connect(":memory:")
            try:
                info = configure_duckdb_lake_views(con, use_local=True, lake_dir=lake_dir, enable_fts=True)
                self.assertEqual(info["mode"], "v2")
                self.assertTrue(info["has_v2"])

                hot_ids = {r[0] for r in con.execute("SELECT auction_id FROM fact_auctions_hot;").fetchall()}
                all_ids = [r[0] for r in con.execute("SELECT auction_id FROM fact_auctions;").fetchall()]
                cold_glob = str(Path(lake_dir) / "v2" / "fact_auctions" / "cold" / "*" / "*" / "*.parquet").replace("\\", "/")
                cold_ids = {r[0] for r in con.execute(f"SELECT auction_id FROM read_parquet('{cold_glob}');").fetchall()}

                # Disjoint partition guarantee & zero duplicates across entire lake
                self.assertEqual(len(hot_ids & cold_ids), 0)
                self.assertEqual(len(all_ids), len(set(all_ids)))
                self.assertEqual(len(all_ids), res["fact_count"])
                self.assertIn(9001, cold_ids)  # 5 days ago -> cold monthly partition
                self.assertIn(1005, hot_ids)   # active future -> hot partition

                # Enriched dim_products precomputed fingerprints
                dim_rows = con.execute(
                    "SELECT product_id, amazon_image_id, title_signature, search_text_norm FROM dim_products;"
                ).fetchall()
                self.assertGreaterEqual(len(dim_rows), 4)
                for pid, img_id, sig, stxt in dim_rows:
                    self.assertTrue(bool(sig), f"Missing title_signature for {pid}")
                    self.assertTrue(bool(stxt), f"Missing search_text_norm for {pid}")

                # Precomputed agg_product_price_stats accuracy
                row_obd = con.execute("""
                    SELECT settled_count, settled_min_price, settled_avg_price
                    FROM agg_product_price_stats
                    WHERE product_key = '71OBD2TEST' AND cond_tier = 'New';
                """).fetchone()
                self.assertIsNotNone(row_obd)
                self.assertEqual(int(row_obd[0]), 2)
                self.assertAlmostEqual(float(row_obd[1]), 8.0, places=2)
                self.assertAlmostEqual(float(row_obd[2]), 10.0, places=2)

                # Precomputed agg_lake_stats exact counts
                lake_stats = con.execute("SELECT total_lots, settled_lots, lots_with_bids, total_bids FROM agg_lake_stats;").fetchone()
                self.assertEqual(tuple(int(x) for x in lake_stats), (7, 4, 3, 9))
            finally:
                con.close()

    def test_incremental_update_adds_new_and_deduplicates_overlap(self):
        """Incremental v2 update merges overlapping auction_id + adds brand-new lot without duplicates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lake_dir = os.path.join(tmpdir, "data_lake")
            res = seed_local_data_lake(lake_dir)
            compact_v1_to_v2(use_r2=False, lake_dir=lake_dir, hot_days=3)

            now_ms = int(time.time() * 1000)
            new_dim, new_facts = transform_records(
                [
                    # Updated bid price on already-existing open lot 1005
                    {
                        "id": 1005,
                        "title": "UMEIJA V011 OBD2 Scanner Diagnostic Tool",
                        "retailPrice": 40.0,
                        "currentPrice": 32.0,
                        "bidCount": 11,
                        "closeTime": now_ms + 3_600_000,
                        "condition": "Brand New",
                        "locationName": "Phoenix",
                        "photos": [{"url": "https://m.media-amazon.com/images/I/71OBD2TEST._AC_SL1500_.jpg"}],
                        "isClosed": False,
                    },
                    # Brand new Sony headphones lot
                    {
                        "id": 20099,
                        "title": "Sony WH-1000XM5 Wireless Noise Canceling Headphones",
                        "retailPrice": 399.99,
                        "currentPrice": 45.0,
                        "bidCount": 5,
                        "closeTime": now_ms + 7_200_000,
                        "condition": "New",
                        "locationName": "Mesa",
                        "photos": [{"url": "https://m.media-amazon.com/images/I/51SONYXM5TEST._AC_SL1500_.jpg"}],
                        "isClosed": False,
                    },
                ],
                scrape_date=res["today_str"],
            )
            m2 = update_v2_incremental(
                new_fact_records=new_facts,
                new_dim_records=new_dim,
                use_r2=False,
                lake_dir=lake_dir,
                hot_days=3,
            )
            self.assertEqual(m2["counts"]["fact_auctions"], res["fact_count"] + 1)

            con = duckdb.connect(":memory:")
            try:
                configure_duckdb_lake_views(con, use_local=True, lake_dir=lake_dir, enable_fts=False)
                p_1005 = con.execute("SELECT final_price FROM fact_auctions WHERE auction_id = 1005;").fetchone()[0]
                self.assertAlmostEqual(float(p_1005), 32.0, places=2)
                total = con.execute("SELECT COUNT(*), COUNT(DISTINCT auction_id) FROM fact_auctions;").fetchone()
                self.assertEqual(total, (res["fact_count"] + 1, res["fact_count"] + 1))
            finally:
                con.close()

    def test_etag_sync_skips_unchanged_parquet_downloads(self):
        """Incremental R2 cache sync skips unchanged cold files via SHA-256 manifest diff."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lake_dir = os.path.join(tmpdir, "data_lake")
            cache_dir = os.path.join(tmpdir, "cache")
            seed_local_data_lake(lake_dir)
            manifest = compact_v1_to_v2(use_r2=False, lake_dir=lake_dir, hot_days=3)

            s3 = _CountingS3Client()
            for rel in manifest["files"]:
                s3.put_object(Key=rel, Body=(Path(lake_dir) / rel).read_bytes())
            s3.put_object(Key="v2/manifest.json", Body=(Path(lake_dir) / "v2" / "manifest.json").read_bytes())

            fake_creds = {
                "access_key_id": "test",
                "secret_access_key": "test",
                "endpoint_url": "https://example.invalid",
                "bucket_name": "test-bucket",
            }
            with mock.patch.dict(os.environ, {"NELLIS_CACHE_DIR": cache_dir}):
                # Cold cache -> downloads manifest + all v2 parquet files
                etag1 = fetch_v2_manifest_etag(use_local=False, r2_creds=fake_creds, s3_client=s3)
                self.assertEqual(etag1, manifest["etag_version"])
                res1 = sync_v2_cache(use_local=False, r2_creds=fake_creds, s3_client=s3)
                self.assertEqual(res1["mode"], "v2")
                self.assertEqual(len(s3.get_keys), 1 + 1 + len(manifest["files"]))  # 1 etag + 1 manifest + N files

                # Warm cache with identical ETag -> downloads ONLY manifest, zero parquet files
                s3.get_keys.clear()
                res2 = sync_v2_cache(use_local=False, r2_creds=fake_creds, s3_client=s3)
                self.assertEqual(res2["mode"], "v2")
                self.assertEqual(s3.get_keys, ["v2/manifest.json"])

                # Simulate hot partition update while cold partition keeps identical hash
                hot_key = "v2/fact_auctions/hot/active_and_recent.parquet"
                new_hot_bytes = s3.objects[hot_key] + b""  # unchanged bytes test first -> bump tag meaningfully
                mut_hot = Path(lake_dir) / hot_key
                mut_bytes = mut_hot.read_bytes()
                # Re-write manifest changing hot sha256 only
                new_manifest = json.loads(json.dumps(manifest))
                new_manifest["files"][hot_key]["sha256"] = hashlib.sha256(mut_bytes + b"1").hexdigest()
                new_manifest["etag_version"] = "newhotetag123"
                s3.put_object(Key=hot_key, Body=mut_bytes)
                s3.put_object(Key="v2/manifest.json", Body=json.dumps(new_manifest).encode("utf-8"))

                s3.get_keys.clear()
                res3 = sync_v2_cache(use_local=False, r2_creds=fake_creds, s3_client=s3)
                self.assertEqual(res3["mode"], "v2")
                self.assertEqual(s3.get_keys, ["v2/manifest.json", hot_key])

    def test_app_agg_mode_and_modal_sql_parity(self):
        """App helpers read v2 agg_product_price_stats in O(1) + drill down modal lots accurately."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lake_dir = os.path.join(tmpdir, "data_lake")
            seed_local_data_lake(lake_dir)
            compact_v1_to_v2(use_r2=False, lake_dir=lake_dir, hot_days=3)

            prev_con = self.app.con
            prev_meta = dict(self.app._LAKE_META)
            con = duckdb.connect(":memory:")
            try:
                info = configure_duckdb_lake_views(con, use_local=True, lake_dir=lake_dir, enable_fts=False)
                self.app.con = con
                self.app._LAKE_META.clear()
                self.app._LAKE_META.update(info)

                self.assertFalse(self.app.has_settled_history_rows())

                hist_idx = self.app.load_historical_price_index()
                self.assertEqual(hist_idx.get("mode"), "agg")

                open_scanner = {
                    "auction_id": 1005,
                    "title": "UMEIJA V011 OBD2 Scanner Diagnostic Tool",
                    "retail_price": 40.0,
                    "final_price": 5.0,
                    "condition": "Brand New (5/5)",
                    "photo_url": "https://m.media-amazon.com/images/I/71OBD2TEST._AC_SL1500_.jpg",
                }
                summ = self.app.summarize_item_history(open_scanner, hist_idx, same_condition_only=True)
                self.assertIsNotNone(summ)
                self.assertEqual(summ["count"], 2)
                self.assertAlmostEqual(summ["min_won"], 8.0, places=2)
                self.assertAlmostEqual(summ["avg_won"], 10.0, places=2)

                # Modal drilldown should return exact lots [1001, 1002] cleanly sorted ascending
                modal_lots = self.app.fetch_modal_historical_lots(open_scanner, same_condition_only=True)
                self.assertEqual([int(m["auction_id"]) for m in modal_lots], [1001, 1002])

                # Self-exclusion on Archive single-sale item (DeWalt 1003 only sold once)
                archive_dewalt = {
                    "auction_id": 1003,
                    "title": "DeWalt 20V MAX Brushless Drill Driver Kit",
                    "retail_price": 199.0,
                    "final_price": 6.0,
                    "condition": "Brand New (5/5)",
                    "photo_url": "https://m.media-amazon.com/images/I/81DEWALTTEST._AC_SL1500_.jpg",
                }
                self.assertIsNone(self.app.summarize_item_history(archive_dewalt, hist_idx, same_condition_only=True))

                # Fast search clause switches to p.search_text_norm in v2 mode
                fast_sql = self.app.build_fast_text_search_sql("dyson vacuum")
                self.assertIn("p.search_text_norm", fast_sql)
            finally:
                con.close()
                self.app.con = prev_con
                self.app._LAKE_META.clear()
                self.app._LAKE_META.update(prev_meta)

    def test_100k_scale_v2_query_latency(self):
        """Benchmark v2 sorted partitions + precomputed agg over 100,000 synthetic lots."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lake_dir = Path(tmpdir) / "data_lake"
            fact_dir = lake_dir / "fact_auctions" / "year=2026" / "month=11"
            dim_dir = lake_dir / "dim_products"
            fact_dir.mkdir(parents=True, exist_ok=True)
            dim_dir.mkdir(parents=True, exist_ok=True)

            gen = duckdb.connect(":memory:")
            try:
                # 20,000 unique products + 100,000 auction lots across 2 locations & 4 conditions
                dim_path = str(dim_dir / "products_catalog.parquet").replace("\\", "/")
                gen.execute(f"""
                    COPY (
                        SELECT
                            'prod_' || i AS product_id,
                            CASE
                                WHEN i % 100 = 0 THEN 'DeWalt 20V Cordless Hammer Drill Kit ' || i
                                WHEN i % 100 = 1 THEN 'Dyson V11 Torque Drive Cordless Vacuum ' || i
                                ELSE 'Sample Consumer Electronics Gadget Item ' || i
                            END AS title,
                            CASE WHEN i % 2 = 0 THEN 'DeWalt' ELSE 'Dyson' END AS brand,
                            CASE WHEN i % 3 = 0 THEN 'Tools & Hardware' ELSE 'Home & General' END AS category_primary,
                            'General' AS category_secondary,
                            ROUND(30.0 + (i % 300), 2) AS base_retail_price,
                            '' AS description,
                            DATE '2026-10-01' AS first_seen_date,
                            'https://m.media-amazon.com/images/I/IMG' || (i % 5000) || '._AC_SL1500_.jpg' AS photo_url
                        FROM range(20000) t(i)
                    ) TO '{dim_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
                """).fetchall()

                fact_path = str(fact_dir / "auctions_2026-11-18.parquet").replace("\\", "/")
                gen.execute(f"""
                    COPY (
                        SELECT
                            CAST(1000000 + i AS BIGINT) AS auction_id,
                            'prod_' || (i % 20000) AS product_id,
                            CASE WHEN i % 2 = 0 THEN 'Phoenix' ELSE 'Mesa' END AS location,
                            CASE
                                WHEN i % 4 = 0 THEN 'Brand New (5/5)'
                                WHEN i % 4 = 1 THEN 'Like New (4/5)'
                                WHEN i % 4 = 2 THEN 'Used (3/5)'
                                ELSE 'Incomplete / Damaged / Parts (2/5)'
                            END AS condition,
                            'Standard' AS completeness,
                            'Functional' AS functional_status,
                            '' AS damage_notes,
                            ROUND(50.0 + (i % 250), 2) AS retail_price,
                            ROUND(5.0 + (i % 45), 2) AS final_price,
                            ROUND((5.0 + (i % 45)) / (50.0 + (i % 250)), 4) AS discount_ratio,
                            CAST(1 + (i % 15) AS INTEGER) AS bid_count,
                            CAST(1 + (i % 6) AS INTEGER) AS unique_bidders,
                            TIMESTAMPTZ '2026-11-18 12:00:00+00' - (i % 90) * INTERVAL 1 DAY AS closed_at,
                            CAST((i % 7) AS TINYINT) AS close_day_of_week,
                            CAST((i % 24) AS TINYINT) AS close_hour,
                            DATE '2026-11-18' AS scraped_date,
                            'https://m.media-amazon.com/images/I/IMG' || ((i % 20000) % 5000) || '._AC_SL1500_.jpg' AS photo_url,
                            TRUE AS is_closed,
                            'sold' AS market_status
                        FROM range(100000) t(i)
                    ) TO '{fact_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
                """).fetchall()
            finally:
                gen.close()

            t_compact = time.perf_counter()
            manifest = compact_v1_to_v2(use_r2=False, lake_dir=str(lake_dir), hot_days=3)
            compact_sec = time.perf_counter() - t_compact
            self.assertEqual(manifest["counts"]["fact_auctions"], 100000)

            con = duckdb.connect(":memory:")
            try:
                info = configure_duckdb_lake_views(con, use_local=True, lake_dir=str(lake_dir), enable_fts=False)
                self.assertEqual(info["mode"], "v2")

                t0 = time.perf_counter()
                top_rows = con.execute("""
                    SELECT f.auction_id, p.title, p.category_primary, f.final_price
                    FROM fact_auctions f
                    JOIN dim_products p ON f.product_id = p.product_id
                    WHERE f.location = 'Phoenix'
                      AND f.condition ILIKE '%Brand New%'
                      AND p.category_primary = 'Tools & Hardware'
                      AND (p.search_text_norm LIKE '%dew%' OR p.search_text_norm LIKE '%alt%')
                    ORDER BY f.closed_at DESC
                    LIMIT 2000;
                """).df()
                dt_search = time.perf_counter() - t0
                self.assertGreater(len(top_rows), 0)

                t1 = time.perf_counter()
                stats_rows = con.execute(
                    "SELECT product_key, cond_tier, settled_count, settled_min_price, settled_avg_price FROM agg_product_price_stats;"
                ).df()
                dt_agg = time.perf_counter() - t1
                self.assertGreater(len(stats_rows), 1000)

                print(
                    f"\n[Benchmark 100k lots] compact={compact_sec:.2f}s | "
                    f"filtered_search_2000={dt_search * 1000:.1f}ms | "
                    f"agg_stats_load={dt_agg * 1000:.1f}ms"
                )
                self.assertLess(dt_search, 2.0)
                self.assertLess(dt_agg, 2.0)
            finally:
                con.close()

    def test_streamlit_apptest_v2_live_and_archive_e2e(self):
        """Full Streamlit AppTest E2E against a compacted v2 lake in Live + Archive views."""
        import streamlit as st

        st.cache_data.clear()
        st.cache_resource.clear()
        with tempfile.TemporaryDirectory() as tmpdir:
            lake_dir = os.path.join(tmpdir, "data_lake")
            seed_local_data_lake(lake_dir)
            compact_v1_to_v2(use_r2=False, lake_dir=lake_dir, hot_days=3)

            now_ms = int(time.time() * 1000)
            srv, _ = start_mock_nellis_server(now_ms)
            try:
                env = {
                    "NELLIS_USE_LOCAL_LAKE": "1",
                    "NELLIS_LAKE_DIR": lake_dir,
                    "NELLIS_API_BASE": f"http://127.0.0.1:{srv.server_port}",
                    "R2_ACCESS_KEY_ID": "",
                    "R2_SECRET_ACCESS_KEY": "",
                }
                with mock.patch.dict(os.environ, env):
                    at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
                    self.assertEqual(len(at.exception), 0, f"Live v2 exceptions: {at.exception}")

                    # Switch to Archive tab and assert cards & summary render without errors
                    nav = at.get("segmented_control")[0]
                    nav.set_value("Archive").run()
                    self.assertEqual(len(at.exception), 0, f"Archive v2 exceptions: {at.exception}")
                    self.assertTrue(any("Matching Lots" in m.value for m in at.markdown))
            finally:
                srv.shutdown()
                srv.server_close()
                st.cache_data.clear()
                st.cache_resource.clear()


if __name__ == "__main__":
    unittest.main()

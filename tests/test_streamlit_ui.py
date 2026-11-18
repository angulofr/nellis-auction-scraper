"""
QA-E2E Streamlit Component UI Tests via `streamlit.testing.v1.AppTest`
(`TC-E2E-001` .. `TC-E2E-006`, `TC-NFR-003`).
Runs completely offline using local seeded DuckDB Parquet lake + local HTTP stub.
"""

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from streamlit.testing.v1 import AppTest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tests.fixtures.nellis_fixtures import (
    seed_local_data_lake,
    start_mock_nellis_server,
)


class TestStreamlitUI(unittest.TestCase):
    def setUp(self):
        import streamlit as st
        st.cache_data.clear()
        st.cache_resource.clear()
        self.tmpdir_obj = tempfile.TemporaryDirectory()
        self.tmpdir = self.tmpdir_obj.name
        self.lake_dir = os.path.join(self.tmpdir, "data_lake")
        self.srv = None

    def tearDown(self):
        if self.srv is not None:
            self.srv.shutdown()
            self.srv.server_close()
        self.tmpdir_obj.cleanup()

    def _run_apptest_env(self, extra_env=None):
        env = {
            "NELLIS_USE_LOCAL_LAKE": "1",
            "NELLIS_LAKE_DIR": self.lake_dir,
            "R2_ACCESS_KEY_ID": "",
            "R2_SECRET_ACCESS_KEY": "",
        }
        if extra_env:
            env.update(extra_env)
        return mock.patch.dict(os.environ, env)

    def test_offline_empty_lake_startup(self):
        """TC-E2E-001 (NFR-REL-003): Boot cleanly with no R2 creds, Nellis mobile-style top nav/filters, and empty lake."""
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            self.assertEqual(len(at.exception), 0, f"Unexpected exceptions: {at.exception}")
            segs = at.get("segmented_control")
            self.assertEqual(
                segs[0].options,
                ["Live", "Archive", "Analytics", "SQL"],
            )
            self.assertEqual(
                segs[1].options,
                ["5m", "10m", "15m", "30m", "1h", "24h", "All"],
            )
            self.assertEqual(at.text_input(key="top_live_kw").value, "")

    def test_deal_finder_short_window_and_likely_cheap_filter(self):
        """TC-E2E-002 (CR-01 & CR-04): 5m/10m/15m closing chips, Est. Retail presets, Steals toggle, & 1-click Clear."""
        seed_local_data_lake(self.lake_dir)
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            self.assertEqual(len(at.exception), 0)

            # Tap 10m chip first, then 15m chip, and verify sidebar Closing Window stays synced
            at.segmented_control(key="top_live_time").set_value("10m").run()
            self.assertEqual(at.sidebar.selectbox(key="sb_live_time").value, "10m")

            at.segmented_control(key="top_live_time").set_value("15m").run()
            self.assertEqual(len(at.exception), 0)
            self.assertEqual(at.sidebar.selectbox(key="sb_live_time").value, "15m")
            all_md = "\n".join(m.value for m in at.markdown)
            self.assertIn("2 active lots found", all_md)
            self.assertNotIn("Late Closing Lamp", all_md)

            # Toggle '🔮 Steals' button in toolbar -> syncs checkbox, lower min_hist_disc to 50
            at.button(key="top_live_steal_btn").click().run()
            self.assertTrue(at.sidebar.checkbox(key="sb_live_predict_cheap").value)
            at.sidebar.number_input(key="sb_live_min_disc").set_value(50).run()
            self.assertEqual(len(at.exception), 0)

            filtered_md = "\n".join(m.value for m in at.markdown)
            self.assertIn("1 active lots found", filtered_md)
            self.assertIn("$5.00", filtered_md)
            self.assertNotIn("$28.00", filtered_md)

            # Verify Est. Retail bucket preset syncs Min/Max bounds & two-way popover <-> sidebar sync & Clear
            at.radio(key="top_live_wh").set_value("Mesa").run()
            self.assertEqual(at.sidebar.radio(key="sb_live_wh").value, "Mesa")

            at.sidebar.radio(key="sb_live_retail_preset").set_value("$20 - $50").run()
            self.assertEqual(int(at.sidebar.number_input(key="sb_live_min_retail").value), 20)
            self.assertEqual(int(at.sidebar.number_input(key="sb_live_max_retail").value), 50)
            self.assertEqual(at.radio(key="top_live_retail_preset").value, "$20 - $50")

            at.text_input(key="top_live_kw").set_value("Scanner").run()
            self.assertEqual(at.text_input(key="top_live_kw").value, "Scanner")

            at.button(key="top_live_reset").click().run()
            self.assertEqual(at.text_input(key="top_live_kw").value, "")
            self.assertEqual(at.sidebar.selectbox(key="sb_live_time").value, "5m")
            self.assertEqual(at.sidebar.radio(key="sb_live_wh").value, "All")
            self.assertEqual(at.sidebar.radio(key="sb_live_retail_preset").value, "Any")
            self.assertFalse(at.sidebar.checkbox(key="sb_live_predict_cheap").value)

    def test_deal_finder_archive_mode(self):
        """TC-E2E-003 (CR-06): Unified Archive searches full lake ('pixel'), syncs date chips, and filters settled sales."""
        seed_local_data_lake(self.lake_dir)
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            at.segmented_control(key="nav_view").set_value("Archive").run()
            self.assertEqual(len(at.exception), 0)
            md_text = "\n".join(m.value for m in at.markdown)
            self.assertIn("WINNING PRICE", md_text)
            self.assertIn("DeWalt 20V MAX Brushless Drill Kit", md_text)
            self.assertIn("items found", md_text)

            # Search 'pixel' via top search bar -> finds Lot 1006 across full lake
            at.text_input(key="top_arch_kw").set_value("pixel").run()
            self.assertEqual(len(at.exception), 0)
            pixel_md = "\n".join(m.value for m in at.markdown)
            self.assertIn("Google Pixel 9 Pro Unlocked Case Bundle", pixel_md)
            self.assertIn("for 'pixel'", pixel_md)

            # Toggle '✅ Settled WON FOR Sales Only' -> excludes Lot 1006 (is_closed=False in lake)
            at.sidebar.checkbox(key="sb_arch_settled_only").set_value(True).run()
            self.assertEqual(len(at.exception), 0)
            settled_only_md = "\n".join(m.value for m in at.markdown)
            self.assertNotIn("Google Pixel 9 Pro Unlocked Case Bundle", settled_only_md)

    def test_best_past_deals_tab_and_sleeper_filter(self):
        """TC-E2E-004 (CR-05): Archive 90%+ Steals toggle & Sleepers checkbox filter properly."""
        seed_local_data_lake(self.lake_dir)
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            at.segmented_control(key="nav_view").set_value("Archive").run()
            self.assertEqual(len(at.exception), 0)

            # Toggle '🏆 90%+ Steals' toolbar button + Sleepers checkbox
            at.button(key="top_arch_steal_btn").click().run()
            self.assertEqual(len(at.exception), 0)
            self.assertEqual(int(at.sidebar.number_input(key="sb_arch_min_disc").value), 90)
            self.assertTrue(at.sidebar.checkbox(key="sb_arch_settled_only").value)
            self.assertEqual(at.segmented_control(key="top_arch_date_preset").value, "7d")

            at.sidebar.checkbox(key="sb_arch_sleeper_only").set_value(True).run()
            self.assertEqual(len(at.exception), 0)
            md_text = "\n".join(m.value for m in at.markdown)
            self.assertIn("DeWalt 20V MAX Brushless Drill Kit", md_text)
            self.assertNotIn("UMEIJA", md_text)

    def test_analytics_hub_all_five_models(self):
        """TC-E2E-005 (CR-08): Cycle through Analytics Hub Models 1-5 including Sniping Curve."""
        seed_local_data_lake(self.lake_dir)
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            at.segmented_control(key="nav_view").set_value("Analytics").run()
            self.assertEqual(len(at.exception), 0)

            sel = at.selectbox[0]
            for opt in sel.options:
                sel.set_value(opt).run()
                self.assertEqual(len(at.exception), 0, f"Model '{opt}' raised: {at.exception}")

            # Verify Model 5 (last selected) populated metrics and curve dataframe
            metric_labels = {m.label for m in at.metric}
            self.assertIn("Lots With Bid Curves", metric_labels)
            self.assertIn("Anti-Snipe Extended", metric_labels)
            self.assertGreaterEqual(len(at.dataframe), 2)

    def test_sql_studio_execution_and_ddl_block(self):
        """TC-E2E-006 (CR-07): SQL Studio example button, SELECT query, and DROP TABLE block."""
        seed_local_data_lake(self.lake_dir)
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            at.segmented_control(key="nav_view").set_value("SQL").run()
            self.assertEqual(len(at.exception), 0)

            # Click "💡 Sniping in Last 60s" example button
            at.button(key="sql_ex_3").click().run()
            self.assertIn("time bucket", at.text_input(key="ai_sql_question").value.lower())

            # Execute valid fact_bids SQL in editor
            at.text_area(key="sql_editor_text").set_value(
                "SELECT time_bucket, COUNT(*) AS bids FROM fact_bids GROUP BY time_bucket;"
            ).run()
            exec_btn = [b for b in at.button if "Execute Custom SQL" in b.label][0]
            exec_btn.click().run()
            self.assertEqual(len(at.exception), 0)
            self.assertGreaterEqual(len(at.dataframe), 1)

            # Attempt DDL injection -> must show Query Error block
            at.text_area(key="sql_editor_text").set_value("DROP TABLE fact_auctions;").run()
            exec_btn = [b for b in at.button if "Execute Custom SQL" in b.label][0]
            exec_btn.click().run()
            self.assertEqual(len(at.exception), 0)
            self.assertTrue(any("Query Error" in e.value for e in at.error))

    def test_graceful_degradation_on_upstream_http_error(self):
        """TC-NFR-003 (NFR-REL-002): HTTP 500 from Nellis API keeps UI responsive with 0 exceptions."""
        seed_local_data_lake(self.lake_dir)
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms, mode="error")
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            self.assertEqual(len(at.exception), 0)


if __name__ == "__main__":
    unittest.main()

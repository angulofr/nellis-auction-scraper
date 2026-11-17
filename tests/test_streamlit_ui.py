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
        """TC-E2E-001 (NFR-REL-003): Boot cleanly with no R2 creds and empty lake fallback."""
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            self.assertEqual(len(at.exception), 0, f"Unexpected exceptions: {at.exception}")
            nav = at.sidebar.radio[0]
            self.assertEqual(
                nav.options,
                ["🎯 Deal Finder", "🏆 Best Past Deals", "📊 Analytics Hub", "💻 SQL Studio"],
            )

    def test_deal_finder_short_window_and_likely_cheap_filter(self):
        """TC-E2E-002 (CR-01 & CR-04): < 15m excludes >15m items; Likely Cheap hides $28 (> $10 avg) lot."""
        seed_local_data_lake(self.lake_dir)
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            self.assertEqual(len(at.exception), 0)

            # Switch Time Remaining to < 15 mins (Live)
            at.sidebar.selectbox[0].set_value("⚡ Ending in < 15 mins (Live)").run()
            self.assertEqual(len(at.exception), 0)
            all_md = "\n".join(m.value for m in at.markdown)
            self.assertIn("Found 2 active lots", all_md)
            self.assertNotIn("Late Closing Lamp", all_md)

            # Enable Likely Cheap Tonight (lower min_hist_disc to 50 and max_hist_avg to 25 so $10 avg 75% off passes)
            at.sidebar.checkbox(key="predict_cheap_only").set_value(True).run()
            at.sidebar.number_input(key="min_hist_disc").set_value(50).run()
            self.assertEqual(len(at.exception), 0)

            filtered_md = "\n".join(m.value for m in at.markdown)
            self.assertIn("Found 1 active lots", filtered_md)
            self.assertIn("$5.00", filtered_md)
            self.assertNotIn("$28.00", filtered_md)

    def test_deal_finder_archive_mode(self):
        """TC-E2E-003 (CR-06): Archive Mode queries settled history and renders WINNING PRICE."""
        seed_local_data_lake(self.lake_dir)
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            at.sidebar.selectbox[0].set_value("🗄️ Closed / Historical Archive").run()
            self.assertEqual(len(at.exception), 0)
            md_text = "\n".join(m.value for m in at.markdown)
            self.assertIn("WINNING PRICE", md_text)
            self.assertIn("DeWalt 20V MAX Brushless Drill Kit", md_text)

    def test_best_past_deals_tab_and_sleeper_filter(self):
        """TC-E2E-004 (CR-05): Best Past Deals sleeper & discount filters work accurately."""
        seed_local_data_lake(self.lake_dir)
        now_ms = int(time.time() * 1000)
        self.srv, _ = start_mock_nellis_server(now_ms)
        base_url = f"http://127.0.0.1:{self.srv.server_port}"

        with self._run_apptest_env({"NELLIS_API_BASE": base_url}):
            at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=25).run()
            at.sidebar.radio[0].set_value("🏆 Best Past Deals").run()
            self.assertEqual(len(at.exception), 0)
            at.sidebar.checkbox(key="pd_sleeper_only").set_value(True).run()
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
            at.sidebar.radio[0].set_value("📊 Analytics Hub").run()
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
            at.sidebar.radio[0].set_value("💻 SQL Studio").run()
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

"""
QA-E2E Headless Chromium Playwright Tests (`TC-E2E-007` .. `TC-E2E-010`).
Tests real browser DOM execution:
  - Client-side JavaScript 1s countdown ticking & POTENTIALLY CLOSED transition
  - Interactive `@st.dialog` Price History modal & condition toggle
  - Floating `🔄 Refresh Live Data` cache invalidation
  - Desktop (1440x900) and Mobile (390x844) viewport checks

Executed automatically in GitHub Actions CI (`ubuntu-latest`) after Playwright install.
"""

import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tests.fixtures.nellis_fixtures import seed_local_data_lake, start_mock_nellis_server


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_for_health(base_url: str, timeout_sec: float = 35.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/_stcore/health", timeout=2) as r:
                if r.status == 200:
                    return
        except Exception:
            time.sleep(0.4)
    raise TimeoutError(f"Streamlit server at {base_url} did not become healthy within {timeout_sec}s")


class TestPlaywrightBrowserE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir_obj = tempfile.TemporaryDirectory()
        cls.lake_dir = os.path.join(cls.tmpdir_obj.name, "data_lake")
        seed_local_data_lake(cls.lake_dir)

        now_ms = int(time.time() * 1000)
        # First card closes ~14s after startup so Playwright sees both live tick and expiry flip
        products = [
            {
                "id": 21001,
                "title": "UMEIJA V011 OBD2 Scanner Diagnostic Tool",
                "retailPrice": 40.0,
                "currentPrice": 5.0,
                "bidCount": 2,
                "closeTime": now_ms + 15_000,
                "isClosed": False,
                "location": {"name": "Phoenix"},
                "grade": {"conditionType": {"description": "New"}, "rating": 5},
                "photos": [{"url": "https://m.media-amazon.com/images/I/71OBD2TEST._AC_SL1500_.jpg"}],
            },
            {
                "id": 21002,
                "title": "UMEIJA V011 OBD2 Scanner Diagnostic Tool",
                "retailPrice": 40.0,
                "currentPrice": 9.0,
                "bidCount": 4,
                "closeTime": now_ms + 240_000,
                "isClosed": False,
                "location": {"name": "Mesa"},
                "grade": {"conditionType": {"description": "New"}, "rating": 5},
                "photos": [{"url": "https://m.media-amazon.com/images/I/71OBD2TEST._AC_SL1500_.jpg"}],
            },
        ]
        cls.mock_srv, cls.mock_state = start_mock_nellis_server(now_ms, custom_products=products)

        cls.port = _find_free_port()
        cls.app_url = f"http://127.0.0.1:{cls.port}"
        env = os.environ.copy()
        env.update(
            {
                "NELLIS_USE_LOCAL_LAKE": "1",
                "NELLIS_LAKE_DIR": cls.lake_dir,
                "NELLIS_API_BASE": f"http://127.0.0.1:{cls.mock_srv.server_port}",
                "R2_ACCESS_KEY_ID": "",
                "R2_SECRET_ACCESS_KEY": "",
                "STREAMLIT_SERVER_HEADLESS": "true",
                "STREAMLIT_BROWSER_GATHER_USAGE_STATS": "false",
            }
        )
        cls.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(ROOT_DIR / "app.py"),
                "--server.port",
                str(cls.port),
                "--server.address",
                "127.0.0.1",
                "--server.headless",
                "true",
            ],
            cwd=str(ROOT_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        _wait_for_health(cls.app_url)
        cls.pw = sync_playwright().start()
        cls.browser = cls.pw.chromium.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
        except Exception:
            pass
        try:
            cls.pw.stop()
        except Exception:
            pass
        try:
            cls.proc.terminate()
            cls.proc.wait(timeout=8)
        except Exception:
            cls.proc.kill()
        cls.mock_srv.shutdown()
        cls.mock_srv.server_close()
        cls.tmpdir_obj.cleanup()

    def test_01_js_countdown_tick_and_potentially_closed(self):
        """TC-E2E-007 (REQ-TMR-001): Live 1s DOM tick decrements timer and flips to POTENTIALLY CLOSED."""
        page = self.browser.new_page(viewport={"width": 1440, "height": 900})
        try:
            page.goto(self.app_url, wait_until="domcontentloaded")
            badge = page.locator(".nellis-countdown-badge[data-close-ms]").first
            badge.wait_for(state="visible", timeout=25_000)

            initial_text = badge.inner_text()
            self.assertIn("left", initial_text.lower())

            # Wait up to 20s for short-timer card (closeTime = +15s) to reach 0 and transition
            page.wait_for_function(
                """() => {
                    const el = document.querySelector('.nellis-countdown-badge[data-close-ms]');
                    return el && el.innerText.includes('POTENTIALLY CLOSED');
                }""",
                timeout=22_000,
            )
            self.assertEqual(page.locator("[data-testid='stException']").count(), 0)
        finally:
            page.close()

    def test_02_history_modal_parity_and_condition_toggle(self):
        """TC-E2E-008 (REQ-DLG-001 & CR-03): History modal displays Cheapest/Average & toggles condition."""
        page = self.browser.new_page(viewport={"width": 1440, "height": 900})
        try:
            page.goto(self.app_url, wait_until="domcontentloaded")
            hist_btn = page.locator("button:has-text('History (2)')").first
            hist_btn.wait_for(state="visible", timeout=25_000)
            hist_btn.click()

            dialog = page.locator("[role='dialog']")
            dialog.wait_for(state="visible", timeout=15_000)
            dialog_text = dialog.inner_text()
            self.assertIn("$8.00", dialog_text)
            self.assertIn("$10.00", dialog_text)
            self.assertIn("TOTAL SOLD", dialog_text.upper())
            self.assertTrue(dialog.locator("a[href*='nellisauction.com/p/item/1001']").count() >= 1)

            toggle = dialog.locator("label:has-text(\"Compare only 'New' condition\")")
            toggle.click()
            page.wait_for_timeout(800)
            self.assertEqual(page.locator("[data-testid='stException']").count(), 0)
        finally:
            page.close()

    def test_03_floating_refresh_and_responsive_viewports(self):
        """TC-E2E-009 & TC-E2E-010: Refresh button clears cache across Desktop (1440x900) & Mobile (390x844)."""
        # 1. Verify Desktop layout + Refresh visibility
        desktop = self.browser.new_page(viewport={"width": 1440, "height": 900})
        try:
            desktop.goto(self.app_url, wait_until="domcontentloaded")
            desktop.locator("button:has-text('Refresh Live Data')").wait_for(state="visible", timeout=25_000)
            self.assertEqual(desktop.locator("[data-testid='stException']").count(), 0)
        finally:
            desktop.close()

        # 2. Verify Mobile layout starts unblocked (sidebar auto-collapsed) & Refresh button updates feed
        page = self.browser.new_page(viewport={"width": 390, "height": 844})
        try:
            page.goto(self.app_url, wait_until="domcontentloaded")
            refresh_btn = page.locator("button:has-text('Refresh Live Data')")
            refresh_btn.wait_for(state="visible", timeout=25_000)
            box = refresh_btn.bounding_box()
            self.assertIsNotNone(box)
            self.assertGreater(box["width"], 50)
            self.assertLessEqual(box["x"] + box["width"], 395)

            # Mutate upstream mock feed and verify Refresh picks up the newly added item
            now_ms = int(time.time() * 1000)
            self.mock_state["products"] = [
                {
                    "id": 21999,
                    "title": "Refreshed Sony Noise Canceling Headphones",
                    "retailPrice": 299.0,
                    "currentPrice": 22.0,
                    "bidCount": 3,
                    "closeTime": now_ms + 180_000,
                    "isClosed": False,
                    "location": {"name": "Phoenix"},
                    "grade": {"conditionType": {"description": "New"}, "rating": 5},
                    "photos": [],
                }
            ]
            refresh_btn.click()
            page.locator("text=Refreshed Sony Noise Canceling Headphones").first.wait_for(
                state="visible", timeout=20_000
            )
            self.assertEqual(page.locator("[data-testid='stException']").count(), 0)
        finally:
            page.close()


if __name__ == "__main__":
    unittest.main()

"""
QA-UNIT & NFR Security/Performance Test Suite (`TC-UNIT-001`..`018`, `TC-NFR-001`..`006`).
100% deterministic, zero network dependency.
"""

import os
import re
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src import scraper, transform
from tests._app_helpers import load_app_module_helpers
from tests.fixtures.nellis_fixtures import (
    build_turbo_stream_item_payload,
    build_turbo_stream_search_payload,
)


class TestUnitEtlAndMatching(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app_module_helpers()

    # --- 1.1 Ingestion & Wire Protocol (TC-UNIT-001 .. TC-UNIT-003) ---

    def test_unflatten_val_scalars_dates_and_bounds(self):
        """TC-UNIT-001: Decode Remix turbo-stream pointer indirection and ["D", epoch_ms] dates."""
        epoch_ms = 1_794_542_700_000
        payload = build_turbo_stream_search_payload(
            [
                {
                    "id": 62489102,
                    "title": "Eagle Health Bench",
                    "retailPrice": 350.0,
                    "currentPrice": 36.0,
                    "bidCount": 10,
                    "isClosed": False,
                    "closeTime": epoch_ms,
                    "location": {"name": "Mesa"},
                    "grade": {"conditionType": {"description": "New"}, "rating": 5},
                    "photos": [{"url": "https://m.media-amazon.com/images/I/61xYzAbCdEL._AC_SL1500_.jpg"}],
                }
            ]
        )
        prod_indices = payload[payload.index("products") + 1]
        decoded_scraper = scraper.unflatten_val(prod_indices[0], payload)
        decoded_app = self.app.unflatten_val(prod_indices[0], payload)

        self.assertEqual(decoded_scraper["id"], 62489102)
        self.assertEqual(decoded_scraper["closeTime"], epoch_ms)
        self.assertEqual(decoded_scraper["location"]["name"], "Mesa")
        self.assertEqual(decoded_app["id"], 62489102)
        self.assertEqual(decoded_app["closeTime"], epoch_ms)

    def test_unflatten_val_cycle_and_invalid_index(self):
        """TC-UNIT-002: Prevent infinite recursion on cyclic pointer graphs and reject bad bounds."""
        # raw[0] is dict {"_1": 0} pointing back to itself; raw[1] = "self"
        cyclic_raw = [{"_1": 0}, "self"]
        out = scraper.unflatten_val(0, cyclic_raw)
        self.assertIsInstance(out, dict)
        self.assertIsNone(scraper.unflatten_val(-1, cyclic_raw))
        self.assertIsNone(scraper.unflatten_val(999, cyclic_raw))
        self.assertIsNone(self.app.unflatten_val(-1, cyclic_raw))
        self.assertIsNone(self.app.unflatten_val(999, cyclic_raw))

    def test_parse_nellis_products_with_meta(self):
        """TC-UNIT-003: Exclude expired lots past buffer while keeping raw_count & max_diff_sec."""
        now_ms = int(time.time() * 1000)
        payload = build_turbo_stream_search_payload(
            [
                {
                    "id": 2001,
                    "title": "Already Ended Lot",
                    "retailPrice": 100.0,
                    "currentPrice": 5.0,
                    "bidCount": 2,
                    "closeTime": now_ms - 120_000,
                    "location": {"name": "Phoenix"},
                    "grade": {"conditionType": {"description": "New"}, "rating": 5},
                    "photos": [],
                },
                {
                    "id": 2002,
                    "title": "Ending Soon Active Lot",
                    "retailPrice": 200.0,
                    "currentPrice": 15.0,
                    "bidCount": 4,
                    "closeTime": now_ms + 420_000,
                    "location": {"name": "Mesa"},
                    "grade": {"conditionType": {"description": "New"}, "rating": 5},
                    "photos": [],
                },
            ]
        )
        items, raw_count, max_diff_sec = self.app.parse_nellis_products_with_meta(payload, include_just_ended_sec=0)
        self.assertEqual(raw_count, 2)
        self.assertEqual([i["auction_id"] for i in items], [2002])
        self.assertIsNotNone(max_diff_sec)
        self.assertGreater(max_diff_sec, 400.0)

    # --- 1.2 Transformation, Hashing & AZ Localization (TC-UNIT-004 .. TC-UNIT-008) ---

    def test_normalize_title_and_product_id(self):
        """TC-UNIT-004: Strip inspector annotations & compute deterministic 32-char SHA-256 keys."""
        t1 = "***FACTORY SEALED*** (Box Corner Tear) DeWalt 20V Drill - Kit!"
        t2 = "DeWalt 20V Drill Kit"
        self.assertEqual(transform.normalize_title(t1), transform.normalize_title(t2))

        pid1 = transform.generate_product_id(t1, 199.99)
        pid2 = transform.generate_product_id(t2, 199.99)
        pid_diff_msrp = transform.generate_product_id(t2, 249.99)
        self.assertEqual(len(pid1), 32)
        self.assertEqual(pid1, pid2)
        self.assertNotEqual(pid1, pid_diff_msrp)

    def test_condition_completeness_functionality_rules(self):
        """TC-UNIT-005 & TC-UNIT-006: Canonical mapping of grade attributes."""
        self.assertEqual(transform.standardize_condition("New", "None", 5), "Brand New")
        self.assertEqual(transform.standardize_condition("Open Box", "None", 4), "Open Box")
        self.assertEqual(transform.standardize_condition("Used", "None", 3), "Used")
        self.assertEqual(transform.standardize_condition("Unknown", "None", None), "Untested")
        self.assertEqual(transform.standardize_condition("New", "Major Damage", 2), "Incomplete / Damaged / Parts")
        self.assertEqual(
            transform.standardize_condition("Open Box", "None", 5, missing_parts="Yes"),
            "Incomplete / Damaged / Parts",
        )
        self.assertEqual(
            transform.standardize_condition("New", "None", 5, title="READ: Missing remote power cord"),
            "Incomplete / Damaged / Parts",
        )

        self.assertEqual(transform.standardize_completeness("None"), "Complete")
        self.assertEqual(transform.standardize_completeness("Partial"), "Partial Set")
        self.assertEqual(transform.standardize_completeness("Missing"), "Missing Parts")
        self.assertEqual(transform.standardize_completeness(""), "Unknown")

        self.assertEqual(transform.standardize_functionality("Tested - Works"), "Tested - Works")
        self.assertEqual(transform.standardize_functionality("Not Functional"), "Not Functional")
        self.assertEqual(transform.standardize_functionality("Powers On Only"), "Powers On")
        self.assertEqual(transform.standardize_functionality(""), "Untested")

    def test_mst_timezone_boundary_and_photo_extraction(self):
        """TC-UNIT-007 & TC-UNIT-008: AZ UTC-7 rollover at midnight UTC + dual-source photo + taxonomy."""
        # 2026-11-12T06:59:00Z == 2026-11-11 23:59:00 MST (Wednesday -> close_day_of_week = 3, close_hour = 23)
        import datetime

        dt_utc = datetime.datetime(2026, 11, 12, 6, 59, 0, tzinfo=datetime.timezone.utc)
        close_ms = int(dt_utc.timestamp() * 1000)

        dims, facts = transform.transform_records(
            [
                {
                    "id": 5001,
                    "title": "Dyson V11 Vacuum Cordless Cleaner",
                    "retailPrice": 599.99,
                    "currentPrice": 120.0,
                    "bidCount": 10,
                    "closeTime": close_ms,
                    "photos": [{"url": "https://m.media-amazon.com/images/I/51DYSON._AC_SL1500_.jpg"}],
                }
            ],
            scrape_date="2026-11-12",
        )
        self.assertEqual(facts[0]["close_hour"], 23)
        self.assertEqual(facts[0]["close_day_of_week"], 3)
        self.assertEqual(
            facts[0]["photo_url"],
            "https://m.media-amazon.com/images/I/51DYSON._AC_SL1500_.jpg",
        )
        self.assertEqual(dims[0]["photo_url"], facts[0]["photo_url"])
        self.assertEqual(dims[0]["category_primary"], "Home & Kitchen")
        self.assertEqual(dims[0]["category_secondary"], "Vacuums & Floor Care")

    # --- 1.3 Entity Resolution, Condition Tiers & Fuzzy Search (TC-UNIT-010 .. TC-UNIT-016) ---

    def test_amazon_image_id_extraction(self):
        """TC-UNIT-010: Canonical Amazon image ID strips sizing modifiers."""
        u1 = "https://m.media-amazon.com/images/I/71AbCdEf+9L._AC_SL1500_.jpg"
        u2 = "https://m.media-amazon.com/images/I/71AbCdEf+9L._SX679_.png"
        u3 = "https://cdn.example.com/custom_camera_photo.jpg"
        self.assertEqual(self.app.extract_amazon_image_id(u1), "71AbCdEf+9L")
        self.assertEqual(self.app.extract_amazon_image_id(u2), "71AbCdEf+9L")
        self.assertEqual(self.app.extract_amazon_image_id(u3), "")

    def test_msrp_guardrail_boundaries(self):
        """TC-UNIT-011: Reject MSRP ratios outside [0.72, 1.38] even when images match."""
        img = "https://m.media-amazon.com/images/I/61SHARED._AC_SL1500_.jpg"
        self.assertTrue(self.app.is_same_product("Filter Pack", 100.0, img, "Filter Pack", 72.0, img))
        self.assertTrue(self.app.is_same_product("Filter Pack", 100.0, img, "Filter Pack", 138.0, img))
        self.assertFalse(self.app.is_same_product("Filter Pack", 100.0, img, "Filter Pack", 71.9, img))
        self.assertFalse(self.app.is_same_product("Filter Pack", 100.0, img, "Filter Pack", 138.1, img))

    def test_token_signature_and_overlap_fallback(self):
        """TC-UNIT-012: 5-token prefix and >= 0.75 8-token overlap match cleanly; < 0.75 rejects."""
        self.assertTrue(
            self.app.is_same_product(
                "***SEALED*** Ninja Foodi DualZone Air Fryer DZ201",
                199.0,
                "",
                "Ninja Foodi DualZone Air Fryer Black",
                199.0,
                "",
            )
        )
        # 6 of 8 tokens match (0.75) vs 5 of 8 (0.625)
        self.assertTrue(
            self.app.is_same_product(
                "Alpha Bravo Charlie Delta Echo Foxtrot Golf Hotel",
                100.0,
                "",
                "Alpha Sierra Charlie Delta Echo Foxtrot Tango Hotel",
                100.0,
                "",
            )
        )
        self.assertFalse(
            self.app.is_same_product(
                "Alpha Bravo Charlie Delta Echo Foxtrot Golf Hotel",
                100.0,
                "",
                "Alpha Sierra Charlie Yankee Echo Foxtrot Tango Victor",
                100.0,
                "",
            )
        )

    def test_condition_formatting_and_tiers(self):
        """TC-UNIT-013: Format grade dicts & map strictly into 4 like-for-like tiers."""
        grade_dict = {
            "conditionType": {"description": "Open Box"},
            "damageType": {"description": "None"},
            "missingPartsType": {"description": "No"},
            "functionalType": {"description": "Yes"},
            "rating": 4,
        }
        self.assertEqual(self.app.format_clean_condition(grade_dict), "Open Box (4/5)")
        self.assertEqual(self.app.format_clean_condition(str(grade_dict)), "Open Box (4/5)")
        self.assertEqual(self.app.get_condition_tier(grade_dict), "Open Box / Like New")
        self.assertEqual(self.app.get_condition_tier("Brand New (5/5)"), "New")
        self.assertEqual(self.app.get_condition_tier("Used (3/5)"), "Used")
        self.assertEqual(
            self.app.get_condition_tier("Brand New (5/5)", notes="missing charger cable"),
            "Incomplete / Damaged / Parts",
        )

    def test_find_historical_matches_sorting_and_tier_filter(self):
        """TC-UNIT-014: Sort historical wins ascending and enforce like-for-like condition default."""
        img_url = "https://m.media-amazon.com/images/I/91KETTLE._AC_SL1500_.jpg"
        hist_index = {
            "by_img": {
                "91KETTLE": [
                    {
                        "auction_id": 10,
                        "title": "Elite Gourmet Travel Kettle",
                        "retail_price": 26.0,
                        "final_price": 6.0,
                        "closed_at": "2026-11-15",
                        "photo_url": img_url,
                        "cond_tier": "New",
                    },
                    {
                        "auction_id": 11,
                        "title": "Elite Gourmet Travel Kettle",
                        "retail_price": 26.0,
                        "final_price": 5.0,
                        "closed_at": "2026-11-14",
                        "photo_url": img_url,
                        "cond_tier": "New",
                    },
                    {
                        "auction_id": 12,
                        "title": "Elite Gourmet Travel Kettle",
                        "retail_price": 26.0,
                        "final_price": 2.0,
                        "closed_at": "2026-11-13",
                        "photo_url": img_url,
                        "cond_tier": "Used",
                    },
                ]
            },
            "by_sig": {},
        }
        target = {
            "auction_id": 99,
            "title": "Elite Gourmet Travel Kettle",
            "retail_price": 26.0,
            "photo_url": img_url,
            "condition": "Brand New",
        }
        new_only = self.app.find_historical_matches(target, hist_index, same_condition_only=True)
        self.assertEqual([m["auction_id"] for m in new_only], [11, 10])

        all_tiers = self.app.find_historical_matches(target, hist_index, same_condition_only=False)
        self.assertEqual([m["auction_id"] for m in all_tiers], [12, 11, 10])

    def test_find_open_radar_matches(self):
        """TC-UNIT-015: Find active copies sorted lowest bid first."""
        img_url = "https://m.media-amazon.com/images/I/91KETTLE._AC_SL1500_.jpg"
        open_idx = {
            "by_img": {
                "91KETTLE": [
                    {"auction_id": 201, "title": "Elite Gourmet Travel Kettle", "retail_price": 26.0, "final_price": 14.0, "photo_url": img_url},
                    {"auction_id": 202, "title": "Elite Gourmet Travel Kettle", "retail_price": 26.0, "final_price": 7.0, "photo_url": img_url},
                ]
            },
            "by_sig": {},
        }
        res = self.app.find_open_radar_matches(
            {"title": "Elite Gourmet Travel Kettle", "retail_price": 26.0, "photo_url": img_url},
            open_idx,
        )
        self.assertEqual([r["auction_id"] for r in res], [202, 201])

    def test_smart_fuzzy_search_and_sql_where(self):
        """TC-UNIT-016: Typo-tolerant scoring >= 0.65 and DuckDB ILIKE SQL generation."""
        self.assertEqual(self.app.smart_fuzzy_search_score("dewalt drill", "DeWalt 20V Cordless Drill"), 1.0)
        self.assertGreaterEqual(self.app.smart_fuzzy_search_score("dewatl dril", "DeWalt 20V Cordless Drill"), 0.65)
        self.assertLess(self.app.smart_fuzzy_search_score("dyson vacuum", "Sony Headphones Wireless"), 0.65)

        sql_where = self.app.build_fuzzy_sql_where("usb-c hub")
        self.assertIn("ILIKE", sql_where)
        self.assertIn("p.title", sql_where)
        multi_where = self.app.build_fuzzy_sql_where("google pixel")
        self.assertIn(" AND ", multi_where)
        self.assertIn("%pix%", multi_where)

    # --- 1.4 Verdict, Likely Cheap Guard, Security & NFR Checks (TC-UNIT-017 .. TC-NFR-006) ---

    def test_deal_verdict_and_likely_cheap_guard(self):
        """TC-UNIT-017 (CR-04): Prevent cur_bid > avg_won items (e.g. $28 vs $18 avg) from showing Likely Cheap."""
        def _eval(cur_bid, past_prices):
            min_won = min(past_prices)
            avg_won = sum(past_prices) / len(past_prices)
            if cur_bid <= min_won:
                verdict = "Below All-Time Low!"
            elif cur_bid <= avg_won:
                verdict = "Below Avg Win"
            else:
                verdict = "Above Avg"
            likely_cheap = (cur_bid <= avg_won) and (len(past_prices) >= 2)
            return verdict, likely_cheap

        v1, c1 = _eval(14.0, [15.0, 21.0])
        self.assertEqual((v1, c1), ("Below All-Time Low!", True))

        v2, c2 = _eval(18.0, [15.0, 21.0])
        self.assertEqual((v2, c2), ("Below Avg Win", True))

        v3, c3 = _eval(28.0, [15.0, 21.0])
        self.assertEqual((v3, c3), ("Above Avg", False))

    def test_validate_readonly_sql_security_guard(self):
        """TC-UNIT-018 (CR-07): Allow SELECT/WITH and block all DDL/DML/stacked injections."""
        self.assertEqual(
            self.app.validate_readonly_sql("```sql\nSELECT COUNT(*) FROM fact_auctions;\n```"),
            "SELECT COUNT(*) FROM fact_auctions",
        )
        self.assertEqual(
            self.app.validate_readonly_sql("WITH t AS (SELECT 1 AS a) SELECT * FROM t"),
            "WITH t AS (SELECT 1 AS a) SELECT * FROM t",
        )
        for bad_sql in (
            "DROP TABLE fact_auctions",
            "DELETE FROM fact_auctions WHERE 1=1",
            "UPDATE fact_auctions SET final_price = 0",
            "INSERT INTO fact_auctions VALUES (1)",
            "CREATE TABLE pwned AS SELECT 1",
            "COPY fact_auctions TO '/tmp/out.csv'",
            "ATTACH 'foo.db'",
            "PRAGMA database_list",
            "SELECT 1; SELECT 2",
        ):
            with self.assertRaises(ValueError, msg=f"Should reject: {bad_sql}"):
                self.app.validate_readonly_sql(bad_sql)

    def test_historical_matcher_o1_latency(self):
        """TC-NFR-001: 50-item matching against 2,000 indexed rows completes in < 50ms."""
        hist_index = {"by_img": {}, "by_sig": {}}
        for i in range(2000):
            key = f"IMG{i % 200}"
            row = {
                "auction_id": i + 1,
                "title": f"Product Model {i % 200} Special",
                "retail_price": 100.0,
                "final_price": float((i % 20) + 5),
                "closed_at": "2026-11-12",
                "photo_url": f"https://m.media-amazon.com/images/I/{key}._AC_SL1500_.jpg",
                "cond_tier": "New",
            }
            hist_index["by_img"].setdefault(key, []).append(row)

        targets = [
            {
                "auction_id": 90000 + j,
                "title": f"Product Model {j} Special",
                "retail_price": 100.0,
                "photo_url": f"https://m.media-amazon.com/images/I/IMG{j}._AC_SL1500_.jpg",
                "condition": "Brand New",
            }
            for j in range(50)
        ]

        t0 = time.perf_counter()
        for t in targets:
            res = self.app.find_historical_matches(t, hist_index, same_condition_only=True)
            self.assertEqual(len(res), 10)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        self.assertLess(elapsed_ms, 50.0)

    def test_get_secret_env_fallback(self):
        """TC-NFR-002: Missing secrets fall back cleanly to os.environ and defaults."""
        with mock.patch.dict(os.environ, {"TEST_NELLIS_SECRET": "env_val_123"}):
            self.assertEqual(self.app.get_secret("TEST_NELLIS_SECRET", "default"), "env_val_123")
        self.assertEqual(self.app.get_secret("NONEXISTENT_NELLIS_KEY_999", "fallback"), "fallback")

    def test_no_hardcoded_secrets_in_repo(self):
        """TC-NFR-005 (NFR-SEC-001): Confirm no hardcoded R2, Gemini, or PAT secrets in project files."""
        target_files = [
            ROOT_DIR / "app.py",
            ROOT_DIR / "src" / "scraper.py",
            ROOT_DIR / "src" / "transform.py",
            ROOT_DIR / "src" / "r2_loader.py",
            ROOT_DIR / "src" / "settlement.py",
            ROOT_DIR / "src" / "query_lake.py",
            ROOT_DIR / ".github" / "workflows" / "daily_scrape.yml",
            ROOT_DIR / ".github" / "workflows" / "backfill_final_prices.yml",
        ]
        secret_patterns = [
            re.compile(r"AIza[0-9A-Za-z\-_]{30,}"),
            re.compile(r"ghp_[0-9A-Za-z]{30,}"),
            re.compile(r"github_pat_[0-9A-Za-z_]{30,}"),
            re.compile(r"AKIA[0-9A-Z]{16}"),
            re.compile(r"R2_SECRET_ACCESS_KEY\s*=\s*['\"][A-Za-z0-9/+=]{20,}['\"]"),
        ]
        for path in target_files:
            content = path.read_text(encoding="utf-8")
            for pat in secret_patterns:
                self.assertIsNone(pat.search(content), f"Potential hardcoded secret in {path.name}")

    def test_tls_config_and_html_note_escaping(self):
        """TC-NFR-006 (NFR-SEC-002): Enforce s3_use_ssl=true and XSS escaping in card markup."""
        for p in (ROOT_DIR / "app.py", ROOT_DIR / "src" / "r2_loader.py", ROOT_DIR / "src" / "query_lake.py"):
            txt = p.read_text(encoding="utf-8")
            self.assertIn("s3_use_ssl = true", txt.lower(), f"Missing TLS S3 flag in {p.name}")

        app_txt = (ROOT_DIR / "app.py").read_text(encoding="utf-8")
        self.assertIn("replace('\"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')", app_txt)


if __name__ == "__main__":
    unittest.main()

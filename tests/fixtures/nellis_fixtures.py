"""
Frozen realistic Nellis Auction wire-format builders (Remix turbo-stream)
and deterministic local Parquet Data Lake seeders for offline QA/UAT tests.
"""

import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import threading
import time
from typing import Dict, List, Optional

import duckdb

from src.settlement import MST_TZ, build_bid_analytics
from src.r2_loader import (
    enforce_fact_auctions_schema,
    enforce_fact_bids_schema,
    merge_dim_products,
    write_fact_auctions,
    write_fact_bids,
)
from src.transform import transform_records


def encode_turbo_stream_value(val, buf: list):
    """
    Serialize a nested Python object/list/primitive into Remix / React Router
    v7 turbo-stream flat array representation (including ["D", epoch_ms] dates).
    """
    if isinstance(val, tuple) and len(val) == 2 and val[0] == "D":
        idx = len(buf)
        buf.append(["D", int(val[1])])
        return idx
    if isinstance(val, dict):
        d_idx = len(buf)
        encoded_dict = {}
        buf.append(encoded_dict)
        for k, v in val.items():
            k_idx = len(buf)
            buf.append(str(k))
            v_idx = encode_turbo_stream_value(v, buf)
            encoded_dict[f"_{k_idx}"] = v_idx
        return d_idx
    if isinstance(val, list):
        l_idx = len(buf)
        item_indices = []
        buf.append(item_indices)
        for elem in val:
            item_indices.append(encode_turbo_stream_value(elem, buf))
        return l_idx
    idx = len(buf)
    buf.append(val)
    return idx


def build_turbo_stream_search_payload(products: List[Dict], total_count: Optional[int] = None) -> list:
    """Build a valid `/search.data` flat turbo-stream JSON array for test mocks."""
    buf: list = [
        "searchResultsCount",
        int(total_count if total_count is not None else len(products)),
        "products",
    ]
    prod_indices_pos = len(buf)
    buf.append([])
    prod_indices = []
    for prod in products:
        p_copy = dict(prod)
        if "closeTime" in p_copy and isinstance(p_copy["closeTime"], (int, float)):
            p_copy["closeTime"] = ("D", int(p_copy["closeTime"]))
        if "openTime" in p_copy and isinstance(p_copy["openTime"], (int, float)):
            p_copy["openTime"] = ("D", int(p_copy["openTime"]))
        if "initialCloseTime" in p_copy and isinstance(p_copy["initialCloseTime"], (int, float)):
            p_copy["initialCloseTime"] = ("D", int(p_copy["initialCloseTime"]))
        prod_indices.append(encode_turbo_stream_value(p_copy, buf))
    buf[prod_indices_pos] = prod_indices
    return buf


def build_turbo_stream_item_payload(product: Dict, bid_history: Optional[List[Dict]] = None) -> list:
    """Build a valid `/p/item/{id}.data` flat turbo-stream JSON array for settlement mocks."""
    buf: list = ["product"]
    p_pos = len(buf)
    buf.append(None)
    p_copy = dict(product)
    for dt_k in ("closeTime", "openTime", "initialCloseTime"):
        if dt_k in p_copy and isinstance(p_copy[dt_k], (int, float)):
            p_copy[dt_k] = ("D", int(p_copy[dt_k]))
    buf[p_pos] = p_pos + 1
    encode_turbo_stream_value(p_copy, buf)

    if bid_history is not None:
        buf.append("bidHistory")
        bh_idx_pos = len(buf)
        buf.append(None)
        hist_encoded = encode_turbo_stream_value(bid_history, buf)
        buf[bh_idx_pos] = hist_encoded
    return buf


def seed_local_data_lake(lake_dir: str) -> Dict:
    """
    Populate a realistic local Star Schema Data Lake inside `lake_dir` for integration & UI tests:
      - Partition 1 (7 days ago): legacy 17-column schema (pre-upgrade)
      - Partition 2 (yesterday / today): full 31-column settled schema + fact_bids + 1 active future lot
      - dim_products catalog
    """
    prev_cwd = os.getcwd()
    parent_dir = os.path.dirname(os.path.abspath(lake_dir))
    base_name = os.path.basename(os.path.abspath(lake_dir))
    if base_name != "data_lake":
        raise ValueError("lake_dir must end with 'data_lake' so r2_loader writes cleanly underneath.")

    os.makedirs(lake_dir, exist_ok=True)
    now_dt = datetime.datetime.now(MST_TZ)
    today_str = now_dt.strftime("%Y-%m-%d")
    yest_dt = now_dt - datetime.timedelta(hours=12)
    old_dt = now_dt - datetime.timedelta(days=5)
    old_str = old_dt.strftime("%Y-%m-%d")

    old_close_ms = int(old_dt.timestamp() * 1000)
    yest_close_ms = int(yest_dt.timestamp() * 1000)
    future_close_ms = int((now_dt + datetime.timedelta(hours=4)).timestamp() * 1000)

    raw_items = [
        # Lot 1001: Brand New OBD2 Scanner settled sold ($8, was extended +15s, MSRP $40 -> 80% off)
        {
            "id": 1001,
            "title": "UMEIJA V011 OBD2 Scanner Diagnostic Tool",
            "retailPrice": 40.0,
            "currentPrice": 8.0,
            "bidCount": 4,
            "bidderCount": 3,
            "openTime": yest_close_ms - 86_400_000,
            "initialCloseTime": yest_close_ms - 15_000,
            "closeTime": yest_close_ms,
            "condition": "Brand New",
            "grade": {"conditionType": {"description": "New"}, "damageType": {"description": "None"}, "missingPartsType": {"description": "No"}, "functionalType": {"description": "Yes"}, "rating": 5},
            "locationName": "Phoenix",
            "photos": [{"url": "https://m.media-amazon.com/images/I/71OBD2TEST._AC_SL1500_.jpg"}],
            "isClosed": True,
            "marketStatus": "sold",
        },
        # Lot 1002: Brand New OBD2 Scanner settled sold ($12)
        {
            "id": 1002,
            "title": "***SEALED*** UMEIJA V011 OBD2 Scanner Diagnostic Tool",
            "retailPrice": 40.0,
            "currentPrice": 12.0,
            "bidCount": 5,
            "bidderCount": 4,
            "openTime": yest_close_ms - 86_400_000,
            "initialCloseTime": yest_close_ms - 300_000,
            "closeTime": yest_close_ms - 300_000,
            "condition": "Brand New",
            "grade": {"conditionType": {"description": "New"}, "damageType": {"description": "None"}, "missingPartsType": {"description": "No"}, "functionalType": {"description": "Yes"}, "rating": 5},
            "locationName": "Mesa",
            "photos": [{"url": "https://m.media-amazon.com/images/I/71OBD2TEST._SX679_.jpg"}],
            "isClosed": True,
            "marketStatus": "sold",
        },
        # Lot 1003: High-discount Brand New Sleeper ($150 MSRP, won for $6 -> 96% off, 2 bids)
        {
            "id": 1003,
            "title": "DeWalt 20V MAX Brushless Drill Kit",
            "retailPrice": 150.0,
            "currentPrice": 6.0,
            "bidCount": 2,
            "bidderCount": 2,
            "openTime": yest_close_ms - 86_400_000,
            "initialCloseTime": yest_close_ms - 600_000,
            "closeTime": yest_close_ms - 600_000,
            "condition": "Brand New",
            "grade": {"conditionType": {"description": "New"}, "damageType": {"description": "None"}, "missingPartsType": {"description": "No"}, "functionalType": {"description": "Yes"}, "rating": 5},
            "locationName": "Phoenix",
            "photos": [{"url": "https://m.media-amazon.com/images/I/81DEWALTTEST._AC_SL1500_.jpg"}],
            "isClosed": True,
            "marketStatus": "sold",
        },
        # Lot 1004: Relisted / 0-bid lot that must be EXCLUDED from winning-price analytics
        {
            "id": 1004,
            "title": "UMEIJA V011 OBD2 Scanner Diagnostic Tool",
            "retailPrice": 40.0,
            "currentPrice": 1.0,
            "bidCount": 0,
            "bidderCount": 0,
            "openTime": yest_close_ms - 86_400_000,
            "initialCloseTime": yest_close_ms - 900_000,
            "closeTime": yest_close_ms - 900_000,
            "condition": "Brand New",
            "grade": {"conditionType": {"description": "New"}, "damageType": {"description": "None"}, "missingPartsType": {"description": "No"}, "functionalType": {"description": "Yes"}, "rating": 5},
            "locationName": "Phoenix",
            "photos": [{"url": "https://m.media-amazon.com/images/I/71OBD2TEST._AC_SL1500_.jpg"}],
            "isClosed": True,
            "marketStatus": "relisted",
        },
        # Lot 1005: Future open active copy of DeWalt Drill ($25 currently) for Open Radar & Active Lake Union
        {
            "id": 1005,
            "title": "DeWalt 20V MAX Brushless Drill Kit",
            "retailPrice": 150.0,
            "currentPrice": 25.0,
            "bidCount": 6,
            "bidderCount": 4,
            "openTime": yest_close_ms,
            "initialCloseTime": future_close_ms,
            "closeTime": future_close_ms,
            "condition": "Brand New",
            "grade": {"conditionType": {"description": "New"}, "damageType": {"description": "None"}, "missingPartsType": {"description": "No"}, "functionalType": {"description": "Yes"}, "rating": 5},
            "locationName": "Mesa",
            "photos": [{"url": "https://m.media-amazon.com/images/I/81DEWALTTEST._AC_SL1500_.jpg"}],
            "isClosed": False,
            "marketStatus": "active",
        },
    ]

    os.chdir(parent_dir)
    try:
        dim_records, fact_records = transform_records(raw_items, scrape_date=today_str)

        # Enrich lot 1001 & 1002 & 1003 with real bid curves & settled flags
        bid_payloads = {
            1001: {
                "auction_id": 1001,
                "open_ms": yest_close_ms - 86_400_000,
                "initial_close_ms": yest_close_ms - 15_000,
                "close_ms": yest_close_ms,
                "final_price": 8.0,
                "bid_count": 4,
                "bid_history": [
                    {"name": "Bidder #1", "amount": "$1", "time": yest_close_ms - 43_200_000, "type": "Outbid"},
                    {"name": "Bidder #2", "amount": "$4", "time": yest_close_ms - 1_800_000, "type": "Outbid"},
                    {"name": "Bidder #3", "amount": "$6", "time": yest_close_ms - 120_000, "type": "Outbid"},
                    {"name": "Bidder #1", "amount": "$8", "time": yest_close_ms - 5_000, "type": "Winning"},
                ],
            },
            1002: {
                "auction_id": 1002,
                "open_ms": yest_close_ms - 86_400_000,
                "initial_close_ms": yest_close_ms - 300_000,
                "close_ms": yest_close_ms - 300_000,
                "final_price": 12.0,
                "bid_count": 3,
                "bid_history": [
                    {"name": "Bidder #1", "amount": "$2", "time": yest_close_ms - 7_200_000, "type": "Outbid"},
                    {"name": "Bidder #2", "amount": "$9", "time": yest_close_ms - 500_000, "type": "Outbid"},
                    {"name": "Bidder #3", "amount": "$12", "time": yest_close_ms - 320_000, "type": "Winning"},
                ],
            },
            1003: {
                "auction_id": 1003,
                "open_ms": yest_close_ms - 86_400_000,
                "initial_close_ms": yest_close_ms - 600_000,
                "close_ms": yest_close_ms - 600_000,
                "final_price": 6.0,
                "bid_count": 2,
                "bid_history": [
                    {"name": "Bidder #1", "amount": "$3", "time": yest_close_ms - 3_600_000, "type": "Outbid"},
                    {"name": "Bidder #2", "amount": "$6", "time": yest_close_ms - 640_000, "type": "Winning"},
                ],
            },
        }

        all_bids = []
        for f_rec in fact_records:
            aid = int(f_rec["auction_id"])
            if aid in bid_payloads:
                b_rows, summary = build_bid_analytics(bid_payloads[aid], product_id=f_rec["product_id"], scraped_date=today_str)
                f_rec.update(summary)
                all_bids.extend(b_rows)

        merge_dim_products(dim_records, use_r2=False)
        write_fact_auctions(fact_records, scrape_date=today_str, use_r2=False)
        write_fact_bids(all_bids, partition_date=today_str, use_r2=False)

        # Also write a legacy 17-column partition (dated 5 days ago) to test mixed-schema union_by_name=true
        legacy_dim, legacy_facts = transform_records(
            [
                {
                    "id": 9001,
                    "title": "Dyson V8 Origin Cordless Vacuum",
                    "retailPrice": 399.99,
                    "currentPrice": 65.0,
                    "bidCount": 14,
                    "closeTime": old_close_ms,
                    "condition": "Used",
                    "locationName": "Mesa",
                    "photos": [{"url": "https://m.media-amazon.com/images/I/61DYSONTEST._AC_SL1500_.jpg"}],
                    "isClosed": True,
                    "marketStatus": "sold",
                }
            ],
            scrape_date=old_str,
        )
        merge_dim_products(legacy_dim, use_r2=False)
        legacy_df = enforce_fact_auctions_schema(
            __import__("pandas").DataFrame(legacy_facts)
        ).iloc[:, :17]
        legacy_dir = os.path.join(
            lake_dir,
            "fact_auctions",
            f"year={old_dt.strftime('%Y')}",
            f"month={old_dt.strftime('%m')}",
        )
        os.makedirs(legacy_dir, exist_ok=True)
        legacy_path = os.path.join(legacy_dir, f"auctions_{old_str}.parquet").replace("\\", "/")
        con = duckdb.connect()
        con.register("leg_df", legacy_df)
        con.execute(f"COPY leg_df TO '{legacy_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');")
        con.close()
    finally:
        os.chdir(prev_cwd)

    return {
        "today_str": today_str,
        "old_str": old_str,
        "fact_count": len(fact_records) + 1,
        "bid_count": len(all_bids),
    }


def start_mock_nellis_server(
    now_ms: Optional[int] = None,
    mode: str = "normal",
    custom_products: Optional[List[Dict]] = None,
):
    """
    Start an ephemeral localhost HTTP server stubbing Nellis `/change-shopping-location`,
    `/search.data`, and `/p/item/{id}.data`. Returns `(server, state_dict)`.
    """
    base_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    default_products = [
        {
            "id": 20001,
            "title": "UMEIJA V011 OBD2 Scanner Diagnostic Tool",
            "retailPrice": 40.0,
            "currentPrice": 5.0,
            "bidCount": 2,
            "closeTime": base_ms + 360_000,
            "isClosed": False,
            "location": {"name": "Phoenix"},
            "grade": {"conditionType": {"description": "New"}, "rating": 5},
            "photos": [{"url": "https://m.media-amazon.com/images/I/71OBD2TEST._AC_SL1500_.jpg"}],
        },
        {
            "id": 20002,
            "title": "UMEIJA V011 OBD2 Scanner Diagnostic Tool",
            "retailPrice": 40.0,
            "currentPrice": 28.0,
            "bidCount": 11,
            "closeTime": base_ms + 480_000,
            "isClosed": False,
            "location": {"name": "Mesa"},
            "grade": {"conditionType": {"description": "New"}, "rating": 5},
            "photos": [{"url": "https://m.media-amazon.com/images/I/71OBD2TEST._AC_SL1500_.jpg"}],
        },
        {
            "id": 20003,
            "title": "Late Closing Lamp",
            "retailPrice": 80.0,
            "currentPrice": 12.0,
            "bidCount": 3,
            "closeTime": base_ms + 2_400_000,
            "isClosed": False,
            "location": {"name": "Phoenix"},
            "grade": {"conditionType": {"description": "New"}, "rating": 5},
            "photos": [],
        },
    ]
    state = {
        "mode": mode,
        "products": custom_products if custom_products is not None else default_products,
    }

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return

        def do_POST(self):
            self.send_response(200)
            self.send_header("Set-Cookie", "location=phoenix")
            self.end_headers()
            self.wfile.write(b"{}")

        def do_GET(self):
            if state["mode"] == "error":
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"upstream error")
                return
            if self.path.startswith("/search.data"):
                payload = build_turbo_stream_search_payload(state["products"])
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode("utf-8"))
                return
            if self.path.startswith("/p/item/"):
                payload = build_turbo_stream_item_payload(
                    {
                        "id": 1001,
                        "currentPrice": 8.0,
                        "bidCount": 4,
                        "isClosed": True,
                        "marketStatus": "sold",
                        "closeTime": base_ms - 600_000,
                    },
                    bid_history=[],
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode("utf-8"))
                return
            self.send_response(404)
            self.end_headers()

    srv = HTTPServer(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, state

#!/usr/bin/env python3
"""
Nellis Auction Post-Close Settlement & Final Price Verifier
Queries https://www.nellisauction.com/p/item/{auction_id}.data for closed auctions
to resolve true final WON FOR price, final bid count, marketStatus (sold/relisted),
anti-snipe closeTime, and inspection condition/notes.

Used both:
  - In real-time by app.py when opening Price History on unverified rows
  - In batch by daily ingestion (--settle-recent) & historical R2 backfill workflow
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import io
import json
import os
import time
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

from src.transform import (
    MST_TZ,
    standardize_completeness,
    standardize_condition,
    standardize_functionality,
)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def unflatten_stream_val(val, raw, memo=None):
    """Resolve React Router / Turbo-Stream graph indices with safe 1-hop scalar resolution."""
    if memo is None:
        memo = {}

    if isinstance(val, int) and not isinstance(val, bool):
        if val < 0 or val >= len(raw):
            return None
        if val in memo:
            return memo[val]
        raw_val = raw[val]
        if isinstance(raw_val, dict):
            res = {}
            memo[val] = res
            for k, v in raw_val.items():
                rk = raw[int(k[1:])] if k.startswith("_") and k[1:].isdigit() else k
                res[rk] = unflatten_stream_val(v, raw, memo)
            return res
        if isinstance(raw_val, list):
            if len(raw_val) == 2 and raw_val[0] == "D" and isinstance(raw_val[1], (int, float)):
                return raw_val[1]
            res = []
            memo[val] = res
            for item in raw_val:
                res.append(unflatten_stream_val(item, raw, memo))
            return res
        memo[val] = raw_val
        return raw_val

    if isinstance(val, dict):
        res = {}
        for k, v in val.items():
            rk = raw[int(k[1:])] if k.startswith("_") and k[1:].isdigit() else k
            res[rk] = unflatten_stream_val(v, raw, memo)
        return res

    if isinstance(val, list):
        if len(val) == 2 and val[0] == "D" and isinstance(val[1], (int, float)):
            return val[1]
        return [unflatten_stream_val(x, raw, memo) for x in val]

    return val


def _build_pooled_session(pool_size: int = 40) -> requests.Session:
    sess = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({"User-Agent": USER_AGENT})
    return sess


def fetch_closed_lot_result(auction_id: int, session: Optional[requests.Session] = None, timeout: int = 10) -> Optional[Dict]:
    """Fetch authoritative current/final state of a single lot from Nellis .data endpoint."""
    aid = int(auction_id)
    url = f"https://www.nellisauction.com/p/item/{aid}.data"
    owns_sess = session is None
    sess = session or _build_pooled_session(4)

    try:
        for attempt in range(2):
            try:
                resp = sess.get(url, timeout=timeout)
                if resp.status_code in (404, 410):
                    return {
                        "auction_id": aid,
                        "is_closed": True,
                        "market_status": "archived_unverified",
                        "not_found": True,
                    }
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, list):
                    return None

                for i, el in enumerate(data):
                    if el == "product" and i + 1 < len(data):
                        p = unflatten_stream_val(data[i + 1], data)
                        if not isinstance(p, dict):
                            return None

                        raw_close = p.get("closeTime")
                        if isinstance(raw_close, list) and raw_close:
                            raw_close = raw_close[0]
                        try:
                            close_ms = int(raw_close) if raw_close is not None else 0
                        except (ValueError, TypeError):
                            close_ms = 0

                        grade = p.get("grade") if isinstance(p.get("grade"), dict) else {}
                        cond_raw = (grade.get("conditionType") or {}).get("description", "") if isinstance(grade.get("conditionType"), dict) else ""
                        dmg_raw = (grade.get("damageType") or {}).get("description", "") if isinstance(grade.get("damageType"), dict) else ""
                        mp_raw = (grade.get("missingPartsType") or {}).get("description", "") if isinstance(grade.get("missingPartsType"), dict) else ""
                        func_raw = (grade.get("functionalType") or {}).get("description", "") if isinstance(grade.get("functionalType"), dict) else ""
                        rating = grade.get("rating") if isinstance(grade.get("rating"), int) else None

                        is_closed = bool(p.get("isClosed", False))
                        if not is_closed and close_ms > 0 and close_ms < (time.time() * 1000 - 30000):
                            is_closed = True

                        return {
                            "auction_id": aid,
                            "is_closed": is_closed,
                            "market_status": str(p.get("marketStatus") or ("sold" if is_closed else "active")).strip().lower(),
                            "final_price": round(float(p.get("currentPrice") or 0.0), 2),
                            "retail_price": round(float(p.get("retailPrice") or 0.0), 2),
                            "bid_count": int(p.get("bidCount") or 0),
                            "close_ms": close_ms,
                            "title": str(p.get("title") or "").strip(),
                            "cond_raw": cond_raw,
                            "dmg_raw": dmg_raw,
                            "mp_raw": mp_raw,
                            "func_raw": func_raw,
                            "rating": rating,
                            "notes": str(p.get("notes") or "").strip(),
                            "not_found": False,
                        }
                return None
            except Exception:
                if attempt == 0:
                    time.sleep(0.3)
        return None
    finally:
        if owns_sess:
            sess.close()


def fetch_closed_lots_batch(auction_ids: Iterable[int], workers: int = 24) -> Dict[int, Dict]:
    """Concurrently fetch authoritative post-close status & winning prices for a batch of IDs."""
    unique_ids = []
    seen = set()
    for x in auction_ids:
        try:
            aid = int(x)
            if aid > 0 and aid not in seen:
                seen.add(aid)
                unique_ids.append(aid)
        except (ValueError, TypeError):
            continue

    if not unique_ids:
        return {}

    sess = _build_pooled_session(pool_size=max(workers + 8, 16))
    results: Dict[int, Dict] = {}
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fut_map = {ex.submit(fetch_closed_lot_result, aid, sess): aid for aid in unique_ids}
            for fut in as_completed(fut_map):
                aid = fut_map[fut]
                try:
                    res = fut.result()
                    if res:
                        results[aid] = res
                except Exception:
                    pass
    finally:
        sess.close()

    return results


def is_verified_sold_result(res: Dict) -> bool:
    """True when an item closed with an actual winning bid (excludes 0-bid/relisted/canceled)."""
    if not res or res.get("not_found"):
        return False
    if not res.get("is_closed"):
        return False
    mkt = str(res.get("market_status") or "").lower()
    if mkt in ("relisted", "canceled", "cancelled", "void", "unsold", "active"):
        return False
    return int(res.get("bid_count") or 0) > 0 and float(res.get("final_price") or 0) > 0.0


def settle_r2_partitions(
    max_files: int = 0,
    workers: int = 32,
    max_items_per_run: int = 0,
    buffer_minutes: int = 5,
    only_unsettled: bool = True,
) -> int:
    """
    Scan R2 fact_auctions Parquet partitions newest-first and update all closed auctions
    whose final WON FOR price has not yet been settled after close.
    """
    import boto3
    from src.r2_loader import (
        R2_ACCESS_KEY_ID,
        R2_BUCKET_NAME,
        R2_ENDPOINT_URL,
        R2_SECRET_ACCESS_KEY,
        is_r2_configured,
    )

    if not is_r2_configured():
        raise RuntimeError("R2 credentials are required for settle_r2_partitions.")

    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )

    # Load product titles once for complete condition checks
    titles_map: Dict[str, str] = {}
    try:
        obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key="dim_products/products_catalog.parquet")
        dim_df = pd.read_parquet(io.BytesIO(obj["Body"].read()), columns=["product_id", "title"])
        titles_map = dict(zip(dim_df["product_id"].astype(str), dim_df["title"].fillna("")))
    except Exception as e:
        print(f"[!] Could not load dim_products titles: {e}")

    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix="fact_auctions/"):
        for item in page.get("Contents", []):
            k = item["Key"]
            if k.endswith(".parquet"):
                keys.append(k)
    keys.sort(reverse=True)
    if max_files > 0:
        keys = keys[:max_files]

    now_utc = pd.Timestamp.utcnow()
    cutoff_utc = now_utc - pd.Timedelta(minutes=buffer_minutes)
    settled_cache: Dict[int, Dict] = {}
    total_updated = 0
    total_fetched = 0

    for idx_file, key in enumerate(keys, 1):
        t0 = time.time()
        obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        if df.empty:
            continue

        df = df.copy()
        if "is_closed" not in df.columns:
            df["is_closed"] = False
        else:
            df["is_closed"] = df["is_closed"].fillna(False).astype(bool)

        if "market_status" not in df.columns:
            df["market_status"] = "unverified"
        else:
            df["market_status"] = df["market_status"].fillna("unverified").astype(str)

        closed_ts = pd.to_datetime(df["closed_at"], utc=True, errors="coerce")
        needs_mask = closed_ts.notna() & (closed_ts <= cutoff_utc)
        if only_unsettled:
            needs_mask = needs_mask & (~df["is_closed"])

        candidate_aids = df.loc[needs_mask, "auction_id"].dropna().astype("int64").unique().tolist()
        to_fetch = [aid for aid in candidate_aids if aid not in settled_cache]

        if max_items_per_run > 0:
            remaining_budget = max(0, max_items_per_run - total_fetched)
            to_fetch = to_fetch[:remaining_budget]

        if to_fetch:
            fetched_map = fetch_closed_lots_batch(to_fetch, workers=workers)
            settled_cache.update(fetched_map)
            total_fetched += len(to_fetch)

        changed_in_file = 0
        for row_idx in df.index[needs_mask]:
            aid = int(df.at[row_idx, "auction_id"])
            res = settled_cache.get(aid)
            if not res:
                continue

            if res.get("not_found"):
                if not bool(df.at[row_idx, "is_closed"]):
                    df.at[row_idx, "is_closed"] = True
                    df.at[row_idx, "market_status"] = "archived_unverified"
                    changed_in_file += 1
                continue

            if not res.get("is_closed"):
                continue

            new_price = float(res.get("final_price") or 0.0)
            new_bids = int(res.get("bid_count") or 0)
            msrp = float(df.at[row_idx, "retail_price"] or res.get("retail_price") or 0.0)
            if msrp <= 0 and float(res.get("retail_price") or 0) > 0:
                msrp = float(res["retail_price"])
                df.at[row_idx, "retail_price"] = msrp

            new_ratio = round(new_price / msrp, 4) if msrp > 0 and new_price >= 0 else df.at[row_idx, "discount_ratio"]
            new_mkt = res.get("market_status") or "sold"

            old_price = float(df.at[row_idx, "final_price"] or 0.0)
            old_bids = int(df.at[row_idx, "bid_count"] or 0)
            old_closed = bool(df.at[row_idx, "is_closed"])
            old_mkt = str(df.at[row_idx, "market_status"] or "")

            df.at[row_idx, "final_price"] = new_price
            df.at[row_idx, "discount_ratio"] = new_ratio
            df.at[row_idx, "bid_count"] = new_bids
            df.at[row_idx, "unique_bidders"] = min(new_bids, max(1, int(new_bids * 0.65))) if new_bids > 0 else 0
            df.at[row_idx, "is_closed"] = True
            df.at[row_idx, "market_status"] = new_mkt

            if res.get("close_ms"):
                dt_mst = datetime.datetime.fromtimestamp(int(res["close_ms"]) / 1000.0, tz=MST_TZ)
                df.at[row_idx, "closed_at"] = pd.Timestamp(dt_mst)
                df.at[row_idx, "close_day_of_week"] = (dt_mst.weekday() + 1) % 7
                df.at[row_idx, "close_hour"] = dt_mst.hour

            # Also tighten condition classification while we already have the item .data payload
            title = titles_map.get(str(df.at[row_idx, "product_id"]), "")
            cur_notes = str(df.at[row_idx, "damage_notes"] or "")
            api_notes = str(res.get("notes") or "")
            merged_notes = api_notes if len(api_notes) > len(cur_notes) else cur_notes
            dmg_raw = res.get("dmg_raw") or ""
            if dmg_raw and dmg_raw.lower() not in ("none", "no", "null"):
                dmg_tag = f"[Damage: {dmg_raw}]"
                if dmg_tag.lower() not in merged_notes.lower():
                    merged_notes = f"{merged_notes} {dmg_tag}".strip()
            new_cond = standardize_condition(
                res.get("cond_raw") or str(df.at[row_idx, "condition"] or ""),
                dmg_raw,
                res.get("rating"),
                missing_parts=res.get("mp_raw") or "",
                func_type=res.get("func_raw") or "",
                title=title,
                notes=merged_notes,
            )
            df.at[row_idx, "damage_notes"] = merged_notes
            df.at[row_idx, "condition"] = new_cond
            if res.get("mp_raw"):
                df.at[row_idx, "completeness"] = standardize_completeness(res["mp_raw"])
            if res.get("func_raw"):
                df.at[row_idx, "functional_status"] = standardize_functionality(res["func_raw"])

            if (not old_closed) or abs(new_price - old_price) > 0.001 or new_bids != old_bids or new_mkt != old_mkt:
                changed_in_file += 1

        if changed_in_file > 0:
            # Strict Parquet types matching r2_loader.py
            df["auction_id"] = df["auction_id"].astype("int64")
            df["product_id"] = df["product_id"].astype("string")
            df["location"] = df["location"].astype("string")
            df["condition"] = df["condition"].astype("string")
            df["completeness"] = df["completeness"].astype("string")
            df["functional_status"] = df["functional_status"].astype("string")
            df["damage_notes"] = df["damage_notes"].fillna("").astype("string")
            df["retail_price"] = df["retail_price"].astype("float64")
            df["final_price"] = df["final_price"].astype("float64")
            df["discount_ratio"] = df["discount_ratio"].astype("float64")
            df["bid_count"] = df["bid_count"].astype("int32")
            df["unique_bidders"] = df["unique_bidders"].astype("int32")
            df["closed_at"] = pd.to_datetime(df["closed_at"], format="ISO8601", errors="coerce")
            df["close_day_of_week"] = df["close_day_of_week"].astype("Int8")
            df["close_hour"] = df["close_hour"].astype("Int8")
            df["scraped_date"] = pd.to_datetime(df["scraped_date"]).dt.date
            df["photo_url"] = df.get("photo_url", pd.Series([""] * len(df))).fillna("").astype("string")
            df["is_closed"] = df["is_closed"].astype("bool")
            df["market_status"] = df["market_status"].astype("string")

            buf = io.BytesIO()
            df.to_parquet(buf, index=False, compression="zstd")
            buf.seek(0)
            s3.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=buf.getvalue())
            total_updated += changed_in_file

        print(
            f"[settle {idx_file}/{len(keys)}] {key}: candidates={len(candidate_aids)}, "
            f"fetched_new={len(to_fetch)}, updated={changed_in_file} ({time.time() - t0:.1f}s)"
        )
        if max_items_per_run > 0 and total_fetched >= max_items_per_run:
            break

    print(f"[✓] Settlement sweep complete: fetched={total_fetched:,}, rows_updated={total_updated:,}")
    return total_updated


def main():
    parser = argparse.ArgumentParser(description="Verify & settle closed Nellis auction winning prices in R2.")
    parser.add_argument("--max-files", type=int, default=0, help="Max R2 Parquet files to scan newest-first (0 = all).")
    parser.add_argument("--workers", type=int, default=32, help="Concurrent HTTP workers (default: 32).")
    parser.add_argument("--max-items", type=int, default=0, help="Max unique auction lookups (0 = unlimited).")
    parser.add_argument("--reverify-all", action="store_true", help="Re-check even already settled rows.")
    args = parser.parse_args()

    settle_r2_partitions(
        max_files=args.max_files,
        workers=args.workers,
        max_items_per_run=args.max_items,
        buffer_minutes=5,
        only_unsettled=not args.reverify_all,
    )


if __name__ == "__main__":
    main()

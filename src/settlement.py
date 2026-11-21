#!/usr/bin/env python3
"""
Nellis Auction Post-Close Settlement, Bid History Extractor & Final Price Verifier
Queries https://www.nellisauction.com/p/item/{auction_id}.data for closed auctions
to resolve:
  1. True WON FOR final price, final bid count, marketStatus, closeTime, grade/notes
  2. Complete chronological bidHistory events -> normalized fact_bids Parquet table
  3. Pre-aggregated timing & bidder metrics on fact_auctions (price at 1h/5m/1m left,
     anti-snipe extensions, last-minute bid velocity, exact unique bidders)
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import io
import os
import re
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

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


def parse_bid_dollar_amount(val) -> float:
    """Parse Nellis bid amount strings like '$9', '$1,250.50', or numeric values into float."""
    if val is None or val == "":
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    cleaned = re.sub(r"[$,\s]", "", str(val))
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def parse_bidder_num(label: str) -> int:
    """Extract sequential lot bidder integer from labels like 'Bidder #7' -> 7."""
    m = re.search(r"(\d+)", str(label or ""))
    return int(m.group(1)) if m else 0


def classify_bid_time_bucket(sec_before_close: float, sec_before_init_close: float) -> str:
    """Bucket a bid timestamp relative to auction close for fast GROUP BY analytics."""
    if sec_before_init_close < 0:
        return "Overtime (Anti-Snipe)"
    if sec_before_close <= 60:
        return "Last 60s"
    if sec_before_close <= 300:
        return "1m - 5m left"
    if sec_before_close <= 900:
        return "5m - 15m left"
    if sec_before_close <= 3600:
        return "15m - 1h left"
    if sec_before_close <= 21600:
        return "1h - 6h left"
    return "Early (> 6h left)"


def build_bid_analytics(res: Dict, product_id: str = "", scraped_date=None) -> Tuple[List[Dict], Dict]:
    """
    Transform raw decoded bidHistory + auction close timestamps into:
      1. Clean chronological fact_bids rows (for bid-curve & sniping analytics)
      2. Pre-aggregated auction timing & bidder metrics (for fast fact_auctions queries)
    Handles proxy/auto-bid ordering accurately using running high-water price cutoffs.
    """
    aid = int(res.get("auction_id") or 0)
    close_ms = int(res.get("close_ms") or 0)
    init_close_ms = int(res.get("initial_close_ms") or close_ms or 0)
    open_ms = int(res.get("open_ms") or 0)
    final_price = float(res.get("final_price") or 0.0)
    raw_bids = res.get("bid_history") or []

    open_dt = pd.Timestamp(datetime.datetime.fromtimestamp(open_ms / 1000.0, tz=MST_TZ)) if open_ms > 0 else pd.NaT
    init_close_dt = pd.Timestamp(datetime.datetime.fromtimestamp(init_close_ms / 1000.0, tz=MST_TZ)) if init_close_ms > 0 else pd.NaT
    ext_sec = int(max(0, round((close_ms - init_close_ms) / 1000.0))) if (close_ms and init_close_ms and close_ms > init_close_ms) else 0
    was_extended = bool(ext_sec >= 5)

    parsed_entries = []
    for idx, entry in enumerate(raw_bids):
        if not isinstance(entry, dict):
            continue
        t_ms = entry.get("time")
        try:
            t_ms_int = int(t_ms) if t_ms is not None else 0
        except (ValueError, TypeError):
            t_ms_int = 0
        if t_ms_int <= 0:
            continue

        amt = round(parse_bid_dollar_amount(entry.get("amount")), 2)
        label = str(entry.get("name") or "").strip()
        status = str(entry.get("type") or "").strip()
        parsed_entries.append({
            "time_ms": t_ms_int,
            "orig_idx": idx,
            "bidder_label": label,
            "bidder_num": parse_bidder_num(label),
            "bid_amount": amt,
            "bid_status": status,
        })

    # Sort strictly chronologically ascending (oldest opening bid first, winning bid last)
    parsed_entries.sort(key=lambda b: (b["time_ms"], b["bid_amount"], -b["orig_idx"]))

    bid_rows: List[Dict] = []
    distinct_bidders = set()
    running_high = 0.0
    price_1h = None
    price_5m = None
    price_1m = None
    bids_5m = 0
    bids_60s = 0
    winning_sec = None

    for seq, b in enumerate(parsed_entries, start=1):
        t_ms = b["time_ms"]
        amt = b["bid_amount"]
        label = b["bidder_label"]
        if label:
            distinct_bidders.add(label)

        inc = round(max(0.0, amt - running_high), 2)
        if amt > running_high:
            running_high = amt

        sec_close = round((close_ms - t_ms) / 1000.0, 2) if close_ms > 0 else None
        sec_init = round((init_close_ms - t_ms) / 1000.0, 2) if init_close_ms > 0 else sec_close
        dt_mst = datetime.datetime.fromtimestamp(t_ms / 1000.0, tz=MST_TZ)

        is_win = b["bid_status"].lower() == "winning"
        if is_win and sec_close is not None:
            winning_sec = sec_close

        if sec_close is not None:
            if sec_close >= 3600:
                price_1h = max(price_1h or 0.0, amt)
            if sec_close >= 300:
                price_5m = max(price_5m or 0.0, amt)
            else:
                bids_5m += 1
            if sec_close >= 60:
                price_1m = max(price_1m or 0.0, amt)
            else:
                bids_60s += 1

        pct_final = round(amt / final_price, 4) if final_price > 0 else None
        bucket = (
            classify_bid_time_bucket(sec_close, sec_init if sec_init is not None else sec_close)
            if sec_close is not None
            else "Unknown"
        )

        bid_rows.append({
            "auction_id": aid,
            "product_id": str(product_id or ""),
            "bid_seq": seq,
            "bidder_label": label,
            "bidder_num": b["bidder_num"],
            "bid_amount": amt,
            "bid_increment": inc,
            "bid_status": b["bid_status"],
            "is_winning_bid": is_win,
            "bid_time": dt_mst.isoformat(),
            "bid_hour": dt_mst.hour,
            "seconds_before_close": sec_close,
            "seconds_before_initial_close": sec_init,
            "time_bucket": bucket,
            "auction_final_price": round(final_price, 2),
            "pct_of_final_price": pct_final,
            "scraped_date": scraped_date or datetime.datetime.now(MST_TZ).date(),
        })

    # For auctions that had bids, fill running pre-cutoff highs monotonically (if no bids in final window, standing price carries forward)
    first_bid = parsed_entries[0]["bid_amount"] if parsed_entries else None
    if parsed_entries:
        if price_1h is None:
            price_1h = 0.0
        if price_5m is None:
            price_5m = price_1h
        if price_1m is None:
            price_1m = price_5m
        if winning_sec is None and bid_rows:
            winning_sec = bid_rows[-1]["seconds_before_close"]

    summary = {
        "unique_bidders": len(distinct_bidders) if distinct_bidders else int(res.get("bid_count") or 0),
        "open_at": open_dt,
        "initial_closed_at": init_close_dt,
        "was_extended": was_extended,
        "extension_seconds": ext_sec,
        "first_bid_price": first_bid,
        "price_at_1h_left": price_1h,
        "price_at_5m_left": price_5m,
        "price_at_1m_left": price_1m,
        "bids_last_5m": int(bids_5m),
        "bids_last_60s": int(bids_60s),
        "winning_bid_sec_before_close": winning_sec,
        "bids_captured": True,
    }
    return bid_rows, summary


def _build_pooled_session(pool_size: int = 40) -> requests.Session:
    sess = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({"User-Agent": USER_AGENT})
    return sess


def _to_int_ms(val) -> int:
    if isinstance(val, list) and val:
        val = val[0]
    try:
        return int(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0


def _is_single_fetch_not_found(resp, data=None) -> bool:
    """True when Nellis returns 404/410 or Remix 202 SingleFetchRedirect for purged items."""
    if resp.status_code in (404, 410):
        return True
    set_cookie = str(resp.headers.get("set-cookie") or "").lower()
    if "item could not be found" in set_cookie:
        return True
    if resp.status_code == 202:
        if isinstance(data, list) and data:
            head = str(data[0])
            if "SingleFetchRedirect" in head or "product" not in data:
                return True
        else:
            return True
    return False


def fetch_closed_lot_result(
    auction_id: int,
    session: Optional[requests.Session] = None,
    timeout: float = 10.0,
    attempts: int = 2,
) -> Optional[Dict]:
    """Fetch authoritative post-close state & full bidHistory of a lot from Nellis .data endpoint."""
    aid = int(auction_id)
    base_url = os.environ.get("NELLIS_API_BASE", "https://www.nellisauction.com").rstrip("/")
    url = f"{base_url}/p/item/{aid}.data"
    owns_sess = session is None
    sess = session or _build_pooled_session(4)

    try:
        for attempt in range(max(1, int(attempts))):
            try:
                resp = sess.get(url, timeout=timeout)
                if resp.status_code == 429:
                    raw_ra = resp.headers.get("Retry-After")
                    try:
                        ra_sec = int(float(raw_ra)) if raw_ra else 0
                    except (ValueError, TypeError):
                        ra_sec = 0
                    return {"auction_id": aid, "_rate_limited": True, "retry_after": ra_sec}

                if resp.status_code in (404, 410):
                    return {
                        "auction_id": aid,
                        "is_closed": True,
                        "market_status": "archived_unverified",
                        "not_found": True,
                        "bid_history": [],
                    }
                resp.raise_for_status()
                data = resp.json()
                if _is_single_fetch_not_found(resp, data):
                    return {
                        "auction_id": aid,
                        "is_closed": True,
                        "market_status": "archived_unverified",
                        "not_found": True,
                        "bid_history": [],
                    }
                if not isinstance(data, list):
                    return None

                p = None
                bid_history = []
                for i, el in enumerate(data):
                    if el == "product" and i + 1 < len(data) and p is None:
                        p = unflatten_stream_val(data[i + 1], data)
                    elif el == "bidHistory" and i + 1 < len(data) and not bid_history:
                        bh = unflatten_stream_val(data[i + 1], data)
                        if isinstance(bh, list):
                            bid_history = bh

                if not isinstance(p, dict):
                    if resp.status_code == 202:
                        return {
                            "auction_id": aid,
                            "is_closed": True,
                            "market_status": "archived_unverified",
                            "not_found": True,
                            "bid_history": [],
                        }
                    return None

                close_ms = _to_int_ms(p.get("closeTime"))
                init_close_ms = _to_int_ms(p.get("initialCloseTime")) or close_ms
                open_ms = _to_int_ms(p.get("openTime"))

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
                    "initial_close_ms": init_close_ms,
                    "open_ms": open_ms,
                    "bid_history": bid_history,
                    "title": str(p.get("title") or "").strip(),
                    "cond_raw": cond_raw,
                    "dmg_raw": dmg_raw,
                    "mp_raw": mp_raw,
                    "func_raw": func_raw,
                    "rating": rating,
                    "notes": str(p.get("notes") or "").strip(),
                    "not_found": False,
                }
            except Exception:
                if attempt == 0:
                    time.sleep(0.3)
        return None
    finally:
        if owns_sess:
            sess.close()


class _RateLimiter:
    """Thread-safe pacer enforcing a smooth max request/sec rate across worker threads."""

    def __init__(self, rate_per_sec: float):
        self._lock = threading.Lock()
        self._rate = max(0.0, float(rate_per_sec))
        self._interval = (1.0 / self._rate) if self._rate > 0 else 0.0
        self._next_slot = time.monotonic()

    @property
    def rate(self) -> float:
        with self._lock:
            return self._rate

    def slow_down(self, factor: float = 0.65, min_rate: float = 0.5) -> float:
        """Reduce sustained rate after an HTTP 429 signal and push the next slot forward."""
        with self._lock:
            if self._rate <= 0:
                return 0.0
            self._rate = max(float(min_rate), round(self._rate * float(factor), 3))
            self._interval = 1.0 / self._rate
            self._next_slot = max(self._next_slot, time.monotonic()) + self._interval
            return self._rate

    def acquire(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            if self._interval <= 0:
                return
            now = time.monotonic()
            if now < self._next_slot:
                wait_s = self._next_slot - now
                self._next_slot += self._interval
            else:
                wait_s = 0.0
                self._next_slot = now + self._interval
        if wait_s > 0:
            time.sleep(wait_s)


def fetch_closed_lots_batch(
    auction_ids: Iterable[int],
    workers: int = 12,
    chunk_size: int = 150,
    max_cooldowns: int = 0,
    cooldown_seconds: int = 65,
    timeout: Optional[float] = None,
    attempts: Optional[int] = None,
    rate_per_sec: float = 0.0,
    deadline_ts: float = 0.0,
    stats_out: Optional[Dict[str, Any]] = None,
) -> Dict[int, Dict]:
    """
    Concurrently fetch authoritative post-close status, winning prices & bidHistory in paced chunks.
    When `rate_per_sec > 0`, smooths requests across workers to avoid triggering burst HTTP 429 bans.
    Respects HTTP 429 rate limits cleanly by cooling down and slowing down when `max_cooldowns > 0`,
    or exiting early for interactive UI callers (`max_cooldowns=0`) without hammering blocked requests.
    """
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

    if stats_out is not None:
        stats_out.clear()
        stats_out.update(
            {
                "resolved": 0,
                "rate_limited_hits": 0,
                "cooldowns": 0,
                "rate_limited_aborted": False,
                "deadline_aborted": False,
            }
        )

    if not unique_ids:
        return {}

    eff_workers = min(max(1, int(workers)), 12)
    eff_chunk = max(1, int(chunk_size))
    if rate_per_sec > 0:
        eff_chunk = min(eff_chunk, 25)
    limiter = _RateLimiter(rate_per_sec) if rate_per_sec > 0 else None

    sess = _build_pooled_session(pool_size=max(eff_workers + 8, 16))
    results: Dict[int, Dict] = {}
    queue = list(unique_ids)
    cooldowns_used = 0
    consecutive_empty_waves = 0
    total_rate_limited = 0
    rate_limited_aborted = False
    deadline_aborted = False

    try:
        while queue:
            if deadline_ts > 0 and time.time() >= deadline_ts:
                deadline_aborted = True
                break

            batch_ids = queue[:eff_chunk]
            queue = queue[eff_chunk:]
            rate_limited_ids = []
            aborted_ids = []
            max_retry_after = 0
            wave_resolved = 0
            wave_abort = threading.Event()
            rl_lock = threading.Lock()
            wave_rl_count = 0

            extra_kw = {}
            if timeout is not None:
                extra_kw["timeout"] = timeout
            if attempts is not None:
                extra_kw["attempts"] = attempts

            def _run_one(aid: int):
                nonlocal wave_rl_count
                if wave_abort.is_set() or (deadline_ts > 0 and time.time() >= deadline_ts):
                    return {"_wave_aborted": True}
                if limiter is not None:
                    limiter.acquire()
                if wave_abort.is_set() or (deadline_ts > 0 and time.time() >= deadline_ts):
                    return {"_wave_aborted": True}
                res = fetch_closed_lot_result(aid, sess, **extra_kw)
                if res and res.get("_rate_limited"):
                    with rl_lock:
                        wave_rl_count += 1
                        if wave_rl_count >= 4:
                            wave_abort.set()
                return res

            with ThreadPoolExecutor(max_workers=eff_workers) as ex:
                fut_map = {ex.submit(_run_one, aid): aid for aid in batch_ids}
                for fut in as_completed(fut_map):
                    aid = fut_map[fut]
                    try:
                        res = fut.result()
                    except Exception:
                        res = None
                    if not res:
                        continue
                    if res.get("_wave_aborted"):
                        aborted_ids.append(aid)
                    elif res.get("_rate_limited"):
                        rate_limited_ids.append(aid)
                        max_retry_after = max(max_retry_after, int(res.get("retry_after") or 0))
                    else:
                        results[aid] = res
                        wave_resolved += 1

            if deadline_ts > 0 and time.time() >= deadline_ts and not rate_limited_ids:
                deadline_aborted = True
                break

            if rate_limited_ids:
                total_rate_limited += len(rate_limited_ids)
                if cooldowns_used >= max_cooldowns:
                    rate_limited_aborted = True
                    break
                consecutive_empty_waves = consecutive_empty_waves + 1 if wave_resolved == 0 else 0
                if consecutive_empty_waves >= 2:
                    rate_limited_aborted = True
                    print("[!] Consecutive cooldown waves remained rate-limited; stopping sweep cleanly.")
                    break
                cooldowns_used += 1
                new_rate = limiter.slow_down() if limiter is not None else 0.0
                base_sleep = int(cooldown_seconds)
                prog_sleep = base_sleep * min(cooldowns_used, 3) if base_sleep > 0 else 0
                sleep_s = max(prog_sleep, min(max_retry_after, 360))
                if deadline_ts > 0:
                    rem_s = max(0.0, deadline_ts - time.time())
                    if sleep_s > rem_s:
                        deadline_aborted = True
                        break
                rate_msg = f", pacing reduced to {new_rate:.2f} req/s" if new_rate > 0 else ""
                print(
                    f"[!] Rate-limited (HTTP 429) on {len(rate_limited_ids)} items "
                    f"(resolved={len(results):,}; cooldown {cooldowns_used}/{max_cooldowns}{rate_msg}, sleeping {sleep_s}s)..."
                )
                if sleep_s > 0:
                    time.sleep(sleep_s)
                # Retry rate-limited & aborted items in original priority order
                retry_set = set(rate_limited_ids) | set(aborted_ids)
                retry_ordered = [aid for aid in batch_ids if aid in retry_set]
                queue = retry_ordered + queue
    finally:
        sess.close()

    if stats_out is not None:
        stats_out.update(
            {
                "resolved": len(results),
                "rate_limited_hits": total_rate_limited,
                "cooldowns": cooldowns_used,
                "rate_limited_aborted": rate_limited_aborted,
                "deadline_aborted": deadline_aborted,
            }
        )

    if total_rate_limited > 0 and max_cooldowns > 0:
        print(f"[*] Batch telemetry: resolved={len(results):,}, rate_limited_hits={total_rate_limited:,}, cooldowns={cooldowns_used}")
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
    workers: int = 4,
    max_items_per_run: int = 0,
    buffer_minutes: int = 5,
    only_unsettled: bool = True,
    max_cooldowns: int = 10,
    cooldown_seconds: int = 120,
    rate_per_sec: float = 2.5,
    max_seconds: int = 0,
) -> int:
    """
    Scan R2 fact_auctions Parquet partitions newest-first:
      1. Resolve final WON FOR prices, bid counts, market_status, and timing metrics
      2. Save full per-bid chronological events into parallel fact_bids Parquet partitions
    """
    import boto3
    from src.r2_loader import (
        R2_ACCESS_KEY_ID,
        R2_BUCKET_NAME,
        R2_ENDPOINT_URL,
        R2_SECRET_ACCESS_KEY,
        enforce_fact_auctions_schema,
        enforce_fact_bids_schema,
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

    now_utc = pd.Timestamp.now("UTC")
    cutoff_utc = now_utc - pd.Timedelta(minutes=buffer_minutes)
    settled_cache: Dict[int, Dict] = {}
    bids_emitted_aids = set()
    total_updated = 0
    total_fetched = 0
    total_resolved = 0
    total_bids_saved = 0
    deadline_ts = (time.time() + int(max_seconds)) if max_seconds and int(max_seconds) > 0 else 0.0

    for idx_file, key in enumerate(keys, 1):
        if deadline_ts > 0 and time.time() >= deadline_ts:
            print("[*] Reached settlement wall-clock budget; stopping cleanly before next partition.")
            break
        t0 = time.time()
        obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        if df.empty:
            continue

        df = enforce_fact_auctions_schema(df)
        closed_ts = pd.to_datetime(df["closed_at"], utc=True, errors="coerce")
        needs_mask = closed_ts.notna() & (closed_ts <= cutoff_utc)
        if only_unsettled:
            # Settle rows either never closed OR closed before fact_bids capture existed
            needs_mask = needs_mask & ((~df["is_closed"]) | (~df["bids_captured"]))

        # Prioritize auctions with bids and highest MSRP first so high-value sales always settle before $0 clutter
        cand_df = df.loc[needs_mask, ["auction_id", "bid_count", "retail_price"]].dropna(subset=["auction_id"]).copy()
        cand_df["_has_bids"] = cand_df["bid_count"].fillna(0) > 0
        cand_df = (
            cand_df.sort_values(by=["_has_bids", "bid_count", "retail_price"], ascending=[False, False, False])
            .drop_duplicates(subset=["auction_id"], keep="first")
        )
        candidate_aids = cand_df["auction_id"].astype("int64").tolist()
        to_fetch = [aid for aid in candidate_aids if aid not in settled_cache]

        if max_items_per_run > 0:
            remaining_budget = max(0, max_items_per_run - total_resolved)
            to_fetch = to_fetch[:remaining_budget]

        batch_stats: Dict[str, Any] = {}
        if to_fetch:
            fetched_map = fetch_closed_lots_batch(
                to_fetch,
                workers=workers,
                chunk_size=25 if rate_per_sec > 0 else 150,
                max_cooldowns=max_cooldowns,
                cooldown_seconds=cooldown_seconds,
                rate_per_sec=rate_per_sec,
                deadline_ts=deadline_ts,
                stats_out=batch_stats,
            )
            settled_cache.update(fetched_map)
            total_fetched += len(to_fetch)
            total_resolved += len(fetched_map)

        changed_in_file = 0
        file_bid_rows: List[Dict] = []

        for row_idx in df.index[needs_mask]:
            aid = int(df.at[row_idx, "auction_id"])
            res = settled_cache.get(aid)
            if not res:
                continue

            if res.get("not_found"):
                if (not bool(df.at[row_idx, "is_closed"])) or (not bool(df.at[row_idx, "bids_captured"])):
                    df.at[row_idx, "is_closed"] = True
                    df.at[row_idx, "market_status"] = "archived_unverified"
                    df.at[row_idx, "bids_captured"] = True
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
            pid = str(df.at[row_idx, "product_id"])
            s_date = df.at[row_idx, "scraped_date"]

            bid_rows, summary = build_bid_analytics(res, product_id=pid, scraped_date=s_date)
            if bid_rows and aid not in bids_emitted_aids:
                file_bid_rows.extend(bid_rows)
                bids_emitted_aids.add(aid)

            old_price = float(df.at[row_idx, "final_price"] or 0.0)
            old_bids = int(df.at[row_idx, "bid_count"] or 0)
            old_closed = bool(df.at[row_idx, "is_closed"])
            old_captured = bool(df.at[row_idx, "bids_captured"])
            old_mkt = str(df.at[row_idx, "market_status"] or "")

            df.at[row_idx, "final_price"] = new_price
            df.at[row_idx, "discount_ratio"] = new_ratio
            df.at[row_idx, "bid_count"] = new_bids
            df.at[row_idx, "unique_bidders"] = summary["unique_bidders"]
            df.at[row_idx, "is_closed"] = True
            df.at[row_idx, "market_status"] = new_mkt
            df.at[row_idx, "open_at"] = summary["open_at"]
            df.at[row_idx, "initial_closed_at"] = summary["initial_closed_at"]
            df.at[row_idx, "was_extended"] = summary["was_extended"]
            df.at[row_idx, "extension_seconds"] = summary["extension_seconds"]
            df.at[row_idx, "first_bid_price"] = summary["first_bid_price"]
            df.at[row_idx, "price_at_1h_left"] = summary["price_at_1h_left"]
            df.at[row_idx, "price_at_5m_left"] = summary["price_at_5m_left"]
            df.at[row_idx, "price_at_1m_left"] = summary["price_at_1m_left"]
            df.at[row_idx, "bids_last_5m"] = summary["bids_last_5m"]
            df.at[row_idx, "bids_last_60s"] = summary["bids_last_60s"]
            df.at[row_idx, "winning_bid_sec_before_close"] = summary["winning_bid_sec_before_close"]
            df.at[row_idx, "bids_captured"] = True

            if res.get("close_ms"):
                dt_mst = datetime.datetime.fromtimestamp(int(res["close_ms"]) / 1000.0, tz=MST_TZ)
                df.at[row_idx, "closed_at"] = pd.Timestamp(dt_mst)
                df.at[row_idx, "close_day_of_week"] = (dt_mst.weekday() + 1) % 7
                df.at[row_idx, "close_hour"] = dt_mst.hour

            # Tighten condition classification with live grade metadata
            title = titles_map.get(pid, "")
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

            if (
                (not old_closed)
                or (not old_captured)
                or abs(new_price - old_price) > 0.001
                or new_bids != old_bids
                or new_mkt != old_mkt
            ):
                changed_in_file += 1

        # Upload parallel fact_bids Parquet for this partition
        bids_key = re.sub(
            r"^fact_auctions/(.*)auctions_(\d{4}-\d{2}-\d{2})\.parquet$",
            r"fact_bids/\1bids_\2.parquet",
            key,
        )
        if file_bid_rows and bids_key != key:
            bids_df_new = enforce_fact_bids_schema(pd.DataFrame(file_bid_rows))
            try:
                existing_obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key=bids_key)
                bids_df_old = enforce_fact_bids_schema(pd.read_parquet(io.BytesIO(existing_obj["Body"].read())))
                bids_df_merged = pd.concat([bids_df_new, bids_df_old], ignore_index=True)
            except Exception:
                bids_df_merged = bids_df_new

            bids_df_merged = (
                bids_df_merged
                .sort_values(by=["auction_id", "bid_seq", "bid_amount"], ascending=[True, True, False])
                .drop_duplicates(subset=["auction_id", "bid_seq"], keep="first")
            )
            bids_buf = io.BytesIO()
            bids_df_merged.to_parquet(bids_buf, index=False, compression="zstd")
            bids_buf.seek(0)
            s3.put_object(Bucket=R2_BUCKET_NAME, Key=bids_key, Body=bids_buf.getvalue())
            total_bids_saved += len(file_bid_rows)

        if changed_in_file > 0:
            df = enforce_fact_auctions_schema(df)
            buf = io.BytesIO()
            df.to_parquet(buf, index=False, compression="zstd")
            buf.seek(0)
            s3.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=buf.getvalue())
            total_updated += changed_in_file

        print(
            f"[settle {idx_file}/{len(keys)}] {key}: candidates={len(candidate_aids)}, "
            f"attempted={len(to_fetch)}, auctions_updated={changed_in_file}, "
            f"bids_saved={len(file_bid_rows)} ({time.time() - t0:.1f}s)"
        )
        if batch_stats.get("rate_limited_aborted"):
            print("[!] Stopping partition sweep early after persistent HTTP 429 rate limiting; resolved items were saved.")
            break
        if deadline_ts > 0 and (batch_stats.get("deadline_aborted") or time.time() >= deadline_ts):
            print("[*] Reached settlement wall-clock budget; stopping cleanly so v2 compaction can run.")
            break
        if max_items_per_run > 0 and total_resolved >= max_items_per_run:
            break

    print(
        f"[✓] Settlement sweep complete: attempted={total_fetched:,}, resolved={total_resolved:,}, "
        f"auctions_updated={total_updated:,}, bids_saved={total_bids_saved:,}"
    )
    return total_updated


def main():
    parser = argparse.ArgumentParser(description="Verify & settle closed Nellis winning prices and bid histories in R2.")
    parser.add_argument("--max-files", type=int, default=0, help="Max R2 Parquet files to scan newest-first (0 = all).")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent HTTP workers (default: 4).")
    parser.add_argument("--max-items", type=int, default=0, help="Max unique auction lookups resolved (0 = unlimited).")
    parser.add_argument("--rate", type=float, default=2.5, help="Sustained max requests/sec (0 = unpaced, default: 2.5).")
    parser.add_argument("--max-minutes", type=int, default=0, help="Soft wall-clock budget in minutes (0 = unlimited).")
    parser.add_argument("--cooldowns", type=int, default=10, help="Max HTTP 429 cooldown cycles per run (default: 10).")
    parser.add_argument("--cooldown-sec", type=int, default=120, help="Base cooldown sleep seconds after HTTP 429 (default: 120).")
    parser.add_argument("--reverify-all", action="store_true", help="Re-check even already settled rows.")
    parser.add_argument("--skip-v2", action="store_true", help="Skip updating compacted v2 lake after settlement.")
    args = parser.parse_args()

    updated = settle_r2_partitions(
        max_files=args.max_files,
        workers=args.workers,
        max_items_per_run=args.max_items,
        buffer_minutes=5,
        only_unsettled=not args.reverify_all,
        max_cooldowns=args.cooldowns,
        cooldown_seconds=args.cooldown_sec,
        rate_per_sec=args.rate,
        max_seconds=max(0, int(args.max_minutes) * 60),
    )
    if updated > 0 and not args.skip_v2:
        from src.compact_lake import compact_v1_to_v2

        print("\n[*] Refreshing compacted v2 lake after settlement updates...")
        compact_v1_to_v2(use_r2=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Retroactive Data Lake Condition & Damage-Tag Backfill Script.

Scans all `fact_auctions/year=*/month=*/auctions_*.parquet` files in Cloudflare R2
(joining `dim_products` for item titles):
  1. Offline pass: Re-evaluates `condition` using `INCOMPLETE_DAMAGED_REGEX`,
     `completeness`, and `functional_status`.
  2. Live item detail pass: Queries `https://www.nellisauction.com/p/item/{auction_id}.data`
     concurrently for items still marked Brand New / Like New / Open Box / Standard
     that lack `[Damage:` tags, extracts `grade` (`damageType`, `missingPartsType`,
     `functionalType`, `conditionType`, `rating`) and `notes`, updates `condition`,
     `completeness`, `functional_status`, and `damage_notes`, and writes the updated
     Parquet back to R2.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
import os
import re
import time
import urllib.error
import urllib.request

import boto3
import pandas as pd

from src.r2_loader import (
    R2_ACCESS_KEY_ID,
    R2_BUCKET_NAME,
    R2_ENDPOINT_URL,
    R2_SECRET_ACCESS_KEY,
    is_r2_configured,
)
from src.transform import (
    standardize_completeness,
    standardize_condition,
    standardize_functionality,
)

LITERAL_INT_KEYS = {
    "id",
    "retailPrice",
    "currentPrice",
    "closeTime",
    "buyerPremium",
}


def unflatten_item_val(val, data, key_name=None, depth=0):
    """Depth-safe Turbo Stream unflatten for /p/item/{id}.data responses."""
    if depth > 8:
        return None
    if isinstance(val, int) and not isinstance(val, bool):
        if val < 0:
            return None
        if key_name in LITERAL_INT_KEYS:
            return val
        if 0 <= val < len(data):
            return unflatten_item_val(data[val], data, key_name=key_name, depth=depth + 1)
        return val
    if isinstance(val, (float, str, bool)) or val is None:
        return val
    if isinstance(val, list):
        if len(val) == 2 and val[0] == "D" and isinstance(val[1], (int, float)):
            return val[1]
        return [unflatten_item_val(i, data, depth=depth + 1) for i in val]
    if isinstance(val, dict):
        res = {}
        for k, v in val.items():
            key = k
            if k.startswith("_"):
                try:
                    k_idx = int(k[1:])
                    if 0 <= k_idx < len(data):
                        key = str(data[k_idx])
                except ValueError:
                    pass
            res[key] = unflatten_item_val(v, data, key_name=key, depth=depth + 1)
        return res
    return val


def fetch_item_grade_details(auction_id: int, timeout: int = 10):
    """Fetch grade & notes for a single auction_id from Nellis .data endpoint."""
    url = f"https://www.nellisauction.com/p/item/{int(auction_id)}.data"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    )
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(raw)
            if not isinstance(data, list):
                return None
            for i, el in enumerate(data):
                if el == "product" and i + 1 < len(data):
                    p = unflatten_item_val(data[i + 1], data)
                    if not isinstance(p, dict):
                        return None
                    grade = p.get("grade") if isinstance(p.get("grade"), dict) else {}
                    cond_raw = (grade.get("conditionType") or {}).get("description", "") if isinstance(grade.get("conditionType"), dict) else ""
                    dmg_raw = (grade.get("damageType") or {}).get("description", "") if isinstance(grade.get("damageType"), dict) else ""
                    mp_raw = (grade.get("missingPartsType") or {}).get("description", "") if isinstance(grade.get("missingPartsType"), dict) else ""
                    func_raw = (grade.get("functionalType") or {}).get("description", "") if isinstance(grade.get("functionalType"), dict) else ""
                    rating = grade.get("rating") if isinstance(grade.get("rating"), int) else None
                    notes = str(p.get("notes") or "").strip()
                    return {
                        "cond_raw": cond_raw,
                        "dmg_raw": dmg_raw,
                        "mp_raw": mp_raw,
                        "func_raw": func_raw,
                        "rating": rating,
                        "notes": notes,
                    }
            return None
        except urllib.error.HTTPError as e:
            if e.code in (404, 410):
                return None
            if attempt == 0:
                time.sleep(0.4)
        except Exception:
            if attempt == 0:
                time.sleep(0.4)
    return None


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def list_r2_fact_keys(s3):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix="fact_auctions/"):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".parquet"):
                keys.append(k)
    # Process newest partitions/files first
    keys.sort(reverse=True)
    return keys


def load_dim_titles_r2(s3) -> dict:
    try:
        obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key="dim_products/products_catalog.parquet")
        dim_df = pd.read_parquet(io.BytesIO(obj["Body"].read()), columns=["product_id", "title"])
        return dict(zip(dim_df["product_id"].astype(str), dim_df["title"].fillna("")))
    except Exception as e:
        print(f"[!] Could not load dim_products titles from R2: {e}")
        return {}


def enforce_fact_types(df: pd.DataFrame) -> pd.DataFrame:
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
    if "photo_url" in df.columns:
        df["photo_url"] = df["photo_url"].fillna("").astype("string")
    else:
        df["photo_url"] = ""
    return df


def backfill_dataframe(df: pd.DataFrame, titles_map: dict, fetch_live: bool = True, workers: int = 24) -> tuple[pd.DataFrame, dict]:
    stats = {"total": len(df), "offline_reclassified": 0, "fetched": 0, "live_updated": 0}
    if df.empty:
        return df, stats

    # Ensure string columns
    df = df.copy()
    df["condition"] = df["condition"].fillna("").astype(str)
    df["completeness"] = df["completeness"].fillna("").astype(str)
    df["functional_status"] = df["functional_status"].fillna("").astype(str)
    df["damage_notes"] = df["damage_notes"].fillna("").astype(str)

    # Pass 1: Offline re-evaluation (title + damage_notes + completeness + functional_status)
    for idx, row in df.iterrows():
        pid = str(row["product_id"])
        title = titles_map.get(pid, "")
        notes = row["damage_notes"]
        comp = row["completeness"]
        func = row["functional_status"]
        old_cond = row["condition"]

        mp_arg = "Missing" if comp.lower() in ("incomplete", "partial") else ""
        fn_arg = "No" if func.lower() in ("non-functional", "broken") else ""
        new_cond = standardize_condition(old_cond, "", None, missing_parts=mp_arg, func_type=fn_arg, title=title, notes=notes)
        if new_cond != old_cond and new_cond == "Incomplete / Damaged / Parts":
            df.at[idx, "condition"] = new_cond
            stats["offline_reclassified"] += 1

    if not fetch_live:
        return enforce_fact_types(df), stats

    # Pass 2: Query individual Nellis item pages for rows still classified as clean/new/open-box/like-new
    # that do not yet have a [Damage: ...] tag in damage_notes
    target_mask = (
        df["condition"].isin(["Brand New", "Like New", "Open Box", "Standard", ""])
        & ~df["damage_notes"].str.contains(r"\[Damage:", case=False, regex=True)
    )
    target_indices = df.index[target_mask].tolist()
    if not target_indices:
        return enforce_fact_types(df), stats

    aid_to_indices = {}
    for idx in target_indices:
        aid = int(df.at[idx, "auction_id"])
        aid_to_indices.setdefault(aid, []).append(idx)

    unique_aids = list(aid_to_indices.keys())
    stats["fetched"] = len(unique_aids)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut_map = {ex.submit(fetch_item_grade_details, aid): aid for aid in unique_aids}
        for fut in as_completed(fut_map):
            aid = fut_map[fut]
            details = fut.result()
            if not details:
                continue

            dmg_raw = details["dmg_raw"]
            mp_raw = details["mp_raw"]
            func_raw = details["func_raw"]
            cond_raw = details["cond_raw"]
            rating = details["rating"]
            api_notes = details["notes"]

            for idx in aid_to_indices[aid]:
                pid = str(df.at[idx, "product_id"])
                title = titles_map.get(pid, "")
                cur_notes = df.at[idx, "damage_notes"]
                merged_notes = api_notes if len(api_notes) > len(cur_notes) else cur_notes

                if dmg_raw and dmg_raw.strip().lower() not in ("", "none", "no", "null"):
                    dmg_tag = f"[Damage: {dmg_raw.strip()}]"
                    if dmg_tag.lower() not in merged_notes.lower():
                        merged_notes = f"{merged_notes} {dmg_tag}".strip()

                if mp_raw and mp_raw.strip().lower() not in ("", "none", "no", "null"):
                    mp_tag = f"[Missing Parts: {mp_raw.strip()}]"
                    if mp_tag.lower() not in merged_notes.lower():
                        merged_notes = f"{merged_notes} {mp_tag}".strip()

                new_cond = standardize_condition(
                    cond_raw or df.at[idx, "condition"],
                    dmg_raw,
                    rating,
                    missing_parts=mp_raw,
                    func_type=func_raw,
                    title=title,
                    notes=merged_notes,
                )
                new_comp = standardize_completeness(mp_raw) if mp_raw else df.at[idx, "completeness"]
                new_func = standardize_functionality(func_raw) if func_raw else df.at[idx, "functional_status"]

                if (
                    new_cond != df.at[idx, "condition"]
                    or merged_notes != df.at[idx, "damage_notes"]
                    or new_comp != df.at[idx, "completeness"]
                    or new_func != df.at[idx, "functional_status"]
                ):
                    df.at[idx, "condition"] = new_cond
                    df.at[idx, "damage_notes"] = merged_notes
                    df.at[idx, "completeness"] = new_comp
                    df.at[idx, "functional_status"] = new_func
                    stats["live_updated"] += 1

    return enforce_fact_types(df), stats


def main():
    parser = argparse.ArgumentParser(description="Backfill Nellis R2 Data Lake condition & damage tags.")
    parser.add_argument("--max-files", type=int, default=0, help="Max Parquet files to process (0 = all).")
    parser.add_argument("--workers", type=int, default=24, help="Concurrent HTTP workers for item lookups.")
    parser.add_argument("--offline-only", action="store_true", help="Skip live Nellis HTTP item lookups.")
    args = parser.parse_args()

    if not is_r2_configured():
        raise RuntimeError("R2 credentials are required in environment variables.")

    s3 = get_s3_client()
    titles_map = load_dim_titles_r2(s3)
    keys = list_r2_fact_keys(s3)
    if args.max_files > 0:
        keys = keys[: args.max_files]

    print(f"[*] Found {len(keys)} Parquet files in R2 to backfill (titles loaded: {len(titles_map)}).")
    total_updated = 0

    for i, key in enumerate(keys, 1):
        t0 = time.time()
        obj = s3.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        updated_df, stats = backfill_dataframe(
            df,
            titles_map=titles_map,
            fetch_live=not args.offline_only,
            workers=args.workers,
        )
        changed = stats["offline_reclassified"] + stats["live_updated"]
        if changed > 0:
            buf = io.BytesIO()
            updated_df.to_parquet(buf, index=False, compression="snappy")
            buf.seek(0)
            s3.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=buf.getvalue())
            total_updated += changed

        print(
            f"[{i}/{len(keys)}] {key}: rows={stats['total']}, "
            f"offline={stats['offline_reclassified']}, fetched={stats['fetched']}, "
            f"live_updated={stats['live_updated']} ({time.time() - t0:.1f}s)"
        )

    print(f"[✓] Backfill complete. Total row updates written: {total_updated}")


if __name__ == "__main__":
    main()

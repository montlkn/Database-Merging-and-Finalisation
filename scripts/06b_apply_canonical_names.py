#!/usr/bin/env python3
"""
Step 6b: Apply canonical building names and normalize formatting.

Input:  data/intermediate/06_names_enriched.csv
Output: data/intermediate/06b_names_canonical.csv

Features:
  * Collapse whitespace/newlines in building_name
  * Apply canonical overrides from:
        1) manual CSV mapping (data/manual/canonical_names.csv) – optional
        2) hard-coded dictionary below
        3) optional Exa API lookups (when --fetch-exa is passed)
  * Persist Exa lookups to a cache file to avoid duplicate API calls
  * Update name_source when overrides are applied
  * Reorder columns to the standard schema before writing

Note: Exa fetching requires network access, a valid API key in config.EXA_API_KEY,
and adheres to a conservative throttle (default 0.5s between calls). Adjust as needed.
"""

import sys
import os
import re
import argparse
import json
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import requests
import pandas as pd
from utils import load_checkpoint, save_checkpoint, logger
import config


MANUAL_MAPPING_PATH = Path("data/manual/canonical_names.csv")
CACHE_PATH = Path("data/intermediate/canonical_name_cache.json")

# Base overrides – augment as needed
CANONICAL_NAME_OVERRIDES: Dict[str, str] = {
    # WTC campus (by address)
    "285 Fulton Street": "One World Trade Center",
    "175 Greenwich Street": "3 World Trade Center",
    "150 Greenwich Street": "4 World Trade Center",
    "200 Greenwich Street": "Ronald O. Perelman Performing Arts Center",
    # Midtown icons
    "200 Park Avenue": "MetLife Building",
}

# Desired column order for downstream steps
COLUMN_ORDER = [
    "address",
    "building_name",
    "architect",
    "alt_architect",
    "owner_developer",
    "style",
    "style_secondary",
    "style_other",
    "year_built",
    "date_combo",
    "building_type",
    "use_original",
    "mat_primary",
    "mat_secondary",
    "mat_tertiary",
    "historic_district",
    "location",
    "latitude",
    "longitude",
    "height",
    "source",
    "source_confidence",
    "num_floors",
    "borough",
    "final_score",
    "geometry",
    "address_normalized",
    "is_potential_duplicate",
    "input_lat",
    "input_lng",
    "borough_hint",
    "bbl",
    "bin",
    "geocoded_lat",
    "geocoded_lng",
    "borough_code",
    "borough_name",
    "normalized_address",
    "geocode_status",
    "is_complex_duplicate",
    "complex_group_size",
    "complex_primary",
    "BBL",
    "numfloors",
    "yearbuilt",
    "bldgarea",
    "landmark",
    "bldgclass",
    "height_estimated",
    "height_roof",
    "geometry_footprint",
    "construction_year",
    "shape_area",
    "name_source",
]


def normalize_name(value: str) -> str:
    if value is None:
        return ""
    # Collapse all whitespace (spaces, tabs, newlines) into single spaces
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned


def load_manual_mapping() -> Dict[str, str]:
    """
    Load user-supplied canonical names from CSV if present.
    Format: address,canonical_name
    """
    if not MANUAL_MAPPING_PATH.exists():
        return {}
    try:
        mapping_df = pd.read_csv(MANUAL_MAPPING_PATH)
    except Exception as exc:
        logger.warning(f"Could not read manual mapping ({MANUAL_MAPPING_PATH}): {exc}")
        return {}

    if "address" not in mapping_df.columns or "canonical_name" not in mapping_df.columns:
        logger.warning(
            f"Manual mapping {MANUAL_MAPPING_PATH} must contain 'address' and 'canonical_name' columns"
        )
        return {}

    mapping = (
        mapping_df.dropna(subset=["address", "canonical_name"])
        .set_index("address")["canonical_name"]
        .to_dict()
    )
    logger.info(f"Loaded {len(mapping)} manual canonical name overrides from CSV.")
    return mapping


def load_cache() -> Dict[str, str]:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text())
        except json.JSONDecodeError:
            logger.warning("Canonical name cache is not valid JSON – starting fresh.")
    return {}


def save_cache(cache: Dict[str, str]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, indent=2))


def exa_lookup(query: str, throttle: float) -> Optional[str]:
    """
    Call Exa API to fetch a canonical name candidate.
    Returns the first result title or None.
    """
    if not getattr(config, "EXA_API_KEY", None):
        logger.warning("EXA_API_KEY missing in config – skipping Exa lookup.")
        return None

    headers = {"x-api-key": config.EXA_API_KEY}
    payload = {
        "query": query,
        "type": "neural",  # Exa search type
        "useAutoprompt": True,
        "numResults": 1,
    }

    try:
        response = requests.post(
            "https://api.exa.ai/search",
            headers=headers,
            json=payload,
            timeout=15,
        )
        logger.info(
            "Exa status %s for '%s': %s",
            response.status_code,
            query,
            response.text[:300].replace("\n", " "),
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        logger.warning(f"Exa lookup failed for '{query}': {exc}")
        return None

    results = data.get("results", [])
    if not results:
        return None

    # Basic heuristic: use the title of the top result
    title = results[0].get("title")
    if title:
        time.sleep(throttle)
        return title.strip()
    time.sleep(throttle)
    return None


def fetch_canonical_names_via_exa(df: pd.DataFrame, fetch_all: bool, throttle: float) -> Dict[str, str]:
    """
    Use Exa to fetch canonical names for buildings that need them.
    Returns a mapping of address to canonical name.
    """
    cache = load_cache()
    updates: Dict[str, str] = {}

    if fetch_all:
        candidate_mask = df["address"].notna()
    else:
        candidate_mask = df["name_source"].isin(["address", "canonical_manual", "unknown"]) | (
            df["building_name"] == df["address"]
        )

    candidates = df[candidate_mask][["address", "building_name", "location", "borough"]].drop_duplicates()
    logger.info(f"Preparing Exa lookups for {len(candidates)} unique addresses "
                f"({'all' if fetch_all else 'address-sourced only'}).")

    for _, row in candidates.iterrows():
        address = row["address"]
        if pd.isna(address) or not address.strip():
            continue
        if address in cache:
            updates[address] = cache[address]
            continue

        location_hint = row.get("location") or row.get("borough") or ""
        query = f"{address}, {location_hint} NYC building".strip().strip(",")
        logger.debug(f"Exa lookup for '{query}'")

        canonical = exa_lookup(query, throttle=throttle)
        if canonical:
            updates[address] = canonical
            cache[address] = canonical
        else:
            cache[address] = ""

    save_cache(cache)

    # Remove unsuccessful entries (empty strings)
    updates = {addr: name for addr, name in updates.items() if name}
    logger.info(f"Exa provided {len(updates)} canonical names.")
    return updates


def apply_canonical_names(
    df: pd.DataFrame,
    args: argparse.Namespace,
) -> pd.DataFrame:
    """
    Apply manual overrides and keep track of name_source where relevant.
    """
    df = df.copy()

    # Normalize existing names first
    df["building_name"] = df["building_name"].apply(normalize_name)

    # If name is still empty, make sure name_source reflects that the address will be used
    missing_mask = df["building_name"] == ""
    if "name_source" not in df.columns:
        df["name_source"] = "unknown"
    df.loc[missing_mask, "name_source"] = df.loc[missing_mask, "name_source"].replace(
        {"original": "original", "manual": "manual"}
    )

    # Manual CSV overrides
    manual_overrides = load_manual_mapping()

    # Combine overrides (manual CSV takes precedence over static dict)
    combined_overrides = {**CANONICAL_NAME_OVERRIDES, **manual_overrides}

    # Optionally supplement via Exa
    if args.fetch_exa:
        exa_overrides = fetch_canonical_names_via_exa(df, fetch_all=args.fetch_all, throttle=args.throttle)
        combined_overrides.update(exa_overrides)

    # Apply canonical overrides
    override_mask = df["address"].isin(combined_overrides.keys())
    overridden_count = override_mask.sum()
    if overridden_count:
        logger.info(f"Applying {overridden_count} canonical name overrides...")
        for address, canonical in combined_overrides.items():
            rows = df["address"] == address
            if not rows.any():
                continue
            df.loc[rows, "building_name"] = canonical
            df.loc[rows, "name_source"] = "canonical_manual"

    # For any remaining blanks, fall back to the cleaned address
    still_blank = df["building_name"] == ""
    if still_blank.any():
        df.loc[still_blank, "building_name"] = df.loc[still_blank, "address"].apply(normalize_name)
        df.loc[still_blank, "name_source"] = "address"

    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder the DataFrame columns to the standard schema.
    Missing columns (if any) are appended at the end to avoid data loss.
    """
    existing_order = [col for col in COLUMN_ORDER if col in df.columns]
    missing_cols = [col for col in df.columns if col not in COLUMN_ORDER]
    return df[existing_order + missing_cols]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply canonical building names.")
    parser.add_argument(
        "--fetch-exa",
        action="store_true",
        help="Use Exa API to enrich canonical names (requires config.EXA_API_KEY).",
    )
    parser.add_argument(
        "--fetch-all",
        action="store_true",
        help="When used with --fetch-exa, query Exa for all addresses (not just address-sourced names).",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.5,
        help="Seconds to wait between Exa requests (default: 0.5).",
    )
    return parser.parse_args()


def main():
    logger.info("=" * 60)
    logger.info("Step 6b: Apply Canonical Building Names")
    logger.info("=" * 60)

    args = parse_args()

    input_path = f"{config.INTERMEDIATE_DIR}/06_names_enriched.csv"
    output_path = f"{config.INTERMEDIATE_DIR}/06b_names_canonical.csv"

    logger.info(f"Loading: {input_path}")
    df = load_checkpoint(input_path)
    logger.info(f"  Loaded {len(df)} records")

    result = apply_canonical_names(df, args)
    result = reorder_columns(result)

    save_checkpoint(result, output_path)
    logger.info(f"✓ Saved canonical names to: {output_path} ({len(result)} rows)")
    logger.info("✓ Step 6b complete")


if __name__ == "__main__":
    main()

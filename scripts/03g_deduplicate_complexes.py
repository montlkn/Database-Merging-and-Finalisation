#!/usr/bin/env python3
"""
Step 3g: Deduplicate building complexes

Input: data/intermediate/03f_fixed_bbls.csv
Output: data/intermediate/03g_deduplicated.csv

Strategy:
- Many BBLs have multiple buildings (complexes like Chelsea Square, WTC, etc.)
- For each BBL with 2+ buildings, keep only ONE representative building
- Selection criteria (in order):
  1. Prefer buildings with building_name filled in
  2. Prefer source='existing_landmarks' (they're official LPC landmarks)
  3. Prefer shorter addresses (main building)
  4. Keep first match if tie

This ensures we have 1:1 mapping of buildings to BBLs.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config


PLACEHOLDER_BBL = 5079660001


def deduplicate_complexes(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each BBL with multiple buildings, keep only one representative building.
    """
    logger.info(f"Deduplicating building complexes...")

    # Find BBLs with duplicates (excluding placeholder)
    dup_mask = df['bbl'].duplicated(keep=False) & (df['bbl'] != PLACEHOLDER_BBL)
    dup_bbls = df[dup_mask]['bbl'].unique()

    logger.info(f"  Found {len(dup_bbls)} BBLs with multiple buildings")

    if len(dup_bbls) == 0:
        logger.info("  No duplicates to process!")
        return df

    # Count buildings before deduplication
    total_before = len(df)
    buildings_in_complexes = dup_mask.sum()

    logger.info(f"  Total buildings in complexes: {buildings_in_complexes}")

    # For each duplicate BBL, select the best representative
    indices_to_keep = []
    indices_to_remove = []

    for bbl in dup_bbls:
        subset = df[df['bbl'] == bbl].copy()

        # Add scoring columns for selection
        subset['has_name'] = subset['building_name'].notna() & (subset['building_name'] != '')
        subset['is_landmark'] = subset['source'] == 'existing_landmarks'
        subset['address_len'] = subset['address'].fillna('').str.len()

        # Sort by priority:
        # 1. Has building name (descending - True first)
        # 2. Is landmark (descending - True first)
        # 3. Address length (ascending - shorter first)
        # 4. Index (ascending - keep first)
        sorted_subset = subset.sort_values(
            by=['has_name', 'is_landmark', 'address_len'],
            ascending=[False, False, True]
        )

        # Keep the first one (best match)
        best_idx = sorted_subset.index[0]
        indices_to_keep.append(best_idx)

        # Mark others for removal
        for idx in sorted_subset.index[1:]:
            indices_to_remove.append(idx)

        # Log the selection
        best_row = df.loc[best_idx]
        logger.debug(f"  BBL {int(bbl)}: Kept '{best_row['building_name'] or best_row['address']}' (removed {len(sorted_subset)-1} others)")

    # Also keep all non-duplicate rows
    non_dup_indices = df[~dup_mask].index.tolist()
    all_indices_to_keep = non_dup_indices + indices_to_keep

    # Create deduplicated dataframe
    result = df.loc[all_indices_to_keep].copy()

    # Sort by index to maintain original order
    result = result.sort_index()

    # Reset index
    result = result.reset_index(drop=True)

    # Final summary
    total_after = len(result)
    removed_count = total_before - total_after

    logger.info(f"✓ Deduplication complete:")
    logger.info(f"  Buildings before: {total_before}")
    logger.info(f"  Buildings after: {total_after}")
    logger.info(f"  Buildings removed: {removed_count}")
    logger.info(f"  BBLs deduplicated: {len(dup_bbls)}")

    return result


def main():
    logger.info("=" * 60)
    logger.info("Step 3g: Deduplicate Building Complexes")
    logger.info("=" * 60)

    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/03f_fixed_bbls.csv"
    logger.info(f"Loading: {input_path}")
    df = load_checkpoint(input_path)

    logger.info(f"  Total buildings: {len(df)}")

    # Check for duplicates
    dup_count = df['bbl'].duplicated(keep=False).sum()
    unique_dup_bbls = df[df['bbl'].duplicated(keep=False)]['bbl'].nunique()

    logger.info(f"  Buildings with duplicate BBLs: {dup_count}")
    logger.info(f"  Unique BBLs that are duplicated: {unique_dup_bbls}")

    # Deduplicate
    result = deduplicate_complexes(df)

    # Verify no duplicates remain (except placeholder)
    remaining_dups = result[
        result['bbl'].duplicated(keep=False) &
        (result['bbl'] != PLACEHOLDER_BBL)
    ]

    if len(remaining_dups) > 0:
        logger.warning(f"⚠ {len(remaining_dups)} duplicate BBLs still remain!")
    else:
        logger.info(f"\n✓ All BBL duplicates resolved!")

    # BBL coverage check
    bbl_count = result['bbl'].notna().sum()
    logger.info(f"\n✓ BBL coverage: {bbl_count}/{len(result)} ({bbl_count/len(result)*100:.1f}%)")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03g_deduplicated.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 3g complete")


if __name__ == "__main__":
    main()

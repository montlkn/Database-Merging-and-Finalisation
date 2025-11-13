#!/usr/bin/env python3
"""
Step 8b: Remove True Duplicates

Intelligently removes true duplicates (same building from different sources)
while preserving legitimate building complexes (multiple buildings on same property).

Strategy:
- Category A (True Duplicates): Same name + same address + different sources → Remove duplicate
- Category B (Building Complexes): Different names or addresses on same BBL/BIN → Keep all
- Category C (Historic Complexes): Multiple historic structures → Keep all

Input: data/intermediate/08_clean.csv
Output: data/intermediate/08b_deduped.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
from utils import save_checkpoint, load_checkpoint, logger
import config
from difflib import SequenceMatcher


def normalize_text(text):
    """Normalize text for comparison (lowercase, strip spaces)."""
    if pd.isna(text):
        return ""
    return str(text).lower().strip()


def are_addresses_similar(addr1, addr2, threshold=0.85):
    """Check if two addresses are similar using fuzzy matching."""
    norm1 = normalize_text(addr1)
    norm2 = normalize_text(addr2)

    if not norm1 or not norm2:
        return False

    # Exact match
    if norm1 == norm2:
        return True

    # Fuzzy match using SequenceMatcher
    ratio = SequenceMatcher(None, norm1, norm2).ratio()
    return ratio >= threshold


def are_names_similar(name1, name2, threshold=0.85):
    """Check if two building names are similar."""
    norm1 = normalize_text(name1)
    norm2 = normalize_text(name2)

    # Handle "0" as building name (placeholder)
    if norm1 == "0" or norm2 == "0":
        return False

    if not norm1 or not norm2:
        return False

    # Exact match
    if norm1 == norm2:
        return True

    # Fuzzy match
    ratio = SequenceMatcher(None, norm1, norm2).ratio()
    return ratio >= threshold


def identify_true_duplicates(group):
    """
    Identify which buildings in a duplicate group are true duplicates.

    Returns: list of indices to remove
    """
    to_remove = []

    # Sort by source priority: keep existing_landmarks over new_additions if duplicate
    group = group.sort_values('source', ascending=False)  # 'new_additions' > 'existing_landmarks' alphabetically

    checked = set()

    for i, (idx1, row1) in enumerate(group.iterrows()):
        if idx1 in checked:
            continue

        for j, (idx2, row2) in enumerate(group.iterrows()):
            if i >= j or idx2 in checked:
                continue

            # Check if names are similar
            names_similar = are_names_similar(row1['building_name'], row2['building_name'])

            # Check if addresses are similar
            addresses_similar = are_addresses_similar(row1['address'], row2['address'])

            # If both name AND address are similar, it's a true duplicate
            if names_similar and addresses_similar:
                # Determine which to keep
                # Priority: existing_landmarks > new_additions
                if row1['source'] == 'existing_landmarks' and row2['source'] == 'new_additions':
                    to_remove.append(idx2)
                    checked.add(idx2)
                    logger.debug(f"      Removing duplicate: {row2['building_name'][:50]} (new_additions)")
                elif row2['source'] == 'existing_landmarks' and row1['source'] == 'new_additions':
                    to_remove.append(idx1)
                    checked.add(idx1)
                    logger.debug(f"      Removing duplicate: {row1['building_name'][:50]} (new_additions)")
                else:
                    # Both from same source - keep first one
                    to_remove.append(idx2)
                    checked.add(idx2)
                    logger.debug(f"      Removing duplicate: {row2['building_name'][:50]} (same source)")

    return to_remove


def main():
    logger.info("=" * 60)
    logger.info("Step 8b: Remove True Duplicates")
    logger.info("=" * 60)
    logger.info("Strategy: Remove true duplicates, keep building complexes\n")

    # Load data
    input_path = f"{config.INTERMEDIATE_DIR}/08_clean.csv"
    df = load_checkpoint(input_path)

    logger.info(f"Loaded {len(df):,} buildings\n")

    # Find BBL/BIN duplicates
    dup_mask = df.duplicated(subset=['bbl', 'bin'], keep=False)
    duplicates = df[dup_mask].copy()

    logger.info(f"Found {len(duplicates)} buildings with duplicate BBL/BIN")
    logger.info(f"In {duplicates.groupby(['bbl', 'bin']).ngroups} duplicate groups\n")

    # Analyze each duplicate group
    logger.info("=" * 60)
    logger.info("ANALYZING DUPLICATE GROUPS")
    logger.info("=" * 60)

    all_indices_to_remove = []
    true_duplicate_count = 0
    complex_count = 0

    grouped = duplicates.groupby(['bbl', 'bin'])

    for (bbl, bin_val), group in grouped:
        if len(group) < 2:
            continue

        logger.info(f"\nBBL: {bbl} | BIN: {bin_val} | Buildings: {len(group)}")

        # Show buildings in group
        for idx, row in group.iterrows():
            name = str(row['building_name'])[:40] if pd.notna(row['building_name']) else "N/A"
            addr = str(row['address'])[:40] if pd.notna(row['address']) else "N/A"
            logger.debug(f"  - {name:40} | {addr:40} | {row['source']}")

        # Identify true duplicates in this group
        indices_to_remove = identify_true_duplicates(group)

        if indices_to_remove:
            logger.info(f"  ✗ TRUE DUPLICATES: Removing {len(indices_to_remove)} building(s)")
            true_duplicate_count += len(indices_to_remove)
            all_indices_to_remove.extend(indices_to_remove)
        else:
            logger.info(f"  ✓ BUILDING COMPLEX: Keeping all {len(group)} buildings")
            complex_count += 1

    # Remove true duplicates
    logger.info(f"\n{'='*60}")
    logger.info("REMOVAL SUMMARY")
    logger.info(f"{'='*60}")

    logger.info(f"\nTrue duplicates to remove: {true_duplicate_count}")
    logger.info(f"Building complexes to keep: {complex_count}")

    if all_indices_to_remove:
        logger.info(f"\nRemoving {len(all_indices_to_remove)} true duplicates:")

        # Show what we're removing
        for idx in all_indices_to_remove[:10]:  # Show first 10
            row = df.loc[idx]
            name = str(row['building_name'])[:50] if pd.notna(row['building_name']) else "N/A"
            addr = str(row['address'])[:40] if pd.notna(row['address']) else "N/A"
            logger.info(f"  ✗ {name:50} | {addr:40} | {row['source']}")

        if len(all_indices_to_remove) > 10:
            logger.info(f"  ... and {len(all_indices_to_remove) - 10} more")

        # Remove duplicates
        df_clean = df.drop(index=all_indices_to_remove).copy()

        logger.info(f"\nBuildings: {len(df):,} → {len(df_clean):,}")
    else:
        logger.info("\nNo true duplicates found - all are legitimate building complexes!")
        df_clean = df.copy()

    # Final validation
    logger.info(f"\n{'='*60}")
    logger.info("FINAL VALIDATION")
    logger.info(f"{'='*60}")

    remaining_dups = df_clean.duplicated(subset=['bbl', 'bin'], keep=False).sum()
    logger.info(f"\nRemaining BBL/BIN duplicates: {remaining_dups}")

    if remaining_dups > 0:
        logger.info(f"  (These are legitimate building complexes)")

    # Check for any exact duplicates on name+address
    exact_dups = df_clean.duplicated(subset=['building_name', 'address'], keep=False).sum()
    if exact_dups > 0:
        logger.warning(f"  ⚠ {exact_dups} buildings with same name+address (may need manual review)")
    else:
        logger.info(f"  ✓ No buildings with same name+address")

    # Save
    output_path = f"{config.INTERMEDIATE_DIR}/08b_deduped.csv"
    save_checkpoint(df_clean, output_path)

    logger.info(f"\n{'='*60}")
    logger.info("✓ Step 8b complete")
    logger.info(f"✓ Saved to: {output_path}")
    logger.info(f"\nFinal dataset: {len(df_clean):,} buildings")
    logger.info(f"Removed: {len(df) - len(df_clean)} true duplicates")
    logger.info(f"Preserved: {complex_count} building complexes")


if __name__ == "__main__":
    main()

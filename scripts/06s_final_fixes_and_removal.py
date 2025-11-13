#!/usr/bin/env python3
"""
Step 6s: Final Fixes and Removal

1. Add BBLs for Richmond County Courthouse and Whitehall Ferry Terminal
2. Remove 2 boring buildings: 833 Spofford Avenue and 59th Street and 2nd Avenue

Input: data/intermediate/06r_final_bbls_fixed.csv
Output: data/intermediate/06s_final_ready.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
from utils import save_checkpoint, load_checkpoint, logger
import config


# Final BBL corrections
BBL_CORRECTIONS = {
    '12-24 Richmond Terrace': '5000060021',  # Richmond County Courthouse
    '11 South Street': '1000020001',          # Whitehall Ferry Terminal
}

# Buildings to remove (boring)
BUILDINGS_TO_REMOVE = [
    '833 Spofford Avenue',
    '59th Street and 2nd Avenue'
]


def main():
    logger.info("=" * 60)
    logger.info("Step 6s: Final Fixes and Removal")
    logger.info("=" * 60)

    # Load data
    input_path = "data/intermediate/06r_final_bbls_fixed.csv"
    df = load_checkpoint(input_path)

    logger.info(f"Loaded {len(df):,} buildings\n")

    # ========================================================================
    # PART 1: Apply final BBL corrections
    # ========================================================================
    logger.info("=" * 60)
    logger.info("PART 1: Apply Final BBL Corrections")
    logger.info("=" * 60)
    logger.info("")

    fixed_count = 0

    for address, correct_bbl in BBL_CORRECTIONS.items():
        # Find the building by address
        mask = df['address'] == address

        if mask.sum() == 0:
            logger.warning(f"  ✗ Could not find: {address}")
            continue

        if mask.sum() > 1:
            logger.warning(f"  ⚠ Multiple matches for: {address} (updating all {mask.sum()})")

        # Get the index(es)
        for idx in df[mask].index:
            old_bbl = df.at[idx, 'bbl']
            df.at[idx, 'bbl'] = correct_bbl

            building_name = df.at[idx, 'building_name']
            logger.info(f"  ✓ {building_name}")
            logger.info(f"    Address: {address}")
            logger.info(f"    Old BBL: {old_bbl}")
            logger.info(f"    New BBL: {correct_bbl}")
            logger.info("")

            fixed_count += 1

    logger.info(f"Fixed {fixed_count} BBLs\n")

    # ========================================================================
    # PART 2: Remove boring buildings
    # ========================================================================
    logger.info("=" * 60)
    logger.info("PART 2: Remove Boring Buildings")
    logger.info("=" * 60)
    logger.info("")

    removed_count = 0
    original_count = len(df)

    for address in BUILDINGS_TO_REMOVE:
        mask = df['address'] == address

        if mask.sum() == 0:
            logger.warning(f"  ✗ Could not find: {address}")
            continue

        # Get details before removing
        for idx in df[mask].index:
            building_name = df.at[idx, 'building_name']
            logger.info(f"  ✗ Removing: {building_name}")
            logger.info(f"    Address: {address}")
            logger.info(f"    Reason: Boring/uninteresting")
            logger.info("")

        # Remove the building(s)
        df = df[~mask]
        removed_count += mask.sum()

    logger.info(f"Removed {removed_count} buildings")
    logger.info(f"Total: {original_count:,} → {len(df):,}\n")

    # ========================================================================
    # FINAL STATISTICS
    # ========================================================================
    logger.info("=" * 60)
    logger.info("FINAL STATISTICS")
    logger.info("=" * 60)

    # Check missing BBL
    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')
    missing_bbl = df['bbl_numeric'].isna().sum()

    logger.info(f"\nMissing BBL: {missing_bbl}")

    # List remaining buildings missing BBL
    parks_keywords = ['park', 'pier', 'plaza', 'island', 'beach', 'garden', 'playground', 'boardwalk']

    missing_bbl_df = df[df['bbl_numeric'].isna()].copy()
    is_park = missing_bbl_df.apply(
        lambda row: any(kw in str(row['building_name']).lower() or kw in str(row['address']).lower()
                       for kw in parks_keywords),
        axis=1
    )

    parks_missing = is_park.sum()
    buildings_missing = len(missing_bbl_df) - parks_missing

    logger.info(f"\nMissing BBL breakdown:")
    logger.info(f"  Parks/Public spaces: {parks_missing:3d} (acceptable ✅)")
    logger.info(f"  Real buildings:      {buildings_missing:3d}")

    if buildings_missing > 0:
        logger.info(f"\nRemaining real buildings missing BBL:")
        for idx, row in missing_bbl_df[~is_park].iterrows():
            has_coords = pd.notna(row['latitude'])
            coord_str = f"({row['latitude']:.4f}, {row['longitude']:.4f})" if has_coords else "No coords"
            logger.info(f"  • {row['building_name']}")
            logger.info(f"    Address: {row['address']}")
            logger.info(f"    {coord_str}")
    else:
        logger.info(f"\n✓ All real buildings have BBL!")

    # Final completeness
    logger.info(f"\n{'='*60}")
    logger.info("FINAL DATA COMPLETENESS")
    logger.info(f"{'='*60}")

    total = len(df)
    bbl_complete = total - missing_bbl
    coords_complete = df['latitude'].notna().sum()
    floors_complete = df['numfloors'].notna().sum()
    height_complete = df['height_roof'].notna().sum()

    logger.info(f"\nTotal buildings: {total:,}")
    logger.info(f"  BBL:         {bbl_complete:6d}/{total} ({bbl_complete/total*100:5.2f}%)")
    logger.info(f"  Coordinates: {coords_complete:6d}/{total} ({coords_complete/total*100:5.2f}%)")
    logger.info(f"  Floors:      {floors_complete:6d}/{total} ({floors_complete/total*100:5.2f}%)")
    logger.info(f"  Height:      {height_complete:6d}/{total} ({height_complete/total*100:5.2f}%)")

    # Check for any placeholder data
    PLACEHOLDER_BBL = 5079660001
    PLACEHOLDER_COORDS = (40.73096, -74.00328)

    placeholder_bbl_count = (df['bbl'] == PLACEHOLDER_BBL).sum()
    placeholder_coords = (
        (df['latitude'].notna()) &
        (df['longitude'].notna()) &
        (abs(df['latitude'] - PLACEHOLDER_COORDS[0]) < 0.0001) &
        (abs(df['longitude'] - PLACEHOLDER_COORDS[1]) < 0.0001)
    ).sum()

    logger.info(f"\n{'='*60}")
    logger.info("DATA QUALITY CHECK")
    logger.info(f"{'='*60}")
    logger.info(f"\nPlaceholder BBL (5079660001): {placeholder_bbl_count} ✅")
    logger.info(f"Placeholder coords (40.73096, -74.00328): {placeholder_coords} ✅")

    if placeholder_bbl_count == 0 and placeholder_coords == 0:
        logger.info(f"\n🎉 ZERO placeholder data - dataset is pristine!")

    # Drop temporary column
    df = df.drop(columns=['bbl_numeric'])

    # Save
    output_path = f"{config.INTERMEDIATE_DIR}/06s_final_ready.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n{'='*60}")
    logger.info("✓ Step 6s complete")
    logger.info(f"✓ Saved to: {output_path}")
    logger.info(f"\n🚀 Data is READY for Step 08 (apply 4K limit)!")


if __name__ == "__main__":
    main()

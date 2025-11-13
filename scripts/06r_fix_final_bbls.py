#!/usr/bin/env python3
"""
Step 6r: Fix Final BBLs

User found the correct BBLs for the remaining 7 buildings.
This script applies those corrections.

Input: data/intermediate/06q_final_clean.csv
Output: data/intermediate/06r_final_bbls_fixed.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
from utils import save_checkpoint, load_checkpoint, logger
import config


# BBL corrections from user research
BBL_CORRECTIONS = {
    '54 Noll Street': '3031520001',
    '1 Idlewild Drive': '4142600001',
    '1 South 1st Street': '3024147501',
    '100 Willoughby Street': '3001457502',
    '262 Ashland Place': '3021070030',
    '843 Fifth Avenue': '3009030190',
}


def main():
    logger.info("=" * 60)
    logger.info("Step 6r: Fix Final BBLs")
    logger.info("=" * 60)

    # Load data
    input_path = "data/intermediate/06q_final_clean.csv"
    df = load_checkpoint(input_path)

    logger.info(f"Loaded {len(df):,} buildings\n")

    # Track fixes
    fixed_count = 0

    logger.info("Applying BBL corrections:")
    logger.info("")

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

    # Final statistics
    logger.info("=" * 60)
    logger.info("FINAL STATISTICS")
    logger.info("=" * 60)

    logger.info(f"\nFixed BBLs: {fixed_count}")

    # Check remaining missing BBL
    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')
    missing_bbl = df['bbl_numeric'].isna().sum()

    logger.info(f"\nRemaining missing BBL: {missing_bbl}")

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
    logger.info(f"  Parks/Public spaces: {parks_missing:3d} (acceptable)")
    logger.info(f"  Real buildings:      {buildings_missing:3d}")

    if buildings_missing > 0:
        logger.info(f"\nRemaining real buildings missing BBL:")
        for idx, row in missing_bbl_df[~is_park].iterrows():
            has_coords = pd.notna(row['latitude'])
            coord_str = f"({row['latitude']:.4f}, {row['longitude']:.4f})" if has_coords else "No coords"
            logger.info(f"  • {row['building_name']}")
            logger.info(f"    Address: {row['address']}")
            logger.info(f"    {coord_str}")

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

    # Drop temporary column
    df = df.drop(columns=['bbl_numeric'])

    # Save
    output_path = f"{config.INTERMEDIATE_DIR}/06r_final_bbls_fixed.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n{'='*60}")
    logger.info("✓ Step 6r complete")
    logger.info(f"✓ Saved to: {output_path}")
    logger.info("\nReady for Step 08 (apply 4K limit and cleanup)")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Step 3f: Fix placeholder BBLs using Building Footprints

Input: data/intermediate/03e_final_bbls.csv
Output: data/intermediate/03f_fixed_bbls.csv

Strategy:
- 72 buildings have placeholder BBL 5079660001 but valid BINs
- Look up BIN → BBL mapping from Building Footprints CSV
- Replace placeholder BBL with correct BBL from Building Footprints
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config


PLACEHOLDER_BBL = 5079660001


def load_building_footprints_bin_bbl_mapping(footprints_path: str) -> dict:
    """
    Load BIN → BBL mapping from Building Footprints CSV.
    """
    logger.info(f"Loading Building Footprints from: {footprints_path}")

    # Only load the columns we need (BIN and BBL)
    # Column names are: 'BIN' and 'BASE_BBL' or 'Map Pluto BBL'
    df = pd.read_csv(footprints_path, usecols=['BIN', 'BASE_BBL'], low_memory=False)

    logger.info(f"  Loaded {len(df)} footprints")

    # Create BIN → BBL mapping (drop any rows with missing values)
    mapping = {}
    for idx, row in df.iterrows():
        if pd.notna(row['BIN']) and pd.notna(row['BASE_BBL']):
            bin_val = int(row['BIN'])
            bbl_val = int(row['BASE_BBL'])
            mapping[bin_val] = bbl_val

    logger.info(f"  Created mapping for {len(mapping)} BINs")

    return mapping


def fix_placeholder_bbls(df: pd.DataFrame, bin_to_bbl: dict) -> pd.DataFrame:
    """
    Replace placeholder BBLs with correct BBLs from Building Footprints.
    """
    logger.info(f"Fixing placeholder BBLs...")

    # Find buildings with placeholder BBL
    placeholder_mask = df['bbl'] == PLACEHOLDER_BBL
    placeholder_count = placeholder_mask.sum()

    logger.info(f"  Found {placeholder_count} buildings with placeholder BBL {PLACEHOLDER_BBL}")

    if placeholder_count == 0:
        logger.info("  No placeholder BBLs to fix!")
        return df

    # Fix each placeholder BBL
    fixed_count = 0
    still_placeholder = 0

    for idx in df[placeholder_mask].index:
        if pd.notna(df.at[idx, 'bin']):
            bin_val = int(df.at[idx, 'bin'])

            if bin_val in bin_to_bbl:
                correct_bbl = bin_to_bbl[bin_val]
                df.at[idx, 'bbl'] = correct_bbl
                fixed_count += 1

                if fixed_count % 10 == 0:
                    logger.info(f"    Fixed {fixed_count} placeholder BBLs...")
            else:
                still_placeholder += 1
        else:
            still_placeholder += 1

    logger.info(f"✓ Fixed {fixed_count} placeholder BBLs")

    if still_placeholder > 0:
        logger.warning(f"⚠ {still_placeholder} buildings still have placeholder BBL (BIN not found in Building Footprints)")

        # Show examples
        still_bad = df[df['bbl'] == PLACEHOLDER_BBL]
        logger.warning("  Examples:")
        for idx, row in still_bad.head(10).iterrows():
            bin_str = f"{int(row['bin'])}" if pd.notna(row['bin']) else 'NO BIN'
            logger.warning(f"    - {row['address']} ({row['building_name'] or 'unnamed'}) | BIN: {bin_str}")

    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 3f: Fix Placeholder BBLs")
    logger.info("=" * 60)

    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/03e_final_bbls.csv"
    logger.info(f"Loading: {input_path}")
    df = load_checkpoint(input_path)

    initial_placeholder_count = (df['bbl'] == PLACEHOLDER_BBL).sum()
    logger.info(f"  Placeholder BBLs: {initial_placeholder_count}")

    # Load Building Footprints mapping
    footprints_path = f"{config.RAW_DATA_DIR}/BUILDING_20251104.csv"
    bin_to_bbl = load_building_footprints_bin_bbl_mapping(footprints_path)

    # Fix placeholder BBLs
    result = fix_placeholder_bbls(df, bin_to_bbl)

    # Final summary
    final_placeholder_count = (result['bbl'] == PLACEHOLDER_BBL).sum()
    logger.info(f"\n✓ Placeholder BBLs remaining: {final_placeholder_count}")
    logger.info(f"  Fixed: {initial_placeholder_count - final_placeholder_count}")

    # BBL coverage check
    bbl_count = result['bbl'].notna().sum()
    logger.info(f"\n✓ BBL coverage: {bbl_count}/{len(result)} ({bbl_count/len(result)*100:.1f}%)")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03f_fixed_bbls.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 3f complete")


if __name__ == "__main__":
    main()

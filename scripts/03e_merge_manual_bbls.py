#!/usr/bin/env python3
"""
Step 3e: Merge manually researched BBLs

After you manually fill in BBL/BIN values in:
  data/intermediate/buildings_missing_bbl_TO_RESEARCH.csv

This script will merge them back into the main dataset.

Input:
  - data/intermediate/03d_complete_bbls.csv (current data)
  - data/intermediate/buildings_missing_bbl_TO_RESEARCH.csv (manual BBLs)
Output: data/intermediate/03e_final_bbls.csv

Goal: 100% BBL coverage
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config


def merge_manual_bbls(df: pd.DataFrame, manual_file: str) -> pd.DataFrame:
    """
    Merge manually researched BBLs back into the dataset.
    """
    logger.info(f"Loading manual BBL data from: {manual_file}")

    try:
        manual = pd.read_csv(manual_file)
    except FileNotFoundError:
        logger.error(f"File not found: {manual_file}")
        logger.error("Please ensure you've filled in the manual BBLs first!")
        return df

    logger.info(f"  Loaded {len(manual)} buildings from manual research file")

    # Count how many have manual BBLs filled in
    manual_bbls_filled = manual['bbl_manual'].notna() & (manual['bbl_manual'] != '')
    logger.info(f"  Found {manual_bbls_filled.sum()} manually filled BBLs")

    # Merge by address (primary key)
    merged_count = 0
    for idx, row in manual.iterrows():
        if pd.notna(row['bbl_manual']) and str(row['bbl_manual']).strip():
            # Find matching building in main dataset by address
            address = row['address']
            match = df[df['address'] == address]

            if len(match) > 0:
                match_idx = match.index[0]
                df.at[match_idx, 'bbl'] = float(str(row['bbl_manual']).strip())

                # Also merge BIN if provided
                if pd.notna(row.get('bin_manual')) and str(row['bin_manual']).strip():
                    df.at[match_idx, 'bin'] = float(str(row['bin_manual']).strip())

                merged_count += 1

    logger.info(f"✓ Merged {merged_count} manually researched BBLs")
    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 3e: Merge Manual BBL Research")
    logger.info("=" * 60)

    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/03d_complete_bbls.csv"
    logger.info(f"Loading: {input_path}")
    df = load_checkpoint(input_path)

    initial_bbl_count = df['bbl'].notna().sum()
    logger.info(f"  Starting BBL coverage: {initial_bbl_count}/{len(df)} ({initial_bbl_count/len(df)*100:.1f}%)")

    # Merge manual BBLs
    manual_file = f"{config.INTERMEDIATE_DIR}/buildings_missing_bbl_TO_RESEARCH.csv"
    result = merge_manual_bbls(df, manual_file)

    # Final summary
    final_bbl_count = result['bbl'].notna().sum()
    logger.info(f"\n✓ Final BBL coverage: {final_bbl_count}/{len(result)} ({final_bbl_count/len(result)*100:.1f}%)")
    logger.info(f"  Improvement: +{final_bbl_count - initial_bbl_count} BBLs")

    # Report remaining missing BBLs
    still_missing = result[result['bbl'].isna()]
    if len(still_missing) > 0:
        logger.warning(f"\n⚠ {len(still_missing)} buildings still missing BBL")
    else:
        logger.info("\n🎉 100% BBL COVERAGE ACHIEVED!")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03e_final_bbls.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 3e complete")


if __name__ == "__main__":
    main()

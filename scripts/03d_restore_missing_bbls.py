#!/usr/bin/env python3
"""
Step 3d: Restore BBLs that were lost in processing

Restores BBLs from:
1. Supertalls from Step 02 (they had BBLs from Building Footprints)
2. Any other buildings where BBL was dropped

Input: data/intermediate/03c_regeocoded.csv
Output: data/intermediate/03d_complete_bbls.csv

Goal: 100% BBL coverage
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config


def restore_bbls_from_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Restore BBLs that existed in Step 02 but got lost.
    """
    logger.info("Restoring BBLs from original gap-finding results...")

    # Load the gaps file (Step 02) - these are ALL supertalls
    gaps = pd.read_csv(f"{config.INTERMEDIATE_DIR}/02_gaps_found.csv")
    logger.info(f"  Loaded {len(gaps)} gap buildings (supertalls) with BBL data")

    # Find supertalls in the current dataframe
    supertalls = df[df['source'] == 'footprints_supertall'].copy()
    logger.info(f"  Found {len(supertalls)} supertall buildings in current data")

    if len(supertalls) != len(gaps):
        logger.warning(f"  ⚠ Mismatch: {len(supertalls)} supertalls vs {len(gaps)} gaps")
        return df

    # The supertalls should be in the same order - restore BBL and BIN by position
    restored_count = 0
    supertall_indices = df[df['source'] == 'footprints_supertall'].index.tolist()

    for i, idx in enumerate(supertall_indices):
        if i < len(gaps):
            df.at[idx, 'bbl'] = gaps.iloc[i]['bbl']
            df.at[idx, 'bin'] = gaps.iloc[i]['bin']
            if pd.notna(gaps.iloc[i].get('height')):
                df.at[idx, 'height'] = gaps.iloc[i]['height']
            restored_count += 1

    logger.info(f"✓ Restored {restored_count} BBLs (and BINs) for supertall buildings")
    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 3d: Restore Missing BBLs")
    logger.info("=" * 60)

    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/03c_regeocoded.csv"
    logger.info(f"Loading: {input_path}")
    df = load_checkpoint(input_path)

    initial_bbl_count = df['bbl'].notna().sum()
    logger.info(f"  Starting BBL coverage: {initial_bbl_count}/{len(df)} ({initial_bbl_count/len(df)*100:.1f}%)")

    # Restore BBLs
    result = restore_bbls_from_gaps(df)

    # Final summary
    final_bbl_count = result['bbl'].notna().sum()
    logger.info(f"\n✓ Final BBL coverage: {final_bbl_count}/{len(result)} ({final_bbl_count/len(result)*100:.1f}%)")
    logger.info(f"  Improvement: +{final_bbl_count - initial_bbl_count} BBLs")

    # Report remaining missing BBLs
    still_missing = result[result['bbl'].isna()]
    if len(still_missing) > 0:
        logger.warning(f"\n⚠ {len(still_missing)} buildings still missing BBL:")
        logger.warning("  By source:")
        for source, count in still_missing.groupby('source').size().items():
            logger.warning(f"    {source}: {count}")
    else:
        logger.info("\n🎉 100% BBL COVERAGE ACHIEVED!")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03d_complete_bbls.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 3d complete")


if __name__ == "__main__":
    main()

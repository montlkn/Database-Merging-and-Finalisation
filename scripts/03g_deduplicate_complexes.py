#!/usr/bin/env python3
"""
Step 3g: Deduplicate building complexes

Input: data/intermediate/03f_fixed_bbls.csv
Output: data/intermediate/03g_deduplicated.csv

Strategy:
- Many BBLs have multiple buildings (complexes like Chelsea Square, WTC, etc.)
- Instead of dropping the “duplicates”, flag them so downstream steps can decide
  how to aggregate or present the complex.
- For each duplicated BBL we identify a preferred “primary” record and annotate
  the rest as secondary members of the same complex.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config


PLACEHOLDER_BBL = 5079660001


def annotate_complexes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Annotate complexes rather than removing buildings that share a BBL.
    Adds:
        - is_complex_duplicate: True when the BBL has multiple records
        - complex_group_size: number of buildings sharing the BBL
        - complex_primary: True for the preferred representative record
    """
    logger.info("Annotating building complexes (no rows removed)...")
    df = df.copy()

    # Initialise annotations
    df['is_complex_duplicate'] = False
    df['complex_group_size'] = 1
    df['complex_primary'] = True

    # Find BBLs with duplicates (excluding placeholder)
    dup_mask = df['bbl'].duplicated(keep=False) & (df['bbl'] != PLACEHOLDER_BBL)
    dup_bbls = df[dup_mask]['bbl'].unique()

    logger.info(f"  Found {len(dup_bbls)} BBLs with multiple buildings")

    if len(dup_bbls) == 0:
        logger.info("  No duplicates to process!")
        return df

    buildings_in_complexes = dup_mask.sum()

    logger.info(f"  Total buildings in complexes: {buildings_in_complexes}")

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
        group_indices = sorted_subset.index.tolist()

        df.loc[group_indices, 'is_complex_duplicate'] = True
        df.loc[group_indices, 'complex_group_size'] = len(group_indices)
        df.loc[group_indices, 'complex_primary'] = False
        df.loc[best_idx, 'complex_primary'] = True

        best_row = df.loc[best_idx]
        logger.debug(
            "  BBL %s: primary '%s' with %d secondary records",
            str(bbl),
            best_row['building_name'] or best_row['address'],
            len(group_indices) - 1,
        )

    logger.info("✓ Complex annotation complete")
    logger.info(f"  Complex BBLs: {len(dup_bbls)}")
    logger.info(f"  Buildings flagged as part of complexes: {buildings_in_complexes}")

    return df


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

    # Annotate
    result = annotate_complexes(df)

    # Summary stats already logged inside annotate_complexes; reiterate headline numbers
    flagged = result['is_complex_duplicate'].sum()
    logger.info(f"\nPost-annotation:")
    logger.info(f"  Complex rows flagged: {flagged}")
    logger.info(f"  Complex primary rows: {result['complex_primary'].sum()}")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03g_deduplicated.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 3g complete")


if __name__ == "__main__":
    main()

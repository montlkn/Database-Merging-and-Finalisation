#!/usr/bin/env python3
"""
Step 3h: Fix BBL/BIN format issues

Input: data/intermediate/03g_deduplicated.csv
Output: data/intermediate/03h_formatted.csv

Issues to fix:
1. BBL and BIN stored as float64 with .0 suffix (e.g., "1013540001.0")
2. Need to convert to clean integer strings (e.g., "1013540001")
3. Ensure proper padding (10 digits for BBL, 7 for BIN)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config


def format_bbl_bin(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert BBL and BIN from float to properly formatted string.
    """
    logger.info(f"Formatting BBL and BIN columns...")

    # BBL: convert float → int64 → string, pad to 10 digits
    if 'bbl' in df.columns:
        logger.info("  Formatting BBL column...")

        # Convert to numeric first (handles any string inputs)
        df['bbl'] = pd.to_numeric(df['bbl'], errors='coerce')

        # Convert to int64, then string, padding with zeros
        df['bbl'] = df['bbl'].fillna(0).astype('int64').astype(str).str.zfill(10)

        # Replace '0000000000' with empty string (for truly missing values)
        df.loc[df['bbl'] == '0000000000', 'bbl'] = None

        valid_count = df['bbl'].notna().sum()
        logger.info(f"    Formatted {valid_count} BBLs")

    # BIN: convert float → int64 → string, pad to 7 digits
    if 'bin' in df.columns:
        logger.info("  Formatting BIN column...")

        # Convert to numeric first
        df['bin'] = pd.to_numeric(df['bin'], errors='coerce')

        # Convert to int64, then string, padding with zeros
        df['bin'] = df['bin'].fillna(0).astype('int64').astype(str).str.zfill(7)

        # Replace '0000000' with empty string (for truly missing values)
        df.loc[df['bin'] == '0000000', 'bin'] = None

        valid_count = df['bin'].notna().sum()
        logger.info(f"    Formatted {valid_count} BINs")

    return df


def validate_format(df: pd.DataFrame) -> None:
    """
    Validate BBL and BIN formats.
    """
    logger.info("\nValidating formats...")

    # Validate BBL format: [1-5]\d{9}
    if 'bbl' in df.columns:
        invalid_bbl = df[
            df['bbl'].notna() &
            ~df['bbl'].astype(str).str.fullmatch(r'[1-5]\d{9}')
        ]

        if len(invalid_bbl) > 0:
            logger.warning(f"  ⚠ {len(invalid_bbl)} BBLs don't match format [1-5]XXXXXXXXX")
            for idx, row in invalid_bbl.head(5).iterrows():
                logger.warning(f"    {row['address']}: BBL={row['bbl']}")
        else:
            logger.info(f"  ✓ All BBLs valid format [1-5]XXXXXXXXX")

    # Validate BIN format: \d{7}
    if 'bin' in df.columns:
        invalid_bin = df[
            df['bin'].notna() &
            ~df['bin'].astype(str).str.fullmatch(r'\d{7}')
        ]

        if len(invalid_bin) > 0:
            logger.warning(f"  ⚠ {len(invalid_bin)} BINs don't match format XXXXXXX")
            for idx, row in invalid_bin.head(5).iterrows():
                logger.warning(f"    {row['address']}: BIN={row['bin']}")
        else:
            logger.info(f"  ✓ All BINs valid format XXXXXXX (7 digits)")


def main():
    logger.info("=" * 60)
    logger.info("Step 3h: Fix BBL/BIN Format")
    logger.info("=" * 60)

    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/03g_deduplicated.csv"
    logger.info(f"Loading: {input_path}")
    df = load_checkpoint(input_path)

    logger.info(f"  Total buildings: {len(df)}")

    # Check current format
    logger.info("\nCurrent BBL sample:")
    logger.info(f"  {df['bbl'].head(3).tolist()}")
    logger.info(f"  Type: {df['bbl'].dtype}")

    if 'bin' in df.columns:
        logger.info("\nCurrent BIN sample:")
        logger.info(f"  {df['bin'].head(3).tolist()}")
        logger.info(f"  Type: {df['bin'].dtype}")

    # Format BBL and BIN
    result = format_bbl_bin(df)

    # Validate
    validate_format(result)

    # Summary
    bbl_count = result['bbl'].notna().sum()
    bin_count = result['bin'].notna().sum() if 'bin' in result.columns else 0

    logger.info(f"\n✓ BBL coverage: {bbl_count}/{len(result)} ({bbl_count/len(result)*100:.1f}%)")
    if 'bin' in result.columns:
        logger.info(f"✓ BIN coverage: {bin_count}/{len(result)} ({bin_count/len(result)*100:.1f}%)")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03h_formatted.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 3h complete")


if __name__ == "__main__":
    main()

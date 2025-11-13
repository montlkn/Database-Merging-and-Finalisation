#!/usr/bin/env python3
"""
Step 6h: Finalize Placeholder BBL Cleanup

For remaining placeholder BBLs, determine if they should be:
1. Kept as placeholder (if we think they might have a real BBL)
2. Set to NA (if they're clearly non-properties like parks, vague addresses)

Input: data/intermediate/08_clean.csv
Output: data/intermediate/06h_finalized.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config

PLACEHOLDER_BBL = 5079660001

# Addresses that are clearly non-properties and should have NA
# These are parks, vague intersections, or locations without property records
NON_PROPERTY_ADDRESSES = {
    '620 atlantic avenue',  # Vague - might be intersection
    '29-59 northern boulevard',  # Range address - not specific
    '22 north 6th street',  # Might not exist or be a park
    '111 north 12th street',  # Might not exist
    '21 india street',  # Might not exist
    '100 willoughby street',  # Might be park or vague
    '833 spofford avenue',  # Might not exist
    '1 prospect park west',  # Park area - might not have BBL
    '1 boerum place',  # Might be park or vague
    '262 ashland place',  # Might not exist
    '54 noll street',  # Might not exist
    '515 high line',  # High Line park area - might not have BBL
    '173 and 176 perry street',  # Multiple addresses - vague
    '300 ashland place',  # Already has coords but no BBL - might be park
    '1 south 1st street',  # Might not exist or be park
}

# Addresses that might have real BBLs and should keep placeholder for now
# (These could be researched further)
MIGHT_HAVE_BBL = {
    # Add addresses here if we want to keep placeholder for further research
}


def finalize_placeholder_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove placeholder BBLs for known non-properties, set to NA.
    """
    df = df.copy()
    
    # Normalize addresses
    df['address_normalized'] = df['address'].str.strip().str.lower()
    
    initial_placeholder_count = (df['bbl'] == PLACEHOLDER_BBL).sum()
    logger.info(f"Starting with {initial_placeholder_count} placeholder BBLs")
    
    # Remove placeholders for non-properties
    removed_count = 0
    for addr in NON_PROPERTY_ADDRESSES:
        mask = (df['address_normalized'] == addr) & (df['bbl'] == PLACEHOLDER_BBL)
        if mask.any():
            df.loc[mask, 'bbl'] = None
            df.loc[mask, 'bin'] = None  # Also remove placeholder BIN
            removed_count += 1
            logger.info(f"  Set to NA: {df.loc[mask, 'address'].iloc[0]}")
    
    final_placeholder_count = (df['bbl'] == PLACEHOLDER_BBL).sum()
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Removed {removed_count} placeholder BBLs (set to NA)")
    logger.info(f"Remaining placeholder BBLs: {final_placeholder_count}")
    
    if final_placeholder_count > 0:
        remaining = df[df['bbl'] == PLACEHOLDER_BBL]
        logger.info(f"\nRemaining placeholder BBLs (kept for potential future research):")
        for idx, row in remaining.iterrows():
            logger.info(f"  - {row['address']}")
    
    df = df.drop(columns=['address_normalized'])
    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 6h: Finalize Placeholder BBL Cleanup")
    logger.info("=" * 60)
    
    input_path = f"{config.INTERMEDIATE_DIR}/08_clean.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)
    
    result = finalize_placeholder_cleanup(df)
    
    output_path = f"{config.INTERMEDIATE_DIR}/06h_finalized.csv"
    save_checkpoint(result, output_path)
    
    logger.info(f"\n✓ Step 6h complete")
    logger.info(f"Final dataset: {len(result)} buildings")


if __name__ == "__main__":
    main()


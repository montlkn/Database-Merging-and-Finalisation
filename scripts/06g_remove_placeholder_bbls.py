#!/usr/bin/env python3
"""
Step 6g: Remove Placeholder BBLs

Replace PLACEHOLDER_BBL (5079660001) with NA for buildings that truly don't have a BBL.
This includes parks, vague addresses, and other non-property locations.

Input: data/intermediate/06d_reenriched.csv
Output: data/intermediate/06g_no_placeholders.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config

PLACEHOLDER_BBL = 5079660001

# Addresses that are known to not have BBLs (parks, vague locations, etc.)
KNOWN_NO_BBL = {
    '1 idlewild drive',  # Airport terminal - not a property
    'flushing meadows corona park',  # Park
    'pier 55',  # Pier - may not have BBL
    'governors island',  # Island - may not have BBL
    '59th street and 2nd avenue',  # Vague intersection
    'gansevoort & washington streets',  # Vague intersection
}


def identify_no_bbl_buildings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify buildings that should have NA instead of placeholder BBL.
    These are typically parks, vague addresses, or non-property locations.
    """
    df = df.copy()
    
    # Normalize addresses for matching
    df['address_normalized'] = df['address'].str.strip().str.lower()
    
    # Mark buildings with placeholder BBL
    has_placeholder = df['bbl'] == PLACEHOLDER_BBL
    
    logger.info(f"Found {has_placeholder.sum()} buildings with placeholder BBL")
    
    # Replace known non-property locations
    known_count = 0
    for addr in KNOWN_NO_BBL:
        mask = df['address_normalized'] == addr
        if mask.any() and df.loc[mask, 'bbl'].iloc[0] == PLACEHOLDER_BBL:
            df.loc[mask, 'bbl'] = None
            df.loc[mask, 'bin'] = None  # Also remove placeholder BIN if it exists
            known_count += 1
            logger.info(f"  Removed placeholder BBL for: {df.loc[mask, 'address'].iloc[0]}")
    
    logger.info(f"  Removed {known_count} known non-property locations")
    
    # For remaining placeholder BBLs, check if they look like they should have a BBL
    # (e.g., have a street address, not a park name)
    remaining_placeholders = df[df['bbl'] == PLACEHOLDER_BBL]
    
    if len(remaining_placeholders) > 0:
        logger.info(f"\nRemaining {len(remaining_placeholders)} buildings with placeholder BBL:")
        logger.info("  These may need manual review or additional Exa searches")
        
        # Show sample
        for idx, row in remaining_placeholders.head(10).iterrows():
            logger.info(f"    - {row['address']}")
        
        if len(remaining_placeholders) > 10:
            logger.info(f"    ... and {len(remaining_placeholders) - 10} more")
    
    # For now, keep remaining placeholders (they might get fixed later)
    # But we could also set them to NA if we're confident they don't have BBLs
    
    df = df.drop(columns=['address_normalized'])
    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 6g: Remove Placeholder BBLs")
    logger.info("=" * 60)
    
    input_path = f"{config.INTERMEDIATE_DIR}/06d_reenriched.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)
    
    initial_placeholder_count = (df['bbl'] == PLACEHOLDER_BBL).sum()
    logger.info(f"  Starting with {initial_placeholder_count} placeholder BBLs")
    
    # Remove placeholders for known non-property locations
    result = identify_no_bbl_buildings(df)
    
    final_placeholder_count = (result['bbl'] == PLACEHOLDER_BBL).sum()
    removed_count = initial_placeholder_count - final_placeholder_count
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Removed {removed_count} placeholder BBLs (set to NA)")
    logger.info(f"Remaining placeholder BBLs: {final_placeholder_count}")
    logger.info(f"{'='*60}")
    
    # Save
    output_path = f"{config.INTERMEDIATE_DIR}/06g_no_placeholders.csv"
    save_checkpoint(result, output_path)
    
    logger.info(f"\n✓ Step 6g complete")
    logger.info(f"Next: Run Step 08 (cleanup)")


if __name__ == "__main__":
    main()


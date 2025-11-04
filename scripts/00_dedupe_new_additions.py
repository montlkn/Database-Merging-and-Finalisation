#!/usr/bin/env python3
"""
Step 0: Remove duplicate addresses from new_additions.csv

Checks if any addresses in new_additions.csv already exist in:
- walk_optimized_landmarks.csv
- Earlier in new_additions.csv itself

Removes duplicates and saves cleaned version.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import logger
import config


def normalize_address(address):
    """Normalize address for comparison"""
    if pd.isna(address):
        return ""
    return str(address).lower().strip()


def main():
    logger.info("=" * 60)
    logger.info("Step 0: Deduplicate new_additions.csv")
    logger.info("=" * 60)

    # Load existing landmarks
    logger.info(f"\nLoading existing landmarks: {config.EXISTING_LANDMARKS_CSV}")
    existing = pd.read_csv(config.EXISTING_LANDMARKS_CSV)
    logger.info(f"  Loaded {len(existing)} existing landmarks")

    # Normalize addresses in existing
    existing['address_norm'] = existing['des_addres'].apply(normalize_address)
    existing_addresses = set(existing['address_norm'])
    logger.info(f"  Normalized {len(existing_addresses)} unique addresses")

    # Load new additions
    logger.info(f"\nLoading new additions: {config.NEW_ADDITIONS_CSV}")
    new_additions = pd.read_csv(config.NEW_ADDITIONS_CSV)
    original_count = len(new_additions)
    logger.info(f"  Loaded {original_count} buildings")

    # Normalize addresses in new additions
    new_additions['address_norm'] = new_additions['des_addres'].apply(normalize_address)

    # Find duplicates with existing landmarks
    mask_existing_dupes = new_additions['address_norm'].isin(existing_addresses)
    existing_dupes = new_additions[mask_existing_dupes]

    if len(existing_dupes) > 0:
        logger.info(f"\n❌ Found {len(existing_dupes)} duplicates with existing landmarks:")
        for _, row in existing_dupes.iterrows():
            logger.info(f"    - {row['des_addres']}")
    else:
        logger.info(f"\n✓ No duplicates with existing landmarks")

    # Find duplicates within new_additions itself
    new_additions['is_duplicate'] = new_additions.duplicated(subset=['address_norm'], keep='first')
    internal_dupes = new_additions[new_additions['is_duplicate']]

    if len(internal_dupes) > 0:
        logger.info(f"\n❌ Found {len(internal_dupes)} internal duplicates in new_additions:")
        for _, row in internal_dupes.iterrows():
            logger.info(f"    - {row['des_addres']}")
    else:
        logger.info(f"\n✓ No internal duplicates")

    # Remove all duplicates
    new_additions_clean = new_additions[~mask_existing_dupes & ~new_additions['is_duplicate']]

    # Drop the temporary columns
    new_additions_clean = new_additions_clean.drop(columns=['address_norm', 'is_duplicate'])

    # Save cleaned version
    logger.info(f"\n{'=' * 60}")
    logger.info("Summary:")
    logger.info(f"{'=' * 60}")
    logger.info(f"  Original count: {original_count}")
    logger.info(f"  Duplicates with existing: {len(existing_dupes)}")
    logger.info(f"  Internal duplicates: {len(internal_dupes)}")
    logger.info(f"  Final count: {len(new_additions_clean)}")
    logger.info(f"  Removed: {original_count - len(new_additions_clean)}")

    if original_count != len(new_additions_clean):
        # Backup original
        backup_path = config.NEW_ADDITIONS_CSV + ".backup"
        logger.info(f"\n📋 Creating backup: {backup_path}")
        new_additions.drop(columns=['address_norm', 'is_duplicate']).to_csv(backup_path, index=False)

        # Save cleaned version
        logger.info(f"💾 Saving cleaned version: {config.NEW_ADDITIONS_CSV}")
        new_additions_clean.to_csv(config.NEW_ADDITIONS_CSV, index=False)

        logger.info(f"\n✓ Cleaned new_additions.csv")
        logger.info(f"✓ Ready to run 01_combine_sources.py")
    else:
        logger.info(f"\n✓ No duplicates found - file is already clean")
        logger.info(f"✓ Ready to run 01_combine_sources.py")


if __name__ == "__main__":
    main()

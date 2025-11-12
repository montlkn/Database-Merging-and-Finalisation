#!/usr/bin/env python3
"""
Step 1: Combine all building sources into one master list

Input:
- data/raw/new_additions.csv (297 buildings, 1952-2023)
- data/raw/walk_optimized_landmarks.csv (3,617 buildings, historic)

Output: data/intermediate/01_combined.csv

Strategy:
1. Load both datasets
2. Standardize field names
3. Identify duplicates (same address or very close coordinates)
4. Merge without enrichment (keep raw data)
5. Mark source provenance

This creates the base list before finding gaps and enriching.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
from utils import validate_dataframe, parse_point, save_checkpoint, logger
import config


def standardize_new_additions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize new_additions.csv to common schema.

    Maps:
    - des_addres → address
    - geom (POINT) → lat, lng (extracted)
    - style_prim → style
    - etc.
    """
    logger.info("Standardizing new_additions...")

    # Parse POINT geometry to lat/lng
    coords = df['geom'].apply(parse_point)

    standardized = pd.DataFrame({
        'address': df['des_addres'],
        'building_name': None,  # Will be enriched later
        'architect': df['arch_build'],
        'alt_architect': df.get('alt_arch_1'),
        'owner_developer': df.get('own_devel'),
        'style': df['style_prim'],
        'style_secondary': df.get('style_sec'),
        'style_other': df.get('style_oth'),
        'year_built': df['build_year'],
        'date_combo': df.get('date_combo'),  # Original date string (e.g., "c. 1952")
        'building_type': df['build_type'],
        'use_original': df.get('use_orig'),
        'mat_primary': df.get('mat_prim'),
        'mat_secondary': df.get('mat_sec'),
        'mat_tertiary': df.get('mat_third'),
        'historic_district': df.get('hist_dist'),
        'location': df.get('location'),  # Neighborhood
        'latitude': coords.apply(lambda x: x[1] if x else None),
        'longitude': coords.apply(lambda x: x[0] if x else None),
        'height': pd.to_numeric(df.get('height_arch'), errors='coerce'),
        'source': 'new_additions',
        'source_confidence': 1.0
    })

    logger.info(f"  Standardized {len(standardized)} new buildings")
    return standardized


def standardize_existing_landmarks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize walk_optimized_landmarks.csv to common schema.
    """
    logger.info("Standardizing existing landmarks...")

    # Extract lat/lng from geometry (assuming it's already in your CSV)
    # If geometry is MULTIPOLYGON, we'll need to extract centroid later

    standardized = pd.DataFrame({
        'address': df['des_addres'],
        'building_name': df.get('build_nme'),
        'architect': df.get('arch_build'),
        'alt_architect': None,  # Not in existing data
        'owner_developer': None,  # Not in existing data
        'style': df.get('style_prim'),
        'style_secondary': None,
        'style_other': None,  # Not in existing data
        'year_built': df.get('build_year'),
        'date_combo': None,  # Not in existing data
        'building_type': df.get('build_type'),
        'use_original': None,  # Not in existing data
        'mat_primary': None,  # Not in existing data
        'mat_secondary': None,  # Not in existing data
        'mat_tertiary': None,  # Not in existing data
        'historic_district': None,  # Not in existing data
        'location': None,  # Not in existing data
        'num_floors': df.get('NumFloors'),
        'borough': df.get('Borough'),
        'final_score': df.get('final_score'),  # Preserve existing ML score
        'latitude': None,  # Will extract from geometry later
        'longitude': None,
        'geometry': df.get('geometry'),  # Keep original geometry
        'bbl': df.get('BBL'),  # Preserve BBL from existing data
        'bin': df.get('BIN'),  # Preserve BIN from existing data
        'is_complex_representative': df.get('is_complex_representative'),  # Preserve complex flag
        'source': 'existing_landmarks',
        'source_confidence': 0.9  # Existing data assumed good
    })

    logger.info(f"  Standardized {len(standardized)} existing landmarks")
    return standardized


def identify_duplicates(df: pd.DataFrame, distance_threshold: float = 20.0) -> pd.DataFrame:
    """
    Identify potential duplicates by address similarity or coordinate proximity.

    Mark duplicates but don't remove them yet (will resolve in merge step).
    """
    logger.info("Identifying potential duplicates...")

    # Simple duplicate detection by exact address match
    df['address_normalized'] = df['address'].str.lower().str.strip()

    # Find duplicates by address
    address_dupes = df.duplicated(subset=['address_normalized'], keep=False)

    # TODO: Add coordinate-based duplicate detection (needs geopy or similar)
    # For now, just mark address duplicates

    df['is_potential_duplicate'] = address_dupes

    dupe_count = address_dupes.sum()
    logger.info(f"  Found {dupe_count} potential duplicates (by address)")

    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 1: Combine Building Sources")
    logger.info("=" * 60)

    # Load new additions
    logger.info(f"\nLoading new additions: {config.NEW_ADDITIONS_CSV}")
    new_additions = pd.read_csv(config.NEW_ADDITIONS_CSV)
    logger.info(f"  Loaded {len(new_additions)} new buildings (1952-2023)")

    # Load supplemental additions if available
    supplemental_path = getattr(config, "SUPPLEMENTAL_ADDITIONS_CSV", None)
    if supplemental_path and os.path.exists(supplemental_path):
        logger.info(f"\nLoading supplemental additions: {supplemental_path}")
        supplemental = pd.read_csv(supplemental_path)
        logger.info(f"  Loaded {len(supplemental)} supplemental buildings")

        if not supplemental.empty:
            combined_additions = pd.concat([new_additions, supplemental], ignore_index=True)
            combined_additions['address_normalized'] = combined_additions['des_addres'].fillna('').str.lower().str.strip()

            # Ensure blank addresses remain unique when deduplicating
            blank_mask = combined_additions['address_normalized'] == ''
            if blank_mask.any():
                combined_additions.loc[blank_mask, 'address_normalized'] = (
                    '__blank__' + combined_additions.index[blank_mask].astype(str)
                )

            before = len(combined_additions)
            combined_additions = combined_additions.drop_duplicates(subset=['address_normalized'], keep='last')
            after = len(combined_additions)
            if before != after:
                logger.info(f"  Replaced {before - after} records with supplemental overrides")

            new_additions = combined_additions.drop(columns=['address_normalized'])

    # Load existing landmarks
    logger.info(f"\nLoading existing landmarks: {config.EXISTING_LANDMARKS_CSV}")
    existing = pd.read_csv(config.EXISTING_LANDMARKS_CSV)
    logger.info(f"  Loaded {len(existing)} existing landmarks")

    # Standardize both to common schema
    new_std = standardize_new_additions(new_additions)
    existing_std = standardize_existing_landmarks(existing)

    # Combine
    logger.info("\nCombining datasets...")
    combined = pd.concat([new_std, existing_std], ignore_index=True)
    logger.info(f"  Combined total: {len(combined)} buildings")

    # Identify duplicates
    combined = identify_duplicates(combined)

    # Summary
    logger.info("\nCombined Dataset Summary:")
    logger.info(f"  Total buildings: {len(combined)}")
    logger.info(f"  From new_additions: {(combined['source'] == 'new_additions').sum()}")
    logger.info(f"  From existing_landmarks: {(combined['source'] == 'existing_landmarks').sum()}")
    logger.info(f"  Potential duplicates: {combined['is_potential_duplicate'].sum()}")

    # Convert year_built to numeric for stats
    combined['year_built'] = pd.to_numeric(combined['year_built'], errors='coerce')
    year_min = combined['year_built'].min()
    year_max = combined['year_built'].max()
    if pd.notna(year_min) and pd.notna(year_max):
        logger.info(f"  Date range: {year_min:.0f} - {year_max:.0f}")
    else:
        logger.info(f"  Date range: Unknown (needs cleaning)")

    # Distribution by source
    logger.info("\nBy source:")
    for source, count in combined['source'].value_counts().items():
        logger.info(f"  {source}: {count}")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/01_combined.csv"
    save_checkpoint(combined, output_path)

    logger.info("\n✓ Step 1 complete")
    logger.info("Next: Run 02_find_gaps.py to identify missing buildings")


if __name__ == "__main__":
    main()

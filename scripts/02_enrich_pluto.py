#!/usr/bin/env python3
"""
Step 2: Enrich with PLUTO data

Input: data/intermediate/01_geocoded.csv
Output: data/intermediate/02_pluto_enriched.csv

Adds columns from PLUTO:
- numfloors: Number of floors
- yearbuilt: Year built from PLUTO
- bldgarea: Building area (sq ft)
- landmark: Landmark flag from PLUTO
- bldgclass: Building class code
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import validate_dataframe, save_checkpoint, load_checkpoint, logger, validate_year, validate_floors
import config


def join_pluto(df: pd.DataFrame, pluto: pd.DataFrame) -> pd.DataFrame:
    """
    Join buildings to PLUTO by BBL.

    Returns dataframe with PLUTO fields added.
    """
    logger.info(f"Joining {len(df)} buildings to PLUTO ({len(pluto)} tax lots)")

    # Select relevant PLUTO columns
    pluto_cols = [
        'BBL', 'numfloors', 'yearbuilt', 'bldgarea',
        'landmark', 'bldgclass', 'borough', 'address'
    ]
    pluto_subset = pluto[pluto_cols].copy()

    # Ensure BBL is string for matching
    df['bbl'] = df['bbl'].astype(str)
    pluto_subset['BBL'] = pluto_subset['BBL'].astype(str)

    # Left join on BBL
    result = df.merge(
        pluto_subset,
        left_on='bbl',
        right_on='BBL',
        how='left',
        suffixes=('', '_pluto')
    )

    # Log match rate
    matched = result['numfloors'].notna().sum()
    logger.info(f"✓ Matched {matched}/{len(df)} buildings to PLUTO ({matched/len(df)*100:.1f}%)")

    # Validate PLUTO data
    valid_floors = result['numfloors'].apply(lambda x: validate_floors(x) if pd.notna(x) else False).sum()
    valid_years = result['yearbuilt'].apply(lambda x: validate_year(x) if pd.notna(x) else False).sum()

    logger.info(f"  Valid floors: {valid_floors}/{matched}")
    logger.info(f"  Valid years: {valid_years}/{matched}")

    return result


def main():
    logger.info("=" * 60)
    logger.info("Step 2: PLUTO Enrichment")
    logger.info("=" * 60)

    # Load geocoded buildings
    geocoded_path = f"{config.INTERMEDIATE_DIR}/01_geocoded.csv"
    logger.info(f"Loading: {geocoded_path}")
    df = load_checkpoint(geocoded_path)

    # Load PLUTO
    logger.info(f"Loading PLUTO: {config.PLUTO_CSV}")
    pluto = pd.read_csv(config.PLUTO_CSV, low_memory=False)
    logger.info(f"  PLUTO has {len(pluto)} tax lots")

    # Join
    result = join_pluto(df, pluto)

    # Summary stats
    logger.info("\nEnrichment Summary:")
    logger.info(f"  Buildings with floors: {result['numfloors'].notna().sum()}")
    logger.info(f"  Buildings with year: {result['yearbuilt'].notna().sum()}")
    logger.info(f"  Buildings marked as landmarks: {result['landmark'].notna().sum()}")
    logger.info(f"  Avg floors (where available): {result['numfloors'].mean():.1f}")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/02_pluto_enriched.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 2 complete")


if __name__ == "__main__":
    main()

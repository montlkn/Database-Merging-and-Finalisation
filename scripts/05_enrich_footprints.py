#!/usr/bin/env python3
"""
Step 5: Enrich with Building Footprints data

Input: data/intermediate/04_pluto_enriched.csv
Output: data/intermediate/05_footprints_enriched.csv

Goal: 100% completion for height and geometry

Adds columns from Building Footprints:
- height_roof: Building height in feet
- geometry_footprint: Building polygon geometry (WKT)
- construction_year: Construction year from footprints (as fallback)
- shape_area: Building footprint area (sq ft)

Strategy:
1. Join on BIN (primary) - fastest and most accurate
2. Join on BBL (fallback) - for buildings without BIN
3. For any remaining, estimate from floor count
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config


def load_building_footprints(footprints_path: str) -> pd.DataFrame:
    """
    Load Building Footprints CSV with only needed columns.
    """
    logger.info(f"Loading Building Footprints from: {footprints_path}")

    # Only load columns we need
    columns = ['BIN', 'BASE_BBL', 'Height Roof', 'the_geom', 'Construction Year', 'SHAPE_AREA']

    df = pd.read_csv(footprints_path, usecols=columns, low_memory=False)

    logger.info(f"  Loaded {len(df)} building footprints")

    # Rename for consistency
    df = df.rename(columns={
        'BIN': 'bin',
        'BASE_BBL': 'bbl',
        'Height Roof': 'height_roof',
        'the_geom': 'geometry_footprint',
        'Construction Year': 'construction_year',
        'SHAPE_AREA': 'shape_area'
    })

    # Convert to proper types
    df['bin'] = pd.to_numeric(df['bin'], errors='coerce').fillna(0).astype('int64').astype(str).str.zfill(7)
    df['bbl'] = pd.to_numeric(df['bbl'], errors='coerce').fillna(0).astype('int64').astype(str).str.zfill(10)

    # Replace zeros with None
    df.loc[df['bin'] == '0000000', 'bin'] = None
    df.loc[df['bbl'] == '0000000000', 'bbl'] = None

    # Convert height to numeric
    df['height_roof'] = pd.to_numeric(df['height_roof'], errors='coerce')

    # Convert construction year to numeric
    df['construction_year'] = pd.to_numeric(df['construction_year'], errors='coerce')

    logger.info(f"  Building footprints with height: {df['height_roof'].notna().sum()}")
    logger.info(f"  Building footprints with geometry: {df['geometry_footprint'].notna().sum()}")

    return df


def join_footprints_by_bin(df: pd.DataFrame, footprints: pd.DataFrame) -> pd.DataFrame:
    """
    Join buildings to footprints by BIN (primary key).
    """
    logger.info(f"Joining by BIN...")

    # Select footprints columns to merge
    fp_cols = ['bin', 'height_roof', 'geometry_footprint', 'construction_year', 'shape_area']
    fp_subset = footprints[fp_cols].copy()

    # Join on BIN
    result = df.merge(
        fp_subset,
        on='bin',
        how='left',
        suffixes=('', '_fp')
    )

    # Count matches
    matched = result['height_roof'].notna().sum()
    logger.info(f"  ✓ Matched {matched}/{len(df)} buildings by BIN ({matched/len(df)*100:.1f}%)")

    return result


def join_footprints_by_bbl(df: pd.DataFrame, footprints: pd.DataFrame) -> pd.DataFrame:
    """
    Join remaining buildings to footprints by BBL (fallback).
    """
    logger.info(f"Joining remaining buildings by BBL...")

    # Find buildings still missing height
    missing_mask = df['height_roof'].isna()
    missing_count = missing_mask.sum()

    logger.info(f"  {missing_count} buildings still need height data")

    if missing_count == 0:
        return df

    # Create BBL lookup from footprints
    # For each BBL, take the tallest building (most likely the main building)
    fp_by_bbl = footprints.groupby('bbl').agg({
        'height_roof': 'max',
        'geometry_footprint': 'first',
        'construction_year': 'first',
        'shape_area': 'max'
    }).reset_index()

    logger.info(f"  Using {len(fp_by_bbl)} unique BBLs from footprints")

    # For buildings missing height, try to match by BBL
    for idx in df[missing_mask].index:
        bbl = df.at[idx, 'bbl']

        if pd.notna(bbl):
            match = fp_by_bbl[fp_by_bbl['bbl'] == bbl]

            if len(match) > 0:
                row = match.iloc[0]

                if pd.notna(row['height_roof']):
                    df.at[idx, 'height_roof'] = row['height_roof']
                if pd.notna(row['geometry_footprint']):
                    df.at[idx, 'geometry_footprint'] = row['geometry_footprint']
                if pd.notna(row['construction_year']):
                    df.at[idx, 'construction_year'] = row['construction_year']
                if pd.notna(row['shape_area']):
                    df.at[idx, 'shape_area'] = row['shape_area']

    # Count new matches
    new_matched = df['height_roof'].notna().sum()
    logger.info(f"  ✓ Total matched after BBL join: {new_matched}/{len(df)} ({new_matched/len(df)*100:.1f}%)")

    return df


def estimate_missing_heights(df: pd.DataFrame) -> pd.DataFrame:
    """
    For buildings still missing height, estimate from number of floors.
    Rule of thumb: ~12 feet per floor for residential, ~14 feet for commercial.
    """
    logger.info(f"Estimating missing heights from floor count...")

    missing_mask = df['height_roof'].isna()
    missing_count = missing_mask.sum()

    logger.info(f"  {missing_count} buildings still need height estimation")

    if missing_count == 0:
        return df

    # Estimate height from floor count
    for idx in df[missing_mask].index:
        num_floors = df.at[idx, 'numfloors']

        if pd.notna(num_floors) and num_floors > 0:
            # Use 13 feet per floor as average (between residential and commercial)
            estimated_height = num_floors * 13
            df.at[idx, 'height_roof'] = estimated_height
            df.at[idx, 'height_estimated'] = True
        else:
            df.at[idx, 'height_estimated'] = False

    # For remaining buildings without floors, use default based on building type
    still_missing = df['height_roof'].isna()
    if still_missing.sum() > 0:
        logger.info(f"  {still_missing.sum()} buildings have no floor data - using defaults")

        # Default heights by building type
        defaults = {
            'Skyscraper': 500,
            'Supertall Skyscraper': 1000,
            'Apartment Building': 100,
            'Office Building': 150,
            'Hotel': 200,
            'Church': 80,
            'Museum': 60,
            'Library': 50,
            'Theater': 60,
        }

        for idx in df[still_missing].index:
            bldg_type = df.at[idx, 'building_type']
            default_height = defaults.get(bldg_type, 50)  # 50 feet default
            df.at[idx, 'height_roof'] = default_height
            df.at[idx, 'height_estimated'] = True

    final_count = df['height_roof'].notna().sum()
    estimated_count = df['height_estimated'].fillna(False).sum()

    logger.info(f"  ✓ Final height coverage: {final_count}/{len(df)} ({final_count/len(df)*100:.1f}%)")
    logger.info(f"    - From footprints: {final_count - estimated_count}")
    logger.info(f"    - Estimated: {estimated_count}")

    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 5: Building Footprints Enrichment")
    logger.info("=" * 60)
    logger.info("TARGET: 100% height and geometry coverage")

    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/04_pluto_enriched.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)

    initial_height_count = df['height'].notna().sum() if 'height' in df.columns else 0
    logger.info(f"  Starting height coverage: {initial_height_count}/{len(df)} ({initial_height_count/len(df)*100:.1f}%)")

    # Convert BIN and BBL to string format for joining
    logger.info("Converting BIN and BBL to string format...")
    df['bin'] = pd.to_numeric(df['bin'], errors='coerce').fillna(0).astype('int64').astype(str).str.zfill(7)
    df['bbl'] = pd.to_numeric(df['bbl'], errors='coerce').fillna(0).astype('int64').astype(str).str.zfill(10)
    df.loc[df['bin'] == '0000000', 'bin'] = None
    df.loc[df['bbl'] == '0000000000', 'bbl'] = None

    # Load Building Footprints
    footprints_path = f"{config.RAW_DATA_DIR}/BUILDING_20251104.csv"
    footprints = load_building_footprints(footprints_path)

    # Initialize height_estimated column
    df['height_estimated'] = False

    # Join by BIN (primary)
    result = join_footprints_by_bin(df, footprints)

    # Join by BBL (fallback)
    result = join_footprints_by_bbl(result, footprints)

    # Estimate remaining heights
    result = estimate_missing_heights(result)

    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("FINAL ENRICHMENT SUMMARY")
    logger.info("=" * 60)

    height_count = result['height_roof'].notna().sum()
    geom_count = result['geometry_footprint'].notna().sum()
    year_count = result['construction_year'].notna().sum()
    area_count = result['shape_area'].notna().sum()

    logger.info(f"Height coverage: {height_count}/{len(result)} ({height_count/len(result)*100:.1f}%)")
    logger.info(f"Geometry coverage: {geom_count}/{len(result)} ({geom_count/len(result)*100:.1f}%)")
    logger.info(f"Construction year: {year_count}/{len(result)} ({year_count/len(result)*100:.1f}%)")
    logger.info(f"Shape area: {area_count}/{len(result)} ({area_count/len(result)*100:.1f}%)")

    if height_count == len(result):
        logger.info("\n🎉 100% HEIGHT COVERAGE ACHIEVED!")
    else:
        logger.warning(f"\n⚠ {len(result) - height_count} buildings still missing height")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/05_footprints_enriched.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 5 complete")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Step 8: Final Data Cleanup

Input: data/intermediate/06_names_enriched.csv
Output: data/intermediate/08_clean.csv

Goal: Clean, deduplicated, consistent dataset

Tasks:
1. Fix borough_name column (extract from BBL first digit)
2. Consolidate duplicate columns (BBL/bbl, year_built/yearbuilt, etc.)
3. Apply primary limit (top 4000 primary representatives + their associated complex buildings)
4. Remove any true duplicates (same BBL, BIN, and name)
5. Standardize column names
6. Final data validation
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
from utils import save_checkpoint, load_checkpoint, logger
import config


def fix_borough_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract borough name from BBL first digit.
    BBL format: BBBBBBBBBBB where first digit is borough code.
    1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island
    """
    logger.info("Fixing borough_name column...")

    borough_map = {
        '1': 'Manhattan',
        '2': 'Bronx',
        '3': 'Brooklyn',
        '4': 'Queens',
        '5': 'Staten Island'
    }

    # Extract first digit from bbl
    df['borough_name'] = df['bbl'].astype(str).str[0].map(borough_map)

    coverage = df['borough_name'].notna().sum()
    logger.info(f"  Borough name coverage: {coverage}/{len(df)} ({coverage/len(df)*100:.1f}%)")

    # Show distribution
    borough_counts = df['borough_name'].value_counts()
    logger.info("  Borough distribution:")
    for borough, count in borough_counts.items():
        logger.info(f"    {borough}: {count} ({count/len(df)*100:.1f}%)")

    return df


def consolidate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolidate duplicate columns and standardize names.
    """
    logger.info("\nConsolidating duplicate columns...")

    initial_cols = len(df.columns)
    logger.info(f"  Starting columns: {initial_cols}")

    # 1. BBL: keep lowercase 'bbl', drop 'BBL'
    if 'BBL' in df.columns and 'bbl' in df.columns:
        # Use BBL if bbl is missing
        df['bbl'] = df['bbl'].fillna(df['BBL'])
        df = df.drop(columns=['BBL'])
        logger.info("  ✓ Consolidated BBL -> bbl")

    # 2. Height: consolidate height and height_roof
    # Original 'height' has accurate data for ~28 supertalls from Step 02
    # 'height_roof' has 100% coverage from Building Footprints but may be roof height
    # PRIORITIZE original height (architectural height), then fall back to height_roof
    if 'height_roof' in df.columns:
        # Use original height first (has accurate supertall data), fill missing with height_roof
        df['height'] = df.get('height', df['height_roof']).fillna(df['height_roof'])
        df = df.drop(columns=['height_roof'])
        logger.info("  ✓ Consolidated height columns -> height (prioritizing architectural height)")

    # 3. Year: consolidate year_built, yearbuilt, construction_year
    year_cols = [col for col in ['year_built', 'yearbuilt', 'construction_year'] if col in df.columns]
    if len(year_cols) > 1:
        # Combine all year columns, prioritizing in order
        df['year_built'] = df['year_built'].fillna(df.get('yearbuilt')).fillna(df.get('construction_year'))

        # Drop redundant columns
        for col in ['yearbuilt', 'construction_year']:
            if col in df.columns:
                df = df.drop(columns=[col])

        logger.info(f"  ✓ Consolidated year columns -> year_built")

    # 4. Number of floors: consolidate num_floors and numfloors
    if 'numfloors' in df.columns and 'num_floors' in df.columns:
        df['num_floors'] = df['num_floors'].fillna(df['numfloors'])
        df = df.drop(columns=['numfloors'])
        logger.info("  ✓ Consolidated numfloors -> num_floors")
    elif 'numfloors' in df.columns:
        df = df.rename(columns={'numfloors': 'num_floors'})
        logger.info("  ✓ Renamed numfloors -> num_floors")

    # 5. Borough code: already have borough_code, can drop borough_hint if present
    if 'borough_hint' in df.columns:
        df = df.drop(columns=['borough_hint'])
        logger.info("  ✓ Dropped redundant borough_hint")

    # 6. Keep only one geometry column (prefer geometry_footprint over geometry if both exist)
    if 'geometry_footprint' in df.columns and 'geometry' in df.columns:
        # Use geometry_footprint if available, otherwise use geometry
        df['geometry'] = df['geometry_footprint'].fillna(df['geometry'])
        df = df.drop(columns=['geometry_footprint'])
        logger.info("  ✓ Consolidated geometry columns")
    elif 'geometry_footprint' in df.columns:
        df = df.rename(columns={'geometry_footprint': 'geometry'})
        logger.info("  ✓ Renamed geometry_footprint -> geometry")

    final_cols = len(df.columns)
    logger.info(f"  Final columns: {final_cols} (removed {initial_cols - final_cols})")

    return df


def apply_primary_limit(df: pd.DataFrame, limit: int = 4000) -> pd.DataFrame:
    """
    Filter to top N primary representatives + their associated complex buildings.

    Strategy:
    1. Identify top N primary representatives (is_complex_representative == True)
    2. Keep ALL associated complex buildings for those primaries
    3. Preserve is_complex_representative column for downstream use

    Args:
        df: Input dataframe
        limit: Number of primary representatives to keep (default 4000)

    Returns:
        Filtered dataframe with primaries + their complexes
    """
    logger.info(f"\nApplying {limit:,} primary representative limit...")

    initial_count = len(df)

    # Check if is_complex_representative column exists
    if 'is_complex_representative' not in df.columns:
        logger.warning("  ⚠ is_complex_representative column not found - skipping filter")
        return df

    # Count primaries and complexes
    primary_count = (df['is_complex_representative'] == True).sum()
    complex_count = (df['is_complex_representative'] == False).sum()

    logger.info(f"  Current dataset:")
    logger.info(f"    Primary representatives: {primary_count:,}")
    logger.info(f"    Complex buildings: {complex_count:,}")
    logger.info(f"    Total: {initial_count:,}")

    if primary_count <= limit:
        logger.info(f"  ✓ Already under {limit:,} primaries - no filtering needed")
        return df

    # Get top N primary representatives
    # Sort by height (descending) to get tallest/most important buildings
    primaries = df[df['is_complex_representative'] == True].copy()

    # Sort by height if available, otherwise by year_built
    if 'height' in primaries.columns:
        primaries_sorted = primaries.sort_values('height', ascending=False, na_position='last')
        logger.info(f"  Sorting primaries by height (descending)")
    elif 'year_built' in primaries.columns:
        primaries_sorted = primaries.sort_values('year_built', ascending=False, na_position='last')
        logger.info(f"  Sorting primaries by year_built (descending)")
    else:
        primaries_sorted = primaries  # Keep original order
        logger.info(f"  Keeping original order (no height or year_built)")

    # Take top N primaries
    top_primaries = primaries_sorted.head(limit)

    # Get BBLs of top primaries to identify their associated complexes
    top_primary_bbls = set(top_primaries['bbl'].dropna().unique())

    logger.info(f"  Selected top {limit:,} primaries from {len(top_primary_bbls):,} unique BBLs")

    # Filter dataframe to include:
    # 1. Top N primaries
    # 2. All complex buildings sharing BBLs with those primaries
    df_filtered = df[
        (df['bbl'].isin(top_primary_bbls)) |  # Buildings sharing BBL with top primaries
        (df['is_complex_representative'] == True).isin(top_primaries.index)  # Or is a top primary
    ].copy()

    final_count = len(df_filtered)
    final_primary_count = (df_filtered['is_complex_representative'] == True).sum()
    final_complex_count = (df_filtered['is_complex_representative'] == False).sum()

    logger.info(f"  Final dataset:")
    logger.info(f"    Primary representatives: {final_primary_count:,}")
    logger.info(f"    Associated complex buildings: {final_complex_count:,}")
    logger.info(f"    Total: {final_count:,}")
    logger.info(f"  Removed {initial_count - final_count:,} buildings")

    return df_filtered


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows based on BBL, BIN, and building name.
    Keep the first occurrence of each duplicate.
    """
    logger.info("\nChecking for duplicates...")

    initial_count = len(df)

    # Find duplicates based on BBL and BIN
    # Note: Some buildings might have the same placeholder BBL/BIN but are different buildings
    # We'll be conservative and only remove exact matches on BBL, BIN, AND building name

    duplicate_mask = df.duplicated(subset=['bbl', 'bin', 'building_name'], keep='first')
    duplicates_found = duplicate_mask.sum()

    if duplicates_found > 0:
        logger.info(f"  Found {duplicates_found} duplicate rows (same BBL, BIN, and name)")

        # Show examples
        dup_rows = df[df.duplicated(subset=['bbl', 'bin', 'building_name'], keep=False)]
        unique_dupes = dup_rows.groupby(['bbl', 'bin', 'building_name']).size().reset_index(name='count')

        logger.info("  Examples of duplicates:")
        for idx, row in unique_dupes.head(5).iterrows():
            logger.info(f"    {row['building_name'][:40]} (BBL: {row['bbl']}, {row['count']} copies)")

        # Remove duplicates
        df = df[~duplicate_mask].copy()
        logger.info(f"  ✓ Removed {duplicates_found} duplicate rows")
    else:
        logger.info("  No exact duplicates found (same BBL, BIN, and name)")

    final_count = len(df)
    logger.info(f"  Rows: {initial_count} -> {final_count}")

    return df


def standardize_column_order(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder columns for better usability - key fields first.
    """
    logger.info("\nStandardizing column order...")

    # Define preferred column order (key fields first)
    priority_cols = [
        'building_name',
        'address',
        'borough_name',
        'bbl',
        'bin',
        'height',
        'year_built',
        'architect',
        'style',
        'num_floors',
        'landmark',
        'geocoded_lat',
        'geocoded_lng',
    ]

    # Get all columns in the dataframe
    existing_priority = [col for col in priority_cols if col in df.columns]
    other_cols = [col for col in df.columns if col not in priority_cols]

    # Reorder
    new_order = existing_priority + sorted(other_cols)
    df = df[new_order]

    logger.info(f"  ✓ Reordered {len(df.columns)} columns")
    logger.info(f"  First 10 columns: {', '.join(df.columns[:10])}")

    return df


def validate_final_data(df: pd.DataFrame) -> None:
    """
    Final validation checks.
    """
    logger.info("\n" + "=" * 60)
    logger.info("FINAL DATA VALIDATION")
    logger.info("=" * 60)

    # Key field coverage
    key_fields = {
        'bbl': 'BBL',
        'building_name': 'Building name',
        'height': 'Height (from Building Footprints)',
        'borough_name': 'Borough name',
    }

    all_pass = True
    for col, label in key_fields.items():
        if col in df.columns:
            coverage = df[col].notna().sum() / len(df) * 100
            status = "✓" if coverage == 100 else "⚠"
            logger.info(f"{status} {label}: {coverage:.1f}%")
            if coverage < 100:
                all_pass = False
        else:
            logger.warning(f"✗ {label}: Column missing!")
            all_pass = False

    # Check for duplicates
    dup_count = df.duplicated(subset=['bbl', 'bin']).sum()
    if dup_count == 0:
        logger.info("✓ No BBL/BIN duplicates")
    else:
        logger.warning(f"⚠ {dup_count} BBL/BIN duplicates remain")
        all_pass = False

    # Data types check
    logger.info(f"\nData types:")
    logger.info(f"  bbl: {df['bbl'].dtype}")
    logger.info(f"  bin: {df['bin'].dtype}")
    if 'height' in df.columns:
        logger.info(f"  height: {df['height'].dtype}")

    if all_pass:
        logger.info("\n✅ ALL VALIDATION CHECKS PASSED!")
    else:
        logger.warning("\n⚠ Some validation checks did not pass")

    return all_pass


def main():
    logger.info("=" * 60)
    logger.info("Step 8: Final Data Cleanup")
    logger.info("=" * 60)
    logger.info("GOAL: Clean, deduplicated, consistent dataset")

    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/06_names_enriched.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)

    logger.info(f"  Starting rows: {len(df)}")
    logger.info(f"  Starting columns: {len(df.columns)}")

    # 1. Fix borough_name
    df = fix_borough_name(df)

    # 2. Consolidate columns
    df = consolidate_columns(df)

    # 3. Apply primary limit (4000 primaries + their complexes)
    df = apply_primary_limit(df, limit=4000)

    # 4. Remove duplicates
    df = remove_duplicates(df)

    # 5. Standardize column order
    df = standardize_column_order(df)

    # 5. Final validation
    validation_passed = validate_final_data(df)

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/08_clean.csv"
    save_checkpoint(df, output_path)

    logger.info("\n" + "=" * 60)
    logger.info("CLEANUP SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Final dataset: {len(df)} buildings")
    logger.info(f"Final columns: {len(df.columns)}")
    logger.info(f"Output: {output_path}")

    if validation_passed:
        logger.info("\n🎉 Step 8 complete - data is clean and ready!")
    else:
        logger.warning("\n⚠ Step 8 complete with warnings - review validation results")

    logger.info("\n✓ Ready for Step 9: Aesthetic Categorization")


if __name__ == "__main__":
    main()

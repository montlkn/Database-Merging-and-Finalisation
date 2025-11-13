#!/usr/bin/env python3
"""
Step 6n: Final Cleanup - Remove All Placeholder Data

Philosophy: N/A (missing data) is better than placeholder/dummy data.
This script:
1. Attempts to fix remaining buildings with valid coordinates
2. Removes all placeholder coordinates (replaces with N/A)
3. Ensures no dummy/placeholder data remains in the dataset

Input: data/intermediate/06m_clean_no_placeholders.csv
Output: data/intermediate/06n_final_clean.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
import requests
import time
from utils import save_checkpoint, load_checkpoint, logger
import config

# Placeholder values to remove
PLACEHOLDER_BBL = 5079660001
PLACEHOLDER_COORDS = (40.73096, -74.00328)  # Default/dummy coordinates


def geocode_with_nyc_geoclient(address: str) -> dict:
    """Try to geocode address with NYC Geoclient API v2."""
    if not hasattr(config, 'NYC_GEOCLIENT_SUBSCRIPTION_KEY'):
        return {}

    try:
        url = f"{config.NYC_GEOCLIENT_BASE_URL}/search.json"
        params = {
            'input': address + ", New York, NY",
            'subscription-key': config.NYC_GEOCLIENT_SUBSCRIPTION_KEY
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get('results') and len(data['results']) > 0:
            result = data['results'][0]
            response_data = result.get('response', {})
            return {
                'bbl': response_data.get('bbl'),
                'bin': response_data.get('buildingIdentificationNumber'),
                'latitude': response_data.get('latitude'),
                'longitude': response_data.get('longitude'),
                'status': 'success'
            }
    except Exception as e:
        logger.debug(f"  Geoclient error for {address}: {e}")

    return {}


def main():
    logger.info("=" * 60)
    logger.info("Step 6n: Final Cleanup - Remove All Placeholders")
    logger.info("=" * 60)
    logger.info("")
    logger.info("Philosophy: N/A is better than placeholder/dummy data")
    logger.info("")

    # Load current data
    input_path = "data/intermediate/06m_clean_no_placeholders.csv"
    df = load_checkpoint(input_path)

    logger.info(f"Loaded {len(df):,} buildings")

    # Track changes
    fixed_bbl_count = 0
    removed_placeholder_coords = 0

    # Step 1: Try to fix the 2 buildings with real (non-placeholder) coordinates but missing BBL
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: Fix Buildings with Valid Coordinates")
    logger.info("=" * 60)

    # 262 Ashland Place - try geocoding with borough
    address_262 = "262 Ashland Place, Brooklyn, NY"
    logger.info(f"\nTrying: {address_262}")
    result = geocode_with_nyc_geoclient(address_262)
    if result.get('bbl'):
        mask = df['address'] == '262 Ashland Place'
        if mask.sum() > 0:
            idx = df[mask].index[0]
            df.at[idx, 'bbl'] = result['bbl']
            df.at[idx, 'bin'] = result['bin']
            if result.get('latitude'):
                df.at[idx, 'latitude'] = result['latitude']
                df.at[idx, 'longitude'] = result['longitude']
            logger.info(f"  ✓ Fixed BBL: {result['bbl']}, BIN: {result['bin']}")
            fixed_bbl_count += 1
        time.sleep(0.3)
    else:
        logger.warning(f"  ✗ Could not geocode 262 Ashland Place")

    # 515 West 29th Street - already fixed earlier, verify
    mask_515 = df['address'] == '515 West 29th Street'
    if mask_515.sum() > 0:
        row_515 = df[mask_515].iloc[0]
        if pd.isna(row_515['bbl']) or row_515['bbl'] == 0:
            logger.info(f"\nTrying: 515 West 29th Street")
            result = geocode_with_nyc_geoclient("515 West 29th Street")
            if result.get('bbl'):
                idx = df[mask_515].index[0]
                df.at[idx, 'bbl'] = result['bbl']
                df.at[idx, 'bin'] = result['bin']
                logger.info(f"  ✓ Fixed BBL: {result['bbl']}, BIN: {result['bin']}")
                fixed_bbl_count += 1
                time.sleep(0.3)
        else:
            logger.info(f"\n✓ 515 West 29th Street already has BBL: {row_515['bbl']}")

    # Step 2: Remove ALL placeholder coordinates
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Remove Placeholder Coordinates")
    logger.info("=" * 60)

    # Identify buildings with placeholder coordinates
    placeholder_mask = (
        (df['latitude'].notna()) &
        (df['longitude'].notna()) &
        (abs(df['latitude'] - PLACEHOLDER_COORDS[0]) < 0.0001) &
        (abs(df['longitude'] - PLACEHOLDER_COORDS[1]) < 0.0001)
    )

    placeholder_buildings = df[placeholder_mask]
    logger.info(f"\nFound {len(placeholder_buildings)} buildings with placeholder coordinates")

    for idx in placeholder_buildings.index:
        address = df.at[idx, 'address']
        logger.info(f"  Removing placeholder coords from: {address}")

        # Replace placeholder coords with NaN
        df.at[idx, 'latitude'] = np.nan
        df.at[idx, 'longitude'] = np.nan
        removed_placeholder_coords += 1

    # Step 3: Clean up any remaining placeholder BBLs (shouldn't be any, but verify)
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Verify No Placeholder BBLs Remain")
    logger.info("=" * 60)

    placeholder_bbl_mask = df['bbl'] == PLACEHOLDER_BBL
    if placeholder_bbl_mask.sum() > 0:
        logger.warning(f"Found {placeholder_bbl_mask.sum()} buildings with placeholder BBL!")
        for idx in df[placeholder_bbl_mask].index:
            df.at[idx, 'bbl'] = np.nan
        logger.info("  Replaced with N/A")
    else:
        logger.info("  ✓ No placeholder BBLs found")

    # Step 4: Final statistics
    logger.info("\n" + "=" * 60)
    logger.info("FINAL STATISTICS")
    logger.info("=" * 60)

    # Count missing data
    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')
    missing_bbl = df['bbl_numeric'].isna().sum()
    missing_coords = df['latitude'].isna().sum()
    missing_year = df['year_built'].isna().sum()
    missing_floors = df['numfloors'].isna().sum()

    logger.info(f"\nMissing data (N/A is acceptable):")
    logger.info(f"  BBL:         {missing_bbl:6d} ({missing_bbl/len(df)*100:5.2f}%)")
    logger.info(f"  Coordinates: {missing_coords:6d} ({missing_coords/len(df)*100:5.2f}%)")
    logger.info(f"  Year built:  {missing_year:6d} ({missing_year/len(df)*100:5.2f}%)")
    logger.info(f"  Floors:      {missing_floors:6d} ({missing_floors/len(df)*100:5.2f}%)")

    logger.info(f"\nChanges made:")
    logger.info(f"  Fixed BBLs:                    {fixed_bbl_count}")
    logger.info(f"  Removed placeholder coords:    {removed_placeholder_coords}")

    # Identify buildings still missing BBL (excluding parks/public spaces)
    parks_keywords = ['park', 'pier', 'plaza', 'island', 'beach', 'garden', 'playground', 'boardwalk']
    missing_bbl_df = df[df['bbl_numeric'].isna()].copy()

    is_park = missing_bbl_df.apply(
        lambda row: any(kw in str(row['building_name']).lower() or kw in str(row['address']).lower()
                       for kw in parks_keywords),
        axis=1
    )

    parks_missing_bbl = is_park.sum()
    real_buildings_missing_bbl = len(missing_bbl_df) - parks_missing_bbl

    logger.info(f"\nMissing BBL breakdown:")
    logger.info(f"  Parks/Public spaces:  {parks_missing_bbl:3d} (acceptable)")
    logger.info(f"  Real buildings:       {real_buildings_missing_bbl:3d} (needs investigation)")

    if real_buildings_missing_bbl > 0:
        logger.info(f"\nReal buildings missing BBL:")
        for idx, row in missing_bbl_df[~is_park].iterrows():
            has_coords = pd.notna(row['latitude'])
            coord_status = f"Coords: ({row['latitude']:.4f}, {row['longitude']:.4f})" if has_coords else "No coords"
            logger.info(f"  • {row['address']:<50} {coord_status}")

    # Drop temporary column
    df = df.drop(columns=['bbl_numeric'])

    # Save cleaned data
    output_path = f"{config.INTERMEDIATE_DIR}/06n_final_clean.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n{'='*60}")
    logger.info("✓ Step 6n complete")
    logger.info(f"✓ Saved to: {output_path}")
    logger.info(f"\nKey principle applied: N/A > Placeholder data")
    logger.info("Next: Run Step 08 (apply 4K limit and cleanup)")


if __name__ == "__main__":
    main()

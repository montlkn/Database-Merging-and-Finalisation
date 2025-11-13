#!/usr/bin/env python3
"""
Step 6o: Data Recovery - Fix Remaining Missing Data

Recovery targets:
1. 3 buildings with coords but no BBL → geocode to get BBL
2. 732 buildings with BBL but missing floors → lookup in PLUTO
3. All buildings with height but no floors → estimate from height
4. 7 new additions missing coords → attempt geocoding

Input: data/intermediate/06n_final_clean.csv
Output: data/intermediate/06o_recovered.csv
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


def estimate_floors_from_height(height_ft: float) -> int:
    """Estimate floor count from height using standard floor height."""
    if pd.isna(height_ft) or height_ft <= 0:
        return None

    # Average floor height is ~13.67 feet (from previous analysis)
    estimated = round(height_ft / 13.67)
    return max(1, min(estimated, 200))  # Clamp between 1-200


def main():
    logger.info("=" * 60)
    logger.info("Step 6o: Data Recovery")
    logger.info("=" * 60)

    # Load current data
    input_path = "data/intermediate/06n_final_clean.csv"
    df = load_checkpoint(input_path)

    logger.info(f"Loaded {len(df):,} buildings\n")

    # Track recoveries
    recovered_bbl = 0
    recovered_floors_pluto = 0
    recovered_floors_height = 0
    recovered_coords = 0

    # ========================================================================
    # STEP 1: Recover BBL for buildings with coordinates
    # ========================================================================
    logger.info("=" * 60)
    logger.info("STEP 1: Recover BBL via Geocoding")
    logger.info("=" * 60)

    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')

    missing_bbl_with_coords = df[
        df['bbl_numeric'].isna() &
        df['latitude'].notna()
    ]

    logger.info(f"\nFound {len(missing_bbl_with_coords)} buildings with coords but no BBL")

    for idx, row in missing_bbl_with_coords.iterrows():
        address = row['address']
        lat = row['latitude']
        lng = row['longitude']

        logger.info(f"\nTrying: {address}")
        logger.info(f"  Coords: ({lat:.4f}, {lng:.4f})")

        # Try with just the address
        result = geocode_with_nyc_geoclient(address)

        # If that fails and it's in Brooklyn, try adding borough
        if not result.get('bbl') and 'ashland' in address.lower():
            logger.info(f"  Retrying with borough...")
            result = geocode_with_nyc_geoclient(f"{address}, Brooklyn")

        if result.get('bbl'):
            df.at[idx, 'bbl'] = result['bbl']
            if result.get('bin'):
                df.at[idx, 'bin'] = result['bin']
            logger.info(f"  ✓ Found BBL: {result['bbl']}")
            recovered_bbl += 1
        else:
            logger.warning(f"  ✗ Could not find BBL")

        time.sleep(0.3)

    # ========================================================================
    # STEP 2: Recover floor counts from PLUTO
    # ========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Recover Floor Counts from PLUTO")
    logger.info("=" * 60)

    # Reload BBL numeric after potential updates
    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')

    missing_floors_with_bbl = df[
        df['numfloors'].isna() &
        df['bbl_numeric'].notna()
    ]

    logger.info(f"\nFound {len(missing_floors_with_bbl)} buildings with BBL but missing floors")

    if len(missing_floors_with_bbl) > 0:
        # Load PLUTO
        logger.info(f"Loading PLUTO: {config.PLUTO_CSV}")
        pluto = pd.read_csv(config.PLUTO_CSV, low_memory=False)
        logger.info(f"  PLUTO has {len(pluto):,} tax lots")

        # Convert BBL to string for matching - do this on the FULL dataframe first
        df['bbl_str'] = df['bbl_numeric'].astype(str).str.replace('.0', '', regex=False)
        pluto['bbl_str'] = pluto['BBL'].astype(str)

        # Get indices of buildings needing floor data
        indices_to_update = missing_floors_with_bbl.index

        # Join with PLUTO
        enriched = df.loc[indices_to_update].merge(
            pluto[['bbl_str', 'numfloors']],
            left_on='bbl_str',
            right_on='bbl_str',
            how='left',
            suffixes=('', '_pluto')
        )

        for orig_idx, row in enriched.iterrows():
            if pd.notna(row['numfloors_pluto']):
                floors = row['numfloors_pluto']
                df.at[orig_idx, 'numfloors'] = floors
                address = df.at[orig_idx, 'address']
                logger.info(f"  ✓ {address[:50]:50} → {floors} floors")
                recovered_floors_pluto += 1

        df = df.drop(columns=['bbl_str'])

    # ========================================================================
    # STEP 3: Estimate floor counts from height
    # ========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Estimate Floor Counts from Height")
    logger.info("=" * 60)

    missing_floors_with_height = df[
        df['numfloors'].isna() &
        df['height_roof'].notna()
    ]

    logger.info(f"\nFound {len(missing_floors_with_height)} buildings with height but no floors")

    for idx, row in missing_floors_with_height.iterrows():
        height = row['height_roof']
        estimated_floors = estimate_floors_from_height(height)

        if estimated_floors:
            df.at[idx, 'numfloors'] = estimated_floors
            address = row['address']
            logger.info(f"  ✓ {address[:50]:50} {height:6.1f}ft → {estimated_floors} floors")
            recovered_floors_height += 1

    # ========================================================================
    # STEP 4: Attempt to geocode buildings missing coordinates
    # ========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Geocode Buildings Missing Coordinates")
    logger.info("=" * 60)

    # Focus on new additions (existing landmarks have coords in geometry field)
    new_additions_no_coords = df[
        df['source'].isin(['new_additions', 'supplemental_additions']) &
        df['latitude'].isna()
    ]

    logger.info(f"\nFound {len(new_additions_no_coords)} new additions missing coords")

    for idx, row in new_additions_no_coords.iterrows():
        address = row['address']

        # Skip obvious parks/landmarks
        if any(kw in address.lower() for kw in ['park', 'pier', 'plaza', 'island']):
            logger.info(f"  Skipping park/public space: {address}")
            continue

        logger.info(f"\nTrying: {address}")
        result = geocode_with_nyc_geoclient(address)

        if result.get('latitude'):
            df.at[idx, 'latitude'] = result['latitude']
            df.at[idx, 'longitude'] = result['longitude']
            if result.get('bbl'):
                df.at[idx, 'bbl'] = result['bbl']
            if result.get('bin'):
                df.at[idx, 'bin'] = result['bin']

            logger.info(f"  ✓ Found coords: ({result['latitude']:.4f}, {result['longitude']:.4f})")
            if result.get('bbl'):
                logger.info(f"    Bonus BBL: {result['bbl']}")
                recovered_bbl += 1
            recovered_coords += 1
        else:
            logger.warning(f"  ✗ Could not geocode")

        time.sleep(0.3)

    # ========================================================================
    # FINAL STATISTICS
    # ========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("RECOVERY SUMMARY")
    logger.info("=" * 60)

    logger.info(f"\nRecovered:")
    logger.info(f"  BBL:                    {recovered_bbl:4d}")
    logger.info(f"  Floor counts (PLUTO):   {recovered_floors_pluto:4d}")
    logger.info(f"  Floor counts (height):  {recovered_floors_height:4d}")
    logger.info(f"  Coordinates:            {recovered_coords:4d}")
    logger.info(f"  Total data points:      {recovered_bbl + recovered_floors_pluto + recovered_floors_height + recovered_coords:4d}")

    # Current state
    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')
    missing_bbl = df['bbl_numeric'].isna().sum()
    missing_coords = df['latitude'].isna().sum()
    missing_floors = df['numfloors'].isna().sum()

    logger.info(f"\nRemaining missing data:")
    logger.info(f"  BBL:         {missing_bbl:6d} ({missing_bbl/len(df)*100:5.2f}%)")
    logger.info(f"  Coordinates: {missing_coords:6d} ({missing_coords/len(df)*100:5.2f}%)")
    logger.info(f"  Floors:      {missing_floors:6d} ({missing_floors/len(df)*100:5.2f}%)")

    # New additions quality
    new_additions = df[df['source'].isin(['new_additions', 'supplemental_additions'])]
    new_complete = new_additions[
        new_additions['bbl_numeric'].notna() &
        new_additions['latitude'].notna() &
        new_additions['numfloors'].notna()
    ]

    logger.info(f"\nNew additions completeness:")
    logger.info(f"  Fully complete: {len(new_complete)}/{len(new_additions)} ({len(new_complete)/len(new_additions)*100:.1f}%)")

    # Drop temporary column
    df = df.drop(columns=['bbl_numeric'])

    # Save
    output_path = f"{config.INTERMEDIATE_DIR}/06o_recovered.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n{'='*60}")
    logger.info("✓ Step 6o complete")
    logger.info(f"✓ Saved to: {output_path}")
    logger.info("\nNext: Run Step 08 (apply 4K limit and cleanup)")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Step 3b: Complete BBL coverage for all buildings

Input: data/intermediate/03_geocoded.csv
Output: data/intermediate/03b_complete_bbls.csv

Strategies for getting BBLs:
1. Extract centroids from MULTIPOLYGON geometry and reverse geocode
2. Use Building Footprints API to match by BIN
3. Use coordinates to do spatial lookup in Building Footprints
4. Manual fallback for remaining buildings

Goal: 100% BBL coverage
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import requests
from shapely import wkt
from shapely.geometry import Point
from utils import save_checkpoint, load_checkpoint, logger
import config
import time


def extract_centroids_from_geometry(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract centroids from MULTIPOLYGON geometry for buildings without coordinates.
    """
    logger.info("Extracting centroids from geometry...")

    count = 0
    for idx, row in df.iterrows():
        # Skip if already has coordinates
        if pd.notna(row.get('geocoded_lat')) and pd.notna(row.get('geocoded_lng')):
            continue

        # Try to extract centroid from geometry
        if pd.notna(row.get('geometry')):
            try:
                geom = wkt.loads(row['geometry'])
                centroid = geom.centroid
                df.at[idx, 'geocoded_lat'] = centroid.y
                df.at[idx, 'geocoded_lng'] = centroid.x
                count += 1
            except Exception as e:
                logger.debug(f"Failed to extract centroid for {row.get('address')}: {e}")

    logger.info(f"✓ Extracted {count} centroids from geometry")
    return df


def get_bbl_from_building_footprints(lat: float, lng: float, bin_num: str = None) -> dict:
    """
    Get BBL from Building Footprints API using coordinates or BIN.
    Returns dict with bbl, bin, height, etc.
    """
    url = config.BUILDING_FOOTPRINTS_API

    # Strategy 1: Search by BIN if available
    if bin_num:
        params = {
            '$where': f"bin='{bin_num}'",
            '$limit': 1,
            '$select': 'bin,base_bbl,height_roof,construction_year',
            '$$app_token': config.SOCRATA_APP_TOKEN
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    return {
                        'bbl': data[0].get('base_bbl'),
                        'bin': data[0].get('bin'),
                        'height_roof': data[0].get('height_roof'),
                        'source': 'footprints_bin'
                    }
        except Exception as e:
            logger.debug(f"BIN lookup failed: {e}")

    # Strategy 2: Find closest building within 50m using coordinates
    if lat and lng:
        # Buffer of ~50 meters in degrees (rough approximation)
        buffer = 0.0005

        params = {
            '$where': f"within_circle(the_geom, {lat}, {lng}, 50)",
            '$limit': 1,
            '$select': 'bin,base_bbl,height_roof,construction_year',
            '$$app_token': config.SOCRATA_APP_TOKEN
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            time.sleep(config.REQUEST_DELAY)

            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    return {
                        'bbl': data[0].get('base_bbl'),
                        'bin': data[0].get('bin'),
                        'height_roof': data[0].get('height_roof'),
                        'source': 'footprints_spatial'
                    }
        except Exception as e:
            logger.debug(f"Spatial lookup failed for ({lat}, {lng}): {e}")

    return {'bbl': None, 'bin': None, 'source': 'not_found'}


def complete_bbls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill in missing BBLs using various strategies.
    """
    logger.info(f"Completing BBLs for {len(df)} buildings...")

    initial_bbl_count = df['bbl'].notna().sum()
    logger.info(f"  Starting BBL coverage: {initial_bbl_count}/{len(df)} ({initial_bbl_count/len(df)*100:.1f}%)")

    # First, extract centroids for buildings without coordinates
    df = extract_centroids_from_geometry(df)

    # Count buildings that still need BBLs
    missing_bbls = df[df['bbl'].isna()].copy()
    logger.info(f"  Buildings still missing BBL: {len(missing_bbls)}")

    if len(missing_bbls) == 0:
        logger.info("✓ All buildings already have BBLs!")
        return df

    # Try to get BBLs from Building Footprints API
    logger.info("Looking up BBLs from Building Footprints API...")

    lookups_success = 0
    for idx, row in missing_bbls.iterrows():
        lat = row.get('geocoded_lat')
        lng = row.get('geocoded_lng')
        bin_num = row.get('bin')

        # Skip if no coordinates and no BIN
        if (pd.isna(lat) or pd.isna(lng)) and pd.isna(bin_num):
            continue

        # Try to get BBL
        result = get_bbl_from_building_footprints(lat, lng, bin_num)

        if result['bbl']:
            df.at[idx, 'bbl'] = result['bbl']
            if result.get('bin') and pd.isna(df.at[idx, 'bin']):
                df.at[idx, 'bin'] = result['bin']
            lookups_success += 1

            if lookups_success % 100 == 0:
                logger.info(f"  Found {lookups_success} BBLs so far...")

    logger.info(f"✓ Found {lookups_success} additional BBLs from Building Footprints")

    # Final summary
    final_bbl_count = df['bbl'].notna().sum()
    logger.info(f"\n✓ Final BBL coverage: {final_bbl_count}/{len(df)} ({final_bbl_count/len(df)*100:.1f}%)")
    logger.info(f"  Improvement: +{final_bbl_count - initial_bbl_count} BBLs")

    # Report buildings still missing BBLs
    still_missing = df[df['bbl'].isna()]
    if len(still_missing) > 0:
        logger.warning(f"\n⚠ {len(still_missing)} buildings still missing BBL:")
        for _, row in still_missing.head(10).iterrows():
            logger.warning(f"  - {row.get('address')} ({row.get('source')})")
        if len(still_missing) > 10:
            logger.warning(f"  ... and {len(still_missing) - 10} more")

    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 3b: Complete BBL Coverage")
    logger.info("=" * 60)

    # Load geocoded buildings
    input_path = f"{config.INTERMEDIATE_DIR}/03_geocoded.csv"
    logger.info(f"Loading: {input_path}")
    df = load_checkpoint(input_path)

    # Complete BBLs
    result = complete_bbls(df)

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03b_complete_bbls.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 3b complete")


if __name__ == "__main__":
    main()

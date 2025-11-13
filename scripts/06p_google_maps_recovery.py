#!/usr/bin/env python3
"""
Step 6p: Google Maps Recovery

Use Google Maps Geocoding API to find:
1. Coordinates for 14 new additions without coords
2. BBL/addresses verification for buildings with coords but no BBL

Requires: GOOGLE_MAPS_API_KEY in config.py or environment variable

Input: data/intermediate/06o_recovered.csv
Output: data/intermediate/06p_google_recovered.csv
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


def geocode_with_google_maps(address: str, api_key: str) -> dict:
    """Geocode address with Google Maps Geocoding API."""
    if not api_key:
        return {}

    try:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'address': f"{address}, New York, NY",
            'key': api_key
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get('status') == 'OK' and len(data.get('results', [])) > 0:
            result = data['results'][0]
            location = result['geometry']['location']
            formatted_address = result.get('formatted_address', '')

            return {
                'latitude': location['lat'],
                'longitude': location['lng'],
                'formatted_address': formatted_address,
                'place_id': result.get('place_id'),
                'status': 'success'
            }
        else:
            logger.debug(f"  Google Maps status: {data.get('status')}")

    except Exception as e:
        logger.debug(f"  Google Maps error for {address}: {e}")

    return {}


def geocode_with_nyc_geoclient(address: str) -> dict:
    """Fallback to NYC Geoclient if Google fails."""
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
    logger.info("Step 6p: Google Maps Recovery")
    logger.info("=" * 60)

    # Check for Google Maps API key
    google_api_key = os.environ.get('GOOGLE_MAPS_API_KEY') or getattr(config, 'GOOGLE_MAPS_API_KEY', None)

    if not google_api_key:
        logger.error("No Google Maps API key found!")
        logger.error("Set GOOGLE_MAPS_API_KEY environment variable or add to config.py")
        logger.error("\nYou can get a free API key from:")
        logger.error("https://developers.google.com/maps/documentation/geocoding/get-api-key")
        return

    logger.info(f"Using Google Maps API key: {google_api_key[:8]}...")

    # Load current data
    input_path = "data/intermediate/06o_recovered.csv"
    df = load_checkpoint(input_path)

    logger.info(f"Loaded {len(df):,} buildings\n")

    # Track recoveries
    recovered_coords = 0
    recovered_bbl = 0
    parks_keywords = ['park', 'pier', 'plaza', 'island', 'beach', 'garden', 'playground']

    # ========================================================================
    # STEP 1: Find coordinates for new additions missing coords
    # ========================================================================
    logger.info("=" * 60)
    logger.info("STEP 1: Geocode New Additions with Google Maps")
    logger.info("=" * 60)

    new_no_coords = df[
        df['source'].isin(['new_additions', 'supplemental_additions']) &
        df['latitude'].isna()
    ]

    logger.info(f"\nFound {len(new_no_coords)} new additions missing coords")

    for idx, row in new_no_coords.iterrows():
        address = row['address']

        # Skip parks
        if any(kw in address.lower() for kw in parks_keywords):
            logger.info(f"  Skipping park: {address}")
            continue

        logger.info(f"\nTrying: {address}")

        # Try Google Maps first
        google_result = geocode_with_google_maps(address, google_api_key)

        if google_result.get('latitude'):
            df.at[idx, 'latitude'] = google_result['latitude']
            df.at[idx, 'longitude'] = google_result['longitude']
            logger.info(f"  ✓ Google Maps: ({google_result['latitude']:.6f}, {google_result['longitude']:.6f})")
            logger.info(f"    Formatted: {google_result['formatted_address']}")
            recovered_coords += 1

            # Now try to get BBL with these coords using NYC Geoclient
            time.sleep(0.2)
            nyc_result = geocode_with_nyc_geoclient(address)
            if nyc_result.get('bbl'):
                df.at[idx, 'bbl'] = nyc_result['bbl']
                if nyc_result.get('bin'):
                    df.at[idx, 'bin'] = nyc_result['bin']
                logger.info(f"    Bonus BBL: {nyc_result['bbl']}")
                recovered_bbl += 1

        else:
            logger.warning(f"  ✗ Google Maps failed")

        # Rate limiting
        time.sleep(0.5)

    # ========================================================================
    # STEP 2: Try to get BBL for buildings with coords but no BBL
    # ========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Find BBL for Buildings with Coordinates")
    logger.info("=" * 60)

    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')

    has_coords_no_bbl = df[
        df['bbl_numeric'].isna() &
        df['latitude'].notna() &
        ~df.apply(
            lambda row: any(kw in str(row['building_name']).lower() or kw in str(row['address']).lower()
                           for kw in parks_keywords),
            axis=1
        )
    ]

    logger.info(f"\nFound {len(has_coords_no_bbl)} buildings with coords but no BBL")

    for idx, row in has_coords_no_bbl.iterrows():
        address = row['address']
        lat = row['latitude']
        lng = row['longitude']

        logger.info(f"\n{address}")
        logger.info(f"  Coords: ({lat:.6f}, {lng:.6f})")

        # Try NYC Geoclient with the address
        result = geocode_with_nyc_geoclient(address)

        if result.get('bbl'):
            df.at[idx, 'bbl'] = result['bbl']
            if result.get('bin'):
                df.at[idx, 'bin'] = result['bin']
            logger.info(f"  ✓ Found BBL: {result['bbl']}")
            recovered_bbl += 1
        else:
            # Try reverse geocoding with Google Maps
            logger.info(f"  Trying reverse geocode...")
            try:
                url = "https://maps.googleapis.com/maps/api/geocode/json"
                params = {
                    'latlng': f"{lat},{lng}",
                    'key': google_api_key
                }
                response = requests.get(url, params=params, timeout=10)
                data = response.json()

                if data.get('status') == 'OK' and len(data.get('results', [])) > 0:
                    better_address = data['results'][0].get('formatted_address', '')
                    logger.info(f"  Google says: {better_address}")

                    # Try NYC Geoclient with better address
                    result = geocode_with_nyc_geoclient(better_address)
                    if result.get('bbl'):
                        df.at[idx, 'bbl'] = result['bbl']
                        if result.get('bin'):
                            df.at[idx, 'bin'] = result['bin']
                        logger.info(f"  ✓ Found BBL: {result['bbl']}")
                        recovered_bbl += 1
                    else:
                        logger.warning(f"  ✗ Still no BBL")
                else:
                    logger.warning(f"  ✗ Reverse geocode failed")

            except Exception as e:
                logger.warning(f"  ✗ Error: {e}")

        time.sleep(0.5)

    # ========================================================================
    # FINAL STATISTICS
    # ========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("RECOVERY SUMMARY")
    logger.info("=" * 60)

    logger.info(f"\nRecovered:")
    logger.info(f"  Coordinates: {recovered_coords:4d}")
    logger.info(f"  BBL:         {recovered_bbl:4d}")
    logger.info(f"  Total:       {recovered_coords + recovered_bbl:4d}")

    # Current state
    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')
    missing_bbl = df['bbl_numeric'].isna().sum()
    missing_coords = df['latitude'].isna().sum()
    missing_floors = df['numfloors'].isna().sum()

    logger.info(f"\nRemaining missing data:")
    logger.info(f"  BBL:         {missing_bbl:6d} ({missing_bbl/len(df)*100:5.2f}%)")
    logger.info(f"  Coordinates: {missing_coords:6d} ({missing_coords/len(df)*100:5.2f}%)")
    logger.info(f"  Floors:      {missing_floors:6d} ({missing_floors/len(df)*100:5.2f}%)")

    # Drop temporary column
    df = df.drop(columns=['bbl_numeric'])

    # Save
    output_path = f"{config.INTERMEDIATE_DIR}/06p_google_recovered.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n{'='*60}")
    logger.info("✓ Step 6p complete")
    logger.info(f"✓ Saved to: {output_path}")
    logger.info("\nNext: Run Step 08 (apply 4K limit and cleanup)")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Step 6c: Fix placeholder BBLs by re-geocoding

For buildings with placeholder BBL (5079660001), attempt to get real BBL/BIN:
1. Use input_lat/input_lng if available
2. Try geocoding address again with NYC Geoclient API
3. Try Google Geocoding API as fallback
4. Update BBL/BIN and mark for PLUTO re-enrichment
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import requests
import time
from utils import save_checkpoint, load_checkpoint, logger
import config

PLACEHOLDER_BBL = 5079660001


def geocode_with_nyc_geoclient(address: str, retries: int = 2) -> dict:
    """Try to geocode address with NYC Geoclient API v2."""
    if not hasattr(config, 'NYC_GEOCLIENT_SUBSCRIPTION_KEY'):
        return {}

    try:
        # NYC Geoclient v2 - Search endpoint
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


def geocode_with_google(address: str) -> dict:
    """
    Try to geocode address with Google Geocoding API.
    Returns lat/lng only (not BBL/BIN).
    """
    if not hasattr(config, 'GOOGLE_MAPS_API_KEY'):
        return {}

    try:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'address': f"{address}, New York, NY",
            'key': config.GOOGLE_MAPS_API_KEY
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get('results'):
            location = data['results'][0]['geometry']['location']
            return {
                'latitude': location['lat'],
                'longitude': location['lng'],
                'status': 'google_success'
            }
    except Exception as e:
        logger.debug(f"  Google error for {address}: {e}")

    return {}


def reverse_geocode_coords(lat: float, lng: float) -> dict:
    """Use lat/lng to get BBL/BIN from NYC Geoclient API."""
    if not hasattr(config, 'NYC_GEOCLIENT_SUBSCRIPTION_KEY'):
        return {}

    try:
        # NYC Geoclient v2 doesn't have reverse geocoding
        # Instead, try using the address/latlong endpoint if coordinates are close to a known address
        # For now, we'll skip this approach
        return {}
    except Exception as e:
        logger.debug(f"  Reverse geocode error for {lat},{lng}: {e}")

    return {}


def main():
    logger.info("=" * 60)
    logger.info("Step 6c: Fix Placeholder BBLs")
    logger.info("=" * 60)

    input_path = f"{config.INTERMEDIATE_DIR}/06b_names_canonical.csv"
    df = load_checkpoint(input_path)

    # Find placeholder BBLs
    placeholder_mask = df['bbl'] == PLACEHOLDER_BBL
    placeholder_count = placeholder_mask.sum()

    logger.info(f"\nFound {placeholder_count} buildings with placeholder BBL")

    if placeholder_count == 0:
        logger.info("No placeholder BBLs to fix!")
        output_path = f"{config.INTERMEDIATE_DIR}/06c_fixed_placeholders.csv"
        save_checkpoint(df, output_path)
        return

    fixed_count = 0
    google_geocoded = 0
    still_placeholder = 0

    for idx in df[placeholder_mask].index:
        address = df.at[idx, 'address']
        input_lat = df.at[idx, 'input_lat']
        input_lng = df.at[idx, 'input_lng']
        current_lat = df.at[idx, 'latitude']
        current_lng = df.at[idx, 'longitude']

        logger.info(f"\nProcessing: {address}")

        # Strategy 1: Use input coords if available
        if pd.notna(input_lat) and pd.notna(input_lng):
            logger.info(f"  Has input coords: {input_lat}, {input_lng}")

            # Try reverse geocode
            result = reverse_geocode_coords(input_lat, input_lng)
            if result.get('bbl'):
                df.at[idx, 'bbl'] = result['bbl']
                df.at[idx, 'bin'] = result['bin']
                df.at[idx, 'latitude'] = input_lat
                df.at[idx, 'longitude'] = input_lng
                df.at[idx, 'geocode_status'] = 'reverse_geocode_success'
                logger.info(f"  ✓ Fixed with reverse geocode: BBL={result['bbl']}")
                fixed_count += 1
                time.sleep(0.1)
                continue

        # Strategy 2: Try geocoding address directly
        result = geocode_with_nyc_geoclient(address)
        if result.get('bbl'):
            df.at[idx, 'bbl'] = result['bbl']
            df.at[idx, 'bin'] = result['bin']
            df.at[idx, 'latitude'] = result.get('latitude', current_lat)
            df.at[idx, 'longitude'] = result.get('longitude', current_lng)
            df.at[idx, 'geocode_status'] = 'geoclient_success'
            logger.info(f"  ✓ Fixed with Geoclient: BBL={result['bbl']}")
            fixed_count += 1
            time.sleep(0.1)
            continue

        # Strategy 3: Use Google to get coords, then reverse geocode
        if pd.isna(current_lat) or pd.isna(current_lng):
            google_result = geocode_with_google(address)
            if google_result.get('latitude'):
                logger.info(f"  Got coords from Google: {google_result['latitude']}, {google_result['longitude']}")

                # Try reverse geocode with Google coords
                reverse_result = reverse_geocode_coords(google_result['latitude'], google_result['longitude'])
                if reverse_result.get('bbl'):
                    df.at[idx, 'bbl'] = reverse_result['bbl']
                    df.at[idx, 'bin'] = reverse_result['bin']
                    df.at[idx, 'latitude'] = google_result['latitude']
                    df.at[idx, 'longitude'] = google_result['longitude']
                    df.at[idx, 'geocode_status'] = 'google_reverse_success'
                    logger.info(f"  ✓ Fixed with Google + reverse: BBL={reverse_result['bbl']}")
                    fixed_count += 1
                    google_geocoded += 1
                    time.sleep(0.15)
                    continue
                else:
                    # At least we got coords
                    df.at[idx, 'latitude'] = google_result['latitude']
                    df.at[idx, 'longitude'] = google_result['longitude']
                    df.at[idx, 'geocode_status'] = 'google_coords_only'
                    google_geocoded += 1
                    logger.warning(f"  ⚠ Got coords but no BBL")
                    time.sleep(0.15)

        logger.warning(f"  ✗ Could not fix placeholder BBL")
        still_placeholder += 1

    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"  Started with: {placeholder_count} placeholder BBLs")
    logger.info(f"  Fixed: {fixed_count}")
    logger.info(f"  Got Google coords only: {google_geocoded - fixed_count}")
    logger.info(f"  Still placeholder: {still_placeholder}")
    logger.info(f"  Success rate: {fixed_count/placeholder_count*100:.1f}%")

    output_path = f"{config.INTERMEDIATE_DIR}/06c_fixed_placeholders.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n✓ Step 6c complete")
    logger.info(f"Next: Re-run Step 04 (PLUTO enrichment) to get year/floors for newly fixed BBLs")


if __name__ == "__main__":
    main()

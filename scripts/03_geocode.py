#!/usr/bin/env python3
"""
Step 3: Geocode addresses to BBL/BIN via NYC Geoclient API

Input: data/intermediate/02_combined_with_gaps.csv
Output: data/intermediate/03_geocoded.csv

Uses NYC Geoclient API to get authoritative BBL, BIN, and normalized addresses.
Falls back to existing coordinates if geocoding fails.

Adds columns:
- bbl: Borough-Block-Lot identifier
- bin: Building Identification Number
- borough_code: 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island
- borough_name: Full borough name
- normalized_address: Cleaned address from Geoclient
- geocoded_lat: Authoritative latitude from Geoclient
- geocoded_lng: Authoritative longitude from Geoclient
- geocode_status: "success", "fallback", or "failed"
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import requests
import time
from tqdm import tqdm
from utils import validate_dataframe, parse_point, save_checkpoint, logger
import config


class NYCGeoclient:
    """NYC Geoclient API wrapper"""

    def __init__(self, subscription_key: str = None):
        self.subscription_key = subscription_key or config.NYC_GEOCLIENT_SUBSCRIPTION_KEY
        self.base_url = config.NYC_GEOCLIENT_BASE_URL
        self.available = bool(self.subscription_key)

        if not self.available:
            logger.warning("⚠ NYC Geoclient key not set - will use fallback coordinates")

    def _make_request(self, endpoint: str, params: dict) -> dict:
        """Make authenticated request to Geoclient API"""
        if not self.available:
            return None

        headers = {
            'Ocp-Apim-Subscription-Key': self.subscription_key,
            'Accept': 'application/json'
        }

        url = f"{self.base_url}/{endpoint}"

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            time.sleep(config.REQUEST_DELAY)  # Rate limiting

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logger.warning("Rate limit hit, waiting 60s...")
                time.sleep(60)
                return None
            else:
                logger.debug(f"Geoclient API error {response.status_code}")
                return None

        except Exception as e:
            logger.debug(f"Geoclient request failed: {e}")
            return None

    def parse_address(self, address_str: str) -> tuple:
        """
        Simple address parser to extract house number and street.

        Examples:
        "390 Park Avenue" -> ("390", "Park Avenue")
        "1 World Trade Center" -> ("1", "World Trade Center")
        """
        if not address_str:
            return (None, None)

        parts = address_str.strip().split(maxsplit=1)
        if len(parts) < 2:
            return (None, None)

        house_number = parts[0]
        street = parts[1]

        # Remove common suffixes like ", Manhattan"
        if ',' in street:
            street = street.split(',')[0].strip()

        return (house_number, street)

    def geocode_address(self, address: str, borough_hint: str = None) -> dict:
        """
        Geocode address to BBL, BIN, coordinates.

        Returns dict with bbl, bin, lat, lng, borough_code, normalized_address, status
        """
        house_number, street = self.parse_address(address)

        if not house_number or not street:
            return {'status': 'parse_failed'}

        # Try borough hint first, then default to Manhattan
        boroughs_to_try = []
        if borough_hint and isinstance(borough_hint, str):
            boroughs_to_try.append(borough_hint.lower())
        boroughs_to_try.extend(['manhattan', 'brooklyn', 'queens', 'bronx', 'staten island'])

        # Remove duplicates while preserving order
        seen = set()
        boroughs_to_try = [b for b in boroughs_to_try if not (b in seen or seen.add(b))]

        for borough in boroughs_to_try:
            params = {
                'houseNumber': house_number,
                'street': street,
                'borough': borough
            }

            response = self._make_request('address.json', params)

            if response and 'address' in response:
                addr_data = response['address']

                # Borough code mapping
                borough_codes = {
                    'manhattan': 1,
                    'bronx': 2,
                    'brooklyn': 3,
                    'queens': 4,
                    'staten island': 5
                }

                return {
                    'bbl': addr_data.get('bbl'),
                    'bin': addr_data.get('buildingIdentificationNumber'),
                    'lat': float(addr_data.get('latitude', 0)) if addr_data.get('latitude') else None,
                    'lng': float(addr_data.get('longitude', 0)) if addr_data.get('longitude') else None,
                    'borough_code': borough_codes.get(borough),
                    'borough_name': addr_data.get('borough'),
                    'normalized_address': addr_data.get('formattedAddress'),
                    'status': 'success'
                }

        return {'status': 'not_found'}


def geocode_buildings(df: pd.DataFrame, geoclient: NYCGeoclient) -> pd.DataFrame:
    """
    Geocode all buildings in dataframe.
    Uses existing coordinates as fallback.
    """
    logger.info(f"Geocoding {len(df)} buildings...")

    # Parse existing coordinates first (from latitude/longitude or geometry columns)
    if 'latitude' in df.columns and 'longitude' in df.columns:
        df['input_lat'] = df['latitude']
        df['input_lng'] = df['longitude']
    elif 'geometry' in df.columns:
        # TODO: Extract centroid from geometry for existing landmarks
        df['input_lat'] = None
        df['input_lng'] = None
    else:
        df['input_lat'] = None
        df['input_lng'] = None

    # Extract borough hint from borough column or location column
    df['borough_hint'] = df.get('borough', df.get('location', ''))

    results = []

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Geocoding"):
        address = row.get('address')
        borough_hint = row.get('borough_hint')

        # Skip if no address
        if pd.isna(address) or not address:
            results.append({
                'bbl': row.get('bbl'),
                'bin': row.get('bin'),
                'geocoded_lat': row.get('input_lat'),
                'geocoded_lng': row.get('input_lng'),
                'borough_code': None,
                'borough_name': borough_hint if pd.notna(borough_hint) else None,
                'normalized_address': None,
                'geocode_status': 'no_address'
            })
            continue

        if not geoclient.available:
            # Fallback mode - use existing coordinates
            result = {
                'bbl': None,
                'bin': None,
                'geocoded_lat': row['input_lat'],
                'geocoded_lng': row['input_lng'],
                'borough_code': None,
                'borough_name': borough_hint if borough_hint else None,
                'normalized_address': address,
                'geocode_status': 'fallback'
            }
        else:
            # Try geocoding
            geocode_result = geoclient.geocode_address(address, borough_hint)

            if geocode_result['status'] == 'success':
                result = {
                    'bbl': geocode_result['bbl'],
                    'bin': geocode_result['bin'],
                    'geocoded_lat': geocode_result['lat'],
                    'geocoded_lng': geocode_result['lng'],
                    'borough_code': geocode_result['borough_code'],
                    'borough_name': geocode_result['borough_name'],
                    'normalized_address': geocode_result['normalized_address'],
                    'geocode_status': 'success'
                }
            else:
                # Use fallback coordinates
                result = {
                    'bbl': None,
                    'bin': None,
                    'geocoded_lat': row['input_lat'],
                    'geocoded_lng': row['input_lng'],
                    'borough_code': None,
                    'borough_name': borough_hint if borough_hint else None,
                    'normalized_address': address,
                    'geocode_status': f'fallback_{geocode_result["status"]}'
                }

        results.append(result)

    # Add results to dataframe
    results_df = pd.DataFrame(results)
    result = pd.concat([df, results_df], axis=1)

    # Summary statistics
    status_counts = result['geocode_status'].value_counts()
    logger.info("\nGeocoding Summary:")
    for status, count in status_counts.items():
        logger.info(f"  {status}: {count}")

    success_count = (result['geocode_status'] == 'success').sum()
    logger.info(f"\n✓ Successfully geocoded: {success_count}/{len(df)} ({success_count/len(df)*100:.1f}%)")

    return result


def main():
    logger.info("=" * 60)
    logger.info("Step 3: Geocoding Addresses")
    logger.info("=" * 60)

    # Initialize geoclient
    geoclient = NYCGeoclient()

    if not geoclient.available:
        logger.warning("\n⚠ Running in FALLBACK mode (no Geoclient API key)")
        logger.warning("  Will use existing coordinates from POINT data")
        logger.warning("  To enable geocoding, set NYC_GEOCLIENT_SUBSCRIPTION_KEY in config.py\n")

    # Load input
    input_path = f"{config.INTERMEDIATE_DIR}/02_combined_with_gaps.csv"
    logger.info(f"Loading: {input_path}")
    df = pd.read_csv(input_path)
    logger.info(f"  Loaded {len(df)} buildings")

    # Check if address column exists
    if 'address' not in df.columns:
        logger.error("  'address' column not found in dataset")
        return

    # Geocode
    result = geocode_buildings(df, geoclient)

    # Examples
    logger.info("\nExample results:")
    sample = result[['address', 'bbl', 'borough_name', 'geocode_status']].head(3)
    for _, row in sample.iterrows():
        logger.info(f"  {row['address']}")
        logger.info(f"    BBL: {row['bbl']}, Borough: {row['borough_name']}, Status: {row['geocode_status']}")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03_geocoded.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 1 complete")


if __name__ == "__main__":
    main()

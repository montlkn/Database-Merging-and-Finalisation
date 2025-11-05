#!/usr/bin/env python3
"""
Step 3c: Re-geocode buildings missing BBLs using Geoclient API

Input: data/intermediate/03b_complete_bbls.csv
Output: data/intermediate/03c_regeocode_for_bbls.csv

Strategy:
- For buildings with addresses but no BBL
- Clean up address format (remove "aka" portions)
- Re-geocode using Geoclient API to get BBL
-

Goal: Get as close to 100% BBL coverage as possible
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import requests
import time
from utils import save_checkpoint, load_checkpoint, logger
import config


class NYCGeoclient:
    """NYC Geoclient API wrapper"""

    def __init__(self, subscription_key: str = None):
        self.subscription_key = subscription_key or config.NYC_GEOCLIENT_SUBSCRIPTION_KEY
        self.base_url = config.NYC_GEOCLIENT_BASE_URL
        self.available = bool(self.subscription_key)

    def _make_request(self, endpoint: str, params: dict) -> dict:
        if not self.available:
            return None

        headers = {
            'Ocp-Apim-Subscription-Key': self.subscription_key,
            'Accept': 'application/json'
        }

        url = f"{self.base_url}/{endpoint}"

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            time.sleep(config.REQUEST_DELAY)

            if response.status_code == 200:
                return response.json()
            else:
                return None
        except Exception as e:
            return None

    def parse_address(self, address_str: str) -> tuple:
        if not address_str:
            return (None, None)

        parts = address_str.strip().split(maxsplit=1)
        if len(parts) < 2:
            return (None, None)

        house_number = parts[0]
        street = parts[1]

        if ',' in street:
            street = street.split(',')[0].strip()

        return (house_number, street)

    def geocode_address(self, address: str, borough_hint: str = None) -> dict:
        house_number, street = self.parse_address(address)

        if not house_number or not street:
            return {'status': 'parse_failed'}

        boroughs_to_try = []
        if borough_hint and isinstance(borough_hint, str):
            boroughs_to_try.append(borough_hint.lower())
        boroughs_to_try.extend(['manhattan', 'brooklyn', 'queens', 'bronx', 'staten island'])

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


def clean_address_for_geocoding(address: str) -> str:
    """
    Clean address format for better geocoding results.
    - Remove "(aka ...)" portions
    - Take first address if multiple
    """
    if pd.isna(address):
        return None

    # Remove everything after " (aka"
    if " (aka" in address:
        address = address.split(" (aka")[0].strip()

    # Remove everything after " aka"
    if " aka" in address.lower():
        address = address.split(" aka")[0].strip()

    return address


def regeocode_for_bbls(df: pd.DataFrame, geoclient: NYCGeoclient) -> pd.DataFrame:
    """
    Re-geocode buildings that have addresses but no BBL.
    """
    logger.info(f"Re-geocoding buildings for BBL completion...")

    if not geoclient.available:
        logger.warning("Geoclient API not available - skipping re-geocoding")
        return df

    # Find buildings that need re-geocoding
    needs_geocoding = df[
        (df['bbl'].isna()) &  # No BBL
        (df['address'].notna())  # Has address
    ].copy()

    logger.info(f"  Found {len(needs_geocoding)} buildings to re-geocode")

    if len(needs_geocoding) == 0:
        return df

    success_count = 0
    for idx, row in needs_geocoding.iterrows():
        address = row['address']

        # Clean address
        clean_addr = clean_address_for_geocoding(address)
        if not clean_addr:
            continue

        # Try geocoding
        borough_hint = row.get('borough') or row.get('location') or row.get('borough_name')
        result = geoclient.geocode_address(clean_addr, borough_hint)

        if result['status'] == 'success' and result.get('bbl'):
            df.at[idx, 'bbl'] = result['bbl']
            if result.get('bin') and pd.isna(df.at[idx, 'bin']):
                df.at[idx, 'bin'] = result['bin']
            if result.get('lat') and pd.isna(df.at[idx, 'geocoded_lat']):
                df.at[idx, 'geocoded_lat'] = result['lat']
                df.at[idx, 'geocoded_lng'] = result['lng']
            if result.get('borough_code') and pd.isna(df.at[idx, 'borough_code']):
                df.at[idx, 'borough_code'] = result['borough_code']
                df.at[idx, 'borough_name'] = result['borough_name']

            success_count += 1

            if success_count % 100 == 0:
                logger.info(f"  Found {success_count} BBLs so far...")

    logger.info(f"✓ Re-geocoded {success_count} buildings successfully")

    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 3c: Re-geocode for BBL Completion")
    logger.info("=" * 60)

    # Initialize geoclient
    geoclient = NYCGeoclient()

    if not geoclient.available:
        logger.error("NYC Geoclient API key not set - cannot proceed")
        logger.error("Set NYC_GEOCLIENT_SUBSCRIPTION_KEY in config.py")
        return

    # Load buildings
    input_path = f"{config.INTERMEDIATE_DIR}/03b_complete_bbls.csv"
    logger.info(f"Loading: {input_path}")
    df = load_checkpoint(input_path)

    initial_bbl_count = df['bbl'].notna().sum()
    logger.info(f"  Starting BBL coverage: {initial_bbl_count}/{len(df)} ({initial_bbl_count/len(df)*100:.1f}%)")

    # Re-geocode
    result = regeocode_for_bbls(df, geoclient)

    # Final summary
    final_bbl_count = result['bbl'].notna().sum()
    logger.info(f"\n✓ Final BBL coverage: {final_bbl_count}/{len(result)} ({final_bbl_count/len(result)*100:.1f}%)")
    logger.info(f"  Improvement: +{final_bbl_count - initial_bbl_count} BBLs")

    # Report buildings still missing BBLs
    still_missing = result[result['bbl'].isna()]
    if len(still_missing) > 0:
        logger.warning(f"\n⚠ {len(still_missing)} buildings still missing BBL:")
        logger.warning("  By source:")
        for source, count in still_missing.groupby('source').size().items():
            logger.warning(f"    {source}: {count}")

        logger.warning("\n  Sample:")
        for _, row in still_missing.head(10).iterrows():
            logger.warning(f"    - {row.get('address')} ({row.get('source')})")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03c_regeocoded.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 3c complete")


if __name__ == "__main__":
    main()

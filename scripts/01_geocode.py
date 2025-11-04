#!/usr/bin/env python3
"""
Step 1: Geocode addresses to BBL/BIN via NYC Geosupport

Input: data/raw/new_additions.csv
Output: data/intermediate/01_geocoded.csv

Adds columns:
- bbl: Borough-Block-Lot identifier
- bin: Building Identification Number
- borough_code: 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island
- validated_lat: Validated latitude
- validated_lng: Validated longitude
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import validate_dataframe, parse_point, save_checkpoint, logger
import config

# TODO: Install NYC Geoclient library
# pip install nyc-geoclient
# from nyc_geoclient import Geoclient


def geocode_address(address: str) -> dict:
    """
    Geocode address using NYC Geosupport/Geoclient API.

    Returns dict with:
    - bbl: str
    - bin: str
    - borough_code: int
    - lat: float
    - lng: float
    - success: bool
    """
    # TODO: Implement actual Geoclient API call
    # For now, return placeholder
    #
    # Example:
    # g = Geoclient(config.NYC_GEOCLIENT_APP_ID, config.NYC_GEOCLIENT_APP_KEY)
    # result = g.address(address)
    # return {
    #     'bbl': result.get('bbl'),
    #     'bin': result.get('buildingIdentificationNumber'),
    #     'borough_code': result.get('firstBoroughName'),
    #     'lat': result.get('latitude'),
    #     'lng': result.get('longitude'),
    #     'success': True
    # }

    logger.warning("Geocoding not yet implemented - using placeholder")
    return {
        'bbl': None,
        'bin': None,
        'borough_code': None,
        'lat': None,
        'lng': None,
        'success': False
    }


def main():
    logger.info("=" * 60)
    logger.info("Step 1: Geocoding Addresses")
    logger.info("=" * 60)

    # Load input
    logger.info(f"Loading: {config.NEW_ADDITIONS_CSV}")
    df = pd.read_csv(config.NEW_ADDITIONS_CSV)
    validate_dataframe(df, ['des_addres', 'geom'])

    logger.info(f"Processing {len(df)} buildings...")

    # Parse existing coordinates
    coords = df['geom'].apply(parse_point)
    df['input_lng'] = coords.apply(lambda x: x[0] if x else None)
    df['input_lat'] = coords.apply(lambda x: x[1] if x else None)

    # Geocode each address
    results = []
    for idx, row in df.iterrows():
        address = row['des_addres']
        logger.info(f"[{idx+1}/{len(df)}] Geocoding: {address}")

        result = geocode_address(address)
        results.append(result)

    # Add results to dataframe
    results_df = pd.DataFrame(results)
    df = pd.concat([df, results_df], axis=1)

    # Validation summary
    success_count = df['success'].sum()
    logger.info(f"Successfully geocoded: {success_count}/{len(df)}")

    if success_count == 0:
        logger.warning("⚠ No addresses geocoded - implement Geoclient API first!")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/01_geocoded.csv"
    save_checkpoint(df, output_path, f"- {success_count} geocoded")

    logger.info("✓ Step 1 complete")


if __name__ == "__main__":
    main()

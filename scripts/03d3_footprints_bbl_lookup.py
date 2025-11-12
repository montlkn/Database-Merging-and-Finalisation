#!/usr/bin/env python3
"""
Step 3d3: Use NYC Building Footprints API to find missing BBLs/BINs

The Building Footprints dataset has BBL/BIN for every building in NYC.
We can search by address or use geocoded coordinates to find the building.

Input: data/intermediate/03d2_exa_bbls.csv
Output: data/intermediate/03d3_footprints_bbls.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import requests
from utils import save_checkpoint, load_checkpoint, logger
import config
import time
from tqdm import tqdm


def search_footprints_by_address(address: str) -> dict:
    """
    Search NYC Building Footprints by address to find BBL/BIN.

    Args:
        address: Building address

    Returns:
        dict with bbl, bin, height
    """
    try:
        # Clean address for search
        search_addr = address.strip().upper()

        # Query the Building Footprints API
        url = config.BUILDING_FOOTPRINTS_API
        params = {
            "$where": f"UPPER(address) LIKE '%{search_addr}%'",
            "$limit": 5,
            "$select": "bin,bbl,heightroof,groundelev,name,address"
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        results = response.json()

        if results and len(results) > 0:
            # Take the first result
            result = results[0]
            bbl = result.get('bbl')
            bin_val = result.get('bin')

            if bbl or bin_val:
                logger.info(f"  ✓ Found via Footprints API: {address} -> BBL={bbl}, BIN={bin_val}")
                return {
                    'bbl': bbl,
                    'bin': bin_val,
                    'height': result.get('heightroof'),
                    'source': 'building_footprints_api'
                }

        return {'bbl': None, 'bin': None, 'height': None, 'source': 'footprints_no_match'}

    except Exception as e:
        logger.warning(f"  Footprints API error for {address}: {str(e)}")
        return {'bbl': None, 'bin': None, 'height': None, 'source': f'footprints_error'}


def main():
    logger.info("=" * 60)
    logger.info("Step 3d3: Use Building Footprints API to Find Missing BBLs")
    logger.info("=" * 60)

    # Load data
    input_path = f"{config.INTERMEDIATE_DIR}/03d2_exa_bbls.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)

    # Find buildings missing BBL
    missing_bbl = df['bbl'].isna()
    num_missing = missing_bbl.sum()

    logger.info(f"\nFound {num_missing} buildings missing BBL/BIN")

    if num_missing == 0:
        logger.info("✓ No buildings missing BBL - nothing to search")
        output_path = f"{config.INTERMEDIATE_DIR}/03d3_footprints_bbls.csv"
        save_checkpoint(df, output_path)
        return

    # Search for missing BBLs using Building Footprints API
    logger.info(f"\nSearching for BBLs using Building Footprints API...")

    found_count = 0

    for idx in tqdm(df[missing_bbl].index, desc="Searching Footprints API"):
        row = df.loc[idx]
        address = row.get('address', '')

        # Skip if no address
        if pd.isna(address) or not str(address).strip():
            continue

        # Search with Building Footprints API
        result = search_footprints_by_address(str(address))

        # Update dataframe if found
        if result.get('bbl'):
            df.loc[idx, 'bbl'] = result['bbl']
            if result.get('bin'):
                df.loc[idx, 'bin'] = result['bin']
            if result.get('height'):
                # Store height for later use
                df.loc[idx, 'height_footprints'] = result['height']

            # Add metadata
            df.loc[idx, 'bbl_source'] = result.get('source', 'building_footprints')

            found_count += 1

        # Rate limit to be respectful of API
        time.sleep(0.1)  # 100ms between requests

    # Summary
    still_missing = df['bbl'].isna().sum()

    logger.info("\n" + "=" * 60)
    logger.info("BUILDING FOOTPRINTS API SEARCH SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Started with {num_missing} missing BBLs")
    logger.info(f"  Found via Footprints API: {found_count}")
    logger.info(f"  Still missing: {still_missing}")
    logger.info(f"  Recovery rate: {found_count/num_missing*100:.1f}%")

    # Save results
    output_path = f"{config.INTERMEDIATE_DIR}/03d3_footprints_bbls.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n✓ Saved: {output_path}")
    logger.info("Next: Run 03e_merge_manual_bbls.py or continue pipeline")


if __name__ == "__main__":
    main()

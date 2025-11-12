#!/usr/bin/env python3
"""
Step 3d2: Use Exa to find missing BBLs/BINs

Searches for buildings missing BBL/BIN using Exa AI to query:
- NYC Zola (property database)
- NYC DOB BIS
- Other NYC property databases

Input: data/intermediate/03d_complete_bbls.csv
Output: data/intermediate/03d2_exa_bbls.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from exa_py import Exa
from utils import save_checkpoint, load_checkpoint, logger
import config
import time
from tqdm import tqdm


def search_bbl_with_exa(address: str, building_name: str = None, exa_client: Exa = None) -> dict:
    """
    Use Exa to search for BBL/BIN for a building.

    Args:
        address: Building address
        building_name: Optional building name
        exa_client: Exa client instance

    Returns:
        dict with bbl, bin, and confidence
    """
    if not exa_client:
        return {'bbl': None, 'bin': None, 'confidence': 0, 'source': 'no_exa'}

    try:
        # Construct search query for NYC property databases
        # Handle NaN building names
        if pd.notna(building_name) and str(building_name).strip():
            query = f"NYC property BBL BIN {building_name} {address} site:zola.nyc.gov OR site:a810-bisweb.nyc.gov"
        else:
            query = f"NYC property BBL BIN {address} site:zola.nyc.gov OR site:a810-bisweb.nyc.gov"

        # Search with Exa
        search_response = exa_client.search_and_contents(
            query,
            type="keyword",
            num_results=3,
            text=True,
        )

        if not search_response.results:
            return {'bbl': None, 'bin': None, 'confidence': 0, 'source': 'exa_no_results'}

        # Parse the results looking for BBL/BIN patterns
        import re

        for result in search_response.results:
            text = result.text if hasattr(result, 'text') else ""

            # Look for BBL pattern (10 digits, often formatted as B-BBBBB-LLLL)
            bbl_match = re.search(r'\b(\d)[-\s]?(\d{5})[-\s]?(\d{4})\b', text)
            if not bbl_match:
                # Try without separators
                bbl_match = re.search(r'\b(\d{10})\b', text)

            # Look for BIN pattern (7 digits)
            bin_match = re.search(r'\bBIN[:\s]+(\d{7})\b', text, re.IGNORECASE)
            if not bin_match:
                bin_match = re.search(r'\b(\d{7})\b', text)

            if bbl_match:
                if len(bbl_match.groups()) == 3:
                    bbl = ''.join(bbl_match.groups())
                else:
                    bbl = bbl_match.group(1)

                bin_val = bin_match.group(1) if bin_match else None

                logger.info(f"  ✓ Found via Exa: BBL={bbl}, BIN={bin_val}")
                return {
                    'bbl': bbl,
                    'bin': bin_val,
                    'confidence': 0.7,
                    'source': 'exa_search',
                    'url': result.url if hasattr(result, 'url') else None
                }

        return {'bbl': None, 'bin': None, 'confidence': 0, 'source': 'exa_no_match'}

    except Exception as e:
        logger.warning(f"  Exa search error for {address}: {str(e)}")
        return {'bbl': None, 'bin': None, 'confidence': 0, 'source': f'exa_error: {str(e)}'}


def main():
    logger.info("=" * 60)
    logger.info("Step 3d2: Use Exa to Find Missing BBLs")
    logger.info("=" * 60)

    # Initialize Exa client
    try:
        exa_client = Exa(api_key=config.EXA_API_KEY)
        logger.info("✓ Exa API initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Exa: {e}")
        logger.error("Skipping Exa BBL search")
        exa_client = None

    # Load data
    input_path = f"{config.INTERMEDIATE_DIR}/03d_complete_bbls.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)

    # Find buildings missing BBL
    missing_bbl = df['bbl'].isna()
    num_missing = missing_bbl.sum()

    logger.info(f"\nFound {num_missing} buildings missing BBL/BIN")

    if num_missing == 0:
        logger.info("✓ No buildings missing BBL - nothing to search")
        output_path = f"{config.INTERMEDIATE_DIR}/03d2_exa_bbls.csv"
        save_checkpoint(df, output_path)
        return

    if not exa_client:
        logger.warning("⚠ No Exa client available - skipping search")
        output_path = f"{config.INTERMEDIATE_DIR}/03d2_exa_bbls.csv"
        save_checkpoint(df, output_path)
        return

    # Search for missing BBLs
    logger.info(f"\nSearching for BBLs using Exa (rate limited to avoid API costs)...")

    found_count = 0

    for idx in tqdm(df[missing_bbl].index, desc="Searching with Exa"):
        row = df.loc[idx]
        address = row.get('address', '')
        building_name = row.get('building_name', '')

        # Skip if no address
        if pd.isna(address) or not str(address).strip():
            continue

        # Search with Exa
        result = search_bbl_with_exa(address, building_name, exa_client)

        # Update dataframe if found
        if result.get('bbl'):
            df.loc[idx, 'bbl'] = result['bbl']
            if result.get('bin'):
                df.loc[idx, 'bin'] = result['bin']

            # Add metadata
            df.loc[idx, 'bbl_source'] = result.get('source', 'exa')
            df.loc[idx, 'bbl_confidence'] = result.get('confidence', 0.7)

            found_count += 1

        # Rate limit to be respectful of API
        time.sleep(1)  # 1 second between requests

    # Summary
    still_missing = df['bbl'].isna().sum()

    logger.info("\n" + "=" * 60)
    logger.info("EXA SEARCH SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Started with {num_missing} missing BBLs")
    logger.info(f"  Found via Exa: {found_count}")
    logger.info(f"  Still missing: {still_missing}")
    logger.info(f"  Recovery rate: {found_count/num_missing*100:.1f}%")

    # Save results
    output_path = f"{config.INTERMEDIATE_DIR}/03d2_exa_bbls.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n✓ Saved: {output_path}")
    logger.info("Next: Run 03e_merge_manual_bbls.py")


if __name__ == "__main__":
    main()

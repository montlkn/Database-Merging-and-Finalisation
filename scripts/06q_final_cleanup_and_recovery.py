#!/usr/bin/env python3
"""
Step 6q: Final Cleanup and Recovery

This is the FINAL data quality pass before Step 08.

Actions:
1. Remove ALL placeholder/default/suspicious values:
   - height = 27.3ft (default placeholder)
   - height = 0.0 (invalid for buildings with floors)
   - numfloors = 2 where height = 27.3ft (estimated from fake height)
2. Final Exa search for remaining missing BBL/coords/floors
3. Generate comprehensive report of buildings with missing/suspicious data

Philosophy: N/A > Placeholder data

Input: data/intermediate/06p_google_recovered.csv
Output: data/intermediate/06q_final_clean.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
import time
from exa_py import Exa
from utils import save_checkpoint, load_checkpoint, logger
import config
import re


# Placeholder/suspicious values to remove
PLACEHOLDER_HEIGHT = 27.3
SUSPICIOUS_FLOOR_COUNT = 2  # When paired with 27.3ft height


def search_with_exa(building_name: str, address: str, exa_client: Exa, needs: dict) -> dict:
    """Search Exa for missing building data."""
    try:
        # Build targeted search query
        search_terms = [address, "New York City"]

        if building_name and building_name != address and building_name != "0":
            search_terms.insert(0, building_name)

        if needs.get('bbl'):
            search_terms.append("BBL BIN block lot property tax")
        if needs.get('coords'):
            search_terms.append("coordinates location address")
        if needs.get('floors'):
            search_terms.append("floors stories height")

        query = " ".join(search_terms)

        logger.info(f"  Searching Exa: {query[:80]}")

        # Search with domain restrictions for better results
        search_response = exa_client.search_and_contents(
            query,
            type="neural",
            num_results=8,
            text=True,
            include_domains=[
                "zola.nyc.gov",
                "a810-bisweb.nyc.gov",
                "propertyshark.com",
                "streeteasy.com",
                "emporis.com",
                "skyscraperpage.com"
            ]
        )

        result = {
            'found_bbl': None,
            'found_bin': None,
            'found_latitude': None,
            'found_longitude': None,
            'found_floors': None,
            'source_url': None
        }

        # Parse results
        for item in search_response.results:
            text = item.text if hasattr(item, 'text') else ""
            url = item.url if hasattr(item, 'url') else ""

            # Extract BBL
            if needs.get('bbl') and not result['found_bbl']:
                _extract_bbl_from_text(text, url, result)

            # Extract BIN
            if needs.get('bbl') and not result['found_bin']:
                bin_match = re.search(r'BIN[:\s]*(\d{7})', text, re.IGNORECASE)
                if bin_match:
                    result['found_bin'] = bin_match.group(1)

            # Extract coordinates
            if needs.get('coords') and not result['found_latitude']:
                coord_match = re.search(r'(-?\d{1,3}\.\d{4,8})\s*,\s*(-?\d{1,3}\.\d{4,8})', text)
                if coord_match:
                    lat, lng = float(coord_match.group(1)), float(coord_match.group(2))
                    # Validate NYC bounds
                    if 40.4 < lat < 41.0 and -74.3 < lng < -73.7:
                        result['found_latitude'] = lat
                        result['found_longitude'] = lng
                        result['source_url'] = url

            # Extract floor count
            if needs.get('floors') and not result['found_floors']:
                floor_patterns = [
                    r'(\d{1,3})\s*(?:story|stories|floors?)',
                    r'(?:story|stories|floors?):\s*(\d{1,3})',
                ]
                for pattern in floor_patterns:
                    floor_match = re.search(pattern, text, re.IGNORECASE)
                    if floor_match:
                        floors = int(floor_match.group(1))
                        if 1 <= floors <= 200:
                            result['found_floors'] = floors
                            break

        # Log findings
        found_items = []
        if result['found_bbl']: found_items.append(f"BBL={result['found_bbl']}")
        if result['found_bin']: found_items.append(f"BIN={result['found_bin']}")
        if result['found_latitude']: found_items.append(f"coords")
        if result['found_floors']: found_items.append(f"floors={result['found_floors']}")

        if found_items:
            logger.info(f"  ✓ Found: {', '.join(found_items)}")
        else:
            logger.info(f"  ✗ Nothing found")

        return result

    except Exception as e:
        logger.error(f"  Exa error: {e}")
        return {}


def _extract_bbl_from_text(text: str, url: str, result: dict) -> None:
    """Extract BBL from text using multiple patterns."""
    PLACEHOLDER_BBL = 5079660001

    if result['found_bbl']:
        return

    # Pattern 1: "BBL: 1-00492-0019"
    bbl_match = re.search(r'BBL[:\s]*(\d)[-\s]?(\d{5})[-\s]?(\d{4})', text, re.IGNORECASE)
    if bbl_match:
        result['found_bbl'] = f"{bbl_match.group(1)}{bbl_match.group(2)}{bbl_match.group(3)}"
        result['source_url'] = url

    # Pattern 2: "Block 492, Lot 19"
    if not result['found_bbl']:
        block_lot_match = re.search(r'Block[:\s]*(\d{1,5})[,\s]+Lot[:\s]*(\d{1,4})', text, re.IGNORECASE)
        if block_lot_match:
            block = block_lot_match.group(1).zfill(5)
            lot = block_lot_match.group(2).zfill(4)
            borough = "1"  # Default Manhattan
            if "brooklyn" in text.lower() or "kings" in text.lower():
                borough = "3"
            elif "queens" in text.lower():
                borough = "4"
            elif "bronx" in text.lower():
                borough = "2"
            elif "staten" in text.lower() or "richmond" in text.lower():
                borough = "5"
            result['found_bbl'] = f"{borough}{block}{lot}"
            result['source_url'] = url

    # Pattern 3: URL-based (zola.nyc.gov)
    if not result['found_bbl'] and 'zola.nyc.gov' in url:
        url_bbl = re.search(r'/(\d{10})', url)
        if url_bbl:
            result['found_bbl'] = url_bbl.group(1)
            result['source_url'] = url

    # Validate BBL
    if result['found_bbl']:
        bbl_str = str(result['found_bbl'])
        if len(bbl_str) != 10 or not bbl_str.isdigit():
            result['found_bbl'] = None
        elif int(bbl_str[0]) not in [1, 2, 3, 4, 5]:
            result['found_bbl'] = None
        elif int(bbl_str) == PLACEHOLDER_BBL:
            result['found_bbl'] = None


def main():
    logger.info("=" * 60)
    logger.info("Step 6q: Final Cleanup and Recovery")
    logger.info("=" * 60)
    logger.info("")
    logger.info("Philosophy: N/A is better than placeholder/fake data")
    logger.info("")

    # Load data
    input_path = "data/intermediate/06p_google_recovered.csv"
    df = load_checkpoint(input_path)

    logger.info(f"Loaded {len(df):,} buildings\n")

    # Initialize Exa client
    exa_api_key = os.environ.get('EXA_API_KEY') or config.EXA_API_KEY
    exa_client = Exa(api_key=exa_api_key)
    logger.info(f"Using Exa API key: {exa_api_key[:8]}...\n")

    # Track changes
    removed_fake_heights = 0
    removed_fake_floors = 0
    removed_zero_heights = 0
    recovered_bbl = 0
    recovered_coords = 0
    recovered_floors = 0

    # ========================================================================
    # STEP 1: Remove placeholder heights (27.3ft)
    # ========================================================================
    logger.info("=" * 60)
    logger.info("STEP 1: Remove Placeholder Heights (27.3ft)")
    logger.info("=" * 60)

    placeholder_height_mask = (
        df['height_roof'].notna() &
        (abs(df['height_roof'] - PLACEHOLDER_HEIGHT) < 0.01)
    )

    placeholder_height_buildings = df[placeholder_height_mask]
    logger.info(f"\nFound {len(placeholder_height_buildings)} buildings with placeholder height (27.3ft)")

    for idx in placeholder_height_buildings.index:
        address = df.at[idx, 'address']
        floors = df.at[idx, 'numfloors']

        logger.info(f"  Removing placeholder height from: {address}")

        # Remove fake height
        df.at[idx, 'height_roof'] = np.nan
        removed_fake_heights += 1

        # If floors = 2 (likely estimated from fake height), remove it too
        if pd.notna(floors) and floors == SUSPICIOUS_FLOOR_COUNT:
            logger.info(f"    Also removing suspicious floor count (2)")
            df.at[idx, 'numfloors'] = np.nan
            removed_fake_floors += 1

    # ========================================================================
    # STEP 2: Fix buildings with height=0 but non-zero floors
    # ========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Remove Invalid Zero Heights")
    logger.info("=" * 60)

    zero_height_mask = (
        (df['height_roof'] == 0) &
        df['numfloors'].notna() &
        (df['numfloors'] > 0)
    )

    zero_height_buildings = df[zero_height_mask]
    logger.info(f"\nFound {len(zero_height_buildings)} buildings with height=0 but have floors")

    for idx in zero_height_buildings.index:
        address = df.at[idx, 'address']
        floors = df.at[idx, 'numfloors']
        logger.info(f"  {address[:50]:50} has {floors} floors but height=0")
        logger.info(f"    Setting height to N/A")
        df.at[idx, 'height_roof'] = np.nan
        removed_zero_heights += 1

    # ========================================================================
    # STEP 3: Final Exa search for remaining missing data
    # ========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Final Exa Search for Missing Data")
    logger.info("=" * 60)

    # Focus on real buildings (not parks)
    parks_keywords = ['park', 'pier', 'plaza', 'island', 'beach', 'garden', 'playground']

    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')

    # Find buildings needing recovery (excluding parks)
    needs_recovery = df[
        (
            df['bbl_numeric'].isna() |
            df['latitude'].isna() |
            df['numfloors'].isna()
        ) &
        ~df.apply(
            lambda row: any(kw in str(row['building_name']).lower() or kw in str(row['address']).lower()
                           for kw in parks_keywords),
            axis=1
        )
    ]

    logger.info(f"\nFound {len(needs_recovery)} real buildings needing data recovery")
    logger.info(f"(Limiting to first 20 to manage API costs)\n")

    # Limit to 20 to manage API costs
    needs_recovery = needs_recovery.head(20)

    for idx, row in needs_recovery.iterrows():
        building_name = str(row['building_name']) if pd.notna(row['building_name']) else ""
        address = row['address']

        needs = {
            'bbl': pd.isna(row['bbl_numeric']),
            'coords': pd.isna(row['latitude']),
            'floors': pd.isna(row['numfloors'])
        }

        # Skip if nothing needed
        if not any(needs.values()):
            continue

        needs_list = []
        if needs['bbl']: needs_list.append("BBL")
        if needs['coords']: needs_list.append("coords")
        if needs['floors']: needs_list.append("floors")

        logger.info(f"\n{building_name if building_name != '0' else address}")
        logger.info(f"  Address: {address}")
        logger.info(f"  Needs: {', '.join(needs_list)}")

        # Search with Exa
        result = search_with_exa(building_name, address, exa_client, needs)

        # Apply findings
        if result.get('found_bbl'):
            df.at[idx, 'bbl'] = result['found_bbl']
            recovered_bbl += 1

        if result.get('found_bin'):
            df.at[idx, 'bin'] = result['found_bin']

        if result.get('found_latitude'):
            df.at[idx, 'latitude'] = result['found_latitude']
            df.at[idx, 'longitude'] = result['found_longitude']
            recovered_coords += 1

        if result.get('found_floors'):
            df.at[idx, 'numfloors'] = result['found_floors']
            recovered_floors += 1

        # Rate limiting
        time.sleep(1.0)

    # ========================================================================
    # STEP 4: Generate comprehensive final report
    # ========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("FINAL DATA QUALITY REPORT")
    logger.info("=" * 60)

    logger.info(f"\nChanges made in this step:")
    logger.info(f"  Removed fake heights (27.3ft):    {removed_fake_heights}")
    logger.info(f"  Removed fake floor counts:        {removed_fake_floors}")
    logger.info(f"  Removed invalid zero heights:     {removed_zero_heights}")
    logger.info(f"  Recovered BBL:                    {recovered_bbl}")
    logger.info(f"  Recovered coordinates:            {recovered_coords}")
    logger.info(f"  Recovered floor counts:           {recovered_floors}")

    # Current completeness
    df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')

    missing_bbl = df['bbl_numeric'].isna().sum()
    missing_coords = df['latitude'].isna().sum()
    missing_floors = df['numfloors'].isna().sum()
    missing_height = df['height_roof'].isna().sum()

    logger.info(f"\nCurrent data completeness:")
    logger.info(f"  BBL:         {len(df) - missing_bbl:6d}/{len(df)} ({(len(df)-missing_bbl)/len(df)*100:5.2f}%)")
    logger.info(f"  Coordinates: {len(df) - missing_coords:6d}/{len(df)} ({(len(df)-missing_coords)/len(df)*100:5.2f}%)")
    logger.info(f"  Floors:      {len(df) - missing_floors:6d}/{len(df)} ({(len(df)-missing_floors)/len(df)*100:5.2f}%)")
    logger.info(f"  Height:      {len(df) - missing_height:6d}/{len(df)} ({(len(df)-missing_height)/len(df)*100:5.2f}%)")

    # Breakdown by type
    missing_bbl_df = df[df['bbl_numeric'].isna()].copy()
    is_park = missing_bbl_df.apply(
        lambda row: any(kw in str(row['building_name']).lower() or kw in str(row['address']).lower()
                       for kw in parks_keywords),
        axis=1
    )

    logger.info(f"\nMissing BBL breakdown:")
    logger.info(f"  Parks/Public spaces: {is_park.sum():3d} (acceptable)")
    logger.info(f"  Real buildings:      {len(missing_bbl_df) - is_park.sum():3d} (needs attention)")

    missing_floors_df = df[df['numfloors'].isna()].copy()
    is_park_floors = missing_floors_df.apply(
        lambda row: any(kw in str(row['building_name']).lower() or kw in str(row['address']).lower()
                       for kw in parks_keywords),
        axis=1
    )

    logger.info(f"\nMissing floors breakdown:")
    logger.info(f"  Parks/Public spaces: {is_park_floors.sum():3d} (acceptable)")
    logger.info(f"  Real buildings:      {len(missing_floors_df) - is_park_floors.sum():3d}")

    # List remaining problem buildings
    real_missing_bbl = missing_bbl_df[~is_park]

    if len(real_missing_bbl) > 0:
        logger.info(f"\nReal buildings still missing BBL ({len(real_missing_bbl)}):")
        for idx, row in real_missing_bbl.head(15).iterrows():
            has_coords = pd.notna(row['latitude'])
            coord_str = f"({row['latitude']:.4f}, {row['longitude']:.4f})" if has_coords else "No coords"
            logger.info(f"  • {row['address'][:45]:45} {coord_str}")

        if len(real_missing_bbl) > 15:
            logger.info(f"  ... and {len(real_missing_bbl) - 15} more")

    # Check for remaining suspicious data
    logger.info(f"\n" + "=" * 60)
    logger.info("SUSPICIOUS DATA CHECK")
    logger.info("=" * 60)

    # Check for other suspicious heights
    suspicious_heights = (
        (df['height_roof'].notna()) &
        (df['height_roof'] < 5) &
        (df['height_roof'] > 0)
    )

    logger.info(f"\nBuildings with suspicious height (0-5ft): {suspicious_heights.sum()}")

    if suspicious_heights.sum() > 0:
        for idx, row in df[suspicious_heights].head(10).iterrows():
            logger.info(f"  • {row['address'][:50]:50} height: {row['height_roof']:.1f}ft")

    # Drop temporary column
    df = df.drop(columns=['bbl_numeric'])

    # Save
    output_path = f"{config.INTERMEDIATE_DIR}/06q_final_clean.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n{'='*60}")
    logger.info("✓ Step 6q complete")
    logger.info(f"✓ Saved to: {output_path}")
    logger.info(f"\nData is now ready for Step 08 (apply 4K limit)")
    logger.info(f"\nKey principle applied: N/A > Placeholder/fake data")


if __name__ == "__main__":
    main()

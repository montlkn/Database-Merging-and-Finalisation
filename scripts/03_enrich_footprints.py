#!/usr/bin/env python3
"""
Step 3: Enrich with NYC Building Footprints (polygons)

Input: data/intermediate/02_pluto_enriched.csv
Output: data/intermediate/03_footprints_enriched.csv

Queries NYC Building Footprints API by BIN/BBL to get actual building polygons.
Uses on-demand API queries instead of downloading entire dataset.

Adds columns:
- footprint_wkt: Building footprint as WKT polygon
- footprint_area_sqft: Ground floor area
- building_height_ft: Height if available
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import requests
import time
from utils import validate_dataframe, save_checkpoint, load_checkpoint, logger
import config


def query_footprint_by_bin(bin_number: str) -> dict:
    """
    Query NYC Building Footprints API by BIN.

    Returns dict with:
    - footprint_wkt: WKT polygon
    - area: Ground floor area
    - height: Building height if available
    - success: bool
    """
    if pd.isna(bin_number):
        return {'footprint_wkt': None, 'area': None, 'height': None, 'success': False}

    try:
        # Socrata API query
        url = f"{config.BUILDING_FOOTPRINTS_API}"
        params = {
            '$where': f"bin='{bin_number}'",
            '$limit': 1,
            '$select': 'the_geom,shape_area,heightroof'
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        if data and len(data) > 0:
            result = data[0]
            return {
                'footprint_wkt': result.get('the_geom', {}).get('coordinates'),
                'area': result.get('shape_area'),
                'height': result.get('heightroof'),
                'success': True
            }

        return {'footprint_wkt': None, 'area': None, 'height': None, 'success': False}

    except Exception as e:
        logger.warning(f"Failed to query footprint for BIN {bin_number}: {e}")
        return {'footprint_wkt': None, 'area': None, 'height': None, 'success': False}


def enrich_footprints(df: pd.DataFrame, batch_size: int = 10) -> pd.DataFrame:
    """
    Enrich buildings with footprint data from API.

    Uses batching and rate limiting to be nice to the API.
    """
    logger.info(f"Querying footprints for {len(df)} buildings...")
    logger.info("This may take a few minutes...")

    results = []
    for idx, row in df.iterrows():
        if idx > 0 and idx % batch_size == 0:
            logger.info(f"  Progress: {idx}/{len(df)}")
            time.sleep(1)  # Rate limit: 1 second per batch

        result = query_footprint_by_bin(row.get('bin'))
        results.append(result)

    # Add results to dataframe
    results_df = pd.DataFrame(results)
    result = pd.concat([df, results_df], axis=1)

    # Summary
    success_count = result['success'].sum()
    logger.info(f"✓ Retrieved {success_count}/{len(df)} footprints ({success_count/len(df)*100:.1f}%)")

    return result


def main():
    logger.info("=" * 60)
    logger.info("Step 3: Building Footprints Enrichment")
    logger.info("=" * 60)

    # Load PLUTO-enriched buildings
    pluto_path = f"{config.INTERMEDIATE_DIR}/02_pluto_enriched.csv"
    logger.info(f"Loading: {pluto_path}")
    df = load_checkpoint(pluto_path)

    # Check if we have BINs
    has_bin = df['bin'].notna().sum()
    logger.info(f"Buildings with BIN: {has_bin}/{len(df)}")

    if has_bin == 0:
        logger.warning("⚠ No BINs available - skipping footprint enrichment")
        logger.warning("  Geocoding (step 1) needs to be implemented first")
        output_path = f"{config.INTERMEDIATE_DIR}/03_footprints_enriched.csv"
        save_checkpoint(df, output_path)
        return

    # Enrich with footprints
    result = enrich_footprints(df)

    # Summary
    logger.info("\nFootprint Summary:")
    logger.info(f"  Buildings with polygons: {result['footprint_wkt'].notna().sum()}")
    logger.info(f"  Buildings with height: {result['height'].notna().sum()}")
    logger.info(f"  Avg area (sqft): {result['area'].mean():.0f}" if result['area'].notna().any() else "  No area data")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/03_footprints_enriched.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 3 complete")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Step 6f: Fix Missing Data Using Building Footprints CSV

Use the local BUILDING_20251104.csv file to:
1. Reverse lookup BBL/BIN from coordinates
2. Get missing coordinates from BBL
3. Get height data
4. Estimate missing floors from height
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
from utils import save_checkpoint, load_checkpoint, logger
import config

PLACEHOLDER_BBL = 5079660001


def load_building_footprints():
    """Load and prep the Building Footprints CSV."""
    logger.info(f"Loading Building Footprints: {config.RAW_DATA_DIR}/BUILDING_20251104.csv")

    footprints = pd.read_csv(
        f"{config.RAW_DATA_DIR}/BUILDING_20251104.csv",
        low_memory=False
    )

    logger.info(f"  Loaded {len(footprints):,} building footprints")

    # Clean up BBL format - remove any decimals
    # Column names: BASE_BBL, BIN, Height Roof, the_geom
    footprints['bbl_clean'] = footprints['BASE_BBL'].astype(str).str.replace('.0', '', regex=False)
    footprints['bin_clean'] = footprints['BIN'].astype(str).str.replace('.0', '', regex=False)

    # Extract coordinates from geometry if available
    if 'the_geom' in footprints.columns:
        # Parse POINT geometry to get lat/lng
        def extract_coords(geom_str):
            if pd.isna(geom_str):
                return None, None
            try:
                # Format: POINT (lng lat) or MULTIPOINT (...)
                if 'POINT' in str(geom_str):
                    coords = str(geom_str).replace('POINT', '').replace('(', '').replace(')', '').strip().split()
                    if len(coords) >= 2:
                        lng, lat = float(coords[0]), float(coords[1])
                        return lat, lng
            except:
                pass
            return None, None

        coords = footprints['the_geom'].apply(extract_coords)
        footprints['footprint_lat'] = [c[0] for c in coords]
        footprints['footprint_lng'] = [c[1] for c in coords]

    return footprints


def find_closest_footprint_by_coords(lat: float, lng: float, footprints: pd.DataFrame, max_distance: float = 0.001) -> dict:
    """Find the closest building footprint to given coordinates."""
    if pd.isna(lat) or pd.isna(lng):
        return {}

    # Filter to footprints with coordinates
    with_coords = footprints[footprints['footprint_lat'].notna()].copy()

    if len(with_coords) == 0:
        return {}

    # Calculate distance (simple Euclidean for now)
    with_coords['distance'] = np.sqrt(
        (with_coords['footprint_lat'] - lat)**2 +
        (with_coords['footprint_lng'] - lng)**2
    )

    # Find closest within max_distance
    closest = with_coords[with_coords['distance'] <= max_distance].sort_values('distance')

    if len(closest) == 0:
        return {}

    match = closest.iloc[0]
    return {
        'bbl': match['bbl_clean'],
        'bin': match['bin_clean'],
        'height': match.get('Height Roof'),
        'latitude': match['footprint_lat'],
        'longitude': match['footprint_lng'],
        'source': 'footprints_coords_match'
    }


def lookup_footprint_by_bbl(bbl: str, footprints: pd.DataFrame) -> dict:
    """Lookup building footprint by BBL."""
    bbl_clean = str(bbl).replace('.0', '')

    if bbl_clean == str(PLACEHOLDER_BBL):
        return {}

    matches = footprints[footprints['bbl_clean'] == bbl_clean]

    if len(matches) == 0:
        return {}

    # If multiple matches, take the first (or aggregate?)
    match = matches.iloc[0]

    return {
        'bin': match['bin_clean'],
        'height': match.get('Height Roof'),
        'latitude': match.get('footprint_lat'),
        'longitude': match.get('footprint_lng'),
        'source': 'footprints_bbl_match'
    }


def estimate_floors_from_height(height_ft: float) -> int:
    """Estimate floor count from height."""
    if pd.isna(height_ft) or height_ft <= 0:
        return None

    # Average floor height is ~13.67 feet
    estimated = round(height_ft / 13.67)
    return max(1, min(estimated, 200))  # Clamp between 1-200


def main():
    logger.info("=" * 60)
    logger.info("Step 6f: Fix Missing Data with Building Footprints CSV")
    logger.info("=" * 60)

    # Load Building Footprints
    footprints = load_building_footprints()

    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/06e_exa_enriched.csv"
    df = load_checkpoint(input_path)

    # Track fixes
    fixed_bbl_from_coords = 0
    fixed_coords_from_bbl = 0
    found_height = 0
    estimated_floors = 0

    # Process new additions
    new_additions_mask = df['source'].isin(['new_additions', 'supplemental_additions'])

    logger.info(f"\nProcessing {new_additions_mask.sum()} new additions...")

    for idx in df[new_additions_mask].index:
        address = df.at[idx, 'address']
        current_bbl = df.at[idx, 'bbl']
        lat = df.at[idx, 'latitude']
        lng = df.at[idx, 'longitude']
        input_lat = df.at[idx, 'input_lat']
        input_lng = df.at[idx, 'input_lng']
        building_name = df.at[idx, 'building_name']
        height = df.at[idx, 'height_roof']

        # Skip parks
        is_park = any(x in str(building_name).lower() for x in ['park', 'pier', 'island', 'plaza'])
        if is_park:
            continue

        updated = False

        # Strategy 1: Use input coordinates to find BBL/BIN (for placeholder BBLs)
        if current_bbl == PLACEHOLDER_BBL and pd.notna(input_lat) and pd.notna(input_lng):
            result = find_closest_footprint_by_coords(input_lat, input_lng, footprints)
            if result.get('bbl'):
                df.at[idx, 'bbl'] = result['bbl']
                df.at[idx, 'bin'] = result['bin']
                df.at[idx, 'latitude'] = input_lat
                df.at[idx, 'longitude'] = input_lng
                if pd.isna(height) and result.get('height'):
                    df.at[idx, 'height_roof'] = result['height']
                    found_height += 1
                logger.info(f"✓ {address[:50]:50} BBL: {result['bbl']}")
                fixed_bbl_from_coords += 1
                updated = True

        # Strategy 2: Use BBL to find missing coordinates and height
        if current_bbl != PLACEHOLDER_BBL and (pd.isna(lat) or pd.isna(lng) or pd.isna(height)):
            result = lookup_footprint_by_bbl(current_bbl, footprints)
            if result:
                if pd.isna(lat) and result.get('latitude'):
                    df.at[idx, 'latitude'] = result['latitude']
                    df.at[idx, 'longitude'] = result['longitude']
                    fixed_coords_from_bbl += 1
                    updated = True
                if pd.isna(height) and result.get('height'):
                    df.at[idx, 'height_roof'] = result['height']
                    found_height += 1
                    updated = True

        # Strategy 3: Use input coords to fill missing coords (even without footprint match)
        if pd.isna(lat) and pd.notna(input_lat):
            df.at[idx, 'latitude'] = input_lat
            df.at[idx, 'longitude'] = input_lng
            fixed_coords_from_bbl += 1
            updated = True

        # Strategy 4: Estimate missing floors from height
        if pd.isna(df.at[idx, 'numfloors']):
            current_height = df.at[idx, 'height_roof']
            if pd.notna(current_height):
                estimated = estimate_floors_from_height(current_height)
                if estimated:
                    df.at[idx, 'numfloors'] = estimated
                    estimated_floors += 1
                    updated = True

    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"  Fixed BBL from coordinates: {fixed_bbl_from_coords}")
    logger.info(f"  Fixed coordinates from BBL: {fixed_coords_from_bbl}")
    logger.info(f"  Found heights: {found_height}")
    logger.info(f"  Estimated floors: {estimated_floors}")
    logger.info(f"  Total improvements: {fixed_bbl_from_coords + fixed_coords_from_bbl + found_height + estimated_floors}")

    # Final stats
    new_additions = df[new_additions_mask]
    remaining_placeholder = (new_additions['bbl'] == PLACEHOLDER_BBL).sum()
    remaining_no_coords = new_additions['latitude'].isna().sum()
    remaining_no_floors = new_additions['numfloors'].isna().sum()
    remaining_no_year = new_additions['year_built'].isna().sum()

    logger.info(f"\n{'='*60}")
    logger.info("REMAINING ISSUES")
    logger.info(f"{'='*60}")
    logger.info(f"  Placeholder BBLs: {remaining_placeholder}")
    logger.info(f"  Missing coordinates: {remaining_no_coords}")
    logger.info(f"  Missing floors: {remaining_no_floors}")
    logger.info(f"  Missing year: {remaining_no_year}")

    output_path = f"{config.INTERMEDIATE_DIR}/06f_footprints_fixed.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n✓ Step 6f complete")
    logger.info(f"Next: Re-run PLUTO enrichment (06d) then Step 08 (cleanup)")


if __name__ == "__main__":
    main()

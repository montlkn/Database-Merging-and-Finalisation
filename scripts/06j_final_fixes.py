#!/usr/bin/env python3
"""
Step 6j: Final Fixes for Remaining 14 Buildings

Uses multiple strategies to fix remaining issues:
1. Geometry: Try spatial search in footprints (nearby buildings)
2. BBLs: Try geocoding API to get BBL from coordinates
3. Coordinates: Try geocoding API
4. Years: Try more Exa searches with DOB-specific queries

Input: data/intermediate/06i_exa_retry_enriched.csv
Output: data/intermediate/06j_final_fixes.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
from shapely.geometry import Point
from shapely import wkt
import geopandas as gpd
from utils import save_checkpoint, load_checkpoint, logger
import config

PLACEHOLDER_BBL = 5079660001


def try_geocoding_for_bbl(address: str, lat: float, lng: float, borough_hint: str = None) -> dict:
    """
    Try to get BBL from address using NYC Geoclient API.
    """
    try:
        # Import NYCGeoclient from the geocode script
        import importlib.util
        geocode_path = os.path.join(os.path.dirname(__file__), '03_geocode.py')
        spec = importlib.util.spec_from_file_location("geocode_module", geocode_path)
        geocode_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(geocode_module)
        NYCGeoclient = geocode_module.NYCGeoclient
        
        geoclient = NYCGeoclient()
        if not geoclient.available:
            return {}
        
        result = geoclient.geocode_address(address, borough_hint)
        
        if result.get('status') == 'success':
            return {
                'bbl': result.get('bbl'),
                'bin': result.get('bin'),
                'lat': result.get('lat'),
                'lng': result.get('lng')
            }
    except Exception as e:
        logger.debug(f"Geocoding failed: {e}")
    
    return {}


def try_spatial_footprint_search(df: pd.DataFrame, footprints: gpd.GeoDataFrame, 
                                  address: str, bbl: int, bin_val: int, 
                                  lat: float, lng: float) -> str:
    """
    Try to find geometry by searching nearby buildings in footprints.
    """
    if pd.isna(lat) or pd.isna(lng):
        return None
    
    try:
        # Create point from coordinates
        building_point = Point(lng, lat)
        building_gdf = gpd.GeoDataFrame([{'geometry': building_point}], crs='EPSG:4326')
        
        # Spatial join to find nearby buildings
        nearby = gpd.sjoin_nearest(
            building_gdf,
            footprints[['the_geom', 'BIN', 'BASE_BBL', 'geometry']],
            how='left',
            max_distance=50,  # 50 meters
            distance_col='distance'
        )
        
        if len(nearby) > 0 and pd.notna(nearby.iloc[0].get('the_geom')):
            # Check if BBL or BIN matches
            row = nearby.iloc[0]
            if (pd.notna(row.get('BASE_BBL')) and int(row['BASE_BBL']) == bbl) or \
               (pd.notna(row.get('BIN')) and int(row['BIN']) == bin_val):
                return row['the_geom']
            
            # If very close, use it anyway
            if row.get('distance', 999) < 10:  # Within 10 meters
                return row['the_geom']
    except Exception as e:
        logger.debug(f"Spatial search failed: {e}")
    
    return None


def try_geocoding_for_coords(address: str, borough_hint: str = None) -> dict:
    """
    Try to get coordinates using geocoding API.
    """
    result = try_geocoding_for_bbl(address, None, None, borough_hint)
    if result.get('lat') and result.get('lng'):
        return {
            'latitude': result['lat'],
            'longitude': result['lng']
        }
    return {}


def main():
    logger.info("=" * 60)
    logger.info("Step 6j: Final Fixes for Remaining 14 Buildings")
    logger.info("=" * 60)
    
    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/06i_exa_retry_enriched.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)
    
    # Load remaining issues
    remaining = pd.read_csv("data/manual/remaining_issues_buildings.csv")
    missing_df = pd.read_csv("data/manual/comprehensive_missing_data.csv")
    remaining = remaining.merge(
        missing_df[['address', 'building_name']],
        on='address',
        how='left'
    )
    
    logger.info(f"Found {len(remaining)} buildings with remaining issues")
    
    # Filter to buildings we can actually fix
    fixable = remaining[
        ~remaining['remaining_issues'].str.contains('missing_bbl', na=False)
    ].copy()
    
    logger.info(f"Will attempt to fix {len(fixable)} buildings")
    
    # Load footprints for geometry search
    logger.info(f"\nLoading footprints for geometry search...")
    try:
        footprints_df = pd.read_csv(f"{config.RAW_DATA_DIR}/BUILDING_20251104.csv", low_memory=False)
        
        # Parse geometry if available
        if 'the_geom' in footprints_df.columns:
            footprints_df['geometry'] = footprints_df['the_geom'].apply(
                lambda x: wkt.loads(x) if pd.notna(x) and isinstance(x, str) else None
            )
            footprints_gdf = gpd.GeoDataFrame(
                footprints_df[footprints_df['geometry'].notna()],
                geometry='geometry',
                crs='EPSG:4326'
            )
            logger.info(f"  Loaded {len(footprints_gdf)} footprints with geometry")
        else:
            footprints_gdf = None
            logger.warning("  No geometry column in footprints")
    except Exception as e:
        logger.warning(f"  Could not load footprints: {e}")
        footprints_gdf = None
    
    fixed_count = 0
    
    for idx, row in fixable.iterrows():
        address = row['address']
        issues = row['remaining_issues'].split(', ')
        
        logger.info(f"\n[{idx+1}/{len(fixable)}] {address}")
        logger.info(f"  Issues: {', '.join(issues)}")
        
        # Find in main dataframe
        main_idx = df[df['address'] == address].index
        if len(main_idx) == 0:
            logger.warning(f"  ⚠ Not found in dataset")
            continue
        
        main_idx = main_idx[0]
        updated = False
        
        # Fix geometry using spatial search
        if 'missing_geometry' in issues:
            bbl = df.at[main_idx, 'bbl']
            bin_val = df.at[main_idx, 'bin']
            lat = df.at[main_idx, 'latitude']
            lng = df.at[main_idx, 'longitude']
            
            if footprints_gdf is not None and pd.notna(lat) and pd.notna(lng):
                geom = try_spatial_footprint_search(df, footprints_gdf, address, bbl, bin_val, lat, lng)
                if geom:
                    df.at[main_idx, 'geometry_footprint'] = geom.wkt if hasattr(geom, 'wkt') else str(geom)
                    logger.info(f"  ✓ Found geometry via spatial search")
                    updated = True
        
        # Get borough hint if available
        borough_hint = df.at[main_idx, 'borough'] if 'borough' in df.columns else None
        
        # Fix BBL using geocoding
        if 'placeholder_bbl' in issues:
            geocode_result = try_geocoding_for_bbl(address, None, None, borough_hint)
            if geocode_result.get('bbl'):
                bbl_val = geocode_result['bbl']
                # Convert to int if it's a string
                if isinstance(bbl_val, str):
                    try:
                        bbl_val = int(bbl_val)
                    except:
                        pass
                if bbl_val != PLACEHOLDER_BBL:
                    df.at[main_idx, 'bbl'] = bbl_val
                    if geocode_result.get('bin'):
                        df.at[main_idx, 'bin'] = geocode_result['bin']
                    logger.info(f"  ✓ Found BBL via geocoding: {bbl_val}")
                    updated = True
        
        # Fix coordinates using geocoding
        if 'missing_coords' in issues:
            geocode_result = try_geocoding_for_coords(address, borough_hint)
            if geocode_result.get('latitude') and geocode_result.get('longitude'):
                df.at[main_idx, 'latitude'] = geocode_result['latitude']
                df.at[main_idx, 'longitude'] = geocode_result['longitude']
                logger.info(f"  ✓ Found coordinates via geocoding: ({geocode_result['latitude']:.4f}, {geocode_result['longitude']:.4f})")
                updated = True
        
        # Fix year - try PLUTO if we have BBL
        if 'missing_year' in issues:
            bbl = df.at[main_idx, 'bbl']
            if pd.notna(bbl) and bbl != PLACEHOLDER_BBL:
                try:
                    pluto = pd.read_csv(config.PLUTO_CSV, low_memory=False)
                    bbl_str = str(int(bbl))
                    pluto_match = pluto[pluto['BBL'].astype(str) == bbl_str]
                    if len(pluto_match) > 0 and pd.notna(pluto_match.iloc[0].get('yearbuilt')):
                        year = pluto_match.iloc[0]['yearbuilt']
                        if year > 0:
                            df.at[main_idx, 'year_built'] = year
                            df.at[main_idx, 'yearbuilt'] = year
                            logger.info(f"  ✓ Found year from PLUTO: {year}")
                            updated = True
                except Exception as e:
                    logger.debug(f"PLUTO lookup failed: {e}")
        
        if updated:
            fixed_count += 1
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Fixed {fixed_count} buildings")
    logger.info(f"{'='*60}")
    
    output_path = f"{config.INTERMEDIATE_DIR}/06j_final_fixes.csv"
    save_checkpoint(df, output_path)
    
    logger.info(f"\n✓ Step 6j complete")


if __name__ == "__main__":
    main()


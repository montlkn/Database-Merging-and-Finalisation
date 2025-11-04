#!/usr/bin/env python3
"""
Step 4: Enrich with PLUTO data

Input: data/intermediate/03_geocoded.csv
Output: data/intermediate/04_pluto_enriched.csv

Adds columns from PLUTO:
- numfloors: Number of floors
- yearbuilt: Year built from PLUTO
- bldgarea: Building area (sq ft)
- landmark: Landmark flag from PLUTO
- bldgclass: Building class code
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point
from utils import validate_dataframe, save_checkpoint, load_checkpoint, logger, validate_year, validate_floors
import config


def join_pluto_spatial(df: pd.DataFrame, pluto: pd.DataFrame) -> pd.DataFrame:
    """
    Join buildings to PLUTO using spatial join on centroids.

    Strategy:
    1. For buildings with geometry (MULTIPOLYGON), extract centroid
    2. For buildings with lat/lng, create Point
    3. Spatial join with PLUTO polygons to find BBL
    4. Fall back to existing BBL if already present
    """
    logger.info(f"Joining {len(df)} buildings to PLUTO ({len(pluto)} tax lots)")

    # Check if PLUTO has geometry
    geom_col = None
    if 'geom' in pluto.columns:
        geom_col = 'geom'
    elif 'the_geom' in pluto.columns:
        geom_col = 'the_geom'

    if geom_col is None:
        logger.warning("PLUTO doesn't have geometry column - falling back to BBL join only")
        return join_pluto_bbl_only(df, pluto)

    # Check if geometry column has any data
    if pluto[geom_col].notna().sum() == 0:
        logger.warning(f"PLUTO '{geom_col}' column is empty - falling back to BBL join only")
        return join_pluto_bbl_only(df, pluto)

    # Create points for buildings
    logger.info("Creating building centroids...")
    points = []
    for idx, row in df.iterrows():
        point = None

        # Try 1: Use existing geometry and extract centroid
        if pd.notna(row.get('geometry')):
            try:
                geom = wkt.loads(row['geometry'])
                point = geom.centroid
            except:
                pass

        # Try 2: Use geocoded lat/lng
        if point is None and pd.notna(row.get('geocoded_lat')) and pd.notna(row.get('geocoded_lng')):
            point = Point(row['geocoded_lng'], row['geocoded_lat'])

        # Try 3: Use original lat/lng
        if point is None and pd.notna(row.get('longitude')) and pd.notna(row.get('latitude')):
            point = Point(row['longitude'], row['latitude'])

        points.append(point)

    df['point'] = points
    valid_points = sum(1 for p in points if p is not None)
    logger.info(f"  Created {valid_points}/{len(df)} valid points")

    # Create GeoDataFrame
    buildings_gdf = gpd.GeoDataFrame(df, geometry='point', crs='EPSG:4326')

    # Parse PLUTO geometry
    logger.info(f"Parsing PLUTO geometries from '{geom_col}' column...")
    pluto['geometry'] = pluto[geom_col].apply(lambda x: wkt.loads(x) if pd.notna(x) else None)
    pluto_gdf = gpd.GeoDataFrame(pluto, geometry='geometry', crs='EPSG:4326')

    # Spatial join
    logger.info("Performing spatial join...")
    joined = gpd.sjoin(buildings_gdf, pluto_gdf[['BBL', 'numfloors', 'yearbuilt', 'bldgarea', 'landmark', 'bldgclass', 'geometry']],
                       how='left', predicate='within')

    # Log match rate
    matched = joined['numfloors'].notna().sum()
    logger.info(f"✓ Matched {matched}/{len(df)} buildings to PLUTO ({matched/len(df)*100:.1f}%)")

    # Validate PLUTO data
    if matched > 0:
        valid_floors = joined['numfloors'].apply(lambda x: validate_floors(x) if pd.notna(x) else False).sum()
        valid_years = joined['yearbuilt'].apply(lambda x: validate_year(x) if pd.notna(x) else False).sum()
        logger.info(f"  Valid floors: {valid_floors}/{matched}")
        logger.info(f"  Valid years: {valid_years}/{matched}")

    # Drop temporary geometry column
    result = pd.DataFrame(joined.drop(columns=['point', 'geometry']))

    return result


def join_pluto_bbl_only(df: pd.DataFrame, pluto: pd.DataFrame) -> pd.DataFrame:
    """
    Fallback: Join buildings to PLUTO by BBL only (no spatial join).
    """
    logger.info(f"Joining {len(df)} buildings to PLUTO by BBL")

    # Select relevant PLUTO columns
    pluto_cols = ['BBL', 'numfloors', 'yearbuilt', 'bldgarea', 'landmark', 'bldgclass']
    pluto_subset = pluto[pluto_cols].copy()

    # Convert BBL to int64 then string for matching
    # BBL from geocoding is float64, needs to be converted to int first
    df['bbl_join'] = df['bbl'].fillna(0).astype('int64').astype(str).replace('0', '')
    pluto_subset['BBL_join'] = pluto_subset['BBL'].astype('int64').astype(str)

    # Left join on BBL
    result = df.merge(pluto_subset, left_on='bbl_join', right_on='BBL_join', how='left', suffixes=('', '_pluto'))

    # Drop temporary join column, keep original BBL
    result = result.drop(columns=['bbl_join', 'BBL_join'])

    # Log match rate
    matched = result['numfloors'].notna().sum()
    logger.info(f"✓ Matched {matched}/{len(df)} buildings to PLUTO ({matched/len(df)*100:.1f}%)")

    # Validate PLUTO data
    if matched > 0:
        valid_floors = result['numfloors'].apply(lambda x: validate_floors(x) if pd.notna(x) else False).sum()
        valid_years = result['yearbuilt'].apply(lambda x: validate_year(x) if pd.notna(x) else False).sum()
        logger.info(f"  Valid floors: {valid_floors}/{matched}")
        logger.info(f"  Valid years: {valid_years}/{matched}")

    # CRITICAL: Mark ALL existing_landmarks as landmarks (they're from LPC landmark database)
    if 'source' in result.columns:
        existing_landmark_mask = result['source'] == 'existing_landmarks'
        result.loc[existing_landmark_mask, 'landmark'] = 'LPC_LANDMARK'
        logger.info(f"✓ Marked {existing_landmark_mask.sum()} existing_landmarks with landmark flag")

    return result


def main():
    logger.info("=" * 60)
    logger.info("Step 4: PLUTO Enrichment")
    logger.info("=" * 60)

    # Load geocoded buildings
    geocoded_path = f"{config.INTERMEDIATE_DIR}/03_geocoded.csv"
    logger.info(f"Loading: {geocoded_path}")
    df = load_checkpoint(geocoded_path)

    # Load PLUTO
    logger.info(f"Loading PLUTO: {config.PLUTO_CSV}")
    pluto = pd.read_csv(config.PLUTO_CSV, low_memory=False)
    logger.info(f"  PLUTO has {len(pluto)} tax lots")

    # Join using spatial join on centroids
    result = join_pluto_spatial(df, pluto)

    # Summary stats
    logger.info("\nEnrichment Summary:")
    logger.info(f"  Buildings with floors: {result['numfloors'].notna().sum()}")
    logger.info(f"  Buildings with year: {result['yearbuilt'].notna().sum()}")
    logger.info(f"  Buildings marked as landmarks: {result['landmark'].notna().sum()}")
    if result['numfloors'].notna().sum() > 0:
        logger.info(f"  Avg floors (where available): {result['numfloors'].mean():.1f}")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/04_pluto_enriched.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 4 complete")


if __name__ == "__main__":
    main()

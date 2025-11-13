#!/usr/bin/env python3
"""
Step 6k: Final Geocoding Pass for Remaining 11 Buildings

Try geocoding for remaining placeholder BBLs and missing coordinates.

Input: data/intermediate/06j_final_fixes.csv
Output: data/intermediate/06k_final_geocoded.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config

PLACEHOLDER_BBL = 5079660001


def main():
    logger.info("=" * 60)
    logger.info("Step 6k: Final Geocoding Pass")
    logger.info("=" * 60)
    
    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/06j_final_fixes.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)
    
    # Import geocoding
    import importlib.util
    geocode_path = os.path.join(os.path.dirname(__file__), '03_geocode.py')
    spec = importlib.util.spec_from_file_location("geocode_module", geocode_path)
    geocode_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(geocode_module)
    NYCGeoclient = geocode_module.NYCGeoclient
    
    geoclient = NYCGeoclient()
    if not geoclient.available:
        logger.warning("Geoclient not available - skipping")
        return
    
    # Remaining buildings
    remaining = [
        'Pier 55',
        '1 South 1st Street',
        '300 Ashland Place',
        '100 Willoughby Street',
        '833 Spofford Avenue',
        '1 Prospect Park West',
        '52-03 Center Boulevard',
        '262 Ashland Place',
        '54 Noll Street',
        '515 High Line',
        '173 and 176 Perry Street'
    ]
    
    new_additions = df[df['source'] == 'new_additions'].copy()
    fixed_count = 0
    
    for address in remaining:
        matched = new_additions[new_additions['address'] == address]
        if len(matched) == 0:
            continue
        
        main_idx = matched.index[0]
        row = matched.iloc[0]
        
        logger.info(f"\n{address}")
        
        # Check what's missing
        needs_bbl = row['bbl'] == PLACEHOLDER_BBL
        needs_coords = pd.isna(row.get('latitude')) or pd.isna(row.get('longitude'))
        
        if not needs_bbl and not needs_coords:
            logger.info("  Already has BBL and coords")
            continue
        
        # Try geocoding
        result = geoclient.geocode_address(address)
        
        if result.get('status') == 'success':
            updated = False
            
            # Update BBL
            if needs_bbl and result.get('bbl'):
                bbl_val = result['bbl']
                if isinstance(bbl_val, str):
                    try:
                        bbl_val = int(bbl_val)
                    except:
                        pass
                if bbl_val != PLACEHOLDER_BBL:
                    df.at[main_idx, 'bbl'] = bbl_val
                    logger.info(f"  ✓ Found BBL: {bbl_val}")
                    updated = True
            
            # Update BIN
            if result.get('bin'):
                df.at[main_idx, 'bin'] = result['bin']
            
            # Update coordinates
            if needs_coords and result.get('lat') and result.get('lng'):
                df.at[main_idx, 'latitude'] = result['lat']
                df.at[main_idx, 'longitude'] = result['lng']
                logger.info(f"  ✓ Found coords: ({result['lat']:.4f}, {result['lng']:.4f})")
                updated = True
            
            if updated:
                fixed_count += 1
        else:
            logger.info(f"  ✗ Not found: {result.get('status')}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Fixed {fixed_count} buildings")
    logger.info(f"{'='*60}")
    
    output_path = f"{config.INTERMEDIATE_DIR}/06k_final_geocoded.csv"
    save_checkpoint(df, output_path)
    
    logger.info(f"\n✓ Step 6k complete")


if __name__ == "__main__":
    main()


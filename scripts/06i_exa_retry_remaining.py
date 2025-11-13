#!/usr/bin/env python3
"""
Step 6i: Retry Exa Enrichment for Remaining Buildings

Improved Exa searches for buildings that still have issues:
- Multiple query variations per building
- More results per search (20 instead of 10)
- Better extraction patterns
- Retry logic with different search terms

Input: data/manual/remaining_issues_buildings.csv
Output: data/intermediate/06i_exa_retry_enriched.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import time
import re
from exa_py import Exa
from utils import save_checkpoint, load_checkpoint, logger
import config

PLACEHOLDER_BBL = 5079660001


def try_multiple_queries(address: str, building_name: str, issues: list, exa_client: Exa) -> dict:
    """
    Try multiple query variations to find missing data.
    """
    result = {
        'found_bbl': None,
        'found_bin': None,
        'found_latitude': None,
        'found_longitude': None,
        'found_year': None,
        'found_floors': None,
        'source_url': None
    }
    
    needs_bbl = 'placeholder_bbl' in issues or 'missing_bbl' in issues
    needs_coords = 'missing_coords' in issues
    needs_year = 'missing_year' in issues
    
    # Query variations to try
    query_variations = []
    
    # Variation 1: With building name if available
    if building_name and building_name != address:
        if needs_bbl:
            query_variations.append(f"{building_name} {address} New York City BBL block lot property tax")
        if needs_coords:
            query_variations.append(f"{building_name} {address} New York City coordinates location address")
        if needs_year:
            query_variations.append(f"{building_name} {address} New York City year built completed construction")
    
    # Variation 2: Address with borough hint
    if needs_bbl:
        query_variations.append(f"{address} New York City BBL BIN property tax lot block")
    if needs_coords:
        query_variations.append(f"{address} New York City location coordinates GPS lat long")
    if needs_year:
        query_variations.append(f"{address} New York City year built construction date completed")
    
    # Variation 3: More specific property search
    if needs_bbl:
        query_variations.append(f"{address} NYC property records tax lot BBL building identification")
    
    # Try each query variation
    for query in query_variations[:3]:  # Limit to 3 variations to avoid too many API calls
        if not any([needs_bbl and not result['found_bbl'], 
                   needs_coords and not result['found_latitude'],
                   needs_year and not result['found_year']]):
            break  # Found everything we need
        
        try:
            logger.info(f"    Trying query: {query[:80]}...")
            search_response = exa_client.search_and_contents(
                query,
                type="neural",
                num_results=20,  # More results
                text=True,
                include_domains=[
                    "zola.nyc.gov",
                    "a810-bisweb.nyc.gov",
                    "propertyshark.com",
                    "streeteasy.com",
                    "nycitymap.com",
                    "nyc.gov",  # Broader NYC domain
                    "wikipedia.org"
                ]
            )
            
            # Extract from results
            for item in search_response.results:
                text = item.text if hasattr(item, 'text') else ""
                url = item.url if hasattr(item, 'url') else ""
                
                # Try BBL extraction
                if needs_bbl and not result['found_bbl']:
                    _extract_bbl_from_text(text, url, result)
                
                # Try coordinates
                if needs_coords and not result['found_latitude']:
                    _extract_coords_from_text(text, result)
                
                # Try year
                if needs_year and not result['found_year']:
                    _extract_year_from_text(text, result)
            
            time.sleep(0.3)  # Rate limiting
            
        except Exception as e:
            logger.debug(f"    Query failed: {e}")
            continue
    
    return result


def _extract_bbl_from_text(text: str, url: str, result: dict) -> None:
    """Extract BBL using multiple patterns."""
    if result['found_bbl']:
        return
    
    # Pattern 1: "BBL: 1-00492-0019"
    bbl_match = re.search(r'BBL[:\s]*(\d)[-\s]?(\d{5})[-\s]?(\d{4})', text, re.IGNORECASE)
    if bbl_match:
        result['found_bbl'] = f"{bbl_match.group(1)}{bbl_match.group(2)}{bbl_match.group(3)}"
        result['source_url'] = url
        return
    
    # Pattern 2: Block and Lot
    block_lot = re.search(r'Block[:\s]*(\d{1,5})[,\s]+Lot[:\s]*(\d{1,4})', text, re.IGNORECASE)
    if block_lot:
        block = block_lot.group(1).zfill(5)
        lot = block_lot.group(2).zfill(4)
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
        return
    
    # Pattern 3: URL extraction
    if 'zola.nyc.gov' in url:
        url_bbl = re.search(r'/(\d{10})', url)
        if url_bbl:
            bbl_str = url_bbl.group(1)
            if int(bbl_str[0]) in [1, 2, 3, 4, 5] and int(bbl_str) != PLACEHOLDER_BBL:
                result['found_bbl'] = bbl_str
                result['source_url'] = url


def _extract_coords_from_text(text: str, result: dict) -> None:
    """Extract coordinates with improved patterns."""
    if result['found_latitude']:
        return
    
    patterns = [
        r'latitude[:\s]*([-]?\d+\.\d+).*longitude[:\s]*([-]?\d+\.\d+)',
        r'lat[:\s]*([-]?\d+\.\d+).*lng[:\s]*([-]?\d+\.\d+)',
        r'(\d{2}\.\d{4,})[,\s]+([-]\d{2,3}\.\d{4,})',
        r'([-]?\d{2}\.\d+)[,\s]+([-]?\d{2,3}\.\d+)',  # More flexible
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                lat = float(match.group(1))
                lng = float(match.group(2))
                if 40.4 < lat < 41.0 and -74.3 < lng < -73.7:
                    result['found_latitude'] = lat
                    result['found_longitude'] = lng
                    return
            except:
                continue


def _extract_year_from_text(text: str, result: dict) -> None:
    """Extract year with improved patterns."""
    if result['found_year']:
        return
    
    patterns = [
        r'built in (\d{4})',
        r'completed in (\d{4})',
        r'constructed in (\d{4})',
        r'year built[:\s]*(\d{4})',
        r'(\d{4})(?:\s*[-–]\s*\d{4})?\s+construction',
        r'constructed[:\s]*(\d{4})',
        r'opened[:\s]*(\d{4})',  # Buildings sometimes say "opened in"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                year = int(match.group(1))
                if 1800 <= year <= 2025:
                    result['found_year'] = year
                    return
            except:
                continue


def main():
    logger.info("=" * 60)
    logger.info("Step 6i: Retry Exa Enrichment for Remaining Buildings")
    logger.info("=" * 60)
    
    exa_api_key = os.environ.get('EXA_API_KEY_ALTERNATE') or config.EXA_API_KEY
    if not exa_api_key:
        logger.error("No Exa API key found!")
        return
    
    try:
        exa_client = Exa(api_key=exa_api_key)
    except Exception as e:
        logger.error(f"Failed to initialize Exa client: {e}")
        return
    
    # Load remaining issues
    remaining_path = "data/manual/remaining_issues_buildings.csv"
    logger.info(f"\nLoading: {remaining_path}")
    remaining_df = pd.read_csv(remaining_path)
    
    # Load comprehensive missing data for building names
    missing_df = pd.read_csv("data/manual/comprehensive_missing_data.csv")
    remaining_df = remaining_df.merge(
        missing_df[['address', 'building_name']],
        on='address',
        how='left'
    )
    
    # Load current pipeline state
    current_path = f"{config.INTERMEDIATE_DIR}/06g_no_placeholders.csv"
    df = load_checkpoint(current_path)
    
    logger.info(f"Found {len(remaining_df)} buildings with remaining issues")
    logger.info(f"Will retry Exa searches with improved strategies")
    
    enriched_count = 0
    
    for idx, row in remaining_df.iterrows():
        address = row['address']
        issues = row['remaining_issues'].split(', ')
        building_name = row.get('building_name', address)
        
        # Skip geometry and missing_bbl (intentionally NA)
        if 'missing_geometry' in issues or 'missing_bbl' in issues:
            continue
        
        logger.info(f"\n[{idx+1}/{len(remaining_df)}] {address}")
        logger.info(f"  Issues: {', '.join(issues)}")
        
        # Try multiple queries
        exa_result = try_multiple_queries(address, building_name, issues, exa_client)
        
        # Find matching row in main dataframe
        main_idx = df[df['address'] == address].index
        if len(main_idx) == 0:
            logger.warning(f"  ⚠ Address not found in main dataframe")
            continue
        
        main_idx = main_idx[0]
        updated = False
        
        # Update with found data
        if exa_result.get('found_bbl') and df.at[main_idx, 'bbl'] == PLACEHOLDER_BBL:
            df.at[main_idx, 'bbl'] = int(exa_result['found_bbl'])
            logger.info(f"  ✓ Found BBL: {exa_result['found_bbl']}")
            updated = True
        
        if exa_result.get('found_latitude') and pd.isna(df.at[main_idx, 'latitude']):
            df.at[main_idx, 'latitude'] = exa_result['found_latitude']
            df.at[main_idx, 'longitude'] = exa_result['found_longitude']
            logger.info(f"  ✓ Found coordinates: ({exa_result['found_latitude']:.4f}, {exa_result['found_longitude']:.4f})")
            updated = True
        
        if exa_result.get('found_year') and pd.isna(df.at[main_idx, 'year_built']):
            df.at[main_idx, 'year_built'] = exa_result['found_year']
            df.at[main_idx, 'yearbuilt'] = exa_result['found_year']
            logger.info(f"  ✓ Found year: {exa_result['found_year']}")
            updated = True
        
        if updated:
            enriched_count += 1
        
        time.sleep(0.2)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Retry Summary: {enriched_count} buildings enriched")
    logger.info(f"{'='*60}")
    
    output_path = f"{config.INTERMEDIATE_DIR}/06i_exa_retry_enriched.csv"
    save_checkpoint(df, output_path)
    
    logger.info(f"\n✓ Step 6i complete")


if __name__ == "__main__":
    main()


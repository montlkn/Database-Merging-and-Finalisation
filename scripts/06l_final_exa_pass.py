#!/usr/bin/env python3
"""
Step 6l: Final Exa Pass with Updated Addresses

One more Exa search with corrected addresses and known information:
- 515 High Line = 515 West 29th Street
- 52-03 Center Boulevard = 52-03 Center Boulevard, Long Island City, NY 11101
- Pier 55 constructed in 2018
- 54 Noll Street - try with updated search

Input: data/intermediate/06k_final_geocoded.csv
Output: data/intermediate/06l_final_exa_pass.csv
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

# Known information
KNOWN_INFO = {
    '515 West 29th Street': {
        'alt_names': ['515 High Line'],
        'search_terms': ['515 West 29th Street', '515 High Line', 'New York City']
    },
    '52-03 Center Boulevard, Long Island City, NY 11101': {
        'alt_names': ['52-03 Center Boulevard'],
        'search_terms': ['52-03 Center Boulevard', 'Long Island City', 'Queens', 'NY 11101']
    },
    'Pier 55': {
        'year': 2018,  # From Wikipedia
        'search_terms': ['Pier 55', 'Little Island', 'Hudson River Park', 'New York City']
    },
    '54 Noll Street': {
        'search_terms': ['54 Noll Street', 'Bushwick', 'Brooklyn', 'Odas Denizen']
    }
}


def search_with_exa(address: str, building_name: str, issues: list, exa_client: Exa) -> dict:
    """Search Exa with improved queries."""
    result = {
        'found_bbl': None,
        'found_bin': None,
        'found_latitude': None,
        'found_longitude': None,
        'found_year': None,
        'source_url': None
    }
    
    needs_bbl = 'placeholder_bbl' in issues
    needs_coords = 'missing_coords' in issues
    needs_year = 'missing_year' in issues
    
    # Check if we have known info
    known = None
    for key, info in KNOWN_INFO.items():
        if address == key or address in info.get('alt_names', []):
            known = info
            break
    
    # Build search query
    if known:
        search_terms = known.get('search_terms', [address, 'New York City'])
    else:
        search_terms = [address, 'New York City']
    
    if building_name and building_name != address:
        search_terms.insert(0, building_name)
    
    if needs_bbl:
        search_terms.append('BBL BIN block lot property tax')
    if needs_coords:
        search_terms.append('coordinates location address')
    if needs_year:
        search_terms.append('year built completed construction')
    
    query = " ".join(search_terms)
    
    try:
        logger.info(f"    Searching: {query[:100]}...")
        search_response = exa_client.search_and_contents(
            query,
            type="neural",
            num_results=15,
            text=True,
            include_domains=[
                "zola.nyc.gov",
                "a810-bisweb.nyc.gov",
                "propertyshark.com",
                "streeteasy.com",
                "nycitymap.com",
                "nyc.gov",
                "wikipedia.org",
                "newyorkyimby.com"  # For 54 Noll Street
            ]
        )
        
        for item in search_response.results:
            text = item.text if hasattr(item, 'text') else ""
            url = item.url if hasattr(item, 'url') else ""
            
            # Extract BBL
            if needs_bbl and not result['found_bbl']:
                _extract_bbl_from_text(text, url, result)
            
            # Extract coordinates
            if needs_coords and not result['found_latitude']:
                _extract_coords_from_text(text, result)
            
            # Extract year
            if needs_year and not result['found_year']:
                _extract_year_from_text(text, result)
        
        time.sleep(0.3)
    except Exception as e:
        logger.debug(f"    Exa search failed: {e}")
    
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
        borough = "1"
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
    """Extract coordinates."""
    if result['found_latitude']:
        return
    
    patterns = [
        r'latitude[:\s]*([-]?\d+\.\d+).*longitude[:\s]*([-]?\d+\.\d+)',
        r'lat[:\s]*([-]?\d+\.\d+).*lng[:\s]*([-]?\d+\.\d+)',
        r'(\d{2}\.\d{4,})[,\s]+([-]\d{2,3}\.\d{4,})',
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
    """Extract year."""
    if result['found_year']:
        return
    
    patterns = [
        r'built in (\d{4})',
        r'completed in (\d{4})',
        r'constructed in (\d{4})',
        r'year built[:\s]*(\d{4})',
        r'opened[:\s]*(\d{4})',
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
    logger.info("Step 6l: Final Exa Pass with Updated Addresses")
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
    
    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/06k_final_geocoded.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)
    
    # Remaining buildings
    remaining = [
        'Pier 55',
        '1 South 1st Street',
        '300 Ashland Place',
        '100 Willoughby Street',
        '833 Spofford Avenue',
        '1 Prospect Park West',
        '52-03 Center Boulevard, Long Island City, NY 11101',
        '262 Ashland Place',
        '54 Noll Street',
        '515 West 29th Street',
        '173 and 176 Perry Street'
    ]
    
    new_additions = df[df['source'] == 'new_additions'].copy()
    missing_df = pd.read_csv("data/manual/comprehensive_missing_data.csv")
    
    fixed_count = 0
    
    for address in remaining:
        # Find in dataframe
        matched = new_additions[new_additions['address'] == address]
        if len(matched) == 0:
            # Try alt names
            for key, info in KNOWN_INFO.items():
                if address in info.get('alt_names', []):
                    matched = new_additions[new_additions['address'] == key]
                    if len(matched) > 0:
                        address = key
                        break
        
        if len(matched) == 0:
            logger.warning(f"  ⚠ {address} not found in dataset")
            continue
        
        main_idx = matched.index[0]
        row = matched.iloc[0]
        
        # Check what's missing
        issues = []
        if row['bbl'] == PLACEHOLDER_BBL:
            issues.append('placeholder_bbl')
        if pd.isna(row.get('latitude')) or pd.isna(row.get('longitude')):
            issues.append('missing_coords')
        if pd.isna(row.get('year_built')):
            issues.append('missing_year')
        
        if not issues:
            logger.info(f"{address}: Already complete")
            continue
        
        logger.info(f"\n{address}")
        logger.info(f"  Issues: {', '.join(issues)}")
        
        # Check for known info first
        known = None
        for key, info in KNOWN_INFO.items():
            if address == key or address in info.get('alt_names', []):
                known = info
                break
        
        # Apply known year
        if 'missing_year' in issues and known and known.get('year'):
            df.at[main_idx, 'year_built'] = known['year']
            df.at[main_idx, 'yearbuilt'] = known['year']
            logger.info(f"  ✓ Found year from known info: {known['year']}")
            issues.remove('missing_year')
            fixed_count += 1
        
        if not issues:
            continue
        
        # Get building name
        building_name = row.get('building_name', address)
        
        # Try Exa search
        exa_result = search_with_exa(address, building_name, issues, exa_client)
        
        updated = False
        
        # Update BBL
        if 'placeholder_bbl' in issues and exa_result.get('found_bbl'):
            bbl_val = int(exa_result['found_bbl'])
            if bbl_val != PLACEHOLDER_BBL:
                df.at[main_idx, 'bbl'] = bbl_val
                logger.info(f"  ✓ Found BBL: {bbl_val}")
                updated = True
        
        # Update coordinates
        if 'missing_coords' in issues and exa_result.get('found_latitude'):
            df.at[main_idx, 'latitude'] = exa_result['found_latitude']
            df.at[main_idx, 'longitude'] = exa_result['found_longitude']
            logger.info(f"  ✓ Found coordinates: ({exa_result['found_latitude']:.4f}, {exa_result['found_longitude']:.4f})")
            updated = True
        
        # Update year
        if 'missing_year' in issues and exa_result.get('found_year'):
            df.at[main_idx, 'year_built'] = exa_result['found_year']
            df.at[main_idx, 'yearbuilt'] = exa_result['found_year']
            logger.info(f"  ✓ Found year: {exa_result['found_year']}")
            updated = True
        
        if updated:
            fixed_count += 1
        
        time.sleep(0.2)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Fixed {fixed_count} buildings")
    logger.info(f"{'='*60}")
    
    output_path = f"{config.INTERMEDIATE_DIR}/06l_final_exa_pass.csv"
    save_checkpoint(df, output_path)
    
    logger.info(f"\n✓ Step 6l complete")


if __name__ == "__main__":
    main()


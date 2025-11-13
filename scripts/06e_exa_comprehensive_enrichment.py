#!/usr/bin/env python3
"""
Step 6e: Comprehensive Exa Enrichment for Missing Data

Uses Exa AI to find missing:
- BBLs (for placeholder BBLs)
- Coordinates (lat/long)
- Year built
- Floor counts
- Canonical building names

Input: data/manual/comprehensive_missing_data.csv
Output: data/intermediate/06e_exa_enriched.csv
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


def search_building_info_with_exa(address: str, building_name: str, exa_client: Exa, issues: str) -> dict:
    """
    Use Exa to search for comprehensive building information.

    Args:
        address: Building address
        building_name: Current building name (may be just address)
        exa_client: Exa API client
        issues: Pipe-separated list of issues (e.g., "placeholder_bbl|missing_coords")

    Returns:
        dict with found information
    """
    if not exa_client:
        return {}

    try:
        # Construct targeted search query based on what's missing
        needs_bbl = 'placeholder_bbl' in issues
        needs_coords = 'missing_coords' in issues
        needs_year = 'missing_year' in issues
        needs_floors = 'missing_floors' in issues

        # Build search query
        search_terms = [address, "New York City"]

        if building_name and building_name != address:
            search_terms.insert(0, building_name)

        if needs_bbl:
            search_terms.append("BBL BIN block lot NYC property tax lot")
        if needs_coords:
            search_terms.append("coordinates location")
        if needs_year:
            search_terms.append("year built completed construction")
        if needs_floors:
            search_terms.append("floors stories height")

        query = " ".join(search_terms)

        # Search with Exa - focus on NYC real estate databases
        logger.info(f"  Searching Exa: {query[:100]}")

        search_response = exa_client.search_and_contents(
            query,
            type="neural",  # Use neural for better semantic understanding
            num_results=10,  # Get more results to increase chances of finding BBL
            text=True,
            include_domains=[
                "zola.nyc.gov",           # NYC property info
                "a810-bisweb.nyc.gov",    # DOB Building info
                "propertyshark.com",      # Property database
                "streeteasy.com",         # Real estate
                "emporis.com",            # Building database
                "skyscraperpage.com",     # Tall buildings
                "wikipedia.org",          # General info
                "nycitymap.com"           # NYC property map
            ]
        )

        result = {
            'found_bbl': None,
            'found_bin': None,
            'found_latitude': None,
            'found_longitude': None,
            'found_year': None,
            'found_floors': None,
            'found_canonical_name': None,
            'source_url': None
        }

        # Parse results
        for item in search_response.results:
            text = item.text if hasattr(item, 'text') else ""
            url = item.url if hasattr(item, 'url') else ""

            # Extract BBL - try multiple patterns
            if needs_bbl and not result['found_bbl']:
                _extract_bbl_from_text(text, url, result)
            
            # Extract BIN (7 digits)
            if needs_bbl and not result['found_bin']:
                bin_match = re.search(r'BIN[:\s]*(\d{7})', text, re.IGNORECASE)
                if bin_match:
                    result['found_bin'] = bin_match.group(1)

            # Extract coordinates
            if needs_coords and not result['found_latitude']:
                # Try various coordinate formats
                coord_patterns = [
                    r'latitude[:\s]*([-]?\d+\.\d+).*longitude[:\s]*([-]?\d+\.\d+)',
                    r'lat[:\s]*([-]?\d+\.\d+).*lng[:\s]*([-]?\d+\.\d+)',
                    r'(\d{2}\.\d{4,})[,\s]+([-]\d{2,3}\.\d{4,})'  # 40.7589, -73.9851
                ]
                for pattern in coord_patterns:
                    coord_match = re.search(pattern, text, re.IGNORECASE)
                    if coord_match:
                        lat = float(coord_match.group(1))
                        lng = float(coord_match.group(2))
                        # Validate NYC bounds (roughly)
                        if 40.4 < lat < 41.0 and -74.3 < lng < -73.7:
                            result['found_latitude'] = lat
                            result['found_longitude'] = lng
                            break

            # Extract year built
            if needs_year and not result['found_year']:
                year_patterns = [
                    r'built in (\d{4})',
                    r'completed in (\d{4})',
                    r'constructed in (\d{4})',
                    r'year built[:\s]*(\d{4})',
                    r'(\d{4})(?:\s*[-–]\s*\d{4})?\s+construction'
                ]
                for pattern in year_patterns:
                    year_match = re.search(pattern, text, re.IGNORECASE)
                    if year_match:
                        year = int(year_match.group(1))
                        if 1800 <= year <= 2025:
                            result['found_year'] = year
                            break

            # Extract floor count
            if needs_floors and not result['found_floors']:
                floor_patterns = [
                    r'(\d{1,3})[\s-](?:story|stories|floor|floors)',
                    r'(?:story|stories|floor|floors)[:\s]*(\d{1,3})',
                    r'height[:\s]*\d+\s*(?:ft|feet).*?(\d{1,3})\s*(?:story|stories|floor|floors)'
                ]
                for pattern in floor_patterns:
                    floor_match = re.search(pattern, text, re.IGNORECASE)
                    if floor_match:
                        floors = int(floor_match.group(1))
                        if 1 <= floors <= 200:
                            result['found_floors'] = floors
                            break

            # Extract canonical building name (from title or prominent mention)
            if not result['found_canonical_name']:
                # Try to get from page title first
                title_match = re.search(r'<title>([^<]+)</title>', text, re.IGNORECASE)
                if title_match:
                    title = title_match.group(1).strip()
                    # Clean up title
                    title = re.sub(r'\s*[-|]\s*(PropertyShark|StreetEasy|Wikipedia|Emporis).*$', '', title)
                    if title and len(title) > 5 and title != address:
                        result['found_canonical_name'] = title[:100]
        
        # If we still need BBL and didn't find it, try a broader search without domain restrictions
        if needs_bbl and not result['found_bbl']:
            logger.info(f"  No BBL found in domain-restricted search, trying broader search...")
            try:
                broader_search = exa_client.search_and_contents(
                    f"{address} New York City BBL block lot property tax",
                    type="neural",
                    num_results=5,
                    text=True
                )
                for item in broader_search.results:
                    text = item.text if hasattr(item, 'text') else ""
                    url = item.url if hasattr(item, 'url') else ""
                    _extract_bbl_from_text(text, url, result)
                    if result['found_bbl']:
                        break
            except Exception as e:
                logger.debug(f"  Broader search failed: {e}")

        # Log what we found
        found_items = []
        if result['found_bbl']: found_items.append(f"BBL={result['found_bbl']}")
        if result['found_bin']: found_items.append(f"BIN={result['found_bin']}")
        if result['found_latitude']: found_items.append(f"coords=({result['found_latitude']:.4f},{result['found_longitude']:.4f})")
        if result['found_year']: found_items.append(f"year={result['found_year']}")
        if result['found_floors']: found_items.append(f"floors={result['found_floors']}")
        if result['found_canonical_name']: found_items.append(f"name={result['found_canonical_name'][:30]}")

        if found_items:
            logger.info(f"  ✓ Found: {', '.join(found_items)}")
        else:
            logger.warning(f"  ✗ Found nothing")

        return result

    except Exception as e:
        logger.error(f"  Exa error: {e}")
        return {}


def _extract_bbl_from_text(text: str, url: str, result: dict) -> None:
    """Helper function to extract BBL from text using multiple patterns."""
    PLACEHOLDER_BBL = 5079660001
    
    if result['found_bbl']:
        return  # Already found
    
    # Pattern 1: "BBL: 1-00492-0019" or "BBL 1-00492-0019"
    bbl_match = re.search(r'BBL[:\s]*(\d)[-\s]?(\d{5})[-\s]?(\d{4})', text, re.IGNORECASE)
    if bbl_match:
        result['found_bbl'] = f"{bbl_match.group(1)}{bbl_match.group(2)}{bbl_match.group(3)}"
        result['source_url'] = url
    
    # Pattern 2: "Block 492, Lot 19" or "Block 492 Lot 19"
    if not result['found_bbl']:
        block_lot_match = re.search(r'Block[:\s]*(\d{1,5})[,\s]+Lot[:\s]*(\d{1,4})', text, re.IGNORECASE)
        if block_lot_match:
            block = block_lot_match.group(1).zfill(5)  # Pad to 5 digits
            lot = block_lot_match.group(2).zfill(4)     # Pad to 4 digits
            # Try to infer borough from context (default to Manhattan=1)
            borough = "1"  # Default to Manhattan
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
    
    # Pattern 3: Standalone 10-digit number (could be BBL)
    if not result['found_bbl']:
        # Look for 10-digit numbers that aren't phone numbers or years
        standalone_bbl = re.search(r'\b(\d)(\d{5})(\d{4})\b', text)
        if standalone_bbl:
            # Validate: first digit should be 1-5 (borough), and not look like a year
            borough_code = int(standalone_bbl.group(1))
            if 1 <= borough_code <= 5:
                # Check if it's not clearly a year (1900-2025)
                full_num = standalone_bbl.group(0)
                if not (1900 <= int(full_num[:4]) <= 2025):
                    result['found_bbl'] = full_num
                    result['source_url'] = url
    
    # Pattern 4: Extract from URL (zola.nyc.gov URLs often contain BBLs)
    if not result['found_bbl'] and 'zola.nyc.gov' in url:
        url_bbl = re.search(r'/(\d{10})', url)
        if url_bbl:
            result['found_bbl'] = url_bbl.group(1)
            result['source_url'] = url
    
    # Validate BBL format before accepting (must be 10 digits, first digit 1-5)
    if result['found_bbl']:
        bbl_str = str(result['found_bbl'])
        if len(bbl_str) != 10 or not bbl_str.isdigit():
            result['found_bbl'] = None
        elif int(bbl_str[0]) not in [1, 2, 3, 4, 5]:
            result['found_bbl'] = None
        elif int(bbl_str) == PLACEHOLDER_BBL:  # Don't accept placeholder
            result['found_bbl'] = None


def main():
    logger.info("=" * 60)
    logger.info("Step 6e: Comprehensive Exa Enrichment")
    logger.info("=" * 60)

    # Allow custom Exa API key via environment variable
    exa_api_key = os.environ.get('EXA_API_KEY_ALTERNATE') or config.EXA_API_KEY

    if not exa_api_key:
        logger.error("No Exa API key found!")
        logger.error("Set EXA_API_KEY_ALTERNATE environment variable or update config.py")
        return

    logger.info(f"Using Exa API key: {exa_api_key[:8]}...")

    try:
        exa_client = Exa(api_key=exa_api_key)
    except Exception as e:
        logger.error(f"Failed to initialize Exa client: {e}")
        return

    # Load missing data CSV
    missing_csv = "data/manual/comprehensive_missing_data.csv"
    logger.info(f"\nLoading: {missing_csv}")
    missing_df = pd.read_csv(missing_csv)

    logger.info(f"  Found {len(missing_df)} buildings with missing data")

    # Load current pipeline state
    current_csv = "data/intermediate/06c_fixed_placeholders.csv"
    df = load_checkpoint(current_csv)

    # Process each building with missing data
    enriched_count = 0

    for idx, row in missing_df.iterrows():
        address = row['address']
        building_name = row['building_name']
        issues = row['issues']

        logger.info(f"\n[{idx+1}/{len(missing_df)}] {address}")
        logger.info(f"  Issues: {issues}")

        # Search with Exa
        exa_result = search_building_info_with_exa(address, building_name, exa_client, issues)

        # Find matching row in main dataframe
        main_idx = df[df['address'] == address].index

        if len(main_idx) == 0:
            logger.warning(f"  ⚠ Address not found in main dataframe: {address}")
            continue

        main_idx = main_idx[0]

        # Update main dataframe with found data
        updated = False

        if exa_result.get('found_bbl') and df.at[main_idx, 'bbl'] == PLACEHOLDER_BBL:
            df.at[main_idx, 'bbl'] = int(exa_result['found_bbl'])
            updated = True

        if exa_result.get('found_bin'):
            df.at[main_idx, 'bin'] = int(exa_result['found_bin'])
            updated = True

        if exa_result.get('found_latitude') and pd.isna(df.at[main_idx, 'latitude']):
            df.at[main_idx, 'latitude'] = exa_result['found_latitude']
            df.at[main_idx, 'longitude'] = exa_result['found_longitude']
            updated = True

        if exa_result.get('found_year') and pd.isna(df.at[main_idx, 'year_built']):
            df.at[main_idx, 'year_built'] = exa_result['found_year']
            df.at[main_idx, 'yearbuilt'] = exa_result['found_year']
            updated = True

        if exa_result.get('found_floors') and pd.isna(df.at[main_idx, 'numfloors']):
            df.at[main_idx, 'numfloors'] = exa_result['found_floors']
            updated = True

        if exa_result.get('found_canonical_name'):
            # Only update if current name is just the address
            if df.at[main_idx, 'building_name'] == address:
                df.at[main_idx, 'building_name'] = exa_result['found_canonical_name']
                df.at[main_idx, 'name_source'] = 'exa'
                updated = True

        if updated:
            enriched_count += 1

        # Rate limiting - be conservative
        time.sleep(0.2)

    logger.info(f"\n{'='*60}")
    logger.info("ENRICHMENT SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"  Buildings processed: {len(missing_df)}")
    logger.info(f"  Buildings enriched: {enriched_count}")
    logger.info(f"  Success rate: {enriched_count/len(missing_df)*100:.1f}%")

    # Save enriched data
    output_path = f"{config.INTERMEDIATE_DIR}/06e_exa_enriched.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n✓ Step 6e complete")
    logger.info(f"Next: Re-run PLUTO enrichment (06d) then Step 08 (cleanup)")


if __name__ == "__main__":
    main()

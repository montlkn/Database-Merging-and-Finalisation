#!/usr/bin/env python3
"""
Step 2: Find missing iconic buildings from various sources

Input:
- data/intermediate/01_combined.csv (current 3,900 buildings)
- NYC Open Data APIs (DOB permits, LPC landmarks)
- Programmatic search criteria (height > 500ft, year > 2000, etc.)

Output:
- data/intermediate/02_gaps_found.csv (50-200 new buildings)
- data/intermediate/02_combined_with_gaps.csv (complete ~4,000-4,500 dataset)

Strategy:
1. Query DOB building permits for tall buildings (2000-2025)
2. Search LPC landmarks database for recent designations
3. Use height/year filters to find iconic modern buildings
4. Cross-reference against existing dataset to avoid duplicates
5. Add found buildings to combined dataset
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
import requests
import time
from typing import List, Dict, Optional
from utils import save_checkpoint, logger
import config


class GapFinder:
    """Find missing iconic buildings from various NYC data sources"""

    def __init__(self, existing_df: pd.DataFrame):
        self.existing_df = existing_df
        self.existing_addresses = set(existing_df['address_normalized'].str.lower().str.strip())
        self.gaps_found = []

    def find_tall_buildings_from_dob(self, min_floors: int = 30, start_year: int = 2000) -> List[Dict]:
        """
        Query DOB (Department of Buildings) for tall building permits.

        Uses NYC Open Data DOB Job Applications API.
        Focus on new buildings (Job Type = NB) with significant height.
        """
        logger.info(f"Searching DOB permits for buildings with {min_floors}+ floors since {start_year}...")

        # DOB Job Applications API endpoint
        url = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"

        # Query parameters - note: fields have double underscores
        params = {
            '$where': f"job_type='NB' AND proposed_no_of_stories >= {min_floors} AND pre__filing_date >= '{start_year}-01-01T00:00:00'",
            '$limit': 5000,
            '$select': 'house__,street_name,borough,proposed_no_of_stories,pre__filing_date,job_description,bin__',
            '$$app_token': config.SOCRATA_APP_TOKEN
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            logger.info(f"  Found {len(data)} DOB permits matching criteria")

            # Convert to standardized format
            buildings = []
            for item in data:
                # Construct address
                house = item.get('house__', '')
                street = item.get('street_name', '')
                address = f"{house} {street}".strip()

                if not address or self._is_duplicate(address):
                    continue

                # Extract year from date
                filing_date = item.get('pre__filing_date', '')
                year = filing_date[:4] if filing_date and len(filing_date) >= 4 else None

                buildings.append({
                    'address': address,
                    'bin': item.get('bin__'),
                    'num_floors': item.get('proposed_no_of_stories'),
                    'borough': item.get('borough'),
                    'year_built': year,
                    'building_type': 'Skyscraper',
                    'source': 'dob_permits',
                    'source_confidence': 0.7,
                    'data_source': 'DOB Job Applications'
                })

            logger.info(f"  Added {len(buildings)} new buildings from DOB (after deduplication)")
            return buildings

        except Exception as e:
            logger.error(f"  Error querying DOB: {e}")
            return []

    def find_recent_landmarks_from_lpc(self, start_year: int = 2000) -> List[Dict]:
        """
        Query LPC (Landmarks Preservation Commission) for recent designations.

        Focus on individual landmarks designated after 2000.
        """
        logger.info(f"Searching LPC database for landmarks designated since {start_year}...")

        try:
            # Load LPC landmarks (already downloaded)
            lpc_df = pd.read_csv(config.LPC_LANDMARKS_CSV)

            # Filter for individual landmarks (not historic districts) with recent designation
            # Field name might vary - check actual column names
            if 'lp_number' in lpc_df.columns:
                # Individual landmarks have LP numbers
                lpc_df = lpc_df[lpc_df['lp_number'].notna()]

            # Filter by designation date if available
            if 'date_desig' in lpc_df.columns or 'caldate' in lpc_df.columns:
                date_col = 'date_desig' if 'date_desig' in lpc_df.columns else 'caldate'
                lpc_df[date_col] = pd.to_datetime(lpc_df[date_col], errors='coerce')
                lpc_df = lpc_df[lpc_df[date_col].dt.year >= start_year]

            logger.info(f"  Found {len(lpc_df)} LPC landmarks from {start_year} onward")

            # Convert to standardized format
            buildings = []
            for _, row in lpc_df.iterrows():
                # Get address (field name varies)
                address = row.get('pluto_addr') or row.get('address') or ''

                if not address or self._is_duplicate(address):
                    continue

                buildings.append({
                    'address': address,
                    'building_name': row.get('lm_name'),
                    'architect': row.get('arch_build'),
                    'style': row.get('style_prim'),
                    'year_built': row.get('date_low'),
                    'borough': row.get('borough'),
                    'source': 'lpc_recent',
                    'source_confidence': 0.9,
                    'data_source': 'LPC Landmarks (recent designations)'
                })

            logger.info(f"  Added {len(buildings)} new buildings from LPC (after deduplication)")
            return buildings

        except Exception as e:
            logger.error(f"  Error processing LPC data: {e}")
            return []

    def find_supertall_buildings(self, min_height_ft: int = 800) -> List[Dict]:
        """
        Find supertall buildings (800+ feet) from Building Footprints API.

        These are iconic skyline buildings that should definitely be included.
        """
        logger.info(f"Searching for supertall buildings ({min_height_ft}+ feet)...")

        url = config.BUILDING_FOOTPRINTS_API

        # Query in batches (API has limits)
        params = {
            '$where': f"height_roof >= {min_height_ft}",
            '$limit': 1000,
            '$select': 'bin,construction_year,height_roof,last_status_type,ground_elevation,base_bbl',
            '$$app_token': config.SOCRATA_APP_TOKEN
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            logger.info(f"  Found {len(data)} supertall buildings")

            # Store BINs for later geocoding (can't get address from footprints directly)
            buildings = []
            for item in data:
                bin_number = item.get('bin')
                if not bin_number:
                    continue

                # We'll need to geocode these BINs to get addresses in step 03
                buildings.append({
                    'address': None,  # Will geocode in step 03
                    'bin': bin_number,
                    'bbl': item.get('base_bbl'),
                    'year_built': item.get('construction_year'),
                    'height': item.get('height_roof'),
                    'building_type': 'Skyscraper',
                    'source': 'footprints_supertall',
                    'source_confidence': 0.8,
                    'data_source': 'Building Footprints (supertall query)',
                    'needs_geocoding': True
                })

            logger.info(f"  Added {len(buildings)} supertall buildings (will geocode addresses in step 03)")
            return buildings

        except Exception as e:
            logger.error(f"  Error querying Building Footprints: {e}")
            return []

    def _is_duplicate(self, address: str) -> bool:
        """Check if address already exists in dataset"""
        normalized = address.lower().strip()
        return normalized in self.existing_addresses

    def find_all_gaps(self) -> pd.DataFrame:
        """
        Run all gap-finding methods and combine results.
        """
        logger.info("=" * 60)
        logger.info("Finding gaps in building dataset...")
        logger.info("=" * 60)

        all_gaps = []

        # 1. DOB tall buildings (2000+)
        all_gaps.extend(self.find_tall_buildings_from_dob(min_floors=30, start_year=2000))
        time.sleep(config.REQUEST_DELAY)

        # 2. Recent LPC landmarks
        all_gaps.extend(self.find_recent_landmarks_from_lpc(start_year=2000))

        # 3. Supertall buildings from footprints
        all_gaps.extend(self.find_supertall_buildings(min_height_ft=800))
        time.sleep(config.REQUEST_DELAY)

        # Convert to DataFrame
        if all_gaps:
            gaps_df = pd.DataFrame(all_gaps)
            logger.info(f"\n✓ Found {len(gaps_df)} potential gap buildings total")
            return gaps_df
        else:
            logger.info("\n✓ No gaps found (dataset appears complete)")
            return pd.DataFrame()


def main():
    logger.info("=" * 60)
    logger.info("Step 2: Find Missing Buildings")
    logger.info("=" * 60)

    # Load existing combined dataset
    logger.info(f"\nLoading existing dataset: data/intermediate/01_combined.csv")
    existing_df = pd.read_csv(f"{config.INTERMEDIATE_DIR}/01_combined.csv")
    logger.info(f"  Current dataset has {len(existing_df)} buildings")

    # Find gaps
    finder = GapFinder(existing_df)
    gaps_df = finder.find_all_gaps()

    if len(gaps_df) > 0:
        # Save gaps found
        gaps_output = f"{config.INTERMEDIATE_DIR}/02_gaps_found.csv"
        save_checkpoint(gaps_df, gaps_output)
        logger.info(f"\n✓ Saved {len(gaps_df)} gap buildings to: {gaps_output}")

        # Standardize gaps to match combined schema
        logger.info("\nStandardizing gap buildings to common schema...")

        # Ensure all required columns exist (fill missing with None)
        required_cols = existing_df.columns.tolist()
        for col in required_cols:
            if col not in gaps_df.columns:
                gaps_df[col] = None

        # Reorder columns to match existing
        gaps_df = gaps_df[required_cols]

        # Combine with existing dataset
        logger.info("\nMerging gaps into combined dataset...")
        combined_with_gaps = pd.concat([existing_df, gaps_df], ignore_index=True)

        # Update duplicate detection
        combined_with_gaps['address_normalized'] = combined_with_gaps['address'].str.lower().str.strip()
        address_dupes = combined_with_gaps.duplicated(subset=['address_normalized'], keep=False)
        combined_with_gaps['is_potential_duplicate'] = address_dupes

        # Summary
        logger.info(f"\n{'=' * 60}")
        logger.info("Combined Dataset with Gaps:")
        logger.info(f"{'=' * 60}")
        logger.info(f"  Original buildings: {len(existing_df)}")
        logger.info(f"  Gap buildings found: {len(gaps_df)}")
        logger.info(f"  Total buildings: {len(combined_with_gaps)}")
        logger.info(f"  Potential duplicates: {combined_with_gaps['is_potential_duplicate'].sum()}")

        # Count by source
        logger.info("\nBy source:")
        for source, count in combined_with_gaps['source'].value_counts().items():
            logger.info(f"  {source}: {count}")

        # Save combined with gaps
        output_path = f"{config.INTERMEDIATE_DIR}/02_combined_with_gaps.csv"
        save_checkpoint(combined_with_gaps, output_path)

        logger.info(f"\n✓ Step 2 complete")
        logger.info("Next: Run 03_geocode.py to get BBL/BIN for all buildings")

    else:
        logger.info("\n✓ No gaps found - proceeding with existing dataset")
        logger.info("Next: Run 03_geocode.py to get BBL/BIN for all buildings")


if __name__ == "__main__":
    main()

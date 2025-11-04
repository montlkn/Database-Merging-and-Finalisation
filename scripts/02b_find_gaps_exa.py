#!/usr/bin/env python3
"""
Step 2B: Use Exa.ai to find missing notable buildings

Uses Exa's neural search to find architecturally significant buildings
that might be missing from the database, focusing on:
- Award-winning buildings (AIA, CTBUH, etc.)
- Starchitect works
- Notable recent constructions (2000-2025)
- Significant modernist buildings (1950-1999)

Output: data/intermediate/02b_exa_suggestions.csv
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import time
from typing import List, Dict, Optional
from exa_py import Exa
from utils import save_checkpoint, logger
import config


class ExaBuildingFinder:
    """Use Exa.ai to find notable NYC buildings"""

    def __init__(self, existing_df: pd.DataFrame):
        self.exa = Exa(api_key=config.EXA_API_KEY)
        self.existing_addresses = set(existing_df['address'].str.lower().str.strip())
        self.suggestions = []

    def search_buildings(self, query: str, num_results: int = 10) -> List[Dict]:
        """
        Search for buildings using Exa's neural search.

        Returns list of URLs about notable buildings.
        """
        logger.info(f"Searching Exa: '{query}'")

        try:
            # Use Exa's neural search
            search_response = self.exa.search_and_contents(
                query,
                type="neural",
                num_results=num_results,
                text=True
            )

            time.sleep(0.5)  # Rate limiting

            logger.info(f"  Found {len(search_response.results)} results")
            return search_response.results

        except Exception as e:
            logger.error(f"  Exa search error: {e}")
            return []

    def extract_building_info(self, result) -> Optional[Dict]:
        """
        Extract building information from Exa result.

        Uses the text content and highlights to extract:
        - Address
        - Building name
        - Architect
        - Year built
        - Style
        """
        try:
            text = result.text if hasattr(result, 'text') else ""
            url = result.url if hasattr(result, 'url') else ""
            title = result.title if hasattr(result, 'title') else ""

            # For now, return the URL and text for manual review
            # In production, you'd use more sophisticated extraction
            return {
                'title': title,
                'url': url,
                'snippet': text[:500] if text else "",
                'needs_manual_review': True
            }

        except Exception as e:
            logger.error(f"  Error extracting info: {e}")
            return None

    def verify_building(self, address: str) -> Dict:
        """
        Use Exa to verify/enrich building information.

        Given an address, find architect, year, style, etc.
        """
        query = f"Architecture details for {address} New York City - architect, year built, architectural style"

        try:
            search_response = self.exa.search_and_contents(
                query,
                type="neural",
                num_results=3,
                text=True,
                use_autoprompt=True
            )

            if search_response.results:
                result = search_response.results[0]
                return {
                    'address': address,
                    'info_source': result.url,
                    'text': result.text[:1000] if hasattr(result, 'text') else "",
                    'needs_parsing': True
                }

        except Exception as e:
            logger.error(f"  Error verifying {address}: {e}")

        return None

    def find_missing_buildings(self) -> pd.DataFrame:
        """
        Run systematic searches for different types of notable buildings.
        """
        queries = [
            # Award winners
            "AIA New York Chapter award winning buildings 2015-2025",
            "CTBUH award winning buildings New York City",
            "Architectural Record Design Vanguard New York buildings",

            # Starchitects
            "Richard Meier buildings in New York City",
            "Steven Holl buildings in Manhattan",
            "Annabelle Selldorf buildings in NYC",
            "Thomas Juul-Hansen buildings New York",
            "Deborah Berke buildings New York City",

            # Recent notable
            "Notable new residential towers New York 2020-2024",
            "Supertall residential buildings Manhattan Billionaires Row",
            "Hudson Yards new buildings architecture",
            "Brooklyn supertall towers 2020-2024",

            # Modernist icons
            "Edward Durell Stone buildings in New York City",
            "Philip Johnson New York City buildings 1950-1980",
            "Paul Rudolph brutalist buildings NYC",
            "Important postmodern buildings New York 1980s",

            # Cultural/Institutional
            "New museum buildings New York City 2010-2024",
            "Notable university buildings Columbia NYU 2015-2024",
            "Performing arts centers NYC 2010-2024",
        ]

        all_results = []

        for query in queries:
            results = self.search_buildings(query, num_results=5)

            for result in results:
                info = self.extract_building_info(result)
                if info:
                    all_results.append(info)

            time.sleep(1)  # Rate limiting between queries

        # Convert to DataFrame for review
        if all_results:
            df = pd.DataFrame(all_results)
            logger.info(f"\n✓ Found {len(df)} potential buildings for review")
            return df
        else:
            logger.info("\n✓ No results from Exa search")
            return pd.DataFrame()


def verify_known_buildings():
    """
    Verify information for buildings we know are missing.
    """
    logger.info("\nVerifying known missing buildings with Exa...")

    exa = Exa(api_key=config.EXA_API_KEY)

    known_missing = [
        "5 East 62nd Street, New York",
        "15 Central Park West, New York",
        "53 East 53rd Street, New York (Citigroup Center)",
        "131 East 69th Street, New York (Rockefeller Guest House)",
        "11 Beach Street, New York",
        "277 Fifth Avenue, New York",
        "570 Broome Street, New York",
        "551 West 21st Street, New York",
    ]

    verified = []

    for address in known_missing:
        logger.info(f"\n  Looking up: {address}")

        try:
            query = f"Architecture and building details: {address} - architect, year built, architectural style"

            search_response = exa.search_and_contents(
                query,
                type="neural",
                num_results=3,
                text=True
            )

            if search_response.results:
                result = search_response.results[0]
                verified.append({
                    'address': address,
                    'source_url': result.url,
                    'source_title': result.title if hasattr(result, 'title') else "",
                    'content_snippet': result.text[:800] if hasattr(result, 'text') else ""
                })
                logger.info(f"    ✓ Found: {result.title if hasattr(result, 'title') else 'No title'}")

            time.sleep(1)  # Rate limiting

        except Exception as e:
            logger.error(f"    Error: {e}")

    return pd.DataFrame(verified)


def main():
    logger.info("=" * 60)
    logger.info("Step 2B: Find Missing Buildings with Exa.ai")
    logger.info("=" * 60)

    # Check if Exa API key is set
    if not config.EXA_API_KEY:
        logger.error("\n❌ EXA_API_KEY not set in config.py")
        logger.error("Skipping Exa search for missing buildings")
        return

    # Load existing dataset
    logger.info(f"\nLoading existing dataset...")
    combined_path = f"{config.INTERMEDIATE_DIR}/02_combined_with_gaps.csv"
    if not os.path.exists(combined_path):
        combined_path = f"{config.INTERMEDIATE_DIR}/01_combined.csv"

    existing_df = pd.read_csv(combined_path)
    logger.info(f"  Current dataset has {len(existing_df)} buildings")

    # First, verify known missing buildings
    verified_df = verify_known_buildings()

    if len(verified_df) > 0:
        output_path = f"{config.INTERMEDIATE_DIR}/02b_exa_verified.csv"
        save_checkpoint(verified_df, output_path)
        logger.info(f"\n✓ Saved {len(verified_df)} verified buildings to: {output_path}")
        logger.info("\n📋 Review this file and manually add buildings to new_additions.csv")

    # Then, search for additional buildings
    logger.info("\n" + "=" * 60)
    logger.info("Searching for additional notable buildings...")
    logger.info("=" * 60)

    finder = ExaBuildingFinder(existing_df)
    suggestions_df = finder.find_missing_buildings()

    if len(suggestions_df) > 0:
        output_path = f"{config.INTERMEDIATE_DIR}/02b_exa_suggestions.csv"
        save_checkpoint(suggestions_df, output_path)
        logger.info(f"\n✓ Saved {len(suggestions_df)} suggestions to: {output_path}")
        logger.info("\n📋 Review this file for potential additions")

    logger.info("\n" + "=" * 60)
    logger.info("Next Steps:")
    logger.info("=" * 60)
    logger.info("1. Review 02b_exa_verified.csv for known missing buildings")
    logger.info("2. Review 02b_exa_suggestions.csv for additional candidates")
    logger.info("3. Manually add verified buildings to new_additions.csv")
    logger.info("4. Re-run 01_combine_sources.py to merge")
    logger.info("5. Continue with 03_geocode.py")


if __name__ == "__main__":
    main()

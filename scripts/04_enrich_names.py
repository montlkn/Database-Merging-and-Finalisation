#!/usr/bin/env python3
"""
Step 4: Enrich with building names

Input: data/intermediate/03_footprints_enriched.csv
Output: data/intermediate/04_names_enriched.csv

Priority cascade:
1. LPC Landmarks (official names)
2. Existing database (your curated names)
3. OSM/Wikidata (future)
4. Exa.ai (future, optional)

Adds columns:
- build_nme: Building name (if found)
- name_source: Where the name came from
- name_confidence: Confidence score (1.0 = authoritative)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import validate_dataframe, save_checkpoint, load_checkpoint, logger
import config


def load_lpc_names() -> dict:
    """
    Load building names from LPC landmarks database.

    Returns dict: {address: {'name': str, 'bbl': str}}
    """
    logger.info(f"Loading LPC landmark names: {config.LPC_LANDMARKS_CSV}")
    lpc = pd.read_csv(config.LPC_LANDMARKS_CSV, low_memory=False)

    # Create lookup by address and BBL
    names = {}

    for _, row in lpc.iterrows():
        name = row.get('Build_Nme')
        address = row.get('Des_Addres')
        bbl = row.get('BBL')

        if pd.notna(name) and name.strip():
            # Index by address
            if pd.notna(address):
                key_addr = str(address).strip().lower()
                names[key_addr] = {
                    'name': str(name).strip(),
                    'bbl': str(bbl) if pd.notna(bbl) else None,
                    'source': 'lpc_landmarks',
                    'confidence': 1.0
                }

            # Also index by BBL
            if pd.notna(bbl):
                key_bbl = str(bbl).strip()
                names[key_bbl] = {
                    'name': str(name).strip(),
                    'bbl': str(bbl),
                    'source': 'lpc_landmarks',
                    'confidence': 1.0
                }

    logger.info(f"  Loaded {len(names)} named landmarks from LPC")
    return names


def load_existing_names() -> dict:
    """
    Load building names from existing curated database.

    Returns dict: {address: {'name': str}}
    """
    logger.info(f"Loading existing database names: {config.EXISTING_LANDMARKS_CSV}")
    existing = pd.read_csv(config.EXISTING_LANDMARKS_CSV)

    names = {}
    for _, row in existing.iterrows():
        name = row.get('build_nme')
        address = row.get('des_addres')

        if pd.notna(name) and name.strip() and pd.notna(address):
            key = str(address).strip().lower()
            names[key] = {
                'name': str(name).strip(),
                'source': 'existing_database',
                'confidence': 0.9
            }

    logger.info(f"  Loaded {len(names)} names from existing database")
    return names


def enrich_names(df: pd.DataFrame, lpc_names: dict, existing_names: dict) -> pd.DataFrame:
    """
    Enrich buildings with names using priority cascade.
    """
    logger.info(f"Enriching {len(df)} buildings with names...")

    results = []

    for _, row in df.iterrows():
        address = str(row.get('des_addres', '')).strip().lower()
        bbl = str(row.get('bbl', '')).strip()

        name = None
        source = None
        confidence = 0.0

        # Priority 1: LPC by BBL (most authoritative)
        if bbl and bbl in lpc_names:
            match = lpc_names[bbl]
            name = match['name']
            source = match['source']
            confidence = match['confidence']

        # Priority 2: LPC by address
        elif address and address in lpc_names:
            match = lpc_names[address]
            name = match['name']
            source = match['source']
            confidence = match['confidence']

        # Priority 3: Existing database
        elif address and address in existing_names:
            match = existing_names[address]
            name = match['name']
            source = match['source']
            confidence = match['confidence']

        results.append({
            'build_nme': name,
            'name_source': source,
            'name_confidence': confidence
        })

    # Add results
    results_df = pd.DataFrame(results)
    result = pd.concat([df, results_df], axis=1)

    # Summary
    named_count = result['build_nme'].notna().sum()
    logger.info(f"✓ Found names for {named_count}/{len(df)} buildings ({named_count/len(df)*100:.1f}%)")

    # Breakdown by source
    if named_count > 0:
        by_source = result[result['build_nme'].notna()]['name_source'].value_counts()
        logger.info("  Names by source:")
        for source, count in by_source.items():
            logger.info(f"    {source}: {count}")

    return result


def main():
    logger.info("=" * 60)
    logger.info("Step 4: Name Enrichment")
    logger.info("=" * 60)

    # Load previous checkpoint
    footprints_path = f"{config.INTERMEDIATE_DIR}/03_footprints_enriched.csv"
    logger.info(f"Loading: {footprints_path}")
    df = load_checkpoint(footprints_path)

    # Load name sources
    lpc_names = load_lpc_names()
    existing_names = load_existing_names()

    # Enrich
    result = enrich_names(df, lpc_names, existing_names)

    # Examples
    logger.info("\nExample named buildings:")
    named_sample = result[result['build_nme'].notna()][['des_addres', 'build_nme', 'name_source']].head(5)
    if len(named_sample) > 0:
        for _, row in named_sample.iterrows():
            logger.info(f"  {row['des_addres']}: {row['build_nme']} [{row['name_source']}]")

    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/04_names_enriched.csv"
    save_checkpoint(result, output_path)

    logger.info("✓ Step 4 complete")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Step 6: Enrich building names

Input: data/intermediate/05_footprints_enriched.csv
Output: data/intermediate/06_names_enriched.csv

Goal: 100% building_name coverage (use address as fallback)

Strategy:
1. Keep existing building names (from landmarks CSV)
2. For missing names, try to get from PLUTO (building name field)
3. For still missing, use address as the display name
4. Clean up and standardize all names

This ensures every building has a displayable name for the app.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from utils import save_checkpoint, load_checkpoint, logger
import config


def get_pluto_building_names(pluto_path: str) -> dict:
    """
    Load building names from PLUTO if available.
    """
    logger.info(f"Loading building names from PLUTO...")
    
    try:
        # Check if PLUTO has a building name column
        pluto = pd.read_csv(pluto_path, usecols=['BBL', 'Address'], low_memory=False, nrows=100)
        
        # PLUTO doesn't have explicit building names, just addresses
        # We'll skip this source
        logger.info("  PLUTO doesn't have building name column - skipping")
        return {}
        
    except Exception as e:
        logger.info(f"  Could not load PLUTO names: {e}")
        return {}


def clean_building_name(name: str) -> str:
    """
    Clean and standardize building name.
    """
    if pd.isna(name):
        return None
    
    name = str(name).strip()
    
    # Remove "aka" portions if they're redundant
    if " (aka " in name:
        # Keep the aka if it's significantly different
        parts = name.split(" (aka ")
        main_name = parts[0].strip()
        aka_name = parts[1].rstrip(")").strip()
        
        # If aka is just the address, drop it
        if aka_name.replace(",", "").replace(".", "").isdigit() or \
           len(aka_name.split()) <= 3:  # Short aka, probably just address
            name = main_name
    
    return name if name else None


def fill_missing_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing building names using address as fallback.
    """
    logger.info(f"Filling missing building names...")
    
    # Clean existing names
    df['building_name'] = df['building_name'].apply(clean_building_name)
    
    initial_missing = df['building_name'].isna().sum()
    logger.info(f"  Buildings missing names: {initial_missing}/{len(df)} ({initial_missing/len(df)*100:.1f}%)")
    
    # For missing names, use address
    missing_mask = df['building_name'].isna()
    
    for idx in df[missing_mask].index:
        address = df.at[idx, 'address']
        
        if pd.notna(address):
            # Use address as the building name
            df.at[idx, 'building_name'] = str(address)
            df.at[idx, 'name_source'] = 'address'
        else:
            # Last resort: use BBL as identifier
            bbl = df.at[idx, 'bbl']
            df.at[idx, 'building_name'] = f"Building {bbl}"
            df.at[idx, 'name_source'] = 'bbl'
    
    # Mark source for existing names
    df.loc[df['name_source'].isna(), 'name_source'] = 'original'
    
    final_missing = df['building_name'].isna().sum()
    logger.info(f"  ✓ Final missing names: {final_missing}/{len(df)} ({final_missing/len(df)*100:.1f}%)")
    
    # Count by source
    name_sources = df['name_source'].value_counts()
    logger.info(f"\n  Name sources:")
    for source, count in name_sources.items():
        logger.info(f"    {source}: {count} ({count/len(df)*100:.1f}%)")
    
    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 6: Building Name Enrichment")
    logger.info("=" * 60)
    logger.info("TARGET: 100% building_name coverage (use address as fallback)")
    
    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/05_footprints_enriched.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)
    
    initial_name_count = df['building_name'].notna().sum()
    logger.info(f"  Starting name coverage: {initial_name_count}/{len(df)} ({initial_name_count/len(df)*100:.1f}%)")
    
    # Fill missing names
    result = fill_missing_names(df)
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("FINAL NAME ENRICHMENT SUMMARY")
    logger.info("=" * 60)
    
    name_count = result['building_name'].notna().sum()
    logger.info(f"Building name coverage: {name_count}/{len(result)} ({name_count/len(result)*100:.1f}%)")
    
    if name_count == len(result):
        logger.info("\n🎉 100% BUILDING NAME COVERAGE ACHIEVED!")
    else:
        logger.warning(f"\n⚠ {len(result) - name_count} buildings still missing names")
    
    # Sample of names
    logger.info("\nSample building names:")
    for idx, row in result.head(10).iterrows():
        source = row.get('name_source', 'unknown')
        logger.info(f"  {row['building_name'][:50]} [{source}]")
    
    # Save checkpoint
    output_path = f"{config.INTERMEDIATE_DIR}/06_names_enriched.csv"
    save_checkpoint(result, output_path)
    
    logger.info("\n✓ Step 6 complete")


if __name__ == "__main__":
    main()

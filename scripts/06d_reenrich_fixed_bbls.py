#!/usr/bin/env python3
"""
Step 6d: Re-enrich newly fixed BBLs with PLUTO data

For buildings that had placeholder BBLs fixed in Step 6c,
re-query PLUTO to get year_built and num_floors.
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
    logger.info("Step 6d: Re-enrich Fixed BBLs with PLUTO")
    logger.info("=" * 60)

    input_path = f"{config.INTERMEDIATE_DIR}/06e_exa_enriched.csv"
    df = load_checkpoint(input_path)

    # Load PLUTO
    logger.info(f"Loading PLUTO: {config.PLUTO_CSV}")
    pluto = pd.read_csv(config.PLUTO_CSV, low_memory=False)
    logger.info(f"  PLUTO has {len(pluto):,} tax lots")

    # Convert BBL to string for matching
    df['bbl_str'] = df['bbl'].astype(str).str.replace('.0', '', regex=False)
    pluto['bbl_str'] = pluto['BBL'].astype(str)

    # Find buildings that were recently fixed (not placeholder anymore, but missing PLUTO data)
    needs_enrichment = df[
        (df['bbl'] != PLACEHOLDER_BBL) &
        (df['year_built'].isna() | df['numfloors'].isna())
    ]

    logger.info(f"\nFound {len(needs_enrichment)} buildings needing PLUTO enrichment")

    if len(needs_enrichment) == 0:
        logger.info("No buildings need re-enrichment!")
        output_path = f"{config.INTERMEDIATE_DIR}/06d_reenriched.csv"
        save_checkpoint(df, output_path)
        return

    # Join with PLUTO
    enriched = needs_enrichment.merge(
        pluto[['bbl_str', 'yearbuilt', 'numfloors']],
        left_on='bbl_str',
        right_on='bbl_str',
        how='left',
        suffixes=('', '_pluto')
    )

    updated_count = 0

    for idx in enriched.index:
        original_idx = enriched.loc[idx].name

        # Update year if missing
        if pd.isna(df.at[original_idx, 'year_built']) and pd.notna(enriched.at[idx, 'yearbuilt_pluto']):
            df.at[original_idx, 'year_built'] = enriched.at[idx, 'yearbuilt_pluto']
            df.at[original_idx, 'yearbuilt'] = enriched.at[idx, 'yearbuilt_pluto']
            logger.info(f"  Updated year for {df.at[original_idx, 'address']}: {enriched.at[idx, 'yearbuilt_pluto']}")
            updated_count += 1

        # Update floors if missing
        if pd.isna(df.at[original_idx, 'numfloors']) and pd.notna(enriched.at[idx, 'numfloors_pluto']):
            df.at[original_idx, 'numfloors'] = enriched.at[idx, 'numfloors_pluto']
            logger.info(f"  Updated floors for {df.at[original_idx, 'address']}: {enriched.at[idx, 'numfloors_pluto']}")
            updated_count += 1

    logger.info(f"\n{'='*60}")
    logger.info(f"Updated {updated_count} fields from PLUTO")
    logger.info(f"{'='*60}")

    # Drop temporary column
    df = df.drop(columns=['bbl_str'])

    output_path = f"{config.INTERMEDIATE_DIR}/06d_reenriched.csv"
    save_checkpoint(df, output_path)

    logger.info(f"\n✓ Step 6d complete")
    logger.info(f"Next: Run Step 08 (cleanup)")


if __name__ == "__main__":
    main()

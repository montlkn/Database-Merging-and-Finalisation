#!/usr/bin/env python3
"""
Step 6m: Cleanup Placeholders and Generate Completeness Report

Replace all placeholder values with NA and generate a report on data completeness.

Input: data/intermediate/06l_final_exa_pass.csv
Output: data/intermediate/06m_clean_no_placeholders.csv
        data/manual/completeness_report.txt
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
from utils import save_checkpoint, load_checkpoint, logger
import config

PLACEHOLDER_BBL = 5079660001
PLACEHOLDER_BIN = 5088547


def get_core_fields():
    """Return list of core fields that should be filled."""
    return [
        'bbl',
        'bin',
        'latitude',
        'longitude',
        'year_built',
        'numfloors',
        'height',
        'building_name',
        'address',
        'borough_name'
    ]


def cleanup_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace placeholder values with NA.
    """
    df = df.copy()
    
    # Replace placeholder BBL
    placeholder_bbl_count = (df['bbl'] == PLACEHOLDER_BBL).sum()
    df.loc[df['bbl'] == PLACEHOLDER_BBL, 'bbl'] = None
    
    # Replace placeholder BIN
    placeholder_bin_count = (df['bin'] == PLACEHOLDER_BIN).sum()
    df.loc[df['bin'] == PLACEHOLDER_BIN, 'bin'] = None
    
    logger.info(f"Replaced {placeholder_bbl_count} placeholder BBLs with NA")
    logger.info(f"Replaced {placeholder_bin_count} placeholder BINs with NA")
    
    return df


def generate_completeness_report(df: pd.DataFrame, output_path: str):
    """
    Generate a comprehensive completeness report.
    """
    core_fields = get_core_fields()
    
    # Filter to fields that exist in dataframe
    existing_fields = [f for f in core_fields if f in df.columns]
    
    report_lines = []
    report_lines.append("=" * 70)
    report_lines.append("DATA COMPLETENESS REPORT")
    report_lines.append("=" * 70)
    report_lines.append("")
    report_lines.append(f"Total buildings: {len(df)}")
    report_lines.append("")
    
    # Field-level completeness
    report_lines.append("FIELD-LEVEL COMPLETENESS:")
    report_lines.append("-" * 70)
    
    field_stats = []
    for field in existing_fields:
        if field in df.columns:
            non_null = df[field].notna().sum()
            pct = (non_null / len(df)) * 100
            field_stats.append((field, non_null, len(df), pct))
            report_lines.append(f"  {field:20s}: {non_null:6d}/{len(df):6d} ({pct:5.1f}%)")
    
    report_lines.append("")
    
    # Building-level completeness
    report_lines.append("BUILDING-LEVEL COMPLETENESS:")
    report_lines.append("-" * 70)
    
    # Count missing fields per building
    df['_missing_fields'] = df[existing_fields].isna().sum(axis=1)
    df['_total_fields'] = len(existing_fields)
    df['_completeness_pct'] = ((df['_total_fields'] - df['_missing_fields']) / df['_total_fields']) * 100
    
    # Categorize
    fully_complete = (df['_missing_fields'] == 0).sum()
    mostly_complete = ((df['_missing_fields'] >= 1) & (df['_missing_fields'] <= 2)).sum()
    partially_complete = ((df['_missing_fields'] >= 3) & (df['_missing_fields'] <= 5)).sum()
    mostly_incomplete = (df['_missing_fields'] > 5).sum()
    
    report_lines.append(f"  Fully complete (0 missing):     {fully_complete:6d} ({fully_complete/len(df)*100:5.1f}%)")
    report_lines.append(f"  Mostly complete (1-2 missing): {mostly_complete:6d} ({mostly_complete/len(df)*100:5.1f}%)")
    report_lines.append(f"  Partially complete (3-5 missing): {partially_complete:6d} ({partially_complete/len(df)*100:5.1f}%)")
    report_lines.append(f"  Mostly incomplete (>5 missing): {mostly_incomplete:6d} ({mostly_incomplete/len(df)*100:5.1f}%)")
    report_lines.append("")
    
    # Average completeness
    avg_completeness = df['_completeness_pct'].mean()
    report_lines.append(f"Average completeness: {avg_completeness:.1f}%")
    report_lines.append("")
    
    # Buildings missing specific critical fields
    report_lines.append("CRITICAL FIELD MISSING:")
    report_lines.append("-" * 70)
    
    critical_fields = ['bbl', 'building_name', 'address', 'latitude', 'longitude']
    for field in critical_fields:
        if field in df.columns:
            missing = df[field].isna().sum()
            if missing > 0:
                report_lines.append(f"  {field:20s}: {missing:6d} buildings ({missing/len(df)*100:5.1f}%)")
    
    report_lines.append("")
    
    # For new_additions specifically
    if 'source' in df.columns:
        new_additions = df[df['source'] == 'new_additions'].copy()
        if len(new_additions) > 0:
            report_lines.append("NEW_ADDITIONS SPECIFIC:")
            report_lines.append("-" * 70)
            report_lines.append(f"Total new_additions: {len(new_additions)}")
            
            na_complete = (new_additions['_missing_fields'] == 0).sum()
            report_lines.append(f"Fully complete: {na_complete:6d} ({na_complete/len(new_additions)*100:5.1f}%)")
            
            na_avg = new_additions['_completeness_pct'].mean()
            report_lines.append(f"Average completeness: {na_avg:.1f}%")
            report_lines.append("")
    
    # Remove helper columns
    df = df.drop(columns=['_missing_fields', '_total_fields', '_completeness_pct'], errors='ignore')
    
    # Write report
    with open(output_path, 'w') as f:
        f.write('\n'.join(report_lines))
    
    logger.info(f"\nCompleteness report saved to: {output_path}")
    
    return df


def main():
    logger.info("=" * 60)
    logger.info("Step 6m: Cleanup Placeholders and Generate Completeness Report")
    logger.info("=" * 60)
    
    # Load current state
    input_path = f"{config.INTERMEDIATE_DIR}/06l_final_exa_pass.csv"
    logger.info(f"\nLoading: {input_path}")
    df = load_checkpoint(input_path)
    
    logger.info(f"Starting with {len(df)} buildings")
    
    # Cleanup placeholders
    logger.info("\nCleaning up placeholders...")
    df = cleanup_placeholders(df)
    
    # Generate completeness report
    logger.info("\nGenerating completeness report...")
    report_path = "data/manual/completeness_report.txt"
    df = generate_completeness_report(df, report_path)
    
    # Save cleaned data
    output_path = f"{config.INTERMEDIATE_DIR}/06m_clean_no_placeholders.csv"
    save_checkpoint(df, output_path)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"✓ Step 6m complete")
    logger.info(f"✓ Cleaned data saved to: {output_path}")
    logger.info(f"✓ Completeness report: {report_path}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()


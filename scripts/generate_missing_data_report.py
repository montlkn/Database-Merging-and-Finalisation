#!/usr/bin/env python3
"""
Generate comprehensive missing data report for 06q dataset.
"""

import pandas as pd
import numpy as np

df = pd.read_csv('data/intermediate/06q_final_clean.csv', low_memory=False)

print('=' * 80)
print('COMPREHENSIVE MISSING DATA REPORT - 06q_final_clean.csv')
print('=' * 80)
print(f'\nTotal buildings: {len(df):,}\n')

# Identify parks/public spaces
parks_keywords = ['park', 'pier', 'plaza', 'island', 'beach', 'garden', 'playground', 'boardwalk']

def is_park(row):
    return any(kw in str(row['building_name']).lower() or kw in str(row['address']).lower()
               for kw in parks_keywords)

df['is_park'] = df.apply(is_park, axis=1)
df['bbl_numeric'] = pd.to_numeric(df['bbl'], errors='coerce')

# Find all buildings missing ANY data
missing_any = df[
    df['bbl_numeric'].isna() |
    df['latitude'].isna() |
    df['numfloors'].isna() |
    df['height_roof'].isna()
]

print('=' * 80)
print(f'BUILDINGS/PARKS MISSING ANY DATA: {len(missing_any)}')
print('=' * 80)

# Group by what's missing
categories = {
    'BBL only': [],
    'Coordinates only': [],
    'Floors only': [],
    'Height only': [],
    'BBL + Coords': [],
    'BBL + Floors': [],
    'BBL + Height': [],
    'Coords + Floors': [],
    'Coords + Height': [],
    'Floors + Height': [],
    'Multiple (3+)': []
}

for idx, row in missing_any.iterrows():
    missing = []
    if pd.isna(row['bbl_numeric']):
        missing.append('BBL')
    if pd.isna(row['latitude']):
        missing.append('Coords')
    if pd.isna(row['numfloors']):
        missing.append('Floors')
    if pd.isna(row['height_roof']):
        missing.append('Height')

    missing_count = len(missing)
    missing_str = ' + '.join(missing)

    type_str = 'PARK' if row['is_park'] else 'BUILDING'

    entry = {
        'name': row['building_name'],
        'address': row['address'],
        'type': type_str,
        'missing': missing,
        'source': row.get('source', 'unknown'),
        'bbl': row.get('bbl', 'N/A'),
        'latitude': row.get('latitude', 'N/A'),
        'numfloors': row.get('numfloors', 'N/A')
    }

    if missing_count >= 3:
        categories['Multiple (3+)'].append(entry)
    elif missing_count == 1:
        if 'BBL' in missing:
            categories['BBL only'].append(entry)
        elif 'Coords' in missing:
            categories['Coordinates only'].append(entry)
        elif 'Floors' in missing:
            categories['Floors only'].append(entry)
        elif 'Height' in missing:
            categories['Height only'].append(entry)
    elif missing_count == 2:
        categories[missing_str].append(entry)

# Print by category
for category, entries in categories.items():
    if len(entries) > 0:
        print(f'\n{"="*80}')
        print(f'{category.upper()}: {len(entries)}')
        print(f'{"="*80}')

        for i, entry in enumerate(entries, 1):
            print(f'\n{i}. [{entry["type"]}] {entry["name"]}')
            print(f'   Address: {entry["address"]}')
            print(f'   Missing: {" + ".join(entry["missing"])}')
            print(f'   Source: {entry["source"]}')

            if entry['type'] == 'BUILDING':
                bbl_val = entry['bbl']
                if pd.notna(bbl_val) and str(bbl_val) != 'N/A' and str(bbl_val) != 'nan':
                    print(f'   BBL: {bbl_val}')

                lat_val = entry['latitude']
                if pd.notna(lat_val) and str(lat_val) != 'N/A' and str(lat_val) != 'nan':
                    print(f'   Coords: ({lat_val}, ...)')

                floors_val = entry['numfloors']
                if pd.notna(floors_val) and str(floors_val) != 'N/A' and str(floors_val) != 'nan':
                    print(f'   Floors: {floors_val}')

# Summary statistics
print(f'\n{"="*80}')
print('SUMMARY BY TYPE')
print(f'{"="*80}')

parks_missing = missing_any[missing_any['is_park']]
buildings_missing = missing_any[~missing_any['is_park']]

print(f'\nParks/Public Spaces with missing data: {len(parks_missing)}')
print(f'Real Buildings with missing data: {len(buildings_missing)}')

# Breakdown by what's missing
print(f'\n{"="*80}')
print('MISSING DATA BREAKDOWN')
print(f'{"="*80}')

missing_bbl = df['bbl_numeric'].isna()
missing_coords = df['latitude'].isna()
missing_floors = df['numfloors'].isna()
missing_height = df['height_roof'].isna()

print(f'\nMissing BBL:')
print(f'  Parks: {missing_bbl[df["is_park"]].sum()}')
print(f'  Buildings: {missing_bbl[~df["is_park"]].sum()}')
print(f'  Total: {missing_bbl.sum()}')

print(f'\nMissing Coordinates:')
print(f'  Parks: {missing_coords[df["is_park"]].sum()}')
print(f'  Buildings: {missing_coords[~df["is_park"]].sum()}')
print(f'  Total: {missing_coords.sum()}')

print(f'\nMissing Floors:')
print(f'  Parks: {missing_floors[df["is_park"]].sum()}')
print(f'  Buildings: {missing_floors[~df["is_park"]].sum()}')
print(f'  Total: {missing_floors.sum()}')

print(f'\nMissing Height:')
print(f'  Parks: {missing_height[df["is_park"]].sum()}')
print(f'  Buildings: {missing_height[~df["is_park"]].sum()}')
print(f'  Total: {missing_height.sum()}')

print(f'\n{"="*80}')
print('END OF REPORT')
print(f'{"="*80}')

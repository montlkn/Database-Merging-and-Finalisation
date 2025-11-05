import csv
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = ROOT / 'bbl_bin_research_helper.csv'
NEW_ADDITIONS_PATH = ROOT / 'data/raw/new_additions.csv'

if not INPUT_PATH.exists() or not NEW_ADDITIONS_PATH.exists():
    raise SystemExit('Required files missing')

borough_terms = {
    'MANHATTAN': ['MANHATTAN', 'MIDTOWN', 'UPPER', 'LOWER EAST', 'CHELSEA', 'SOHO', 'HARLEM', 'TRIBECA', 'INWOOD'],
    'BROOKLYN': ['BROOKLYN', 'WILLIAMSBURG', 'DUMBO', 'BUSHWICK', 'PROSPECT', 'PARK SLOPE', 'DOWNTOWN BROOKLYN'],
    'QUEENS': ['QUEENS', 'ASTORIA', 'LONG ISLAND CITY', 'LIC', 'JACKSON HEIGHTS', 'FLUSHING'],
    'BRONX': ['BRONX'],
    'STATEN ISLAND': ['STATEN ISLAND', 'ST. GEORGE']
}

coord_map = {}
with open(NEW_ADDITIONS_PATH, newline='') as f:
    reader = csv.reader(f)
    headers = next(reader)  # discard headers
    for row in reader:
        if len(row) < 2:
            continue
        address = row[1].strip()
        if not address:
            continue
        lat = lon = None
        borough_guess = None
        for value in row:
            if value and value.startswith('POINT('):
                try:
                    inside = value.split('(')[1].split(')')[0]
                    lon_str, lat_str = inside.split()
                    lon = float(lon_str)
                    lat = float(lat_str)
                except Exception:
                    pass
            elif value:
                upper_val = value.strip().upper()
                for borough, terms in borough_terms.items():
                    if any(term in upper_val for term in terms):
                        borough_guess = borough
                        break
                if borough_guess:
                    # Don't break outer loop; still need POINT
                    pass
        if lat is not None and lon is not None:
            coord_map[address.upper()] = (lat, lon, borough_guess)

df = pd.read_csv(INPUT_PATH)

mask_missing_coords = (
    df['geocoded_lat'].isna()
    | df['geocoded_lng'].isna()
    | (df['geocoded_lat'].astype(str).str.strip() == '')
    | (df['geocoded_lng'].astype(str).str.strip() == '')
)

filled = 0
for idx, row in df[mask_missing_coords].iterrows():
    address = (row.get('address') or '').strip().upper()
    if not address:
        continue
    match = coord_map.get(address)
    if not match:
        continue
    lat, lon, borough_guess = match
    df.at[idx, 'geocoded_lat'] = lat
    df.at[idx, 'geocoded_lng'] = lon
    if (pd.isna(row.get('borough_col')) or str(row.get('borough_col')).strip() == '') and borough_guess:
        df.at[idx, 'borough_col'] = borough_guess[:2]
    filled += 1

if filled:
    df.to_csv(INPUT_PATH, index=False)

print(f'Filled {filled} rows from new_additions lookup')

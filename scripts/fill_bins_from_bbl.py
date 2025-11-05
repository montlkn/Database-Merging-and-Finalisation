from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / 'data/intermediate/output_with_bbl_bin.csv'
FOOTPRINTS_PATH = ROOT / 'data/raw/BUILDING_20251104.csv'

if not OUTPUT_PATH.exists() or not FOOTPRINTS_PATH.exists():
    raise SystemExit('Required files missing')

df = pd.read_csv(OUTPUT_PATH)
missing_bin_mask = df['bin_final'].isna() | df['bin_final'].astype(str).str.strip().eq('')
if not missing_bin_mask.any():
    print('No missing BIN entries; nothing to fill.')
    raise SystemExit

foot = pd.read_csv(FOOTPRINTS_PATH, usecols=['BASE_BBL', 'BIN'])
foot = foot.dropna(subset=['BASE_BBL', 'BIN'])
foot['BASE_BBL'] = foot['BASE_BBL'].astype(float).astype('Int64').astype(str)
foot['BIN'] = foot['BIN'].astype(float).astype('Int64').astype(str)
lookup = foot.drop_duplicates('BASE_BBL').set_index('BASE_BBL')['BIN']

def normalize_bbl(val):
    if pd.isna(val):
        return None
    try:
        return str(int(float(val)))
    except Exception:
        text = str(val).strip()
        return text if text else None

fill_mask = missing_bin_mask & df['bbl_final'].notna()
normalized_bbl = df.loc[fill_mask, 'bbl_final'].apply(normalize_bbl)
filled_bins = normalized_bbl.map(lookup)
filled_count = filled_bins.notna().sum()

if filled_count:
    df.loc[fill_mask, 'bin_final'] = df.loc[fill_mask, 'bin_final'].where(df.loc[fill_mask, 'bin_final'].notna(), filled_bins)
    source_mask = df['bin_source'].isna() & fill_mask & filled_bins.notna()
    df.loc[source_mask, 'bin_source'] = 'BuildingFootprintsLookup'
    df.to_csv(OUTPUT_PATH, index=False)
    print(f'Filled {filled_count} BIN values using BASE_BBL matching')
else:
    print('No BINs found via BASE_BBL lookup')

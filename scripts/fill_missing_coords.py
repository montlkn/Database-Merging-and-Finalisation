import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bbl_bin_batch import clean_single_line_address, extract_borough

INPUT_PATH = ROOT / 'bbl_bin_research_helper.csv'
SUBSCRIPTION_KEY = '08cf69e9f1e7479aa4d963368b9293a9'
API_URL = 'https://api.nyc.gov/geo/geoclient/v2/search.json'
SLEEP = 0.05

if not INPUT_PATH.exists():
    raise SystemExit(f"Input file not found: {INPUT_PATH}")

df = pd.read_csv(INPUT_PATH)

mask_missing_coords = (
    df['geocoded_lat'].isna()
    | df['geocoded_lng'].isna()
    | (df['geocoded_lat'].astype(str).str.strip() == '')
    | (df['geocoded_lng'].astype(str).str.strip() == '')
)

session = requests.Session()
session.headers.update({'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY})

success = 0
failures = []

for idx, row in df[mask_missing_coords].iterrows():
    borough = extract_borough(row)
    single = clean_single_line_address(row.get('address'), borough)
    if not single:
        failures.append((idx, 'no_address'))
        continue
    try:
        resp = session.get(API_URL, params={'input': single}, timeout=10)
    except Exception as exc:
        failures.append((idx, f'request_error:{exc}'))
        continue
    if resp.status_code != 200:
        failures.append((idx, f'status_{resp.status_code}'))
        continue
    data = resp.json()
    results = data.get('results') or data.get('search', {}).get('results') or []
    if not results:
        failures.append((idx, 'no_results'))
        continue
    props = results[0].get('response', results[0])

    lat = props.get('latitude') or props.get('latitudeInternal')
    lon = props.get('longitude') or props.get('longitudeInternal')
    if lat:
        df.at[idx, 'geocoded_lat'] = float(lat)
    if lon:
        df.at[idx, 'geocoded_lng'] = float(lon)

    bbl = props.get('bbl')
    if bbl and 'bbl' in df.columns:
        if pd.isna(df.at[idx, 'bbl']) or str(df.at[idx, 'bbl']).strip() == '':
            df.at[idx, 'bbl'] = str(bbl)

    bin_ = props.get('buildingIdentificationNumber')
    if bin_ and 'bin' in df.columns:
        if pd.isna(df.at[idx, 'bin']) or str(df.at[idx, 'bin']).strip() == '':
            df.at[idx, 'bin'] = str(bin_)

    if pd.isna(row.get('borough_col')) or str(row.get('borough_col')).strip() == '':
        borough_text = props.get('boroughName') or props.get('borough')
        if borough_text:
            df.at[idx, 'borough_col'] = borough_text.split()[0][:2].upper()

    success += 1
    time.sleep(SLEEP)

df.to_csv(INPUT_PATH, index=False)

print(f"Filled {success} rows with coordinates")
if failures:
    print('Failures:')
    for idx, reason in failures:
        print(idx, reason)

# Exa Enrichment Instructions

This script will use Exa AI to find missing data for 112 buildings.

## What It Will Find

For each building with missing data, Exa will search NYC property databases to find:

- **BBL/BIN** (for 35 buildings with placeholder BBL 5079660001)
- **Coordinates** (lat/long for 61 buildings)
- **Year Built** (for 62 buildings)
- **Floor Count** (for 41 buildings)
- **Canonical Building Names** (for buildings where name = address)

## Setup

### Option 1: Use Alternate Exa API Key (Recommended)

If you have a different Exa account with credits:

```bash
export EXA_API_KEY_ALTERNATE="your-alternate-exa-api-key-here"
source venv/bin/activate
python scripts/06e_exa_comprehensive_enrichment.py
```

### Option 2: Update config.py

Edit `/Users/lucienmount/coding/nyc-buildings-pipeline/config.py` and add billing to the existing key:

```python
EXA_API_KEY = "29dfcf82-1653-416a-8dd7-9cbd9a94279b"  # Add billing at exa.ai
```

Then run:

```bash
source venv/bin/activate
python scripts/06e_exa_comprehensive_enrichment.py
```

## Expected Cost

- **112 buildings** to process
- **~5 searches per building** = 560 searches
- At Exa's current pricing, this should cost approximately **$5-10**

## Runtime

- With 0.2s rate limiting: ~2-3 minutes
- Script will show progress for each building

## What Happens After

The script will:
1. Read `data/manual/comprehensive_missing_data.csv`
2. Search Exa for each building's missing information
3. Update `data/intermediate/06e_exa_enriched.csv` with found data
4. Show summary of what was found

Then you'll need to run:

```bash
# Re-enrich newly found BBLs with PLUTO
python scripts/06d_reenrich_fixed_bbls.py

# Apply 4K limit and cleanup
python scripts/08_cleanup.py
```

## Domains Searched

The script focuses on authoritative NYC sources:
- zola.nyc.gov (NYC property info)
- a810-bisweb.nyc.gov (DOB Building info)
- propertyshark.com
- streeteasy.com
- emporis.com (building database)
- skyscraperpage.com (tall buildings)
- wikipedia.org
- nycitymap.com

## Notes

- Some buildings (parks, vague addresses) may not be found even with Exa
- That's OK - they'll score low in ML and won't make the top 4,000 cut
- You can always manually fill remaining gaps in Step 11

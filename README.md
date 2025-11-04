# NYC Buildings Database Pipeline

Clean, modular pipeline to enrich and merge NYC building data for the architecture app.

## What It Does

1. Geocodes addresses → BBL/BIN via NYC Geosupport
2. Enriches with PLUTO data (floors, year built, etc.)
3. Joins NYC Building Footprints (proper polygons)
4. Fetches building names (LPC/OSM/Wikidata/Exa)
5. Applies ML landmark scoring
6. Finds missing buildings (DOB/AIA/CTBUH feeds)
7. Merges + deduplicates
8. Aesthetic categorization (9 categories)
9. Exports ranked top 100/500

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set up config
cp config.py.example config.py
# Edit config.py with your API keys

# Run pipeline step by step
python scripts/01_geocode.py
python scripts/02_enrich_pluto.py
# ... etc

# Or run all
./run_pipeline.sh
```

## Input

- `data/raw/new_additions.csv` - Your 297 new buildings
- `data/raw/walk_optimized_landmarks.csv` - Existing 4K database

## Output

- `data/output/buildings_final.csv` - Merged, scored, categorized
- `data/output/top_100.csv` - Top 100 for scanning
- `data/output/top_500.csv` - Top 500 for scanning

## Structure

- `scripts/` - Pipeline steps (run in order)
- `utils.py` - Shared helpers
- `config.py` - API keys, paths
- `data/` - Raw, intermediate, output (gitignored)

## Principles

- One script = one job
- Fail fast with clear errors
- No overengineering
- CSV in/out (simple)

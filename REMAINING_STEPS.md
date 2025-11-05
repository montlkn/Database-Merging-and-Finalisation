# NYC Buildings Pipeline - Remaining Steps (07-10)

**Current Status:** Steps 01-06 complete and committed
**Location:** `/Users/lucienmount/coding/nyc-buildings-pipeline`
**GitHub:** https://github.com/montlkn/Database-Merging-and-Finalisation

---

## Current Data State

**File:** `data/intermediate/06_names_enriched.csv`
**Total Buildings:** 3,599

### Completeness:
-  BBL: 100% (3,599/3,599)
-  BIN: 99.9% (3,592/3,599)
-  Building names: 100% (3,599/3,599)
-  Height: 100% (3,599/3,599)
-  Geometry: 99.8% (3,591/3,599)
-  Architect: 99.2% (3,567/3,599)
-  Year built: 98.9% (3,556/3,599)
-  Style: 99.2% (3,567/3,599)
-  Number of floors: 98.1% (3,526/3,599)
-  Landmark flag: 92.8% (3,336/3,599)

### Known Issues to Fix:
- [ ] 4 duplicate rows (3,599 vs expected 3,595) - need deduplication in Step 08
- [ ] `borough_name` column is empty (0% coverage) - fix in Step 08
- [ ] Some columns duplicated (e.g., `BBL` vs `bbl`) - consolidate in Step 08

---

## Quick Start (When Cloning Repo)

```bash
# Clone repo
git clone https://github.com/montlkn/Database-Merging-and-Finalisation.git
cd Database-Merging-and-Finalisation

# Setup environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Copy environment config if needed
# cp config.py.example config.py

# Continue from Step 07
python scripts/07_calculate_scores.py
python scripts/08_cleanup.py
python scripts/09_categorize.py
python scripts/10_export.py
```

---

## Step 07: Calculate Final Aesthetic/Importance Score

**Goal:** 100% score coverage (0-100 scale for all buildings)

### Scoring Algorithm

**Component Weights:**
1. **Landmark Status (30 points)** - Official LPC designation
2. **Building Height (25 points)** - Architectural prominence (logarithmic scale)
3. **Architect Significance (20 points)** - Famous architects
4. **Age/Historical Value (15 points)** - Pre-1900 buildings score highest
5. **Style Rarity (10 points)** - Unique architectural styles

### Famous Architects List

Add to scoring script (or extend as needed):
- Frank Lloyd Wright (100/100)
- I. M. Pei, Le Corbusier, Mies van der Rohe (95/100)
- Frank Gehry, Zaha Hadid, Philip Johnson (90/100)
- Norman Foster, Renzo Piano (85/100)
- SOM, Kohn Pedersen Fox (80/100)
- And more...

### Run Step 07

See `scripts/07_calculate_scores.py` (create from template in this doc)

```bash
cd /Users/lucienmount/coding/nyc-buildings-pipeline
source venv/bin/activate
python scripts/07_calculate_scores.py
```

**Expected Output:**
- Score coverage: 100% (3,599/3,599)
- Score range: ~5-95 points
- Mean score: ~30-40 points
- Top 10 buildings list with scores

**Output File:** `data/intermediate/07_scored.csv`

---

## Step 08: Final Data Cleanup

**Goal:** Clean, deduplicated, consistent dataset (3,595 buildings)

### Tasks

1. **Remove duplicate rows** (3,599 ’ 3,595)
   - Keep building with highest score for duplicate BBLs

2. **Fix borough_name column**
   - Extract from BBL first digit
   - Map: 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island

3. **Consolidate duplicate columns**
   - Drop `BBL` (keep `bbl`)
   - Merge `year_built` and `yearbuilt`
   - Rename columns for consistency

4. **Final column list** (standardized names)

### Run Step 08

```bash
python scripts/08_cleanup.py
```

**Expected Output:**
- Final rows: 3,595
- borough_name: 100% coverage
- No duplicate BBLs
- Standardized column names

**Output File:** `data/intermediate/08_clean.csv`

---

## Step 09: Aesthetic Categorization

**Goal:** Classify ALL buildings into 9 aesthetic categories

### Categories

1. **Art Deco** - 1920s-1940s ornamental style (Chrysler Building)
2. **Brutalist** - Raw concrete, geometric (NY Telephone Building)
3. **Classical** - Greek/Roman inspired (Grand Central)
4. **Gothic** - Pointed arches, ornate (St. Patrick's Cathedral)
5. **Modernist/International** - Glass boxes, minimalist (Seagram Building)
6. **Postmodern** - Eclectic, colorful (550 Madison Ave)
7. **Contemporary** - 2000+, cutting-edge (One Vanderbilt)
8. **Beaux-Arts** - Ornate French academic (NY Public Library)
9. **Other** - Catch-all for uncategorized

### Categorization Logic

Based on `style` column and `year_built`:
- Match keywords in style string
- Use year as secondary classifier
- Default to "Other" if no match

### Run Step 09

```bash
python scripts/09_categorize.py
```

**Expected Output:**
- Category coverage: 100% (3,595/3,595)
- Distribution across 9 categories
- Largest category: likely Modernist/International (~30%)

**Output File:** `data/intermediate/09_categorized.csv`

---

## Step 10: Export Final Datasets

**Goal:** Generate app-ready exports in multiple formats

### Outputs

1. **`data/final/top_100.csv`** - Top 100 buildings by score
2. **`data/final/top_500.csv`** - Top 500 buildings by score
3. **`data/final/full_dataset.csv`** - All 3,595 buildings
4. **`data/final/full_dataset.geojson`** - GeoJSON for mapping
5. **`data/final/summary_stats.json`** - Summary statistics

### Export Columns (for app)

**Minimal columns for top_100/top_500:**
- building_name
- address
- borough_name
- final_score
- height
- year_built
- architect
- style
- aesthetic_category
- geocoded_lat, geocoded_lng
- bbl, bin

### Run Step 10

```bash
python scripts/10_export.py
```

**Expected Output:**
- 5 files in `data/final/` directory
- GeoJSON with 3,595 point features
- Summary stats JSON with score distributions

---

## Final Verification Checklist

Run this validation script to confirm pipeline completion:

```bash
python << 'VALIDATION_EOF'
import pandas as pd
import os

print("=" * 60)
print("FINAL VALIDATION")
print("=" * 60)

# Check files exist
files = [
    "data/final/top_100.csv",
    "data/final/top_500.csv",
    "data/final/full_dataset.csv",
    "data/final/full_dataset.geojson",
    "data/final/summary_stats.json"
]

print("\nFile Check:")
for f in files:
    exists = os.path.exists(f)
    status = "" if exists else "L"
    print(f"{status} {f}")

# Load full dataset
df = pd.read_csv("data/final/full_dataset.csv")

print(f"\nTotal buildings: {len(df)}")

# Check completeness
checks = {
    'bbl': 'BBL',
    'building_name': 'Building name',
    'height': 'Height',
    'final_score': 'Final score',
    'aesthetic_category': 'Category',
    'borough_name': 'Borough'
}

all_pass = True
for col, label in checks.items():
    if col in df.columns:
        coverage = df[col].notna().sum() / len(df) * 100
        status = "" if coverage == 100 else "L"
        print(f"{status} {label}: {coverage:.1f}%")
        if coverage < 100:
            all_pass = False
    else:
        print(f"L {label}: Column missing!")
        all_pass = False

# Score distribution
if 'final_score' in df.columns:
    print(f"\nScore Distribution:")
    print(f"  Mean:   {df['final_score'].mean():.1f}")
    print(f"  Median: {df['final_score'].median():.1f}")
    print(f"  Range:  {df['final_score'].min():.1f} - {df['final_score'].max():.1f}")

# Top 5
if 'final_score' in df.columns and 'building_name' in df.columns:
    print(f"\nTop 5 Buildings:")
    for idx, row in df.nlargest(5, 'final_score').iterrows():
        name = row['building_name'][:50]
        score = row['final_score']
        print(f"  {score:5.1f} - {name}")

# Borough distribution
if 'borough_name' in df.columns:
    print(f"\nBy Borough:")
    for borough, count in df['borough_name'].value_counts().items():
        pct = count / len(df) * 100
        print(f"  {borough:20s}: {count:4d} ({pct:5.1f}%)")

if all_pass:
    print("\n" + "=" * 60)
    print("<‰ ALL CHECKS PASSED - PIPELINE COMPLETE!")
    print("=" * 60)
else:
    print("\n   Some checks failed - review data")

VALIDATION_EOF
```

**Expected Results:**
-  All 5 files exist
-  3,595 total buildings
-  100% coverage on all key fields
-  Score range: 5-95
-  Top building score: 85-95 points

---

## Troubleshooting

### Issue: Missing intermediate files

**Solution:** Re-run from last successful step. Check `data/intermediate/` directory.

### Issue: Import errors

**Solution:**
```bash
pip install pandas geopandas shapely requests
```

### Issue: Different row count

**Solution:** Check for duplicates in Step 08. Should be exactly 3,595 after cleanup.

### Issue: Empty scores

**Solution:** Verify all component score functions are working. Check for missing height/year data.

---

## Next Steps After Pipeline

Once pipeline is complete:

1. **Review top_100.csv** - Validate top buildings make sense
2. **Test GeoJSON** - Load in QGIS/Mapbox to visualize
3. **Integrate with app** - Import CSVs into your architecture app
4. **Add images** - Fetch building photos for top buildings
5. **User testing** - Get feedback on building selections

---

## Script Templates

All script templates for Steps 07-10 are included in the sections above. Copy the Python code blocks into the respective script files:

- `scripts/07_calculate_scores.py`
- `scripts/08_cleanup.py`
- `scripts/09_categorize.py`
- `scripts/10_export.py`

Each script follows the same pattern:
1. Load from previous step
2. Process/transform data
3. Validate 100% coverage
4. Save checkpoint
5. Log results

---

## Contact / Issues

**GitHub Repo:** https://github.com/montlkn/Database-Merging-and-Finalisation

If you encounter issues:
1. Check console logs for error messages
2. Verify intermediate files exist
3. Review data types (BBL/BIN should be strings)
4. File GitHub issue with error details

---

**Last Updated:** 2025-11-05
**Pipeline Status:** Steps 01-06 complete  | Steps 07-10 pending ó
**Ready for:** Score calculation, cleanup, categorization, and export

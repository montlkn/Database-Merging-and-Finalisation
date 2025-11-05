# Pipeline Execution Order

## Correct Flow

### Phase 1: Build Complete Dataset (Before Enrichment)
1. **01_combine_sources.py** - Merge new_additions + existing_landmarks
2. **02_find_gaps.py** - Identify missing buildings (DOB, AIA, CTBUH, etc.)
3. *(Result: Complete raw dataset of ~4,000-4,500 buildings)*

### Phase 2: Enrich Complete Dataset (One Time)
4. **03_geocode.py** - BBL/BIN via Geoclient API
5. **04_enrich_pluto.py** - NumFloors, YearBuilt from PLUTO
6. **05_enrich_footprints.py** - Building polygons
7. **06_enrich_names.py** - Official names (LPC → OSM → Exa)
8. **Standardize CSV to include all information needed for app**
9. *(Result: Fully enriched dataset)*

### Phase 3: Score & Rank
9. **07_ml_scoring.py** - Apply landmark pruning ML model → final_score
10. **08_dedupe_merge.py** - Resolve duplicates, assign canonical IDs
11. **09_aesthetic.py** - 9 aesthetic categories on final dataset
12. **10_export.py** - Generate top 100/500 rankings

## Why This Order?

**Problem with old approach:**
- ❌ Enriching new_additions separately from existing landmarks
- ❌ Would need to enrich twice (inefficient, inconsistent)
- ❌ Duplicate detection harder after enrichment

**Correct approach:**
- ✅ Combine ALL sources first (raw data)
- ✅ Find gaps and add to combined dataset
- ✅ Enrich ONCE on complete dataset
- ✅ Score ONCE on enriched dataset
- ✅ Dedupe/merge at end with full context

## Run Order

```bash
# Phase 1: Build dataset
python scripts/01_combine_sources.py
python scripts/02_find_gaps.py

# Phase 2: Enrich
python scripts/03_geocode.py
python scripts/04_enrich_pluto.py
python scripts/05_enrich_footprints.py
python scripts/06_enrich_names.py

# Phase 3: Score & export
python scripts/07_ml_scoring.py
python scripts/08_dedupe_merge.py
python scripts/09_aesthetic.py
python scripts/10_export.py
```

## Data Flow

```
new_additions.csv (297)        ─┐
                                ├─→ 01_combined.csv (3,914)
walk_optimized_landmarks (3,617)─┘

                    ↓

02_find_gaps → found_gaps.csv (100-200)

                    ↓

01_combined + gaps → 02_complete_raw.csv (~4,000-4,500)

                    ↓

03-06: Enrichment → 06_fully_enriched.csv

                    ↓

07-10: Scoring → buildings_final.csv
                → top_100.csv
                → top_500.csv
```

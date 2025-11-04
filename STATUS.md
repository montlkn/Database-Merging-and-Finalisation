# NYC Buildings Pipeline - Status

## ✅ Completed (Scripts 01-04)

### Data Sources Ready
- ✅ `new_additions.csv` - 297 new buildings (1952-2023)
- ✅ `walk_optimized_landmarks.csv` - 3,617 existing buildings
- ✅ `pluto.csv` - 857K tax lots (292MB)
- ✅ `lpc_landmarks.csv` - 37K official landmarks (27MB)
- ✅ `points_of_interest.csv` - 20K POIs (4.2MB)

### Scripts Built
1. ✅ **01_geocode.py** - NYC Geoclient API integration (with fallback mode)
2. ✅ **02_enrich_pluto.py** - BBL join for floors, year built, building class
3. ✅ **03_enrich_footprints.py** - Building polygons via Socrata API
4. ✅ **04_enrich_names.py** - Name enrichment (LPC → existing DB → Exa)

## 🚧 TODO (Scripts 05-09)

### 5. ML Scoring (Landmark Pruning)
Apply your original ML model that generated `final_score` for walk optimization.

**Needs:**
- Location of your ML scoring script
- Feature requirements (what fields does it need?)
- Model weights/config file

### 6. Find Missing Buildings (Gap Analysis)
Programmatic search for missing iconic buildings (2000-2025).

**Sources to ingest:**
- DOB permits (BIS/DOB NOW API)
- AIA NY awards
- CTBUH tall buildings
- Additional LPC designations

### 7. Merge & Dedupe
Combine 297 new + gaps + 3,617 existing = ~4K total.

**Logic:**
- Cluster by: same BBL OR centroid <20m + year diff ≤2
- Resolve by data completeness + source priority
- Assign canonical_id

### 8. Aesthetic Categorization
Run `/Users/lucienmount/coding/aesthetic-scoring` on final merged dataset.

**Generates:**
- 9 aesthetic category scores
- primary_aesthetic + secondary_aesthetic

### 9. Export Rankings
Generate top 100/500 CSVs for Phase 2 scanning.

**Output:**
- `top_100.csv` - Highest scoring buildings
- `top_500.csv` - For Phase 2 beta
- Provenance columns + QA summary

## 🔑 Next Steps

1. **Get Geoclient API Key** (if you want BBL/BIN from step 01)
   - https://developer.cityofnewyork.us/
   - Add to `config.py` as `NYC_GEOCLIENT_SUBSCRIPTION_KEY`

2. **Run Script 01** to test geocoding:
   ```bash
   python scripts/01_geocode.py
   ```

3. **Provide ML Scoring Script** for step 05

4. **Then continue with remaining scripts**

## 📁 Current Repo Structure

```
nyc-buildings-pipeline/
├── README.md
├── STATUS.md (this file)
├── config.py (configured with Socrata token)
├── utils.py
├── requirements.txt
├── data/
│   ├── raw/ (5 data sources, 323MB total)
│   ├── intermediate/ (checkpoints)
│   └── output/ (final results)
└── scripts/
    ├── 01_geocode.py ✅
    ├── 02_enrich_pluto.py ✅
    ├── 03_enrich_footprints.py ✅
    ├── 04_enrich_names.py ✅
    ├── 05_ml_scoring.py (TODO)
    ├── 06_find_gaps.py (TODO)
    ├── 07_merge.py (TODO)
    ├── 08_aesthetic.py (TODO)
    └── 09_export.py (TODO)
```

## 🎯 Timeline

- **Scripts 01-04**: ✅ Done
- **Scripts 05-09**: ~1-2 days remaining
- **Testing & QA**: ~0.5 day
- **Total**: Ready for Phase 2 in ~2-3 days

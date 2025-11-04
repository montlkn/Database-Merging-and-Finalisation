# Field Mapping & Verification

## Current Status (after Step 01)

### ✅ Fields We Have
```
address              - ✓ From both sources
building_name        - ⚠ Mostly empty (will enrich in step 06)
architect            - ✓ From both sources
style                - ✓ Primary style
style_secondary      - ✓ From new_additions only
year_built           - ✓ From both sources
building_type        - ✓ From both sources
latitude             - ⚠ Only from new_additions (need to extract from geometry)
longitude            - ⚠ Only from new_additions (need to extract from geometry)
source               - ✓ Provenance tracking
source_confidence    - ✓ Quality score
num_floors           - ⚠ Only from existing_landmarks
borough              - ⚠ Only from existing_landmarks
final_score          - ⚠ Only from existing_landmarks (need ML scoring)
geometry             - ⚠ Only from existing_landmarks (MULTIPOLYGON)
address_normalized   - ✓ For deduplication
is_potential_duplicate - ✓ For deduplication
```

## ❌ Missing from new_additions.csv

These fields exist in the original CSV but weren't mapped:

1. **owner/developer** (`own_devel`) - Not critical for app
2. **materials** (`mat_prim`, `mat_sec`, `mat_third`) - Could be useful
3. **historic district** (`hist_dist`) - Useful context
4. **location/neighborhood** (`location`) - **IMPORTANT for app**

## 🎯 What the App Needs (from BuildingInfoScreen.js)

Based on your app code, these fields are displayed:

```javascript
buildingData.name          → building_name ✓
buildingData.address       → address ✓
buildingData.architect     → architect ✓
buildingData.year_built    → year_built ✓
buildingData.style         → style ✓
buildingData.height        → ❌ MISSING (need from footprints)
buildingData.floors        → num_floors ⚠ (partial)
buildingData.image_url     → ❌ NOT IN SCOPE (would need separate scraping)
buildingData.description   → ❌ NOT IN SCOPE (could use Exa later)
```

## 📋 Action Items

### Immediate (Step 01 fix):
- [ ] Add `location` field from new_additions.csv
- [ ] Add `materials` fields (mat_prim, mat_sec, mat_third)
- [ ] Add `historic_district` field
- [ ] Extract lat/lng from existing_landmarks geometry

### During Enrichment (Steps 03-06):
- [ ] Step 03 (Geocode): Add BBL, BIN, borough for new_additions
- [ ] Step 04 (PLUTO): Add num_floors for new_additions
- [ ] Step 05 (Footprints): Add building height, footprint polygon
- [ ] Step 06 (Names): Fill building_name for both sources

### During Scoring (Step 07):
- [ ] ML scoring: Generate final_score for new_additions

### Before Export (Step 08):
- [ ] Standardize for app: Ensure all app fields populated
- [ ] Add placeholder height if not from footprints
- [ ] Clean/validate all required fields

## Recommended: Update Step 01

Add these to `standardize_new_additions()`:

```python
'location': df.get('location'),              # Neighborhood
'mat_primary': df.get('mat_prim'),           # Primary material
'mat_secondary': df.get('mat_sec'),          # Secondary material
'historic_district': df.get('hist_dist'),    # Historic district
'owner_developer': df.get('own_devel'),      # Owner/developer
```

This preserves more context from the original data.

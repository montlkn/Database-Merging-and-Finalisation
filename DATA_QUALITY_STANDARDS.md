# Data Quality Standards

## Core Principle: N/A > Placeholder Data

**IMPORTANT**: Missing data (N/A, NaN, null) is ALWAYS preferable to placeholder/dummy data.

### Why?

1. **Placeholder data is misleading** - It appears complete but contains false information
2. **N/A is honest** - It clearly indicates where data is missing
3. **Placeholder data breaks analysis** - Dummy values corrupt statistics and calculations
4. **N/A can be filtered** - Missing data can be easily identified and handled appropriately

## Prohibited Placeholder Values

### ❌ NEVER USE:
- **BBL**: `5079660001` (or any dummy BBL)
- **Coordinates**: `(40.73096, -74.00328)` (or any default lat/long)
- **Year**: `0`, `1900`, `9999` (use N/A for unknown years)
- **Floors**: `0`, `999` (use N/A for unknown floor counts)
- **Height**: `0`, `-1` (use N/A for unknown heights)
- **Names**: "Unknown", "N/A", "TBD" (use actual N/A/null instead)

### ✅ USE INSTEAD:
- Python: `np.nan`, `None`, `pd.NA`
- CSV: Empty cell (not "NULL" or "N/A" strings)
- Database: `NULL`

## Data Validation Rules

### BBL (Borough Block Lot)
- Must be exactly 10 digits
- First digit must be 1-5 (NYC borough codes)
- If BBL cannot be found → set to N/A
- If building is a park/plaza/public space → N/A is acceptable

### Coordinates (Latitude/Longitude)
- Must be within NYC bounds: `40.4 < lat < 41.0` and `-74.3 < lng < -73.7`
- If geocoding fails → set to N/A (DO NOT use default coordinates)
- Existing landmarks may use `geometry` or `the_geom` GIS fields instead

### Year Built
- Must be between 1600-2030 (reasonable range for NYC buildings)
- If year is unknown → set to N/A (DO NOT use 0 or 1900)

### Floor Count
- Must be between 1-200 (reasonable range)
- If floor count is unknown → set to N/A (DO NOT use 0)

## Acceptable Missing Data

Some buildings legitimately lack certain data:

1. **Parks/Plazas/Public Spaces** - May not have BBL
2. **Existing Landmarks** - May have coordinates in `geometry` field instead of `latitude`/`longitude`
3. **Very Old Buildings** - May have uncertain year_built
4. **Low-rise Buildings** - May not have height data

## Data Quality Checks

Before committing data, verify:
- [ ] No placeholder BBLs (check for `5079660001`)
- [ ] No default coordinates (check for `40.73096, -74.00328`)
- [ ] No zero values for year/floors/height (unless legitimately zero floors)
- [ ] Missing data properly represented as N/A/NaN/null

## Current Data Quality Status

As of 2025-11-13:
- **Total buildings**: 37,385
- **BBL coverage**: 99.94% (22 missing, 11 are parks/public spaces)
- **Coordinate coverage**: 100% (all buildings have coordinates in some form)
- **Year built**: 100%
- **Floor count**: 98.02%
- **Placeholder data removed**: ✅ All 17 buildings with dummy coordinates cleaned

## Scripts Following This Standard

- ✅ `06n_final_cleanup_placeholders.py` - Enforces no placeholder data
- ✅ `03e_geocode_missing.py` - Uses N/A for failed geocodes
- ✅ `06c_fix_placeholder_bbls.py` - Attempts to fix, uses N/A if unfixable

## When You Find Placeholder Data

1. **Try to find real data** - Use geocoding, Exa, PLUTO, Building Footprints
2. **If unfixable** - Replace with N/A
3. **Document why** - Add comment explaining why data is missing
4. **Never commit placeholder data** - Better to have N/A

---

**Remember**: Honest missing data > Dishonest fake data

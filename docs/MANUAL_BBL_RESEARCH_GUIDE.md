# Manual BBL Research Guide

## Current Status
- **BBL Coverage**: 91.5% (3,610 out of 3,947 buildings)
- **Remaining**: 337 buildings need manual research

## File Location
`data/intermediate/buildings_missing_bbl_TO_RESEARCH.csv`

## Instructions

### 1. Open the CSV file
The file contains 337 buildings with these columns:
- `address` - Building address
- `building_name` - Official building name
- `architect`, `year_built`, `style` - Architectural details
- `source` - Where the building came from (existing_landmarks or new_additions)
- `geocoded_lat`, `geocoded_lng` - Coordinates (if available)
- **`bbl_manual`** - FILL THIS IN with the 10-digit BBL
- **`bin_manual`** - FILL THIS IN with the 7-digit BIN (optional but helpful)
- `notes` - Add any research notes

### 2. Research Tools

#### NYC Department of Finance ACRIS
- https://a836-acris.nyc.gov/DS/DocumentSearch/BBLResult
- Search by address to find BBL

#### NYC Zola (Property Search)
- https://zola.planning.nyc.gov/
- Interactive map with BBL/BIN data
- Can search by address or click on map

#### NYC GeoClient API
- Already tried automated geocoding
- These are edge cases that need manual research

#### NYC DOB BIS (Building Information System)
- https://a810-bisweb.nyc.gov/bisweb/bispi00.jsp
- Search by address or BIN

### 3. Common Issues & Solutions

**Complex Addresses** (e.g., "130-132 West 42nd Street and 113-137 West 41st Street")
- These are often multi-lot developments
- Choose the primary BBL or list multiple separated by semicolons
- Example: `1008430001;1008430002`

**Historic Districts / Bridges**
- Some may not have a single BBL
- Add note in `notes` column: "Historic district - no single BBL"
- Leave `bbl_manual` empty or use primary lot

**Parks and Public Spaces** (e.g., "Flushing Meadows Corona Park", "Pier 55")
- Use NYC Parks property database
- May need to use land parcel BBL

**Intersections** (e.g., "59th Street and 2nd Avenue")
- These might be referring to a specific building at that corner
- Research the actual building at that intersection

### 4. BBL Format
- **10 digits**: `BBBBLLLLLL`
  - First digit: Borough (1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island)
  - Next 5 digits: Block number (with leading zeros)
  - Last 4 digits: Lot number (with leading zeros)
- Example: `1012345678` = Manhattan, Block 12345, Lot 678

### 5. After Research

Once you've filled in the BBLs, run:
```bash
cd /Users/lucienmount/coding/nyc-buildings-pipeline
source venv/bin/activate
python scripts/03e_merge_manual_bbls.py
```

This will:
1. Load your manually researched BBLs
2. Merge them back into the main dataset
3. Report final BBL coverage percentage
4. Create `03e_final_bbls.csv` with complete data

### 6. Quality Check

Before running the merge script, verify:
- ✅ BBLs are 10 digits (no spaces, dashes, or letters)
- ✅ BINs are 7 digits (if provided)
- ✅ Borough code matches the location (1=MN, 2=BX, 3=BK, 4=QN, 5=SI)

## Breakdown by Source

### Existing Landmarks (269 buildings)
Most are LPC-designated landmarks with complex addresses or multi-lot historic districts.

**Examples:**
- Bush Tower: "130-132 West 42nd Street and 113-137 West 41st Street"
- Brooklyn Bridge: Spanning multiple lots
- Historic districts: Multiple properties grouped together

### New Additions (68 buildings)
Modern buildings with non-standard addresses or landmarks without street addresses.

**Examples:**
- Pier 55 (Little Island)
- Flushing Meadows Corona Park structures
- Buildings at intersections ("59th Street and 2nd Avenue")

## Tips for Efficient Research

1. **Batch by neighborhood** - Research buildings in the same area together
2. **Use Zola map** - Click on the map to quickly find BBLs
3. **Google Street View** - Verify you have the correct building
4. **Start with easy ones** - Buildings with simple addresses first
5. **Mark impossible ones** - Some genuinely don't have BBLs (bridges, parks)

## Need Help?

If you get stuck:
1. Check the building's Wikipedia page for address details
2. Search NYC LPC designation reports (PDF files often have BBL info)
3. Contact NYC311 for official property information
4. Leave a note in the `notes` column and skip to the next one

## Goal

Get as close to 100% BBL coverage as possible. Even 95%+ would be excellent for the pipeline!

Current: **91.5%** → Target: **95-100%**

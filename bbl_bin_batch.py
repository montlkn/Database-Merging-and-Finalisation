#!/usr/bin/env python3
"""
Batch-fill BBL and BIN for a CSV of NYC buildings.

Strategy
--------
1) Spatial join points -> MapPLUTO polygons to get BBL.
2) Spatial join points -> Building Footprints polygons to get BIN.
3) Fallback: call NYC Geoclient for rows still missing BBL/BIN using address+borough.
   Uses conservative throttling to respect usage guidelines.

Inputs
------
- Input CSV must include columns:
    address, borough, geocoded_lat, geocoded_lng
  Note: geocoded_lat/geocoded_lng can be WGS84 (typical lat/lon) OR
        NY State Plane Long Island feet (EPSG:2263) as numeric X/Y.
        The script auto-detects based on magnitude.

- Local GIS data (download these beforehand):
    * MapPLUTO (latest) -> a polygon layer with BBL, boro, block, lot fields.
      Source: https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-pluto-mappluto.page
    * Building Footprints -> polygon layer with BIN field.
      Source: https://data.cityofnewyork.us/City-Government/BUILDING/5zhs-2jue

  Unzip to folders and point to the layer files below.

- Optional: NYC Geoclient credentials for fallback.
    Register: https://developer.cityofnewyork.us/api/geoclient-api
    You will get app_id and app_key.

Usage
-----
python bbl_bin_batch.py   --in /path/to/buildings_missing_bbl_TO_RESEARCH.csv   --out /path/to/output_with_bbl_bin.csv   --pluto /path/to/MapPLUTO.shp   --footprints /path/to/BuildingFootprints.shp   --app-id YOUR_ID --app-key YOUR_KEY

You can omit --app-id/--app-key to skip API fallback.
"""
import argparse
import csv
import time
import re
import sys
import math
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely import wkt as shapely_wkt
from shapely.geometry import Point
from pyproj import CRS
import requests

PLUTO_BBL_FIELD_CANDIDATES = ["BBL", "bbl", "bbl_10", "bbl10", "boro_block_lot"]
FOOTPRINTS_BIN_FIELD_CANDIDATES = ["BIN", "bin", "buildingid", "building_id"]
FOOTPRINTS_BBL_FIELD_CANDIDATES = [
    "Map Pluto BBL",
    "map_pluto_bbl",
    "MAP_PLUTO_BBL",
    "BASE_BBL",
    "base_bbl",
    "BBL",
    "bbl",
]

def detect_crs(x_series, y_series):
    # If values look like State Plane feet (NY LI): x~800k-1.1M, y~150k-300k
    x_med = x_series.median(skipna=True)
    y_med = y_series.median(skipna=True)

    def looks_like_state_plane(a, b):
        return (600000 <= a <= 1200000) and (100000 <= b <= 400000)

    def looks_like_wgs84(a, b):
        return (-80 <= a <= -70) and (35 <= b <= 45)

    if looks_like_state_plane(x_med, y_med):
        return CRS.from_epsg(2263), False  # NAD83 / New York Long Island (ftUS)
    if looks_like_state_plane(y_med, x_med):
        return CRS.from_epsg(2263), True
    if looks_like_wgs84(x_med, y_med):
        return CRS.from_epsg(4326), False
    if looks_like_wgs84(y_med, x_med):
        return CRS.from_epsg(4326), True
    # Default: assume WGS84 and no swap
    return CRS.from_epsg(4326), False

def boronorm(boro):
    if boro is None or (isinstance(boro, float) and math.isnan(boro)):
        return None
    if isinstance(boro, (int, float)):
        code = int(boro)
        mapping_numeric = {
            1: "MANHATTAN",
            2: "BRONX",
            3: "BROOKLYN",
            4: "QUEENS",
            5: "STATEN ISLAND",
        }
        return mapping_numeric.get(code)
    if not isinstance(boro, str):
        return None
    b = boro.strip().upper()
    mapping = {
        "MN":"MANHATTAN","MANHATTAN":"MANHATTAN","NEW YORK":"MANHATTAN","NY":"MANHATTAN",
        "BX":"BRONX","BRONX":"BRONX",
        "BK":"BROOKLYN","KINGS":"BROOKLYN","BROOKLYN":"BROOKLYN",
        "QN":"QUEENS","QNS":"QUEENS","QUEENS":"QUEENS",
        "SI":"STATEN ISLAND","STATEN ISLAND":"STATEN ISLAND","RICHMOND":"STATEN ISLAND"
    }
    return mapping.get(b, b)

def clean_single_line_address(addr, boro):
    # Keep first anchor of address. Strip "aka", semicolons, parentheses.
    if not isinstance(addr, str):
        return None
    a = addr
    a = re.split(r"\baka\b|;|\(|\)", a, flags=re.IGNORECASE)[0]
    a = re.sub(r"\s+", " ", a).strip()
    b = boronorm(boro)
    if b and b not in a.upper():
        a = f"{a}, {b}"
    if "NEW YORK" not in a.upper() and "NY" not in a.upper():
        a = f"{a}, NY"
    return a

def extract_borough(row):
    candidates = [
        "borough",
        "borough_col",
        "borough_name_col",
        "borough_code",
        "borough_code_guess",
    ]
    for key in candidates:
        if key in row:
            val = row.get(key)
            if val is None:
                continue
            if isinstance(val, float) and math.isnan(val):
                continue
            text = str(val).strip()
            if text:
                return val
    return None

def geoclient_search(single_line, session, app_id=None, app_key=None, subscription_key=None):
    url = None
    params = {"input": single_line}
    headers = {}
    if subscription_key:
        url = "https://api.nyc.gov/geo/geoclient/v2/search.json"
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    elif app_id and app_key:
        url = "https://maps.nyc.gov/geoclient/v2/search.json"
        params["app_id"] = app_id
        params["app_key"] = app_key
    else:
        return None, None

    try:
        r = session.get(url, params=params, headers=headers, timeout=10)
        if r.status_code != 200:
            return None, None
        data = r.json()
        results = data.get("results") or data.get("search", {}).get("results") or []
        if not results:
            return None, None
        top = results[0]
        props = top.get("response", top)
        bbl = props.get("bbl") or props.get("bblTaxBlock") and props.get("bblBoroughCode")
        bin_ = props.get("buildingIdentificationNumber") or props.get("buildingIdentificationNumberIn")
        if isinstance(bbl, dict):
            bbl = bbl.get("bbl")
        return str(bbl) if bbl else None, str(bin_) if bin_ else None
    except Exception:
        return None, None

def normalize_value(val):
    if pd.isna(val):
        return None
    text = str(val).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return text

def _load_csv_geodata(path, geometry_candidates, columns_to_keep):
    geometry_candidates = [c for c in (geometry_candidates or []) if c]
    columns_to_keep = columns_to_keep or []
    geometries = []
    sample_x = []
    sample_y = []
    src_crs_hint = None
    point_candidates = [
        (("xcoord", "ycoord"), CRS.from_epsg(2263)),
        (("XCoord", "YCoord"), CRS.from_epsg(2263)),
        (("XCOORD", "YCOORD"), CRS.from_epsg(2263)),
        (("longitude", "latitude"), CRS.from_epsg(4326)),
        (("Longitude", "Latitude"), CRS.from_epsg(4326)),
        (("lon", "lat"), CRS.from_epsg(4326)),
    ]
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise RuntimeError(f"CSV file has no header: {path}")
        keep_cols = [col for col in columns_to_keep if col in reader.fieldnames]
        data_columns = {col: [] for col in keep_cols}
        for row in reader:
            geom = None
            for geom_col in geometry_candidates:
                val = row.get(geom_col)
                if not val:
                    continue
                try:
                    geom = shapely_wkt.loads(val)
                    break
                except Exception:
                    continue
            if geom is None:
                for (cx, cy), candidate_crs in point_candidates:
                    xv = row.get(cx)
                    yv = row.get(cy)
                    if not xv or not yv:
                        continue
                    try:
                        x = float(str(xv).replace(",", ""))
                        y = float(str(yv).replace(",", ""))
                    except ValueError:
                        continue
                    geom = Point(x, y)
                    if src_crs_hint is None:
                        src_crs_hint = candidate_crs
                    break
            if geom is None:
                continue
            for col in keep_cols:
                data_columns[col].append(row.get(col))
            geometries.append(geom)
            if len(sample_x) < 500:
                rep = geom.representative_point()
                sample_x.append(rep.x)
                sample_y.append(rep.y)
    if not geometries:
        return None
    if data_columns:
        df = pd.DataFrame(data_columns)
    else:
        df = pd.DataFrame(index=range(len(geometries)))
    gdf = gpd.GeoDataFrame(df.reset_index(drop=True), geometry=geometries)
    if src_crs_hint is not None:
        gdf = gdf.set_crs(src_crs_hint, allow_override=True)
    else:
        if sample_x:
            detected_crs, _ = detect_crs(pd.Series(sample_x), pd.Series(sample_y))
            gdf = gdf.set_crs(detected_crs, allow_override=True)
    return gdf

def load_geodata(path, geometry_candidates=None, columns_to_keep=None, require_geometry=True):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        gdf = _load_csv_geodata(path, geometry_candidates, columns_to_keep)
    else:
        gdf = gpd.read_file(path)
        if columns_to_keep:
            keep = [c for c in columns_to_keep if c in gdf.columns]
            if keep:
                cols = ["geometry"] + keep if "geometry" in gdf.columns else keep
                gdf = gdf[cols]
    if gdf is None or gdf.empty or ("geometry" in gdf and gdf["geometry"].isna().all()):
        if require_geometry:
            raise RuntimeError(f"Could not load geometries from {path}")
        return None
    if gdf.crs is None or gdf.crs.to_epsg() is None:
        sample = gdf.geometry.dropna().head(500)
        if not sample.empty:
            reps = sample.apply(lambda geom: geom.representative_point())
            detected_crs, _ = detect_crs(pd.Series([pt.x for pt in reps]), pd.Series([pt.y for pt in reps]))
            gdf = gdf.set_crs(detected_crs, allow_override=True)
    return gdf

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="input_csv", required=True)
    ap.add_argument("--out", dest="output_csv", required=True)
    ap.add_argument("--pluto", dest="pluto_path", required=True)
    ap.add_argument("--footprints", dest="fp_path", required=True)
    ap.add_argument("--app-id", dest="app_id", default=None)
    ap.add_argument("--app-key", dest="app_key", default=None)
    ap.add_argument("--subscription-key", dest="subscription_key", default=None,
                    help="Optional: NYC Geoclient subscription key (API gateway)")
    ap.add_argument("--sleep", dest="sleep", type=float, default=0.05, help="Seconds between API calls")
    args = ap.parse_args()

    df = pd.read_csv(args.input_csv)
    if not {"geocoded_lat","geocoded_lng"}.issubset(df.columns):
        print("ERROR: CSV must have geocoded_lat and geocoded_lng columns", file=sys.stderr)
        sys.exit(1)

    raw_x = df["geocoded_lat"].astype(float)  # Some inputs store X here, others store Y (or true latitude)
    raw_y = df["geocoded_lng"].astype(float)  # Likewise, may hold Y or longitude depending on source
    src_crs, swap_xy = detect_crs(raw_x, raw_y)

    if swap_xy:
        point_iter = zip(raw_y, raw_x)
    else:
        point_iter = zip(raw_x, raw_y)

    gdf = gpd.GeoDataFrame(df.copy(), geometry=[Point(xx, yy) for xx, yy in point_iter], crs=src_crs)

    # Load PLUTO and Footprints (supports CSV with WKT or coordinate columns)
    pluto = load_geodata(
        args.pluto_path,
        geometry_candidates=["geometry", "geom", "the_geom", "wkt"],
        columns_to_keep=PLUTO_BBL_FIELD_CANDIDATES,
        require_geometry=False,
    )
    footprints = load_geodata(
        args.fp_path,
        geometry_candidates=["the_geom", "geometry", "geom", "wkt"],
        columns_to_keep=FOOTPRINTS_BIN_FIELD_CANDIDATES + FOOTPRINTS_BBL_FIELD_CANDIDATES,
    )

    # Ensure same CRS
    if pluto is None or pluto.empty or pluto.geometry.isna().all():
        pluto = None
        print(f"WARNING: No usable geometry found for {args.pluto_path}; skipping PLUTO spatial join.", file=sys.stderr)
    if footprints.crs is None:
        footprints = footprints.set_crs(2263, allow_override=True)

    if gdf.crs.to_epsg() != 2263:
        gdf = gdf.to_crs(2263)
    if pluto is not None and pluto.crs.to_epsg() != 2263:
        pluto = pluto.to_crs(2263)
    if footprints.crs.to_epsg() != 2263:
        footprints = footprints.to_crs(2263)

    # Identify field names
    pluto_bbl_col = None
    if pluto is not None:
        for c in PLUTO_BBL_FIELD_CANDIDATES:
            if c in pluto.columns:
                pluto_bbl_col = c
                break
        if pluto_bbl_col is None:
            raise RuntimeError("Could not find BBL field in PLUTO. Expected one of: %s" % PLUTO_BBL_FIELD_CANDIDATES)

    fp_bin_col = None
    for c in FOOTPRINTS_BIN_FIELD_CANDIDATES:
        if c in footprints.columns:
            fp_bin_col = c
            break
    if fp_bin_col is None:
        raise RuntimeError("Could not find BIN field in Building Footprints. Expected one of: %s" % FOOTPRINTS_BIN_FIELD_CANDIDATES)

    fp_bbl_col = None
    for c in FOOTPRINTS_BBL_FIELD_CANDIDATES:
        if c in footprints.columns:
            fp_bbl_col = c
            break

    # Spatial joins
    out = df.copy()
    out["bbl_filled"] = None
    out["bin_filled"] = None
    out["bbl_source"] = None
    out["bin_source"] = None

    if pluto is not None:
        pluto_nonnull = pluto.dropna(subset=["geometry"])
        if pluto_nonnull.empty:
            print(f"WARNING: PLUTO geometry is empty after dropping nulls; skipping PLUTO spatial join.", file=sys.stderr)
        else:
            if pluto_nonnull.geom_type.isin(["Polygon", "MultiPolygon"]).any():
                gdf_bbl = gpd.sjoin(
                    gdf[["geometry"]],
                    pluto_nonnull[[pluto_bbl_col, "geometry"]],
                    predicate="within",
                    how="left",
                )
            else:
                gdf_bbl = gpd.sjoin_nearest(
                    gdf[["geometry"]],
                    pluto_nonnull[[pluto_bbl_col, "geometry"]],
                    how="left",
                )
            pluto_bbl_series = gdf_bbl[pluto_bbl_col].apply(normalize_value)
            mask_pluto = pluto_bbl_series.notna()
            if mask_pluto.any():
                out.loc[mask_pluto, "bbl_filled"] = pluto_bbl_series[mask_pluto]
                out.loc[mask_pluto, "bbl_source"] = "PLUTO"
            # Nearest fallback for remaining rows within 25 ft
            remaining_pluto = out["bbl_filled"].isna()
            if remaining_pluto.any():
                nearest = gpd.sjoin_nearest(
                    gdf.loc[remaining_pluto, ["geometry"]],
                    pluto_nonnull[[pluto_bbl_col, "geometry"]],
                    how="left",
                    distance_col="dist_pluto",
                )
                near_series = nearest[pluto_bbl_col].apply(normalize_value)
                good = near_series.notna() & (nearest["dist_pluto"] <= 25)
                if good.any():
                    idx_map = near_series[good].index
                    out.loc[idx_map, "bbl_filled"] = near_series[good]
                    out.loc[idx_map, "bbl_source"] = "PLUTO_NEAREST"

    fp_cols = ["geometry", fp_bin_col]
    if fp_bbl_col:
        fp_cols.append(fp_bbl_col)
    gdf_fp = gpd.sjoin(gdf[["geometry"]], footprints[fp_cols], predicate="within", how="left")

    fp_bin_series = gdf_fp[fp_bin_col].apply(normalize_value)
    mask_fp_bin = fp_bin_series.notna()
    if mask_fp_bin.any():
        out.loc[mask_fp_bin, "bin_filled"] = fp_bin_series[mask_fp_bin]
        out.loc[mask_fp_bin, "bin_source"] = "BuildingFootprints"

    if fp_bbl_col:
        fp_bbl_series = gdf_fp[fp_bbl_col].apply(normalize_value)
        mask_fp_bbl = fp_bbl_series.notna() & out["bbl_filled"].isna()
        if mask_fp_bbl.any():
            out.loc[mask_fp_bbl, "bbl_filled"] = fp_bbl_series[mask_fp_bbl]
            out.loc[mask_fp_bbl, "bbl_source"] = "BuildingFootprints"
    # Nearest/bin buffer fallback
    remaining_bin = out["bin_filled"].isna()
    if remaining_bin.any():
        nearest_fp = gpd.sjoin_nearest(
            gdf.loc[remaining_bin, ["geometry"]],
            footprints[[fp_bin_col, "geometry"]],
            how="left",
            distance_col="dist_fp",
        )
        near_bins = nearest_fp[fp_bin_col].apply(normalize_value)
        good_near = near_bins.notna() & (nearest_fp["dist_fp"] <= 20)
        if good_near.any():
            idx_map = near_bins[good_near].index
            out.loc[idx_map, "bin_filled"] = near_bins[good_near]
            out.loc[idx_map, "bin_source"] = "BuildingFootprintsNearest"
        remaining_after_near = out["bin_filled"].isna()
        if remaining_after_near.any():
            buffered = gdf.loc[remaining_after_near].copy()
            buffered["geometry"] = buffered.buffer(5)
            buffered = gpd.GeoDataFrame(buffered, geometry="geometry", crs=gdf.crs)
            join_buffer = gpd.sjoin(
                buffered[["geometry"]],
                footprints[[fp_bin_col, "geometry"]],
                predicate="intersects",
                how="left",
            )
            buf_bins = join_buffer[fp_bin_col].apply(normalize_value)
            good_buf = buf_bins.notna()
            if good_buf.any():
                idx_map = buf_bins[good_buf].index
                out.loc[idx_map, "bin_filled"] = buf_bins[good_buf]
                out.loc[idx_map, "bin_source"] = "BuildingFootprintsBuffered"

    # Fallback with Geoclient if configured
    if args.app_id and args.app_key or args.subscription_key:
        session = requests.Session()
        for i, row in out[out["bbl_filled"].isna() | out["bin_filled"].isna()].iterrows():
            borough_guess = extract_borough(row)
            single = clean_single_line_address(row.get("address"), borough_guess)
            if not single:
                continue
            bbl, bin_ = geoclient_search(
                single,
                session,
                app_id=args.app_id,
                app_key=args.app_key,
                subscription_key=args.subscription_key,
            )
            if pd.isna(out.at[i, "bbl_filled"]) and bbl:
                out.at[i, "bbl_filled"] = bbl
                out.at[i, "bbl_source"] = "Geoclient"
            if pd.isna(out.at[i, "bin_filled"]) and bin_:
                out.at[i, "bin_filled"] = bin_
                out.at[i, "bin_source"] = "Geoclient"
            time.sleep(args.sleep)

    # Preserve original bbl/bin if present; otherwise use filled
    def coalesce(a, b):
        return a if pd.notna(a) and str(a).strip() != "" else b

    out["bbl_final"] = [coalesce(a, b) for a, b in zip(out.get("bbl", [None]*len(out)), out["bbl_filled"])]
    out["bin_final"] = [coalesce(a, b) for a, b in zip(out.get("bin", [None]*len(out)), out["bin_filled"])]

    # Output
    out.to_csv(args.output_csv, index=False)
    print(f"Wrote: {args.output_csv}")

if __name__ == "__main__":
    main()

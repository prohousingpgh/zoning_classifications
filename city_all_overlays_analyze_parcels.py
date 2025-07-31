import geopandas as gpd
import pandas as pd
import os

# Define input file paths
parcels_fp = "parcels.geojson"
zoning_fp = "zoning_base.geojson"
output_fp = "parcel_zoning_overlay_results_all.xlsx"

# Define overlay filenames and result column names
overlay_files = {
    "overlay_IZ.geojson": "overlay_IZ",
    "overlay_FP.geojson": "overlay_FP",
    "overlay_UM.geojson": "overlay_UM",
    "overlay_HR.geojson": "overlay_HR",
    "overlay_NSCP.geojson": "overlay_NSCP",  # Make sure filename matches!
    "overlay_PERA.geojson": "overlay_PERA",
    "overlay_height.geojson": "overlay_height",
    "overlay_LS.geojson": "overlay_LS",
    "overlay_BC.geojson": "overlay_BC"
}

# Load GeoJSONs
parcels = gpd.read_file(parcels_fp).to_crs("EPSG:3857")
zoning_base = gpd.read_file(zoning_fp).to_crs(parcels.crs)

# Load overlays into a dictionary of GeoDataFrames
overlays = {
    name: gpd.read_file(fname).to_crs(parcels.crs)
    for fname, name in overlay_files.items()
}

# Build spatial index for zoning_base
zoning_index = zoning_base.sindex

# Collect results
results = []

for idx, parcel in parcels.iterrows():
    parcel_geom = parcel.geometry
    mapblocklo = parcel.get("MAPBLOCKLO", str(idx))

    # Find zoning match
    zon_new = "None"
    candidates = zoning_index.intersection(parcel_geom.bounds)
    for zid in candidates:
        zone = zoning_base.iloc[zid]
        if zone.geometry.intersects(parcel_geom):
            zon_new = zone.get("zon_new", "Unknown")
            break

    if zon_new == "None":
        continue

    # Check each overlay
    overlay_flags = {}
    for name, gdf in overlays.items():
        intersects = gdf.intersects(parcel_geom).any()
        overlay_flags[name] = intersects

    # Build result row
    row = {
        "MAPBLOCKLO": mapblocklo,
        "zon_new": zon_new,
        **overlay_flags
    }
    results.append(row)

# Save to Excel
df = pd.DataFrame(results)

# Ensure column order
ordered_cols = ["MAPBLOCKLO", "zon_new"] + list(overlay_files.values())
df = df[ordered_cols]

df.to_excel(output_fp, index=False)
print(f"✅ Analysis complete. Results saved to: {output_fp}")

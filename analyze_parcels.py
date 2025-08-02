import geopandas as gpd
import pandas as pd
import time
from tqdm import tqdm
import os

# Start timer
start_time = time.time()

# Define input file paths
parcels_fp = "sources/parcels.geojson"
zoning_fp = "sources/zoning_base.geojson"

# Define output file paths
output_fp = "output/parcel_zoning_overlay_results.csv"
output_summary_fp = "output/parcel_zoning_overlay_results_summary.csv"

# Define overlay filenames and result column names
overlay_files = {
    "sources/overlay_IZ.geojson": "IZ", # Inclusionary Zoning Overlay
    "sources/overlay_FP.geojson": "FP", # Floodplain Overlay
    "sources/overlay_UM.geojson": "UM", # Undermined Area Overlay
    "sources/overlay_HR.geojson": "HR", # Height Reduction Overlay
    "sources/overlay_NP.geojson": "NP",  # North Side Commercial Parking Overlay
    "sources/overlay_PR.geojson": "PR", # Parking Elimination/Reduction Area
    "sources/overlay_HO.geojson": "HO", # Height Overlay
    "sources/overlay_LS.geojson": "LS", # Landslide Prone Area
    "sources/overlay_BC.geojson": "BC", # Baum Centre Corridor Overlay
    "sources/overlay_TR.geojson": "TR", # 1500' Major Transit Buffer
    "sources/overlay_RR.geojson": "RR", # RIV Riparian Buffer (125ft)
    "sources/overlay_SR.geojson": "SR", # Stormwater Riparian Buffer
    "sources/overlay_PS.geojson": "PS", # Potential Steep Slope Overlay
    "sources/overlay_HD.geojson": "HD", # Designated Historic Districts
    "sources/overlay_HS.geojson": "HS", # Designated Historic Sites
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

for idx, parcel in tqdm(parcels.iterrows(), total=len(parcels), desc="Processing parcels"):
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

    overlay_flags = {}
    present_overlays = []
    for name, gdf in overlays.items():
        if gdf.intersects(parcel_geom).any():
            overlay_flags[name] = name
            present_overlays.append(name)
        else:
            overlay_flags[name] = ""

    # Create summary string
    zoning_summary = "-".join([zon_new] + present_overlays)

    # Build result row
    row = {
        "MAPBLOCKLO": mapblocklo,
        "zon_new": zon_new,
        **overlay_flags,
        "zoning_summary": zoning_summary
    }
    results.append(row)

# Save to Excel
df = pd.DataFrame(results)

# Ensure column order
ordered_cols = ["MAPBLOCKLO", "zon_new"] + list(overlay_files.values()) + ["zoning_summary"]
df = df[ordered_cols]

# Results csv
df.to_csv(output_fp, index=False)

# Summary csv
summary_df = df["zoning_summary"].value_counts().reset_index()
summary_df.columns = ["zoning_summary", "count"]
summary_df.to_csv(output_summary_fp, index=False)

# Print results to screen
elapsed = time.time() - start_time
print(f"✅ Analysis complete. Results saved to: {output_fp}")
print(f"✅ Zoning summary saved to: {output_summary_fp}")
print(f"⏱️ Script completed in {elapsed:.2f} seconds.")
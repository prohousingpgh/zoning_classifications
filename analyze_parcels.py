import geopandas as gpd
import pandas as pd
import time
from tqdm import tqdm
import os
import warnings
tqdm.pandas()
warnings.filterwarnings("ignore", category=RuntimeWarning, message="Several features with id = 0")

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

# Load parcel data
print("📂 Loading parcel data (this may take a few minutes)...")
parcels = gpd.read_file(parcels_fp).to_crs("EPSG:3857")

# Define target MUNICODE values
target_municodes = list(range(101, 133)) + [
    801, 803, 810, 812, 818, 819, 831, 835, 838, 839, 840,
    842, 850, 854, 863, 866, 870, 874, 877, 902, 919,
    926, 931, 934, 937, 939, 940, 941
]

# Filter parcels by MUNICODE
parcels = parcels[parcels["MUNICODE"].isin(target_municodes)].copy()
print(f"🔍 Filtered to {len(parcels)} parcels in specified MUNICODEs.")

# Load zoning base
print("📂 Loading base zoning...")
zoning_base = gpd.read_file(zoning_fp).to_crs(parcels.crs)

# Spatial join to get zoning
print("🔗 Performing spatial join with base zoning...")
joined = gpd.sjoin(parcels, zoning_base[["zon_new", "geometry"]], how="left", predicate="intersects")

# Drop parcels with no zoning
joined = joined[~joined["zon_new"].isna()].copy()

# Add square footage
joined["parcel_sqft"] = joined.geometry.area / 0.092903

# Track overlay results
overlay_results = {}
print("📂 Loading and joining overlays...")
for fname, name in tqdm(overlay_files.items(), desc="Processing overlays"):
    overlay = gpd.read_file(fname).to_crs(parcels.crs)

    # Perform spatial join and identify intersecting parcel indices
    joined_overlay = gpd.sjoin(joined[["geometry"]], overlay[["geometry"]], how="inner", predicate="intersects")

    # Mark matched parcels
    matched_parcels = joined_overlay.index.unique()
    joined[name] = joined.index.isin(matched_parcels)
    joined[name] = joined[name].apply(lambda x: name if x else "")

# Construct zoning summary
print("🏠 Constructing zoning classification summary...")
overlay_names = list(overlay_files.values())
joined["zoning_summary"] = joined.progress_apply(
    lambda row: "-".join([row["zon_new"]] + [val for val in row[overlay_names] if val]),
    axis=1
)

# Build final DataFrame
print("📂 Building output files...")
joined["parcel_id"] = joined["MAPBLOCKLO"]
joined["municode"] = joined["MUNICODE"]
cols = ["parcel_id", "municode", "zon_new", "parcel_sqft"] + overlay_names + ["zoning_summary"]
final_df = joined[cols].copy()

# Drop duplicate rows before saving
final_df = final_df.drop_duplicates()

# Save full output
final_df.to_csv(output_fp, index=False)

# Save summary
summary_df = final_df.groupby("zoning_summary").agg(
    count=("parcel_id", "count"),
    total_sqft=("parcel_sqft", "sum")
).reset_index()
summary_df.to_csv(output_summary_fp, index=False)

# Timing
elapsed = time.time() - start_time
print(f"✅ Analysis complete. Results saved to: {output_fp}")
print(f"✅ Zoning summary saved to: {output_summary_fp}")
print(f"⏱️ Script completed in {elapsed:.2f} seconds.")

#!/usr/bin/env python3
"""
Enhanced GTFS Data Pruner
Removes unnecessary columns while preserving GTFS relationships and required fields
"""

import pandas as pd
import os
import sys

print("=== GTFS DATA PRUNER ===")

# Get script directory and use relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GTFS_DIR = os.path.join(SCRIPT_DIR, "gtfs_vancouver")
OUT_DIR = os.path.join(SCRIPT_DIR, "gtfs_pruned")

print(f"Input directory: {GTFS_DIR}")
print(f"Output directory: {OUT_DIR}")
os.makedirs(OUT_DIR, exist_ok=True)

# Define columns to keep for each GTFS file
# Includes all essential columns for GTFS compliance and analysis
KEEP = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone"],
    "stops.txt": ["stop_id", "stop_name", "stop_lat", "stop_lon"],
    "routes.txt": [
        "route_id", 
        "agency_id",  # CRITICAL: Required for GTFS when multiple agencies exist
        "route_short_name", 
        "route_long_name", 
        "route_type"
    ],
    "trips.txt": [
        "route_id", 
        "service_id", 
        "trip_id", 
        "direction_id", 
        "shape_id"
    ],
    "stop_times.txt": [
        "trip_id", 
        "arrival_time", 
        "departure_time", 
        "stop_id", 
        "stop_sequence"
    ],
    "calendar.txt": [
        "service_id", 
        "monday", "tuesday", "wednesday", "thursday", 
        "friday", "saturday", "sunday", 
        "start_date", 
        "end_date"
    ],
    "calendar_dates.txt": ["service_id", "date", "exception_type"],
    "shapes.txt": [
        "shape_id", 
        "shape_pt_lat", 
        "shape_pt_lon", 
        "shape_pt_sequence", 
        "shape_dist_traveled"
    ],
}

# Process each file
print(f"\n=== PROCESSING {len(KEEP)} FILES ===")
errors = []
processed = 0

for fname, required_cols in KEEP.items():
    print(f"\nProcessing {fname}...")
    path_in = os.path.join(GTFS_DIR, fname)
    path_out = os.path.join(OUT_DIR, fname)
    
    if not os.path.exists(path_in):
        print(f"  ⚠️  {path_in} not found, skipping...")
        errors.append(f"{fname}: File not found")
        continue
    
    try:
        # Read with string dtype to preserve all values
        df = pd.read_csv(path_in, dtype=str, low_memory=False)
        original_rows = len(df)
        original_cols = len(df.columns)
        
        print(f"  📂 Read {path_in}")
        print(f"  📊 Original: {original_cols} columns, {original_rows:,} rows")
        
        # Filter to keep only specified columns
        to_keep = [c for c in required_cols if c in df.columns]
        missing = [c for c in required_cols if c not in df.columns]
        extra = [c for c in df.columns if c not in required_cols]
        
        if missing:
            print(f"  ⚠️  Missing columns: {missing}")
            errors.append(f"{fname}: Missing columns {missing}")
        
        if extra:
            print(f"  ℹ️  Dropping {len(extra)} extra columns: {', '.join(extra[:5])}{'...' if len(extra) > 5 else ''}")
        
        # Create pruned dataframe
        if not to_keep:
            print(f"  ❌ ERROR: No valid columns to keep for {fname}!")
            errors.append(f"{fname}: No valid columns")
            continue
        
        pruned = df[to_keep].copy()
        
        # Special handling for trips.txt: normalize empty shape_id values
        if fname == "trips.txt" and "shape_id" in pruned.columns:
            # Replace empty strings with NaN so they're written as empty in CSV
            # This ensures gtfs-to-sql treats them as NULL
            pruned['shape_id'] = pruned['shape_id'].replace('', pd.NA)
        
        # Remove completely empty rows
        pruned = pruned.dropna(how='all')
        
        # Save to CSV
        # For trips.txt, use na_rep='' to write empty shape_ids as empty strings (not 'nan')
        na_rep = '' if fname == "trips.txt" else 'nan'
        pruned.to_csv(path_out, index=False, encoding="utf-8", na_rep=na_rep)
        final_rows = len(pruned)
        
        print(f"  ✅ Saved {path_out}")
        print(f"  📊 Final: {len(to_keep)} columns, {final_rows:,} rows")
        
        if final_rows < original_rows:
            print(f"  ℹ️  Removed {original_rows - final_rows:,} empty rows")
        
        processed += 1
        
    except Exception as e:
        print(f"  ❌ ERROR processing {fname}: {e}")
        errors.append(f"{fname}: {str(e)}")
        continue

print(f"\n=== DATA PRUNER COMPLETE ===")
print(f"Processed: {processed}/{len(KEEP)} files")
print(f"Output directory: {OUT_DIR}/")

if errors:
    print(f"\n⚠️  Warnings/Errors ({len(errors)}):")
    for error in errors:
        print(f"  - {error}")
    sys.exit(1)
else:
    print("\n✅ All files processed successfully!")

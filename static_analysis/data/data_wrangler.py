#!/usr/bin/env python3
# data_wrangler.py - Vancouver Area Data Filter and Time Format Cleaner

import pandas as pd
import os
import re

print("=== VANCOUVER AREA DATA FILTER ===")

# Define Vancouver bounding box coordinates in WGS84 (lat/lon)
# Greater Vancouver Area approximate bounds
MIN_LON = -123.3  # West longitude
MAX_LON = -122.3  # East longitude  
MIN_LAT = 49.0    # South latitude
MAX_LAT = 49.5    # North latitude

print(f"📍 Vancouver bounding box (WGS84):")
print(f"   Longitude: {MIN_LON} to {MAX_LON}")
print(f"   Latitude: {MIN_LAT} to {MAX_LAT}")

# Get script directory for relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GTFS_PRUNED_DIR = os.path.join(SCRIPT_DIR, "gtfs_pruned")
GTFS_BACKUP_DIR = os.path.join(SCRIPT_DIR, "gtfs_full_backup")

# ============================================================================
# STEP 1: Geographic Filtering
# ============================================================================

# Initialize variables
geographic_filtering_completed = False
filtered_stop_times_after_geo = None

# Load stops.txt to filter by coordinates
print("\n🗺️  STEP 1: Geographic Filtering")
print("📂 Loading stops.txt...")
stops_path = os.path.join(GTFS_PRUNED_DIR, "stops.txt")

if not os.path.exists(stops_path):
    print(f"⚠️  {stops_path} not found, skipping geographic filtering")
else:
    stops_df = pd.read_csv(stops_path)
    print(f"📊 Original stops: {len(stops_df):,}")

    # Filter stops within Vancouver area
    print("🗺️ Filtering stops within Vancouver area...")
    filtered_stops = stops_df[
        (stops_df['stop_lon'] >= MIN_LON) & (stops_df['stop_lon'] <= MAX_LON) &
        (stops_df['stop_lat'] >= MIN_LAT) & (stops_df['stop_lat'] <= MAX_LAT)
    ]

    print(f"📊 Vancouver stops: {len(filtered_stops):,}")

    if len(filtered_stops) == 0:
        print("⚠️ No stops found in specified coordinates. Checking coordinate system...")
        print(f"Sample coordinates from data:")
        print(stops_df[['stop_lon', 'stop_lat']].head())
        print("Note: You may need to check if coordinates are in lat/lon (WGS84) vs projected (Web Mercator)")
        geographic_filtering_completed = False
    else:
        geographic_filtering_completed = True
        
        # Get list of stop IDs in Vancouver area
        vancouver_stop_ids = set(filtered_stops['stop_id'].astype(str))

        # Filter stop_times.txt to only include stops in Vancouver
        print("\n📂 Loading stop_times.txt...")
        stop_times_path = os.path.join(GTFS_PRUNED_DIR, "stop_times.txt")
        stop_times_df = pd.read_csv(stop_times_path, dtype=str)
        print(f"📊 Original stop_times: {len(stop_times_df):,}")

        print("🔍 Filtering stop_times for Vancouver stops...")
        filtered_stop_times = stop_times_df[stop_times_df['stop_id'].isin(vancouver_stop_ids)]
        print(f"📊 Vancouver stop_times: {len(filtered_stop_times):,}")

        # Get list of trips that serve Vancouver stops
        vancouver_trip_ids = set(filtered_stop_times['trip_id'])
        print(f"📊 Trips serving Vancouver: {len(vancouver_trip_ids):,}")

        # Filter trips.txt
        print("\n📂 Loading and filtering trips.txt...")
        trips_path = os.path.join(GTFS_PRUNED_DIR, "trips.txt")
        trips_df = pd.read_csv(trips_path, dtype=str)
        filtered_trips = trips_df[trips_df['trip_id'].isin(vancouver_trip_ids)]

        # Note: We keep trips without shape_id - they can be handled via map-matching
        # GTFS allows optional shape_id, and we'll use stop-to-stop connections for those without shapes
        if 'shape_id' in filtered_trips.columns:
            trips_with_shape = filtered_trips[
                filtered_trips['shape_id'].notna() & 
                (filtered_trips['shape_id'].astype(str).str.strip() != '') &
                (filtered_trips['shape_id'].astype(str) != 'nan')
            ]
            trips_without_shape = len(filtered_trips) - len(trips_with_shape)
            if trips_without_shape > 0:
                print(f"  ℹ️  {trips_without_shape:,} trips without shape_id (will use map-matching/stop-to-stop connections)")

        print(f"📊 Vancouver trips: {len(filtered_trips):,} out of {len(trips_df):,}")

        # Get routes used by Vancouver trips
        vancouver_route_ids = set(filtered_trips['route_id'].astype(str))

        # Filter routes.txt
        print("\n📂 Loading and filtering routes.txt...")
        routes_path = os.path.join(GTFS_PRUNED_DIR, "routes.txt")
        routes_df = pd.read_csv(routes_path, dtype=str)
        filtered_routes = routes_df[routes_df['route_id'].astype(str).isin(vancouver_route_ids)]
        print(f"📊 Vancouver routes: {len(filtered_routes):,} out of {len(routes_df):,}")

        # Get agencies used by Vancouver routes (if agency_id column exists)
        vancouver_agency_ids = set()
        if 'agency_id' in filtered_routes.columns:
            vancouver_agency_ids = set(filtered_routes['agency_id'].astype(str).dropna())

        # Filter agency.txt
        print("\n📂 Loading and filtering agency.txt...")
        agency_path = os.path.join(GTFS_PRUNED_DIR, "agency.txt")
        agency_df = pd.read_csv(agency_path, dtype=str)

        if vancouver_agency_ids:
            filtered_agency = agency_df[agency_df['agency_id'].astype(str).isin(vancouver_agency_ids)]
        else:
            # If no agency_id in routes, keep all agencies
            filtered_agency = agency_df

        print(f"📊 Vancouver agencies: {len(filtered_agency):,} out of {len(agency_df):,}")

        # Filter calendar.txt and calendar_dates.txt based on services used by Vancouver trips
        vancouver_service_ids = set(filtered_trips['service_id'].astype(str))

        print("\n📂 Loading and filtering calendar.txt...")
        calendar_path = os.path.join(GTFS_PRUNED_DIR, "calendar.txt")
        calendar_df = pd.read_csv(calendar_path, dtype=str)
        filtered_calendar = calendar_df[calendar_df['service_id'].astype(str).isin(vancouver_service_ids)]
        print(f"📊 Vancouver calendar entries: {len(filtered_calendar):,} out of {len(calendar_df):,}")

        print("\n📂 Loading and filtering calendar_dates.txt...")
        calendar_dates_path = os.path.join(GTFS_PRUNED_DIR, "calendar_dates.txt")
        calendar_dates_df = pd.read_csv(calendar_dates_path, dtype=str)
        filtered_calendar_dates = calendar_dates_df[calendar_dates_df['service_id'].astype(str).isin(vancouver_service_ids)]
        print(f"📊 Vancouver calendar_dates: {len(filtered_calendar_dates):,} out of {len(calendar_dates_df):,}")

        # Filter shapes.txt based on shapes used by Vancouver trips
        vancouver_shape_ids = set(filtered_trips['shape_id'].dropna().astype(str))

        if len(vancouver_shape_ids) > 0:
            print("\n📂 Loading and filtering shapes.txt...")
            shapes_path = os.path.join(GTFS_PRUNED_DIR, "shapes.txt")
            shapes_df = pd.read_csv(shapes_path, dtype=str)
            filtered_shapes = shapes_df[shapes_df['shape_id'].astype(str).isin(vancouver_shape_ids)]
            print(f"📊 Vancouver shapes: {len(filtered_shapes):,} out of {len(shapes_df):,}")
        else:
            print("\n⚠️ No shape_ids found in Vancouver trips")
            filtered_shapes = pd.DataFrame()

        # Create backup directory
        print("\n💾 Creating backup of original files...")
        os.makedirs(GTFS_BACKUP_DIR, exist_ok=True)

        # Backup original files
        files_to_backup = ['stops.txt', 'stop_times.txt', 'trips.txt', 'routes.txt', 
                           'agency.txt', 'calendar.txt', 'calendar_dates.txt', 'shapes.txt']

        for file in files_to_backup:
            src = os.path.join(GTFS_PRUNED_DIR, file)
            dst = os.path.join(GTFS_BACKUP_DIR, file)
            if os.path.exists(src):
                pd.read_csv(src, dtype=str).to_csv(dst, index=False)

        print(f"✅ Original files backed up to {GTFS_BACKUP_DIR}/")

        # Save filtered files (except stop_times.txt which will be saved after time format cleaning)
        print("\n💾 Saving filtered GTFS files...")
        filtered_stops.to_csv(os.path.join(GTFS_PRUNED_DIR, "stops.txt"), index=False)
        # Store filtered_stop_times for later time format cleaning (don't save yet)
        filtered_stop_times_after_geo = filtered_stop_times.copy()
        # For trips, write with na_rep='' to ensure empty shape_ids are written as empty (not 'nan')
        filtered_trips.to_csv(os.path.join(GTFS_PRUNED_DIR, "trips.txt"), index=False, na_rep='')
        filtered_routes.to_csv(os.path.join(GTFS_PRUNED_DIR, "routes.txt"), index=False)
        filtered_agency.to_csv(os.path.join(GTFS_PRUNED_DIR, "agency.txt"), index=False)
        filtered_calendar.to_csv(os.path.join(GTFS_PRUNED_DIR, "calendar.txt"), index=False)
        filtered_calendar_dates.to_csv(os.path.join(GTFS_PRUNED_DIR, "calendar_dates.txt"), index=False)

        if len(filtered_shapes) > 0:
            filtered_shapes.to_csv(os.path.join(GTFS_PRUNED_DIR, "shapes.txt"), index=False)

        print("\n✅ Vancouver area filtering completed!")
        print(f"📊 Geographic Filtering Summary:")
        print(f"  - Stops: {len(stops_df):,} → {len(filtered_stops):,}")
        print(f"  - Stop times: {len(stop_times_df):,} → {len(filtered_stop_times):,} (will be cleaned in Step 2)")
        print(f"  - Trips: {len(trips_df):,} → {len(filtered_trips):,}")
        print(f"  - Routes: {len(routes_df):,} → {len(filtered_routes):,}")
        print(f"  - Agencies: {len(agency_df):,} → {len(filtered_agency):,}")
        print(f"  - Calendar: {len(calendar_df):,} → {len(filtered_calendar):,}")
        print(f"  - Calendar dates: {len(calendar_dates_df):,} → {len(filtered_calendar_dates):,}")
        if len(vancouver_shape_ids) > 0:
            print(f"  - Shapes: {len(shapes_df):,} → {len(filtered_shapes):,}")

# ============================================================================
# STEP 2: Time Format Cleaning and Validation
# ============================================================================

def clean_time_format(time_str):
    """Clean time format to match GTFS spec: HH:MM:SS or H:MM:SS"""
    if pd.isna(time_str) or time_str == '':
        return time_str
    
    # Convert to string and strip whitespace
    time_str = str(time_str).strip()
    
    # Handle empty strings
    if not time_str:
        return time_str
    
    # Parse time format: handle " 6:16:00" or "6:16:00" -> "06:16:00"
    # Pattern: optional spaces, 1-2 digits, colon, 2 digits, optional colon and 2 digits
    match = re.match(r'^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?$', time_str)
    if match:
        hours = int(match.group(1))
        minutes = match.group(2)
        seconds = match.group(3) if match.group(3) else '00'
        
        # Format as HH:MM:SS (pad hours to 2 digits if needed)
        return f"{hours:02d}:{minutes}:{seconds}"
    
    # If it doesn't match, return as-is (might be invalid, but let gtfs-to-sql handle it)
    return time_str

# Clean stop_times.txt time formats and filter by valid trip_ids
print("\n🔧 STEP 2: Cleaning time formats and validating trip_ids...")
stop_times_path = os.path.join(GTFS_PRUNED_DIR, "stop_times.txt")
trips_path = os.path.join(GTFS_PRUNED_DIR, "trips.txt")

# Use already filtered stop_times from geographic filtering if available
if geographic_filtering_completed and filtered_stop_times_after_geo is not None:
    print("📂 Using stop_times from geographic filtering...")
    stop_times_df = filtered_stop_times_after_geo.copy()
    print(f"📊 Stop_times rows to clean: {len(stop_times_df):,}")
elif os.path.exists(stop_times_path):
    print(f"📂 Loading {stop_times_path}...")
    stop_times_df = pd.read_csv(stop_times_path, dtype=str)
    print(f"📊 Original stop_times rows: {len(stop_times_df):,}")
else:
    print(f"⚠️  {stop_times_path} not found, skipping time format cleaning")
    stop_times_df = None

if stop_times_df is not None:
    # Clean arrival_time and departure_time
    print("🧹 Cleaning arrival_time...")
    stop_times_df['arrival_time'] = stop_times_df['arrival_time'].apply(clean_time_format)
    
    print("🧹 Cleaning departure_time...")
    stop_times_df['departure_time'] = stop_times_df['departure_time'].apply(clean_time_format)
    
    # Filter stop_times to only include valid trip_ids
    if os.path.exists(trips_path):
        print(f"📂 Loading {trips_path} to get valid trip_ids...")
        trips_df = pd.read_csv(trips_path, dtype=str)
        valid_trip_ids = set(trips_df['trip_id'].astype(str))
        print(f"📊 Valid trips: {len(valid_trip_ids):,}")
        
        # Filter stop_times
        original_count = len(stop_times_df)
        stop_times_df = stop_times_df[stop_times_df['trip_id'].astype(str).isin(valid_trip_ids)]
        filtered_count = len(stop_times_df)
        removed_count = original_count - filtered_count
        
        if removed_count > 0:
            print(f"🗑️  Removed {removed_count:,} stop_times rows with invalid trip_ids")
        print(f"📊 Filtered stop_times rows: {filtered_count:,}")
    else:
        print(f"⚠️  {trips_path} not found, skipping trip_id validation")
    
    # Save cleaned file
    print(f"💾 Saving cleaned file to {stop_times_path}...")
    stop_times_df.to_csv(stop_times_path, index=False)
    print(f"✅ Cleaned and filtered {len(stop_times_df):,} rows")
    print("\n✅ Time format cleaning and validation completed!")

print("\n=== DATA WRANGLING COMPLETE ===")
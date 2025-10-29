#!/usr/bin/env python3
# data_wrangler.py - Manchester Area Data Filter

import pandas as pd
import os

print("=== MANCHESTER AREA DATA FILTER ===")

# Define Manchester bounding box coordinates in WGS84 (lat/lon)
# Converted from Web Mercator coordinates to approximate lat/lon
# MIN_LON = -2.4  # West longitude
# MAX_LON = -2.1  # East longitude  
# MIN_LAT = 53.4  # South latitude
# MAX_LAT = 53.6  # North latitude

MIN_LON = -2.197188  # West longitude
MAX_LON = -2.187103  # East longitude  
MIN_LAT = 53.441632  # South latitude
MAX_LAT = 53.446054  # North latitude

print(f"📍 Manchester bounding box (WGS84):")
print(f"   Longitude: {MIN_LON} to {MAX_LON}")
print(f"   Latitude: {MIN_LAT} to {MAX_LAT}")

# Load stops.txt to filter by coordinates
print("📂 Loading stops.txt...")
stops_df = pd.read_csv("src/data/gtfs_pruned/stops.txt")
print(f"📊 Original stops: {len(stops_df)}")

# Filter stops within Manchester area
print("🗺️ Filtering stops within Manchester area...")
filtered_stops = stops_df[
    (stops_df['stop_lon'] >= MIN_LON) & (stops_df['stop_lon'] <= MAX_LON) &
    (stops_df['stop_lat'] >= MIN_LAT) & (stops_df['stop_lat'] <= MAX_LAT)
]

print(f"📊 Manchester stops: {len(filtered_stops)}")

if len(filtered_stops) == 0:
    print("⚠️ No stops found in specified coordinates. Checking coordinate system...")
    print(f"Sample coordinates from data:")
    print(stops_df[['stop_lon', 'stop_lat']].head())
    print("Note: You may need to check if coordinates are in lat/lon (WGS84) vs projected (Web Mercator)")

# Get list of stop IDs in Manchester area
manchester_stop_ids = set(filtered_stops['stop_id'].astype(str))

# Filter stop_times.txt to only include stops in Manchester
print("📂 Loading stop_times.txt...")
stop_times_df = pd.read_csv("src/data/gtfs_pruned/stop_times.txt", dtype=str)
print(f"📊 Original stop_times: {len(stop_times_df)}")

print("🔍 Filtering stop_times for Manchester stops...")
filtered_stop_times = stop_times_df[stop_times_df['stop_id'].isin(manchester_stop_ids)]
print(f"📊 Manchester stop_times: {len(filtered_stop_times)}")

# Get list of trips that serve Manchester stops
manchester_trip_ids = set(filtered_stop_times['trip_id'])
print(f"📊 Trips serving Manchester: {len(manchester_trip_ids)}")

# Filter trips.txt
print("📂 Loading and filtering trips.txt...")
trips_df = pd.read_csv("src/data/gtfs_pruned/trips.txt")
filtered_trips = trips_df[trips_df['trip_id'].isin(manchester_trip_ids)]
print(f"📊 Manchester trips: {len(filtered_trips)} out of {len(trips_df)}")

# Get routes used by Manchester trips
manchester_route_ids = set(filtered_trips['route_id'].astype(str))

# Filter routes.txt
print("📂 Loading and filtering routes.txt...")
routes_df = pd.read_csv("src/data/gtfs_pruned/routes.txt")
filtered_routes = routes_df[routes_df['route_id'].astype(str).isin(manchester_route_ids)]
print(f"📊 Manchester routes: {len(filtered_routes)} out of {len(routes_df)}")

# Get agencies used by Manchester routes (if agency_id column exists)
manchester_agency_ids = set()
if 'agency_id' in filtered_routes.columns:
    manchester_agency_ids = set(filtered_routes['agency_id'].astype(str).dropna())

# Filter agency.txt
print("📂 Loading and filtering agency.txt...")
agency_df = pd.read_csv("src/data/gtfs_pruned/agency.txt")

if manchester_agency_ids:
    filtered_agency = agency_df[agency_df['agency_id'].astype(str).isin(manchester_agency_ids)]
else:
    # If no agency_id in routes, keep all agencies
    filtered_agency = agency_df

print(f"📊 Manchester agencies: {len(filtered_agency)} out of {len(agency_df)}")

# Filter calendar.txt and calendar_dates.txt based on services used by Manchester trips
manchester_service_ids = set(filtered_trips['service_id'].astype(str))

print("📂 Loading and filtering calendar.txt...")
calendar_df = pd.read_csv("src/data/gtfs_pruned/calendar.txt")
filtered_calendar = calendar_df[calendar_df['service_id'].astype(str).isin(manchester_service_ids)]
print(f"📊 Manchester calendar entries: {len(filtered_calendar)} out of {len(calendar_df)}")

print("📂 Loading and filtering calendar_dates.txt...")
calendar_dates_df = pd.read_csv("src/data/gtfs_pruned/calendar_dates.txt")
filtered_calendar_dates = calendar_dates_df[calendar_dates_df['service_id'].astype(str).isin(manchester_service_ids)]
print(f"📊 Manchester calendar_dates: {len(filtered_calendar_dates)} out of {len(calendar_dates_df)}")

# Filter shapes.txt based on shapes used by Manchester trips
manchester_shape_ids = set(filtered_trips['shape_id'].dropna().astype(str))

if len(manchester_shape_ids) > 0:
    print("📂 Loading and filtering shapes.txt...")
    shapes_df = pd.read_csv("src/data/gtfs_pruned/shapes.txt")
    filtered_shapes = shapes_df[shapes_df['shape_id'].astype(str).isin(manchester_shape_ids)]
    print(f"📊 Manchester shapes: {len(filtered_shapes)} out of {len(shapes_df)}")
else:
    print("⚠️ No shape_ids found in Manchester trips")
    filtered_shapes = pd.DataFrame()

# Save filtered data
print("\n💾 Saving filtered GTFS files...")

# Create backup directory
backup_dir = "src/data/gtfs_full_backup"
os.makedirs(backup_dir, exist_ok=True)

# Backup original files
files_to_backup = ['stops.txt', 'stop_times.txt', 'trips.txt', 'routes.txt', 
                   'agency.txt', 'calendar.txt', 'calendar_dates.txt', 'shapes.txt']

for file in files_to_backup:
    src = f"src/data/gtfs_pruned/{file}"
    dst = f"{backup_dir}/{file}"
    if os.path.exists(src):
        pd.read_csv(src).to_csv(dst, index=False)

print(f"✅ Original files backed up to {backup_dir}/")

# Save filtered files
filtered_stops.to_csv("src/data/gtfs_pruned/stops.txt", index=False)
filtered_stop_times.to_csv("src/data/gtfs_pruned/stop_times.txt", index=False)
filtered_trips.to_csv("src/data/gtfs_pruned/trips.txt", index=False)
filtered_routes.to_csv("src/data/gtfs_pruned/routes.txt", index=False)
filtered_agency.to_csv("src/data/gtfs_pruned/agency.txt", index=False)
filtered_calendar.to_csv("src/data/gtfs_pruned/calendar.txt", index=False)
filtered_calendar_dates.to_csv("src/data/gtfs_pruned/calendar_dates.txt", index=False)

if len(filtered_shapes) > 0:
    filtered_shapes.to_csv("src/data/gtfs_pruned/shapes.txt", index=False)

print("\n✅ Manchester area filtering completed!")
print(f"📊 Summary:")
print(f"  - Stops: {len(stops_df)} → {len(filtered_stops)}")
print(f"  - Stop times: {len(stop_times_df)} → {len(filtered_stop_times)}")
print(f"  - Trips: {len(trips_df)} → {len(filtered_trips)}")
print(f"  - Routes: {len(routes_df)} → {len(filtered_routes)}")
print(f"  - Agencies: {len(agency_df)} → {len(filtered_agency)}")
print(f"  - Calendar: {len(calendar_df)} → {len(filtered_calendar)}")
print(f"  - Calendar dates: {len(calendar_dates_df)} → {len(filtered_calendar_dates)}")
if len(manchester_shape_ids) > 0:
    print(f"  - Shapes: {len(shapes_df)} → {len(filtered_shapes)}")

print("\n=== FILTERING COMPLETE ===")
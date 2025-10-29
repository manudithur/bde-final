#!/usr/bin/env python3
# split_into_segments_mapmatched.py - Generate map-matched route segments from GTFS

import argparse
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from shapely.geometry import LineString, Point
import zipfile
import tempfile
import os
import requests
import time
from typing import List, Tuple, Optional
import json

def load_gtfs_from_zip(zip_path):
    """Load GTFS data from zip file"""
    print("📂 Loading GTFS data from zip file...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Load required GTFS files
        stops = pd.read_csv(os.path.join(temp_dir, 'stops.txt'))
        stop_times = pd.read_csv(os.path.join(temp_dir, 'stop_times.txt'))
        trips = pd.read_csv(os.path.join(temp_dir, 'trips.txt'))
        routes = pd.read_csv(os.path.join(temp_dir, 'routes.txt'))
        calendar = pd.read_csv(os.path.join(temp_dir, 'calendar.txt'))
        
        # Try to load calendar_dates (optional)
        try:
            calendar_dates = pd.read_csv(os.path.join(temp_dir, 'calendar_dates.txt'))
        except FileNotFoundError:
            calendar_dates = pd.DataFrame()
        
        print(f"✅ Loaded: {len(stops)} stops, {len(stop_times)} stop_times, {len(trips)} trips, {len(routes)} routes")
        
        return stops, stop_times, trips, routes, calendar, calendar_dates

def filter_by_date_range(trips, calendar, calendar_dates, start_date, end_date):
    """Filter trips by service date range"""
    print(f"🗓️ Filtering trips for date range: {start_date} to {end_date}")
    
    # Convert dates to integers for comparison
    start_int = int(start_date.replace('-', ''))
    end_int = int(end_date.replace('-', ''))
    
    # Get valid service_ids from calendar
    valid_services = set()
    
    # From calendar.txt
    calendar['start_date'] = pd.to_numeric(calendar['start_date'], errors='coerce')
    calendar['end_date'] = pd.to_numeric(calendar['end_date'], errors='coerce')
    
    active_calendar = calendar[
        (calendar['start_date'] <= end_int) & 
        (calendar['end_date'] >= start_int)
    ]
    valid_services.update(active_calendar['service_id'])
    
    # From calendar_dates.txt (if available)
    if not calendar_dates.empty:
        calendar_dates['date'] = pd.to_numeric(calendar_dates['date'], errors='coerce')
        active_dates = calendar_dates[
            (calendar_dates['date'] >= start_int) & 
            (calendar_dates['date'] <= end_int)
        ]
        valid_services.update(active_dates['service_id'])
    
    # Filter trips
    filtered_trips = trips[trips['service_id'].isin(valid_services)]
    print(f"📊 Filtered trips: {len(filtered_trips)} out of {len(trips)}")
    
    return filtered_trips

def get_osrm_route(start_coord: Tuple[float, float], end_coord: Tuple[float, float], 
                   osrm_server: str = "http://router.project-osrm.org") -> Optional[LineString]:
    """
    Get map-matched route between two coordinates using OSRM
    
    Args:
        start_coord: (lon, lat) tuple for start point
        end_coord: (lon, lat) tuple for end point  
        osrm_server: OSRM server URL
        
    Returns:
        LineString geometry following roads, or None if routing fails
    """
    try:
        # OSRM route API endpoint
        url = f"{osrm_server}/route/v1/driving/{start_coord[0]},{start_coord[1]};{end_coord[0]},{end_coord[1]}"
        params = {
            'overview': 'full',
            'geometries': 'geojson'
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('routes') and len(data['routes']) > 0:
                route = data['routes'][0]
                if 'geometry' in route and 'coordinates' in route['geometry']:
                    coords = route['geometry']['coordinates']
                    if len(coords) >= 2:
                        # Convert coordinates to (lon, lat) tuples for Shapely
                        return LineString(coords)
        
        # Fall back to straight line if routing fails
        return LineString([start_coord, end_coord])
        
    except Exception as e:
        print(f"⚠️ Routing failed for segment, using straight line: {e}")
        return LineString([start_coord, end_coord])

def create_mapmatched_segments(stops, stop_times, filtered_trips, use_map_matching=True, 
                             osrm_server="http://router.project-osrm.org"):
    """Create map-matched route segments from stop-to-stop connections"""
    print("🔗 Creating map-matched segments from stop-to-stop connections...")
    print(f"🗺️ Map matching: {'Enabled' if use_map_matching else 'Disabled (straight lines)'}")
    
    if use_map_matching:
        print(f"🌐 Using OSRM server: {osrm_server}")
        print("⏳ This will take longer due to routing API calls...")
    
    # Create stops lookup
    stops_dict = stops.set_index('stop_id').to_dict('index')
    
    segments = []
    trip_count = 0
    routing_cache = {}  # Cache routes between same stop pairs
    
    for trip_id in filtered_trips['trip_id']:
        trip_count += 1
        if trip_count % 100 == 0:  # Reduced frequency to avoid spam with API calls
            print(f"  Processing trip {trip_count}/{len(filtered_trips)}...")
        
        # Get stop_times for this trip, sorted by stop_sequence
        trip_stops = stop_times[stop_times['trip_id'] == trip_id].sort_values('stop_sequence')
        
        if len(trip_stops) < 2:
            continue
            
        # Get trip info
        trip_info = filtered_trips[filtered_trips['trip_id'] == trip_id].iloc[0]
        
        # Create segments between consecutive stops
        for i in range(len(trip_stops) - 1):
            current_stop = trip_stops.iloc[i]
            next_stop = trip_stops.iloc[i + 1]
            
            current_stop_id = current_stop['stop_id']
            next_stop_id = next_stop['stop_id']
            
            # Get stop coordinates
            if current_stop_id in stops_dict and next_stop_id in stops_dict:
                current_coords = stops_dict[current_stop_id]
                next_coords = stops_dict[next_stop_id]
                
                start_coord = (current_coords['stop_lon'], current_coords['stop_lat'])
                end_coord = (next_coords['stop_lon'], next_coords['stop_lat'])
                
                # Create geometry (map-matched or straight line)
                if use_map_matching:
                    # Check cache first
                    cache_key = f"{current_stop_id}->{next_stop_id}"
                    if cache_key in routing_cache:
                        line = routing_cache[cache_key]
                    else:
                        line = get_osrm_route(start_coord, end_coord, osrm_server)
                        routing_cache[cache_key] = line
                        # Small delay to be nice to the API
                        time.sleep(0.1)
                else:
                    line = LineString([start_coord, end_coord])
                
                if line is None:
                    continue
                
                # Create segment record
                segment = {
                    'trip_id': trip_id,
                    'route_id': trip_info['route_id'],
                    'service_id': trip_info['service_id'],
                    'direction_id': trip_info.get('direction_id', None),
                    'from_stop_id': current_stop_id,
                    'to_stop_id': next_stop_id,
                    'from_stop_name': current_coords['stop_name'],
                    'to_stop_name': next_coords['stop_name'],
                    'stop_sequence_from': current_stop['stop_sequence'],
                    'stop_sequence_to': next_stop['stop_sequence'],
                    'arrival_time': next_stop['arrival_time'],
                    'departure_time': current_stop['departure_time'],
                    'is_map_matched': use_map_matching,
                    'geometry': line
                }
                
                segments.append(segment)
    
    print(f"✅ Created {len(segments)} segments from {trip_count} trips")
    if use_map_matching:
        print(f"🗺️ Used {len(routing_cache)} unique route calculations (cached duplicates)")
    
    # Convert to GeoDataFrame
    segments_gdf = gpd.GeoDataFrame(segments, crs='EPSG:4326')
    
    return segments_gdf

def save_to_postgis(gdf, table_name, db_host, db_port, db_user, db_pass, db_name):
    """Save GeoDataFrame to PostGIS"""
    print(f"💾 Saving {len(gdf)} segments to PostGIS table '{table_name}'...")
    
    # Construct the connection URL
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    
    # Create SQLAlchemy engine
    engine = create_engine(db_url)
    
    # Save to PostGIS
    gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
    
    print(f"✅ Data saved to {table_name} table in the {db_name} database.")

def main():
    parser = argparse.ArgumentParser(description='Generate map-matched route segments from GTFS')
    parser.add_argument('gtfs_zip', help='Path to the GTFS zip file')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD format)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD format)')
    parser.add_argument('--table-name', default='segments', help='PostgreSQL table name (default: segments)')
    parser.add_argument('--db-host', required=True, help='Database host')
    parser.add_argument('--db-port', default='5432', help='Database port (default: 5432)')
    parser.add_argument('--db-user', required=True, help='Database username')
    parser.add_argument('--db-pass', required=True, help='Database password')
    parser.add_argument('--db-name', required=True, help='Database name')
    parser.add_argument('--no-map-matching', action='store_true', help='Disable map matching (use straight lines)')
    parser.add_argument('--osrm-server', default='http://router.project-osrm.org', 
                       help='OSRM server URL (default: public server)')
    
    args = parser.parse_args()
    
    print("=== GTFS MAP-MATCHED SEGMENTS GENERATOR ===")
    print(f"Input: {args.gtfs_zip}")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Output: {args.table_name} table in {args.db_name}")
    print(f"Map matching: {'Disabled' if args.no_map_matching else 'Enabled'}")
    
    try:
        # Load GTFS data
        stops, stop_times, trips, routes, calendar, calendar_dates = load_gtfs_from_zip(args.gtfs_zip)
        
        # Filter by date range
        filtered_trips = filter_by_date_range(trips, calendar, calendar_dates, args.start_date, args.end_date)
        
        # Create segments (map-matched or straight line)
        segments_gdf = create_mapmatched_segments(
            stops, stop_times, filtered_trips, 
            use_map_matching=not args.no_map_matching,
            osrm_server=args.osrm_server
        )
        
        # Save to PostGIS
        save_to_postgis(segments_gdf, args.table_name, args.db_host, args.db_port,
                       args.db_user, args.db_pass, args.db_name)
        
        print("🎉 Map-matched segments generation completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())
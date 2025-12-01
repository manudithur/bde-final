#!/usr/bin/env python3
"""
Route Visualization for Vancouver Transit
Creates multiple visualizations of routes from different perspectives:
- Interactive map showing all routes
- Network density map
- Routes by transport mode
"""

import psycopg2
import pandas as pd
import folium
from folium import plugins
import os
from dotenv import load_dotenv
import json

load_dotenv()

# Output directory (go up 3 levels: visualization/ -> analysis/ -> queries/ -> static_analysis/)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results', 'route_visualization')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

# Vancouver center coordinates
VANCOUVER_CENTER = [49.2827, -123.1207]

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

def fetch_route_data():
    """Fetch route visualization data from database"""
    query = """
    SELECT 
        route_id,
        route_short_name,
        route_long_name,
        mode_name,
        num_trips,
        ST_AsGeoJSON(route_geometry)::json AS geometry
    FROM route_visualization
    WHERE route_geometry IS NOT NULL
    ORDER BY num_trips DESC;
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return rows

def fetch_route_density():
    """Fetch route density data for heatmap"""
    query = """
    SELECT 
        ST_AsGeoJSON(seg_geom)::json AS geometry,
        num_routes
    FROM segment_route_density
    WHERE seg_geom IS NOT NULL
    LIMIT 5000;  -- Limit for performance
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return rows

def create_all_routes_map(route_data):
    """Create interactive map showing all routes"""
    m = folium.Map(location=VANCOUVER_CENTER, zoom_start=11, tiles='OpenStreetMap')
    
    # Color scheme by mode
    mode_colors = {
        'Bus': '#0066CC',
        'Subway': '#CC0000',
        'Rail': '#009900',
        'Ferry': '#006699',
        'Other': '#666666'
    }
    
    # Group routes by mode
    routes_by_mode = {}
    for row in route_data:
        route_id, short_name, long_name, mode_name, num_trips, geom = row
        if mode_name not in routes_by_mode:
            routes_by_mode[mode_name] = []
        routes_by_mode[mode_name].append((route_id, short_name, long_name, num_trips, geom))
    
    # Add routes to map grouped by mode
    for mode_name, routes in routes_by_mode.items():
        color = mode_colors.get(mode_name, '#666666')
        feature_group = folium.FeatureGroup(name=f'{mode_name} ({len(routes)} routes)')
        
        for route_id, short_name, long_name, num_trips, geom in routes:
            if geom and 'coordinates' in geom:
                # Handle both LineString and MultiLineString
                coords = geom['coordinates']
                if geom['type'] == 'MultiLineString':
                    for line in coords:
                        folium.PolyLine(
                            locations=[[lat, lon] for lon, lat in line],
                            color=color,
                            weight=2,
                            opacity=0.6,
                            popup=folium.Popup(
                                f"<b>Route {short_name}</b><br>"
                                f"{long_name}<br>"
                                f"Mode: {mode_name}<br>"
                                f"Trips: {num_trips}",
                                max_width=200
                            )
                        ).add_to(feature_group)
                elif geom['type'] == 'LineString':
                    folium.PolyLine(
                        locations=[[lat, lon] for lon, lat in coords],
                        color=color,
                        weight=2,
                        opacity=0.6,
                        popup=folium.Popup(
                            f"<b>Route {short_name}</b><br>"
                            f"{long_name}<br>"
                            f"Mode: {mode_name}<br>"
                            f"Trips: {num_trips}",
                            max_width=200
                        )
                    ).add_to(feature_group)
        
        feature_group.add_to(m)
    
    folium.LayerControl().add_to(m)
    return m

def create_density_heatmap(density_data):
    """Create heatmap showing route density"""
    m = folium.Map(location=VANCOUVER_CENTER, zoom_start=11, tiles='OpenStreetMap')
    
    # Prepare data for heatmap
    heat_data = []
    for geom, num_routes in density_data:
        if geom and 'coordinates' in geom:
            coords = geom['coordinates']
            if geom['type'] == 'LineString':
                # Use midpoint of segment
                mid_idx = len(coords) // 2
                lon, lat = coords[mid_idx]
                heat_data.append([lat, lon, num_routes])
            elif geom['type'] == 'MultiLineString':
                for line in coords:
                    if line:
                        mid_idx = len(line) // 2
                        lon, lat = line[mid_idx]
                        heat_data.append([lat, lon, num_routes])
    
    if heat_data:
        plugins.HeatMap(heat_data, radius=15, blur=10, max_zoom=1).add_to(m)
    
    return m

def create_mode_comparison_map(route_data):
    """Create map comparing different transport modes"""
    m = folium.Map(location=VANCOUVER_CENTER, zoom_start=11, tiles='OpenStreetMap')
    
    mode_colors = {
        'Bus': '#0066CC',
        'Subway': '#CC0000',
        'Rail': '#009900',
        'Ferry': '#006699',
        'Other': '#666666'
    }
    
    # Create separate layers for each mode
    for mode_name in ['Bus', 'Subway', 'Rail', 'Ferry', 'Other']:
        feature_group = folium.FeatureGroup(name=mode_name)
        color = mode_colors.get(mode_name, '#666666')
        
        for row in route_data:
            route_id, short_name, long_name, route_mode, num_trips, geom = row
            if route_mode == mode_name and geom and 'coordinates' in geom:
                coords = geom['coordinates']
                if geom['type'] == 'LineString':
                    folium.PolyLine(
                        locations=[[lat, lon] for lon, lat in coords],
                        color=color,
                        weight=3,
                        opacity=0.7
                    ).add_to(feature_group)
                elif geom['type'] == 'MultiLineString':
                    for line in coords:
                        folium.PolyLine(
                            locations=[[lat, lon] for lon, lat in line],
                            color=color,
                            weight=3,
                            opacity=0.7
                        ).add_to(feature_group)
        
        feature_group.add_to(m)
    
    folium.LayerControl().add_to(m)
    return m

def main():
    print("Fetching route data...")
    route_data = fetch_route_data()
    print(f"Found {len(route_data)} routes")
    
    if not route_data:
        print("No route data found. Make sure you've run:")
        print("  1. data_loading/mobilitydb_import.sql")
        print("  2. queries/analysis/spatial_queries.sql")
        return
    
    # Create all routes map
    print("Creating all routes map...")
    all_routes_map = create_all_routes_map(route_data)
    output_path = os.path.join(OUTPUT_DIR, 'route_map_all.html')
    all_routes_map.save(output_path)
    print(f"Saved '{output_path}'")
    
    # Create mode comparison map
    print("Creating mode comparison map...")
    mode_map = create_mode_comparison_map(route_data)
    output_path = os.path.join(OUTPUT_DIR, 'route_map_by_mode.html')
    mode_map.save(output_path)
    print(f"Saved '{output_path}'")
    
    # Create density heatmap
    print("Creating density heatmap...")
    density_data = fetch_route_density()
    print(f"Found {len(density_data)} segments for density analysis")
    if density_data:
        density_map = create_density_heatmap(density_data)
        output_path = os.path.join(OUTPUT_DIR, 'route_density_heatmap.html')
        density_map.save(output_path)
        print(f"Saved '{output_path}'")
    
    print("\nAll visualizations created successfully!")

if __name__ == "__main__":
    main()


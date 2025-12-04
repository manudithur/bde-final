#!/usr/bin/env python3
"""
Stadium Transit Access Analysis for Vancouver Transit
Analyzes which stadiums have best/worst access to public transit
"""

import psycopg2
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import contextily as ctx
import folium
import os
import warnings
from dotenv import load_dotenv

# Suppress pandas SQLAlchemy warning for psycopg2 connections
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

load_dotenv()

# Output directory (go up 3 levels: visualization/ -> analysis/ -> queries/ -> static_analysis/)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results', 'stadium_proximity')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

def fetch_stadium_transit_access():
    """Fetch stadium transit access metrics from materialized view - BUS routes only"""
    # Using materialized view created by qgis_queries/08_stadium_proximity.sql
    query = """
    WITH stadium_stops AS (
        SELECT 
            stadium_name,
            team,
            stop_id,
            distance_m
        FROM qgis_stadium_proximity
    )
    SELECT 
        ss.stadium_name,
        ss.team,
        COUNT(DISTINCT ss.stop_id) AS stops_600m,
        COUNT(DISTINCT t.route_id) AS unique_routes_600m,
        0 AS skytrain_routes,
        COUNT(DISTINCT t.route_id) AS bus_routes,
        9999 AS nearest_skytrain_distance_m,
        'N/A' AS nearest_skytrain_station,
        COUNT(DISTINCT stt.trip_id) AS trips_per_day,
        MIN(ss.distance_m) AS nearest_stop_distance_m
    FROM stadium_stops ss
    JOIN stop_times stt ON ss.stop_id = stt.stop_id
    JOIN trips t ON stt.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    WHERE r.route_type = '3'
    GROUP BY ss.stadium_name, ss.team
    ORDER BY trips_per_day DESC;
    """

    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            print("⚠️  qgis_stadium_proximity view does not exist.")
            print("   Make sure you've run qgis_queries/run_sql.py first.")
            df = pd.DataFrame()
        else:
            raise
    finally:
        conn.close()
    return df

def plot_stops_and_routes(df):
    """Plot stops and routes within 600m"""
    if df.empty:
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    x = range(len(df))
    width = 0.35
    ax.bar([i - width/2 for i in x], df['stops_600m'], width, 
            label='Stops (600m)', color='#2ecc71', alpha=0.8)
    ax.bar([i + width/2 for i in x], df['unique_routes_600m'], width, 
            label='Routes', color='#3498db', alpha=0.8)
    ax.set_ylabel('Count')
    ax.set_title('BUS Stops & Routes Within 600m of Stadiums', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(df['stadium_name'], rotation=15, ha='right', fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'stadium_stops_and_routes.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def plot_trips_per_day(df):
    """Plot daily trip frequency"""
    if df.empty:
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['stadium_name'], df['trips_per_day'], color='#e74c3c', alpha=0.8)
    ax.set_xlabel('Trips Per Day')
    ax.set_title('Daily BUS Trip Frequency (Within 600m)', fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    for i, (idx, row) in enumerate(df.iterrows()):
        if row['trips_per_day'] > 0:
            ax.text(row['trips_per_day'] + max(df['trips_per_day']) * 0.02, i, 
                    f"{int(row['trips_per_day']):,}", va='center', fontsize=9)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'stadium_trips_per_day.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def fetch_stadiums_and_nearby_stops():
    """Fetch stadiums and stops from materialized view - BUS routes only"""
    # Using materialized view created by qgis_queries/08_stadium_proximity.sql
    query = """
    WITH stadium_stops AS (
        SELECT 
            stadium_name,
            team,
            stop_id,
            stop_name,
            distance_m
        FROM qgis_stadium_proximity
    ),
    stop_stadium_count AS (
        SELECT 
            stop_id,
            COUNT(DISTINCT stadium_name) AS stadium_count
        FROM stadium_stops
        GROUP BY stop_id
    )
    SELECT 
        ss.*,
        ssc.stadium_count
    FROM stadium_stops ss
    JOIN stop_stadium_count ssc ON ss.stop_id = ssc.stop_id
    ORDER BY ss.stadium_name, ss.distance_m;
    """
    
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            print("⚠️  qgis_stadium_proximity view does not exist.")
            print("   Make sure you've run qgis_queries/run_sql.py successfully.")
            df = pd.DataFrame()
        else:
            raise
    finally:
        conn.close()
    return df

def fetch_stadiums_gdf():
    """Fetch stadiums as GeoDataFrame"""
    query = """
    SELECT 
        name AS stadium_name,
        team,
        latitude,
        longitude,
        geom
    FROM football_stadiums;
    """
    
    conn = get_db_connection()
    try:
        gdf = gpd.read_postgis(query, conn, geom_col="geom")
    except Exception as e:
        print(f"Error fetching stadiums: {e}")
        gdf = gpd.GeoDataFrame(columns=["stadium_name", "team", "latitude", "longitude", "geom"], geometry="geom")
    finally:
        conn.close()
    
    if gdf.empty:
        return gdf
    
    gdf = gdf.set_geometry("geom")
    gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    return gdf

def fetch_stops_near_stadiums_gdf():
    """Fetch stops within 600m of stadiums as GeoDataFrame - BUS routes only"""
    query = """
    WITH stadium_stops AS (
        SELECT DISTINCT
            s.name AS stadium_name,
            st.stop_id,
            st.stop_name,
            st.stop_loc::geometry AS stop_geom,
            ST_DistanceSphere(s.geom, st.stop_loc::geometry) AS distance_m
        FROM football_stadiums s
        CROSS JOIN stops st
        WHERE ST_DistanceSphere(s.geom, st.stop_loc::geometry) <= 600
            AND EXISTS (
                SELECT 1 
                FROM stop_times stt
                JOIN trips t ON stt.trip_id = t.trip_id
                JOIN routes r ON t.route_id = r.route_id
                WHERE stt.stop_id = st.stop_id
                    AND r.route_type = '3'
            )
    )
    SELECT 
        stadium_name,
        stop_id,
        stop_name,
        distance_m,
        stop_geom
    FROM stadium_stops;
    """
    
    conn = get_db_connection()
    try:
        gdf = gpd.read_postgis(query, conn, geom_col="stop_geom")
    except Exception as e:
        print(f"Error fetching stops: {e}")
        gdf = gpd.GeoDataFrame(columns=["stadium_name", "stop_id", "stop_name", "distance_m", "stop_geom"], geometry="stop_geom")
    finally:
        conn.close()
    
    if gdf.empty:
        return gdf
    
    gdf = gdf.set_geometry("stop_geom")
    gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    return gdf

# Map generation removed - maps are created manually in QGIS using qgis_queries/08_stadium_proximity.sql
def plot_stadium_proximity_map_removed():
    """Create geographic map showing stadiums and nearby BUS stops"""
    print("Fetching stadium and stop data for map...")
    gdf_stadiums = fetch_stadiums_gdf()
    gdf_stops = fetch_stops_near_stadiums_gdf()
    
    if gdf_stadiums.empty:
        print("No stadium data available for map")
        return
    
    # Reproject to Web Mercator for basemap compatibility
    gdf_stadiums_mercator = gdf_stadiums.to_crs(epsg=3857)
    
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Plot stops first (so they appear behind stadiums)
    if not gdf_stops.empty:
        gdf_stops_mercator = gdf_stops.to_crs(epsg=3857)
        # Color stops by distance
        gdf_stops_mercator.plot(
            ax=ax,
            column="distance_m",
            cmap="YlGnBu",
            markersize=30,
            alpha=0.7,
            legend=True,
            legend_kwds={"label": "Distance to Stadium (m)", "shrink": 0.8},
        )
    
    # Plot stadiums on top
    gdf_stadiums_mercator.plot(
        ax=ax,
        color="red",
        markersize=200,
        marker="*",
        edgecolor="black",
        linewidth=2,
        alpha=0.9,
        label="Stadiums"
    )
    
    # Add basemap
    try:
        ctx.add_basemap(ax, crs=gdf_stadiums_mercator.crs, source=ctx.providers.CartoDB.Positron)
    except Exception as e:
        print(f"Warning: Could not add basemap: {e}")
    
    # Add stadium labels
    for idx, row in gdf_stadiums_mercator.iterrows():
        ax.annotate(
            row['stadium_name'],
            xy=(row.geom.x, row.geom.y),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=10,
            fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7)
        )
    
    ax.set_title(
        "Stadium Proximity Map - BUS Stops Within 600m",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.axis("off")
    ax.legend(loc="upper right")
    
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'stadium_proximity_map.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', format='png')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def create_stadium_stops_map():
    """Create interactive HTML map showing stadiums and nearby stops"""
    print("Fetching stadium and stop data...")
    df = fetch_stadiums_and_nearby_stops()
    
    if df.empty:
        print("No data available for map")
        return
    
    # Get unique stadiums
    stadiums = df[['stadium_name', 'team', 'latitude', 'longitude']].drop_duplicates()
    
    # Calculate center of map (average of all stadiums)
    center_lat = stadiums['latitude'].mean()
    center_lon = stadiums['longitude'].mean()
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=12,
        tiles='OpenStreetMap'
    )
    
    # Color palette for different stadiums
    colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred', 'beige', 
              'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'white', 'pink', 'lightblue', 
              'lightgreen', 'gray', 'black', 'lightgray']
    
    # Track which stops we've already added (to avoid duplicates)
    added_stops = set()
    
    # Add stops grouped by stadium
    for idx, (_, stadium_data) in enumerate(stadiums.iterrows()):
        color = colors[idx % len(colors)]
        
        # Get stops for this stadium
        stadium_stops = df[df['stadium_name'] == stadium_data['stadium_name']]
        
        # Add stadium marker
        folium.Marker(
            [stadium_data['latitude'], stadium_data['longitude']],
            popup=folium.Popup(
                f"<b>{stadium_data['stadium_name']}</b><br>"
                f"Team: {stadium_data['team']}<br>"
                f"Stops within 600m: {len(stadium_stops)}",
                max_width=200
            ),
            tooltip=stadium_data['stadium_name'],
            icon=folium.Icon(color=color, icon='star', prefix='fa')
        ).add_to(m)
        
        # Add stops for this stadium
        for _, stop in stadium_stops.iterrows():
            stop_key = (stop['stop_id'], stop['stop_lat'], stop['stop_lon'])
            
            # If stop is near multiple stadiums, use special styling
            if stop['stadium_count'] > 1:
                # Use orange/red for shared stops
                stop_color = 'orange'
                stop_radius = 6
                stop_weight = 2
                popup_text = f"<b>{stop['stop_name']}</b><br>"
                popup_text += f"Stop ID: {stop['stop_id']}<br>"
                popup_text += f"Distance: {stop['distance_m']:.0f}m from {stadium_data['stadium_name']}<br>"
                popup_text += f"<b>Near {int(stop['stadium_count'])} stadiums</b>"
                tooltip_text = f"{stop['stop_name']} ({stop['distance_m']:.0f}m) - Near {int(stop['stadium_count'])} stadiums"
            else:
                # Use stadium color for unique stops
                stop_color = color
                stop_radius = 4
                stop_weight = 1
                popup_text = f"<b>{stop['stop_name']}</b><br>"
                popup_text += f"Stop ID: {stop['stop_id']}<br>"
                popup_text += f"Distance: {stop['distance_m']:.0f}m from {stadium_data['stadium_name']}"
                tooltip_text = f"{stop['stop_name']} ({stop['distance_m']:.0f}m)"
            
            # Only add stop once (if near multiple stadiums, show it with shared styling)
            if stop_key not in added_stops:
                folium.CircleMarker(
                    [stop['stop_lat'], stop['stop_lon']],
                    radius=stop_radius,
                    popup=folium.Popup(popup_text, max_width=200),
                    tooltip=tooltip_text,
                    color=stop_color,
                    fillColor=stop_color,
                    fillOpacity=0.7,
                    weight=stop_weight
                ).add_to(m)
                added_stops.add(stop_key)
    
    # Add legend
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 220px; height: auto; 
                background-color: white; z-index:9999; font-size:14px;
                border:2px solid grey; padding: 10px">
    <p><b>Legend</b></p>
    <p><i class="fa fa-star" style="color:red"></i> Stadium</p>
    <p><span style="color:blue">●</span> Stop within 600m (unique)</p>
    <p><span style="color:orange">●</span> Stop near multiple stadiums</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Save map
    output_path = os.path.join(OUTPUT_DIR, 'stadium_stops_map.html')
    m.save(output_path)
    print(f"Map saved as '{output_path}'")

def print_access_summary(df):
    """Print detailed summary of transit access for each stadium"""
    print("\n" + "="*80)
    print("STADIUM TRANSIT ACCESS SUMMARY")
    print("="*80)
    
    for idx, row in df.iterrows():
        print(f"\n🏟️  {row['stadium_name']} ({row['team']})")
        print(f"   {'─'*70}")
        print(f"   Stops within 600m:        {int(row['stops_600m'])}")
        print(f"   Unique routes (600m):     {int(row['unique_routes_600m'])}")
        print(f"     - SkyTrain routes:      {int(row['skytrain_routes'])}")
        print(f"     - Bus routes:           {int(row['bus_routes'])}")
        print(f"   Daily trips:              {int(row['trips_per_day']):,}")
        print(f"   Nearest stop:             {row['nearest_stop_distance_m']:.0f}m")
        print(f"   Nearest SkyTrain:         {row['nearest_skytrain_station']}")
        if row['nearest_skytrain_distance_m'] < 9999:
            print(f"     Distance:              {row['nearest_skytrain_distance_m']:.0f}m")
    
    # Ranking by daily trips
    print("\n" + "="*80)
    print("RANKING BY DAILY TRIP FREQUENCY")
    print("="*80)
    
    df_rank = df.sort_values('trips_per_day', ascending=False)
    
    for i, (idx, row) in enumerate(df_rank.iterrows(), 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        print(f"{medal} {row['stadium_name']:20s} - {int(row['trips_per_day']):,} trips/day")
        print(f"   ({row['stops_600m']} stops, {row['unique_routes_600m']} routes within 600m)")
    
    print("\n" + "="*80)
    print("\nNOTE: 'Daily Transit Trip Frequency' counts how many unique scheduled")
    print("transit trips (buses, trains, etc.) pass through stops within 600m")
    print("of each stadium during a typical day. Higher numbers indicate better")
    print("transit service frequency and access.\n")

def main():
    print("Fetching stadium transit access data...")
    df = fetch_stadium_transit_access()
    
    if df.empty:
        print("⚠️  No stadium transit access data found.")
        print("   Make sure you've run:")
        print("  1. data_loading/mobilitydb_import.sql")
        print("  2. qgis_queries/run_sql.py (or static_analysis/queries/run_all_analyses.py)")
        return
    
    print(f"Found data for {len(df)} stadiums")
    
    print_access_summary(df)
    
    print("\nGenerating graph visualizations...")
    print("Note: Map visualizations are created manually in QGIS using qgis_queries/08_stadium_proximity.sql and 09_stadiums.sql")
    plot_stops_and_routes(df)
    plot_trips_per_day(df)
    
    print("\n✓ Analysis complete!")
    print("  Use QGIS with qgis_queries/08_stadium_proximity.sql and 09_stadiums.sql for map visualizations")

if __name__ == "__main__":
    main()

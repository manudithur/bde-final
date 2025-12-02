#!/usr/bin/env python3
"""
Stadium Transit Access Analysis for Vancouver Transit
Analyzes which stadiums have best/worst access to public transit
"""

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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
    """Fetch stadium transit access metrics"""
query = """
    SELECT 
        stadium_name,
        team,
        stops_500m,
        unique_routes_500m,
        skytrain_routes,
        bus_routes,
        nearest_skytrain_distance_m,
        nearest_skytrain_station,
        trips_per_day,
        nearest_stop_distance_m
    FROM stadium_transit_access
    ORDER BY trips_per_day DESC;
"""

    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            print("⚠️  stadium_transit_access view does not exist.")
            print("   Make sure you've run queries/analysis/spatial_queries.sql successfully.")
            df = pd.DataFrame()
        else:
            raise
    finally:
    conn.close()
    return df

def create_transit_access_comparison(df):
    """Create clean comparison chart of stadium transit access"""
    if df.empty:
        print("No data available for visualization")
        return
    
    # Create simple 1x2 layout (2 charts side by side)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Stadium Transit Access Comparison - Vancouver', fontsize=16, fontweight='bold')
    
    # 1. Stops and Routes (combined)
    ax1 = axes[0]
    x = range(len(df))
    width = 0.35
    ax1.bar([i - width/2 for i in x], df['stops_500m'], width, 
            label='Stops (500m)', color='#2ecc71', alpha=0.8)
    ax1.bar([i + width/2 for i in x], df['unique_routes_500m'], width, 
            label='Routes', color='#3498db', alpha=0.8)
    ax1.set_ylabel('Count')
    ax1.set_title('Stops & Routes Within 500m', fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(df['stadium_name'], rotation=15, ha='right', fontsize=10)
    ax1.legend(fontsize=9)
    ax1.grid(axis='y', alpha=0.3)
    
    # 2. Daily Trip Frequency
    ax2 = axes[1]
    bars2 = ax2.barh(df['stadium_name'], df['trips_per_day'], color='#e74c3c', alpha=0.8)
    ax2.set_xlabel('Trips Per Day')
    ax2.set_title('Daily Transit Trip Frequency (Within 500m)', fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    for i, (idx, row) in enumerate(df.iterrows()):
        if row['trips_per_day'] > 0:
            ax2.text(row['trips_per_day'] + max(df['trips_per_day']) * 0.02, i, 
                    f"{int(row['trips_per_day']):,}", va='center', fontsize=9)
    
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'stadium_proximity_analysis.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Chart saved as '{output_path}'")
    plt.close()

def fetch_stadiums_and_nearby_stops():
    """Fetch stadiums and stops within 500m, including stops near multiple stadiums"""
    query = """
    WITH stadium_stops AS (
        SELECT 
            s.name AS stadium_name,
            s.team,
            s.latitude,
            s.longitude,
            st.stop_id,
            st.stop_name,
            ST_Y(st.stop_loc::geometry) AS stop_lat,
            ST_X(st.stop_loc::geometry) AS stop_lon,
            ST_DistanceSphere(s.geom, st.stop_loc::geometry) AS distance_m
        FROM football_stadiums s
        CROSS JOIN stops st
        WHERE ST_DistanceSphere(s.geom, st.stop_loc::geometry) <= 500
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
            print("⚠️  Required tables do not exist.")
            print("   Make sure you've run queries/analysis/spatial_queries.sql successfully.")
            df = pd.DataFrame()
        else:
            raise
    finally:
        conn.close()
    return df

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
                f"Stops within 500m: {len(stadium_stops)}",
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
    <p><span style="color:blue">●</span> Stop within 500m (unique)</p>
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
        print(f"   Stops within 500m:        {int(row['stops_500m'])}")
        print(f"   Unique routes (500m):     {int(row['unique_routes_500m'])}")
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
        print(f"   ({row['stops_500m']} stops, {row['unique_routes_500m']} routes within 500m)")
    
    print("\n" + "="*80)
    print("\nNOTE: 'Daily Transit Trip Frequency' counts how many unique scheduled")
    print("transit trips (buses, trains, etc.) pass through stops within 500m")
    print("of each stadium during a typical day. Higher numbers indicate better")
    print("transit service frequency and access.\n")

def main():
    print("Fetching stadium transit access data...")
    df = fetch_stadium_transit_access()
    
    if df.empty:
        print("⚠️  No stadium transit access data found.")
        print("   Make sure you've run:")
        print("  1. data_loading/mobilitydb_import.sql")
        print("  2. queries/analysis/spatial_queries.sql")
        return
    
    print(f"Found data for {len(df)} stadiums")
    
    print_access_summary(df)
    
    print("\nGenerating visualizations...")
    create_transit_access_comparison(df)
    create_stadium_stops_map()
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()

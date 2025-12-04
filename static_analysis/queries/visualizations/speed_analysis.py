#!/usr/bin/env python3
"""
Speed Analysis for Vancouver Transit
Analyzes vehicle speeds and identifies sections with high planned speeds
"""

import psycopg2
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import contextily as ctx
import os
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results', 'speed_analysis')
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

def fetch_speed_stats():
    """Fetch route speed statistics from materialized view - BUS routes only"""
    # Using materialized view created by qgis_queries/05_speed_segments.sql
    query = """
    SELECT 
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        COUNT(*) AS num_segments,
        AVG(qs.speed_kmh) AS avg_speed_kmh,
        MIN(qs.speed_kmh) AS min_speed_kmh,
        MAX(qs.speed_kmh) AS max_speed_kmh,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY qs.speed_kmh) AS median_speed_kmh
    FROM qgis_speed_segments qs
    JOIN routes r ON qs.route_id = r.route_id
    WHERE qs.speed_kmh IS NOT NULL
    GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type
    ORDER BY avg_speed_kmh DESC;
    """
    
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            print("⚠️  qgis_speed_segments view does not exist.")
            print("   Make sure you've run qgis_queries/run_sql.py first.")
            df = pd.DataFrame()
        else:
            raise
    finally:
        conn.close()
    return df

def fetch_high_speed_segments():
    """Fetch segments with unusually high speeds from qgis_speed_segments."""
    query = """
    SELECT 
        qs.route_id,
        r.route_short_name,
        qs.speed_kmh,
        COUNT(*) AS segment_count
    FROM qgis_speed_segments qs
    JOIN routes r ON qs.route_id = r.route_id
    WHERE qs.speed_kmh > 60
    GROUP BY qs.route_id, r.route_short_name, qs.speed_kmh
    HAVING COUNT(*) >= 3
    ORDER BY qs.speed_kmh DESC;
    """
    
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            print("⚠️  qgis_speed_segments view does not exist.")
            print("   Make sure you've run qgis_queries/run_sql.py first.")
            df = pd.DataFrame()
        else:
            raise
    finally:
        conn.close()
    return df

def fetch_schedule_speeds():
    """Fetch all segment speeds from materialized view for distribution analysis"""
    # Using materialized view created by qgis_queries/05_speed_segments.sql
    query = """
    SELECT speed_kmh
    FROM qgis_speed_segments
    WHERE speed_kmh IS NOT NULL;
    """
    
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            print("⚠️  qgis_speed_segments view does not exist.")
            print("   Make sure you've run qgis_queries/run_sql.py first.")
            df = pd.DataFrame()
        else:
            raise
    finally:
        conn.close()
    return df

# Geometry fetching removed - maps are created manually in QGIS using:
# - qgis_queries/05_speed_segments.sql
# - qgis_queries/06_speed_highest.sql
# - qgis_queries/07_speed_slowest.sql

def plot_speed_histogram(df_speeds):
    """Plot histogram of speed distribution"""
    if df_speeds.empty:
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(df_speeds['speed_kmh'], bins=50, color='#3794eb', edgecolor='black', alpha=0.7)
    ax.axvline(df_speeds['speed_kmh'].mean(), color='red', linestyle='--', 
              linewidth=2, label=f"Mean: {df_speeds['speed_kmh'].mean():.1f} km/h")
    ax.axvline(df_speeds['speed_kmh'].median(), color='green', linestyle='--', 
              linewidth=2, label=f"Median: {df_speeds['speed_kmh'].median():.1f} km/h")
    ax.set_xlabel('Speed (km/h)')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of BUS Segment Speeds')
    ax.legend()
    ax.grid(axis='y', alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'speed_distribution_histogram.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def plot_speed_by_mode(df_stats):
    """Plot box plot of speeds by transport mode"""
    if df_stats.empty:
        return
    
    route_type_map = {
        '0': 'Streetcar', '1': 'Subway', '2': 'Rail', '3': 'Bus',
        '4': 'Ferry', '5': 'Cable', '6': 'Aerial', '7': 'Funicular'
    }
    df_stats['mode_name'] = df_stats['route_type'].map(route_type_map).fillna('Other')
    
    bus_speeds = df_stats[df_stats['mode_name'] == 'Bus']['avg_speed_kmh']
    subway_speeds = df_stats[df_stats['mode_name'] == 'Subway']['avg_speed_kmh']
    rail_speeds = df_stats[df_stats['mode_name'] == 'Rail']['avg_speed_kmh']
    
    data_to_plot = []
    labels = []
    if not bus_speeds.empty:
        data_to_plot.append(bus_speeds)
        labels.append('Bus')
    if not subway_speeds.empty:
        data_to_plot.append(subway_speeds)
        labels.append('Subway')
    if not rail_speeds.empty:
        data_to_plot.append(rail_speeds)
        labels.append('Rail')
    
    if not data_to_plot:
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.boxplot(data_to_plot, labels=labels)
    ax.set_ylabel('Average Speed (km/h)')
    ax.set_title('BUS Speed Distribution')
    ax.grid(axis='y', alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'speed_by_mode.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def plot_top_speed_routes(df_stats):
    """Plot top routes by average speed"""
    if df_stats.empty:
        return
    
    fig, ax = plt.subplots(figsize=(10, 8))
    top_routes = df_stats.head(20)
    ax.barh(range(len(top_routes)), top_routes['avg_speed_kmh'], color='#cc0000')
    ax.set_yticks(range(len(top_routes)))
    ax.set_yticklabels([f"{row['route_short_name'] or row['route_id']}" 
                         for _, row in top_routes.iterrows()], fontsize=9)
    ax.set_xlabel('Average Speed (km/h)')
    ax.set_title('Top 20 BUS Routes by Average Speed')
    ax.grid(axis='x', alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'speed_top_routes.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def plot_speed_vs_segments(df_stats):
    """Plot scatter of speed vs number of segments"""
    if df_stats.empty or 'num_segments' not in df_stats.columns:
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(df_stats['num_segments'], df_stats['avg_speed_kmh'], 
              alpha=0.5, color='#0066cc', s=50)
    ax.set_xlabel('Number of Segments')
    ax.set_ylabel('Average Speed (km/h)')
    ax.set_title('BUS Speed vs Route Length (Number of Segments)')
    ax.grid(alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'speed_vs_segments.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def plot_high_speed_routes(df_high):
    """Plot routes with high speed segments"""
    if df_high.empty:
        print("No high-speed segments found")
        return
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    top_routes = df_high.head(20)
    bars = ax.barh(range(len(top_routes)), top_routes['speed_kmh'], color='#ff6b6b')
    ax.set_yticks(range(len(top_routes)))
    ax.set_yticklabels([f"{row['route_short_name'] or row['route_id']} ({row['route_type']})" 
                         for _, row in top_routes.iterrows()], fontsize=9)
    ax.set_xlabel('Average Speed (km/h)', fontsize=12)
    ax.set_title('BUS Routes with High-Speed Segments (>60 km/h)', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.5)
    
    # Add value labels on bars
    for i, (idx, row) in enumerate(top_routes.iterrows()):
        ax.text(row['speed_kmh'] + 1, i, f"{row['speed_kmh']:.1f}", 
               va='center', fontsize=8)
    
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'high_speed_routes.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', format='png')
    print(f"Saved '{output_path}'")
    plt.close(fig)

# Map generation functions removed - maps are created manually in QGIS

def print_speed_statistics(df_stats, df_speeds, df_high):
    """Print speed statistics"""
    print("\n" + "="*60)
    print("SPEED ANALYSIS SUMMARY")
    print("="*60)
    
    if not df_speeds.empty:
        print(f"\nOverall Speed Statistics:")
        print(f"  Total segments analyzed: {len(df_speeds)}")
        print(f"  Mean speed: {df_speeds['speed_kmh'].mean():.2f} km/h")
        print(f"  Median speed: {df_speeds['speed_kmh'].median():.2f} km/h")
        print(f"  Standard deviation: {df_speeds['speed_kmh'].std():.2f} km/h")
        print(f"  Min speed: {df_speeds['speed_kmh'].min():.2f} km/h")
        print(f"  Max speed: {df_speeds['speed_kmh'].max():.2f} km/h")
        
        # Speed categories
        slow = len(df_speeds[df_speeds['speed_kmh'] < 20])
        medium = len(df_speeds[(df_speeds['speed_kmh'] >= 20) & (df_speeds['speed_kmh'] < 40)])
        fast = len(df_speeds[(df_speeds['speed_kmh'] >= 40) & (df_speeds['speed_kmh'] < 60)])
        very_fast = len(df_speeds[df_speeds['speed_kmh'] >= 60])
        
        print(f"\nSpeed Categories:")
        print(f"  < 20 km/h (slow): {slow} segments ({slow/len(df_speeds)*100:.1f}%)")
        print(f"  20-40 km/h (medium): {medium} segments ({medium/len(df_speeds)*100:.1f}%)")
        print(f"  40-60 km/h (fast): {fast} segments ({fast/len(df_speeds)*100:.1f}%)")
        print(f"  >= 60 km/h (very fast): {very_fast} segments ({very_fast/len(df_speeds)*100:.1f}%)")
    
    if not df_stats.empty:
        print(f"\nRoute-level Statistics:")
        print(f"  Total routes analyzed: {len(df_stats)}")
        print(f"  Average route speed: {df_stats['avg_speed_kmh'].mean():.2f} km/h")
        
        # By mode
        if 'route_type' in df_stats.columns:
            bus_stats = df_stats[df_stats['route_type'] == '3']
            if not bus_stats.empty:
                print(f"\n  Bus routes: {len(bus_stats)} routes, avg speed: {bus_stats['avg_speed_kmh'].mean():.2f} km/h")
    
    if not df_high.empty:
        print(f"\nHigh-Speed Segments (>60 km/h):")
        print(f"  Routes with high-speed segments: {len(df_high)}")
        print(f"  Highest average speed: {df_high['speed_kmh'].max():.2f} km/h")
        print(f"\n  Top 5 routes with high-speed segments:")
        for idx, row in df_high.head(5).iterrows():
            print(f"    {row['route_short_name'] or row['route_id']}: {row['speed_kmh']:.2f} km/h ({row['segment_count']} segments)")
    
    print("\n" + "="*60)

def main():
    print("Fetching speed data...")
    df_stats = fetch_speed_stats()
    df_speeds = fetch_schedule_speeds()
    df_high = fetch_high_speed_segments()
    
    if df_stats.empty and df_speeds.empty:
        print("⚠️  No speed data found.")
        if df_stats.empty:
            print("   qgis_speed_segments view does not exist - make sure qgis_queries/run_sql.py ran successfully")
        if df_speeds.empty:
            print("   qgis_speed_segments view does not exist - make sure qgis_queries/run_sql.py ran successfully")
        print("\n   Make sure you've run:")
        print("  1. data_loading/mobilitydb_import.sql")
        print("  2. qgis_queries/run_sql.py (or static_analysis/queries/run_all_analyses.py)")
        return
    
    print_speed_statistics(df_stats, df_speeds, df_high)
    
    print("\nGenerating visualizations...")
    if not df_speeds.empty:
        plot_speed_histogram(df_speeds)
    if not df_stats.empty:
        plot_speed_by_mode(df_stats)
        plot_top_speed_routes(df_stats)
        plot_speed_vs_segments(df_stats)
    if not df_high.empty:
        plot_high_speed_routes(df_high)
    
    # Map generation removed - maps are created manually in QGIS using:
    # - qgis_queries/05_speed_segments.sql
    # - qgis_queries/06_speed_highest.sql
    # - qgis_queries/07_speed_slowest.sql
    
    # Map generation removed - maps are created manually in QGIS using:
    # - qgis_queries/05_speed_segments.sql
    # - qgis_queries/06_speed_highest.sql
    # - qgis_queries/07_speed_slowest.sql
    
    print("\n✓ Analysis complete!")
    print("  Use QGIS with qgis_queries/05-07_speed*.sql for map visualizations")

if __name__ == "__main__":
    main()


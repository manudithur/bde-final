#!/usr/bin/env python3
"""
Speed Analysis for Vancouver Transit
Analyzes vehicle speeds and identifies sections with high planned speeds
"""

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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
    """Fetch route speed statistics"""
    query = """
    SELECT 
        route_id,
        route_short_name,
        route_long_name,
        route_type,
        num_trips,
        num_segments,
        avg_speed_kmh,
        min_speed_kmh,
        max_speed_kmh,
        median_speed_kmh
    FROM route_speed_stats
    WHERE avg_speed_kmh IS NOT NULL
    ORDER BY avg_speed_kmh DESC;
    """
    
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            print("⚠️  route_speed_stats view does not exist.")
            print("   Make sure you've run queries/analysis/spatial_queries.sql successfully.")
            df = pd.DataFrame()
        else:
            raise
    finally:
        conn.close()
    return df

def fetch_high_speed_segments():
    """Fetch segments with unusually high speeds"""
    query = """
    SELECT 
        route_id,
        route_short_name,
        route_type,
        speed_kmh,
        segment_count
    FROM high_speed_segments
    ORDER BY speed_kmh DESC;
    """
    
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            df = pd.DataFrame()
        else:
            raise
    finally:
        conn.close()
    return df

def fetch_schedule_speeds():
    """Fetch all segment speeds for distribution analysis"""
    query = """
    SELECT speed_kmh
    FROM schedule_speeds
    WHERE speed_kmh IS NOT NULL
    AND speed_kmh > 0
    AND speed_kmh < 150;  -- Filter outliers
    """
    
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            df = pd.DataFrame()
        else:
            raise
    finally:
        conn.close()
    return df

def plot_speed_distribution(df_speeds, df_stats=None):
    """Plot distribution of speeds"""
    if df_stats is None:
        df_stats = fetch_speed_stats()
    
    has_stats = not df_stats.empty
    
    # If no stats available, only show histogram
    if not has_stats:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(df_speeds['speed_kmh'], bins=50, color='#3794eb', edgecolor='black', alpha=0.7)
        ax.axvline(df_speeds['speed_kmh'].mean(), color='red', linestyle='--', 
                  linewidth=2, label=f"Mean: {df_speeds['speed_kmh'].mean():.1f} km/h")
        ax.axvline(df_speeds['speed_kmh'].median(), color='green', linestyle='--', 
                  linewidth=2, label=f"Median: {df_speeds['speed_kmh'].median():.1f} km/h")
        ax.set_xlabel('Speed (km/h)')
        ax.set_ylabel('Frequency')
        ax.set_title('Distribution of Segment Speeds')
        ax.legend()
        ax.grid(axis='y', alpha=0.5)
        plt.tight_layout()
        output_path = os.path.join(OUTPUT_DIR, 'speed_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Saved '{output_path}'")
        plt.close()
        return
    
    # Check what data we have for additional plots
    route_type_map = {
        '0': 'Streetcar', '1': 'Subway', '2': 'Rail', '3': 'Bus',
        '4': 'Ferry', '5': 'Cable', '6': 'Aerial', '7': 'Funicular'
    }
    df_stats['mode_name'] = df_stats['route_type'].map(route_type_map).fillna('Other')
    
    bus_speeds = df_stats[df_stats['mode_name'] == 'Bus']['avg_speed_kmh']
    subway_speeds = df_stats[df_stats['mode_name'] == 'Subway']['avg_speed_kmh']
    rail_speeds = df_stats[df_stats['mode_name'] == 'Rail']['avg_speed_kmh']
    
    has_box_plot = not (bus_speeds.empty and subway_speeds.empty and rail_speeds.empty)
    has_top_routes = len(df_stats) > 0
    has_scatter = len(df_stats) > 0 and 'num_segments' in df_stats.columns
    
    # Count how many plots we'll create
    num_plots = 1 + sum([has_box_plot, has_top_routes, has_scatter])
    
    # Create appropriate layout
    if num_plots == 1:
        # Only histogram
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(df_speeds['speed_kmh'], bins=50, color='#3794eb', edgecolor='black', alpha=0.7)
        ax.axvline(df_speeds['speed_kmh'].mean(), color='red', linestyle='--', 
                  linewidth=2, label=f"Mean: {df_speeds['speed_kmh'].mean():.1f} km/h")
        ax.axvline(df_speeds['speed_kmh'].median(), color='green', linestyle='--', 
                  linewidth=2, label=f"Median: {df_speeds['speed_kmh'].median():.1f} km/h")
        ax.set_xlabel('Speed (km/h)')
        ax.set_ylabel('Frequency')
        ax.set_title('Distribution of Segment Speeds')
        ax.legend()
        ax.grid(axis='y', alpha=0.5)
    else:
        # Multiple plots - use 2x2 grid and hide unused subplots
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Histogram of all speeds
        axes[0, 0].hist(df_speeds['speed_kmh'], bins=50, color='#3794eb', edgecolor='black', alpha=0.7)
        axes[0, 0].axvline(df_speeds['speed_kmh'].mean(), color='red', linestyle='--', 
                          linewidth=2, label=f"Mean: {df_speeds['speed_kmh'].mean():.1f} km/h")
        axes[0, 0].axvline(df_speeds['speed_kmh'].median(), color='green', linestyle='--', 
                          linewidth=2, label=f"Median: {df_speeds['speed_kmh'].median():.1f} km/h")
        axes[0, 0].set_xlabel('Speed (km/h)')
        axes[0, 0].set_ylabel('Frequency')
        axes[0, 0].set_title('Distribution of Segment Speeds')
        axes[0, 0].legend()
        axes[0, 0].grid(axis='y', alpha=0.5)
        
        # 2. Box plot by route type
        if has_box_plot:
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
            
            axes[0, 1].boxplot(data_to_plot, labels=labels)
            axes[0, 1].set_ylabel('Average Speed (km/h)')
            axes[0, 1].set_title('Speed Distribution by Transport Mode')
            axes[0, 1].grid(axis='y', alpha=0.5)
        else:
            axes[0, 1].axis('off')
        
        # 3. Top routes by average speed
        if has_top_routes:
            top_routes = df_stats.head(20)
            axes[1, 0].barh(range(len(top_routes)), top_routes['avg_speed_kmh'], color='#cc0000')
            axes[1, 0].set_yticks(range(len(top_routes)))
            axes[1, 0].set_yticklabels([f"{row['route_short_name'] or row['route_id']}" 
                                         for _, row in top_routes.iterrows()], fontsize=8)
            axes[1, 0].set_xlabel('Average Speed (km/h)')
            axes[1, 0].set_title('Top 20 Routes by Average Speed')
            axes[1, 0].grid(axis='x', alpha=0.5)
        else:
            axes[1, 0].axis('off')
        
        # 4. Speed vs number of segments
        if has_scatter:
            axes[1, 1].scatter(df_stats['num_segments'], df_stats['avg_speed_kmh'], 
                              alpha=0.5, color='#0066cc', s=50)
            axes[1, 1].set_xlabel('Number of Segments')
            axes[1, 1].set_ylabel('Average Speed (km/h)')
            axes[1, 1].set_title('Speed vs Route Length (Number of Segments)')
            axes[1, 1].grid(alpha=0.5)
        else:
            axes[1, 1].axis('off')
    
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'speed_analysis.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved '{output_path}'")
    plt.close()

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
    ax.set_title('Routes with High-Speed Segments (>60 km/h)', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.5)
    
    # Add value labels on bars
    for i, (idx, row) in enumerate(top_routes.iterrows()):
        ax.text(row['speed_kmh'] + 1, i, f"{row['speed_kmh']:.1f}", 
               va='center', fontsize=8)
    
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'high_speed_routes.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved '{output_path}'")
    plt.close()

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
            print("   route_speed_stats view does not exist - make sure queries/analysis/spatial_queries.sql ran successfully")
        if df_speeds.empty:
            print("   schedule_speeds view does not exist - make sure queries/analysis/spatial_queries.sql ran successfully")
        print("\n   Make sure you've run:")
        print("  1. data_loading/mobilitydb_import.sql")
        print("  2. queries/analysis/spatial_queries.sql")
        return
    
    print_speed_statistics(df_stats, df_speeds, df_high)
    
    print("\nGenerating visualizations...")
    if not df_speeds.empty:
        plot_speed_distribution(df_speeds, df_stats)
    
    if not df_high.empty:
        plot_high_speed_routes(df_high)
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()


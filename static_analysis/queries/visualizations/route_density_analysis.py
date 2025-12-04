#!/usr/bin/env python3
"""
Route Density Analysis for Vancouver Transit
Generates histogram showing number of routes per segment.

Uses the same data as qgis_queries/02_route_density.sql.
Note: Map visualizations are created manually in QGIS.
"""

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

load_dotenv()

# Output directory (go up 3 levels: visualization/ -> analysis/ -> queries/ -> static_analysis/)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results', 'route_density')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

def fetch_num_routes():
    """Fetch number of BUS routes per segment from materialized view (for histogram)"""
    # Using materialized view created by qgis_queries/02_route_density.sql
    query = """
    SELECT num_routes
    FROM qgis_route_density;
    """
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    # Flatten to 1D list
    return [row[0] for row in data]

# Geometry fetching removed - maps are created manually in QGIS

def plot_histogram(num_routes_list):
    """Plot histogram of routes per segment"""
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(num_routes_list, bins=30, color="#3794eb", edgecolor="black")
    ax.set_xlabel('Number of Routes per Segment')
    ax.set_ylabel('Count of Segments')
    ax.set_title('Histogram of Number of BUS Routes per Segment - Vancouver Transit')
    ax.grid(axis='y', alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'route_density_histogram.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', format='png')
    print(f"Histogram saved as '{output_path}'")
    plt.close(fig)

# Map generation removed - maps are created manually in QGIS using qgis_queries/02_route_density.sql

if __name__ == "__main__":
    print("=" * 60)
    print("ROUTE DENSITY ANALYSIS")
    print("=" * 60)
    print("\nNote: Map visualizations are created manually in QGIS using qgis_queries/02_route_density.sql")
    print("This script generates complementary graphs showing route density statistics.\n")
    
    print("Fetching route density data (using QGIS query)...")
    num_routes_list = fetch_num_routes()
    print(f"Found {len(num_routes_list)} segments")
    
    if num_routes_list:
        print("\nGenerating histogram...")
        plot_histogram(num_routes_list)
        print("\n✓ Graph visualization created successfully!")
        print("  Use QGIS with qgis_queries/02_route_density.sql for map visualizations")
    else:
        print("No data found. Make sure you've run data_loading/mobilitydb_import.sql and qgis_queries/run_sql.py")



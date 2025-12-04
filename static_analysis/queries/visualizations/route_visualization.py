#!/usr/bin/env python3
"""
Route Visualization for Vancouver Transit (graph outputs)

Generates statistical graphs complementing QGIS map visualizations.
Uses the same data as qgis_queries/01_route_visualization.sql.

Note: Map visualizations are created manually in QGIS.
"""

import os
import sys
from pathlib import Path

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Add parent directories for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

load_dotenv()

# Output directory (go up 3 levels: visualization/ -> analysis/ -> queries/ -> static_analysis/)
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "results",
    "route_visualization",
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def fetch_route_data() -> pd.DataFrame:
    """Fetch route data from materialized view created by QGIS queries."""
    # Using materialized view created by qgis_queries/01_route_visualization.sql
    query = """
    SELECT 
        route_id,
        route_short_name,
        route_long_name,
        route_type,
        num_trips
    FROM qgis_route_visualization
    ORDER BY num_trips DESC;
    """
    
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df




def plot_route_statistics(df: pd.DataFrame):
    """Generate graphs showing route statistics (complements QGIS map visualization)."""
    if df.empty:
        print("No route data available for plotting.")
        return

    # Graph 1: Top routes by trip count
    fig, ax = plt.subplots(figsize=(12, 8))
    top_routes = df.head(20)
    ax.barh(range(len(top_routes)), top_routes['num_trips'], color='#3498db')
    ax.set_yticks(range(len(top_routes)))
    ax.set_yticklabels([f"{row['route_short_name']} - {row['route_long_name'][:30]}..." 
                       if len(row['route_long_name']) > 30 else f"{row['route_short_name']} - {row['route_long_name']}"
                       for _, row in top_routes.iterrows()], fontsize=8)
    ax.set_xlabel('Number of Trips', fontsize=12)
    ax.set_title('Top 20 BUS Routes by Number of Trips', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, "route_trip_statistics.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor='white', format='png')
    plt.close(fig)
    print(f"Saved '{output_path}'")

    # Graph 2: Distribution of trips per route
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(df['num_trips'], bins=30, color='#2ecc71', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Number of Trips per Route', fontsize=12)
    ax.set_ylabel('Number of Routes', fontsize=12)
    ax.set_title('Distribution of Trips per BUS Route', fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, "route_trip_distribution.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor='white', format='png')
    plt.close(fig)
    print(f"Saved '{output_path}'")


# Map generation removed - maps are created manually in QGIS using qgis_queries/01_route_visualization.sql


def main():
    print("=" * 60)
    print("ROUTE VISUALIZATION ANALYSIS")
    print("=" * 60)
    print("\nNote: Map visualizations are created manually in QGIS using qgis_queries/01_route_visualization.sql")
    print("This script generates complementary graphs showing route statistics.\n")
    
    print("Fetching route data (using QGIS query)...")
    df_routes = fetch_route_data()
    print(f"Found {len(df_routes)} BUS routes")
    
    if df_routes.empty:
        print("No route data found. Make sure you've run:")
        print("  1. data_loading/mobilitydb_import.sql")
        print("  2. qgis_queries/run_sql.py (or static_analysis/queries/run_all_analyses.py to build qgis_* views)")
        return
    
    print("\nGenerating route statistics graphs...")
    plot_route_statistics(df_routes)
    
    print("\n✓ Graph visualizations created successfully!")
    print("  Use QGIS with qgis_queries/01_route_visualization.sql for map visualizations")


if __name__ == "__main__":
    main()


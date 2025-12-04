#!/usr/bin/env python3
"""
Population Density Analysis for Vancouver Transit
Analyzes the relationship between population density and transit route coverage.
"""

import os
import sys
from pathlib import Path

import psycopg2
import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Add parent directories for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

load_dotenv()

# Output directory
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "results" / "population_density"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
        password=DB_PASS
    )


def fetch_population_density():
    """Fetch population density data from materialized view."""
    # Using materialized view created by qgis_queries/10_population_density.sql
    query = """
    SELECT 
        id,
        geom,
        population_density,
        area_km2
    FROM qgis_population_density
    WHERE geom IS NOT NULL
        AND population_density IS NOT NULL
        AND population_density > 0;
    """
    
    conn = get_db_connection()
    try:
        gdf = gpd.read_postgis(query, conn, geom_col="geom")
    except Exception as e:
        print(f"Error fetching population density: {e}")
        print("Make sure you've run download_population_data.py first")
        gdf = gpd.GeoDataFrame()
    finally:
        conn.close()
    
    if gdf.empty:
        return gdf
    
    gdf = gdf.set_geometry("geom")
    gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    return gdf


# Route density fetching removed - not needed for population analysis graphs
# Use qgis_route_density materialized view if needed


def calculate_transit_coverage(pop_gdf=None, route_gdf=None):
    """Calculate transit route coverage using materialized view."""
    # Using materialized view created by qgis_queries/11_population_transit_overlay.sql
    query = """
    SELECT 
        id,
        population_density,
        num_segments,
        route_length_km,
        area_km2,
        route_density_km_per_km2
    FROM qgis_population_transit_overlay
    WHERE geom IS NOT NULL
        AND population_density IS NOT NULL;
    """
    
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
        # route_density_km_per_km2 is already in the view, but calculate if missing
        if 'route_density_km_per_km2' not in df.columns:
            df['route_density_km_per_km2'] = df['route_length_km'] / df['area_km2']
            df['route_density_km_per_km2'] = df['route_density_km_per_km2'].fillna(0)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            print(f"⚠️  qgis_population_transit_overlay view does not exist.")
            print("   Make sure you've run qgis_queries/run_sql.py first")
        else:
            print(f"Error calculating transit coverage: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()
    
    return df


def plot_transit_vs_population(df_coverage):
    """Plot relationship between population density and transit coverage."""
    if df_coverage.empty:
        print("No coverage data available")
        return
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Filter out outliers
    df_filtered = df_coverage[
        (df_coverage['population_density'] < df_coverage['population_density'].quantile(0.95)) &
        (df_coverage['route_density_km_per_km2'] < df_coverage['route_density_km_per_km2'].quantile(0.95))
    ]
    
    scatter = ax.scatter(
        df_filtered['population_density'],
        df_filtered['route_density_km_per_km2'],
        c=df_filtered['num_segments'],
        cmap='viridis',
        alpha=0.6,
        s=50
    )
    
    ax.set_xlabel("Population Density (people/km²)", fontsize=12)
    ax.set_ylabel("Transit Route Density (km/km²)", fontsize=12)
    ax.set_title(
        "BUS Transit Coverage vs Population Density",
        fontsize=14,
        fontweight="bold"
    )
    ax.grid(alpha=0.3)
    
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("Number of Route Segments")
    
    plt.tight_layout()
    output_path = OUTPUT_DIR / "transit_vs_population_scatter.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', format='png')
    print(f"Saved '{output_path}'")
    plt.close()


def plot_coverage_by_density_category(df_coverage):
    """Plot average transit coverage by population density category."""
    if df_coverage.empty:
        return
    
    # Categorize population density
    df_coverage['density_category'] = pd.cut(
        df_coverage['population_density'],
        bins=[0, 1000, 5000, 10000, float('inf')],
        labels=['Low (<1k)', 'Medium (1k-5k)', 'High (5k-10k)', 'Very High (>10k)']
    )
    
    category_stats = df_coverage.groupby('density_category').agg({
        'route_density_km_per_km2': 'mean',
        'num_segments': 'mean',
        'population_density': 'mean'
    }).reset_index()
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.bar(range(len(category_stats)), category_stats['route_density_km_per_km2'], 
           color='#3498db', alpha=0.8)
    ax.set_xticks(range(len(category_stats)))
    ax.set_xticklabels(category_stats['density_category'])
    ax.set_xlabel("Population Density Category", fontsize=12)
    ax.set_ylabel("Average Transit Route Density (km/km²)", fontsize=12)
    ax.set_title(
        "Average BUS Transit Coverage by Population Density",
        fontsize=14,
        fontweight="bold"
    )
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = OUTPUT_DIR / "coverage_by_density_category.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', format='png')
    print(f"Saved '{output_path}'")
    plt.close()


def print_statistics(df_coverage):
    """Print summary statistics."""
    if df_coverage.empty:
        return
    
    print("\n" + "=" * 70)
    print("POPULATION DENSITY vs TRANSIT COVERAGE ANALYSIS")
    print("=" * 70)
    
    print(f"\nTotal areas analyzed: {len(df_coverage):,}")
    print(f"Areas with transit coverage: {(df_coverage['num_segments'] > 0).sum():,}")
    print(f"Areas without transit coverage: {(df_coverage['num_segments'] == 0).sum():,}")
    
    print(f"\n--- Population Density Statistics ---")
    print(f"  Mean:   {df_coverage['population_density'].mean():.2f} people/km²")
    print(f"  Median: {df_coverage['population_density'].median():.2f} people/km²")
    print(f"  Max:    {df_coverage['population_density'].max():.2f} people/km²")
    
    print(f"\n--- Transit Coverage Statistics ---")
    print(f"  Mean route density:   {df_coverage['route_density_km_per_km2'].mean():.4f} km/km²")
    print(f"  Median route density: {df_coverage['route_density_km_per_km2'].median():.4f} km/km²")
    print(f"  Max route density:    {df_coverage['route_density_km_per_km2'].max():.4f} km/km²")
    
    # Correlation
    correlation = df_coverage['population_density'].corr(df_coverage['route_density_km_per_km2'])
    print(f"\n--- Correlation ---")
    print(f"  Population Density vs Route Density: {correlation:.3f}")
    
    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    print("=" * 60)
    print("POPULATION DENSITY ANALYSIS")
    print("=" * 60)
    
    print("\nFetching population density data...")
    gdf_pop = fetch_population_density()
    
    if gdf_pop.empty:
        print("⚠️  No population density data found.")
        print("   Please run: python static_analysis/data/download_population_data.py")
        return 1
    
    print(f"✓ Found {len(gdf_pop)} population density areas")
    
    print("\nFetching transit coverage from materialized view...")
    df_coverage = calculate_transit_coverage()  # Using materialized view directly
    
    if df_coverage.empty:
        print("⚠️  Could not calculate transit coverage.")
        return 1
    
    print(f"✓ Calculated coverage for {len(df_coverage)} areas")
    
    print("\nGenerating graph visualizations...")
    print("Note: Map visualizations are created manually in QGIS using the queries in qgis_queries/")
    plot_transit_vs_population(df_coverage)
    plot_coverage_by_density_category(df_coverage)
    
    print_statistics(df_coverage)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())


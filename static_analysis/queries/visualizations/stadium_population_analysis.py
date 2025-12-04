#!/usr/bin/env python3
"""
Stadium vs Population Density Analysis for Vancouver Transit
Analyzes how stadiums connect with high-density population areas
"""

import os
import sys
from pathlib import Path

import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Add parent directories for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

load_dotenv()

# Output directory
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "results" / "stadium_population"
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


def fetch_stadium_population_data():
    """Fetch stadium connectivity to high-density areas data from materialized view."""
    # Using materialized view created by sql/12_stadium_population_overlay.sql
    query = """
    SELECT 
        stadium_name,
        team,
        num_high_density_areas_connected,
        total_population_connected,
        avg_density_connected,
        max_density_connected,
        total_connecting_segments,
        total_route_length_km,
        avg_distance_to_dense_areas_m,
        nearest_dense_area_distance_m,
        num_segments_near_stadium,
        route_length_km_near_stadium,
        connectivity_score_segments_per_million
    FROM qgis_stadium_population_overlay
    ORDER BY total_population_connected DESC;
    """
    
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        if 'does not exist' in str(e) or 'UndefinedTable' in str(e):
            print(f"⚠️  qgis_stadium_population_overlay view does not exist.")
            print("   Make sure you've run sql/run_sql.py first")
            df = pd.DataFrame()
        else:
            print(f"Error fetching stadium population data: {e}")
            df = pd.DataFrame()
    finally:
        conn.close()
    
    return df


def plot_connectivity_vs_population(df):
    """Plot relationship between connected population and transit connectivity for stadiums."""
    if df.empty:
        print("No data available for connectivity vs population plot")
        return
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Create scatter plot
    scatter = ax.scatter(
        df['total_population_connected'],
        df['total_connecting_segments'],
        s=df['num_high_density_areas_connected'] * 50,  # Size by number of areas connected
        c=df['connectivity_score_segments_per_million'],
        cmap='viridis',
        alpha=0.7,
        edgecolors='black',
        linewidth=1.5
    )
    
    # Add stadium labels
    for idx, row in df.iterrows():
        ax.annotate(
            row['stadium_name'],
            xy=(row['total_population_connected'], row['total_connecting_segments']),
            xytext=(5, 5),
            textcoords='offset points',
            fontsize=9,
            fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8)
        )
    
    ax.set_xlabel("Total Population Connected (people in high-density areas)", fontsize=12)
    ax.set_ylabel("Total Connecting Transit Segments", fontsize=12)
    ax.set_title(
        "Stadium Connectivity to High-Density Areas via Transit",
        fontsize=14,
        fontweight="bold"
    )
    ax.grid(alpha=0.3)
    
    # Add colorbar for connectivity score
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("Connectivity Score (segments per million people)")
    
    # Add size legend
    sizes = [1, 3, 5]
    labels = [f"{s} areas" for s in sizes]
    legend_elements = [
        plt.scatter([], [], s=s*50, c='gray', alpha=0.7, edgecolors='black', linewidth=1.5)
        for s in sizes
    ]
    ax.legend(legend_elements, labels, title="High-Density Areas Connected", 
              loc='upper left', framealpha=0.9)
    
    plt.tight_layout()
    output_path = OUTPUT_DIR / "stadium_connectivity_vs_population.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', format='png')
    print(f"Saved '{output_path}'")
    plt.close()


def plot_connected_population_by_stadium(df):
    """Plot total population in connected high-density areas for each stadium."""
    if df.empty:
        print("No data available for connected population by stadium plot")
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Sort by connected population
    df_sorted = df.sort_values('total_population_connected', ascending=True)
    
    bars = ax.barh(
        df_sorted['stadium_name'],
        df_sorted['total_population_connected'],
        color='#3498db',
        alpha=0.8,
        edgecolor='black',
        linewidth=1
    )
    
    # Add value labels
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        if row['total_population_connected'] > 0:
            ax.text(
                row['total_population_connected'] + max(df_sorted['total_population_connected']) * 0.02,
                i,
                f"{int(row['total_population_connected']):,}",
                va='center',
                fontsize=10,
                fontweight='bold'
            )
    
    ax.set_xlabel("Total Population in Connected High-Density Areas", fontsize=12)
    ax.set_ylabel("Stadium", fontsize=12)
    ax.set_title(
        "Stadium Connectivity: Population in Connected High-Density Areas",
        fontsize=14,
        fontweight="bold"
    )
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    output_path = OUTPUT_DIR / "stadium_connected_population.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', format='png')
    print(f"Saved '{output_path}'")
    plt.close()


def plot_connectivity_metrics(df):
    """Compare connectivity metrics: areas connected vs segments."""
    if df.empty:
        print("No data available for connectivity metrics plot")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Left plot: Number of areas connected
    df_sorted1 = df.sort_values('num_high_density_areas_connected', ascending=True)
    bars1 = ax1.barh(
        df_sorted1['stadium_name'],
        df_sorted1['num_high_density_areas_connected'],
        color='#2ecc71',
        alpha=0.8,
        edgecolor='black',
        linewidth=1
    )
    ax1.set_xlabel("Number of High-Density Areas Connected", fontsize=12)
    ax1.set_ylabel("Stadium", fontsize=12)
    ax1.set_title("High-Density Areas Connected via Transit", fontsize=13, fontweight="bold")
    ax1.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, (idx, row) in enumerate(df_sorted1.iterrows()):
        if row['num_high_density_areas_connected'] > 0:
            ax1.text(
                row['num_high_density_areas_connected'] + 0.1,
                i,
                f"{int(row['num_high_density_areas_connected'])}",
                va='center',
                fontsize=10,
                fontweight='bold'
            )
    
    # Right plot: Connecting segments
    df_sorted2 = df.sort_values('total_connecting_segments', ascending=True)
    bars2 = ax2.barh(
        df_sorted2['stadium_name'],
        df_sorted2['total_connecting_segments'],
        color='#e74c3c',
        alpha=0.8,
        edgecolor='black',
        linewidth=1
    )
    ax2.set_xlabel("Total Connecting Transit Segments", fontsize=12)
    ax2.set_ylabel("Stadium", fontsize=12)
    ax2.set_title("Transit Segments Connecting to Dense Areas", fontsize=13, fontweight="bold")
    ax2.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, (idx, row) in enumerate(df_sorted2.iterrows()):
        if row['total_connecting_segments'] > 0:
            ax2.text(
                row['total_connecting_segments'] + max(df_sorted2['total_connecting_segments']) * 0.02,
                i,
                f"{int(row['total_connecting_segments'])}",
                va='center',
                fontsize=10,
                fontweight='bold'
            )
    
    plt.tight_layout()
    output_path = OUTPUT_DIR / "stadium_connectivity_metrics.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', format='png')
    print(f"Saved '{output_path}'")
    plt.close()


def print_statistics(df):
    """Print summary statistics."""
    if df.empty:
        return
    
    print("\n" + "=" * 70)
    print("STADIUM CONNECTIVITY TO HIGH-DENSITY AREAS ANALYSIS")
    print("=" * 70)
    
    print(f"\nTotal stadiums analyzed: {len(df)}")
    
    print(f"\n--- Connectivity Statistics ---")
    print(f"  Mean areas connected:   {df['num_high_density_areas_connected'].mean():.1f} areas")
    print(f"  Mean population connected:   {df['total_population_connected'].mean():.0f} people")
    print(f"  Mean connecting segments:  {df['total_connecting_segments'].mean():.1f} segments")
    
    print(f"\n--- Distance Statistics ---")
    print(f"  Mean distance to dense areas:   {df['avg_distance_to_dense_areas_m'].mean():.0f} m")
    print(f"  Mean nearest dense area:   {df['nearest_dense_area_distance_m'].mean():.0f} m")
    
    print(f"\n--- Transit Coverage Near Stadiums ---")
    print(f"  Mean segments near stadium:   {df['num_segments_near_stadium'].mean():.1f} segments")
    print(f"  Mean route length near stadium:  {df['route_length_km_near_stadium'].mean():.2f} km")
    
    print(f"\n--- Connectivity Score ---")
    print(f"  Mean connectivity score:   {df['connectivity_score_segments_per_million'].mean():.2f} segments/million")
    
    # Correlation
    if df['total_population_connected'].sum() > 0:
        correlation = df['total_population_connected'].corr(df['total_connecting_segments'])
        print(f"\n--- Correlation ---")
        print(f"  Connected Population vs Connecting Segments: {correlation:.3f}")
    
    print("\n--- Stadium Rankings ---")
    print("\nTop 3 by Connected Population:")
    top_pop = df.nlargest(3, 'total_population_connected')
    for i, (idx, row) in enumerate(top_pop.iterrows(), 1):
        print(f"  {i}. {row['stadium_name']}: {int(row['total_population_connected']):,} people")
    
    print("\nTop 3 by Number of Areas Connected:")
    top_areas = df.nlargest(3, 'num_high_density_areas_connected')
    for i, (idx, row) in enumerate(top_areas.iterrows(), 1):
        print(f"  {i}. {row['stadium_name']}: {int(row['num_high_density_areas_connected'])} areas")
    
    print("\nTop 3 by Connectivity Score:")
    top_score = df.nlargest(3, 'connectivity_score_segments_per_million')
    for i, (idx, row) in enumerate(top_score.iterrows(), 1):
        print(f"  {i}. {row['stadium_name']}: {row['connectivity_score_segments_per_million']:.2f} segments/million")
    
    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    print("=" * 60)
    print("STADIUM vs POPULATION DENSITY ANALYSIS")
    print("=" * 60)
    
    print("\nFetching stadium population data...")
    df = fetch_stadium_population_data()
    
    if df.empty:
        print("⚠️  No stadium population data found.")
        print("   Please run: python static_analysis/queries/sql/run_sql.py")
        return 1
    
    print(f"✓ Found data for {len(df)} stadiums")
    
    print("\nGenerating graph visualizations...")
    print("Note: Map visualizations are created manually in QGIS using sql/12_stadium_population_overlay.sql")
    plot_connectivity_vs_population(df)
    plot_connected_population_by_stadium(df)
    plot_connectivity_metrics(df)
    
    print_statistics(df)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())


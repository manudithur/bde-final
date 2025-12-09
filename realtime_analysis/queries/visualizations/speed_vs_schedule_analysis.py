#!/usr/bin/env python3
"""
Speed vs Schedule Analysis for Vancouver Transit Realtime Data
Compares scheduled (planned) velocities with actual observed velocities from GTFS-Realtime.
"""

import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend

# Add parent directories for imports
# Go up from visualizations/ -> queries/ -> realtime_analysis/ -> project root
script_dir = Path(__file__).resolve()
# parents[0] = file, [1] = visualizations/, [2] = queries/, [3] = realtime_analysis/, [4] = root
# Actually: [0] = file, [1] = visualizations/, [2] = queries/, [3] = realtime_analysis/, [4] = root
project_root = script_dir.parents[3]  # This is the project root (bde-final)
sys.path.insert(0, str(project_root))

from realtime_analysis.utility.config import load_settings
from realtime_analysis.utility.utils import get_connection

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "speed_vs_schedule"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    """Clear all files in the results directory before generating new ones."""
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def get_timestamp_suffix() -> str:
    """Generate a timestamp suffix for output files."""
    return ""  # No timestamp suffix


def fetch_speed_comparison_data(conn) -> pd.DataFrame:
    """
    Fetch speed comparison between scheduled and actual for all available segments - BUS routes only.
    Uses materialized view for better performance.
    """
    query = """
    SELECT
        s.trip_instance_id,
        s.trip_id,
        s.route_short_name,
        s.route_long_name,
        s.route_type,
        s.route_id,
        s.service_date,
        s.stop_sequence,
        s.next_stop_sequence,
        s.stop_id,
        s.next_stop_id,
        s.from_stop_name,
        s.to_stop_name,
        s.segment_length_m,
        s.scheduled_seconds,
        s.actual_seconds,
        s.arrival_delay_seconds,
        s.hour_of_day,
        s.day_of_week,
        s.scheduled_speed_kmh,
        s.actual_speed_kmh
    FROM realtime_speed_comparison s
    JOIN routes r ON s.route_id = r.route_id
    WHERE r.route_type = '3'
      AND s.scheduled_speed_kmh IS NOT NULL
      AND s.actual_speed_kmh IS NOT NULL
      AND s.scheduled_speed_kmh > 0 AND s.scheduled_speed_kmh < 150
      AND s.actual_speed_kmh > 0 AND s.actual_speed_kmh < 150
    ORDER BY s.trip_instance_id, s.stop_sequence;
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        return df
    
    # Add day_type based on day_of_week
    df["day_type"] = df["day_of_week"].apply(lambda x: "Weekend" if x in [0, 6] else "Weekday")
    
    # Calculate speed differences
    df["speed_delta_kmh"] = df["actual_speed_kmh"] - df["scheduled_speed_kmh"]
    df["speed_ratio"] = df["actual_speed_kmh"] / df["scheduled_speed_kmh"]
    
    return df


def plot_speed_scatter(df: pd.DataFrame, suffix: str) -> Path:
    """Create scatter plot of scheduled vs actual speeds."""
    fig, ax = plt.subplots(figsize=(10, 8))
    
    scatter = ax.scatter(
        df["scheduled_speed_kmh"],
        df["actual_speed_kmh"],
        c=df["speed_delta_kmh"],
        cmap="RdYlGn",
        alpha=0.5,
        s=20
    )
    
    # Add perfect correlation line
    max_speed = max(df["scheduled_speed_kmh"].max(), df["actual_speed_kmh"].max())
    ax.plot([0, max_speed], [0, max_speed], 'r--', linewidth=2, label="Perfect Match")
    
    ax.set_xlabel("Scheduled Speed (km/h)", fontsize=12)
    ax.set_ylabel("Actual Speed (km/h)", fontsize=12)
    ax.set_title("BUS Scheduled vs Actual Speed Comparison", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(alpha=0.3)
    
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("Speed Difference (km/h)")
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_scatter.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_speed_distribution_scheduled(df: pd.DataFrame, suffix: str) -> Path:
    """Create histogram of scheduled speeds."""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.hist(df["scheduled_speed_kmh"], bins=50, color='#1f77b4', edgecolor='black', alpha=0.7)
    ax.axvline(df["scheduled_speed_kmh"].mean(), color='red', linestyle='--', 
               linewidth=2, label=f"Mean: {df['scheduled_speed_kmh'].mean():.1f} km/h")
    ax.axvline(df["scheduled_speed_kmh"].median(), color='green', linestyle='--', 
               linewidth=2, label=f"Median: {df['scheduled_speed_kmh'].median():.1f} km/h")
    
    ax.set_xlabel("Scheduled Speed (km/h)", fontsize=12)
    ax.set_ylabel("Frequency", fontsize=12)
    ax.set_title("Distribution of BUS Scheduled Speeds", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_distribution_scheduled.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_speed_distribution_actual(df: pd.DataFrame, suffix: str) -> Path:
    """Create histogram of actual speeds."""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.hist(df["actual_speed_kmh"], bins=50, color='#ff7f0e', edgecolor='black', alpha=0.7)
    ax.axvline(df["actual_speed_kmh"].mean(), color='red', linestyle='--', 
               linewidth=2, label=f"Mean: {df['actual_speed_kmh'].mean():.1f} km/h")
    ax.axvline(df["actual_speed_kmh"].median(), color='green', linestyle='--', 
               linewidth=2, label=f"Median: {df['actual_speed_kmh'].median():.1f} km/h")
    
    ax.set_xlabel("Actual Speed (km/h)", fontsize=12)
    ax.set_ylabel("Frequency", fontsize=12)
    ax.set_title("Distribution of BUS Actual Speeds", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_distribution_actual.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_speed_difference(df: pd.DataFrame, suffix: str) -> Path:
    """Create histogram of speed differences."""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.hist(df["speed_delta_kmh"], bins=50, color='#2ca02c', edgecolor='black', alpha=0.7)
    ax.axvline(0, color='red', linestyle='-', linewidth=2, label="No difference")
    ax.axvline(df["speed_delta_kmh"].mean(), color='blue', linestyle='--', 
               linewidth=2, label=f"Mean: {df['speed_delta_kmh'].mean():.1f} km/h")
    
    ax.set_xlabel("Speed Difference: Actual - Scheduled (km/h)", fontsize=12)
    ax.set_ylabel("Frequency", fontsize=12)
    ax.set_title("Distribution of BUS Speed Differences", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_difference.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_speed_by_route(df: pd.DataFrame, suffix: str) -> Path:
    """Compare average speeds by route."""
    route_stats = df.groupby("route_short_name").agg({
        "scheduled_speed_kmh": "mean",
        "actual_speed_kmh": "mean",
        "trip_instance_id": "count"
    }).reset_index()
    route_stats.columns = ["Route", "Scheduled", "Actual", "Samples"]
    route_stats = route_stats.sort_values("Actual", ascending=True).tail(20)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    y_pos = range(len(route_stats))
    width = 0.35
    
    ax.barh([y - width/2 for y in y_pos], route_stats["Scheduled"], width, 
            label='Scheduled', color='#1f77b4', alpha=0.8)
    ax.barh([y + width/2 for y in y_pos], route_stats["Actual"], width,
            label='Actual', color='#ff7f0e', alpha=0.8)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(route_stats["Route"])
    ax.set_xlabel("Speed (km/h)", fontsize=12)
    ax.set_ylabel("Route", fontsize=12)
    ax.set_title("Average BUS Speed by Route: Scheduled vs Actual", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_by_route.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path



def plot_speed_by_day_type(df: pd.DataFrame, suffix: str) -> Path:
    """Analyze speed differences by day type (weekend vs weekday)."""
    day_type_stats = df.groupby("day_type").agg({
        "scheduled_speed_kmh": "mean",
        "actual_speed_kmh": "mean",
        "speed_delta_kmh": "mean"
    }).reset_index()
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x = range(len(day_type_stats))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], day_type_stats["scheduled_speed_kmh"], width,
                   label='Scheduled', color='#1f77b4', alpha=0.8)
    bars2 = ax.bar([i + width/2 for i in x], day_type_stats["actual_speed_kmh"], width,
                   label='Actual', color='#ff7f0e', alpha=0.8)
    
    ax.set_xlabel("Day Type", fontsize=12)
    ax.set_ylabel("Average Speed (km/h)", fontsize=12)
    ax.set_title("Average BUS Speed by Day Type", fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(day_type_stats["day_type"])
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_by_day_type.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def generate_summary_csv(df: pd.DataFrame, suffix: str) -> Path:
    """Generate summary statistics CSV."""
    summary = df.groupby(["route_short_name", "from_stop_name", "to_stop_name"]).agg({
        "scheduled_speed_kmh": ["mean", "std"],
        "actual_speed_kmh": ["mean", "std"],
        "speed_delta_kmh": ["mean", "std", "min", "max"],
        "segment_length_m": "first",
        "trip_instance_id": "count"
    }).reset_index()
    
    summary.columns = [
        "Route", "From Stop", "To Stop",
        "Sched Speed Mean", "Sched Speed Std",
        "Actual Speed Mean", "Actual Speed Std",
        "Speed Delta Mean", "Speed Delta Std", "Speed Delta Min", "Speed Delta Max",
        "Segment Length (m)", "Sample Count"
    ]
    
    output_path = RESULTS_DIR / f"speed_summary.csv"
    summary.to_csv(output_path, index=False)
    return output_path


def print_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics to console."""
    print("\n" + "=" * 70)
    print("SPEED VS SCHEDULE ANALYSIS SUMMARY (BUS Routes)")
    print("=" * 70)
    
    print(f"\nTotal segments analyzed: {len(df):,}")
    print(f"Unique trips: {df['trip_instance_id'].nunique():,}")
    print(f"Unique routes: {df['route_short_name'].nunique()}")
    
    print(f"\n--- Scheduled Speed Statistics ---")
    print(f"  Mean:   {df['scheduled_speed_kmh'].mean():.2f} km/h")
    print(f"  Median: {df['scheduled_speed_kmh'].median():.2f} km/h")
    print(f"  Std:    {df['scheduled_speed_kmh'].std():.2f} km/h")
    
    print(f"\n--- Actual Speed Statistics ---")
    print(f"  Mean:   {df['actual_speed_kmh'].mean():.2f} km/h")
    print(f"  Median: {df['actual_speed_kmh'].median():.2f} km/h")
    print(f"  Std:    {df['actual_speed_kmh'].std():.2f} km/h")
    
    print(f"\n--- Speed Difference (Actual - Scheduled) ---")
    print(f"  Mean:   {df['speed_delta_kmh'].mean():.2f} km/h")
    print(f"  Median: {df['speed_delta_kmh'].median():.2f} km/h")
    
    faster = len(df[df["speed_delta_kmh"] > 0])
    slower = len(df[df["speed_delta_kmh"] < 0])
    print(f"\n  Faster than scheduled: {faster:,} segments ({faster/len(df)*100:.1f}%)")
    print(f"  Slower than scheduled: {slower:,} segments ({slower/len(df)*100:.1f}%)")
    
    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Speed vs Schedule Analysis")
    parser.add_argument(
        "--clear-output",
        action="store_true",
        help="Clear existing output files before generating new ones"
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("SPEED VS SCHEDULE ANALYSIS")
    print("=" * 60)
    
    settings = load_settings()
    
    print("\nConnecting to database...")
    with get_connection(settings) as conn:
        print("Fetching speed comparison data...")
        df = fetch_speed_comparison_data(conn)
    
    if df.empty:
        print("⚠️  No speed comparison data found.")
        print("   Make sure you have:")
        print("   1. Run the realtime ingestion (ingest_realtime.py)")
        print("   2. Built the static schedule (mobilitydb_import.sql)")
        print("   3. Run realtime_queries.sql to create materialized views:")
        print("      cd realtime_analysis/queries && python sql/run_sql.py")
        print("   4. Trip updates with arrival times are available")
        return 1
    
    print(f"✓ Retrieved {len(df):,} segment speed comparisons")
    
    if args.clear_output:
        print("\nClearing previous results...")
        clear_results_dir()
    else:
        print("\nPreserving existing results (use --clear-output to delete old files)")
    
    suffix = get_timestamp_suffix()
    
    print("Generating visualizations...")
    
    path = plot_speed_scatter(df, suffix)
    print(f"  ✓ Speed scatter: {path}")
    
    path = plot_speed_distribution_scheduled(df, suffix)
    print(f"  ✓ Scheduled speed distribution: {path}")
    
    path = plot_speed_distribution_actual(df, suffix)
    print(f"  ✓ Actual speed distribution: {path}")
    
    path = plot_speed_difference(df, suffix)
    print(f"  ✓ Speed difference: {path}")
    
    path = plot_speed_by_route(df, suffix)
    print(f"  ✓ Speed by route: {path}")
    
    path = plot_speed_by_day_type(df, suffix)
    print(f"  ✓ Speed by day type: {path}")
    
    csv_path = generate_summary_csv(df, suffix)
    print(f"  ✓ Summary CSV: {csv_path}")
    
    # Add speed maps
    
    print_statistics(df)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

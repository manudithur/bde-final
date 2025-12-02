#!/usr/bin/env python3
"""
Speed vs Schedule Analysis for Vancouver Transit Realtime Data
Compares scheduled (planned) velocities with actual observed velocities from GTFS-Realtime.
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend

# Add parent directories for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from realtime_analysis.config import load_settings
from realtime_analysis.utils import get_connection

RESULTS_DIR = Path(__file__).resolve().parent.parent.parent / "results" / "speed_vs_schedule"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    """Clear all files in the results directory before generating new ones."""
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def get_timestamp_suffix() -> str:
    """Generate a timestamp suffix for output files."""
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def fetch_speed_comparison_data(conn) -> pd.DataFrame:
    """
    Fetch speed comparison between scheduled and actual for all available segments.
    Uses materialized view for better performance.
    """
    query = """
    SELECT
        trip_instance_id,
        trip_id,
        route_short_name,
        route_long_name,
        route_type,
        route_id,
        service_date,
        stop_sequence,
        next_stop_sequence,
        stop_id,
        next_stop_id,
        from_stop_name,
        to_stop_name,
        segment_length_m,
        scheduled_seconds,
        actual_seconds,
        arrival_delay_seconds,
        hour_of_day,
        day_of_week,
        scheduled_speed_kmh,
        actual_speed_kmh
    FROM realtime_speed_comparison
    WHERE scheduled_speed_kmh IS NOT NULL
      AND actual_speed_kmh IS NOT NULL
      AND scheduled_speed_kmh > 0 AND scheduled_speed_kmh < 150
      AND actual_speed_kmh > 0 AND actual_speed_kmh < 150
    ORDER BY trip_instance_id, stop_sequence;
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        return df
    
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
    ax.set_title("Scheduled vs Actual Speed Comparison", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(alpha=0.3)
    
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("Speed Difference (km/h)")
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_scatter_{suffix}.png"
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
    ax.set_title("Distribution of Scheduled Speeds", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_distribution_scheduled_{suffix}.png"
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
    ax.set_title("Distribution of Actual Speeds", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_distribution_actual_{suffix}.png"
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
    ax.set_title("Distribution of Speed Differences", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_difference_{suffix}.png"
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
    ax.set_title("Average Speed by Route: Scheduled vs Actual", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_by_route_{suffix}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_speed_by_hour(df: pd.DataFrame, suffix: str) -> Path:
    """Analyze speed differences by hour of day."""
    hourly = df.groupby("hour_of_day").agg({
        "scheduled_speed_kmh": "mean",
        "actual_speed_kmh": "mean",
        "speed_delta_kmh": "mean"
    }).reset_index()
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(hourly["hour_of_day"], hourly["scheduled_speed_kmh"], 
            'o-', color='#1f77b4', linewidth=2, markersize=8, label='Scheduled')
    ax.plot(hourly["hour_of_day"], hourly["actual_speed_kmh"], 
            's-', color='#ff7f0e', linewidth=2, markersize=8, label='Actual')
    
    ax.set_xlabel("Hour of Day", fontsize=12)
    ax.set_ylabel("Average Speed (km/h)", fontsize=12)
    ax.set_title("Average Speed by Hour of Day", fontsize=14, fontweight='bold')
    ax.set_xticks(range(0, 24))
    ax.legend()
    ax.grid(alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"speed_by_hour_{suffix}.png"
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
    
    output_path = RESULTS_DIR / f"speed_summary_{suffix}.csv"
    summary.to_csv(output_path, index=False)
    return output_path


def print_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics to console."""
    print("\n" + "=" * 70)
    print("SPEED VS SCHEDULE ANALYSIS SUMMARY")
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
        print("   3. Trip updates with arrival times are available")
        return 1
    
    print(f"✓ Retrieved {len(df):,} segment speed comparisons")
    
    print("\nClearing previous results...")
    clear_results_dir()
    
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
    
    path = plot_speed_by_hour(df, suffix)
    print(f"  ✓ Speed by hour: {path}")
    
    csv_path = generate_summary_csv(df, suffix)
    print(f"  ✓ Summary CSV: {csv_path}")
    
    print_statistics(df)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Delay Segments Analysis for Vancouver Transit Realtime Data
Identifies segments with the highest delays and analyzes when/where traffic congestion occurs.
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
matplotlib.use('Agg')

# Add parent directories for imports
# Go up from visualizations/ -> queries/ -> realtime_analysis/ -> project root
script_dir = Path(__file__).resolve()
# parents[0] = file, [1] = visualizations/, [2] = queries/, [3] = realtime_analysis/, [4] = root
# Actually: [0] = file, [1] = visualizations/, [2] = queries/, [3] = realtime_analysis/, [4] = root
project_root = script_dir.parents[3]  # This is the project root (bde-final)
sys.path.insert(0, str(project_root))

from realtime_analysis.utility.config import load_settings
from realtime_analysis.utility.utils import get_connection

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "delay_segments"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    """Clear all files in the results directory before generating new ones."""
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def get_timestamp_suffix() -> str:
    """Generate a timestamp suffix for output files."""
    return ""  # No timestamp suffix


def fetch_segment_delays(conn) -> pd.DataFrame:
    """Fetch segment-level delay data comparing scheduled vs actual travel times - BUS routes only.
    Uses materialized view for better performance.
    """
    query = """
    SELECT
        d.trip_instance_id,
        d.trip_id,
        d.route_short_name,
        d.route_long_name,
        d.route_type,
        d.route_id,
        d.service_date,
        d.from_seq,
        d.to_seq,
        d.from_stop_id,
        d.to_stop_id,
        d.from_stop_name,
        d.to_stop_name,
        d.from_lat,
        d.from_lon,
        d.to_lat,
        d.to_lon,
        d.segment_length_m,
        d.scheduled_seconds,
        d.actual_seconds,
        d.from_delay,
        d.to_delay,
        d.segment_delay_change,
        d.segment_delay_minutes,
        d.hour_of_day,
        d.day_of_week,
        d.day_type,
        d.time_period
    FROM realtime_delay_analysis d
    JOIN routes r ON d.route_id = r.route_id
    WHERE r.route_type = '3'
        AND d.segment_delay_minutes BETWEEN -30 AND 60
    ORDER BY d.trip_instance_id, d.from_seq;
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        return df
    
    # Calculate additional metrics
    df["segment_delay_seconds"] = df["actual_seconds"] - df["scheduled_seconds"]
    df["delay_per_km"] = df["segment_delay_seconds"] / (df["segment_length_m"] / 1000)
    
    df["scheduled_speed_kmh"] = (df["segment_length_m"] / df["scheduled_seconds"]) * 3.6
    df["actual_speed_kmh"] = (df["segment_length_m"] / df["actual_seconds"]) * 3.6
    df["speed_reduction_pct"] = ((df["scheduled_speed_kmh"] - df["actual_speed_kmh"]) / 
                                  df["scheduled_speed_kmh"]) * 100
    
    df = df[df["actual_speed_kmh"] < 150]
    
    df["delay_severity"] = pd.cut(
        df["segment_delay_minutes"],
        bins=[-float("inf"), -7, -3, 3, 7, float("inf")],
        labels=["Severe Early (<-7)", "Minor Early (-7 to -3)", "On Time (±3)", 
                "Minor Late (3 to 7)", "Severe Late (>7)"]
    )
    
    return df


def plot_delay_by_time_period(df: pd.DataFrame, suffix: str) -> Path:
    """Create bar chart of average delay by time period."""
    period_order = ["Night", "Morning Rush", "Midday", "Evening Rush", "Evening"]
    period_delays = df.groupby("time_period")["segment_delay_minutes"].mean()
    period_delays = period_delays.reindex(period_order).dropna()
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    colors = ['#3498db', '#e74c3c', '#2ecc71', '#e74c3c', '#f39c12']
    ax.bar(range(len(period_delays)), period_delays.values, 
           color=colors[:len(period_delays)], alpha=0.8)
    ax.set_xticks(range(len(period_delays)))
    ax.set_xticklabels(period_delays.index)
    
    # Add reference lines for the new scale
    ax.axhline(0, color='green', linestyle='-', linewidth=2, label='On Time (0 min)')
    ax.axhline(-3, color='orange', linestyle='--', linewidth=1, alpha=0.5, label='±3 min')
    ax.axhline(3, color='orange', linestyle='--', linewidth=1, alpha=0.5)
    ax.axhline(-7, color='red', linestyle=':', linewidth=1, alpha=0.5, label='±7 min (Severe)')
    ax.axhline(7, color='red', linestyle=':', linewidth=1, alpha=0.5)
    
    # Set y-axis limits to show the full scale including thresholds
    data_range = max(abs(period_delays.values.min()), abs(period_delays.values.max())) if len(period_delays) > 0 else 0
    y_max = max(7.0, data_range * 1.1)  # At least show ±7, or 10% more than data range
    ax.set_ylim(-y_max, y_max)
    
    ax.set_xlabel("Time Period", fontsize=12)
    ax.set_ylabel("Average Delay (minutes)", fontsize=12)
    ax.set_title("Average BUS Segment Delay by Time Period", fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    ax.legend(loc='best', fontsize=9)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_by_time_period.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path




def plot_worst_segments(df: pd.DataFrame, suffix: str) -> Path:
    """Visualize the worst-performing segments."""
    segment_stats = df.groupby(
        ["from_stop_name", "to_stop_name", "route_short_name"]
    ).agg({
        "segment_delay_minutes": ["mean", "count"]
    }).reset_index()
    segment_stats.columns = ["From", "To", "Route", "Avg Delay", "Samples"]
    
    segment_stats = segment_stats[segment_stats["Samples"] >= 3]
    worst = segment_stats.nlargest(20, "Avg Delay")
    worst["Segment"] = worst["From"].str[:15] + " → " + worst["To"].str[:15]
    
    fig, ax = plt.subplots(figsize=(12, 10))
    
    colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(worst)))
    ax.barh(range(len(worst)), worst["Avg Delay"], color=colors)
    ax.set_yticks(range(len(worst)))
    ax.set_yticklabels([f"{row['Segment']} ({row['Route']})" for _, row in worst.iterrows()],
                       fontsize=9)
    
    ax.set_xlabel("Average Delay (minutes)", fontsize=12)
    ax.set_ylabel("Segment", fontsize=12)
    ax.set_title("Top 20 Most Delayed BUS Segments", fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    ax.invert_yaxis()
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"worst_segments.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_delay_severity(df: pd.DataFrame, suffix: str) -> Path:
    """Create pie chart of delay severity."""
    severity_counts = df["delay_severity"].value_counts()
    colors = ['#2ecc71', '#f1c40f', '#f39c12', '#e74c3c', '#c0392b']
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    ax.pie(
        severity_counts.values,
        labels=severity_counts.index,
        colors=colors[:len(severity_counts)],
        autopct='%1.1f%%',
        startangle=90
    )
    ax.set_title("BUS Delay Severity Distribution", fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_severity.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path



def generate_summary_csv(df: pd.DataFrame, suffix: str) -> Path:
    """Generate CSV of worst segments by time period."""
    summary = df.groupby(
        ["route_short_name", "from_stop_name", "to_stop_name", "time_period"]
    ).agg({
        "segment_delay_minutes": ["mean", "std", "count"],
        "speed_reduction_pct": "mean"
    }).reset_index()
    
    summary.columns = [
        "Route", "From Stop", "To Stop", "Time Period",
        "Avg Delay (min)", "Std Delay", "Sample Count", "Speed Reduction %"
    ]
    
    summary = summary.sort_values("Avg Delay (min)", ascending=False)
    
    output_path = RESULTS_DIR / f"delay_segments_summary.csv"
    summary.to_csv(output_path, index=False)
    return output_path


def print_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics to console."""
    print("\n" + "=" * 70)
    print("DELAY SEGMENTS ANALYSIS SUMMARY (BUS Traffic Analysis)")
    print("=" * 70)
    
    print(f"\nTotal segments analyzed: {len(df):,}")
    print(f"Unique trips: {df['trip_instance_id'].nunique():,}")
    print(f"Unique routes: {df['route_short_name'].nunique()}")
    
    print(f"\n--- Segment Delay Statistics ---")
    print(f"  Mean delay:   {df['segment_delay_minutes'].mean():.2f} min")
    print(f"  Median delay: {df['segment_delay_minutes'].median():.2f} min")
    print(f"  Std:          {df['segment_delay_minutes'].std():.2f} min")
    
    print(f"\n--- Average Delay by Time Period ---")
    period_delays = df.groupby("time_period")["segment_delay_minutes"].mean().sort_values(ascending=False)
    for period, delay in period_delays.items():
        print(f"  {period}: {delay:.2f} min")
    
    print(f"\n--- Rush Hour Impact ---")
    rush_hour = df[df["time_period"].isin(["Morning Rush", "Evening Rush"])]
    off_peak = df[~df["time_period"].isin(["Morning Rush", "Evening Rush"])]
    
    print(f"  Rush hour avg delay:  {rush_hour['segment_delay_minutes'].mean():.2f} min")
    print(f"  Off-peak avg delay:   {off_peak['segment_delay_minutes'].mean():.2f} min")
    
    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Delay Segments Analysis")
    parser.add_argument(
        "--clear-output",
        action="store_true",
        help="Clear existing output files before generating new ones"
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("DELAY SEGMENTS ANALYSIS (Traffic Patterns)")
    print("=" * 60)
    
    settings = load_settings()
    
    print("\nConnecting to database...")
    with get_connection(settings) as conn:
        print("Fetching segment delay data...")
        df = fetch_segment_delays(conn)
    
    if df.empty:
        print("⚠️  No segment delay data found.")
        print("   Make sure you have:")
        print("   1. Run the realtime ingestion (ingest_realtime.py)")
        print("   2. Static schedule loaded (route_segments table)")
        print("   3. Run sql/run_sql.py to create materialized views:")
        print("      python3 realtime_analysis/queries/sql/run_sql.py")
        print("      (or run_all_analyses.py which runs this automatically)")
        print("   4. Trip updates with arrival times")
        print("\n   Note: Map visualizations are created manually in QGIS using qgis_realtime_* materialized views.")
        return 1
    
    print(f"✓ Retrieved {len(df):,} segment delay observations")
    
    if args.clear_output:
        print("\nClearing previous results...")
        clear_results_dir()
    else:
        print("\nPreserving existing results (use --clear-output to delete old files)")
    
    suffix = get_timestamp_suffix()
    
    print("Generating visualizations...")
    
    path = plot_delay_by_time_period(df, suffix)
    print(f"  ✓ Delay by time period: {path}")
    
    path = plot_worst_segments(df, suffix)
    print(f"  ✓ Worst segments: {path}")
    
    path = plot_delay_severity(df, suffix)
    print(f"  ✓ Delay severity: {path}")
    
    csv_path = generate_summary_csv(df, suffix)
    print(f"  ✓ Summary CSV: {csv_path}")
    
    print_statistics(df)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

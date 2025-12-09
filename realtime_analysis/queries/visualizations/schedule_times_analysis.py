#!/usr/bin/env python3
"""
Schedule Times Analysis for Vancouver Transit Realtime Data
Compares scheduled arrival/departure times with actual observed times from GTFS-Realtime.
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

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "schedule_times"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    """Clear all files in the results directory before generating new ones."""
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def get_timestamp_suffix() -> str:
    """Generate a timestamp suffix for output files."""
    return ""  # No timestamp suffix


def fetch_schedule_times_data(conn) -> pd.DataFrame:
    """Fetch arrival/departure time comparison between scheduled and actual - BUS routes only.
    Uses materialized view for better performance.
    """
    query = """
    SELECT
        st.trip_instance_id,
        st.trip_id,
        st.route_short_name,
        st.route_long_name,
        st.route_type,
        st.route_id,
        st.service_date,
        st.stop_sequence,
        st.stop_id,
        st.stop_name,
        st.stop_lat,
        st.stop_lon,
        st.scheduled_arrival_interval,
        st.actual_arrival,
        st.actual_departure,
        st.arrival_delay_seconds,
        st.departure_delay_seconds,
        st.delay_minutes,
        st.hour_of_day,
        st.day_of_week,
        st.day_type
    FROM realtime_schedule_times st
    JOIN routes r ON st.route_id = r.route_id
    WHERE r.route_type = '3'
    ORDER BY st.trip_instance_id, st.stop_sequence;
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        return df
    
    df["delay_category"] = pd.cut(
        df["delay_minutes"],
        bins=[-float("inf"), -7, -3, 3, 7, float("inf")],
        labels=["Severe Early (<-7 min)", "Minor Early (-7 to -3 min)", "On Time (±3 min)", 
                "Minor Late (3 to 7 min)", "Severe Late (>7 min)"]
    )
    
    return df


def plot_delay_histogram(df: pd.DataFrame, suffix: str) -> Path:
    """Create histogram of arrival delays."""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Use symmetric range centered at 0
    max_abs_delay = max(abs(df["delay_minutes"].min()), abs(df["delay_minutes"].max()))
    delay_capped = df["delay_minutes"].clip(-max_abs_delay, max_abs_delay)
    ax.hist(delay_capped, bins=60, color='#1f77b4', edgecolor='black', alpha=0.7)
    ax.axvline(0, color='green', linestyle='-', linewidth=2, label="On Time (0 min)")
    ax.axvline(-3, color='orange', linestyle='--', linewidth=1, alpha=0.5, label="±3 min")
    ax.axvline(3, color='orange', linestyle='--', linewidth=1, alpha=0.5)
    ax.axvline(-7, color='red', linestyle=':', linewidth=1, alpha=0.5, label="±7 min (Severe)")
    ax.axvline(7, color='red', linestyle=':', linewidth=1, alpha=0.5)
    ax.axvline(df["delay_minutes"].mean(), color='orange', linestyle='--', 
               linewidth=2, label=f"Mean: {df['delay_minutes'].mean():.1f} min")
    
    ax.set_xlabel("Delay (minutes)", fontsize=12)
    ax.set_ylabel("Frequency", fontsize=12)
    ax.set_title("Distribution of BUS Arrival Delays", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_histogram.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_delay_categories(df: pd.DataFrame, suffix: str) -> Path:
    """Create pie chart of delay categories."""
    fig, ax = plt.subplots(figsize=(10, 8))
    
    category_counts = df["delay_category"].value_counts()
    colors = ['#2ecc71', '#82e0aa', '#f7dc6f', '#f39c12', '#e74c3c', '#c0392b']
    
    wedges, texts, autotexts = ax.pie(
        category_counts.values,
        labels=category_counts.index,
        colors=colors[:len(category_counts)],
        autopct='%1.1f%%',
        pctdistance=0.75
    )
    
    ax.set_title("BUS Delay Categories Distribution", fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_categories.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_delay_by_route(df: pd.DataFrame, suffix: str) -> Path:
    """Create bar chart of average delay by route."""
    route_delays = df.groupby("route_short_name")["delay_minutes"].mean().sort_values()
    route_delays = route_delays.tail(20)  # Top 20
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    colors = ['#e74c3c' if v > 0 else '#2ecc71' for v in route_delays.values]
    ax.barh(range(len(route_delays)), route_delays.values, color=colors, alpha=0.8)
    ax.set_yticks(range(len(route_delays)))
    ax.set_yticklabels(route_delays.index)
    ax.axvline(0, color='black', linestyle='-', linewidth=1)
    
    ax.set_xlabel("Average Delay (minutes)", fontsize=12)
    ax.set_ylabel("Route", fontsize=12)
    ax.set_title("Average BUS Delay by Route (Top 20)", fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_by_route.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_on_time_performance(df: pd.DataFrame, suffix: str) -> Path:
    """Create on-time performance by route."""
    df["on_time"] = (df["delay_minutes"] >= -3) & (df["delay_minutes"] <= 3)
    df["early"] = df["delay_minutes"] < -3
    df["late"] = df["delay_minutes"] > 3
    
    route_otp = df.groupby("route_short_name").agg({
        "on_time": "mean",
        "early": "mean",
        "late": "mean",
        "delay_minutes": "count"
    }).reset_index()
    route_otp.columns = ["Route", "On-Time", "Early", "Late", "Samples"]
    route_otp = route_otp[route_otp["Samples"] >= 10]
    route_otp = route_otp.sort_values("On-Time", ascending=True).tail(20)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    y_pos = range(len(route_otp))
    
    ax.barh(y_pos, route_otp["Late"] * 100, color='#e74c3c', label='Late (>3 min)')
    ax.barh(y_pos, route_otp["On-Time"] * 100, left=route_otp["Late"] * 100, 
            color='#2ecc71', label='On-Time (±3 min)')
    ax.barh(y_pos, route_otp["Early"] * 100, 
            left=(route_otp["Late"] + route_otp["On-Time"]) * 100,
            color='#3498db', label='Early (<-3 min)')
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(route_otp["Route"])
    ax.set_xlabel("Percentage", fontsize=12)
    ax.set_ylabel("Route", fontsize=12)
    ax.set_title("BUS On-Time Performance by Route", fontsize=14, fontweight='bold')
    ax.legend(loc='lower right')
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"on_time_performance.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path




def generate_summary_csv(df: pd.DataFrame, suffix: str) -> Path:
    """Generate summary statistics CSV."""
    summary = df.groupby(["route_short_name", "stop_name"]).agg({
        "delay_minutes": ["mean", "std", "min", "max", "count"],
        "hour_of_day": lambda x: x.mode().iloc[0] if len(x) > 0 else None
    }).reset_index()
    
    summary.columns = [
        "Route", "Stop Name",
        "Avg Delay (min)", "Std Delay", "Min Delay", "Max Delay", "Sample Count",
        "Most Common Hour"
    ]
    
    output_path = RESULTS_DIR / f"schedule_times_summary.csv"
    summary.to_csv(output_path, index=False)
    return output_path


def print_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics to console."""
    print("\n" + "=" * 70)
    print("SCHEDULE TIMES ANALYSIS SUMMARY (BUS Routes)")
    print("=" * 70)
    
    print(f"\nTotal observations: {len(df):,}")
    print(f"Unique trips: {df['trip_instance_id'].nunique():,}")
    print(f"Unique routes: {df['route_short_name'].nunique()}")
    print(f"Unique stops: {df['stop_id'].nunique()}")
    
    print(f"\n--- Delay Statistics (minutes) ---")
    print(f"  Mean:   {df['delay_minutes'].mean():.2f}")
    print(f"  Median: {df['delay_minutes'].median():.2f}")
    print(f"  Std:    {df['delay_minutes'].std():.2f}")
    
    print(f"\n--- On-Time Performance ---")
    on_time = ((df["delay_minutes"] >= -3) & (df["delay_minutes"] <= 3)).sum()
    minor_early = ((df["delay_minutes"] >= -7) & (df["delay_minutes"] < -3)).sum()
    minor_late = ((df["delay_minutes"] > 3) & (df["delay_minutes"] <= 7)).sum()
    severe_early = (df["delay_minutes"] < -7).sum()
    severe_late = (df["delay_minutes"] > 7).sum()
    
    print(f"  On-time (±3 min):       {on_time:,} ({on_time/len(df)*100:.1f}%)")
    print(f"  Minor Early (-7 to -3): {minor_early:,} ({minor_early/len(df)*100:.1f}%)")
    print(f"  Minor Late (3 to 7):    {minor_late:,} ({minor_late/len(df)*100:.1f}%)")
    print(f"  Severe Early (<-7):     {severe_early:,} ({severe_early/len(df)*100:.1f}%)")
    print(f"  Severe Late (>7):       {severe_late:,} ({severe_late/len(df)*100:.1f}%)")
    
    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Schedule Times Analysis")
    parser.add_argument(
        "--clear-output",
        action="store_true",
        help="Clear existing output files before generating new ones"
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("SCHEDULE TIMES ANALYSIS")
    print("=" * 60)
    
    settings = load_settings()
    
    print("\nConnecting to database...")
    with get_connection(settings) as conn:
        print("Fetching schedule times data...")
        df = fetch_schedule_times_data(conn)
    
    if df.empty:
        print("⚠️  No schedule times data found.")
        print("   Make sure you have:")
        print("   1. Run the realtime ingestion (ingest_realtime.py)")
        print("   2. Run sql/run_sql.py to create materialized views:")
        print("      python3 realtime_analysis/queries/sql/run_sql.py")
        print("      (or run_all_analyses.py which runs this automatically)")
        print("   3. Trip updates with arrival_delay_seconds are available")
        print("\n   Note: Map visualizations are created manually in QGIS using qgis_realtime_* materialized views.")
        return 1
    
    print(f"✓ Retrieved {len(df):,} schedule time observations")
    
    if args.clear_output:
        print("\nClearing previous results...")
        clear_results_dir()
    else:
        print("\nPreserving existing results (use --clear-output to delete old files)")
    
    suffix = get_timestamp_suffix()
    
    print("Generating visualizations...")
    
    path = plot_delay_histogram(df, suffix)
    print(f"  ✓ Delay histogram: {path}")
    
    path = plot_delay_categories(df, suffix)
    print(f"  ✓ Delay categories: {path}")
    
    path = plot_delay_by_route(df, suffix)
    print(f"  ✓ Delay by route: {path}")
    
    path = plot_on_time_performance(df, suffix)
    print(f"  ✓ On-time performance: {path}")
    
    
    csv_path = generate_summary_csv(df, suffix)
    print(f"  ✓ Summary CSV: {csv_path}")
    
    print_statistics(df)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

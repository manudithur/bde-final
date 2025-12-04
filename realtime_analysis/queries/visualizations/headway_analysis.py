#!/usr/bin/env python3
"""
Headway Analysis for Vancouver Transit Realtime Data
Analyzes time gaps between consecutive vehicles on the same route.
Detects bus bunching (when vehicles arrive too close together) and gaps (too far apart).
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

from realtime_analysis.config import load_settings
from realtime_analysis.utils import get_connection

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "headway_analysis"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    """Clear all files in the results directory before generating new ones."""
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def get_timestamp_suffix() -> str:
    """Generate a timestamp suffix for output files."""
    return ""  # No timestamp suffix


def fetch_headway_data(conn) -> pd.DataFrame:
    """
    Fetch vehicle arrival data at stops to calculate headways - BUS routes only.
    Headway = time between consecutive vehicles at the same stop on the same route.
    Uses materialized view for better performance.
    """
    query = """
    SELECT
        h.route_id,
        h.route_short_name,
        h.route_long_name,
        h.stop_id,
        h.stop_name,
        h.stop_lat,
        h.stop_lon,
        h.trip_instance_id,
        h.prev_trip_instance_id,
        h.arrival_time,
        h.prev_arrival,
        h.headway_minutes,
        h.hour_of_day,
        h.day_of_week,
        h.day_type,
        h.time_period
    FROM realtime_headway_stats h
    JOIN routes r ON h.route_id = r.route_id
    WHERE r.route_type = '3'
    ORDER BY h.route_short_name, h.stop_id, h.arrival_time;
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        return df
    
    # Categorize headway quality
    # Bunching: < 3 min headway (vehicles too close)
    # Good: 3-15 min (ideal for frequent routes)
    # Gap: > 20 min (long wait for passengers)
    df["headway_category"] = pd.cut(
        df["headway_minutes"],
        bins=[0, 3, 10, 20, float("inf")],
        labels=["Bunched (<3 min)", "Good (3-10 min)", "Acceptable (10-20 min)", "Gap (>20 min)"]
    )
    
    return df


def fetch_scheduled_headways(conn) -> pd.DataFrame:
    """Fetch scheduled headways for comparison - BUS routes only."""
    query = """
    WITH trip_times AS (
        SELECT
            t.route_id,
            r.route_short_name,
            ts.stop_id,
            t.trip_id,
            ts.arrival_time
        FROM trips t
        JOIN transit_stops ts ON ts.trip_id = t.trip_id
        JOIN routes r ON r.route_id = t.route_id
        WHERE ts.arrival_time IS NOT NULL
            AND r.route_type = '3'
    ),
    with_prev AS (
        SELECT
            *,
            LAG(arrival_time) OVER (
                PARTITION BY route_id, stop_id
                ORDER BY arrival_time
            ) AS prev_arrival
        FROM trip_times
    )
    SELECT
        route_short_name,
        stop_id,
        AVG(EXTRACT(EPOCH FROM (arrival_time - prev_arrival)) / 60.0) AS scheduled_headway_minutes
    FROM with_prev
    WHERE prev_arrival IS NOT NULL
      AND EXTRACT(EPOCH FROM (arrival_time - prev_arrival)) > 0
      AND EXTRACT(EPOCH FROM (arrival_time - prev_arrival)) < 7200
    GROUP BY route_short_name, stop_id;
    """
    
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


def plot_headway_distribution(df: pd.DataFrame, suffix: str) -> Path:
    """Create histogram of headway distribution."""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Cap for visualization
    headway_capped = df["headway_minutes"].clip(0, 60)
    
    ax.hist(headway_capped, bins=60, color='#3498db', edgecolor='black', alpha=0.7)
    ax.axvline(df["headway_minutes"].median(), color='green', linestyle='--', 
               linewidth=2, label=f"Median: {df['headway_minutes'].median():.1f} min")
    ax.axvline(3, color='red', linestyle='-', linewidth=2, label="Bunching threshold (3 min)")
    ax.axvline(20, color='orange', linestyle='-', linewidth=2, label="Gap threshold (20 min)")
    
    ax.set_xlabel("Headway (minutes)", fontsize=12)
    ax.set_ylabel("Frequency", fontsize=12)
    ax.set_title("Distribution of Headways Between Consecutive BUS Vehicles", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"headway_distribution.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_headway_categories(df: pd.DataFrame, suffix: str) -> Path:
    """Create pie chart of headway categories."""
    fig, ax = plt.subplots(figsize=(10, 8))
    
    category_counts = df["headway_category"].value_counts()
    colors = ['#e74c3c', '#2ecc71', '#f39c12', '#9b59b6']
    
    wedges, texts, autotexts = ax.pie(
        category_counts.values,
        labels=category_counts.index,
        colors=colors[:len(category_counts)],
        autopct='%1.1f%%',
        startangle=90,
        explode=[0.05 if 'Bunched' in str(c) or 'Gap' in str(c) else 0 for c in category_counts.index]
    )
    
    ax.set_title("BUS Headway Quality Distribution", fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"headway_categories.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_headway_by_route(df: pd.DataFrame, suffix: str) -> Path:
    """Create bar chart of average headway and bunching rate by route."""
    route_stats = df.groupby("route_short_name").agg({
        "headway_minutes": ["mean", "std", "count"],
        "headway_category": lambda x: (x == "Bunched (<3 min)").sum() / len(x) * 100
    }).reset_index()
    route_stats.columns = ["Route", "Avg Headway", "Std Headway", "Count", "Bunching Rate %"]
    route_stats = route_stats[route_stats["Count"] >= 10]
    route_stats = route_stats.sort_values("Bunching Rate %", ascending=False).head(20)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    colors = plt.cm.Reds(route_stats["Bunching Rate %"] / route_stats["Bunching Rate %"].max())
    ax.barh(range(len(route_stats)), route_stats["Bunching Rate %"], color=colors, alpha=0.8)
    ax.set_yticks(range(len(route_stats)))
    ax.set_yticklabels(route_stats["Route"])
    
    ax.set_xlabel("Bunching Rate (%)", fontsize=12)
    ax.set_ylabel("Route", fontsize=12)
    ax.set_title("BUS Routes with Highest Bunching Rate", fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"bunching_by_route.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_headway_by_hour(df: pd.DataFrame, suffix: str) -> Path:
    """Create line chart of headway patterns by hour."""
    hourly = df.groupby("hour_of_day").agg({
        "headway_minutes": ["mean", "std"],
        "headway_category": lambda x: (x == "Bunched (<3 min)").sum() / len(x) * 100
    }).reset_index()
    hourly.columns = ["Hour", "Avg Headway", "Std Headway", "Bunching Rate"]
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    color1 = '#3498db'
    ax1.plot(hourly["Hour"], hourly["Avg Headway"], 'o-', color=color1, 
             linewidth=2, markersize=8, label='Avg Headway')
    ax1.fill_between(hourly["Hour"], 
                     hourly["Avg Headway"] - hourly["Std Headway"],
                     hourly["Avg Headway"] + hourly["Std Headway"],
                     alpha=0.2, color=color1)
    ax1.set_xlabel("Hour of Day", fontsize=12)
    ax1.set_ylabel("Average Headway (min)", fontsize=12, color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.set_xticks(range(0, 24))
    
    ax2 = ax1.twinx()
    color2 = '#e74c3c'
    ax2.bar(hourly["Hour"], hourly["Bunching Rate"], alpha=0.3, color=color2, label='Bunching Rate')
    ax2.set_ylabel("Bunching Rate (%)", fontsize=12, color=color2)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    ax1.set_title("BUS Headway Patterns by Hour of Day", fontsize=14, fontweight='bold')
    ax1.grid(alpha=0.3)
    
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"headway_by_hour.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_headway_by_time_period(df: pd.DataFrame, suffix: str) -> Path:
    """Compare headway quality across time periods."""
    period_order = ["Night", "Morning Rush", "Midday", "Evening Rush", "Evening"]
    
    period_stats = df.groupby("time_period").agg({
        "headway_minutes": "mean",
        "headway_category": lambda x: (x == "Bunched (<3 min)").sum() / len(x) * 100
    }).reset_index()
    period_stats.columns = ["Time Period", "Avg Headway", "Bunching Rate"]
    period_stats["Time Period"] = pd.Categorical(period_stats["Time Period"], categories=period_order, ordered=True)
    period_stats = period_stats.sort_values("Time Period")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x = range(len(period_stats))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], period_stats["Avg Headway"], width, 
                   label='Avg Headway (min)', color='#3498db', alpha=0.8)
    
    ax2 = ax.twinx()
    bars2 = ax2.bar([i + width/2 for i in x], period_stats["Bunching Rate"], width,
                    label='Bunching Rate (%)', color='#e74c3c', alpha=0.8)
    
    ax.set_xticks(x)
    ax.set_xticklabels(period_stats["Time Period"])
    ax.set_xlabel("Time Period", fontsize=12)
    ax.set_ylabel("Average Headway (min)", fontsize=12, color='#3498db')
    ax2.set_ylabel("Bunching Rate (%)", fontsize=12, color='#e74c3c')
    
    ax.set_title("BUS Headway Quality by Time Period", fontsize=14, fontweight='bold')
    ax.legend(loc='upper left')
    ax2.legend(loc='upper right')
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"headway_by_time_period.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path




def generate_summary_csv(df: pd.DataFrame, suffix: str) -> Path:
    """Generate summary CSV of headway data by route and stop."""
    summary = df.groupby(["route_short_name", "stop_name", "time_period"]).agg({
        "headway_minutes": ["mean", "std", "min", "max", "count"],
        "headway_category": lambda x: (x == "Bunched (<3 min)").sum() / len(x) * 100
    }).reset_index()
    
    summary.columns = [
        "Route", "Stop", "Time Period",
        "Avg Headway", "Std Headway", "Min Headway", "Max Headway", "Count",
        "Bunching Rate %"
    ]
    
    summary = summary.sort_values("Bunching Rate %", ascending=False)
    
    output_path = RESULTS_DIR / f"headway_summary.csv"
    summary.to_csv(output_path, index=False)
    return output_path


def print_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics to console."""
    print("\n" + "=" * 70)
    print("HEADWAY ANALYSIS SUMMARY (BUS Bunching)")
    print("=" * 70)
    
    print(f"\nTotal headway observations: {len(df):,}")
    print(f"Unique routes: {df['route_short_name'].nunique()}")
    print(f"Unique stops: {df['stop_id'].nunique()}")
    
    print(f"\n--- Headway Statistics ---")
    print(f"  Mean headway:   {df['headway_minutes'].mean():.2f} min")
    print(f"  Median headway: {df['headway_minutes'].median():.2f} min")
    print(f"  Std:            {df['headway_minutes'].std():.2f} min")
    
    print(f"\n--- Service Quality ---")
    bunched = (df["headway_category"] == "Bunched (<3 min)").sum()
    good = (df["headway_category"] == "Good (3-10 min)").sum()
    acceptable = (df["headway_category"] == "Acceptable (10-20 min)").sum()
    gap = (df["headway_category"] == "Gap (>20 min)").sum()
    
    print(f"  Bunched (<3 min):     {bunched:,} ({bunched/len(df)*100:.1f}%)")
    print(f"  Good (3-10 min):      {good:,} ({good/len(df)*100:.1f}%)")
    print(f"  Acceptable (10-20):   {acceptable:,} ({acceptable/len(df)*100:.1f}%)")
    print(f"  Gap (>20 min):        {gap:,} ({gap/len(df)*100:.1f}%)")
    
    # Worst routes for bunching
    print(f"\n--- Top 5 Routes with Highest Bunching ---")
    route_bunching = df.groupby("route_short_name").apply(
        lambda x: (x["headway_category"] == "Bunched (<3 min)").sum() / len(x) * 100
    ).sort_values(ascending=False).head(5)
    for route, rate in route_bunching.items():
        print(f"  Route {route}: {rate:.1f}% bunching rate")
    
    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Headway Analysis")
    parser.add_argument(
        "--clear-output",
        action="store_true",
        help="Clear existing output files before generating new ones"
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("HEADWAY ANALYSIS (Bus Bunching)")
    print("=" * 60)
    
    settings = load_settings()
    
    print("\nConnecting to database...")
    with get_connection(settings) as conn:
        print("Fetching headway data...")
        df = fetch_headway_data(conn)
    
    if df.empty:
        print("⚠️  No headway data found.")
        print("   Make sure you have:")
        print("   1. Run the realtime ingestion (ingest_realtime.py)")
        print("   2. Run sql/run_sql.py to create materialized views:")
        print("      python3 realtime_analysis/queries/sql/run_sql.py")
        print("      (or run_all_analyses.py which runs this automatically)")
        print("   3. Trip updates with arrival times at stops")
        print("\n   Note: Map visualizations are created manually in QGIS using qgis_realtime_* materialized views.")
        return 1
    
    print(f"✓ Retrieved {len(df):,} headway observations")
    
    if args.clear_output:
        print("\nClearing previous results...")
        clear_results_dir()
    else:
        print("\nPreserving existing results (use --clear-output to delete old files)")
    
    suffix = get_timestamp_suffix()
    
    print("Generating visualizations...")
    
    path = plot_headway_distribution(df, suffix)
    print(f"  ✓ Headway distribution: {path}")
    
    path = plot_headway_categories(df, suffix)
    print(f"  ✓ Headway categories: {path}")
    
    path = plot_headway_by_route(df, suffix)
    print(f"  ✓ Bunching by route: {path}")
    
    path = plot_headway_by_hour(df, suffix)
    print(f"  ✓ Headway by hour: {path}")
    
    path = plot_headway_by_time_period(df, suffix)
    print(f"  ✓ Headway by time period: {path}")
    
    csv_path = generate_summary_csv(df, suffix)
    print(f"  ✓ Summary CSV: {csv_path}")
    
    print_statistics(df)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


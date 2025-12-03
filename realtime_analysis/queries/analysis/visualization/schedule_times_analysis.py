#!/usr/bin/env python3
"""
Schedule Times Analysis for Vancouver Transit Realtime Data
Compares scheduled arrival/departure times with actual observed times from GTFS-Realtime.
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

import plotly.express as px

# Add parent directories for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from realtime_analysis.utility.config import load_settings
from realtime_analysis.utility.utils import get_connection

RESULTS_DIR = Path(__file__).resolve().parent.parent.parent / "results" / "schedule_times"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    """Clear all files in the results directory before generating new ones."""
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def get_timestamp_suffix() -> str:
    """Generate a timestamp suffix for output files."""
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def fetch_schedule_times_data(conn) -> pd.DataFrame:
    """Fetch arrival/departure time comparison between scheduled and actual.
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
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        scheduled_arrival_interval,
        actual_arrival,
        actual_departure,
        arrival_delay_seconds,
        departure_delay_seconds,
        delay_minutes,
        hour_of_day,
        day_of_week,
        day_type
    FROM realtime_schedule_times
    ORDER BY trip_instance_id, stop_sequence;
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        return df
    
    df["delay_category"] = pd.cut(
        df["delay_minutes"],
        bins=[-float("inf"), -5, -1, 1, 5, 10, float("inf")],
        labels=["Very Early (>5 min)", "Early (1-5 min)", "On Time (±1 min)", 
                "Late (1-5 min)", "Very Late (5-10 min)", "Severely Late (>10 min)"]
    )
    
    return df


def plot_delay_histogram(df: pd.DataFrame, suffix: str) -> Path:
    """Create histogram of arrival delays."""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    delay_capped = df["delay_minutes"].clip(-20, 30)
    ax.hist(delay_capped, bins=60, color='#1f77b4', edgecolor='black', alpha=0.7)
    ax.axvline(0, color='red', linestyle='-', linewidth=2, label="On Time")
    ax.axvline(df["delay_minutes"].mean(), color='orange', linestyle='--', 
               linewidth=2, label=f"Mean: {df['delay_minutes'].mean():.1f} min")
    
    ax.set_xlabel("Delay (minutes)", fontsize=12)
    ax.set_ylabel("Frequency", fontsize=12)
    ax.set_title("Distribution of Arrival Delays", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_histogram_{suffix}.png"
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
    
    ax.set_title("Delay Categories Distribution", fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_categories_{suffix}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_delay_by_hour(df: pd.DataFrame, suffix: str) -> Path:
    """Create box plot of delays by hour."""
    fig, ax = plt.subplots(figsize=(14, 6))
    
    hours = sorted(df["hour_of_day"].unique())
    data = [df[df["hour_of_day"] == h]["delay_minutes"].dropna().values for h in hours]
    
    bp = ax.boxplot(data, labels=[str(int(h)) for h in hours], patch_artist=True)
    for patch in bp['boxes']:
        patch.set_facecolor('#ff7f0e')
        patch.set_alpha(0.7)
    
    ax.axhline(0, color='red', linestyle='--', linewidth=1)
    ax.set_xlabel("Hour of Day", fontsize=12)
    ax.set_ylabel("Delay (minutes)", fontsize=12)
    ax.set_title("Delay Distribution by Hour of Day", fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_by_hour_{suffix}.png"
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
    ax.set_title("Average Delay by Route (Top 20)", fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_by_route_{suffix}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_delay_heatmap(df: pd.DataFrame, suffix: str) -> Path:
    """Create heatmap of delays by hour and day."""
    pivot = df.pivot_table(
        values="delay_minutes",
        index="day_of_week",
        columns="hour_of_day",
        aggfunc="mean"
    )
    
    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    pivot.index = [day_names[int(i)] for i in pivot.index]
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    im = ax.imshow(pivot.values, cmap='RdYlGn_r', aspect='auto')
    
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([str(int(c)) for c in pivot.columns])
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    
    ax.set_xlabel("Hour of Day", fontsize=12)
    ax.set_ylabel("Day of Week", fontsize=12)
    ax.set_title("Average Delay Heatmap (minutes)", fontsize=14, fontweight='bold')
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Avg Delay (min)")
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_heatmap_{suffix}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_on_time_performance(df: pd.DataFrame, suffix: str) -> Path:
    """Create on-time performance by route."""
    df["on_time"] = (df["delay_minutes"] >= -1) & (df["delay_minutes"] <= 5)
    df["early"] = df["delay_minutes"] < -1
    df["late"] = df["delay_minutes"] > 5
    
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
    
    ax.barh(y_pos, route_otp["Late"] * 100, color='#e74c3c', label='Late (>5 min)')
    ax.barh(y_pos, route_otp["On-Time"] * 100, left=route_otp["Late"] * 100, 
            color='#2ecc71', label='On-Time (±5 min)')
    ax.barh(y_pos, route_otp["Early"] * 100, 
            left=(route_otp["Late"] + route_otp["On-Time"]) * 100,
            color='#3498db', label='Early (>1 min)')
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(route_otp["Route"])
    ax.set_xlabel("Percentage", fontsize=12)
    ax.set_ylabel("Route", fontsize=12)
    ax.set_title("On-Time Performance by Route", fontsize=14, fontweight='bold')
    ax.legend(loc='lower right')
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"on_time_performance_{suffix}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_delay_map(df: pd.DataFrame, suffix: str):
    """Create map of average delay at each stop as PNG (sin HTML)."""
    stop_delays = df.groupby(["stop_id", "stop_name", "stop_lat", "stop_lon"]).agg({
        "delay_minutes": ["mean", "std", "count"]
    }).reset_index()
    stop_delays.columns = ["stop_id", "stop_name", "lat", "lon", "avg_delay", "std_delay", "count"]
    stop_delays = stop_delays[stop_delays["count"] >= 3]
    stop_delays = stop_delays.dropna(subset=["lat", "lon"])
    
    if stop_delays.empty:
        return None
    
    # Interactive map (solo para generar imagen estática PNG)
    fig = px.scatter_mapbox(
        stop_delays,
        lat="lat",
        lon="lon",
        color="avg_delay",
        size="count",
        size_max=20,
        hover_name="stop_name",
        hover_data={"avg_delay": ":.1f", "std_delay": ":.1f", "count": True},
        color_continuous_scale="RdYlGn_r",
        color_continuous_midpoint=0,
        title="Average Delay by Stop Location"
    )
    
    fig.update_layout(
        mapbox_style="open-street-map",
        mapbox_center={
            "lat": stop_delays["lat"].mean(),
            "lon": stop_delays["lon"].mean(),
        },
        mapbox_zoom=11,
        height=700,
        width=1000,
        margin=dict(l=0, r=0, t=50, b=0),
    )
    
    # PNG snapshot únicamente
    try:
        png_path = RESULTS_DIR / f"delay_map_{suffix}.png"
        fig.write_image(png_path, scale=2)
    except Exception as e:
        print(f"  ⚠ Could not save map PNG (install kaleido): {e}")
        png_path = None
    
    return png_path


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
    
    output_path = RESULTS_DIR / f"schedule_times_summary_{suffix}.csv"
    summary.to_csv(output_path, index=False)
    return output_path


def print_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics to console."""
    print("\n" + "=" * 70)
    print("SCHEDULE TIMES ANALYSIS SUMMARY")
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
    on_time = ((df["delay_minutes"] >= -1) & (df["delay_minutes"] <= 5)).sum()
    early = (df["delay_minutes"] < -1).sum()
    late = (df["delay_minutes"] > 5).sum()
    
    print(f"  On-time (-1 to +5 min): {on_time:,} ({on_time/len(df)*100:.1f}%)")
    print(f"  Early (< -1 min):       {early:,} ({early/len(df)*100:.1f}%)")
    print(f"  Late (> 5 min):         {late:,} ({late/len(df)*100:.1f}%)")
    
    print("\n" + "=" * 70)


def main():
    """Main entry point."""
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
        print("   2. Run realtime_queries.sql to create materialized views:")
        print("      cat realtime_analysis/queries/analysis/realtime_queries.sql | psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE")
        print("   3. Trip updates with arrival_delay_seconds are available")
        return 1
    
    print(f"✓ Retrieved {len(df):,} schedule time observations")
    
    print("\nClearing previous results...")
    clear_results_dir()
    
    suffix = get_timestamp_suffix()
    
    print("Generating visualizations...")
    
    path = plot_delay_histogram(df, suffix)
    print(f"  ✓ Delay histogram: {path}")
    
    path = plot_delay_categories(df, suffix)
    print(f"  ✓ Delay categories: {path}")
    
    path = plot_delay_by_hour(df, suffix)
    print(f"  ✓ Delay by hour: {path}")
    
    path = plot_delay_by_route(df, suffix)
    print(f"  ✓ Delay by route: {path}")
    
    path = plot_delay_heatmap(df, suffix)
    print(f"  ✓ Delay heatmap: {path}")
    
    path = plot_on_time_performance(df, suffix)
    print(f"  ✓ On-time performance: {path}")
    
    png_path = plot_delay_map(df, suffix)
    if png_path:
        print(f"  ✓ Delay map (PNG): {png_path}")
    
    csv_path = generate_summary_csv(df, suffix)
    print(f"  ✓ Summary CSV: {csv_path}")
    
    print_statistics(df)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

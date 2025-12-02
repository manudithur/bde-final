#!/usr/bin/env python3
"""
Delay Segments Analysis for Vancouver Transit Realtime Data
Identifies segments with the highest delays and analyzes when/where traffic congestion occurs.
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
import json

import plotly.graph_objects as go

# Add parent directories for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from realtime_analysis.config import load_settings
from realtime_analysis.utils import get_connection

RESULTS_DIR = Path(__file__).resolve().parent.parent.parent / "results" / "delay_segments"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    """Clear all files in the results directory before generating new ones."""
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def get_timestamp_suffix() -> str:
    """Generate a timestamp suffix for output files."""
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def fetch_segment_delays(conn) -> pd.DataFrame:
    """Fetch segment-level delay data comparing scheduled vs actual travel times.
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
        from_seq,
        to_seq,
        from_stop_id,
        to_stop_id,
        from_stop_name,
        to_stop_name,
        from_lat,
        from_lon,
        to_lat,
        to_lon,
        segment_length_m,
        seg_geom,
        seg_geojson,
        scheduled_seconds,
        actual_seconds,
        from_delay,
        to_delay,
        segment_delay_change,
        segment_delay_minutes,
        hour_of_day,
        day_of_week,
        day_type,
        time_period
    FROM realtime_delay_analysis
    WHERE segment_delay_minutes BETWEEN -30 AND 60
    ORDER BY trip_instance_id, from_seq;
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
        bins=[-float("inf"), 0, 2, 5, 10, float("inf")],
        labels=["No Delay", "Minor (0-2)", "Moderate (2-5)", 
                "Significant (5-10)", "Severe (>10)"]
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
    ax.axhline(0, color='black', linestyle='-', linewidth=0.5)
    
    ax.set_xlabel("Time Period", fontsize=12)
    ax.set_ylabel("Average Delay (minutes)", fontsize=12)
    ax.set_title("Average Segment Delay by Time Period", fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_by_time_period_{suffix}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_delay_by_hour(df: pd.DataFrame, suffix: str) -> Path:
    """Create line chart of delay by hour of day."""
    hourly = df.groupby("hour_of_day").agg({
        "segment_delay_minutes": ["mean", "std"]
    }).reset_index()
    hourly.columns = ["hour", "mean", "std"]
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(hourly["hour"], hourly["mean"], 'o-', color='#e74c3c', 
            linewidth=2, markersize=8)
    ax.fill_between(hourly["hour"], 
                    hourly["mean"] - hourly["std"],
                    hourly["mean"] + hourly["std"],
                    alpha=0.2, color='#e74c3c')
    ax.axhline(0, color='green', linestyle='--', linewidth=1, label='No Delay')
    
    ax.set_xlabel("Hour of Day", fontsize=12)
    ax.set_ylabel("Average Delay (minutes)", fontsize=12)
    ax.set_title("Segment Delay by Hour of Day", fontsize=14, fontweight='bold')
    ax.set_xticks(range(0, 24))
    ax.legend()
    ax.grid(alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_by_hour_{suffix}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_delay_heatmap(df: pd.DataFrame, suffix: str) -> Path:
    """Create heatmap of delays by hour and day of week."""
    pivot = df.pivot_table(
        values="segment_delay_minutes",
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
    ax.set_title("Traffic Delay Patterns: When Delays Occur", fontsize=14, fontweight='bold')
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Avg Delay (min)")
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_heatmap_{suffix}.png"
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
    ax.set_title("Top 20 Most Delayed Segments", fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    ax.invert_yaxis()
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"worst_segments_{suffix}.png"
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
    ax.set_title("Delay Severity Distribution", fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"delay_severity_{suffix}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_weekday_vs_weekend(df: pd.DataFrame, suffix: str) -> Path:
    """Compare weekday vs weekend delays."""
    day_type_delays = df.groupby("day_type")["segment_delay_minutes"].mean()
    
    fig, ax = plt.subplots(figsize=(8, 6))
    
    ax.bar(range(len(day_type_delays)), day_type_delays.values, 
           color=['#3498db', '#f39c12'], alpha=0.8)
    ax.set_xticks(range(len(day_type_delays)))
    ax.set_xticklabels(day_type_delays.index)
    ax.axhline(0, color='black', linestyle='-', linewidth=0.5)
    
    ax.set_xlabel("Day Type", fontsize=12)
    ax.set_ylabel("Average Delay (minutes)", fontsize=12)
    ax.set_title("Weekday vs Weekend Delays", fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTS_DIR / f"weekday_vs_weekend_{suffix}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path


def plot_delay_hotspots_map(df: pd.DataFrame, suffix: str):
    """Create map showing delay hotspots as PNG (sin HTML)."""
    segment_stats = df.groupby(
        ["from_stop_id", "to_stop_id", "from_stop_name", "to_stop_name",
         "from_lat", "from_lon", "to_lat", "to_lon"]
    ).agg({
        "segment_delay_minutes": ["mean", "count"],
        "seg_geojson": "first"
    }).reset_index()
    segment_stats.columns = [
        "from_stop_id", "to_stop_id", "from_stop", "to_stop",
        "from_lat", "from_lon", "to_lat", "to_lon",
        "avg_delay", "count", "geojson"
    ]
    
    delayed = segment_stats[segment_stats["avg_delay"] > 1]
    delayed = delayed.dropna(subset=["from_lat", "from_lon", "to_lat", "to_lon"])
    
    if delayed.empty:
        return None
    
    fig = go.Figure()
    
    for _, row in delayed.iterrows():
        if pd.notna(row["geojson"]):
            try:
                geo = json.loads(row["geojson"])
                coords = geo.get("coordinates", [])
                if coords:
                    if geo["type"] == "LineString":
                        lons, lats = zip(*coords)
                    else:
                        lons, lats = zip(*[c for line in coords for c in line])
                    
                    if row["avg_delay"] > 5:
                        color = "#e74c3c"
                    elif row["avg_delay"] > 3:
                        color = "#f39c12"
                    else:
                        color = "#f1c40f"
                    
                    fig.add_trace(go.Scattermapbox(
                        lat=lats,
                        lon=lons,
                        mode="lines",
                        line=dict(width=4, color=color),
                        name=f"{row['from_stop']} → {row['to_stop']}",
                        hovertemplate=(
                            f"{row['from_stop']} → {row['to_stop']}<br>"
                            f"Avg Delay: {row['avg_delay']:.1f} min<br>"
                            f"Samples: {int(row['count'])}<extra></extra>"
                        )
                    ))
            except:
                pass
    
    fig.update_layout(
        title="Delay Hotspots Map (segments with >1 min avg delay)",
        mapbox_style="open-street-map",
        mapbox_center={
            "lat": delayed["from_lat"].mean(),
            "lon": delayed["from_lon"].mean(),
        },
        mapbox_zoom=11,
        height=700,
        width=1000,
        margin=dict(l=0, r=0, t=50, b=0),
        showlegend=False,
    )
    
    # Guardar solo PNG
    try:
        png_path = RESULTS_DIR / f"delay_hotspots_map_{suffix}.png"
        fig.write_image(png_path, scale=2)
    except Exception as e:
        print(f"  ⚠ Could not save map PNG: {e}")
        png_path = None
    
    return png_path


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
    
    output_path = RESULTS_DIR / f"delay_segments_summary_{suffix}.csv"
    summary.to_csv(output_path, index=False)
    return output_path


def print_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics to console."""
    print("\n" + "=" * 70)
    print("DELAY SEGMENTS ANALYSIS SUMMARY (Traffic Analysis)")
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
        print("   3. Trip updates with arrival times")
        return 1
    
    print(f"✓ Retrieved {len(df):,} segment delay observations")
    
    print("\nClearing previous results...")
    clear_results_dir()
    
    suffix = get_timestamp_suffix()
    
    print("Generating visualizations...")
    
    path = plot_delay_by_time_period(df, suffix)
    print(f"  ✓ Delay by time period: {path}")
    
    path = plot_delay_by_hour(df, suffix)
    print(f"  ✓ Delay by hour: {path}")
    
    path = plot_delay_heatmap(df, suffix)
    print(f"  ✓ Delay heatmap: {path}")
    
    path = plot_worst_segments(df, suffix)
    print(f"  ✓ Worst segments: {path}")
    
    path = plot_delay_severity(df, suffix)
    print(f"  ✓ Delay severity: {path}")
    
    path = plot_weekday_vs_weekend(df, suffix)
    print(f"  ✓ Weekday vs weekend: {path}")
    
    png_path = plot_delay_hotspots_map(df, suffix)
    if png_path:
        print(f"  ✓ Hotspots map (PNG): {png_path}")
    
    csv_path = generate_summary_csv(df, suffix)
    print(f"  ✓ Summary CSV: {csv_path}")
    
    print_statistics(df)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

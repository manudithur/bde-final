#!/usr/bin/env python3
"""
Load vs Delay Analysis for Vancouver Transit Realtime Data
Correlates vehicle occupancy (GTFS-RT occupancy_status) with segment delays.
"""

import sys
import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

matplotlib.use("Agg")

# Add parent directories for imports
script_dir = Path(__file__).resolve()
project_root = script_dir.parents[3]  # bde-final
sys.path.insert(0, str(project_root))

from realtime_analysis.utility.config import load_settings
from realtime_analysis.utility.utils import get_connection

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "load_delay"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def fetch_load_delay_data(conn) -> pd.DataFrame:
    """
    Load segment delays joined with latest occupancy bucket.
    Focus on BUS routes only and keep reasonable delay bounds.
    """
    query = """
    SELECT
        route_short_name,
        occupancy_bucket,
        occupancy_status,
        segment_delay_minutes,
        hour_of_day,
        time_period,
        day_type
    FROM realtime_load_delay
    WHERE route_type = '3'
      AND occupancy_bucket IS NOT NULL
      AND segment_delay_minutes BETWEEN -30 AND 60
    """
    return pd.read_sql_query(query, conn)


def plot_delay_by_bucket(df: pd.DataFrame) -> Path:
    fig, ax = plt.subplots(figsize=(10, 6))
    order = ["Plenty of seats", "Few seats left", "Standing room", "Crowded", "Unknown"]
    ordered = [b for b in order if b in df["occupancy_bucket"].unique()]
    df.boxplot(column="segment_delay_minutes", by="occupancy_bucket", ax=ax, grid=False)
    ax.set_title("Segment Delay by Occupancy Bucket (BUS)")
    ax.set_ylabel("Delay (minutes)")
    ax.set_xlabel("Occupancy Bucket")
    plt.suptitle("")
    plt.tight_layout()
    path = RESULTS_DIR / "delay_by_bucket.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    return path


def plot_bucket_by_time(df: pd.DataFrame) -> Path:
    pivot = df.pivot_table(
        values="segment_delay_minutes",
        index="occupancy_bucket",
        columns="time_period",
        aggfunc="mean",
    )
    pivot = pivot.reindex(sorted(pivot.index, key=lambda x: pivot.loc[x].mean()))

    fig, ax = plt.subplots(figsize=(12, 6))
    pivot.T.plot(kind="bar", ax=ax)
    ax.set_title("Average Delay by Occupancy Bucket and Time Period (BUS)")
    ax.set_ylabel("Average Delay (minutes)")
    ax.set_xlabel("Time Period")
    ax.legend(title="Occupancy")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    path = RESULTS_DIR / "delay_bucket_time_period.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    return path


def plot_crowded_routes(df: pd.DataFrame) -> Path:
    crowded = df[df["occupancy_bucket"] == "Crowded"]
    if crowded.empty:
        return None
    route_stats = crowded.groupby("route_short_name")["segment_delay_minutes"].mean()
    route_stats = route_stats.sort_values(ascending=False).head(15)

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(route_stats.index, route_stats.values, color="#e74c3c", alpha=0.8)
    ax.set_xlabel("Average Delay (minutes)")
    ax.set_title("Worst BUS Routes When Crowded (Avg Segment Delay)")
    ax.grid(axis="x", alpha=0.3)
    ax.invert_yaxis()
    plt.tight_layout()
    path = RESULTS_DIR / "crowded_routes.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    return path


def generate_summary_csv(df: pd.DataFrame) -> Path:
    summary = df.groupby(["route_short_name", "occupancy_bucket"]).agg(
        avg_delay=("segment_delay_minutes", "mean"),
        std_delay=("segment_delay_minutes", "std"),
        samples=("segment_delay_minutes", "count"),
    ).reset_index()
    summary = summary.sort_values("avg_delay", ascending=False)
    path = RESULTS_DIR / "load_delay_summary.csv"
    summary.to_csv(path, index=False)
    return path


def print_statistics(df: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("LOAD VS DELAY SUMMARY (BUS)")
    print("=" * 60)
    print(f"Observations: {len(df):,}")
    print(f"Routes: {df['route_short_name'].nunique()}")
    print("\nAverage delay by occupancy bucket:")
    bucket_stats = df.groupby("occupancy_bucket")["segment_delay_minutes"].mean().sort_values(ascending=False)
    for bucket, val in bucket_stats.items():
        print(f"  {bucket}: {val:.2f} min")
    crowded = df[df["occupancy_bucket"] == "Crowded"]
    if not crowded.empty:
        print(f"\nCrowded samples: {len(crowded):,} (avg delay {crowded['segment_delay_minutes'].mean():.2f} min)")
    print("\n" + "=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(description="Load vs Delay Analysis")
    parser.add_argument("--clear-output", action="store_true", help="Clear existing outputs before writing new ones")
    args = parser.parse_args()

    print("=" * 60)
    print("LOAD VS DELAY ANALYSIS")
    print("=" * 60)

    settings = load_settings()
    with get_connection(settings) as conn:
        print("Fetching load/delay data...")
        df = fetch_load_delay_data(conn)

    if df.empty:
        print("⚠️  No load/delay data found. Ensure:")
        print("    1) Realtime ingestion captured occupancy_status")
        print("    2) realtime_queries.sql has been run to build realtime_load_delay")
        print("    3) Trip updates and vehicle positions overlap in time")
        return 1

    print(f"✓ Retrieved {len(df):,} observations")

    if args.clear_output:
        print("Clearing previous results...")
        clear_results_dir()
    else:
        print("Preserving existing results (use --clear-output to delete old files)")

    print("Generating visualizations...")
    path = plot_delay_by_bucket(df)
    print(f"  ✓ Delay by occupancy bucket: {path}")

    path = plot_bucket_by_time(df)
    print(f"  ✓ Delay by bucket/time: {path}")

    crowded_path = plot_crowded_routes(df)
    if crowded_path:
        print(f"  ✓ Crowded route ranking: {crowded_path}")
    else:
        print("  ℹ No 'Crowded' samples to plot routes.")

    csv_path = generate_summary_csv(df)
    print(f"  ✓ Summary CSV: {csv_path}")

    print_statistics(df)
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



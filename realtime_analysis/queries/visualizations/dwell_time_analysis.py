#!/usr/bin/env python3
"""
Dwell Time Analysis for Vancouver Transit Realtime Data
Identifies stops with longest dwell times and temporal patterns.
"""

import sys
import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

matplotlib.use("Agg")

# Add project root for imports
script_dir = Path(__file__).resolve()
project_root = script_dir.parents[3]
sys.path.insert(0, str(project_root))

from realtime_analysis.utility.config import load_settings
from realtime_analysis.utility.utils import get_connection

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "dwell_times"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def fetch_dwell_data(conn) -> pd.DataFrame:
    """
    Fetch dwell durations from the materialized view.
    """
    query = """
    SELECT
        route_short_name,
        route_long_name,
        route_type,
        route_id,
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        dwell_minutes,
        hour_of_day,
        day_of_week,
        day_type,
        time_period
    FROM realtime_dwell_times
    WHERE dwell_minutes IS NOT NULL
      AND dwell_minutes BETWEEN 0.1 AND 15
    """
    return pd.read_sql_query(query, conn)


def plot_dwell_histogram(df: pd.DataFrame) -> Path:
    fig, ax = plt.subplots(figsize=(10, 6))
    capped = df["dwell_minutes"].clip(0, 15)
    ax.hist(capped, bins=50, color="#1f77b4", edgecolor="black", alpha=0.7)
    ax.axvline(df["dwell_minutes"].mean(), color="orange", linestyle="--", linewidth=2, label=f"Mean: {df['dwell_minutes'].mean():.1f} min")
    ax.set_xlabel("Dwell time (minutes)")
    ax.set_ylabel("Frequency")
    ax.set_title("Distribution of BUS dwell times")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    out = RESULTS_DIR / "dwell_histogram.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    return out


def plot_top_stops(df: pd.DataFrame) -> Path:
    stop_stats = (
        df.groupby(["stop_id", "stop_name"])
        .agg(avg_dwell=("dwell_minutes", "mean"), count=("dwell_minutes", "count"))
        .reset_index()
    )
    stop_stats = stop_stats[stop_stats["count"] >= 10]
    top = stop_stats.nlargest(15, "avg_dwell")

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.barh(range(len(top)), top["avg_dwell"], color="#e74c3c", alpha=0.8)
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels(top["stop_name"])
    ax.invert_yaxis()
    ax.set_xlabel("Average dwell (minutes)")
    ax.set_title("Stops with longest average dwell (>=10 samples)")
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    out = RESULTS_DIR / "top_dwell_stops.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    return out


def plot_dwell_by_hour(df: pd.DataFrame) -> Path:
    hourly = (
        df.groupby("hour_of_day")["dwell_minutes"]
        .agg(["mean", "std"])
        .reset_index()
        .rename(columns={"mean": "avg", "std": "std"})
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(hourly["hour_of_day"], hourly["avg"], "o-", color="#3498db", linewidth=2, markersize=8)
    ax.fill_between(
        hourly["hour_of_day"],
        hourly["avg"] - hourly["std"],
        hourly["avg"] + hourly["std"],
        alpha=0.2,
        color="#3498db",
    )
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Average dwell (minutes)")
    ax.set_title("Average dwell by hour of day")
    ax.set_xticks(range(0, 24))
    ax.grid(alpha=0.3)
    plt.tight_layout()
    out = RESULTS_DIR / "dwell_by_hour.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    return out


def generate_summary_csv(df: pd.DataFrame) -> Path:
    summary = (
        df.groupby(["route_short_name", "stop_name", "time_period"])
        .agg(
            avg_dwell=("dwell_minutes", "mean"),
            std_dwell=("dwell_minutes", "std"),
            p90=("dwell_minutes", lambda x: x.quantile(0.9)),
            samples=("dwell_minutes", "count"),
        )
        .reset_index()
        .sort_values("avg_dwell", ascending=False)
    )
    out = RESULTS_DIR / "dwell_summary.csv"
    summary.to_csv(out, index=False)
    return out


def print_stats(df: pd.DataFrame) -> None:
    print("\n" + "=" * 70)
    print("DWELL TIME ANALYSIS SUMMARY (BUS)")
    print("=" * 70)
    print(f"Observations: {len(df):,}")
    print(f"Stops: {df['stop_id'].nunique():,}")
    print(f"Routes: {df['route_short_name'].nunique()}")
    print(f"Mean dwell:   {df['dwell_minutes'].mean():.2f} min")
    print(f"Median dwell: {df['dwell_minutes'].median():.2f} min")
    print(f"Std:          {df['dwell_minutes'].std():.2f} min")
    print("\nTop 5 stops by avg dwell (>=10 samples):")
    stop_stats = (
        df.groupby(["stop_name"])
        .agg(avg=("dwell_minutes", "mean"), count=("dwell_minutes", "count"))
        .reset_index()
    )
    stop_stats = stop_stats[stop_stats["count"] >= 10].nlargest(5, "avg")
    for _, row in stop_stats.iterrows():
        print(f"  {row['stop_name']}: {row['avg']:.2f} min ({int(row['count'])} samples)")
    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Dwell Time Analysis")
    parser.add_argument("--clear-output", action="store_true", help="Clear existing output files before generating new ones")
    args = parser.parse_args()

    settings = load_settings()
    print("=" * 60)
    print("DWELL TIME ANALYSIS")
    print("=" * 60)

    with get_connection(settings) as conn:
        print("Fetching dwell data...")
        df = fetch_dwell_data(conn)

    if df.empty:
        print("⚠️  No dwell data found. Run realtime ingestion and sql/run_sql.py first.")
        return 1

    if args.clear_output:
        print("Clearing previous results...")
        clear_results_dir()
    else:
        print("Preserving existing results (use --clear-output to delete old files)")

    print("Generating visualizations...")
    print(f"  ✓ {plot_dwell_histogram(df)}")
    print(f"  ✓ {plot_top_stops(df)}")
    print(f"  ✓ {plot_dwell_by_hour(df)}")

    csv_path = generate_summary_csv(df)
    print(f"  ✓ Summary CSV: {csv_path}")

    print_stats(df)
    print("\n✓ Analysis complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



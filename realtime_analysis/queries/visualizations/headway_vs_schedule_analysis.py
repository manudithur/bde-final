#!/usr/bin/env python3
import sys
import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

matplotlib.use("Agg")

# Add project root to path
script_dir = Path(__file__).resolve()
project_root = script_dir.parents[3]
sys.path.insert(0, str(project_root))

from realtime_analysis.utility.config import load_settings
from realtime_analysis.utility.utils import get_connection

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "headway_vs_schedule"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def clear_results_dir() -> None:
    for f in RESULTS_DIR.glob("*"):
        if f.is_file():
            f.unlink()


def fetch_data(conn) -> pd.DataFrame:
    query = """
    SELECT
        route_short_name,
        stop_name,
        observations,
        avg_actual_headway_min,
        scheduled_headway_minutes,
        headway_delta_min,
        bunching_rate_pct,
        gap_rate_pct
    FROM qgis_realtime_headway_vs_schedule
    WHERE observations >= 3
      AND scheduled_headway_minutes IS NOT NULL
    ORDER BY headway_delta_min DESC;
    """
    return pd.read_sql_query(query, conn)


def plot_delta_distribution(df: pd.DataFrame) -> Path:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(df["headway_delta_min"].clip(-30, 30), bins=50, color="#3498db", edgecolor="black", alpha=0.75)
    ax.axvline(df["headway_delta_min"].mean(), color="red", linestyle="--", linewidth=2, label=f"Mean: {df['headway_delta_min'].mean():.1f} min")
    ax.axvline(0, color="black", linestyle="-", linewidth=1, label="On schedule")
    ax.set_title("Observed vs Scheduled Headway Delta (minutes)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Actual - Scheduled Headway (minutes)")
    ax.set_ylabel("Frequency")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    out = RESULTS_DIR / "headway_delta_distribution.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    return out


def plot_worst_stops(df: pd.DataFrame) -> Path:
    top = df.sort_values("headway_delta_min", ascending=False).head(20)
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.barh(range(len(top)), top["headway_delta_min"], color="#e74c3c", alpha=0.8)
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels([f"{row.stop_name[:28]} (Rt {row.route_short_name})" for _, row in top.iterrows()])
    ax.invert_yaxis()
    ax.set_xlabel("Avg Headway Delta (min)")
    ax.set_title("Stops with Largest Positive Headway Delta (Actual > Scheduled)", fontsize=14, fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    out = RESULTS_DIR / "worst_stops.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    return out


def generate_summary_csv(df: pd.DataFrame) -> Path:
    summary = df.groupby(["route_short_name", "stop_name"]).agg({
        "observations": "sum",
        "avg_actual_headway_min": "mean",
        "scheduled_headway_minutes": "mean",
        "headway_delta_min": "mean",
        "bunching_rate_pct": "mean",
        "gap_rate_pct": "mean"
    }).reset_index()
    out = RESULTS_DIR / "headway_vs_schedule_summary.csv"
    summary.to_csv(out, index=False)
    return out


def print_statistics(df: pd.DataFrame) -> None:
    print("\n" + "=" * 70)
    print("HEADWAY VS SCHEDULE ANALYSIS SUMMARY")
    print("=" * 70)
    print(f"Total stop-period observations: {len(df):,}")
    print(f"Unique routes: {df['route_short_name'].nunique()}")
    print(f"Unique stops: {df['stop_name'].nunique()}")
    print(f"\nMean headway delta: {df['headway_delta_min'].mean():.2f} min")
    print(f"Median headway delta: {df['headway_delta_min'].median():.2f} min")
    print(f"Mean bunching rate: {df['bunching_rate_pct'].mean():.1f}%")
    print(f"Mean gap rate: {df['gap_rate_pct'].mean():.1f}%")
    print("=" * 70)


def main() -> int:
    parser = argparse.ArgumentParser(description="Headway vs Scheduled Headway Analysis")
    parser.add_argument("--clear-output", action="store_true", help="Clear previous output files")
    args = parser.parse_args()

    if args.clear_output:
        clear_results_dir()
    else:
        print("Preserving existing results (use --clear-output to remove old files)")

    settings = load_settings()
    with get_connection(settings) as conn:
        df = fetch_data(conn)

    if df.empty:
        print("⚠️  No data found in qgis_realtime_headway_vs_schedule.")
        print("   Ensure you've ingested realtime data and run sql/run_sql.py.")
        return 1

    print(f"✓ Retrieved {len(df):,} records for headway vs schedule analysis")

    plot_delta_distribution(df)
    plot_worst_stops(df)
    generate_summary_csv(df)
    print_statistics(df)

    print("\n✓ Analysis complete! Results saved to", RESULTS_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


#!/usr/bin/env python3
"""
Run all realtime GTFS analyses
Executes all analysis scripts in sequence
"""

import subprocess
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
VISUALIZATION_DIR = SCRIPT_DIR / "visualizations"
RESULTS_DIR = SCRIPT_DIR / "results"


def run_script(script_path, name, clear_output=False):
    """Run a Python script and handle errors"""
    print(f"\n{'='*60}")
    print(f"Running: {name}")
    print(f"Script:  {script_path.name}")
    print('='*60)
    
    try:
        cmd = [sys.executable, str(script_path)]
        if clear_output:
            cmd.append("--clear-output")
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False,
            cwd=str(script_path.parent)
        )
        return True, "Success"
    except subprocess.CalledProcessError as e:
        return False, f"Exit code {e.returncode}"
    except FileNotFoundError:
        return False, "Script not found"
    except Exception as e:
        return False, str(e)


def main():
    """Run all analysis scripts"""
    parser = argparse.ArgumentParser(
        description="Run all realtime GTFS analyses",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
By default, existing output files are preserved. Use --clear-output to delete
existing results before generating new ones.
        """
    )
    parser.add_argument(
        "--clear-output",
        action="store_true",
        help="Clear existing output files before generating new ones (default: preserve)"
    )
    parser.add_argument(
        "--skip-sql",
        action="store_true",
        help="Skip creation of materialized views (assumes they already exist)"
    )
    args = parser.parse_args()
    
    start_time = datetime.now()
    
    SQL_DIR = SCRIPT_DIR / "sql"
    SQL_RUNNER = SQL_DIR / "run_sql.py"
    
    scripts = [
        ("Speed vs Schedule Analysis", VISUALIZATION_DIR / "speed_vs_schedule_analysis.py"),
        ("Schedule Times Analysis", VISUALIZATION_DIR / "schedule_times_analysis.py"),
        ("Delay Segments Analysis", VISUALIZATION_DIR / "delay_segments_analysis.py"),
        ("Headway Analysis", VISUALIZATION_DIR / "headway_analysis.py"),
        ("Load vs Delay Analysis", VISUALIZATION_DIR / "load_delay_analysis.py"),
    ]
    
    print("="*60)
    print("VANCOUVER GTFS REALTIME ANALYSIS SUITE")
    print("="*60)
    print("\nNote: This suite generates graph visualizations only.")
    print("      Map visualizations are created manually in QGIS using materialized views.")
    print(f"\nStarted at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Create materialized views from SQL queries
    if not args.skip_sql:
        print("\n" + "="*60)
        print("STEP 1: Creating Materialized Views from SQL Queries")
        print("="*60)
        
        if SQL_RUNNER.exists():
            print(f"\nRunning SQL query processor: {SQL_RUNNER.name}")
            try:
                result = subprocess.run(
                    [sys.executable, str(SQL_RUNNER)],
                    check=True,
                    capture_output=False,
                    cwd=str(SQL_DIR)
                )
                print("\n✓ Materialized views created successfully")
            except subprocess.CalledProcessError as e:
                print(f"\n✗ Failed to create materialized views: exit code {e.returncode}")
                print("  Continuing with visualizations (views may already exist)...")
            except Exception as e:
                print(f"\n✗ Error running SQL query processor: {e}")
                print("  Continuing with visualizations (views may already exist)...")
        else:
            print(f"\n⚠ SQL query runner not found: {SQL_RUNNER}")
            print("  Skipping view creation. Make sure materialized views exist.")
    else:
        print("\n" + "="*60)
        print("STEP 1: Skipping Materialized View Creation (--skip-sql flag set)")
        print("="*60)
        print("  Assuming materialized views already exist.")
    
    # Step 2: Run visualization scripts
    print("\n" + "="*60)
    print("STEP 2: Generating Graph Visualizations")
    print("="*60)
    print(f"Running {len(scripts)} visualization scripts...")
    
    results = []
    for name, script_path in scripts:
        if script_path.exists():
            success, message = run_script(script_path, name, clear_output=args.clear_output)
            results.append({"name": name, "success": success, "message": message})
            if success:
                print(f"\n✓ {name} completed successfully")
            else:
                print(f"\n✗ {name} failed: {message}")
        else:
            print(f"\n⚠ Skipping {name} - script not found: {script_path}")
            results.append({"name": name, "success": None, "message": "Script not found"})
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*60)
    print("ANALYSIS SUMMARY")
    print("="*60)
    
    successful = sum(1 for r in results if r["success"] is True)
    failed = sum(1 for r in results if r["success"] is False)
    skipped = sum(1 for r in results if r["success"] is None)
    
    print(f"\n✓ Successful: {successful}")
    print(f"✗ Failed:     {failed}")
    print(f"⚠ Skipped:    {skipped}")
    print(f"\nTotal time: {duration:.1f} seconds")
    
    if failed > 0:
        print("\nFailed analyses:")
        for r in results:
            if r["success"] is False:
                print(f"  - {r['name']}: {r['message']}")
    
    print(f"\nResults saved to: {RESULTS_DIR}")
    
    print("\n" + "="*60)
    print("Analysis complete!")
    print("="*60)
    print("\nNext steps:")
    print("  1. Materialized views (qgis_realtime_*) are ready for QGIS import")
    print("  2. Graph PNGs are saved in results/ directories")
    print("  3. Create map visualizations manually in QGIS using the materialized views")
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())


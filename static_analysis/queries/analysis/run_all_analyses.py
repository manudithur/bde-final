#!/usr/bin/env python3
"""
Run all static GTFS analyses
Executes all analysis scripts in sequence
"""

import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
VISUALIZATION_DIR = SCRIPT_DIR / "visualization"
RESULTS_DIR = SCRIPT_DIR.parent / "results"


def run_script(script_path, name):
    """Run a Python script and handle errors"""
    print(f"\n{'='*60}")
    print(f"Running: {name}")
    print(f"Script:  {script_path.name}")
    print('='*60)
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
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
    start_time = datetime.now()
    
    scripts = [
        ("Route Visualization", VISUALIZATION_DIR / "route_visualization.py"),
        ("Route Duplication Analysis", VISUALIZATION_DIR / "route_duplication_analysis.py"),
        ("Speed Analysis", VISUALIZATION_DIR / "speed_analysis.py"),
        ("Route Density Analysis", VISUALIZATION_DIR / "route_density_analysis.py"),
        ("Stadium Proximity Analysis", VISUALIZATION_DIR / "stadium_proximity_analysis.py")
    ]
    
    print("="*60)
    print("VANCOUVER GTFS STATIC ANALYSIS SUITE")
    print("="*60)
    print(f"\nStarted at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Running {len(scripts)} analysis scripts...")
    
    results = []
    for name, script_path in scripts:
        if script_path.exists():
            success, message = run_script(script_path, name)
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
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())


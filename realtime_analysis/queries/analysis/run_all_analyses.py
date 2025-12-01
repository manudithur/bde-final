#!/usr/bin/env python3
"""
Run All Realtime GTFS Analyses
Executes all realtime analysis scripts in sequence and provides a summary.
"""

import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
VISUALIZATION_DIR = SCRIPT_DIR / "visualization"
RESULTS_DIR = SCRIPT_DIR.parent / "results"

ANALYSES = [
    {
        "name": "Speed vs Schedule Analysis",
        "script": "speed_vs_schedule_analysis.py",
        "description": "Compares scheduled velocities with actual observed speeds"
    },
    {
        "name": "Schedule Times Analysis", 
        "script": "schedule_times_analysis.py",
        "description": "Compares scheduled arrival/departure times with actual times"
    },
    {
        "name": "Delay Segments Analysis",
        "script": "delay_segments_analysis.py",
        "description": "Analyzes traffic patterns and congestion by time/location"
    },
    {
        "name": "Headway Analysis",
        "script": "headway_analysis.py",
        "description": "Analyzes bus bunching and headway regularity"
    }
]


def run_script(script_path: Path, name: str) -> tuple[bool, str]:
    """Run a Python script and return success status and output."""
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
    """Run all analysis scripts."""
    start_time = datetime.now()
    
    print("="*60)
    print("VANCOUVER GTFS REALTIME ANALYSIS SUITE")
    print("="*60)
    print(f"\nStarted at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Running {len(ANALYSES)} analysis scripts...")
    
    results = []
    
    for analysis in ANALYSES:
        script_path = VISUALIZATION_DIR / analysis["script"]
        
        if script_path.exists():
            success, message = run_script(script_path, analysis["name"])
            results.append({
                "name": analysis["name"],
                "success": success,
                "message": message
            })
            
            if success:
                print(f"\n✓ {analysis['name']} completed successfully")
            else:
                print(f"\n✗ {analysis['name']} failed: {message}")
        else:
            print(f"\n⚠ Skipping {analysis['name']} - script not found: {script_path}")
            results.append({
                "name": analysis["name"],
                "success": None,
                "message": "Script not found"
            })
    
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
    
    # Results location
    print(f"\nResults saved to: {RESULTS_DIR}")
    print("\nSubdirectories:")
    for subdir in ["speed_vs_schedule", "schedule_times", "delay_segments", "headway_analysis"]:
        subpath = RESULTS_DIR / subdir
        if subpath.exists():
            file_count = len(list(subpath.glob("*")))
            print(f"  - {subdir}/ ({file_count} files)")
    
    print("\n" + "="*60)
    print("Analysis complete!")
    print("="*60)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())


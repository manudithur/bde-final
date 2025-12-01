#!/usr/bin/env python3
"""
Run all static GTFS analyses
Executes all analysis scripts in sequence
"""

import subprocess
import sys
import os

def run_script(script_name):
    """Run a Python script and handle errors"""
    print(f"\n{'='*60}")
    print(f"Running {script_name}...")
    print('='*60)
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            capture_output=False
        )
        print(f"✓ {script_name} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {script_name} failed with error code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"✗ {script_name} not found")
        return False

def main():
    """Run all analysis scripts"""
    visualization_dir = os.path.join(os.path.dirname(__file__), 'visualization')
    scripts = [
        os.path.join(visualization_dir, 'route_visualization.py'),
        os.path.join(visualization_dir, 'route_duplication_analysis.py'),
        os.path.join(visualization_dir, 'speed_analysis.py'),
        os.path.join(visualization_dir, 'route_density_analysis.py'),
        os.path.join(visualization_dir, 'stadium_proximity_analysis.py')
    ]
    
    print("="*60)
    print("VANCOUVER GTFS STATIC ANALYSIS SUITE")
    print("="*60)
    print(f"\nRunning {len(scripts)} analysis scripts...")
    
    results = []
    for script in scripts:
        script_name = os.path.basename(script)
        if os.path.exists(script):
            success = run_script(script)
            results.append((script_name, success))
        else:
            print(f"\n⚠ Skipping {script_name} (file not found)")
            results.append((script_name, None))
    
    # Summary
    print("\n" + "="*60)
    print("ANALYSIS SUMMARY")
    print("="*60)
    
    successful = sum(1 for _, success in results if success is True)
    failed = sum(1 for _, success in results if success is False)
    skipped = sum(1 for _, success in results if success is None)
    
    print(f"\nSuccessful: {successful}")
    print(f"Failed: {failed}")
    print(f"Skipped: {skipped}")
    
    if failed > 0:
        print("\nFailed scripts:")
        for script, success in results:
            if success is False:
                print(f"  - {script}")
    
    print("\n" + "="*60)
    print("Analysis complete!")
    print("="*60)

if __name__ == "__main__":
    main()


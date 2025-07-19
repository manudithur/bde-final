#!/usr/bin/env python3
"""
GTFS Realtime Importer for STIB-MIVB Brussels
Usage:
    python run_importer.py single      # Run single import
    python run_importer.py continuous  # Run continuous import
    python run_importer.py dashboard   # Run dashboard
    python run_importer.py visualize   # Run visualization
"""

import sys
import os

from gtfs_realtime_importer import GTFSRealtimeImporter
from visualizer import GTFSRealtimeVisualizer
from dashboard import GTFSRealtimeDashboard

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "single":
        print("Running single import...")
        importer = GTFSRealtimeImporter()
        importer.import_realtime_data()
        print("Single import completed!")
        
    elif command == "continuous":
        print("Running continuous import (Press Ctrl+C to stop)...")
        importer = GTFSRealtimeImporter()
        importer.run_continuous_import()
        
    elif command == "dashboard":
        print("Starting dashboard...")
        dashboard = GTFSRealtimeDashboard()
        dashboard.run()
        
    elif command == "visualize":
        print("Running visualization...")
        visualizer = GTFSRealtimeVisualizer()
        
        # Get available lines
        lines = visualizer.get_available_lines()
        print(f"Available lines: {lines}")
        
        # Visualize all vehicle positions
        fig = visualizer.visualize_vehicle_positions()
        fig.show()
        
        # If there are lines available, visualize the first one
        if lines:
            line_id = lines[0]
            print(f"Visualizing line {line_id}")
            
            # Show trajectory
            fig_trajectory = visualizer.visualize_line_trajectory(line_id)
            fig_trajectory.show()
            
            # Show statistics
            stats = visualizer.generate_line_statistics(line_id)
            print(f"Statistics for line {line_id}:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
    
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)

if __name__ == "__main__":
    main()
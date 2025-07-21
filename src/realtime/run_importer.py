#!/usr/bin/env python3
"""
GTFS Realtime Importer for STIB-MIVB Brussels
Usage:
    python run_importer.py single      # Run single import
    python run_importer.py continuous  # Run continuous import
"""

import sys
import os

from gtfs_realtime_importer import GTFSRealtimeImporter
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
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)

if __name__ == "__main__":
    main()
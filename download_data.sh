#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Process North West England GTFS data
echo "Processing North West England GTFS data..."

# Create data directory structure if it doesn't exist
mkdir -p src/data

# Check if North West England data already exists
if [ -f "src/data/itm_north_west_gtfs.zip" ]; then
    echo "North West England GTFS data found!"
    echo "Using existing file: src/data/itm_north_west_gtfs.zip"
    
    # Clean up existing directories for fresh extraction
    echo "Cleaning up existing data directories..."
    rm -rf src/data/gtfs_be
    rm -rf src/data/gtfs_pruned
    rm -rf src/data/gtfs_full_backup
    
    # Extract the zip file to the expected directory
    echo "Extracting GTFS data..."
    cd src/data
    mkdir -p gtfs_be
    unzip -o itm_north_west_gtfs.zip -d gtfs_be/
    cd ../..
    
    echo "Extraction completed!"
    echo "GTFS files are now available in src/data/gtfs_be/ directory"
    
    # Run data processing pipeline
    echo "Running data processing pipeline..."
    
    echo "Step 1: Running data pruner..."
    python3 src/static/data_pruner.py
    if [ $? -ne 0 ]; then
        echo "Data pruner failed!"
        exit 1
    fi
    
    echo "Step 2: Running Manchester area filter..."
    python3 src/static/data_wrangler.py
    if [ $? -ne 0 ]; then
        echo "Manchester area filter failed!"
        exit 1
    fi
    
    echo "Data processing pipeline completed successfully!"
    echo "Processed GTFS files are available in src/data/gtfs_pruned/ directory"
    echo "You can now run: import_gtfs.sh (requires database setup)"
else
    echo "North West England GTFS data not found!"
    echo "Please ensure the file 'src/data/itm_north_west_gtfs.zip' exists"
    exit 1
fi
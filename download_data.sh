#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Download GTFS data from STIB-MIVB API
echo "Downloading GTFS data from STIB-MIVB..."

# Create data directory structure if it doesn't exist
mkdir -p src/data

# Download the zip file with the expected naming convention
curl -o src/data/gtfs-be.zip "https://data.stib-mivb.brussels/api/explore/v2.1/catalog/datasets/gtfs-files-production/alternative_exports/gtfszip/"

if [ $? -eq 0 ]; then
    echo "Download completed successfully!"
    echo "GTFS data saved to: src/data/gtfs-be.zip"
    
    # Extract the zip file to the expected directory
    echo "Extracting GTFS data..."
    cd src/data
    mkdir -p gtfs_be
    unzip -o gtfs-be.zip -d gtfs_be/
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
    
    echo "Step 2: Running data wrangler..."
    python3 src/static/data_wrangler.py
    if [ $? -ne 0 ]; then
        echo "Data wrangler failed!"
        exit 1
    fi
    
    echo "Data processing pipeline completed successfully!"
    echo "Processed GTFS files are available in src/data/gtfs_pruned/ directory"
    echo "You can now run: import_gtfs.sh (requires database setup)"
else
    echo "Download failed!"
    exit 1
fi
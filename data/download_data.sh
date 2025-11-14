#!/bin/bash

# Load environment variables
# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(cat "$PROJECT_ROOT/.env" | grep -v '^#' | xargs)
fi

# Process Vancouver GTFS data
echo "Processing Vancouver GTFS data..."

# Change to script directory (data/)
cd "$SCRIPT_DIR"

# GTFS data URL
GTFS_URL="https://gtfs-static.translink.ca/gtfs/History/2025-11-14/google_transit.zip"
GTFS_ZIP="gtfs_vancouver.zip"

# Check if zip file exists, if not download it
if [ ! -f "$GTFS_ZIP" ]; then
    echo "Vancouver GTFS zip file not found locally."
    echo "Downloading from: $GTFS_URL"
    
    # Check for curl or wget
    if command -v curl &> /dev/null; then
        curl -L -o "$GTFS_ZIP" "$GTFS_URL"
        DOWNLOAD_STATUS=$?
    elif command -v wget &> /dev/null; then
        wget -O "$GTFS_ZIP" "$GTFS_URL"
        DOWNLOAD_STATUS=$?
    else
        echo "❌ Error: Neither curl nor wget is available. Please install one of them to download the file."
        exit 1
    fi
    
    if [ $DOWNLOAD_STATUS -ne 0 ]; then
        echo "❌ Error downloading GTFS data from $GTFS_URL"
        exit 1
    fi
    
    echo "✅ Download completed!"
fi

# Extract the zip file
if [ -f "$GTFS_ZIP" ]; then
    echo "Vancouver GTFS zip file found!"
    echo "Extracting $GTFS_ZIP..."
    
    # Remove existing directory if it exists for fresh extraction
    if [ -d "gtfs_vancouver" ]; then
        echo "Removing existing gtfs_vancouver directory..."
        rm -rf gtfs_vancouver
    fi
    
    # Extract the zip file
    unzip -o "$GTFS_ZIP" -d gtfs_vancouver/
    
    if [ $? -ne 0 ]; then
        echo "❌ Error extracting $GTFS_ZIP"
        exit 1
    fi
    
    echo "✅ Extraction completed!"
    echo "GTFS files are now available in gtfs_vancouver/ directory"
elif [ -d "gtfs_vancouver" ]; then
    echo "Vancouver GTFS data directory found!"
    echo "Using existing directory: gtfs_vancouver/"
else
    echo "❌ Vancouver GTFS data not found!"
    echo "Please ensure either:"
    echo "  - The file 'data/gtfs_vancouver.zip' exists, OR"
    echo "  - The directory 'data/gtfs_vancouver' exists"
    echo "Current directory: $(pwd)"
    exit 1
fi

# Run data processing pipeline
echo ""
echo "Running data processing pipeline..."

echo "Step 1: Running enhanced data pruner..."
python3 data_pruner.py
if [ $? -ne 0 ]; then
    echo "⚠️  Data pruner had warnings, but continuing..."
fi

echo "Step 2: Running Vancouver area filter..."
python3 data_wrangler.py
if [ $? -ne 0 ]; then
    echo "Vancouver area filter failed!"
    exit 1
fi

echo ""
echo "✅ Data processing pipeline completed successfully!"
echo "Processed GTFS files are available in gtfs_pruned/ directory"
echo "You can now run: import_gtfs.sh (requires database setup)"
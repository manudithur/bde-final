# Brussels Transit Data Analysis

Process and analyze GTFS transit data from Brussels' STIB-MIVB public transport system.

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   npm install -g gtfs-via-postgres
   ```

2. **Setup Environment Variables**
   Create a `.env` file in the project root:
   ```bash
   # STIB-MIVB API Key (get yours at: https://data.stib-mivb.brussels/account/)
   STIB_API_KEY=your_api_key_here
   
   # Database Configuration
   PGHOST=localhost
   PGUSER=postgres
   PGPASSWORD=postgres
   PGDATABASE=gtfs_be
   ```

3. **Download and Process Data**
   ```bash
   ./download_data.sh
   ```

4. **Setup Database**
   ```bash
   createdb gtfs_be
   ```

5. **Import and Process Data** (All-in-one)
   ```bash
   ./import_gtfs.sh --start-date 2025-07-07 --end-date 2025-08-03
   ```

   Or manually:
   - Import to Database:
     ```bash
     cd src/data/gtfs_pruned
     export PGDATABASE=gtfs_be
     export PGUSER=postgres
     export PGPASSWORD=postgres
     gtfs-to-sql --require-dependencies -- *.txt | psql -b
     ```
   - Create Route Segments:
     ```bash
     python src/scripts/split_into_segments.py src/data/gtfs_pruned.zip \
       --start-date 2025-07-07 --end-date 2025-08-03 \
       --db-host localhost --db-user postgres --db-pass postgres --db-name gtfs_be
     ```

## What This Does

- Downloads GTFS data from STIB-MIVB Brussels transit
- Processes and cleans the data automatically
- Imports transit routes, stops, and schedules into PostgreSQL
- Creates route segments for analysis and visualization
- Enables real-time data collection and analysis

## Project Structure

```
src/
├── data/              # GTFS and real-time data storage
├── scripts/           # Data processing scripts
├── queries/           # SQL analysis queries
└── realtime/          # Real-time data collection
```

## Prerequisites

- PostgreSQL with MobilityDB extensions
- Python 3.8+
- Node.js > 14 (for GTFS tools)


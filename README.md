# Vancouver GTFS Analysis

Spatial database analysis of Vancouver transit data with both scheduled GTFS and GTFS-Realtime feeds.

---

## Prerequisites

Before running any commands, ensure the following are in place:

### Database Requirements
- **PostgreSQL database** named `gtfs` must already exist and be running
- **PostGIS extension** must be installed and enabled on the database
- **MobilityDB extension** must be installed and enabled on the database
- Database connection accessible (credentials via exported environment variables)

### Database Connection Configuration
Export database connection variables in your terminal. These are required for shell commands (like `psql`) and will also be used by Python scripts:

```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=gtfs
```

To make these persistent across terminal sessions, add them to your shell profile (e.g., `~/.bashrc`, `~/.zshrc`).

**Important:** Shell commands (like `psql`) require exported environment variables. You must export these variables in your terminal. Optionally, you can also create a `.env` file for Python scripts to load automatically via `load_dotenv()`, but exported variables are still required for shell commands.

**Default values (if not set):**
- `PGHOST=localhost`
- `PGPORT=5432`
- `PGUSER=postgres`
- `PGPASSWORD=postgres`
- `PGDATABASE=gtfs`

### Python Dependencies
```bash
pip install -r requirements.txt
pip install -r static_analysis/requirements.txt
pip install -r realtime_analysis/requirements.txt
```

### External Tools
- `gtfs-to-sql` (install via npm): `npm install -g gtfs-via-postgres`

### Database Connection
You can test your connection with:
```bash
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "SELECT PostGIS_version(), MobilityDB_version();"
```
This should return version numbers for both extensions if they're properly installed.

---

## GTFS Static Workflow

**Prerequisites:** Database `gtfs` must exist with PostGIS and MobilityDB extensions enabled.

1. **Download & preprocess schedule data**
   ```bash
   bash static_analysis/data/download_data.sh
   ```
   Produces `static_analysis/data/gtfs_pruned/`.

2. **Import GTFS tables**
   ```bash
   cd static_analysis/data/gtfs_pruned
   gtfs-to-sql --require-dependencies -- *.txt | psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE
   ```
   Requires: Database `gtfs` with PostGIS extension enabled

3. **Load MobilityDB schema & derived tables**
   ```bash
   cd ../../..
   cat static_analysis/data_loading/mobilitydb_import.sql | psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE
   ```
   Requires: MobilityDB extension enabled, GTFS tables from step 2

4. **Run analysis queries**
   ```bash
   cat static_analysis/queries/analysis/spatial_queries.sql | psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE
   ```
   Requires: `scheduled_trips_mdb` and other MobilityDB tables from step 3

5. **Generate static visualizations**
   ```bash
   cd static_analysis/queries/analysis
   # Run all analyses at once
   python run_all_analyses.py
   
   # Or run individually:
   python visualization/route_visualization.py          # Interactive route maps
   python visualization/route_duplication_analysis.py   # Route duplication analysis
   python visualization/speed_analysis.py               # Speed analysis
   python visualization/route_density_analysis.py       # Route density
   python visualization/stadium_proximity_analysis.py   # Stadium proximity
   ```
   
   **Visualization Scripts:**
   - **route_visualization.py** - Creates interactive route maps (all routes, by mode, density heatmap)
   - **route_duplication_analysis.py** - Analyzes and visualizes route duplication
   - **route_density_analysis.py** - Creates route density histograms
   - **speed_analysis.py** - Analyzes vehicle speeds and creates speed distribution charts
   - **stadium_proximity_analysis.py** - Analyzes transit access near stadiums and landmarks
   
   **Outputs:**
   - All results are saved to `static_analysis/queries/results/` organized by analysis type
   - Interactive HTML maps: route maps, route duplication maps, stadium proximity maps
   - Statistical charts: duplication heatmaps, speed distributions, route density histograms
   - See `static_analysis/queries/results/README.md` for list of generated result files
   
   **Requirements:**
   - Database connection (via environment variables: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`)
   - Python dependencies from `static_analysis/requirements.txt`
   - Materialized views from `spatial_queries.sql` (step 4 of static workflow)

6. **Inspect in GIS (optional)**
   - Install QGIS from https://qgis.org/
   - Layer → Add Layer → Add PostGIS Layer
   - Connection: Use your database credentials (Host, Port, DB `gtfs`, User, Password from environment variables)
   - Load tables such as `stops`, `route_segments`, `scheduled_trips_mdb`

---

## GTFS Realtime Workflow

**Prerequisites:** 
- Database `gtfs` with PostGIS and MobilityDB extensions enabled
- Static GTFS data loaded (from Static Workflow steps 1-3)
- API key for TransLink GTFS-Realtime feeds

1. **Set API key**
   ```bash
   export TRANSLINK_GTFSR_API_KEY="your-translink-api-key"
   # Default endpoints:
   #   Positions: https://gtfsapi.translink.ca/v3/gtfsposition
   #   Trip updates: https://gtfsapi.translink.ca/v3/gtfsrealtime
   # Override via GTFS_VEHICLE_POSITIONS_URL / GTFS_TRIP_UPDATES_URL if needed.
   ```

2. **Create realtime tables**
   ```bash
   cat realtime_analysis/realtime_schema.sql | psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE
   ```
   Requires: Database `gtfs` with PostGIS and MobilityDB extensions enabled

3. **Ingest GTFS-Realtime feeds**
   ```bash
   python -m realtime_analysis.ingest_realtime \
     --duration-minutes 20 \
     --poll-interval 30
   ```
   Requires: Realtime tables from step 2, TRANSLINK_GTFSR_API_KEY environment variable
   Use the same route filters as the static study. Pass `--once` for a single snapshot.

4. **Build map-matched actual trajectories**
   ```bash
   python -m realtime_analysis.build_realtime_trajectories --hours 2
   ```
   Limit to processing data until --hours behind
   Requires: Realtime vehicle positions from step 3, scheduled_trips_mdb from static workflow
   Raw GPS points are deduplicated and snapped onto the scheduled shape before
   upserting into `realtime_trips_mdb`.

5. **Run realtime analysis queries**
   ```bash
   cat realtime_analysis/queries/analysis/realtime_queries.sql | psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE
   ```
   Requires: Realtime data in `rt_trip_updates` table from step 3, `realtime_trips_mdb` from step 4, static `route_segments` table from static workflow
   This creates materialized views for faster analysis queries. The visualization scripts will use these views if available, falling back to inline queries otherwise.

7. **Run comprehensive realtime analyses**
   ```bash
   cd realtime_analysis/queries/analysis
   # Run all analyses at once
   python run_all_analyses.py
   
   # Or run individually:
   python visualization/speed_vs_schedule_analysis.py    # Scheduled vs actual speeds
   python visualization/schedule_times_analysis.py       # Scheduled vs actual times
   python visualization/delay_segments_analysis.py       # Traffic/congestion patterns
   python visualization/headway_analysis.py              # Bus bunching and headway analysis
   ```
   
   **Visualization Scripts:**
   - **speed_vs_schedule_analysis.py** - Compares planned velocities with actual observed speeds
   - **schedule_times_analysis.py** - Compares scheduled arrival/departure times with actual times
   - **delay_segments_analysis.py** - Analyzes traffic patterns and congestion by time/location
   - **headway_analysis.py** - Analyzes bus bunching and headway regularity
   
   **Outputs:**
   - All results saved to `realtime_analysis/queries/results/` organized by analysis type
   - Interactive HTML maps and charts (Plotly-based)
   - CSV files with detailed metrics
   - See `realtime_analysis/queries/results/README.md` for list of generated result files
   
   **Requirements:**
   - Database connection (via environment variables: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`)
   - Python dependencies from `realtime_analysis/requirements.txt`
   - Realtime trip updates in `rt_trip_updates` table (from step 3)
   - Static schedule tables: `route_segments`, `stops`, `routes` (from static workflow)
   - Materialized views from step 4 recommended for performance

---

## Project Structure

```
.
├── static_analysis/          # Static schedule analysis
│   ├── data/                # GTFS download & preprocessing scripts
│   │   ├── download_data.sh
│   │   └── gtfs_pruned/
│   ├── data_loading/        # Database schema setup
│   │   └── mobilitydb_import.sql
│   ├── queries/             # Analysis queries and visualizations
│   │   ├── analysis/        # Analysis queries and scripts
│   │   │   ├── spatial_queries.sql  # SQL queries with materialized views
│   │   │   ├── run_all_analyses.py  # Run all visualization scripts
│   │   │   └── visualization/  # Python scripts for visualizing queries
│   │   └── results/         # Output files (PNG, HTML) - see results/README.md
│   ├── utility/             # Utility scripts
│   │   ├── check_data.sql
│   │   └── fix_trips_shape_id.sql
├── realtime_analysis/       # Real-time ingestion & comparison
│   ├── realtime_schema.sql
│   ├── ingest_realtime.py
│   ├── build_realtime_trajectories.py
│   ├── analyze_realtime.py
│   ├── utils.py
│   ├── queries/             # Realtime analysis queries
│   │   ├── analysis/        # Analysis queries and scripts
│   │   │   ├── realtime_queries.sql  # SQL queries with materialized views
│   │   │   ├── run_all_analyses.py   # Run all visualization scripts
│   │   │   └── visualization/  # Python scripts for visualizing queries
│   │   └── results/         # Output files (HTML, CSV, PNG) - see results/README.md
```

---

## Troubleshooting

- **Database connection errors:** Verify your environment variables are set correctly and the database is running. Test with:
  ```bash
  psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "SELECT version();"
  ```

- **PostGIS/MobilityDB extensions missing:** These must be installed on your PostgreSQL instance before running any commands. Verify with:
  ```bash
  psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "SELECT PostGIS_version(), MobilityDB_version();"
  ```

- **Missing tables:** Ensure you've run all workflow steps in order. Static workflow steps 2-4 must complete before realtime workflow.

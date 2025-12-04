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

4. **(Optional) Load population density data for Vancouver**
   ```bash
   python static_analysis/data/download_population_data.py \
     --geo static_analysis/data/population/vancouver_geo.geojson
   ```
   This imports census tract geometries and a `population_areas` table with a `population_density` column for Vancouver, used by the static population‑density analyses.

5. **Run static analysis SQL & visualizations**
   ```bash
   cd static_analysis/queries
   # Run all analyses at once (creates materialized views + graphs)
   python run_all_analyses.py
   ```
   This will:
   - Build all materialized views defined in `static_analysis/queries/sql/*.sql` (tables/views prefixed with `qgis_`), which can be loaded directly into QGIS.
   - Generate PNG graph visualizations into `static_analysis/queries/results/` organized by analysis type.

   **Run individual visualization scripts (optional):**
   ```bash
   cd static_analysis/queries/visualizations
   python route_visualization.py            # Route statistics graphs
   python route_density_analysis.py         # Route density histograms
   python speed_analysis.py                 # Speed analysis graphs
   python population_density_analysis.py    # Population vs transit coverage graphs
   python stadium_proximity_analysis.py     # Stadium proximity graphs
   ```
   
   **Visualization Scripts:**
   - **route_visualization.py** - Route-level statistics and distributions (PNG graphs)
   - **route_density_analysis.py** - Route density histograms (PNG)
   - **speed_analysis.py** - Speed distributions and top routes (PNG)
   - **population_density_analysis.py** - Population density vs transit coverage (PNG)
   - **stadium_proximity_analysis.py** - Stadium transit access metrics (PNG)
   
   **Outputs:**
   - All results are saved to `static_analysis/queries/results/` organized by analysis type.
   - Map visualizations are created manually in QGIS using the `qgis_*` materialized views created by `static_analysis/queries/sql/run_sql.py`.
   - Statistical charts: route statistics, speed distributions, route density histograms, population vs transit coverage, stadium proximity summaries.
   - See `static_analysis/queries/results/README.md` for list of generated result files.
   
   **Requirements:**
   - Database connection (via environment variables: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`)
   - Python dependencies from `static_analysis/requirements.txt`
   - Static schedule tables and MobilityDB schema from steps 2–3

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
   cat realtime_analysis/data_loading/realtime_schema.sql | psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE
   ```
   Requires: Database `gtfs` with PostGIS and MobilityDB extensions enabled

3. **Ingest GTFS-Realtime feeds**
   ```bash
   python -m realtime_analysis.data.ingest_realtime \
     --duration-minutes 20 \
     --poll-interval 30
   ```
   Requires: Realtime tables from step 2, TRANSLINK_GTFSR_API_KEY environment variable
   Use the same route filters as the static study. Pass `--once` for a single snapshot.

4. **Build map-matched actual trajectories**
   ```bash
   python -m realtime_analysis.data.build_realtime_trajectories
   ```
   Requires: Realtime vehicle positions from step 3, scheduled_trips_mdb from static workflow
   Raw GPS points are deduplicated and snapped onto the scheduled shape before
   upserting into `realtime_trips_mdb`.

5. **Run realtime analysis SQL & visualizations**
   ```bash
   cd realtime_analysis/queries
   # Run all analyses at once (creates materialized views + graphs)
   python run_all_analyses.py
   ```
   This will:
   - Execute `realtime_analysis/queries/sql/realtime_queries.sql` and other SQL files in `realtime_analysis/queries/sql/` to create base materialized views (`realtime_*`) and QGIS‑friendly views (`qgis_realtime_*`).
   - Generate PNG graph visualizations into `realtime_analysis/queries/results/` organized by analysis type.

   **Run individual visualization scripts (optional):**
   ```bash
   cd realtime_analysis/queries/visualizations
   python speed_vs_schedule_analysis.py    # Scheduled vs actual speeds
   python schedule_times_analysis.py       # Scheduled vs actual times
   python delay_segments_analysis.py       # Traffic/congestion patterns
   python headway_analysis.py              # Bus bunching and headway analysis
   ```
   
   **Visualization Scripts:**
   - **speed_vs_schedule_analysis.py** - Compares planned velocities with actual observed speeds
   - **schedule_times_analysis.py** - Compares scheduled arrival/departure times with actual times
   - **delay_segments_analysis.py** - Analyzes traffic patterns and congestion by time/location
   - **headway_analysis.py** - Analyzes bus bunching and headway regularity
   
   **Outputs:**
   - All results saved to `realtime_analysis/queries/results/` organized by analysis type (PNG graphs + CSV metrics).
   - Map visualizations are created manually in QGIS using the `qgis_realtime_*` materialized views created by `realtime_analysis/queries/sql/run_sql.py`.
   - See `realtime_analysis/queries/results/README.md` for list of generated result files.
   
   **Requirements:**
   - Database connection (via environment variables: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`)
   - Python dependencies from `realtime_analysis/requirements.txt`
   - Realtime trip updates in `rt_trip_updates` table (from step 3)
   - Static schedule tables: `route_segments`, `stops`, `routes` (from static workflow)

---

## Project Structure

```
.
├── static_analysis/          # Static schedule analysis
│   ├── data/                 # GTFS & population download/preprocessing scripts
│   │   ├── download_data.sh
│   │   ├── download_population_data.py
│   │   ├── gtfs_pruned/
│   │   ├── gtfs_vancouver/
│   │   └── population/
│   ├── data_loading/        # Database schema setup
│   │   └── mobilitydb_import.sql
│   ├── queries/              # Static analysis SQL + visualizations
│   │   ├── sql/              # SQL files that build materialized views (qgis_*)
│   │   │   └── run_sql.py    # Builds all static materialized views
│   │   ├── visualizations/   # Python scripts for graph visualizations (PNG)
│   │   ├── run_all_analyses.py  # Run SQL + all visualization scripts
│   │   └── results/          # Output files (PNG) - see results/README.md
├── realtime_analysis/       # Real-time ingestion & comparison
│   ├── realtime_schema.sql
│   ├── ingest_realtime.py
│   ├── build_realtime_trajectories.py
│   ├── analyze_realtime.py
│   ├── utils.py
│   ├── queries/              # Realtime analysis SQL + visualizations
│   │   ├── sql/              # SQL files building realtime & qgis_realtime_* views
│   │   │   └── run_sql.py    # Builds all realtime materialized views
│   │   ├── visualizations/   # Python scripts for graph visualizations (PNG)
│   │   ├── run_all_analyses.py   # Run SQL + all visualization scripts
│   │   └── results/          # Output files (CSV, PNG) - see results/README.md
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

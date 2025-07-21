import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import time
import psycopg2
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional
import logging
import os
import signal
from config import ENDPOINTS, HEADERS, API_PARAMS, DATA_DIR, PARQUET_DIR, REFRESH_INTERVAL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GTFSRealtimeImporter:
    def __init__(self):
        self.setup_directories()
        self.stop_coordinates = {}  # Cache for coordinates
        self.db_connection = None
        self.running = True
        self.preload_all_stops()  # Preload all stops at initialization
        self.setup_database_connection()
        self.setup_signal_handlers()
        
    def setup_directories(self):
        """Create necessary directories for data storage"""
        Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
        Path(PARQUET_DIR).mkdir(parents=True, exist_ok=True)
        
    def setup_database_connection(self):
        """Setup connection to PostgreSQL database with GTFS static data"""
        try:
            self.db_connection = psycopg2.connect(
                host=os.getenv('PGHOST', 'localhost'),
                port=os.getenv('PGPORT', '5432'),
                user=os.getenv('PGUSER', 'postgres'),
                password=os.getenv('PGPASSWORD', ''),
                database=os.getenv('PGDATABASE', 'gtfs_be')
            )
            logger.info("Connected to PostgreSQL database for GTFS schedule data")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to database: {e}")
            self.db_connection = None
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
        
    def preload_all_stops(self):
        """Preload all stop coordinates from the API"""
        logger.info("Preloading all stop coordinates...")
        try:
            offset = 0
            limit = 100  # Use smaller batches
            total_stops_loaded = 0
            
            while True:
                url = ENDPOINTS['stop_details']
                params = {
                    "limit": limit,
                    "offset": offset,
                    **API_PARAMS
                }
                
                response = requests.get(url, headers=HEADERS, params=params)
                response.raise_for_status()
                
                data = response.json()
                results = data.get("results", [])
                
                # If no more results, break
                if not results:
                    break
                
                batch_loaded = 0
                for result in results:
                    stop_id = result.get("id")
                    gps_coordinates_str = result.get("gpscoordinates", "{}")
                    
                    if stop_id:
                        try:
                            gps_coordinates = json.loads(gps_coordinates_str)
                            coordinates = {
                                "latitude": gps_coordinates.get("latitude"),
                                "longitude": gps_coordinates.get("longitude")
                            }
                            self.stop_coordinates[stop_id] = coordinates
                            batch_loaded += 1
                        except json.JSONDecodeError:
                            logger.warning(f"Could not parse GPS coordinates for stop {stop_id}")
                            self.stop_coordinates[stop_id] = {"latitude": None, "longitude": None}
                
                total_stops_loaded += batch_loaded
                logger.info(f"Loaded batch of {batch_loaded} stops (total: {total_stops_loaded})")
                
                # If we got fewer results than the limit, we've reached the end
                if len(results) < limit:
                    break
                
                offset += limit
            
            logger.info(f"Preloaded {total_stops_loaded} stop coordinates")
            
        except requests.RequestException as e:
            logger.error(f"Error preloading stop coordinates: {e}")
        except Exception as e:
            logger.error(f"Unexpected error preloading stop coordinates: {e}")
    
    def get_stop_coordinates(self, stop_id: str) -> Dict[str, Optional[float]]:
        """Get coordinates for a specific stop from preloaded cache"""
        return self.stop_coordinates.get(stop_id, {"latitude": None, "longitude": None})
    
        
    def fetch_vehicle_positions(self) -> Optional[pd.DataFrame]:
        """Fetch vehicle positions from STIB-MIVB API"""
        try:
            url = ENDPOINTS["vehicle_positions"]
            params = {
                "limit": 100,
                "offset": 0,
                **API_PARAMS
            }
            
            response = requests.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                logger.warning("No vehicle position records found")
                return None
                
            # Extract vehicle position data
            vehicle_positions = []
            for result in results:
                line_id = result.get("lineid")
                vehicle_positions_str = result.get("vehiclepositions", "[]")
                
                # Parse the JSON string containing vehicle positions
                try:
                    import json
                    positions = json.loads(vehicle_positions_str)
                    for pos in positions:
                        # Essential data from STIB API
                        point_id = pos.get("pointId")
                        distance_from_point = pos.get("distanceFromPoint", 0)
                        direction_id = pos.get("directionId")
                        
                        # Get stop coordinates for spatial analysis
                        coordinates = self.get_stop_coordinates(point_id)
                        
                        # Create vehicle ID from line and direction
                        vehicle_id = f"line_{line_id}_dir_{direction_id}"
                        current_timestamp = datetime.now(timezone.utc)
                        
                        # Essential vehicle position record - keep minimal data for analysis
                        vehicle_positions.append({
                            "id": f"{line_id}_{point_id}_{direction_id}_{int(current_timestamp.timestamp())}",
                            "schedule_relationship": "SCHEDULED",
                            "latitude": coordinates.get("latitude"),
                            "longitude": coordinates.get("longitude"),
                            "current_status": "IN_TRANSIT_TO" if distance_from_point > 0 else "STOPPED_AT",
                            "timestamp": int(current_timestamp.timestamp()),
                            "stop_id": point_id,
                            "vehicle_id": vehicle_id,
                            "vehicle_label": f"Line {line_id}",
                            "line_id": line_id,
                            "direction_id": direction_id,
                            "destination": f"Direction {direction_id}",
                            "distance_from_point": distance_from_point,  # Key field from API
                            "updated_at": current_timestamp.isoformat()
                        })
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse vehicle positions for line {line_id}")
                    continue
            
            df = pd.DataFrame(vehicle_positions)
            logger.info(f"Fetched {len(df)} vehicle positions")
            return df
            
        except requests.RequestException as e:
            logger.error(f"Error fetching vehicle positions: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    
    
    def save_to_postgres(self, df: pd.DataFrame, table_name: str):
        """Save DataFrame to PostgreSQL table for MobilityDB processing"""
        if not self.db_connection or df.empty:
            logger.warning("No database connection or empty DataFrame, skipping save")
            return
            
        try:
            cursor = self.db_connection.cursor()
            
            if table_name == "vehicle_positions":
                # Create vehicle_positions table for essential data
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS vehicle_positions(
                    id text PRIMARY KEY,
                    timestamp bigint, 
                    departure_stop_id text, 
                    vehicle_id text, 
                    line_id text,
                    destination_stop_id text,
                    distance_from_point integer,
                    updated_at timestamp
                );
                
                CREATE INDEX IF NOT EXISTS idx_vehicle_positions_timestamp ON vehicle_positions (timestamp);
                CREATE INDEX IF NOT EXISTS idx_vehicle_positions_vehicle_id ON vehicle_positions (vehicle_id);
                """
                
                cursor.execute(create_table_sql)
                
                # Insert or update records
                for _, row in df.iterrows():
                    if row['latitude'] and row['longitude']:
                        insert_sql = """
                        INSERT INTO vehicle_positions 
                        (id, timestamp, 
                         departure_stop_id, vehicle_id, line_id, destination_stop_id, 
                         distance_from_point, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            timestamp = EXCLUDED.timestamp,
                            updated_at = EXCLUDED.updated_at
                        """
                        
                        cursor.execute(insert_sql, (
                            row['id'],
                            int(datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00')).timestamp()), 
                            row['point_id'], row['vehicle_id'],
                            row['line_id'], row['direction_id'],
                            row['distance_from_point'], datetime.fromisoformat(row['updated_at'].replace('Z', '+00:00'))
                        ))
            
            
            
            self.db_connection.commit()
            cursor.close()
            logger.info(f"Saved {len(df)} records to PostgreSQL table {table_name}")
            
        except psycopg2.Error as e:
            logger.error(f"Error saving to PostgreSQL: {e}")
            self.db_connection.rollback()
    
    def save_to_parquet(self, df: pd.DataFrame, filename: str):
        """Save DataFrame to parquet file (legacy method)"""
        try:
            filepath = Path(PARQUET_DIR) / filename
            
            # If file exists, append data
            if filepath.exists():
                existing_df = pd.read_parquet(filepath)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                # Remove duplicates based on id and timestamp (if both columns exist)
                if 'id' in combined_df.columns and 'timestamp' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(subset=['id', 'timestamp'], keep='last')
                elif 'id' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(subset=['id'], keep='last')
                else:
                    # If no id column, just keep all records
                    pass
            else:
                combined_df = df
            
            # Save to parquet
            combined_df.to_parquet(filepath, index=False)
            logger.info(f"Saved {len(df)} new records to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to parquet: {e}")
    
    def import_realtime_data(self):
        """Import all realtime data types"""
        logger.info("Starting GTFS Realtime data import")
        
        # Fetch vehicle positions
        vehicle_df = self.fetch_vehicle_positions()
        if vehicle_df is not None:
            # Save to both PostgreSQL (primary) and Parquet (backup)
            self.save_to_postgres(vehicle_df, "vehicle_positions")
            self.save_to_parquet(vehicle_df, "vehicle_positions.parquet")
        
        
        
        logger.info("GTFS Realtime data import completed")
    
    def run_continuous_import(self):
        """Run continuous import with specified refresh interval"""
        logger.info(f"Starting continuous import with {REFRESH_INTERVAL}s interval")
        
        try:
            while self.running:
                self.import_realtime_data()
                if not self.running:
                    break
                logger.info(f"Sleeping for {REFRESH_INTERVAL} seconds")
                
                # Sleep with interruption checks
                for _ in range(REFRESH_INTERVAL):
                    if not self.running:
                        break
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            logger.info("Import stopped by user (Ctrl+C)")
            self.running = False
        except Exception as e:
            logger.error(f"Error in continuous import: {e}")
            self.running = False
        finally:
            logger.info("Saving parquet to postgres...")
            # Save all parquet files to PostgreSQL at the end
            parquet_vehicle_positions = pd.read_parquet(Path(PARQUET_DIR) / "vehicle_positions.parquet")
            if not parquet_vehicle_positions.empty:
                self.save_to_postgres(parquet_vehicle_positions, "vehicle_positions")
            logger.info("Continuous import finished")


    def __del__(self):
        """Clean up database connection"""
        if self.db_connection:
            self.db_connection.close()
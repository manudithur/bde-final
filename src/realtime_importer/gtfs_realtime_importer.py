import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import time
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import logging

from config import ENDPOINTS, HEADERS, API_PARAMS, DATA_DIR, PARQUET_DIR, REFRESH_INTERVAL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GTFSRealtimeImporter:
    def __init__(self):
        self.setup_directories()
        self.stop_coordinates = {}  # Cache for coordinates
        self.vehicle_history = {}  # Track previous positions for speed calculation
        self.preload_all_stops()  # Preload all stops at initialization
        
    def setup_directories(self):
        """Create necessary directories for data storage"""
        Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
        Path(PARQUET_DIR).mkdir(parents=True, exist_ok=True)
        
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
    
    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two GPS coordinates using Haversine formula (in meters)"""
        if None in [lat1, lon1, lat2, lon2]:
            return 0.0
            
        # Convert latitude and longitude from degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Radius of earth in meters
        r = 6371000
        return c * r
    
    def calculate_realistic_speed(self, vehicle_id: str, current_lat: float, current_lon: float, 
                                current_timestamp: datetime, distance_from_point: int) -> tuple[float, float]:
        """Calculate realistic speed based on historical positions and estimate delay"""
        current_time = current_timestamp
        
        # Check if we have previous data for this vehicle
        if vehicle_id in self.vehicle_history:
            prev_data = self.vehicle_history[vehicle_id]
            prev_lat = prev_data['latitude']
            prev_lon = prev_data['longitude'] 
            prev_time = prev_data['timestamp']
            
            # Calculate time difference in seconds
            time_diff = (current_time - prev_time).total_seconds()
            
            # Only calculate if we have a reasonable time difference (between 10 seconds and 10 minutes)
            if 10 <= time_diff <= 600 and prev_lat and prev_lon and current_lat and current_lon:
                # Calculate distance traveled in meters
                distance_traveled = self.calculate_distance(prev_lat, prev_lon, current_lat, current_lon)
                
                # Calculate speed in km/h
                if time_diff > 0:
                    speed_kmh = (distance_traveled / time_diff) * 3.6
                    
                    # Apply realistic constraints for public transit (0-80 km/h)
                    speed_kmh = max(0, min(speed_kmh, 80))
                    
                    # If speed seems unrealistic (too high), use previous speed or reasonable default
                    if speed_kmh > 60:  # Very high for city transit
                        speed_kmh = prev_data.get('speed', 25)  # Use previous speed or 25 km/h default
                else:
                    speed_kmh = 0
            else:
                # Not enough time passed or invalid coordinates, use previous speed or default
                speed_kmh = prev_data.get('speed', 20)
        else:
            # No previous data, estimate based on distance from stop
            if distance_from_point == 0:
                speed_kmh = 5  # At stop, very slow
            elif distance_from_point < 100:
                speed_kmh = 15  # Near stop, moderate speed
            else:
                speed_kmh = 25  # Between stops, normal transit speed
        
        # Update vehicle history
        self.vehicle_history[vehicle_id] = {
            'latitude': current_lat,
            'longitude': current_lon,
            'timestamp': current_time,
            'speed': speed_kmh,
            'distance_from_point': distance_from_point
        }
        
        # Estimate delay based on distance from scheduled point
        # Closer to scheduled point = less delay
        if distance_from_point == 0:
            estimated_delay = 0  # On time
        elif distance_from_point < 200:
            estimated_delay = distance_from_point / 100  # Minor delay
        else:
            estimated_delay = 2 + (distance_from_point - 200) / 300  # Significant delay
            
        return round(speed_kmh, 1), round(estimated_delay, 2)
        
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
                        # Get coordinates for this point/stop
                        point_id = pos.get("pointId")
                        coordinates = self.get_stop_coordinates(point_id)
                        
                        # Create vehicle ID from line and direction
                        vehicle_id = f"line_{line_id}_dir_{pos.get('directionId')}"
                        distance_from_point = pos.get("distanceFromPoint", 0)
                        
                        # Calculate realistic speed and delay using historical positions
                        current_timestamp = datetime.now(timezone.utc)
                        current_lat = coordinates.get("latitude")
                        current_lon = coordinates.get("longitude")
                        
                        if current_lat and current_lon:
                            estimated_speed, estimated_delay = self.calculate_realistic_speed(
                                vehicle_id, current_lat, current_lon, current_timestamp, distance_from_point
                            )
                        else:
                            # Fallback for missing coordinates
                            estimated_speed = 20  # Default speed
                            estimated_delay = abs(distance_from_point) / 200 if distance_from_point else 0
                        
                        # Destination based on direction
                        destination = f"Direction {pos.get('directionId', 'Unknown')}"
                        
                        vehicle_positions.append({
                            "id": f"{line_id}_{pos.get('pointId')}_{pos.get('directionId')}",
                            "vehicle_id": vehicle_id,
                            "line_id": line_id,
                            "direction_id": pos.get("directionId"),
                            "latitude": coordinates.get("latitude"),
                            "longitude": coordinates.get("longitude"),
                            "speed": round(estimated_speed, 1),
                            "timestamp": current_timestamp.isoformat(),
                            "delay": round(estimated_delay, 2),
                            "destination": destination,
                            "point_id": pos.get("pointId"),
                            "distance_from_point": distance_from_point,
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
    
    def fetch_waiting_times(self) -> Optional[pd.DataFrame]:
        """Fetch waiting times (trip updates) from STIB-MIVB API"""
        try:
            url = ENDPOINTS["waiting_times"]
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
                logger.warning("No waiting time records found")
                return None
                
            # Extract waiting time data
            waiting_times = []
            for result in results:
                point_id = result.get("pointid")
                line_id = result.get("lineid")
                passing_times_str = result.get("passingtimes", "[]")
                
                # Parse the JSON string containing passing times
                try:
                    import json
                    passing_times = json.loads(passing_times_str)
                    for pt in passing_times:
                        # Get coordinates for this stop
                        coordinates = self.get_stop_coordinates(point_id)
                        
                        waiting_times.append({
                            "id": f"{point_id}_{line_id}_{pt.get('expectedArrivalTime', '')}",
                            "stop_id": point_id,
                            "line_id": line_id,
                            "direction_id": None,  # Not available in this format
                            "destination": None,  # Not available in this format
                            "waiting_time_min": None,  # Need to calculate from expectedArrivalTime
                            "expected_arrival_time": pt.get("expectedArrivalTime"),
                            "message": pt.get("message"),
                            "latitude": coordinates.get("latitude"),
                            "longitude": coordinates.get("longitude"),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "updated_at": datetime.now(timezone.utc).isoformat()
                        })
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse passing times for stop {point_id}, line {line_id}")
                    continue
            
            df = pd.DataFrame(waiting_times)
            logger.info(f"Fetched {len(df)} waiting time records")
            return df
            
        except requests.RequestException as e:
            logger.error(f"Error fetching waiting times: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def fetch_disruptions(self) -> Optional[pd.DataFrame]:
        """Fetch service disruptions (alerts) from STIB-MIVB API"""
        try:
            url = ENDPOINTS["disruptions"]
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
                logger.warning("No disruption records found")
                return None
                
            # Extract disruption data
            disruptions = []
            for idx, result in enumerate(results):
                content_str = result.get("content", "[]")
                lines_str = result.get("lines", "[]")
                points_str = result.get("points", "[]")
                
                # Parse the JSON strings
                try:
                    import json
                    content = json.loads(content_str)
                    lines = json.loads(lines_str)
                    points = json.loads(points_str)
                    
                    # Extract line IDs
                    line_ids = [line.get("id") for line in lines]
                    point_ids = [point.get("id") for point in points]
                    
                    # Extract messages from content
                    messages = {}
                    for content_item in content:
                        if "text" in content_item:
                            for text_item in content_item["text"]:
                                if "en" in text_item:
                                    messages["en"] = text_item["en"]
                                if "fr" in text_item:
                                    messages["fr"] = text_item["fr"]
                                if "nl" in text_item:
                                    messages["nl"] = text_item["nl"]
                    
                    disruptions.append({
                        "id": f"disruption_{idx}_{result.get('priority', 0)}",
                        "line_ids": line_ids,
                        "point_ids": point_ids,
                        "type": result.get("type"),
                        "priority": result.get("priority"),
                        "message_fr": messages.get("fr"),
                        "message_nl": messages.get("nl"),
                        "message_en": messages.get("en"),
                        "start_time": None,  # Not available in this format
                        "end_time": None,  # Not available in this format
                        "timestamp": datetime.now(timezone.utc).isoformat(),  # Add timestamp field
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse disruption data for record {idx}")
                    continue
            
            df = pd.DataFrame(disruptions)
            logger.info(f"Fetched {len(df)} disruption records")
            return df
            
        except requests.RequestException as e:
            logger.error(f"Error fetching disruptions: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def save_to_parquet(self, df: pd.DataFrame, filename: str):
        """Save DataFrame to parquet file"""
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
            self.save_to_parquet(vehicle_df, "vehicle_positions.parquet")
        
        # Fetch waiting times (trip updates)
        waiting_df = self.fetch_waiting_times()
        if waiting_df is not None:
            self.save_to_parquet(waiting_df, "trip_updates.parquet")
        
        # Fetch disruptions (service alerts)
        disruption_df = self.fetch_disruptions()
        if disruption_df is not None:
            self.save_to_parquet(disruption_df, "service_alerts.parquet")
        
        logger.info("GTFS Realtime data import completed")
    
    def run_continuous_import(self):
        """Run continuous import with specified refresh interval"""
        logger.info(f"Starting continuous import with {REFRESH_INTERVAL}s interval")
        
        try:
            while True:
                self.import_realtime_data()
                logger.info(f"Sleeping for {REFRESH_INTERVAL} seconds")
                time.sleep(REFRESH_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Import stopped by user")
        except Exception as e:
            logger.error(f"Error in continuous import: {e}")

if __name__ == "__main__":
    importer = GTFSRealtimeImporter()
    
    # Run single import
    importer.import_realtime_data()
    
    # Uncomment to run continuous import
    # importer.run_continuous_import()
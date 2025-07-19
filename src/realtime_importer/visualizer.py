import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from typing import List, Dict
import logging

from config import PARQUET_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GTFSRealtimeVisualizer:
    def __init__(self):
        self.parquet_dir = Path(PARQUET_DIR)
        
    def query_vehicle_positions(self, line_id: str = None, vehicle_id: str = None, limit: int = None) -> pd.DataFrame:
        """Query vehicle positions from parquet file"""
        try:
            parquet_file = self.parquet_dir / "vehicle_positions.parquet"
            
            if not parquet_file.exists():
                logger.error(f"Parquet file not found: {parquet_file}")
                return pd.DataFrame()
            
            con = duckdb.connect()
            
            conditions = []
            if line_id:
                conditions.append(f"line_id = '{line_id}'")
            if vehicle_id:
                conditions.append(f"vehicle_id = '{vehicle_id}'")
            
            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            limit_clause = f"LIMIT {limit}" if limit is not None else ""
            
            query = f"""
            SELECT *
            FROM parquet_scan('{parquet_file}')
            {where_clause}
            ORDER BY timestamp DESC
            {limit_clause}
            """
            
            df = con.execute(query).fetchdf()
            con.close()
            
            logger.info(f"Queried {len(df)} vehicle position records")
            return df
            
        except Exception as e:
            logger.error(f"Error querying vehicle positions: {e}")
            return pd.DataFrame()
    
    def query_line_trajectory(self, line_id: str) -> pd.DataFrame:
        """Query trajectory for a specific line"""
        try:
            parquet_file = self.parquet_dir / "vehicle_positions.parquet"
            
            if not parquet_file.exists():
                logger.error(f"Parquet file not found: {parquet_file}")
                return pd.DataFrame()
            
            con = duckdb.connect()
            
            query = f"""
            SELECT *
            FROM parquet_scan('{parquet_file}')
            WHERE line_id = '{line_id}'
            AND latitude IS NOT NULL
            AND longitude IS NOT NULL
            ORDER BY timestamp
            """
            
            df = con.execute(query).fetchdf()
            con.close()
            
            logger.info(f"Queried {len(df)} trajectory records for line {line_id}")
            return df
            
        except Exception as e:
            logger.error(f"Error querying line trajectory: {e}")
            return pd.DataFrame()
    
    def get_available_lines(self) -> List[str]:
        """Get list of available line IDs"""
        try:
            parquet_file = self.parquet_dir / "vehicle_positions.parquet"
            
            if not parquet_file.exists():
                logger.error(f"Parquet file not found: {parquet_file}")
                return []
            
            con = duckdb.connect()
            
            query = f"""
            SELECT DISTINCT line_id
            FROM parquet_scan('{parquet_file}')
            WHERE line_id IS NOT NULL
            ORDER BY line_id
            """
            
            df = con.execute(query).fetchdf()
            con.close()
            
            return df['line_id'].tolist()
            
        except Exception as e:
            logger.error(f"Error getting available lines: {e}")
            return []
    
    def get_available_vehicles(self, line_id: str = None) -> List[str]:
        """Get list of available vehicle IDs, optionally filtered by line"""
        try:
            parquet_file = self.parquet_dir / "vehicle_positions.parquet"
            
            if not parquet_file.exists():
                logger.error(f"Parquet file not found: {parquet_file}")
                return []
            
            con = duckdb.connect()
            
            if line_id:
                where_clause = f"WHERE line_id = '{line_id}' AND vehicle_id IS NOT NULL"
            else:
                where_clause = "WHERE vehicle_id IS NOT NULL"
            
            query = f"""
            SELECT DISTINCT vehicle_id
            FROM parquet_scan('{parquet_file}')
            {where_clause}
            ORDER BY vehicle_id
            """
            
            df = con.execute(query).fetchdf()
            con.close()
            
            return df['vehicle_id'].tolist()
            
        except Exception as e:
            logger.error(f"Error getting available vehicles: {e}")
            return []
    
    def visualize_vehicle_positions(self, line_id: str = None, vehicle_id: str = None, limit: int = None) -> go.Figure:
        """Visualize current vehicle positions"""
        df = self.query_vehicle_positions(line_id=line_id, vehicle_id=vehicle_id, limit=limit)
        
        if df.empty:
            logger.warning("No data available for visualization")
            # Return empty figure with proper layout
            fig = go.Figure()
            fig.update_layout(
                title="No data available for visualization",
                mapbox_style="open-street-map",
                mapbox_center={"lat": 50.8503, "lon": 4.3517},
                mapbox_zoom=11,
                margin={"r": 0, "t": 30, "l": 0, "b": 0},
                height=400
            )
            return fig
        
        # Filter out invalid coordinates
        df = df[(df['latitude'].notna()) & (df['longitude'].notna())]
        df = df[(df['latitude'] != 0) & (df['longitude'] != 0)]
        
        if df.empty:
            logger.warning("No valid coordinates found")
            # Return empty figure with proper layout
            fig = go.Figure()
            fig.update_layout(
                title="No valid coordinates found",
                mapbox_style="open-street-map",
                mapbox_center={"lat": 50.8503, "lon": 4.3517},
                mapbox_zoom=11,
                margin={"r": 0, "t": 30, "l": 0, "b": 0},
                height=400
            )
            return fig
        
        if vehicle_id and line_id:
            title = f"Vehicle {vehicle_id} - Line {line_id}"
        elif vehicle_id:
            title = f"Vehicle {vehicle_id}"
        elif line_id:
            title = f"Vehicle Positions - Line {line_id}"
        else:
            title = "All Vehicle Positions"
        
        fig = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            hover_name="vehicle_id",
            hover_data=["line_id", "destination", "speed", "delay"],
            color="speed",
            color_continuous_scale="Viridis",
            title=title,
            zoom=11,
            size_max=20
        )
        
        fig.update_traces(marker=dict(size=12))
        
        # Center map on Brussels
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox_center={"lat": 50.8503, "lon": 4.3517},
            margin={"r": 0, "t": 30, "l": 0, "b": 0}
        )
        
        return fig
    
    def visualize_line_trajectory(self, line_id: str) -> go.Figure:
        """Visualize trajectory for a specific line with speed information"""
        df = self.query_line_trajectory(line_id)
        
        if df.empty:
            logger.warning(f"No trajectory data available for line {line_id}")
            # Return empty figure with proper layout
            fig = go.Figure()
            fig.update_layout(
                title=f"No trajectory data available for line {line_id}",
                mapbox_style="open-street-map",
                mapbox_center={"lat": 50.8503, "lon": 4.3517},
                mapbox_zoom=11,
                margin={"r": 0, "t": 30, "l": 0, "b": 0},
                height=400
            )
            return fig
        
        # Filter out invalid coordinates
        df = df[(df['latitude'].notna()) & (df['longitude'].notna())]
        df = df[(df['latitude'] != 0) & (df['longitude'] != 0)]
        
        if df.empty:
            logger.warning(f"No valid coordinates found for line {line_id}")
            # Return empty figure with proper layout
            fig = go.Figure()
            fig.update_layout(
                title=f"No valid coordinates found for line {line_id}",
                mapbox_style="open-street-map",
                mapbox_center={"lat": 50.8503, "lon": 4.3517},
                mapbox_zoom=11,
                margin={"r": 0, "t": 30, "l": 0, "b": 0},
                height=400
            )
            return fig
        
        fig = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            hover_name="vehicle_id",
            hover_data=["destination", "speed", "delay", "timestamp"],
            color="speed",
            color_continuous_scale="Hot",
            title=f"Trajectory of Line {line_id}",
            zoom=12,
            size_max=20
        )
        
        fig.update_traces(marker=dict(size=12))
        
        # Center map on data
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox_center={
                "lat": df["latitude"].mean(),
                "lon": df["longitude"].mean()
            },
            margin={"r": 0, "t": 30, "l": 0, "b": 0}
        )
        
        return fig
    
    def generate_line_statistics(self, line_id: str = None, vehicle_id: str = None) -> Dict:
        """Generate statistics for a specific line or vehicle"""
        if vehicle_id:
            df = self.query_vehicle_positions(line_id=line_id, vehicle_id=vehicle_id, limit=10000)
        else:
            df = self.query_line_trajectory(line_id)
        
        if df.empty:
            entity_name = f"vehicle {vehicle_id}" if vehicle_id else f"line {line_id}"
            return {"error": f"No data available for {entity_name}"}
        
        # Filter valid data
        df = df[(df['speed'].notna()) & (df['delay'].notna())]
        
        stats = {
            "line_id": line_id if line_id else "N/A",
            "vehicle_id": vehicle_id if vehicle_id else "N/A",
            "total_vehicles": df['vehicle_id'].nunique(),
            "total_records": len(df),
            "avg_speed": df['speed'].mean(),
            "max_speed": df['speed'].max(),
            "min_speed": df['speed'].min(),
            "avg_delay": df['delay'].mean(),
            "max_delay": df['delay'].max(),
            "min_delay": df['delay'].min(),
            "destinations": df['destination'].unique().tolist()
        }
        
        return stats
    
    def get_dataframe_statistics(self) -> Dict:
        """Generate comprehensive DataFrame statistics"""
        try:
            parquet_file = self.parquet_dir / "vehicle_positions.parquet"
            
            if not parquet_file.exists():
                logger.error(f"Parquet file not found: {parquet_file}")
                return {"error": "No data file found"}
            
            con = duckdb.connect()
            
            # Get total row count
            total_rows_query = f"SELECT COUNT(*) as total_rows FROM parquet_scan('{parquet_file}')"
            total_rows = con.execute(total_rows_query).fetchdf()['total_rows'].iloc[0]
            
            # Get oldest and newest timestamps
            timestamp_query = f"""
            SELECT 
                MIN(timestamp) as oldest_point,
                MAX(timestamp) as newest_point
            FROM parquet_scan('{parquet_file}')
            WHERE timestamp IS NOT NULL
            """
            timestamp_data = con.execute(timestamp_query).fetchdf()
            
            # Get speed statistics
            speed_query = f"""
            SELECT 
                MAX(speed) as top_speed,
                AVG(speed) as avg_speed,
                MIN(speed) as min_speed,
                COUNT(CASE WHEN speed IS NOT NULL THEN 1 END) as speed_data_points
            FROM parquet_scan('{parquet_file}')
            """
            speed_data = con.execute(speed_query).fetchdf()
            
            # Get line and vehicle counts
            counts_query = f"""
            SELECT 
                COUNT(DISTINCT line_id) as unique_lines,
                COUNT(DISTINCT vehicle_id) as unique_vehicles,
                COUNT(DISTINCT point_id) as unique_stops
            FROM parquet_scan('{parquet_file}')
            WHERE line_id IS NOT NULL AND vehicle_id IS NOT NULL
            """
            counts_data = con.execute(counts_query).fetchdf()
            
            # Get coordinate data points
            coord_query = f"""
            SELECT 
                COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as valid_coordinates,
                COUNT(*) as total_records
            FROM parquet_scan('{parquet_file}')
            """
            coord_data = con.execute(coord_query).fetchdf()
            
            con.close()
            
            stats = {
                "total_lines": int(total_rows),
                "total_data_points": int(coord_data['valid_coordinates'].iloc[0]),
                "total_records": int(coord_data['total_records'].iloc[0]),
                "top_speed": float(speed_data['top_speed'].iloc[0]) if pd.notna(speed_data['top_speed'].iloc[0]) else 0,
                "avg_speed": float(speed_data['avg_speed'].iloc[0]) if pd.notna(speed_data['avg_speed'].iloc[0]) else 0,
                "min_speed": float(speed_data['min_speed'].iloc[0]) if pd.notna(speed_data['min_speed'].iloc[0]) else 0,
                "speed_data_points": int(speed_data['speed_data_points'].iloc[0]),
                "oldest_point": timestamp_data['oldest_point'].iloc[0] if pd.notna(timestamp_data['oldest_point'].iloc[0]) else "N/A",
                "newest_point": timestamp_data['newest_point'].iloc[0] if pd.notna(timestamp_data['newest_point'].iloc[0]) else "N/A",
                "unique_lines": int(counts_data['unique_lines'].iloc[0]),
                "unique_vehicles": int(counts_data['unique_vehicles'].iloc[0]),
                "unique_stops": int(counts_data['unique_stops'].iloc[0])
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error generating DataFrame statistics: {e}")
            return {"error": f"Error generating statistics: {str(e)}"}
    

if __name__ == "__main__":
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
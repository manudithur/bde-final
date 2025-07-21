import psycopg2
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
import os
from datetime import datetime, timedelta

class VehicleTrajectoryVisualizer:
    def __init__(self):
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.setup_layout()
        self.setup_callbacks()
        
    def connect_to_postgres(self):
        """Connect to PostgreSQL database"""
        try:
            conn = psycopg2.connect(
                host=os.getenv('PGHOST', 'localhost'),
                port=os.getenv('PGPORT', '5432'),
                user=os.getenv('PGUSER', 'postgres'),
                password=os.getenv('PGPASSWORD', ''),
                database=os.getenv('PGDATABASE', 'gtfs_be')
            )
            return conn
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return None
    
    def fetch_available_lines(self):
        """Fetch available line IDs from vehicle_positions table"""
        conn = self.connect_to_postgres()
        if conn is None:
            return []
        
        try:
            query = """
            SELECT DISTINCT line_id 
            FROM vehicle_positions 
            WHERE line_id IS NOT NULL 
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
            ORDER BY line_id;
            """
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df['line_id'].tolist()
        except Exception as e:
            print(f"Error fetching line IDs: {e}")
            if conn:
                conn.close()
            return []
    
    def fetch_available_vehicles(self, line_id=None):
        """Fetch available vehicle IDs, optionally filtered by line"""
        conn = self.connect_to_postgres()
        if conn is None:
            return []
        
        try:
            if line_id:
                query = """
                SELECT DISTINCT vehicle_id 
                FROM vehicle_positions 
                WHERE line_id = %s 
                AND latitude IS NOT NULL 
                AND longitude IS NOT NULL
                ORDER BY vehicle_id;
                """
                df = pd.read_sql_query(query, conn, params=[line_id])
            else:
                query = """
                SELECT DISTINCT vehicle_id 
                FROM vehicle_positions 
                WHERE latitude IS NOT NULL 
                AND longitude IS NOT NULL
                ORDER BY vehicle_id;
                """
                df = pd.read_sql_query(query, conn)
            
            conn.close()
            return df['vehicle_id'].tolist()
        except Exception as e:
            print(f"Error fetching vehicle IDs: {e}")
            if conn:
                conn.close()
            return []
    
    def fetch_vehicle_positions(self, line_id=None, vehicle_id=None, hours_back=6):
        """Fetch vehicle positions for visualization"""
        conn = self.connect_to_postgres()
        if conn is None:
            return pd.DataFrame()
        
        try:
            # Calculate time threshold
            time_threshold = datetime.now() - timedelta(hours=hours_back)
            unix_threshold = int(time_threshold.timestamp())
            
            # Build query based on filters
            where_conditions = [
                "latitude IS NOT NULL",
                "longitude IS NOT NULL", 
                f"timestamp >= {unix_threshold}"
            ]
            params = []
            
            if line_id:
                where_conditions.append("line_id = %s")
                params.append(line_id)
                
            if vehicle_id:
                where_conditions.append("vehicle_id = %s")
                params.append(vehicle_id)
            
            query = f"""
            SELECT 
                id,
                vehicle_id,
                line_id,
                direction_id,
                stop_id,
                latitude,
                longitude,
                timestamp,
                distance_from_point,
                updated_at,
                TO_TIMESTAMP(timestamp) as datetime
            FROM vehicle_positions 
            WHERE {' AND '.join(where_conditions)}
            ORDER BY vehicle_id, timestamp;
            """
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            # Convert timestamp to datetime for better visualization
            if not df.empty:
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
                df['time_str'] = df['datetime'].dt.strftime('%H:%M:%S')
            
            return df
        except Exception as e:
            print(f"Error fetching vehicle positions: {e}")
            if conn:
                conn.close()
            return pd.DataFrame()
    
    def setup_layout(self):
        """Setup the Dash app layout"""
        # Get initial data for dropdowns
        lines = self.fetch_available_lines()
        vehicles = self.fetch_available_vehicles()
        
        self.app.layout = dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("Brussels STIB Vehicle Trajectory Visualizer", 
                           className="text-center mb-4"),
                ], width=12)
            ]),
            
            dbc.Row([
                dbc.Col([
                    html.Label("Select Line:", className="fw-bold"),
                    dcc.Dropdown(
                        id="line-dropdown",
                        options=[{'label': f"Line {line}", 'value': line} for line in lines],
                        value=lines[0] if lines else None,
                        placeholder="Select a line",
                        clearable=True,
                        className="mb-3"
                    )
                ], width=4),
                
                dbc.Col([
                    html.Label("Select Vehicle:", className="fw-bold"),
                    dcc.Dropdown(
                        id="vehicle-dropdown",
                        placeholder="Select a vehicle (optional)",
                        clearable=True,
                        className="mb-3"
                    )
                ], width=4),
                
                dbc.Col([
                    html.Label("Time Window:", className="fw-bold"),
                    dcc.Dropdown(
                        id="time-dropdown",
                        options=[
                            {'label': 'Last 1 hour', 'value': 1},
                            {'label': 'Last 3 hours', 'value': 3},
                            {'label': 'Last 6 hours', 'value': 6},
                            {'label': 'Last 12 hours', 'value': 12},
                            {'label': 'Last 24 hours', 'value': 24}
                        ],
                        value=6,
                        clearable=False,
                        className="mb-3"
                    )
                ], width=4)
            ]),
            
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id="trajectory-map", style={'height': '600px'})
                ], width=12)
            ]),
            
            dbc.Row([
                dbc.Col([
                    html.Div(id="data-summary", className="mt-3")
                ], width=12)
            ])
            
        ], fluid=True)
    
    def setup_callbacks(self):
        """Setup Dash callbacks"""
        
        @self.app.callback(
            Output("vehicle-dropdown", "options"),
            Input("line-dropdown", "value")
        )
        def update_vehicle_dropdown(line_id):
            if line_id:
                vehicles = self.fetch_available_vehicles(line_id)
                return [{'label': vehicle, 'value': vehicle} for vehicle in vehicles]
            else:
                vehicles = self.fetch_available_vehicles()
                return [{'label': vehicle, 'value': vehicle} for vehicle in vehicles]
        
        @self.app.callback(
            [Output("trajectory-map", "figure"),
             Output("data-summary", "children")],
            [Input("line-dropdown", "value"),
             Input("vehicle-dropdown", "value"),
             Input("time-dropdown", "value")]
        )
        def update_map(line_id, vehicle_id, hours_back):
            # Fetch data
            df = self.fetch_vehicle_positions(line_id, vehicle_id, hours_back)
            
            if df.empty:
                empty_fig = px.scatter_mapbox(
                    title="No vehicle position data found",
                    center={"lat": 50.8503, "lon": 4.3517},  # Brussels center
                    zoom=10
                )
                empty_fig.update_layout(mapbox_style="open-street-map")
                return empty_fig, html.P("No data available for the selected filters.")
            
            # Create map visualization
            if vehicle_id:
                # Single vehicle trajectory
                fig = px.line_mapbox(
                    df, 
                    lat="latitude", 
                    lon="longitude",
                    hover_data=["vehicle_id", "stop_id", "distance_from_point", "time_str"],
                    title=f"Trajectory for {vehicle_id} (Line {line_id})",
                    zoom=12
                )
                fig.update_traces(line=dict(color="red", width=3))
                
                # Add start and end points
                if len(df) > 1:
                    start_point = df.iloc[0]
                    end_point = df.iloc[-1]
                    
                    fig.add_trace(go.Scattermapbox(
                        lat=[start_point['latitude']],
                        lon=[start_point['longitude']],
                        mode='markers',
                        marker=dict(size=15, color='green'),
                        name='Start',
                        hovertemplate="Start: %{lat}, %{lon}<extra></extra>"
                    ))
                    
                    fig.add_trace(go.Scattermapbox(
                        lat=[end_point['latitude']],
                        lon=[end_point['longitude']],
                        mode='markers',
                        marker=dict(size=15, color='blue'),
                        name='End',
                        hovertemplate="End: %{lat}, %{lon}<extra></extra>"
                    ))
            else:
                # Multiple vehicles - show as points colored by vehicle
                fig = px.scatter_mapbox(
                    df,
                    lat="latitude", 
                    lon="longitude",
                    color="vehicle_id",
                    hover_data=["vehicle_id", "stop_id", "distance_from_point", "time_str"],
                    title=f"Vehicle Positions for Line {line_id}" if line_id else "All Vehicle Positions",
                    zoom=11
                )
                fig.update_traces(marker=dict(size=8))
            
            # Update map layout
            fig.update_layout(
                mapbox_style="open-street-map",
                mapbox_center={"lat": df["latitude"].mean(), "lon": df["longitude"].mean()},
                margin={"r": 0, "t": 50, "l": 0, "b": 0},
                showlegend=True if not vehicle_id else False
            )
            
            # Create summary
            summary = self.create_data_summary(df, line_id, vehicle_id, hours_back)
            
            return fig, summary
    
    def create_data_summary(self, df, line_id, vehicle_id, hours_back):
        """Create a summary of the displayed data"""
        if df.empty:
            return html.P("No data to summarize.")
        
        summary_data = []
        
        # Basic stats
        total_points = len(df)
        unique_vehicles = df['vehicle_id'].nunique()
        time_span = (df['timestamp'].max() - df['timestamp'].min()) / 3600  # hours
        
        summary_data.extend([
            html.H5("Data Summary", className="fw-bold"),
            html.P(f"Total position points: {total_points:,}"),
            html.P(f"Unique vehicles: {unique_vehicles}"),
            html.P(f"Time span: {time_span:.1f} hours"),
        ])
        
        if line_id:
            summary_data.append(html.P(f"Line: {line_id}"))
        
        if vehicle_id:
            vehicle_data = df[df['vehicle_id'] == vehicle_id]
            if not vehicle_data.empty:
                stops_visited = vehicle_data['stop_id'].nunique()
                summary_data.extend([
                    html.P(f"Vehicle: {vehicle_id}"),
                    html.P(f"Stops visited: {stops_visited}"),
                ])
        
        return html.Div(summary_data)
    
    def run_server(self, debug=True, port=8050):
        """Run the Dash server"""
        self.app.run_server(debug=debug, port=port)

if __name__ == "__main__":
    visualizer = VehicleTrajectoryVisualizer()
    print("Starting Brussels STIB Vehicle Trajectory Visualizer...")
    print("Open http://localhost:8050 in your browser")
    visualizer.run_server(debug=True, port=8050)
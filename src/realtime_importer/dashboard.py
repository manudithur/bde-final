import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from visualizer import GTFSRealtimeVisualizer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GTFSRealtimeDashboard:
    def __init__(self):
        self.visualizer = GTFSRealtimeVisualizer()
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.setup_layout()
        self.setup_callbacks()
    
    def setup_layout(self):
        """Setup the dashboard layout"""
        # Get available lines for dropdown
        lines = self.visualizer.get_available_lines()
        vehicles = self.visualizer.get_available_vehicles()
        
        self.app.layout = dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("STIB-MIVB Real-time GTFS Dashboard", className="text-center mb-4"),
                    html.Hr()
                ])
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Controls", className="card-title"),
                            html.Label("Select Line:"),
                            dcc.Dropdown(
                                id="line-dropdown",
                                options=[{"label": f"Line {line}", "value": line} for line in lines] + 
                                        [{"label": "All Lines", "value": "all"}],
                                value="all",
                                clearable=False
                            ),
                            html.Br(),
                            html.Label("Select Vehicle:"),
                            dcc.Dropdown(
                                id="vehicle-dropdown",
                                options=[{"label": f"Vehicle {vehicle}", "value": vehicle} for vehicle in vehicles[:50]] + 
                                        [{"label": "All Vehicles", "value": "all"}],
                                value="all",
                                clearable=False
                            ),
                            html.Br(),
                            html.Label("Data Points Limit:"),
                            dcc.Dropdown(
                                id="limit-dropdown",
                                options=[
                                    {"label": "1,000 points", "value": 1000},
                                    {"label": "2,000 points", "value": 2000},
                                    {"label": "5,000 points", "value": 5000},
                                    {"label": "10,000 points", "value": 10000},
                                    {"label": "All points", "value": "all"}
                                ],
                                value=1000,
                                clearable=False
                            ),
                            html.Br(),
                            html.Label("Auto-refresh:"),
                            dcc.Interval(
                                id="interval-component",
                                interval=30*1000,  # 30 seconds
                                n_intervals=0
                            ),
                            dcc.Checklist(
                                id="auto-refresh-toggle",
                                options=[{"label": "Auto-refresh (30s)", "value": "enabled"}],
                                value=["enabled"]
                            ),
                            html.Br(),
                            dbc.Button("Refresh Now", id="refresh-button", color="primary", className="mt-2"),
                            html.Div(id="refresh-status", className="mt-2"),
                            html.Hr(),
                            html.Small([
                                "📍 Positions are estimated from stop locations and distance data.",
                                html.Br(),
                                "🚄 Speed calculated from proximity to stops (5-25 km/h).",
                                html.Br(), 
                                "⏱️ Delay estimated from distance deviation from scheduled points."
                            ], className="text-muted")
                        ])
                    ])
                ], width=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Vehicle Positions", className="card-title"),
                            dcc.Graph(id="vehicle-positions-map")
                        ])
                    ])
                ], width=9)
            ], className="mb-4"),
            
            dcc.Tabs(id="main-tabs", value="analysis-tab", children=[
                dcc.Tab(label="Analysis", value="analysis-tab", children=[
                    html.Div([
                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H4("Line Statistics", className="card-title"),
                                        html.Div(id="line-statistics")
                                    ])
                                ])
                            ], width=6),
                            
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H4("Delay Analysis", className="card-title"),
                                        dcc.Graph(id="delay-histogram")
                                    ])
                                ])
                            ], width=6)
                        ], className="mb-4"),
                        
                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H4("Speed Distribution", className="card-title"),
                                        dcc.Graph(id="speed-histogram")
                                    ])
                                ])
                            ], width=12)
                        ])
                    ], style={"padding": "20px"})
                ]),
                
                dcc.Tab(label="Data Quality", value="quality-tab", children=[
                    html.Div([
                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H4("DataFrame Statistics", className="card-title"),
                                        html.Div(id="dataframe-statistics")
                                    ])
                                ])
                            ], width=6),
                            
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H4("Data Quality", className="card-title"),
                                        dcc.Graph(id="data-quality-chart")
                                    ])
                                ])
                            ], width=6)
                        ])
                    ], style={"padding": "20px"})
                ])
            ])
        ], fluid=True)
    
    def setup_callbacks(self):
        """Setup dashboard callbacks"""
        
        @self.app.callback(
            Output("vehicle-dropdown", "options"),
            [Input("line-dropdown", "value")]
        )
        def update_vehicle_dropdown(selected_line):
            if selected_line == "all":
                vehicles = self.visualizer.get_available_vehicles()
            else:
                vehicles = self.visualizer.get_available_vehicles(selected_line)
            
            return [{"label": f"Vehicle {vehicle}", "value": vehicle} for vehicle in vehicles[:50]] + \
                   [{"label": "All Vehicles", "value": "all"}]
        
        @self.app.callback(
            [Output("vehicle-positions-map", "figure"),
             Output("dataframe-statistics", "children"),
             Output("line-statistics", "children"),
             Output("speed-histogram", "figure"),
             Output("data-quality-chart", "figure"),
             Output("delay-histogram", "figure"),
             Output("refresh-status", "children")],
            [Input("line-dropdown", "value"),
             Input("vehicle-dropdown", "value"),
             Input("limit-dropdown", "value"),
             Input("refresh-button", "n_clicks"),
             Input("interval-component", "n_intervals")],
            [State("auto-refresh-toggle", "value")]
        )
        def update_dashboard(selected_line, selected_vehicle, selected_limit, n_clicks, n_intervals, auto_refresh_enabled):
            try:
                # Determine which line and vehicle to visualize
                line_id = None if selected_line == "all" else selected_line
                vehicle_id = None if selected_vehicle == "all" else selected_vehicle
                limit = None if selected_limit == "all" else selected_limit
                
                # Get vehicle positions map
                vehicle_map = self.visualizer.visualize_vehicle_positions(line_id, vehicle_id, limit)
                
                # Get DataFrame statistics
                df_stats = self.visualizer.get_dataframe_statistics()
                if "error" in df_stats:
                    df_stats_content = html.Div([
                        html.P(df_stats["error"], className="text-danger")
                    ])
                else:
                    df_stats_content = html.Div([
                        html.P(f"📊 Total Records: {df_stats['total_records']:,}"),
                        html.P(f"📍 Valid Data Points: {df_stats['total_data_points']:,}"),
                        html.P(f"⚡ Top Speed: {df_stats['top_speed']:.1f} km/h"),
                        html.P(f"🚌 Unique Vehicles: {df_stats['unique_vehicles']:,}"),
                        html.P(f"🚇 Unique Lines: {df_stats['unique_lines']:,}"),
                        html.P(f"🎯 Unique Stops: {df_stats['unique_stops']:,}"),
                        html.P(f"⏰ Oldest Point: {df_stats['oldest_point'][:19] if df_stats['oldest_point'] != 'N/A' else 'N/A'}"),
                        html.P(f"🔄 Newest Point: {df_stats['newest_point'][:19] if df_stats['newest_point'] != 'N/A' else 'N/A'}")
                    ])
                
                # Get line/vehicle statistics
                if line_id or vehicle_id:
                    stats = self.visualizer.generate_line_statistics(line_id, vehicle_id)
                    if "error" in stats:
                        stats_content = html.Div([
                            html.P(stats["error"], className="text-danger")
                        ])
                    else:
                        stats_items = []
                        if vehicle_id:
                            stats_items.extend([
                                html.P(f"Vehicle ID: {stats['vehicle_id']}"),
                                html.P(f"Line ID: {stats['line_id']}")
                            ])
                        else:
                            stats_items.append(html.P(f"Line ID: {stats['line_id']}"))
                        
                        stats_items.extend([
                            html.P(f"Total Vehicles: {stats['total_vehicles']}"),
                            html.P(f"Total Records: {stats['total_records']}"),
                            html.P(f"Average Speed: {stats['avg_speed']:.2f} km/h"),
                            html.P(f"Average Delay: {stats['avg_delay']:.2f} minutes"),
                            html.P(f"Destinations: {', '.join(stats['destinations'][:3])}...")
                        ])
                        
                        stats_content = html.Div(stats_items)
                else:
                    stats_content = html.Div([
                        html.P("Select a specific line or vehicle to see statistics")
                    ])
                
                # Get speed and delay histograms
                df = self.visualizer.query_vehicle_positions(line_id, vehicle_id, limit=limit)
                
                if not df.empty and 'speed' in df.columns:
                    # Create speed buckets
                    speed_data = df[df['speed'].notna()].copy()
                    speed_data['speed_bucket'] = pd.cut(
                        speed_data['speed'],
                        bins=[0, 15, 30, 45, float('inf')],
                        labels=['0-15', '15-30', '30-45', '45+'],
                        include_lowest=True
                    )
                    speed_counts = speed_data['speed_bucket'].value_counts().reindex(['0-15', '15-30', '30-45', '45+'], fill_value=0)
                    
                    speed_hist = px.bar(
                        x=speed_counts.index,
                        y=speed_counts.values,
                        title="Speed Distribution",
                        labels={"x": "Speed (km/h)", "y": "Count"},
                        color=speed_counts.index,
                        color_discrete_map={
                            '0-15': '#440154',   # Dark purple (low speed)
                            '15-30': '#31688e',  # Dark blue 
                            '30-45': '#35b779',  # Green
                            '45+': '#fde725'     # Bright yellow (high speed)
                        }
                    )
                else:
                    speed_hist = px.bar(title="No speed data available")
                
                if not df.empty and 'delay' in df.columns:
                    # Create delay buckets
                    delay_data = df[df['delay'].notna()].copy()
                    delay_data['delay_bucket'] = pd.cut(
                        delay_data['delay'],
                        bins=[0, 15, 20, 25, 30, 35, float('inf')],
                        labels=['0-15', '15-20', '20-25', '25-30', '30-35', '35+'],
                        include_lowest=True
                    )
                    delay_counts = delay_data['delay_bucket'].value_counts().reindex(['0-15', '15-20', '20-25', '25-30', '30-35', '35+'], fill_value=0)
                    
                    delay_hist = px.bar(
                        x=delay_counts.index,
                        y=delay_counts.values,
                        title="Delay Distribution",
                        labels={"x": "Delay (minutes)", "y": "Count"},
                        color=delay_counts.index,
                        color_discrete_map={
                            '0-15': '#2166ac',   # Blue (good)
                            '15-20': '#5aae61',  # Light green
                            '20-25': '#fee08b',  # Yellow
                            '25-30': '#fdae61',  # Orange
                            '30-35': '#f46d43',  # Red-orange
                            '35+': '#d73027'     # Red (bad)
                        }
                    )
                else:
                    delay_hist = px.bar(title="No delay data available")
                
                # Create data quality chart
                if not df.empty:
                    quality_data = {
                        "Metric": ["Records with Speed", "Records with Coordinates", "Records with Delays"],
                        "Count": [
                            len(df[df['speed'].notna()]),
                            len(df[(df['latitude'].notna()) & (df['longitude'].notna())]),
                            len(df[df['delay'].notna()])
                        ]
                    }
                    quality_fig = px.bar(
                        x=quality_data["Metric"],
                        y=quality_data["Count"],
                        title="Data Quality Overview",
                        labels={"x": "Data Type", "y": "Count"},
                        color=quality_data["Count"],
                        color_continuous_scale="Blues"
                    )
                else:
                    quality_fig = px.bar(title="No data available for quality analysis")
                
                auto_status = " (Auto-refresh enabled)" if auto_refresh_enabled and "enabled" in auto_refresh_enabled else ""
                status_message = html.Div([
                    html.P(f"Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}{auto_status}", 
                           className="text-success small")
                ])
                
                return vehicle_map, df_stats_content, stats_content, speed_hist, quality_fig, delay_hist, status_message
                
            except Exception as e:
                logger.error(f"Error updating dashboard: {e}")
                empty_fig = px.scatter_mapbox()
                error_msg = html.Div([
                    html.P(f"Error: {str(e)}", className="text-danger")
                ])
                return empty_fig, error_msg, error_msg, empty_fig, empty_fig, empty_fig, error_msg
    
    def run(self, debug=False, port=8050):
        """Run the dashboard"""
        logger.info(f"Starting dashboard on port {port}")
        self.app.run_server(debug=debug, port=port)

if __name__ == "__main__":
    dashboard = GTFSRealtimeDashboard()
    dashboard.run(debug=False, port=8050)
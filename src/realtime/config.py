import os
from dotenv import load_dotenv

load_dotenv()

# STIB-MIVB API Configuration
API_KEY = os.getenv("STIB_API_KEY", "")
BASE_URL = "https://data.stib-mivb.brussels/api/explore/v2.1"

# API Endpoints
ENDPOINTS = {
    "vehicle_positions": f"{BASE_URL}/catalog/datasets/vehicle-position-rt-production/records",
    "waiting_times": f"{BASE_URL}/catalog/datasets/waiting-time-rt-production/records",
    "disruptions": f"{BASE_URL}/catalog/datasets/travellers-information-rt-production/records",
    "stop_details": f"{BASE_URL}/catalog/datasets/stop-details-production/records",
    "stop_details_by_id": f"{BASE_URL}/catalog/datasets/stop-details-production/records"
}

# Data Storage Configuration
DATA_DIR = "src/data/realtime"
PARQUET_DIR = f"{DATA_DIR}/parquet"
VEHICLE_POSITIONS_FILE = f"{PARQUET_DIR}/vehicle_positions.parquet"
TRIP_UPDATES_FILE = f"{PARQUET_DIR}/trip_updates.parquet"
SERVICE_ALERTS_FILE = f"{PARQUET_DIR}/service_alerts.parquet"

# Refresh interval in seconds
REFRESH_INTERVAL = 20

# Request headers
HEADERS = {
    "Content-Type": "application/json"
}

# API Key parameter (passed as query parameter)
API_PARAMS = {
    "apikey": API_KEY
}
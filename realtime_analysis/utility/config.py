"""
Configuration helpers for realtime GTFS ingestion and analysis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()


def _parse_csv_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [token.strip() for token in value.split(",") if token.strip()]


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    db_host: str = os.getenv("PGHOST", "localhost")
    db_port: int = int(os.getenv("PGPORT", "5432"))
    db_name: str = os.getenv("PGDATABASE", "gtfs")
    db_user: str = os.getenv("PGUSER", "postgres")
    db_password: str = os.getenv("PGPASSWORD", "postgres")

    vehicle_positions_url: str = os.getenv(
        "GTFS_VEHICLE_POSITIONS_URL",
        "https://gtfsapi.translink.ca/v3/gtfsposition",
    )
    trip_updates_url: str = os.getenv(
        "GTFS_TRIP_UPDATES_URL",
        "https://gtfsapi.translink.ca/v3/gtfsrealtime",
    )
    api_key: Optional[str] = os.getenv("TRANSLINK_GTFSR_API_KEY")

    default_poll_interval: int = int(os.getenv("GTFS_RT_POLL_INTERVAL", "15"))
    default_duration_minutes: int = int(os.getenv("GTFS_RT_DURATION_MINUTES", "30"))

    target_route_ids: List[str] = field(
        default_factory=lambda: _parse_csv_list(os.getenv("TARGET_ROUTE_IDS"))
    )
    target_route_short_names: List[str] = field(
        default_factory=lambda: _parse_csv_list(
            os.getenv("TARGET_ROUTE_SHORT_NAMES")
        )
    )

    output_dir: Path = Path(
        os.getenv("GTFS_RT_OUTPUT_DIR", "realtime_analysis/output")
    ).expanduser()

    def has_route_filter(self) -> bool:
        return bool(self.target_route_ids or self.target_route_short_names)


def load_settings() -> Settings:
    """
    Helper exposed for callers that prefer a simple function.
    """

    return Settings()




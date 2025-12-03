"""
Utility helpers shared across realtime ingestion and analysis scripts.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Sequence, Set

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor

from realtime_analysis.utility.config import Settings, load_settings


@dataclass(frozen=True)
class RouteFilter:
    """
    Normalized route filter representation.

    Instances contain the resolved GTFS `route_id` values plus the original
    short-name tokens so scripts can echo the user's intent in logs.
    """

    route_ids: Set[str]
    route_short_names: Set[str]

    def applies(self) -> bool:
        return bool(self.route_ids or self.route_short_names)


def get_connection(settings: Optional[Settings] = None, **overrides: str) -> connection:
    """
    Build a psycopg2 connection using either the provided `Settings` object
    or the defaults loaded from the environment.
    """

    settings = settings or load_settings()
    params = dict(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )
    params.update(overrides)
    return psycopg2.connect(**params)


@contextmanager
def get_cursor(
    settings: Optional[Settings] = None,
    *,
    cursor_factory=RealDictCursor,
    autocommit: bool = False,
):
    """
    Convenience context manager returning an open cursor.
    """

    conn = get_connection(settings)
    try:
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield cur
            if not autocommit:
                conn.commit()
    finally:
        conn.close()


def resolve_route_filter(
    conn: connection,
    route_ids: Optional[Sequence[str]] = None,
    route_short_names: Optional[Sequence[str]] = None,
) -> RouteFilter:
    """
    Expand user-provided route identifiers into concrete GTFS `route_id` values.

    Parameters
    ----------
    conn:
        Active psycopg2 connection.
    route_ids:
        Iterable of `route_id` strings.
    route_short_names:
        Iterable of short names (e.g., "99") as used in GTFS `routes.txt`.
    """

    norm_route_ids: Set[str] = {
        token.strip() for token in (route_ids or []) if token and token.strip()
    }
    short_names: Set[str] = {
        token.strip() for token in (route_short_names or []) if token and token.strip()
    }

    if short_names:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT route_id, route_short_name
                FROM routes
                WHERE route_short_name = ANY(%s)
                """,
                (list(short_names),),
            )
            for route_id, route_short_name in cur.fetchall():
                norm_route_ids.add(route_id)
                short_names.add(route_short_name or "")

    return RouteFilter(route_ids=norm_route_ids, route_short_names=short_names)


def parse_service_date(raw: Optional[str]) -> Optional[date]:
    """
    Convert a GTFS-Realtime `start_date` payload (YYYYMMDD) to `datetime.date`.
    """

    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError:
        return None


def build_trip_instance_id(
    *,
    trip_id: Optional[str],
    start_date: Optional[date],
    start_time: Optional[str],
    vehicle_id: Optional[str],
    fallback_timestamp: datetime,
) -> str:
    """
    Derive a reproducible identifier representing a physical trip instance.

    The identifier is used to associate vehicle position samples, trip updates,
    and derived trajectories that belong to the same in-service run.
    """

    parts = []
    if trip_id:
        parts.append(trip_id)
    if start_date:
        parts.append(start_date.strftime("%Y%m%d"))
    if start_time:
        parts.append(start_time.replace(":", ""))

    if parts:
        return "_".join(parts)

    if vehicle_id:
        return f"{vehicle_id}_{fallback_timestamp.strftime('%Y%m%dT%H%M%S')}"

    return f"trip_{fallback_timestamp.strftime('%Y%m%dT%H%M%S')}"


def ensure_output_dir(path: Optional[str]) -> Path:
    """
    Ensure the target output directory exists and return it as a Path instance.
    """

    if path is None:
        raise ValueError("Output directory path must not be None.")

    out_path = Path(path).expanduser()
    out_path.mkdir(parents=True, exist_ok=True)
    return out_path



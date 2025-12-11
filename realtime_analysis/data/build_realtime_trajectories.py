#!/usr/bin/env python3
"""
Convert GTFS-Realtime vehicle position samples into MobilityDB trajectories.

Map Matching Strategy:
This script supports two map matching approaches (controlled by USE_VALHALLA_MAPMATCHING env var):

**Valhalla Map Matching (Recommended)**:
- Uses Valhalla's Meili map matching engine to match GPS traces to actual road network
- Follows streets and roads accurately using OpenStreetMap data
- Handles GPS noise and sparse data using Hidden Markov Models
- Requires Valhalla service running (see docker-compose.yml)
- Set USE_VALHALLA_MAPMATCHING=true to enable

**SQL-based Matching (Fallback)**:
- Hierarchical approach using GTFS shapes and scheduled trajectories
- Faster but less accurate - projects points onto lines, doesn't follow streets
- Used when Valhalla is disabled or unavailable
- Falls back in this order:
  1. GTFS Shape Matching (trajectories table from shapes.txt)
  2. Scheduled Trip Trajectory Matching (scheduled_trips_mdb)
  3. Raw GPS coordinates
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, List, Dict, Any
import json

import requests

from realtime_analysis.utility.config import load_settings
from realtime_analysis.utility.utils import get_connection, resolve_route_filter

LOG = logging.getLogger("realtime_analysis.build_trajs")


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_iso_timestamp(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


def _decode_polyline6(encoded: str) -> List[Dict[str, float]]:
    """Decode a Valhalla polyline6 string into a list of lat/lon dicts."""
    if not encoded:
        return []

    coordinates: List[Dict[str, float]] = []
    index = 0
    lat = 0
    lon = 0
    length = len(encoded)

    def _decode_value() -> int:
        nonlocal index
        result = 0
        shift = 0
        while index < length:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        delta = ~(result >> 1) if (result & 1) else (result >> 1)
        return delta

    while index < length:
        lat += _decode_value()
        lon += _decode_value()
        coordinates.append({"lat": lat / 1e6, "lon": lon / 1e6})

    return coordinates


def _resample_points(points: List[Dict[str, float]], target_len: int) -> List[Dict[str, float]]:
    """Resample/align a list of points to the desired length."""
    if target_len <= 0 or not points:
        return []
    if len(points) == target_len:
        return points
    if target_len == 1:
        return [points[0]]

    resampled: List[Dict[str, float]] = []
    max_index = len(points) - 1
    for i in range(target_len):
        ratio = i / (target_len - 1)
        idx = int(round(ratio * max_index))
        resampled.append(points[idx])
    return resampled


def valhalla_map_match(
    points: List[Dict[str, Any]], valhalla_url: str, timeout: int = 30, verbose: bool = False
) -> Optional[Dict[str, List[Dict[str, float]]]]:
    """
    Map match a sequence of GPS points using Valhalla's trace_route API (OSRM format).
    
    Args:
        points: List of dicts with 'lat', 'lon', and optionally 'time' (ISO timestamp)
        valhalla_url: Base URL for Valhalla service
        timeout: Request timeout in seconds
    
    Returns:
        List of matched points with 'lat' and 'lon', or None if matching failed
    """
    if not points or len(points) < 2:
        return None
    
    # Prepare shape for Valhalla (list of dicts with named fields)
    shape: List[Dict[str, float]] = []
    has_timestamps = True  # Only stays True if every point has a timestamp
    
    for pt in points:
        lon = float(pt["lon"])
        lat = float(pt["lat"])
        
        # Validate coordinates
        if not (-180 <= lon <= 180) or not (-90 <= lat <= 90):
            LOG.warning("Invalid coordinates: lon=%f, lat=%f", lon, lat)
            return None
        
        shape_item: Dict[str, float] = {"lon": lon, "lat": lat}

        timestamp = pt.get("time")
        if timestamp is not None:
            if isinstance(timestamp, datetime):
                shape_item["time"] = int(timestamp.timestamp())
            else:
                shape_item["time"] = int(timestamp)
        else:
            has_timestamps = False

        shape.append(shape_item)
    
    if len(shape) < 2:
        LOG.warning("Need at least 2 points for map matching, got %d", len(shape))
        return None
    
    # Valhalla trace_route endpoint for map matching
    url = f"{valhalla_url}/trace_route"
    
    # Build payload - start with minimal required fields
    payload = {
        "shape": shape,
        "costing": "auto",
        "shape_match": "map_snap",
        "format": "osrm",
        "trace_options": {"search_radius": 75},
    }
    
    # Use timestamps only if every point had one
    if has_timestamps:
        payload["use_timestamps"] = True
    
    # Add optional parameters
    payload["gps_accuracy"] = 10.0  # GPS accuracy in meters
    payload["search_radius"] = 50.0  # Search radius in meters
    
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        
        # Check status before parsing JSON
        if response.status_code != 200:
            try:
                error_detail = response.json()
                # Handle different error response formats
                if isinstance(error_detail, dict):
                    error_msg = error_detail.get("error", error_detail.get("message", str(error_detail)))
                    if isinstance(error_msg, dict):
                        error_msg = error_msg.get("message", str(error_msg))
                else:
                    error_msg = str(error_detail)
                LOG.warning("Valhalla returned status %d: %s", response.status_code, error_msg)
                if verbose:
                    LOG.debug("Full error response: %s", json.dumps(error_detail, indent=2))
                    LOG.debug("Request payload (first 1000 chars): %s", json.dumps(payload, indent=2)[:1000])
            except Exception as e:
                LOG.warning("Valhalla returned status %d: %s (parse error: %s)", 
                           response.status_code, response.text[:200], e)
                if verbose:
                    LOG.debug("Request payload: %s", json.dumps(payload, indent=2)[:1000])
            return None
        
        result = response.json()
        
        # Extract matched shape/points from OSRM format
        full_shape: List[Dict[str, float]] = []
        matched_points: List[Dict[str, float]] = []

        matchings = result.get("matchings")
        tracepoints = result.get("tracepoints")

        if isinstance(matchings, list) and matchings:
            geometry = matchings[0].get("geometry")
            if geometry:
                full_shape = _decode_polyline6(geometry)

        if isinstance(tracepoints, list):
            for tp in tracepoints:
                if tp and "location" in tp:
                    lon, lat = tp["location"]
                    matched_points.append({"lat": float(lat), "lon": float(lon)})
                else:
                    matched_points.append(None)  # placeholder to keep length

        # Replace None entries or use full_shape resampling
        cleaned_points: List[Dict[str, float]] = []
        valid = True
        for idx, mp in enumerate(matched_points):
            if mp is None:
                valid = False
                break
            cleaned_points.append(mp)

        if not valid:
            matched_points = []
        else:
            matched_points = cleaned_points

        if not matched_points and full_shape:
            matched_points = _resample_points(full_shape, len(points))

        if matched_points and len(matched_points) == len(points):
            return {"points": matched_points, "shape": full_shape or matched_points}

        LOG.warning(
            "Valhalla returned %d points, expected %d (keys: %s)",
            len(matched_points),
            len(points),
            list(result.keys()),
        )
        if verbose:
            LOG.debug("Valhalla response sample: %s", str(result)[:500])
        return None
            
    except requests.exceptions.HTTPError as e:
        # Try to get error details from response
        try:
            error_detail = e.response.json() if e.response else {}
            LOG.warning("Valhalla map matching failed (HTTP %d): %s", 
                       e.response.status_code if e.response else 0,
                       error_detail.get("error", str(e)))
            if verbose:
                LOG.debug("Request payload: %s", json.dumps(payload)[:500])
        except:
            LOG.warning("Valhalla map matching failed: %s", e)
        return None
    except requests.exceptions.RequestException as e:
        LOG.warning("Valhalla map matching failed: %s", e)
        if verbose:
            LOG.debug("Request payload: %s", json.dumps(payload)[:500])
        return None
    except (KeyError, ValueError, IndexError) as e:
        LOG.warning("Error parsing Valhalla response: %s", e)
        return None


def build_trajs(
    *,
    start_ts: datetime,
    end_ts: datetime,
    route_ids: Optional[Sequence[str]],
    route_short_names: Optional[Sequence[str]],
    verbose: bool = False,
    truncate_table: bool = True,
) -> int:
    settings = load_settings()
    with get_connection(settings) as conn:
        route_filter = resolve_route_filter(
            conn, route_ids=route_ids, route_short_names=route_short_names
        )

        if truncate_table:
            LOG.info("Truncating realtime_trips_mdb before rebuild.")
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE realtime_trips_mdb;")
            conn.commit()

        LOG.info(
            "Building trajectories from %s to %s (routes: %s)",
            start_ts.isoformat(),
            end_ts.isoformat(),
            ", ".join(sorted(route_filter.route_ids)) if route_filter.route_ids else "ALL",
        )

        # If Valhalla is enabled, use it for map matching
        if settings.use_valhalla:
            return _build_trajs_with_valhalla(
                conn, start_ts, end_ts, route_filter, settings, verbose
            )
        else:
            return _build_trajs_sql_only(
                conn, start_ts, end_ts, route_filter, verbose
            )


def _build_trajs_with_valhalla(
    conn, start_ts: datetime, end_ts: datetime, route_filter, settings, verbose: bool
) -> int:
    """Build trajectories using Valhalla for map matching."""
    LOG.info("Using Valhalla for map matching")
    
    # Fetch GPS points grouped by trip_instance_id
    fetch_sql = """
        SELECT
            trip_instance_id,
            trip_id,
            route_id,
            COALESCE(start_date, entity_timestamp::date) AS service_date,
            vehicle_id,
            entity_timestamp,
            latitude,
            longitude
        FROM rt_vehicle_positions
        WHERE trip_instance_id IS NOT NULL
          AND entity_timestamp BETWEEN %(start_ts)s AND %(end_ts)s
          AND (
                %(filter_routes)s = FALSE
                OR route_id = ANY(%(route_ids)s)
          )
        ORDER BY trip_instance_id, entity_timestamp
    """
    
    params = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "filter_routes": route_filter.applies(),
        "route_ids": list(route_filter.route_ids),
    }
    
    with conn.cursor() as cur:
        cur.execute(fetch_sql, params)
        rows = cur.fetchall()
    
    # Group by trip_instance_id and map match each trip
    trips_data: Dict[str, List[Dict[str, Any]]] = {}
    last_ts_by_trip: Dict[str, Optional[datetime]] = {}
    for row in rows:
        trip_instance_id = row[0]
        entity_ts = row[5]

        # Skip non-increasing timestamps (MobilityDB requires strictly increasing)
        last_ts = last_ts_by_trip.get(trip_instance_id)
        if last_ts is not None and entity_ts <= last_ts:
            continue
        last_ts_by_trip[trip_instance_id] = entity_ts

        if trip_instance_id not in trips_data:
            trips_data[trip_instance_id] = []
        trips_data[trip_instance_id].append({
            "trip_id": row[1],
            "route_id": row[2],
            "service_date": row[3],
            "vehicle_id": row[4],
            "time": entity_ts,
            "lat": row[6],
            "lon": row[7],
        })
    
    # Map match each trip using Valhalla
    matched_trips: Dict[str, Dict[str, Any]] = {}
    total_trips = len(trips_data)
    matched_count = 0
    skipped_count = 0
    
    for trip_instance_id, points in trips_data.items():
        if len(points) < 2:
            continue
        
        matched = valhalla_map_match(points, settings.valhalla_url, verbose=verbose)
        if matched and len(matched["points"]) == len(points):
            matched_trips[trip_instance_id] = {
                "points": [
                    {**points[i], "lat": matched["points"][i]["lat"], "lon": matched["points"][i]["lon"]}
                    for i in range(len(points))
                ],
                "shape": matched["shape"],
            }
            matched_count += 1
        else:
            skipped_count += 1
            LOG.warning(
                "Skipping trip_instance_id %s: Valhalla map matching failed (points=%d, matched=%s)",
                trip_instance_id,
                len(points),
                "None" if matched is None else len(matched),
            )
            continue
        
        if verbose and (matched_count % 10 == 0 or matched_count == total_trips):
            LOG.debug("Map matched %d/%d trips", matched_count, total_trips)
    
    LOG.info(
        "Valhalla matched %d/%d trips (skipped %d)",
        matched_count,
        total_trips,
        skipped_count,
    )
    
    # Insert matched points into a temporary table
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE temp_matched_points (
                trip_instance_id text,
                trip_id text,
                route_id text,
                service_date date,
                vehicle_id text,
                entity_timestamp timestamptz,
                latitude double precision,
                longitude double precision
            )
        """)
        
        for trip_instance_id, data in matched_trips.items():
            points = data["points"]
            prev_ts: Optional[datetime] = None
            for pt in points:
                ts = pt["time"]
                if prev_ts is not None and ts <= prev_ts:
                    continue
                prev_ts = ts
                cur.execute("""
                    INSERT INTO temp_matched_points VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    trip_instance_id,
                    pt["trip_id"],
                    pt["route_id"],
                    pt["service_date"],
                    pt["vehicle_id"],
                    ts,
                    pt["lat"],
                    pt["lon"],
                ))

        # Store Valhalla polyline geometry for each trip
        cur.execute("""
            CREATE TEMP TABLE temp_matched_shapes (
                trip_instance_id text PRIMARY KEY,
                shape geometry(LineString, 4326)
            )
        """)
        for trip_instance_id, data in matched_trips.items():
            shape_coords = data["shape"]
            if not shape_coords or len(shape_coords) < 2:
                continue
            linestring_wkt = "LINESTRING({})".format(
                ", ".join(f"{coord['lon']} {coord['lat']}" for coord in shape_coords)
            )
            cur.execute(
                "INSERT INTO temp_matched_shapes VALUES (%s, ST_SetSRID(ST_GeomFromText(%s), 4326))",
                (trip_instance_id, linestring_wkt),
            )
        
        # Build trajectories from matched points
        sql = """
        WITH sequences AS (
            SELECT
                trip_instance_id,
                trip_id,
                route_id,
                service_date,
                vehicle_id,
                tgeompoint(
                    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),
                    entity_timestamp
                ) AS point
            FROM temp_matched_points
        ),
        combined AS (
            SELECT
                trip_instance_id,
                (array_agg(DISTINCT trip_id) FILTER (WHERE trip_id IS NOT NULL))[1] AS trip_id,
                (array_agg(DISTINCT route_id) FILTER (WHERE route_id IS NOT NULL))[1] AS route_id,
                (array_agg(DISTINCT service_date) FILTER (WHERE service_date IS NOT NULL))[1] AS service_date,
                (array_agg(DISTINCT vehicle_id) FILTER (WHERE vehicle_id IS NOT NULL))[1] AS vehicle_id,
                array_agg(point ORDER BY getTimestamp(point)) AS points
            FROM sequences
            GROUP BY trip_instance_id
            HAVING COUNT(*) > 1
        ),
        trajs AS (
            SELECT
                trip_instance_id,
                trip_id,
                route_id,
                service_date,
                vehicle_id,
                tgeompointseq(points) AS trip,
                trajectory(tgeompointseq(points)) AS traj
            FROM combined
        )
        INSERT INTO realtime_trips_mdb (
            trip_instance_id, trip_id, route_id, service_date,
            vehicle_id, trip, traj, starttime, updated_at
        )
        SELECT
            t.trip_instance_id,
            t.trip_id,
            t.route_id,
            t.service_date,
            t.vehicle_id,
            t.trip,
            COALESCE(ms.shape, t.traj) AS traj,
            startTimestamp(t.trip) AS starttime,
            now()
        FROM trajs t
        LEFT JOIN temp_matched_shapes ms USING (trip_instance_id)
        ON CONFLICT (trip_instance_id) DO UPDATE
        SET
            trip = EXCLUDED.trip,
            traj = EXCLUDED.traj,
            service_date = EXCLUDED.service_date,
            route_id = EXCLUDED.route_id,
            vehicle_id = EXCLUDED.vehicle_id,
            starttime = EXCLUDED.starttime,
            updated_at = now()
        RETURNING trip_instance_id;
        """
        
        cur.execute(sql)
        inserted = cur.fetchall()
        conn.commit()
    
    LOG.info("Updated %s trajectory records.", len(inserted))
    return len(inserted)


def _build_trajs_sql_only(
    conn, start_ts: datetime, end_ts: datetime, route_filter, verbose: bool
) -> int:
    """Build trajectories using SQL-based map matching (original approach)."""
    sql = """
        WITH base AS (
            SELECT
                trip_instance_id,
                trip_id,
                route_id,
                COALESCE(start_date, entity_timestamp::date) AS service_date,
                vehicle_id,
                entity_timestamp,
                fetch_timestamp,
                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom,
                ROW_NUMBER() OVER (
                    PARTITION BY trip_instance_id, entity_timestamp
                    ORDER BY fetch_timestamp
                ) AS rn
            FROM rt_vehicle_positions
            WHERE trip_instance_id IS NOT NULL
              AND entity_timestamp BETWEEN %(start_ts)s AND %(end_ts)s
              AND (
                    %(filter_routes)s = FALSE
                    OR route_id = ANY(%(route_ids)s)
              )
        ),
        dedup AS (
            SELECT *
            FROM base
            WHERE rn = 1
        ),
        -- Get shape_id for each trip (GTFS shapes represent actual route geometry)
        trip_shapes AS (
            SELECT DISTINCT trip_id, shape_id
            FROM trips
            WHERE shape_id IS NOT NULL AND shape_id != ''
        ),
        -- Match GPS points to GTFS shapes (better than scheduled trajectories)
        -- GTFS shapes represent the actual route path, not just scheduled trips
        sequences AS (
            SELECT
                d.trip_instance_id,
                d.trip_id,
                d.route_id,
                d.service_date,
                d.vehicle_id,
                tgeompoint(
                    CASE
                        -- First try: Match to GTFS shape (actual route geometry)
                        WHEN t.traj IS NOT NULL THEN
                            ST_LineInterpolatePoint(
                                ST_LineMerge(t.traj),
                                LEAST(1.0, GREATEST(0.0, ST_LineLocatePoint(ST_LineMerge(t.traj), d.geom)))
                            )
                        -- Fallback: Match to scheduled trip trajectory
                        WHEN s.traj IS NOT NULL THEN
                            ST_LineInterpolatePoint(
                                ST_LineMerge(s.traj),
                                LEAST(1.0, GREATEST(0.0, ST_LineLocatePoint(ST_LineMerge(s.traj), d.geom)))
                            )
                        -- Last resort: Use original GPS point
                        ELSE d.geom
                    END,
                    d.entity_timestamp
                ) AS point
            FROM dedup d
            -- Try to match to GTFS shape first (most accurate)
            LEFT JOIN trip_shapes ts ON d.trip_id = ts.trip_id
            LEFT JOIN trajectories t ON ts.shape_id = t.shape_id
            -- Fallback to scheduled trip trajectory
            LEFT JOIN scheduled_trips_mdb s 
                ON d.trip_id = s.trip_id 
                AND d.service_date = s.date
        ),
        combined AS (
            SELECT
                trip_instance_id,
                -- Take the first non-null value for each field
                (array_agg(DISTINCT trip_id) FILTER (WHERE trip_id IS NOT NULL))[1] AS trip_id,
                (array_agg(DISTINCT route_id) FILTER (WHERE route_id IS NOT NULL))[1] AS route_id,
                (array_agg(DISTINCT service_date) FILTER (WHERE service_date IS NOT NULL))[1] AS service_date,
                (array_agg(DISTINCT vehicle_id) FILTER (WHERE vehicle_id IS NOT NULL))[1] AS vehicle_id,
                array_agg(point ORDER BY getTimestamp(point)) AS points
            FROM sequences
            GROUP BY trip_instance_id
            HAVING COUNT(*) > 1
        ),
        trajs AS (
            SELECT
                trip_instance_id,
                trip_id,
                route_id,
                service_date,
                vehicle_id,
                tgeompointseq(points) AS trip,
                trajectory(tgeompointseq(points)) AS traj
            FROM combined
        )
        INSERT INTO realtime_trips_mdb (
            trip_instance_id, trip_id, route_id, service_date,
            vehicle_id, trip, traj, starttime, updated_at
        )
        SELECT
            trip_instance_id,
            trip_id,
            route_id,
            service_date,
            vehicle_id,
            trip,
            traj,
            startTimestamp(trip) AS starttime,
            now()
        FROM trajs
        ON CONFLICT (trip_instance_id) DO UPDATE
        SET
            trip = EXCLUDED.trip,
            traj = EXCLUDED.traj,
            service_date = EXCLUDED.service_date,
            route_id = EXCLUDED.route_id,
            vehicle_id = EXCLUDED.vehicle_id,
            starttime = EXCLUDED.starttime,
            updated_at = now()
        RETURNING trip_instance_id;
        """

    params = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "filter_routes": route_filter.applies(),
        "route_ids": list(route_filter.route_ids),
    }

    with conn.cursor() as cur:
        cur.execute(sql, params)
        inserted = cur.fetchall()
        conn.commit()

    LOG.info("Updated %s trajectory records.", len(inserted))
    return len(inserted)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build MobilityDB trajectories from realtime vehicle positions."
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=3.0,
        help="Look back this many hours from now when selecting samples (default: 3).",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="ISO-8601 timestamp for the start of the window (overrides --hours).",
    )
    parser.add_argument(
        "--until",
        type=str,
        help="ISO-8601 timestamp for the end of the window (defaults to now).",
    )
    parser.add_argument(
        "--route-id",
        action="append",
        help="Restrict to these GTFS route_ids.",
    )
    parser.add_argument(
        "--route-short-name",
        action="append",
        help="Restrict to these GTFS route_short_names.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    parser.add_argument(
        "--keep-data",
        action="store_false",
        dest="truncate",
        help="Skip truncating realtime_trips_mdb before rebuilding.",
    )
    parser.set_defaults(truncate=True)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    end_ts = parse_iso_timestamp(args.until) or datetime.now(timezone.utc)
    if args.since:
        start_ts = parse_iso_timestamp(args.since)
        if not start_ts:
            raise ValueError("Failed to parse --since timestamp.")
    else:
        start_ts = end_ts - timedelta(hours=args.hours)

    if start_ts >= end_ts:
        raise ValueError("--since/--hours must define a window before --until/now.")

    build_trajs(
        start_ts=start_ts,
        end_ts=end_ts,
        route_ids=args.route_id,
        route_short_names=args.route_short_name,
        verbose=args.verbose,
        truncate_table=args.truncate,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




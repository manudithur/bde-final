#!/usr/bin/env python3
"""
Compare scheduled Vancouver GTFS data with GTFS-Realtime observations.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

import pandas as pd
import plotly.graph_objects as go

from realtime_analysis.config import load_settings
from realtime_analysis.utils import (
    ensure_output_dir,
    get_connection,
    resolve_route_filter,
)

LOG = logging.getLogger("realtime_analysis.analyze")


@dataclass
class TripSelection:
    trip_instance_id: str
    trip_id: str
    route_id: str
    service_date: Optional[datetime]


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def select_trip(
    conn,
    *,
    trip_instance_id: Optional[str],
    trip_id: Optional[str],
    route_ids: Optional[Sequence[str]],
    route_short_names: Optional[Sequence[str]],
) -> TripSelection:
    route_filter = resolve_route_filter(
        conn, route_ids=route_ids, route_short_names=route_short_names
    )

    with conn.cursor() as cur:
        params = {
            "trip_instance_id": trip_instance_id,
            "trip_id": trip_id,
            "filter_routes": route_filter.applies(),
            "route_ids": list(route_filter.route_ids),
        }

        cur.execute(
            """
            WITH candidates AS (
                SELECT
                    trip_instance_id,
                    trip_id,
                    route_id,
                    MAX(entity_timestamp) AS last_seen,
                    MAX(start_date) AS start_date
                FROM rt_vehicle_positions
                WHERE trip_instance_id IS NOT NULL
                  AND (
                        %(trip_instance_id)s IS NULL
                        OR trip_instance_id = %(trip_instance_id)s
                  )
                  AND (
                        %(trip_id)s IS NULL
                        OR trip_id = %(trip_id)s
                  )
                  AND (
                        %(filter_routes)s = FALSE
                        OR route_id = ANY(%(route_ids)s)
                  )
                GROUP BY trip_instance_id, trip_id, route_id
            )
            SELECT trip_instance_id, trip_id, route_id, start_date
            FROM candidates
            ORDER BY last_seen DESC
            LIMIT 1;
            """,
            params,
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                "No realtime trip found for the provided filters. "
                "Ensure that ingestion has run recently and filters match available data."
            )

    return TripSelection(
        trip_instance_id=row[0], trip_id=row[1], route_id=row[2], service_date=row[3]
    )


def fetch_actual_points(conn, trip_instance_id: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                vp.entity_timestamp,
                vp.latitude,
                vp.longitude,
                ST_Y(matched.geom) AS snapped_lat,
                ST_X(matched.geom) AS snapped_lon
            FROM rt_vehicle_positions vp
            LEFT JOIN LATERAL (
                SELECT
                    ST_LineInterpolatePoint(
                        sched.traj,
                        ST_LineLocatePoint(
                            sched.traj,
                            ST_SetSRID(ST_MakePoint(vp.longitude, vp.latitude), 4326)
                        )
                    ) AS geom
                FROM scheduled_trips_mdb sched
                WHERE sched.trip_id = vp.trip_id
                ORDER BY sched.date
                LIMIT 1
            ) matched ON TRUE
            WHERE vp.trip_instance_id = %s
            ORDER BY vp.entity_timestamp
            """,
            (trip_instance_id,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["timestamp", "lat", "lon", "snap_lat", "snap_lon"])


def fetch_scheduled_traj(conn, trip_id: str) -> Optional[Dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ST_AsGeoJSON(traj)
            FROM scheduled_trips_mdb
            WHERE trip_id = %s
            LIMIT 1
            """,
            (trip_id,),
        )
        row = cur.fetchone()
    if not row or not row[0]:
        return None
    return json.loads(row[0])


def fetch_segment_metrics(conn, trip_instance_id: str, trip_id: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    trip_instance_id,
                    trip_id,
                    route_id,
                    start_date,
                    stop_sequence,
                    stop_id,
                    arrival_time,
                    fetch_timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY trip_instance_id, stop_sequence
                        ORDER BY fetch_timestamp DESC
                    ) AS rn
                FROM rt_trip_updates
                WHERE trip_instance_id = %s
                  AND arrival_time IS NOT NULL
            ),
            dedup AS (
                SELECT *
                FROM ranked
                WHERE rn = 1
            ),
            ordered AS (
                SELECT
                    trip_instance_id,
                    trip_id,
                    route_id,
                    start_date,
                    stop_sequence,
                    stop_id,
                    arrival_time,
                    LEAD(stop_sequence) OVER w AS next_stop_sequence,
                    LEAD(stop_id) OVER w AS next_stop_id,
                    LEAD(arrival_time) OVER w AS next_arrival_time
                FROM dedup
                WINDOW w AS (
                    PARTITION BY trip_instance_id
                    ORDER BY stop_sequence
                )
            )
            SELECT
                o.trip_instance_id,
                o.trip_id,
                o.route_id,
                o.start_date AS service_date,
                o.stop_sequence,
                o.next_stop_sequence,
                o.stop_id,
                o.next_stop_id,
                s1.stop_name,
                s2.stop_name,
                rs.seg_length,
                EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)) AS scheduled_seconds,
                EXTRACT(EPOCH FROM (o.next_arrival_time - o.arrival_time)) AS actual_seconds,
                ST_AsGeoJSON(rs.seg_geom) AS seg_geojson
            FROM ordered o
            JOIN route_segments rs
              ON rs.trip_id = o.trip_id
             AND rs.stop1_sequence = o.stop_sequence
            LEFT JOIN stops s1 ON s1.stop_id = o.stop_id
            LEFT JOIN stops s2 ON s2.stop_id = o.next_stop_id
            WHERE o.next_arrival_time IS NOT NULL
              AND rs.seg_length > 0
              AND EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)) > 0
              AND EXTRACT(EPOCH FROM (o.next_arrival_time - o.arrival_time)) > 0
            ORDER BY o.stop_sequence;
            """,
            (trip_instance_id,),
        )
        rows = cur.fetchall()

    cols = [
        "trip_instance_id",
        "trip_id",
        "route_id",
        "service_date",
        "stop_sequence",
        "next_stop_sequence",
        "stop_id",
        "next_stop_id",
        "stop_name",
        "next_stop_name",
        "seg_length",
        "scheduled_seconds",
        "actual_seconds",
        "seg_geojson",
    ]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        raise RuntimeError(
            "No trip updates available for this trip. "
            "Ensure the ingestion captured TripUpdates alongside VehiclePositions."
        )

    df["stop_name"] = df["stop_name"].fillna(
        df["stop_sequence"].astype(int).apply(lambda x: f"Stop {x}")
    )
    df["next_stop_name"] = df["next_stop_name"].fillna(
        df["next_stop_sequence"].astype(int).apply(lambda x: f"Stop {x}")
    )
    df["segment_label"] = df["stop_name"] + " → " + df["next_stop_name"]

    numeric_cols = ["seg_length", "scheduled_seconds", "actual_seconds"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["scheduled_speed_kmh"] = df["seg_length"] / df["scheduled_seconds"] * 3.6
    df["actual_speed_kmh"] = df["seg_length"] / df["actual_seconds"] * 3.6
    df["speed_delta_kmh"] = df["actual_speed_kmh"] - df["scheduled_speed_kmh"]
    df["time_delta_seconds"] = df["actual_seconds"] - df["scheduled_seconds"]
    return df


def _extract_line_coordinates(geojson_obj: Dict) -> Tuple[Sequence[float], Sequence[float]]:
    coords = geojson_obj.get("coordinates")
    if not coords:
        return [], []
    if geojson_obj["type"] == "LineString":
        lon, lat = zip(*coords)
        return lat, lon
    # MultiLineString
    flattened = [point for line in coords for point in line]
    lon, lat = zip(*flattened)
    return lat, lon


def plot_trajectory_map(
    actual_points: pd.DataFrame,
    scheduled_geojson: Optional[Dict],
    output_dir: Path,
    trip: TripSelection,
) -> Path:
    if actual_points.empty:
        raise RuntimeError("No vehicle position samples found for this trip.")

    actual_points["lat"] = pd.to_numeric(actual_points["lat"], errors="coerce")
    actual_points["lon"] = pd.to_numeric(actual_points["lon"], errors="coerce")
    actual_points["snap_lat"] = pd.to_numeric(actual_points.get("snap_lat"), errors="coerce")
    actual_points["snap_lon"] = pd.to_numeric(actual_points.get("snap_lon"), errors="coerce")

    display_lat = actual_points["snap_lat"].where(
        actual_points["snap_lat"].notna(), actual_points["lat"]
    )
    display_lon = actual_points["snap_lon"].where(
        actual_points["snap_lon"].notna(), actual_points["lon"]
    )

    lat_center = display_lat.mean()
    lon_center = display_lon.mean()

    fig = go.Figure()

    if scheduled_geojson:
        sched_lat, sched_lon = _extract_line_coordinates(scheduled_geojson)
        fig.add_trace(
            go.Scattermapbox(
                name="Scheduled trajectory",
                lat=sched_lat,
                lon=sched_lon,
                mode="lines",
                line=dict(color="#1f77b4", width=3),
                hoverinfo="skip",
            )
        )

    fig.add_trace(
        go.Scattermapbox(
            name="Actual vehicle path",
            lat=display_lat,
            lon=display_lon,
            mode="markers+lines",
            marker=dict(
                size=7,
                color=actual_points.index,
                colorscale="Turbo",
                showscale=False,
            ),
            text=actual_points["timestamp"].dt.strftime("%H:%M:%S"),
            hovertemplate="Time %{text}<extra></extra>",
        )
    )

    fig.update_layout(
        title=f"Actual vs scheduled trajectory ({trip.trip_id})",
        mapbox=dict(style="open-street-map", center=dict(lat=lat_center, lon=lon_center), zoom=12),
        margin=dict(l=0, r=0, t=50, b=0),
        legend=dict(bgcolor="rgba(255,255,255,0.6)"),
    )

    output_path = output_dir / f"trajectory_{trip.trip_instance_id}.html"
    fig.write_html(output_path)
    return output_path


def plot_speed_delta_map(df: pd.DataFrame, output_dir: Path, trip: TripSelection) -> Path:
    fig = go.Figure()
    for _, row in df.iterrows():
        geo = json.loads(row["seg_geojson"])
        lat, lon = _extract_line_coordinates(geo)
        fig.add_trace(
            go.Scattermapbox(
                lat=lat,
                lon=lon,
                mode="lines",
                line=dict(
                    width=5,
                    color="#2ca02c" if row["speed_delta_kmh"] >= 0 else "#d62728",
                ),
                name=f"{row['segment_label']} ({row['speed_delta_kmh']:.1f} km/h)",
                hovertemplate=(
                    f"{row['segment_label']}<br>"
                    f"Δ speed: {row['speed_delta_kmh']:.1f} km/h<br>"
                    f"Actual: {row['actual_speed_kmh']:.1f} km/h<br>"
                    f"Scheduled: {row['scheduled_speed_kmh']:.1f} km/h<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        title=f"Speed delta by segment ({trip.trip_id})",
        mapbox=dict(style="open-street-map"),
        margin=dict(l=0, r=0, t=50, b=0),
        showlegend=False,
    )

    output_path = output_dir / f"speed_delta_map_{trip.trip_instance_id}.html"
    fig.write_html(output_path)
    return output_path


def plot_travel_time_bars(df: pd.DataFrame, output_dir: Path, trip: TripSelection) -> Path:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="Scheduled",
            x=df["segment_label"],
            y=df["scheduled_seconds"] / 60.0,
            marker_color="#1f77b4",
        )
    )
    fig.add_trace(
        go.Bar(
            name="Actual",
            x=df["segment_label"],
            y=df["actual_seconds"] / 60.0,
            marker_color="#ff7f0e",
        )
    )
    fig.update_layout(
        title=f"Travel time per segment ({trip.trip_id})",
        xaxis_title="Consecutive stops",
        yaxis_title="Minutes",
        barmode="group",
        bargap=0.2,
        xaxis_tickangle=-45,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=60, b=120),
    )

    output_path = output_dir / f"travel_time_{trip.trip_instance_id}.png"
    fig.write_image(output_path, scale=2)
    return output_path


def summarize_segments(df: pd.DataFrame, threshold_kmh: float = 5.0) -> Dict[str, pd.DataFrame]:
    faster = df[df["speed_delta_kmh"] > threshold_kmh]
    slower = df[df["speed_delta_kmh"] < -threshold_kmh]
    return {"faster": faster, "slower": slower}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze realtime trajectories against scheduled data."
    )
    parser.add_argument("--trip-instance-id", help="Explicit trip_instance_id to analyze.")
    parser.add_argument("--trip-id", help="GTFS trip_id to analyze (latest instance).")
    parser.add_argument(
        "--route-id",
        action="append",
        help="Restrict to these route_ids when picking a trip.",
    )
    parser.add_argument(
        "--route-short-name",
        action="append",
        help="Restrict to these route_short_names.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="realtime_analysis/queries/results/single_trip",
        help="Directory for generated artifacts.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)
    output_dir = ensure_output_dir(args.output_dir)

    settings = load_settings()
    with get_connection(settings) as conn:
        trip = select_trip(
            conn,
            trip_instance_id=args.trip_instance_id,
            trip_id=args.trip_id,
            route_ids=args.route_id or settings.target_route_ids,
            route_short_names=args.route_short_name or settings.target_route_short_names,
        )
        LOG.info(
            "Analyzing trip_instance_id=%s (trip_id=%s route_id=%s)",
            trip.trip_instance_id,
            trip.trip_id,
            trip.route_id,
        )

        actual_points = fetch_actual_points(conn, trip.trip_instance_id)
        actual_points["timestamp"] = pd.to_datetime(actual_points["timestamp"], utc=True)

        scheduled_geojson = fetch_scheduled_traj(conn, trip.trip_id)
        segment_df = fetch_segment_metrics(conn, trip.trip_instance_id, trip.trip_id)

    csv_path = output_dir / f"segment_metrics_{trip.trip_instance_id}.csv"
    segment_df.to_csv(csv_path, index=False)

    traj_path = plot_trajectory_map(actual_points, scheduled_geojson, output_dir, trip)
    speed_map_path = plot_speed_delta_map(segment_df, output_dir, trip)
    travel_bar_path = plot_travel_time_bars(segment_df, output_dir, trip)

    summary = summarize_segments(segment_df)
    if not summary["faster"].empty:
        LOG.info("Segments faster than schedule (>5 km/h):")
        LOG.info(summary["faster"][["segment_label", "speed_delta_kmh"]].to_string(index=False))
    if not summary["slower"].empty:
        LOG.info("Segments slower than schedule (<-5 km/h):")
        LOG.info(summary["slower"][["segment_label", "speed_delta_kmh"]].to_string(index=False))

    LOG.info("Artifacts written to %s", output_dir)
    LOG.info("Trajectory map: %s", traj_path)
    LOG.info("Speed delta map: %s", speed_map_path)
    LOG.info("Travel time bars: %s", travel_bar_path)
    LOG.info("Segment metrics CSV: %s", csv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



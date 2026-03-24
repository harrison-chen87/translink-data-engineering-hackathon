"""
Transit Congestion Map — Databricks App Backend

All serving from Lakebase (autoscale Postgres). No SQL warehouse dependency.
- Routes + stops: pre-synced from Delta by pipeline step 03
- Congestion: cache-through with Google Routes API on miss
- Background eviction: 24h TTL on congestion cache
"""

import asyncio
import json
import logging
import os
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transit-congestion-map")

# Vancouver timezone — all time-based logic uses Pacific time
try:
    from zoneinfo import ZoneInfo
    PACIFIC = ZoneInfo("America/Vancouver")
except ImportError:
    import pytz
    PACIFIC = pytz.timezone("America/Vancouver")


def pacific_now():
    """Get current time in Pacific timezone."""
    return datetime.now(timezone.utc).astimezone(PACIFIC)

LAKEBASE_PROJECT = os.environ.get("LAKEBASE_PROJECT", "transit-cache")
LAKEBASE_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT",
    "projects/transit-cache/branches/production/endpoints/primary")
LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST",
    "ep-round-tree-d2ped0gk.database.us-east-1.cloud.databricks.com")
SECRETS_SCOPE = os.environ.get("SECRETS_SCOPE", "transit_congestion_map")
MAX_API_SEGMENTS = 200

# --- Connections ---

_pg_conn = None
_w = None
_api_key = None
_translink_api_key = None
_sampled_waypoints = None
_live_vehicles = {"vehicles": [], "timestamp": None, "count": 0}
_route_names = {}  # route_id -> route_short_name lookup

# Bus speed tracking: vehicle_id -> {lat, lon, ts, route}
_prev_positions = {}
# Grid-based speed aggregation: (grid_lat, grid_lon) -> {speeds: [], level: str, route: str}
_bus_congestion = {}
# Route-painted congestion: list of {start, end, level, speed, route, source}
_route_painted_congestion = []
# Route coordinates cache: route_short_name -> [[lon,lat], ...]
_route_coords = {}
SPEED_SLOW_THRESHOLD = 15  # km/h — below this, bus is "slow"
SPEED_JAM_THRESHOLD = 8    # km/h — below this, bus is "jammed"
GRID_SIZE = 0.003          # ~300m grid cells

# Key arterial corridors not well covered by bus routes (bridges, highways)
ARTERIAL_CORRIDORS = [
    # Lions Gate Bridge
    {"origin_lat": 49.3155, "origin_lon": -123.1385, "dest_lat": 49.3284, "dest_lon": -123.1363, "name": "Lions Gate Bridge"},
    # Ironworkers Memorial Bridge
    {"origin_lat": 49.2935, "origin_lon": -123.0235, "dest_lat": 49.3055, "dest_lon": -123.0205, "name": "Ironworkers Memorial"},
    # Alex Fraser Bridge
    {"origin_lat": 49.1645, "origin_lon": -122.9440, "dest_lat": 49.1805, "dest_lon": -122.9365, "name": "Alex Fraser Bridge"},
    # Port Mann Bridge
    {"origin_lat": 49.2095, "origin_lon": -122.8165, "dest_lat": 49.2185, "dest_lon": -122.7945, "name": "Port Mann Bridge"},
    # Pattullo Bridge
    {"origin_lat": 49.2075, "origin_lon": -122.8885, "dest_lat": 49.2145, "dest_lon": -122.8895, "name": "Pattullo Bridge"},
    # Highway 1 through Burnaby
    {"origin_lat": 49.2640, "origin_lon": -123.0100, "dest_lat": 49.2580, "dest_lon": -122.9400, "name": "Hwy 1 Burnaby"},
    {"origin_lat": 49.2580, "origin_lon": -122.9400, "dest_lat": 49.2485, "dest_lon": -122.8600, "name": "Hwy 1 Coquitlam"},
    # Highway 99 Oak St Bridge / Arthur Laing
    {"origin_lat": 49.2105, "origin_lon": -123.1295, "dest_lat": 49.2275, "dest_lon": -123.1270, "name": "Oak Street Bridge"},
    {"origin_lat": 49.2015, "origin_lon": -123.1560, "dest_lat": 49.2115, "dest_lon": -123.1475, "name": "Arthur Laing Bridge"},
    # Massey Tunnel
    {"origin_lat": 49.1360, "origin_lon": -123.0715, "dest_lat": 49.1475, "dest_lon": -123.0665, "name": "Massey Tunnel"},
    # Knight Street Bridge
    {"origin_lat": 49.2055, "origin_lon": -123.0765, "dest_lat": 49.2175, "dest_lon": -123.0765, "name": "Knight St Bridge"},
    # Kingsway (no rapid transit)
    {"origin_lat": 49.2320, "origin_lon": -123.0465, "dest_lat": 49.2435, "dest_lon": -123.0160, "name": "Kingsway Burnaby"},
    # Marine Drive
    {"origin_lat": 49.2095, "origin_lon": -123.1605, "dest_lat": 49.2085, "dest_lon": -123.1095, "name": "Marine Drive"},
    # Lougheed Highway
    {"origin_lat": 49.2565, "origin_lon": -122.8925, "dest_lat": 49.2535, "dest_lon": -122.8165, "name": "Lougheed Hwy"},
    # Canada Way
    {"origin_lat": 49.2425, "origin_lon": -123.0050, "dest_lat": 49.2500, "dest_lon": -122.9475, "name": "Canada Way"},
]


def _get_workspace_client():
    global _w
    if _w is None:
        _w = WorkspaceClient()
    return _w


def get_pg_connection():
    """Get Lakebase Postgres connection with OAuth credential refresh."""
    global _pg_conn
    if _pg_conn is not None:
        try:
            cur = _pg_conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return _pg_conn
        except Exception:
            try:
                _pg_conn.close()
            except Exception:
                pass
            _pg_conn = None

    w = _get_workspace_client()
    username = w.current_user.me().user_name
    native_pw = os.environ.get("LAKEBASE_NATIVE_PASSWORD", "")

    if native_pw:
        # Native Postgres login (required for Lakebase Autoscale + Databricks Apps)
        _pg_conn = psycopg2.connect(
            host=LAKEBASE_HOST, port=5432,
            dbname="databricks_postgres",
            user=username,
            password=native_pw,
            sslmode="require", connect_timeout=15,
        )
    else:
        # OAuth credential (works for human users, not for app service principals on autoscale)
        cred = w.api_client.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": LAKEBASE_ENDPOINT},
        )
        _pg_conn = psycopg2.connect(
            host=LAKEBASE_HOST, port=5432,
            dbname="databricks_postgres",
            user=username,
            password=cred["token"],
            sslmode="require", connect_timeout=15,
        )
    _pg_conn.autocommit = False
    return _pg_conn


def get_api_key():
    global _api_key
    if _api_key is None:
        w = _get_workspace_client()
        _api_key = w.dbutils.secrets.get(scope=SECRETS_SCOPE, key="google_routes_api_key").strip()
    return _api_key


def get_translink_api_key():
    global _translink_api_key
    if _translink_api_key is None:
        w = _get_workspace_client()
        _translink_api_key = w.dbutils.secrets.get(scope=SECRETS_SCOPE, key="translink_api_key").strip()
    return _translink_api_key


def get_sampled_waypoints():
    """Load sampled waypoints from Lakebase."""
    global _sampled_waypoints
    if _sampled_waypoints is not None:
        return _sampled_waypoints

    import random
    random.seed(42)

    pg = get_pg_connection()
    cur = pg.cursor()
    cur.execute("SELECT origin_lat, origin_lon, dest_lat, dest_lon, route_short_name FROM route_segments")
    all_segments = [
        {"origin_lat": r[0], "origin_lon": r[1], "dest_lat": r[2], "dest_lon": r[3], "route_name": r[4]}
        for r in cur.fetchall()
    ]
    cur.close()

    # Stratified sample across routes
    by_route = {}
    for s in all_segments:
        by_route.setdefault(s["route_name"] or "unknown", []).append(s)
    sampled = []
    routes = sorted(by_route.keys(), key=lambda x: x or "")
    per_route = max(1, MAX_API_SEGMENTS // len(routes)) if routes else 1
    for route in routes:
        segs = by_route[route]
        step = max(1, len(segs) // min(per_route, len(segs)))
        sampled.extend(segs[i] for i in range(0, len(segs), step)[:per_route])

    _sampled_waypoints = sampled[:MAX_API_SEGMENTS]
    logger.info(f"Loaded {len(_sampled_waypoints)} sampled waypoints from Lakebase")
    return _sampled_waypoints


# --- Google Routes API ---

def decode_polyline(encoded):
    points = []
    index = lat = lon = 0
    while index < len(encoded):
        for is_lon in (False, True):
            shift = result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if (result & 1) else (result >> 1)
            if is_lon:
                lon += delta
            else:
                lat += delta
        points.append((lat / 1e5, lon / 1e5))
    return points


def call_routes_api(origin_lat, origin_lon, dest_lat, dest_lon, api_key):
    url = "https://routes.googleapis.com/directions/v2:computeRoutes"
    body = {
        "origin": {"location": {"latLng": {"latitude": origin_lat, "longitude": origin_lon}}},
        "destination": {"location": {"latLng": {"latitude": dest_lat, "longitude": dest_lon}}},
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE",
        "extraComputations": ["TRAFFIC_ON_POLYLINE"],
        "polylineQuality": "HIGH_QUALITY",
    }
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline,routes.travelAdvisory.speedReadingIntervals",
    }
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503):
                time.sleep(1.0 * (2 ** attempt))
                continue
            raise
        except (urllib.error.URLError, OSError):
            time.sleep(1.0 * (2 ** attempt))
    return None


def query_routes_api_batch(waypoints, api_key):
    """Call Google Routes API for multiple waypoints, return congestion segments."""
    segments = []

    def process(wp):
        data = call_routes_api(wp["origin_lat"], wp["origin_lon"], wp["dest_lat"], wp["dest_lon"], api_key)
        if not data:
            return []
        rows = []
        timestamp = datetime.now(timezone.utc).isoformat()
        for route in data.get("routes", []):
            duration = int(route.get("duration", "0s").rstrip("s"))
            distance = route.get("distanceMeters", 0)
            polyline = route.get("polyline", {}).get("encodedPolyline", "")
            intervals = route.get("travelAdvisory", {}).get("speedReadingIntervals", [])
            if not intervals:
                rows.append({
                    "start": [wp["origin_lon"], wp["origin_lat"]],
                    "end": [wp["dest_lon"], wp["dest_lat"]],
                    "level": "LOW", "speed": "UNKNOWN",
                    "duration": duration, "distance": distance, "samples": 1,
                })
                continue
            points = decode_polyline(polyline)
            jam = sum(1 for i in intervals if i.get("speed") == "TRAFFIC_JAM")
            slow = sum(1 for i in intervals if i.get("speed") == "SLOW")
            total = len(intervals)
            level = "HIGH" if total and jam / total > 0.3 else ("MEDIUM" if total and slow / total > 0.3 else "LOW")
            for idx, iv in enumerate(intervals):
                si = iv.get("startPolylinePointIndex", 0)
                ei = iv.get("endPolylinePointIndex", si + 1)
                sp = points[si] if si < len(points) else (0, 0)
                ep = points[ei] if ei < len(points) else sp
                rows.append({
                    "start": [sp[1], sp[0]], "end": [ep[1], ep[0]],
                    "level": level, "speed": iv.get("speed", "NORMAL"),
                    "duration": duration, "distance": distance, "samples": 1,
                })
        return rows

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process, wp) for wp in waypoints]
        for f in futures:
            try:
                segments.extend(f.result())
            except Exception as e:
                logger.warning(f"API call failed: {e}")
    return segments


# --- Lakebase cache operations ---

def check_cache(hour, day):
    try:
        pg = get_pg_connection()
        cur = pg.cursor()
        cur.execute(
            "UPDATE congestion_cache SET last_accessed_at = NOW() "
            "WHERE cache_key = %s RETURNING segments_json",
            (f"{hour}_{day}",)
        )
        row = cur.fetchone()
        pg.commit()
        cur.close()
        if row:
            logger.info(f"Cache HIT hour={hour} day={day}")
            data = row[0]
            return json.loads(data) if isinstance(data, str) else data
        logger.info(f"Cache MISS hour={hour} day={day}")
        return None
    except Exception as e:
        logger.warning(f"Cache read failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass
        return None


def write_cache(hour, day, segments):
    try:
        pg = get_pg_connection()
        cur = pg.cursor()
        cur.execute(
            "INSERT INTO congestion_cache (cache_key, hour_of_day, day_of_week, segments_json) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (cache_key) DO UPDATE SET "
            "segments_json = EXCLUDED.segments_json, last_accessed_at = NOW()",
            (f"{hour}_{day}", hour, day, json.dumps(segments))
        )
        pg.commit()
        cur.close()
        logger.info(f"Cache WRITE hour={hour} day={day} ({len(segments)} segments)")
    except Exception as e:
        logger.warning(f"Cache write failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


# --- Background eviction ---

async def evict_stale_cache():
    while True:
        await asyncio.sleep(3600)
        try:
            pg = get_pg_connection()
            cur = pg.cursor()
            cur.execute(
                "DELETE FROM congestion_cache "
                "WHERE last_accessed_at < NOW() - INTERVAL '24 hours'"
            )
            deleted = cur.rowcount
            pg.commit()
            cur.close()
            if deleted > 0:
                logger.info(f"Evicted {deleted} stale cache entries")
        except Exception as e:
            logger.warning(f"Eviction error: {e}")


# --- Live vehicle positions (TransLink GTFS-RT) ---

def _load_route_names():
    """Load route_id -> short_name mapping from Lakebase."""
    global _route_names
    if _route_names:
        return _route_names
    try:
        pg = get_pg_connection()
        cur = pg.cursor()
        cur.execute("SELECT DISTINCT route_id, route_short_name FROM stop_times WHERE route_short_name IS NOT NULL")
        _route_names = {str(r[0]): r[1] for r in cur.fetchall()}
        cur.close()
        logger.info(f"Loaded {len(_route_names)} route name mappings")
    except Exception as e:
        logger.warning(f"Route name load failed: {e}")
    return _route_names


def _haversine_km(lat1, lon1, lat2, lon2):
    """Approximate distance in km between two points."""
    import math
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _load_route_coords():
    """Load route coordinates from Lakebase for painting congestion along routes."""
    global _route_coords
    if _route_coords:
        return _route_coords
    try:
        pg = get_pg_connection()
        cur = pg.cursor()
        cur.execute("SELECT route_short_name, coordinates FROM routes WHERE route_short_name IS NOT NULL")
        for r in cur.fetchall():
            name = r[0]
            coords = r[1] if isinstance(r[1], list) else json.loads(r[1])
            if name not in _route_coords:
                _route_coords[name] = coords
            elif len(coords) > len(_route_coords[name]):
                _route_coords[name] = coords  # keep longest shape
        cur.close()
        logger.info(f"Loaded {len(_route_coords)} route coordinate sets")
    except Exception as e:
        logger.warning(f"Route coords load failed: {e}")
    return _route_coords


def _paint_route_congestion(route_speeds):
    """Paint congestion along full route geometries based on bus speeds per route.
    route_speeds: {route_name: [speed1, speed2, ...]}
    """
    global _route_painted_congestion
    route_coords = _load_route_coords()
    painted = []

    for route_name, speeds in route_speeds.items():
        if not speeds or route_name not in route_coords:
            continue
        coords = route_coords[route_name]
        if len(coords) < 2:
            continue

        avg_speed = sum(speeds) / len(speeds)
        if avg_speed < SPEED_JAM_THRESHOLD:
            level = "HIGH"
        elif avg_speed < SPEED_SLOW_THRESHOLD:
            level = "MEDIUM"
        else:
            level = "LOW"

        # Paint segments along the route polyline
        step = max(1, len(coords) // 30)  # ~30 segments per route
        for i in range(0, len(coords) - step, step):
            j = min(i + step, len(coords) - 1)
            painted.append({
                "start": coords[i],   # [lon, lat]
                "end": coords[j],     # [lon, lat]
                "level": level,
                "speed": f"{round(avg_speed, 1)} km/h",
                "source": "bus_route",
                "route": route_name,
                "samples": len(speeds),
            })

    _route_painted_congestion = painted
    return painted


def _compute_bus_speeds(vehicles):
    """Compare current positions to previous, compute speeds, update grid + route painting."""
    global _prev_positions, _bus_congestion
    now = time.time()
    new_grid = {}
    route_speeds = {}  # route_name -> [speed_kmh, ...]

    for v in vehicles:
        vid = v["id"]
        lat, lon = v["lat"], v["lon"]
        prev = _prev_positions.get(vid)

        speed_kmh = None
        if v.get("speed") and v["speed"] > 0:
            speed_kmh = v["speed"]
        elif prev:
            dt = now - prev["ts"]
            if 5 < dt < 120:  # between 5s and 2min
                dist = _haversine_km(prev["lat"], prev["lon"], lat, lon)
                if dist > 0.005:  # moved at least 5m
                    speed_kmh = round(dist / (dt / 3600), 1)

        _prev_positions[vid] = {"lat": lat, "lon": lon, "ts": now, "route": v.get("route", "")}

        if speed_kmh is not None and speed_kmh < 80:  # filter out GPS glitches
            route = v.get("route", "")
            # Grid cell
            grid_key = (round(lat / GRID_SIZE) * GRID_SIZE, round(lon / GRID_SIZE) * GRID_SIZE)
            if grid_key not in new_grid:
                new_grid[grid_key] = {"speeds": [], "routes": set()}
            new_grid[grid_key]["speeds"].append(speed_kmh)
            new_grid[grid_key]["routes"].add(route)
            v["computed_speed"] = speed_kmh

            # Accumulate per-route speeds for painting
            if route:
                route_speeds.setdefault(route, []).append(speed_kmh)

    # Build congestion from grid
    for key, data in new_grid.items():
        avg_speed = sum(data["speeds"]) / len(data["speeds"])
        if avg_speed < SPEED_JAM_THRESHOLD:
            level = "HIGH"
        elif avg_speed < SPEED_SLOW_THRESHOLD:
            level = "MEDIUM"
        else:
            level = "LOW"
        new_grid[key]["avg_speed"] = round(avg_speed, 1)
        new_grid[key]["level"] = level
        new_grid[key]["count"] = len(data["speeds"])
        new_grid[key]["routes"] = list(data["routes"])

    _bus_congestion = new_grid

    # Paint congestion along full route geometries
    _paint_route_congestion(route_speeds)

    slow_cells = sum(1 for d in new_grid.values() if d["level"] in ("HIGH", "MEDIUM"))
    logger.info(f"Bus speeds: {len(new_grid)} grid cells, {slow_cells} slow/jammed, {len(_route_painted_congestion)} route segments painted")


def poll_vehicle_positions():
    """Fetch live vehicle positions from TransLink GTFS-RT feed."""
    global _live_vehicles
    try:
        from google.transit import gtfs_realtime_pb2
        api_key = get_translink_api_key()
        url = f"https://gtfsapi.translink.ca/v3/gtfsposition?apikey={api_key}"
        req = urllib.request.Request(url, headers={"Accept": "application/x-protobuf"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(resp.read())

        route_names = _load_route_names()
        vehicles = []
        for entity in feed.entity:
            v = entity.vehicle
            if not v.position.latitude:
                continue
            route_id = v.trip.route_id or ""
            vehicles.append({
                "id": v.vehicle.id or entity.id,
                "lat": round(v.position.latitude, 6),
                "lon": round(v.position.longitude, 6),
                "route": route_names.get(str(route_id), route_id),
                "route_id": route_id,
                "trip": v.trip.trip_id or "",
                "bearing": round(v.position.bearing, 1),
                "speed": round(v.position.speed * 3.6, 1) if v.position.speed else None,
                "ts": v.timestamp,
            })

        # Compute bus speeds and grid congestion
        _compute_bus_speeds(vehicles)

        _live_vehicles = {
            "vehicles": vehicles,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": len(vehicles),
        }
        logger.info(f"Live vehicles updated: {len(vehicles)} positions")
    except Exception as e:
        logger.warning(f"Vehicle position poll failed: {e}")


def _get_slow_corridor_segments():
    """Find route_segments near grid cells where buses are slow. Returns targeted waypoints."""
    slow_cells = [(k, v) for k, v in _bus_congestion.items() if v["level"] in ("HIGH", "MEDIUM")]
    if not slow_cells:
        return []

    try:
        pg = get_pg_connection()
        cur = pg.cursor()
        # For each slow cell, find nearby route_segments
        targeted = []
        for (glat, glon), data in slow_cells[:30]:  # cap at 30 cells
            cur.execute(
                """SELECT origin_lat, origin_lon, dest_lat, dest_lon, route_short_name
                   FROM route_segments
                   WHERE ABS(origin_lat - %s) < %s AND ABS(origin_lon - %s) < %s
                   LIMIT 3""",
                (glat, GRID_SIZE * 2, glon, GRID_SIZE * 2)
            )
            for r in cur.fetchall():
                targeted.append({
                    "origin_lat": r[0], "origin_lon": r[1],
                    "dest_lat": r[2], "dest_lon": r[3],
                    "route_name": r[4],
                    "trigger": data["level"],
                    "bus_speed": data["avg_speed"],
                })
        cur.close()
        # Deduplicate by origin/dest
        seen = set()
        unique = []
        for t in targeted:
            key = (round(t["origin_lat"], 4), round(t["origin_lon"], 4),
                   round(t["dest_lat"], 4), round(t["dest_lon"], 4))
            if key not in seen:
                seen.add(key)
                unique.append(t)
        return unique[:50]  # max 50 targeted API calls
    except Exception as e:
        logger.warning(f"Slow corridor segment lookup failed: {e}")
        return []


def _targeted_google_query():
    """Query Google Routes API only for corridors where buses are slow."""
    slow_segments = _get_slow_corridor_segments()
    if not slow_segments:
        return

    logger.info(f"Targeted Google API: {len(slow_segments)} segments near slow buses")
    try:
        api_key = get_api_key()
    except Exception:
        return

    results = query_routes_api_batch(slow_segments, api_key)
    if not results:
        return

    # Store with 15-min granularity
    now = datetime.now(timezone.utc)
    quarter = (now.minute // 15) * 15
    cache_key = f"{now.hour}_{now.isoweekday() % 7 + 1}_q{quarter}"

    # Merge with existing cache entry (append, don't replace)
    existing = check_cache_by_key(cache_key)
    if existing:
        results = existing + results

    write_cache_by_key(cache_key, now.hour, now.isoweekday() % 7 + 1, results)
    logger.info(f"Targeted cache write: {cache_key} ({len(results)} segments)")


def check_cache_by_key(cache_key):
    """Check cache by exact key."""
    try:
        pg = get_pg_connection()
        cur = pg.cursor()
        cur.execute(
            "SELECT segments_json FROM congestion_cache WHERE cache_key = %s",
            (cache_key,)
        )
        row = cur.fetchone()
        cur.close()
        if row:
            data = row[0]
            return json.loads(data) if isinstance(data, str) else data
        return None
    except Exception:
        try:
            pg.rollback()
        except Exception:
            pass
        return None


def write_cache_by_key(cache_key, hour, day, segments):
    """Write cache with specific key."""
    try:
        pg = get_pg_connection()
        cur = pg.cursor()
        cur.execute(
            "INSERT INTO congestion_cache (cache_key, hour_of_day, day_of_week, segments_json) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (cache_key) DO UPDATE SET "
            "segments_json = EXCLUDED.segments_json, last_accessed_at = NOW()",
            (cache_key, hour, day, json.dumps(segments))
        )
        pg.commit()
        cur.close()
    except Exception as e:
        logger.warning(f"Cache write failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


_arterial_cache = {}  # cache_key -> {segments: [], ts: float}
ARTERIAL_CACHE_TTL = 900  # 15 minutes


def _query_arterials():
    """Query Google Routes API for key arterial corridors (bridges, highways)."""
    global _arterial_cache
    now = time.time()

    # Check if cache is fresh
    now_pac = pacific_now()
    cache_key = f"arterials_{now_pac.hour}_{now_pac.minute // 15}"
    if cache_key in _arterial_cache and (now - _arterial_cache[cache_key]["ts"]) < ARTERIAL_CACHE_TTL:
        return

    try:
        api_key = get_api_key()
    except Exception:
        return

    logger.info(f"Querying {len(ARTERIAL_CORRIDORS)} arterial corridors")
    segments = query_routes_api_batch(ARTERIAL_CORRIDORS, api_key)

    # Tag each segment with source and corridor name
    for i, seg in enumerate(segments):
        seg["source"] = "google_arterial"

    _arterial_cache[cache_key] = {"segments": segments, "ts": now}
    logger.info(f"Arterial query done: {len(segments)} segments")

    # Write to 15-min cache in Lakebase too
    now_dt = datetime.now(timezone.utc)
    quarter = (now_dt.minute // 15) * 15
    fine_key = f"arterial_{now_dt.hour}_{now_dt.isoweekday() % 7 + 1}_q{quarter}"
    write_cache_by_key(fine_key, now_dt.hour, now_dt.isoweekday() % 7 + 1, segments)


async def poll_vehicles_loop():
    """Background task: poll TransLink every 30 seconds, trigger targeted Google queries."""
    await asyncio.sleep(5)
    poll_count = 0
    while True:
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, poll_vehicle_positions)
            poll_count += 1

            # Every 3rd poll (~90s), check for slow corridors and query Google
            if poll_count % 3 == 0:
                await loop.run_in_executor(None, _targeted_google_query)

            # Every 30th poll (~15 min), query arterial corridors
            if poll_count % 30 == 0:
                await loop.run_in_executor(None, _query_arterials)
        except Exception as e:
            logger.warning(f"Vehicle poll loop error: {e}")
        await asyncio.sleep(30)


# --- Pre-warm cache for peak hours ---

async def prewarm_cache():
    """Background task: pre-fill cache for upcoming peak hours."""
    await asyncio.sleep(60)  # let app stabilize
    while True:
        try:
            now = pacific_now()
            day = now.isoweekday() % 7 + 1
            # Pre-warm next 2 hours if during peak periods (6-10 AM, 3-7 PM)
            upcoming = []
            for offset in range(1, 3):
                h = (now.hour + offset) % 24
                if 6 <= h <= 10 or 15 <= h <= 19:
                    cache_key = f"{h}_{day}"
                    if not check_cache_by_key(cache_key):
                        upcoming.append(h)

            if upcoming:
                logger.info(f"Pre-warming cache for hours: {upcoming}")
                api_key = get_api_key()
                waypoints = get_sampled_waypoints()
                for h in upcoming:
                    segments = query_routes_api_batch(waypoints, api_key)
                    if segments:
                        write_cache(h, day, segments)
                    await asyncio.sleep(5)  # small delay between batches
        except Exception as e:
            logger.warning(f"Pre-warm error: {e}")
        await asyncio.sleep(1800)  # check every 30 min


# --- FastAPI app ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    eviction_task = asyncio.create_task(evict_stale_cache())
    vehicle_task = asyncio.create_task(poll_vehicles_loop())
    prewarm_task = asyncio.create_task(prewarm_cache())
    yield
    eviction_task.cancel()
    vehicle_task.cancel()
    prewarm_task.cancel()
    global _pg_conn
    if _pg_conn:
        _pg_conn.close()


app = FastAPI(title="Transit Congestion Map", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html") as f:
        return f.read()


@app.get("/api/routes")
async def get_routes():
    """Get all transit route geometries from Lakebase."""
    pg = get_pg_connection()
    cur = pg.cursor()
    cur.execute(
        "SELECT shape_id, route_short_name, route_long_name, "
        "route_type, route_color, coordinates FROM routes"
    )
    routes = [
        {
            "shape_id": r[0],
            "route_name": r[1],
            "route_long_name": r[2],
            "route_type": r[3],
            "route_color": r[4] or "0000FF",
            "coordinates": r[5] if isinstance(r[5], list) else json.loads(r[5]),
        }
        for r in cur.fetchall()
    ]
    cur.close()
    return JSONResponse(
        content={"routes": routes},
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/api/stops")
async def get_stops(route: Optional[str] = Query(None)):
    """Get transit stops from Lakebase. When route is set, only stops served by that route."""
    pg = get_pg_connection()
    cur = pg.cursor()
    if route:
        cur.execute(
            "SELECT DISTINCT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon, s.location_type "
            "FROM stops s JOIN stop_times st ON s.stop_id = st.stop_id "
            "WHERE LOWER(st.route_short_name) = LOWER(%s)",
            (route,)
        )
    else:
        cur.execute("SELECT stop_id, stop_name, stop_lat, stop_lon, location_type FROM stops")
    stops = [
        {"stop_id": r[0], "stop_name": r[1], "lat": r[2], "lon": r[3], "location_type": r[4]}
        for r in cur.fetchall()
    ]
    cur.close()
    return JSONResponse(
        content={"stops": stops, "route": route},
        headers={"Cache-Control": f"public, max-age={'300' if route else '3600'}"},
    )


def _get_route_waypoints(route_filter):
    """Get all waypoints for a specific route (no sampling limit)."""
    pg = get_pg_connection()
    cur = pg.cursor()
    cur.execute(
        "SELECT origin_lat, origin_lon, dest_lat, dest_lon, route_short_name "
        "FROM route_segments WHERE LOWER(route_short_name) = LOWER(%s)",
        (route_filter,)
    )
    waypoints = [
        {"origin_lat": r[0], "origin_lon": r[1], "dest_lat": r[2], "dest_lon": r[3], "route_name": r[4]}
        for r in cur.fetchall()
    ]
    cur.close()
    logger.info(f"Route filter '{route_filter}': {len(waypoints)} waypoints")
    return waypoints


@app.get("/api/congestion")
async def get_congestion(
    hour: Optional[int] = Query(None, ge=0, le=23),
    day: Optional[int] = Query(None, ge=1, le=7),
    live: bool = Query(False),
    route: Optional[str] = Query(None),
):
    """Get congestion data. In live mode, combines bus speeds + cached Google data.
    In explorer mode, uses Lakebase cache → Google Routes API on miss.
    Optional route filter returns congestion only for that route's segments."""
    if hour is None:
        hour = pacific_now().hour
    if day is None:
        day = pacific_now().isoweekday() % 7 + 1

    segments = []
    sources = []

    # Route-filtered mode: query Google API for just this route's waypoints
    if route and not live:
        route_waypoints = _get_route_waypoints(route)
        if route_waypoints:
            # Check route-specific cache first
            route_cache_key = f"{hour}_{day}_r_{route.lower()}"
            cached = check_cache_by_key(route_cache_key)
            if cached:
                for s in cached:
                    s["source"] = "google_route"
                    s["routes"] = [route]
                segments = cached
                sources.append("google_route_cached")
            else:
                api_key = get_api_key()
                google_segments = query_routes_api_batch(route_waypoints, api_key)
                if google_segments:
                    for s in google_segments:
                        s["source"] = "google_route"
                        s["routes"] = [route]
                    write_cache_by_key(route_cache_key, hour, day, google_segments)
                    segments = google_segments
                sources.append("google_route_api")
        return {"segments": segments, "sources": sources, "count": len(segments), "route": route}

    # Live mode with route filter: only bus-speed congestion for this route
    if route and live:
        filtered = [s for s in _route_painted_congestion if s.get("route", "").lower() == route.lower()]
        if filtered:
            segments.extend(filtered)
            sources.append("bus_route")
        return {"segments": segments, "sources": sources, "count": len(segments), "route": route}

    # 1. Route-painted congestion from bus speeds (full corridor lines)
    if live and _route_painted_congestion:
        segments.extend(_route_painted_congestion)
        sources.append("bus_route")

    # 1b. Arterial corridor data (bridges, highways)
    if live:
        for cache_data in _arterial_cache.values():
            if time.time() - cache_data["ts"] < ARTERIAL_CACHE_TTL:
                segments.extend(cache_data["segments"])
                if "google_arterial" not in sources:
                    sources.append("google_arterial")

    # 2. Check 15-min cache first (more granular)
    now = datetime.now(timezone.utc)
    quarter = (now.minute // 15) * 15
    fine_key = f"{hour}_{day}_q{quarter}"
    fine_cached = check_cache_by_key(fine_key)
    if fine_cached:
        for s in fine_cached:
            s["source"] = "google_targeted"
        segments.extend(fine_cached)
        sources.append("google_targeted")

    # 3. Check hourly cache
    cached = check_cache(hour, day)
    if cached is not None:
        for s in cached:
            s.setdefault("source", "google")
        segments.extend(cached)
        sources.append("google_cached")
    elif not live:
        # Explorer mode cache miss — query Google Routes API
        logger.info(f"Calling Google Routes API for hour={hour} day={day}")
        waypoints = get_sampled_waypoints()
        api_key = get_api_key()
        google_segments = query_routes_api_batch(waypoints, api_key)
        if google_segments:
            write_cache(hour, day, google_segments)
            for s in google_segments:
                s["source"] = "google"
            segments.extend(google_segments)
        sources.append("google_api")

    return {"segments": segments, "sources": sources, "count": len(segments)}


@app.get("/api/bus-congestion")
async def bus_congestion():
    """Return current bus-speed-derived congestion grid."""
    cells = []
    for (glat, glon), data in _bus_congestion.items():
        cells.append({
            "lat": glat, "lon": glon,
            "avg_speed": data["avg_speed"],
            "level": data["level"],
            "count": data["count"],
            "routes": data.get("routes", []),
        })
    slow = sum(1 for c in cells if c["level"] in ("HIGH", "MEDIUM"))
    return {"cells": cells, "total": len(cells), "slow": slow}


@app.get("/api/top-corridors")
async def top_corridors(
    hour: Optional[int] = Query(None, ge=0, le=23),
    limit: int = Query(10, ge=1, le=20),
):
    """Top busiest corridors with frequency + route geometry for map overlay."""
    if hour is None:
        hour = pacific_now().hour

    pg = get_pg_connection()
    cur = pg.cursor()

    # Get top routes by trip count for this hour, with route geometry
    cur.execute(
        """
        WITH route_freq AS (
            SELECT
                COALESCE(st.route_short_name, 'SkyTrain') AS route_name,
                st.route_id,
                COUNT(DISTINCT st.trip_id) AS trips,
                COUNT(DISTINCT st.stop_id) AS stops_served,
                MIN(st.shape_id) AS sample_shape_id
            FROM stop_times st
            WHERE CAST(SUBSTRING(st.departure_time, 1, 2) AS INTEGER) = %s
                AND st.route_short_name IS NOT NULL
            GROUP BY COALESCE(st.route_short_name, 'SkyTrain'), st.route_id
            ORDER BY trips DESC
            LIMIT %s
        )
        SELECT
            rf.route_name,
            rf.route_id,
            rf.trips,
            rf.stops_served,
            r.route_long_name,
            r.route_color,
            r.coordinates
        FROM route_freq rf
        LEFT JOIN routes r ON CAST(rf.sample_shape_id AS TEXT) = CAST(r.shape_id AS TEXT)
        ORDER BY rf.trips DESC
        """,
        (hour, limit)
    )

    corridors = []
    for r in cur.fetchall():
        coords = r[6]
        if coords and isinstance(coords, str):
            coords = json.loads(coords)
        corridors.append({
            "route": r[0],
            "route_id": r[1],
            "trips_per_hour": r[2],
            "stops_served": r[3],
            "long_name": r[4] or "",
            "color": r[5] or "0066cc",
            "coordinates": coords,
        })

    # Also get hourly profile for each route (for sparkline)
    route_ids = [c["route_id"] for c in corridors]
    if route_ids:
        placeholders = ",".join(["%s"] * len(route_ids))
        cur.execute(
            f"""
            SELECT route_id,
                CAST(SUBSTRING(departure_time, 1, 2) AS INTEGER) AS hr,
                COUNT(DISTINCT trip_id) AS trips
            FROM stop_times
            WHERE route_id IN ({placeholders})
                AND CAST(SUBSTRING(departure_time, 1, 2) AS INTEGER) BETWEEN 5 AND 24
            GROUP BY route_id, hr
            ORDER BY route_id, hr
            """,
            route_ids
        )
        profiles = {}
        for r in cur.fetchall():
            profiles.setdefault(r[0], []).append({"hour": r[1], "trips": r[2]})
        for c in corridors:
            c["hourly_profile"] = profiles.get(c["route_id"], [])

    cur.close()
    return {"corridors": corridors, "hour": hour}


@app.get("/api/frequency")
async def get_frequency(
    hour: Optional[int] = Query(None, ge=0, le=23),
    route: Optional[str] = Query(None),
):
    """Get stop frequency data for heatmap. When route is set, only stops on that route."""
    if hour is None:
        hour = pacific_now().hour
    pg = get_pg_connection()
    cur = pg.cursor()
    if route:
        cur.execute(
            "SELECT sf.stop_id, sf.stop_name, sf.stop_lat, sf.stop_lon, sf.trip_count "
            "FROM stop_frequency sf "
            "WHERE sf.hour_of_day = %s AND sf.stop_id IN ("
            "  SELECT DISTINCT stop_id FROM stop_times WHERE LOWER(route_short_name) = LOWER(%s)"
            ")",
            (hour, route)
        )
    else:
        cur.execute(
            "SELECT stop_id, stop_name, stop_lat, stop_lon, trip_count "
            "FROM stop_frequency WHERE hour_of_day = %s",
            (hour,)
        )
    stops = [
        {"stop_id": r[0], "name": r[1], "lat": r[2], "lon": r[3], "trips": r[4]}
        for r in cur.fetchall()
    ]
    cur.close()
    return {"stops": stops, "hour": hour, "route": route}


@app.get("/api/nearest-stops")
async def nearest_stops(
    lat: float = Query(...),
    lon: float = Query(...),
    limit: int = Query(5, ge=1, le=20),
):
    """Find nearest transit stops to a given lat/lon using Haversine approx."""
    pg = get_pg_connection()
    cur = pg.cursor()
    # Approximate distance in meters using equirectangular projection
    cur.execute(
        """
        SELECT stop_id, stop_name, stop_lat, stop_lon, location_type,
            111320.0 * SQRT(
                POWER((stop_lat - %s), 2) +
                POWER((stop_lon - %s) * COS(RADIANS(%s)), 2)
            ) AS distance_m
        FROM stops
        ORDER BY distance_m
        LIMIT %s
        """,
        (lat, lon, lat, limit)
    )
    stops = [
        {"stop_id": r[0], "name": r[1], "lat": r[2], "lon": r[3],
         "location_type": r[4], "distance_m": round(r[5], 1)}
        for r in cur.fetchall()
    ]
    cur.close()
    return {"stops": stops}


@app.get("/api/trip-plan")
async def trip_plan(
    origin_lat: float = Query(...),
    origin_lon: float = Query(...),
    dest_lat: float = Query(...),
    dest_lon: float = Query(...),
    hour: Optional[int] = Query(None, ge=0, le=23),
):
    """Find direct transit routes between two locations, with driving comparison.

    Searches nearby stops at both origin and destination (within ~800m)
    and finds trips connecting any matching pair.
    """
    if hour is None:
        hour = pacific_now().hour

    pg = get_pg_connection()
    cur = pg.cursor()

    # Find direct routes by searching nearby stops at both ends
    # Uses equirectangular distance approximation (~800m radius)
    cur.execute(
        """
        WITH origin_stops AS (
            SELECT stop_id, stop_name, stop_lat, stop_lon,
                111320.0 * SQRT(
                    POWER((stop_lat - %s), 2) +
                    POWER((stop_lon - %s) * COS(RADIANS(%s)), 2)
                ) AS dist
            FROM stops
            WHERE ABS(stop_lat - %s) < 0.008 AND ABS(stop_lon - %s) < 0.012
        ),
        dest_stops AS (
            SELECT stop_id, stop_name, stop_lat, stop_lon,
                111320.0 * SQRT(
                    POWER((stop_lat - %s), 2) +
                    POWER((stop_lon - %s) * COS(RADIANS(%s)), 2)
                ) AS dist
            FROM stops
            WHERE ABS(stop_lat - %s) < 0.008 AND ABS(stop_lon - %s) < 0.012
        )
        SELECT DISTINCT ON (COALESCE(o.route_short_name, r.route_long_name, o.route_id), o.departure_time)
            COALESCE(o.route_short_name, r.route_long_name, o.route_id) AS route_label,
            o.route_id,
            o.trip_id,
            o.departure_time AS board_time,
            d.arrival_time AS alight_time,
            os.stop_name AS board_stop,
            ds.stop_name AS alight_stop,
            os.stop_lat AS board_lat, os.stop_lon AS board_lon,
            ds.stop_lat AS alight_lat, ds.stop_lon AS alight_lon
        FROM stop_times o
        JOIN stop_times d ON o.trip_id = d.trip_id
            AND d.stop_sequence > o.stop_sequence
        JOIN origin_stops os ON o.stop_id = os.stop_id AND os.dist < 800
        JOIN dest_stops ds ON d.stop_id = ds.stop_id AND ds.dist < 800
        LEFT JOIN routes r ON CAST(o.shape_id AS TEXT) = CAST(r.shape_id AS TEXT)
        WHERE CAST(SUBSTRING(o.departure_time, 1, 2) AS INTEGER) >= %s
            AND CAST(SUBSTRING(o.departure_time, 1, 2) AS INTEGER) <= %s + 2
        ORDER BY COALESCE(o.route_short_name, r.route_long_name, o.route_id), o.departure_time
        LIMIT 20
        """,
        (origin_lat, origin_lon, origin_lat, origin_lat, origin_lon,
         dest_lat, dest_lon, dest_lat, dest_lat, dest_lon,
         hour, hour)
    )

    def parse_time(t):
        parts = t.split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])

    # Deduplicate: keep earliest departure per route
    seen_routes = {}
    for r in cur.fetchall():
        route = r[0]
        duration_s = parse_time(r[4]) - parse_time(r[3])
        trip = {
            "route": route, "route_id": r[1], "trip_id": r[2],
            "board_time": r[3], "alight_time": r[4],
            "duration_min": round(max(0, duration_s) / 60, 1),
            "board_stop": r[5], "alight_stop": r[6],
            "board": [float(r[8]), float(r[7])],  # [lon, lat]
            "alight": [float(r[10]), float(r[9])],
        }
        if route not in seen_routes:
            seen_routes[route] = trip

    trips = sorted(seen_routes.values(), key=lambda t: t["board_time"])
    cur.close()

    # Get driving time from Google Routes API
    driving = None
    try:
        api_key = get_api_key()
        data = call_routes_api(origin_lat, origin_lon, dest_lat, dest_lon, api_key)
        if data and data.get("routes"):
            route = data["routes"][0]
            driving = {
                "duration_min": round(int(route.get("duration", "0s").rstrip("s")) / 60, 1),
                "distance_km": round(route.get("distanceMeters", 0) / 1000, 1),
            }
    except Exception as e:
        logger.warning(f"Driving time lookup failed: {e}")

    return {"trips": trips, "driving": driving}


@app.get("/api/cache/stats")
async def cache_stats():
    """Return cache and data statistics."""
    try:
        pg = get_pg_connection()
        cur = pg.cursor()
        stats = {}
        for table in ["routes", "stops", "route_segments", "congestion_cache",
                       "stop_frequency", "stop_times"]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cur.fetchone()[0]
            except Exception:
                pg.rollback()
                stats[table] = None
        cur.execute("SELECT MIN(last_accessed_at), MAX(last_accessed_at) FROM congestion_cache")
        row = cur.fetchone()
        stats["cache_oldest_access"] = str(row[0]) if row[0] else None
        stats["cache_newest_access"] = str(row[1]) if row[1] else None
        cur.close()
        stats["live_vehicles"] = _live_vehicles.get("count", 0)
        return stats
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/live-vehicles")
async def live_vehicles():
    """Return latest snapshot of live vehicle positions from TransLink GTFS-RT."""
    return _live_vehicles

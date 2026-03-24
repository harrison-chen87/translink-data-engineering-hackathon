# Transit Congestion Map — Project Spec

## Goal

Real-time transit congestion map for TransLink (Vancouver). Currently
operates in **Live mode** — real-time bus positions, bus-speed-derived
congestion, arterial traffic from Google, route/vehicle filtering,
auto-refreshing every 10s.

Explorer mode (historical congestion, trip planner) is disabled pending
further development.

## Architecture

```
                  +---------------------------+
                  |   Databricks App          |
                  |   (FastAPI + deck.gl)     |
                  +-----------+---------------+
                              |
          +-------------------+--------------------+
          |                   |                    |
    TransLink GTFS-RT    Lakebase (PG)       Google Routes API
    (every 30s)          (hot cache)          (targeted only)
          |                   |                    |
    +-----------+       +------------+       +------------+
    | Vehicle   |       | routes     |       | Slow bus   |
    | Positions |       | stops      |       | corridors  |
    | -> speeds |       | stop_times |       | (~50 calls)|
    | -> paint  |       | stop_freq  |       |            |
    | corridors |       | congestion |       | Arterials  |
    +-----------+       | cache(15m) |       | (~15 calls |
                        +------------+       |  every 15m)|
                                             +------------+

    GTFS Static Feed -> spark.read.csv() -> Delta Tables (gtfs_*)
                                         -> Lakebase sync (routes, stops)
```

### Congestion System (Hybrid)

Three data sources, combined in live mode:

1. **Bus-speed-derived (free, real-time)** — Computes vehicle speed from
   consecutive GTFS-RT positions. Paints congestion along full route
   geometries. Coverage: every transit corridor, updated every 30s.

2. **Arterial corridors (scheduled, low cost)** — 15 predefined corridors
   (bridges, highways, major roads without transit). Queried via Google Routes
   API every 15 min. Coverage: Lions Gate, Ironworkers, Alex Fraser, Port
   Mann, Pattullo, Knight St, Oak St, Arthur Laing bridges; Massey Tunnel;
   Hwy 1, Hwy 99, Kingsway, Marine Dr, Lougheed Hwy, Canada Way.

3. **Targeted deep-dive (event-driven)** — When bus speeds drop below 15 km/h,
   query Google Routes API for nearby road segments. ~50 calls per trigger.

**Cost:** ~$1-3/day vs ~$50+/day for blanket polling.

### Cache System

- **15-minute granularity** — cache keys like `8_2_q15` (hour 8, day 2, quarter 15)
- **Route-specific** — keys like `8_2_r_099` for filtered queries
- **24h TTL** — background eviction of stale entries
- **Pre-warm** — background task fills cache for upcoming peak hours

## Filtering

See [FILTER_SPEC.md](FILTER_SPEC.md) for the full specification. Summary:

### Route filter

Type a route number (e.g. `099`) to focus the entire map:

- **Congestion**: only segments along the filtered route (server-side)
- **Stops**: only stops served by the route (server-side JOIN on `stop_times`)
- **Frequency**: only frequency data for the route's stops (server-side)
- **Vehicles**: only vehicles on the filtered route (client-side)
- **Routes layer**: only the filtered route line rendered
- **Corridors**: hidden (not route-specific)

### Vehicle filter

Type a vehicle ID (e.g. `19045`) to see its route context:

- Resolves the vehicle's current route from the live feed
- Applies the same route filter to all server-side queries
- Only the single filtered vehicle is rendered (not route siblings)

### Zoom behavior

- Vehicle dots: visible at zoom >= 13 (or always when filtered)
- Route:ID labels: visible at zoom >= 14 (or always when filtered)
- Layers rebuild on zoom threshold crossings

## Data Sources

### GTFS Static Data
- **Source:** TransLink GTFS static feed (published weekly)
- **Ingestion:** Pipeline downloads zips to a Volume, loads with `spark.read.csv()`

### GTFS Realtime (TransLink Open API)
- **Vehicle Positions:** `gtfsapi.translink.ca/v3/gtfsposition?apikey={KEY}`
- **Format:** GTFS-RT Protocol Buffers
- **API Key:** Databricks Secrets, key: `translink_api_key`
- **Poll rate:** Every 30 seconds

### Google Routes API
- **API Key:** Databricks Secrets, key: `google_routes_api_key`
- **Usage:** Targeted queries only (slow corridors + arterials)

## Infrastructure

Configured at deploy time via `deploy.sh` — see [DEPLOY.md](DEPLOY.md).

- **Compute:** Serverless (environment version 1)
- **Catalog:** Customer-provided
- **Schema:** `transit_congestion_map`
- **Secrets scope:** Configurable via `SECRETS_SCOPE` env var
- **Lakebase:** `transit-cache` (autoscale Postgres)

### Map Layer

deck.gl for data rendering on MapLibre GL. Default basemap: CARTO Positron.
Swappable to Esri/ArcGIS, Mapbox, or self-hosted tiles by changing the style
URL. All data layers are basemap-independent.

### Lakebase Tables

| Table | Rows | Description |
|---|---|---|
| `routes` | 1,054 | Route geometries (coordinates as JSONB) |
| `stops` | 8,813 | Stop locations |
| `stop_times` | 1,833,726 | Denormalized schedules (trip + route info) |
| `stop_frequency` | 170,977 | Trips per stop per hour |
| `route_segments` | 21,444 | Waypoint pairs for API queries |
| `congestion_cache` | variable | Cached congestion (15-min + hourly + route keys) |

### Delta Tables

| Table | Description |
|---|---|
| `gtfs_agency`, `gtfs_calendar`, `gtfs_calendar_dates`, `gtfs_feed_info` | GTFS metadata |
| `gtfs_routes`, `gtfs_shapes`, `gtfs_stop_times`, `gtfs_stops`, `gtfs_trips`, `gtfs_transfers` | GTFS schedule data |
| `route_segments` | Waypoint pairs |

## Pipeline

### Step 01 — Ingest GTFS (`src/pipeline/01_ingest_gtfs.py`)
Downloads GTFS zip files, loads into Delta with `feed_date` snapshot tracking.

### Step 02 — Build Route Segments (`src/pipeline/02_build_route_segments.py`)
Creates origin/destination waypoint pairs from route shapes.

### Step 03 — Sync to Lakebase (`src/pipeline/03_sync_to_lakebase.py`)
Syncs routes, stops, route_segments from Delta to Lakebase.

Traffic data is collected automatically by the app at runtime via the hybrid
congestion system.

## Databricks App (`src/app/`)

FastAPI backend + deck.gl frontend.

### Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Map frontend (deck.gl + MapLibre) |
| `GET /api/routes` | Route geometries from Lakebase |
| `GET /api/stops?route=` | Stop locations (optional route filter) |
| `GET /api/congestion?hour=&day=&live=&route=` | Hybrid congestion (optional route filter) |
| `GET /api/live-vehicles` | Live GTFS-RT vehicle positions |
| `GET /api/frequency?hour=&route=` | Stop frequency heatmap (optional route filter) |
| `GET /api/top-corridors?hour=&limit=` | Top N busiest routes |
| `GET /api/nearest-stops?lat=&lon=` | Nearest stops to a coordinate |
| `GET /api/cache/stats` | Table row counts and cache health |

### Background Tasks

| Task | Interval | Purpose |
|---|---|---|
| Vehicle position poll | 30s | Fetch GTFS-RT, compute bus speeds, paint route congestion |
| Targeted Google query | 90s | Query Google Routes API where buses are slow |
| Arterial corridor query | 15 min | Query Google for bridges/highways |
| Cache pre-warm | 30 min | Fill cache for upcoming peak hours |
| Cache eviction | 1 hour | Delete entries not accessed in 24h |

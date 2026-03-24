# TransLink Transit Congestion Map

Real-time transit congestion map for Metro Vancouver, powered by Databricks.
Combines live TransLink bus speeds with targeted Google Traffic data to show
congestion across the transit network — without expensive blanket API polling.

## Features

- **Live bus tracking** — Real-time vehicle positions from TransLink GTFS-RT, updated every 30s
- **Bus-speed congestion** — Computes vehicle speeds from consecutive positions, paints congestion along transit corridors
- **Smart Google Traffic** — Only queries Google Routes API where buses detect slowdowns, plus 15 key arterials (bridges, highways)
- **Route filter** — Type a route number (e.g. `099`) to focus the entire map on that route: congestion, stops, frequency, vehicles
- **Vehicle filter** — Type a vehicle ID to see its route context: congestion, stops, and frequency for the route it's currently on
- **Zoom-aware** — Vehicle markers and labels appear only when zoomed in, keeping the map clean at overview zoom levels

## Architecture

| Layer | Technology |
|---|---|
| Frontend | deck.gl + MapLibre GL |
| Backend | FastAPI on Databricks Apps |
| Serving DB | Lakebase (managed Postgres) |
| Data lake | Delta tables on Unity Catalog |
| Live feed | TransLink GTFS-RT (protobuf) |
| Traffic data | Google Routes API (targeted) |
| Secrets | Databricks secret scope |

### Hybrid Congestion System

Instead of polling Google Routes API for 200+ segments (expensive), we use a three-tier approach:

1. **Bus speeds (free, real-time)** — Derive congestion from actual transit vehicle speeds. Covers every bus/train corridor.
2. **Arterial corridors (scheduled, ~15 calls/15 min)** — Google Traffic for bridges, highways, and roads without transit service.
3. **Targeted deep-dive (event-driven, ~50 calls when triggered)** — Google Traffic queried only where bus speeds drop below 15 km/h.

**Result:** ~$1-3/day instead of ~$50+/day, with better coverage.

## Deployment

See [DEPLOY.md](DEPLOY.md) for full deployment instructions. Quick start:

```bash
git clone https://github.com/databricks-field-eng/can-hunter.git
cd can-hunter/customer_prototypes/translink/transit-congestion-map
./deploy.sh
```

The script prompts for your workspace, catalog, and API keys, then creates all
infrastructure and deploys the app.

## Project Structure

```
src/
  pipeline/
    01_ingest_gtfs.py              # Download + load GTFS static data
    02_build_route_segments.py     # Build waypoint pairs from route shapes
    03_sync_to_lakebase.py         # Sync routes/stops/segments to Lakebase
  app/
    main.py                        # FastAPI backend (endpoints + background tasks)
    static/index.html              # deck.gl map frontend
    app.yaml                       # Databricks App config
    requirements.txt
DEPLOY.md                          # Customer deployment guide
FILTER_SPEC.md                     # Filter feature specification
SPEC.md                            # Project specification
deploy.sh                          # Automated deployment script
```

## Data

### Lakebase (Postgres)

| Table | Rows | Purpose |
|---|---|---|
| `routes` | 1,054 | Route geometries |
| `stops` | 8,813 | Stop locations |
| `stop_times` | 1,833,726 | Denormalized schedules |
| `stop_frequency` | 170,977 | Trips per stop per hour |
| `route_segments` | 21,444 | Waypoint pairs for API queries |
| `congestion_cache` | variable | Congestion cache (15-min + hourly keys) |

### Delta Tables

`<catalog>.transit_congestion_map`: GTFS tables (agency, calendar, routes,
shapes, stop_times, stops, trips, transfers), route_segments.

## API Keys

Both stored in Databricks Secrets. Never in code or repo.

| Key | Purpose |
|---|---|
| `google_routes_api_key` | Google Routes API (traffic data) |
| `translink_api_key` | TransLink GTFS-RT (live positions) |

## Prerequisites

- Databricks workspace with Unity Catalog and serverless compute
- Google Cloud API key with Routes API enabled
- TransLink Open API key (free at developer.translink.ca)

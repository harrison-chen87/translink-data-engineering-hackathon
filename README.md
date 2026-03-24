# TransLink Data Engineering Hackathon

A hands-on hackathon for TransLink's data engineering team to build migration-ready skills on Databricks. Participants will ingest real GTFS transit data, build incremental pipelines with schedule-vs-reality analysis, and deploy everything with DABs.

## Prerequisites

- **Databricks CLI** (v0.200+) installed and authenticated to your workspace
- **Python 3.8+** installed locally
- A Unity Catalog **catalog** you have CREATE SCHEMA permission on
- A **SQL warehouse** in your workspace (any size — used for schema/volume creation)
- A **Google Maps Platform API key** with the Routes API enabled (see setup below)
- `curl` and `unzip` installed locally (for GTFS download)

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/databricks-field-eng/can-hunter.git
cd can-hunter/customer_prototypes/translink/data_engineering_hackathon

# 2. Authenticate the Databricks CLI
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile hackathon

# 3. Deploy everything
./deploy.sh --profile hackathon --catalog YOUR_CATALOG --api-key YOUR_GOOGLE_API_KEY

# Or deploy without the API key (GTFS pipeline works without it):
./deploy.sh --profile hackathon --catalog YOUR_CATALOG
```

The deploy script will:
1. Download TransLink GTFS data (3 weekly snapshots from the public feed)
2. Find a SQL warehouse and create the UC schema and volume
3. Upload GTFS data + generate route corridors for API polling
4. Store the Google API key in a workspace secrets scope
5. Deploy the SDP pipeline and workflow via DABs
6. Run the pipeline (bronze → silver → gold)

### Deploy Options

| Flag | Description | Default |
|------|-------------|---------|
| `--profile` | Databricks CLI profile name | **required** |
| `--catalog` | Unity Catalog catalog name | **required** |
| `--schema` | Schema name | `traffic_hackathon_de` |
| `--volume` | Volume name | `traffic_data` |
| `--target` | DABs target | _(none)_ |
| `--api-key` | Google Routes API key | _(set later via secrets)_ |
| `--skip-download` | Skip GTFS data download | `false` |
| `--skip-upload` | Skip data upload to volume | `false` |
| `--skip-pipeline` | Skip pipeline run | `false` |

## Google Maps Platform API Setup

You need a Google Maps Platform API key to pull live traffic data from the Routes API.

### 1. Create a Google Cloud Account

1. Go to [Google Maps Platform — Get Started](https://developers.google.com/maps/get-started/)
2. Click **Get Started** and sign in with a Google account
3. Create a new Google Cloud project (e.g., `translink-hackathon`)
4. Add a billing account with a credit card

> **Cost:** New accounts get **$300 free trial credit (91 days)** plus **$200/month ongoing Maps credit**. The hackathon will use well under $10 worth of API calls.

### 2. Enable the Routes API

1. Go to the [API Library](https://console.cloud.google.com/apis/library) in the Cloud Console
2. Search for **Routes API**
3. Click **Enable**

### 3. Create an API Key

1. Go to [Credentials](https://console.cloud.google.com/apis/credentials)
2. Click **Create Credentials → API Key**
3. Copy the key — you'll need it during the hackathon

### 4. Restrict the API Key

For security, restrict the key so it can only be used for the Routes API:

1. Click on the API key you just created
2. Under **API restrictions**, select **Restrict key**
3. Select **Routes API** from the dropdown
4. Click **Save**

### Routes API Pricing

| Request Type | Cost per 1,000 requests |
|---|---|
| Compute Routes | ~$5 |
| Compute Route Matrix | ~$5-10 |

Polling ~25 route segments every 15 minutes for a full day ≈ 2,400 requests = well under $15.

## Data Model

```
GTFS Files (3 weekly snapshots from TransLink public feed)
  │
  ├──► bronze_gtfs_routes       (Streaming Table — Auto Loader CSV)
  ├──► bronze_gtfs_stops        (Streaming Table — Auto Loader CSV)
  ├──► bronze_gtfs_trips        (Streaming Table — Auto Loader CSV)
  ├──► bronze_gtfs_stop_times   (Streaming Table — Auto Loader CSV, 1.8M rows/snapshot)
  ├──► bronze_gtfs_shapes       (Streaming Table — Auto Loader CSV)
  └──► bronze_gtfs_calendar     (Streaming Table — Auto Loader CSV)
        │
        ▼
  silver_gtfs_routes            (Auto CDC — SCD Type 1, route changes across snapshots)
  silver_gtfs_stops             (Auto CDC — SCD Type 2, stop changes with history)
  silver_gtfs_scheduled_times   (MV — stop-to-stop scheduled travel times)
  silver_gtfs_route_shapes      (MV — route polylines for mapping)

Google Routes API ──► Python Data Source ──► Volume (JSON)
                                              │
                                              ▼
                                    bronze_traffic_api (Streaming Table — Auto Loader JSON)
                                              │
                                              ▼
                                    silver_traffic_readings (MV — congestion classification)
        │                                     │
        └──────────────┬──────────────────────┘
                       ▼
                 Gold (joins GTFS + API)
                   gold_schedule_vs_actual     (scheduled vs real travel time per route)
                   gold_route_delay_patterns   (which routes are late and when)
                   gold_corridor_daily_summary (daily corridor performance with route metadata)
                   gold_congestion_patterns    (hourly congestion by corridor)
                       │
                       ▼
                 Metric Views
                   traffic_congestion_metrics
                   route_performance_metrics
```

### GTFS Data (from TransLink public feed)

| Table | Rows/Snapshot | Description |
|-------|---------------|-------------|
| `routes` | 242 | All TransLink routes — bus, SkyTrain, SeaBus |
| `stops` | 8,936 | Bus/train stops with lat/lng coordinates |
| `trips` | 62,543 | Individual trip instances per route per service day |
| `stop_times` | 1,833,726 | Scheduled arrival/departure at every stop for every trip |
| `shapes` | 220,157 | GPS polylines for route mapping |
| `calendar` | ~20 | Service patterns (weekday/weekend, date ranges) |

Three weekly snapshots are ingested (2026-03-06, 03-13, 03-20) to demonstrate
SCD change tracking on real schedule data.

### Data Quality Expectations

| Layer | Rule | Action |
|-------|------|--------|
| Silver | Null route_id or stop_id | DROP ROW |
| Silver | Latitude outside Vancouver area (48-50) | WARN |
| Silver | Longitude outside Vancouver area (-124 to -122) | WARN |
| Silver | Scheduled segment time negative or > 2 hours | Filtered |
| Silver | Congestion ratio < 1.0 | WARN |

## Key Concepts Taught

### 1. SDP Interoperability — Mix SQL, Python, and All Table Types

This pipeline demonstrates streaming tables, materialized views, Auto CDC, and both SQL and Python — all in one pipeline with unified lineage.

| Notebook | Language | Table Type | Pattern |
|----------|----------|------------|---------|
| `bronze_gtfs_*.sql` (6 files) | SQL | Streaming Table | Auto Loader on CSV with snapshot metadata |
| `bronze_traffic_api.sql` | SQL | Streaming Table | Auto Loader on JSON |
| `silver_gtfs_routes.sql` | SQL | Streaming Table | Auto CDC (SCD Type 1) |
| `silver_gtfs_stops.sql` | SQL | Streaming Table | Auto CDC (SCD Type 2) |
| `silver_gtfs_scheduled_times.sql` | SQL | Materialized View | Window functions (LEAD) for segment times |
| `silver_gtfs_route_shapes.sql` | SQL | Materialized View | Spatial aggregation |
| `silver_traffic_readings.sql` | SQL | Materialized View | Deduplication + enrichment |
| `gold_schedule_vs_actual.sql` | SQL | Materialized View | Multi-source join (GTFS + API) |
| `gold_route_delay_patterns.sql` | SQL | Materialized View | Aggregation with percentiles |
| `gold_corridor_daily_summary.sql` | SQL | Materialized View | Daily summary with route metadata |
| `gold_congestion_patterns.sql` | SQL | Materialized View | Hourly patterns |

### 2. Python Data Sources (API Ingestion)

Custom PySpark DataSource (`src/python_data_sources/google_routes_source.py`) with batch and streaming support:

- Calls the Google Routes API with `TRAFFIC_AWARE` routing for GTFS-derived route corridors
- Returns structured rows with congestion metrics
- Retry with exponential backoff on transient errors
- Requires serverless compute (streaming mode requires `DataSourceStreamReader` support)

### 3. Auto CDC (Incremental CDC) on Real Schedule Data

Auto CDC with real GTFS snapshots — not synthetic updates:
- **SCD Type 1** (routes): route changes across weekly snapshots overwrite previous values
- **SCD Type 2** (stops): stop changes tracked with `__START_AT` / `__END_AT` history
- **SEQUENCE BY** `gtfs_snapshot_date`: ensures correct ordering across snapshots

### 4. Schedule vs. Reality Analysis (the headline use case)

The `gold_schedule_vs_actual` table joins GTFS scheduled travel times with live Google Routes API congestion data:
- "The 99 B-Line averages 6 minutes late at 5pm on weekdays"
- "Worst segment: Commercial-Broadway to Cambie"
- This is exactly what TransLink would build after migrating from Synapse

### 5. Data Quality Expectations

Available in both SQL and Python:
- **SQL:** `EXPECT (condition) ON VIOLATION DROP ROW / WARN`
- Visible in the SDP pipeline UI event log

### 6. Primary Keys & Foreign Keys

Unity Catalog informational constraints (`src/pipeline/constraints_and_tags.sql`):
- PK/FK constraints require regular Delta tables — not supported on MVs or streaming tables
- Constraints are included as commented-out reference for production use

### 7. Table Comments & Tags

- Table `COMMENT` for documentation
- `SET TAGS` for domain, source, and tier classification (with governed tag policy handling)
- Visible in Catalog Explorer

### 8. Metric Views

Governed business metrics:

```sql
-- Route delay analysis
SELECT "Route Name", "Avg Delay (seconds)", "% On Time"
FROM YOUR_CATALOG.traffic_hackathon_de.route_performance_metrics
GROUP BY ALL;

-- Congestion patterns
SELECT corridor_name, congestion_ratio
FROM YOUR_CATALOG.traffic_hackathon_de.traffic_congestion_metrics
ORDER BY congestion_ratio DESC;
```

### 9. DABs (Databricks Asset Bundles)

Full CI/CD deployment:
- Parameterized `databricks.yml` with variables for catalog, schema, volume
- Pipeline + workflow definitions in `resources/`
- `deploy.sh` for one-command deployment
- Dev/prod targets

### 10. Workflow Orchestration

Databricks Workflow with task dependencies:
- Task 1: Poll Routes API (notebook — polls GTFS-derived route corridors)
- Task 2: Run SDP pipeline (depends on Task 1)
- Task 3: Apply comments and tags (depends on Task 2)
- Task 4: Deploy metric views (depends on Task 2)

## Validation

After deployment, run these in the SQL Editor:

```sql
-- Check GTFS table row counts
SELECT 'silver_gtfs_routes' AS tbl, COUNT(*) AS cnt
  FROM YOUR_CATALOG.traffic_hackathon_de.silver_gtfs_routes
UNION ALL SELECT 'silver_gtfs_stops', COUNT(*)
  FROM YOUR_CATALOG.traffic_hackathon_de.silver_gtfs_stops
UNION ALL SELECT 'gold_schedule_vs_actual', COUNT(*)
  FROM YOUR_CATALOG.traffic_hackathon_de.gold_schedule_vs_actual
UNION ALL SELECT 'gold_route_delay_patterns', COUNT(*)
  FROM YOUR_CATALOG.traffic_hackathon_de.gold_route_delay_patterns;

-- Route delay analysis — the headline query
SELECT route_name, hour_of_day, day_type,
       avg_delay_seconds, pct_on_time, avg_congestion_ratio
FROM YOUR_CATALOG.traffic_hackathon_de.gold_route_delay_patterns
ORDER BY avg_delay_seconds DESC
LIMIT 10;

-- Check tags
SELECT * FROM information_schema.table_tags
WHERE schema_name = 'traffic_hackathon_de';
```

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Schema/volume not created` | SQL warehouse was stopped or starting | Start a warehouse in the Databricks UI, then re-run |
| `Error: no such directory: /Volumes/...` | Volume doesn't exist | Create manually: `CREATE VOLUME IF NOT EXISTS catalog.schema.volume;` |
| `Secret does not exist with scope` | API key not configured | Run `deploy.sh` with `--api-key YOUR_KEY` |
| `Failed to download GTFS` | Network issue or URL changed | Check TransLink's [developer resources](https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources) |
| `Tag value X is not allowed` | Governed tag policies | Tags are set with error handling — check warnings in output |
| `ImportError: DataSourceStreamReader` | Older runtime | Batch mode still works; streaming requires newer runtime |

## Project Structure

```
data_engineering_hackathon/
├── databricks.yml                              # DABs bundle config
├── deploy.sh                                   # One-command deploy (downloads GTFS + deploys)
├── README.md                                   # This file
├── SPEC_gtfs_integration.md                    # Integration spec
├── resources/
│   ├── traffic_pipeline.yml                    # SDP pipeline definition (16 notebooks)
│   └── traffic_workflow.yml                    # Workflow + scheduling
└── src/
    ├── data_gen/
    │   ├── corridors.json                      # Fallback corridor definitions (16 routes)
    │   └── generate_route_corridors.py         # Generates corridors from GTFS route data
    ├── python_data_sources/
    │   └── google_routes_source.py             # PySpark DataSource: batch + streaming, retry logic
    ├── notebooks/
    │   ├── poll_routes_api.py                  # Poll API: batch (JSON → Volume) or streaming (→ Delta)
    │   ├── travel_time_prediction.py           # Predict travel time by corridor and time of day
    │   ├── python_data_source_guide.py         # Deep dive: Python Data Source API
    │   └── participant_guide.py                # Hackathon participant guide with exercises
    ├── pipeline/
    │   ├── bronze_gtfs_routes.sql              # Auto Loader: GTFS routes (242 routes)
    │   ├── bronze_gtfs_stops.sql               # Auto Loader: GTFS stops (8,936 stops)
    │   ├── bronze_gtfs_trips.sql               # Auto Loader: GTFS trips (62K trips)
    │   ├── bronze_gtfs_stop_times.sql          # Auto Loader: GTFS stop times (1.8M rows)
    │   ├── bronze_gtfs_shapes.sql              # Auto Loader: GTFS shapes (220K points)
    │   ├── bronze_gtfs_calendar.sql            # Auto Loader: GTFS service calendar
    │   ├── bronze_traffic_api.sql              # Auto Loader: Google Routes API JSON
    │   ├── silver_gtfs_routes.sql              # Auto CDC SCD Type 1: route changes
    │   ├── silver_gtfs_stops.sql               # Auto CDC SCD Type 2: stop changes
    │   ├── silver_gtfs_scheduled_times.sql     # MV: stop-to-stop scheduled travel times
    │   ├── silver_gtfs_route_shapes.sql        # MV: route polylines for mapping
    │   ├── silver_traffic_readings.sql         # MV: cleaned API data + congestion severity
    │   ├── gold_schedule_vs_actual.sql         # MV: scheduled vs actual travel time
    │   ├── gold_route_delay_patterns.sql       # MV: which routes are late and when
    │   ├── gold_corridor_daily_summary.sql     # MV: daily corridor summary with route metadata
    │   ├── gold_congestion_patterns.sql        # MV: hourly congestion patterns
    │   └── constraints_and_tags.sql            # Table comments and tags
    └── metric_views/
        ├── traffic_congestion_metrics.sql      # Governed congestion metrics (reference SQL)
        ├── route_performance_metrics.sql       # Governed route delay metrics (reference SQL)
        └── deploy_metric_views.py              # Notebook to deploy metric views
```

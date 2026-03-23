# Data Engineering Hackathon — Implementation Plan

## Overview

Build a complete ingestion-to-silver pipeline that mirrors TransLink's real migration from Synapse to Databricks. Two source types:

1. **API Source** — Google Routes API (live traffic data for 15 Vancouver corridors)
2. **EAP Source** — Synthetic normalized Delta tables (facts + dimensions with cursor columns)

Pipeline uses SDP (Spark Declarative Pipelines) with Apply Changes, data quality expectations, and is deployed via DABs with scheduled orchestration.

---

## Corridor Definitions

15 Vancouver corridors with origin/destination coordinates for the Routes API:

| # | Corridor | Origin (lat, lng) | Destination (lat, lng) |
|---|----------|-------------------|----------------------|
| 1 | Broadway & Commercial → VCC-Clark | 49.2575, -123.0680 | 49.2660, -123.0789 |
| 2 | Broadway (Commercial → Arbutus) | 49.2575, -123.0680 | 49.2641, -123.1557 |
| 3 | Knight Street Bridge (Vancouver → Richmond) | 49.2180, -123.0775 | 49.1900, -123.0775 |
| 4 | Lions Gate Bridge (Downtown → North Van) | 49.2985, -123.1371 | 49.3199, -123.1363 |
| 5 | Ironworkers Memorial (Burnaby → North Van) | 49.2880, -123.0264 | 49.3072, -123.0264 |
| 6 | Granville Street Bridge (Downtown → Fairview) | 49.2790, -123.1330 | 49.2660, -123.1330 |
| 7 | Oak Street Bridge (Vancouver → Richmond) | 49.2100, -123.1261 | 49.1900, -123.1261 |
| 8 | Alex Fraser Bridge (Richmond → Surrey) | 49.1700, -122.9429 | 49.1500, -122.9429 |
| 9 | Kingsway (Vancouver → Burnaby) | 49.2620, -123.1010 | 49.2270, -123.0040 |
| 10 | Hastings St (Downtown → Burnaby) | 49.2827, -123.1100 | 49.2827, -123.0200 |
| 11 | Marine Drive East (Knight → Boundary) | 49.2090, -123.0775 | 49.2090, -123.0230 |
| 12 | Hwy 1 (Burnaby → Coquitlam) | 49.2650, -123.0100 | 49.2770, -122.8700 |
| 13 | 49th Ave (Fraser → Knight) | 49.2255, -123.0917 | 49.2255, -123.0775 |
| 14 | Cambie Bridge (Downtown → Cambie Village) | 49.2775, -123.1149 | 49.2665, -123.1149 |
| 15 | Pattullo Bridge (New West → Surrey) | 49.2110, -122.8912 | 49.2020, -122.8912 |

---

## Architecture

```
Google Routes API ──► Python Data Source ──► Bronze (Streaming Table)
                                                    │
                                                    ▼
                                              Silver (Materialized Views)
                                                - silver_traffic_readings
                                                - silver_corridor_stats

EAP Delta Tables ──► Auto Loader / Stream ──► Bronze (Streaming Table)
                                                    │
                                                    ▼
                                              Silver (Apply Changes Into)
                                                - silver_dim_corridors (SCD Type 1)
                                                - silver_fact_traffic_counts (MERGE with cursor)
```

---

## Pipeline Components

### 1. Python Data Source: `GoogleRoutesDataSource`

**File:** `src/python_data_sources/google_routes_source.py`

A custom PySpark DataSource (requires DBR 15.4+) that:
- Reads corridor definitions from a config table or JSON
- Calls `POST https://routes.googleapis.com/directions/v2:computeRoutes` for each corridor
- Uses `TRAFFIC_AWARE` routing preference
- Returns schema:

| Column | Type | Description |
|--------|------|-------------|
| corridor_id | STRING | e.g., `corridor_01` |
| corridor_name | STRING | e.g., `Lions Gate Bridge` |
| origin_lat | DOUBLE | Origin latitude |
| origin_lng | DOUBLE | Origin longitude |
| dest_lat | DOUBLE | Destination latitude |
| dest_lng | DOUBLE | Destination longitude |
| duration_seconds | INT | Travel time with traffic |
| static_duration_seconds | INT | Travel time without traffic |
| distance_meters | INT | Route distance |
| congestion_ratio | DOUBLE | duration / static_duration |
| api_response_json | STRING | Full API response (for debugging) |
| polled_at | TIMESTAMP | When the API was called |

**Batch mode** for the hackathon (stream mode is a stretch goal). Each `spark.read.format("google_routes").load()` call polls all 15 corridors once.

### 2. Synthetic EAP Tables

**File:** `src/data_gen/generate_eap_data.py`

Generate synthetic Delta tables that mimic EAP's normalized structure:

| Table | Description | Cursor Column |
|-------|-------------|---------------|
| `eap_fact_traffic_counts` | Hourly traffic counts by corridor + direction | `last_modified_ts` |
| `eap_dim_corridors` | Corridor metadata (name, type, zone, speed limit) | `last_modified_ts` |
| `eap_dim_sensors` | Sensor/camera locations on each corridor | `last_modified_ts` |

Seed with:
- ~500K fact rows (90 days × 24 hours × 15 corridors × ~15 sensors)
- Late-arriving records (5% with older timestamps but new data)
- Updates to dimension records (corridor name changes, sensor relocations)
- Data quality issues: null sensor readings (~2%), duplicate records (~0.5%), negative counts (~0.3%)

### 3. Bronze Layer (SDP Streaming Tables)

**File:** `src/pipeline/bronze_traffic_api.sql`

```sql
CREATE OR REFRESH STREAMING TABLE bronze_traffic_api
COMMENT 'Raw traffic readings from Google Routes API'
AS SELECT * FROM STREAM read_files(
  '${catalog}.${schema}.${volume}/traffic_api/',
  format => 'json',
  schema => '...'
);
```

**File:** `src/pipeline/bronze_eap_fact_traffic.sql`

```sql
CREATE OR REFRESH STREAMING TABLE bronze_eap_fact_traffic
COMMENT 'Raw EAP fact traffic counts ingested via Auto Loader'
AS SELECT * FROM STREAM read_files(
  '${catalog}.${schema}.${volume}/eap/fact_traffic_counts/',
  format => 'delta'
);
```

**File:** `src/pipeline/bronze_eap_dim_corridors.sql` (and dim_sensors)

Same pattern for each EAP dimension table.

**Alternative approach for API data:** Instead of landing JSON files in a Volume then using Auto Loader, the Python Data Source could be called from a notebook task in a Workflow that writes directly to a Delta table. The bronze streaming table then reads from that Delta table. This is more realistic for API-based ingestion where there's no file drop.

### 4. Silver Layer

#### silver_traffic_readings (Materialized View — API data)

**File:** `src/pipeline/silver_traffic_readings.sql`

Cleans and enriches the API traffic data:
- Parse duration strings to integers
- Calculate `congestion_ratio` = duration_seconds / static_duration_seconds
- Classify congestion: `free_flow` (<1.1), `light` (1.1-1.3), `moderate` (1.3-1.6), `heavy` (1.6-2.0), `severe` (>2.0)
- Add `hour_of_day`, `day_of_week`, `is_rush_hour` columns
- Deduplicate by corridor_id + polled_at

**Data Quality Expectations:**
```sql
CONSTRAINT valid_duration EXPECT (duration_seconds > 0) ON VIOLATION WARN
CONSTRAINT valid_distance EXPECT (distance_meters > 0) ON VIOLATION WARN
CONSTRAINT valid_congestion EXPECT (congestion_ratio >= 1.0) ON VIOLATION WARN
CONSTRAINT polled_at_not_null EXPECT (polled_at IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT corridor_not_null EXPECT (corridor_id IS NOT NULL) ON VIOLATION DROP ROW
```

#### silver_fact_traffic_counts (Apply Changes — EAP data)

**File:** `src/pipeline/silver_fact_traffic_counts.sql`

Uses `APPLY CHANGES INTO` (AUTO CDC) with cursor column for incremental merge:

```sql
CREATE OR REFRESH STREAMING TABLE silver_fact_traffic_counts;

CREATE FLOW apply_fact_traffic
AS APPLY CHANGES INTO silver_fact_traffic_counts
FROM STREAM(bronze_eap_fact_traffic)
KEYS (corridor_id, sensor_id, count_hour)
SEQUENCE BY last_modified_ts;
```

**Data Quality Expectations:**
```sql
CONSTRAINT count_positive EXPECT (vehicle_count >= 0) ON VIOLATION WARN
CONSTRAINT sensor_not_null EXPECT (sensor_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT timestamp_valid EXPECT (count_hour <= current_timestamp()) ON VIOLATION WARN
```

#### silver_dim_corridors (Apply Changes SCD Type 1 — EAP data)

**File:** `src/pipeline/silver_dim_corridors.sql`

```sql
CREATE OR REFRESH STREAMING TABLE silver_dim_corridors;

CREATE FLOW apply_dim_corridors
AS APPLY CHANGES INTO silver_dim_corridors
FROM STREAM(bronze_eap_dim_corridors)
KEYS (corridor_id)
SEQUENCE BY last_modified_ts
STORED AS SCD TYPE 1;
```

#### silver_dim_sensors (Apply Changes SCD Type 2 — EAP data)

**File:** `src/pipeline/silver_dim_sensors.sql`

SCD Type 2 for sensors — track sensor relocations over time:

```sql
CREATE OR REFRESH STREAMING TABLE silver_dim_sensors;

CREATE FLOW apply_dim_sensors
AS APPLY CHANGES INTO silver_dim_sensors
FROM STREAM(bronze_eap_dim_sensors)
KEYS (sensor_id)
SEQUENCE BY last_modified_ts
STORED AS SCD TYPE 2;
```

---

## Data Quality Strategy

| Layer | Approach | On Violation |
|-------|----------|-------------|
| Bronze | No expectations — raw data preserved as-is | N/A |
| Silver (critical) | `ON VIOLATION DROP ROW` — null keys, null timestamps | Row removed |
| Silver (advisory) | `ON VIOLATION WARN` — negative counts, future timestamps, unusual congestion | Row kept, logged |

Quality metrics visible in the SDP pipeline UI and queryable via `event_log`.

---

## Scheduling & Orchestration

**File:** `resources/traffic_workflow.yml`

A Databricks Workflow with two task groups:

```
┌─────────────────────────────────────┐
│  Task 1: Poll Routes API            │
│  (Python notebook, runs every 15m)  │
│  - Calls Google Routes API          │
│  - Writes JSON to Volume            │
├─────────────────────────────────────┤
│  Task 2: Run SDP Pipeline           │
│  (Triggered after Task 1)           │
│  - Bronze ingests new API data      │
│  - Bronze ingests EAP changes       │
│  - Silver: MV refresh + CDC flows   │
├─────────────────────────────────────┤
│  Schedule: Every 15 minutes         │
│  (or cron for hackathon demo)       │
└─────────────────────────────────────┘
```

For the hackathon: manual trigger or short cron (every 15 minutes during the workshop). In production this would be continuous with the streaming tables and a scheduled trigger for the API polling.

---

## DABs Bundle Structure

```
data_engineering_hackathon/
├── databricks.yml                         # Bundle config (parameterized)
├── deploy.sh                              # One-command deploy
├── README.md                              # Workshop guide + API setup
├── PLAN.md                                # This file
├── resources/
│   ├── traffic_pipeline.yml               # SDP pipeline definition
│   └── traffic_workflow.yml               # Workflow with scheduling
└── src/
    ├── data_gen/
    │   └── generate_eap_data.py           # Synthetic EAP tables + seeded issues
    ├── python_data_sources/
    │   └── google_routes_source.py        # Custom PySpark DataSource
    ├── notebooks/
    │   └── poll_routes_api.py             # Notebook that calls the DataSource + writes to Volume
    └── pipeline/
        ├── bronze_traffic_api.sql         # Auto Loader for API JSON
        ├── bronze_eap_fact_traffic.sql    # Auto Loader for EAP facts
        ├── bronze_eap_dim_corridors.sql   # Auto Loader for EAP dims
        ├── bronze_eap_dim_sensors.sql     # Auto Loader for EAP dims
        ├── silver_traffic_readings.sql    # MV: cleaned API data + congestion classification
        ├── silver_fact_traffic_counts.sql # Apply Changes: EAP facts with cursor
        ├── silver_dim_corridors.sql       # Apply Changes SCD1: corridor metadata
        └── silver_dim_sensors.sql         # Apply Changes SCD2: sensor locations
```

---

## Build Order

1. **Synthetic data generator** — generate EAP Delta tables with cursor columns and seeded issues
2. **Corridor config** — JSON with the 15 corridors and coordinates
3. **Python Data Source** — `GoogleRoutesDataSource` that hits the Routes API
4. **Poll notebook** — calls the DataSource, writes JSON to Volume
5. **Bronze tables** — Auto Loader streaming tables (API + EAP)
6. **Silver tables** — materialized view (API) + Apply Changes (EAP facts + dims)
7. **Data quality expectations** — add to all silver tables
8. **Workflow** — orchestrate polling + pipeline refresh
9. **DABs bundle** — `databricks.yml` + deploy script
10. **README** — workshop instructions, challenges, validation queries

---

## Key Teaching Moments

| Their Synapse Pattern | What They'll Build |
|---|---|
| Python + requests + manual watermark | Python Data Source + Auto Loader |
| Self-managed cursor tracking on facts | `APPLY CHANGES INTO` with `SEQUENCE BY` cursor column |
| Manual dimension updates | `APPLY CHANGES INTO` SCD Type 1 / Type 2 |
| No built-in data quality | `EXPECT` constraints with WARN / DROP ROW |
| Synapse if-then-else pipelines | Workflows with task dependencies + conditions |
| Manual deployment | `databricks bundle deploy --target dev` |

---

## Open Questions

- **API key management:** Use Databricks Secrets to store the Google API key? Or pass as a widget parameter for the hackathon?
- **Volume vs direct write:** Should the API poller write JSON files to a Volume (Auto Loader pattern) or write directly to a Delta table (more realistic for API sources)?
- **Gold layer:** Out of scope per request (silver only), but we could add a simple gold view as a stretch goal (e.g., daily corridor congestion summary).

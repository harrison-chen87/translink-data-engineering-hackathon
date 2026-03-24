# Transit Congestion Map — Deployment Guide

Deploy the Transit Congestion Map on your own Databricks workspace. This guide
assumes you are running commands from a laptop with the Databricks CLI installed.

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Databricks workspace** | Any cloud (AWS/Azure/GCP). Must have Unity Catalog enabled and serverless compute available. |
| **Databricks CLI** | v0.230+ installed and authenticated (`databricks auth login`). |
| **Google Cloud project** | With the **Routes API** enabled (not Directions API). |
| **Google Routes API key** | From Google Cloud Console → APIs & Services → Credentials. |
| **TransLink API key** | Register at [TransLink Open API](https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/rtti). |

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/databricks-field-eng/can-hunter.git
cd can-hunter/customer_prototypes/translink/transit-congestion-map

# 2. Run the deployment script — it will prompt for your configuration
./deploy.sh
```

The script creates all infrastructure (catalog schema, volume, secrets, Lakebase
database), uploads the pipeline notebooks, runs them in sequence, and deploys the
Databricks App.

---

## What the script does

### Step 1 — Collect configuration

You will be prompted for:

| Prompt | Example | Notes |
|---|---|---|
| Databricks CLI profile | `DEFAULT` | Profile name from `~/.databrickscfg` or `DEFAULT` to use env vars. |
| Unity Catalog name | `my_catalog` | Must already exist. You need `USE CATALOG` + `CREATE SCHEMA` privileges. |
| Google Routes API key | `AIza...` | Stored in Databricks Secrets — never written to disk. |
| TransLink API key | `abc123` | Stored in Databricks Secrets. |

### Step 2 — Create infrastructure

The script creates:

- **Schema**: `<catalog>.transit_congestion_map`
- **Volume**: `<catalog>.transit_congestion_map.gtfs_data`
- **Secrets scope**: `transit_congestion_map` with keys `google_routes_api_key` and `translink_api_key`
- **Lakebase database**: `transit-cache` (autoscale Postgres for sub-100ms serving)

### Step 3 — Upload and run pipeline

Three notebooks are uploaded to `/Workspace/Users/<you>/transit-congestion-map/pipeline/`
and executed in order on serverless compute:

1. **01_ingest_gtfs** — Downloads TransLink GTFS static data, loads into Delta tables
2. **02_build_route_segments** — Builds waypoint pairs from route shapes
3. **03_sync_to_lakebase** — Syncs Delta tables to Lakebase Postgres

Traffic data is collected automatically by the app once running (bus speed tracking
+ targeted Google Routes API calls on congestion events).

### Step 4 — Deploy the app

The app is deployed as a Databricks App with:
- FastAPI backend (real-time congestion from bus speeds + Google Routes API)
- deck.gl / MapLibre frontend
- Lakebase Postgres for sub-100ms queries
- Background tasks for live vehicle tracking and cache management

---

## Manual deployment

If you prefer to run steps individually instead of using `deploy.sh`:

### 1. Create schema and volume

```bash
PROFILE="your-profile"
CATALOG="your_catalog"

databricks sql execute --profile $PROFILE \
  --statement "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.transit_congestion_map"

databricks sql execute --profile $PROFILE \
  --statement "CREATE VOLUME IF NOT EXISTS ${CATALOG}.transit_congestion_map.gtfs_data"
```

### 2. Create secrets

```bash
databricks secrets create-scope transit_congestion_map --profile $PROFILE

databricks secrets put-secret transit_congestion_map google_routes_api_key \
  --string-value "YOUR_GOOGLE_API_KEY" --profile $PROFILE

databricks secrets put-secret transit_congestion_map translink_api_key \
  --string-value "YOUR_TRANSLINK_API_KEY" --profile $PROFILE
```

### 3. Create Lakebase database

```bash
databricks lakebase databases create transit-cache --profile $PROFILE
```

Wait for the database to become `ACTIVE` (usually 1-2 minutes):

```bash
databricks lakebase databases get transit-cache --profile $PROFILE
```

### 4. Upload and run notebooks

```bash
# Upload pipeline notebooks
databricks workspace mkdirs /Workspace/Users/$(databricks current-user me --profile $PROFILE | jq -r '.userName')/transit-congestion-map/pipeline --profile $PROFILE

for nb in 01_ingest_gtfs 02_build_route_segments 03_sync_to_lakebase; do
  databricks workspace import \
    src/pipeline/${nb}.py \
    /Workspace/Users/$(databricks current-user me --profile $PROFILE | jq -r '.userName')/transit-congestion-map/pipeline/${nb} \
    --format SOURCE --language PYTHON --overwrite \
    --profile $PROFILE
done

# Run each notebook (serverless compute)
for nb in 01_ingest_gtfs 02_build_route_segments 03_sync_to_lakebase; do
  echo "Running ${nb}..."
  databricks jobs submit --json '{
    "run_name": "'"${nb}"'",
    "tasks": [{
      "task_key": "'"${nb}"'",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/'"$(databricks current-user me --profile $PROFILE | jq -r '.userName')"'/transit-congestion-map/pipeline/'"${nb}"'"
      },
      "environment_key": "default"
    }],
    "environments": [{
      "environment_key": "default",
      "spec": { "client": "1" }
    }]
  }' --profile $PROFILE --wait
done
```

### 5. Deploy the app

```bash
databricks apps deploy transit-congestion-map \
  --source-code-path /Workspace/Users/$(databricks current-user me --profile $PROFILE | jq -r '.userName')/transit-congestion-map/app \
  --profile $PROFILE
```

---

## Configuration reference

### Environment variables (app.yaml)

| Variable | Description |
|---|---|
| `LAKEBASE_PROJECT` | Lakebase database name (default: `transit-cache`) |
| `LAKEBASE_ENDPOINT` | Lakebase endpoint path — auto-populated by deploy script |
| `LAKEBASE_HOST` | Lakebase hostname — auto-populated by deploy script |

### Secrets scope: `transit_congestion_map`

| Key | Description |
|---|---|
| `google_routes_api_key` | Google Routes API key for traffic data |
| `translink_api_key` | TransLink Open API key for GTFS-RT vehicle positions |

### Estimated API costs

| Source | Calls/day | Cost/day |
|---|---|---|
| Bus speed tracking (GTFS-RT) | ~2,880 (free) | $0 |
| Arterial corridors (Google) | ~60 | ~$0.30 |
| Targeted deep-dives (Google) | ~200 | ~$1.00 |
| **Total** | | **~$1-3/day** |

---

## Troubleshooting

| Issue | Fix |
|---|---|
| `Secret scope not found` | Run `databricks secrets list-scopes` to verify the scope was created |
| Lakebase connection timeout | Check that the Lakebase database is `ACTIVE`: `databricks lakebase databases get transit-cache` |
| Google API 403 | Ensure the **Routes API** (not Directions API) is enabled in your GCP project |
| App deploy fails | Verify your workspace supports Databricks Apps (serverless-enabled workspace) |
| No live vehicles | The TransLink GTFS-RT feed only returns data during service hours (approx 5 AM - 1 AM Pacific) |
| Pipeline notebook fails on `dbutils.secrets` | Ensure the secrets scope name matches `transit_congestion_map` in the notebook |

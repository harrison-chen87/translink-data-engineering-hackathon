# TransLink Data Engineering Hackathon

A hands-on hackathon for TransLink's data engineering team to build migration-ready skills on Databricks. Participants will ingest live traffic data from the Google Routes API, build incremental pipelines against EAP-style Delta tables, orchestrate workflows, and deploy everything with DABs.

## Prerequisites

- **Databricks CLI** installed and authenticated to your workspace
- **Python 3.8+** installed locally
- A Unity Catalog **catalog** you have CREATE SCHEMA permission on
- A **Google Maps Platform API key** with the Routes API enabled (see setup below)

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/databricks-field-eng/can-hunter.git
cd can-hunter/customer_prototypes/translink/data_engineering_hackathon

# 2. Authenticate the Databricks CLI
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile hackathon

# 3. Deploy everything
./deploy.sh --profile hackathon --catalog YOUR_CATALOG --api-key YOUR_GOOGLE_API_KEY

# Or deploy without the API key (add it later):
./deploy.sh --profile hackathon --catalog YOUR_CATALOG
```

The deploy script will:
1. Generate ~500K rows of synthetic EAP data locally (90 days of traffic counts)
2. Create the UC schema and volume
3. Upload synthetic data + corridor config to the volume
4. Store the Google API key in Databricks Secrets
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
| `--days` | Days of synthetic history | `90` |
| `--skip-data-gen` | Skip synthetic data generation | `false` |
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

Polling 15 corridors every 15 minutes for a full day = ~1,400 requests = well under $10.

Full pricing details: [Routes API Usage and Billing](https://developers.google.com/maps/documentation/routes/usage-and-billing)

## Data Model

```
Google Routes API ──► Python Data Source ──► Volume (JSON)
                                                │
                                                ▼
                                          Bronze (Streaming Tables)
                                            ├── bronze_traffic_api
                                            ├── bronze_eap_fact_traffic
                                            ├── bronze_eap_dim_corridors
                                            └── bronze_eap_dim_sensors
                                                │
                                                ▼
                                          Silver (Auto CDC + Materialized Views)
                                            ├── silver_traffic_readings    (MV — API data, congestion classification)
                                            ├── silver_fact_traffic_counts  (Auto CDC — cursor-based CDC)
                                            ├── silver_dim_corridors        (Auto CDC — SCD Type 1)
                                            └── silver_dim_sensors          (Auto CDC — SCD Type 2)
                                                │
                                                ▼
                                          Gold (Materialized Views — SQL + Python mixed)
                                            ├── gold_corridor_daily_summary  (SQL — API + EAP combined)
                                            ├── gold_congestion_patterns     (SQL — hourly patterns)
                                            └── gold_sensor_reliability      (Python @dlt.table — data quality)
                                                │
                                                ▼
                                          Metric Views (Governed Business Metrics)
                                            ├── traffic_congestion_metrics
                                            └── sensor_reliability_metrics
```

### Corridors (15 Vancouver routes)

| # | Corridor | Type |
|---|----------|------|
| 1 | Broadway & Commercial to VCC-Clark | arterial |
| 2 | Broadway (Commercial to Arbutus) | arterial |
| 3 | Knight Street Bridge | bridge |
| 4 | Lions Gate Bridge | bridge |
| 5 | Ironworkers Memorial | bridge |
| 6 | Granville Street Bridge | bridge |
| 7 | Oak Street Bridge | bridge |
| 8 | Alex Fraser Bridge | bridge |
| 9 | Kingsway (Vancouver to Burnaby) | arterial |
| 10 | Hastings St (Downtown to Burnaby) | arterial |
| 11 | Marine Drive East | arterial |
| 12 | Hwy 1 (Burnaby to Coquitlam) | highway |
| 13 | 49th Ave (Fraser to Knight) | arterial |
| 14 | Cambie Bridge | bridge |
| 15 | Pattullo Bridge | bridge |

### Synthetic EAP Data

| Table | Rows | Description |
|-------|------|-------------|
| `dim_corridors` | 15 + 2 updates | Corridor metadata with SCD changes |
| `dim_sensors` | 60 + 3 updates | Sensor locations with relocations |
| `fact_traffic_counts` | ~500K + corrections | Hourly counts with cursor column |

Seeded data quality issues: ~2% null counts, ~0.5% duplicates, ~0.3% negative counts.

### Data Quality Expectations

| Layer | Rule | Action |
|-------|------|--------|
| Silver | Null corridor_id or sensor_id | DROP ROW |
| Silver | Null timestamps | DROP ROW |
| Silver | Negative vehicle counts | WARN (keep row) |
| Silver | Speed > 200 km/h | WARN (keep row) |
| Silver | Congestion ratio < 1.0 | WARN (keep row) |
| Silver | Future timestamps | WARN (keep row) |

## Key Concepts Taught

### 1. SDP Interoperability — Mix SQL, Python, Streaming & Batch

A single Spark Declarative Pipeline can mix and match all of these — and they reference each other seamlessly:

| Notebook | Language | Table Type | Pattern |
|----------|----------|------------|---------|
| `bronze_traffic_api.sql` | SQL | Streaming Table | Auto Loader → `STREAM read_files()` |
| `bronze_eap_*.sql` | SQL | Streaming Table | Auto Loader → `STREAM read_files()` |
| `silver_traffic_readings.sql` | SQL | Materialized View | `CREATE OR REFRESH MATERIALIZED VIEW` |
| `silver_dim_corridors.sql` | SQL | Streaming Table | Auto CDC (SCD1) |
| `silver_dim_sensors.sql` | SQL | Streaming Table | Auto CDC (SCD2) |
| `silver_fact_traffic_counts.sql` | SQL | Streaming Table | Auto CDC with cursor |
| `gold_corridor_daily_summary.sql` | SQL | Materialized View | SQL aggregation over upstream tables |
| `gold_congestion_patterns.sql` | SQL | Materialized View | SQL aggregation over upstream tables |
| `gold_sensor_reliability.py` | **Python** | Materialized View | **`@dlt.table` + PySpark DataFrames** |

The `gold_sensor_reliability.py` notebook is **intentionally Python** to demonstrate that:
- Python notebooks use `dlt.read("table_name")` to read from SQL-defined upstream tables
- `@dlt.table` decorator replaces `CREATE OR REFRESH MATERIALIZED VIEW`
- `@dlt.expect_or_drop` replaces SQL `EXPECT ... ON VIOLATION DROP ROW`
- Standard PySpark DataFrame operations (joins, groupBy, agg) work naturally
- The SDP engine resolves dependencies across languages automatically

**This is the key takeaway for TransLink's migration:** you don't have to pick one approach. Complex transformations can use Python DataFrames, simple aggregations can use SQL, and they all compose into a single pipeline with dependency tracking, lineage, and data quality.

### 2. Python Data Sources (API Ingestion)

Custom PySpark DataSource (`src/python_data_sources/google_routes_source.py`) that:
- Subclasses `DataSource` and `DataSourceReader`
- Calls the Google Routes API with `TRAFFIC_AWARE` routing
- Returns structured rows with congestion metrics
- Requires DBR 15.4+ or serverless compute

### 3. Auto CDC (Incremental CDC)

`CREATE FLOW ... AS AUTO CDC INTO` (formerly `APPLY CHANGES INTO`) replaces TransLink's self-managed watermarking from Synapse:
- **SCD Type 1** (corridors, fact counts): updates overwrite previous version
- **SCD Type 2** (sensors): tracks history with `__START_AT` / `__END_AT`
- **Cursor column** (`last_modified_ts`): `SEQUENCE BY` ensures correct ordering
- **No custom MERGE logic needed** — SDP handles deduplication, late-arriving data, and cursor tracking automatically

This is one of SDP's core value propositions: what previously required custom watermarking notebooks in Synapse is now a declarative statement.

### 4. Data Quality Expectations

Available in both SQL and Python:
- **SQL:** `EXPECT (condition) ON VIOLATION DROP ROW / WARN`
- **Python:** `@dlt.expect_or_drop("name", "condition")` / `@dlt.expect("name", "condition")`
- Visible in the SDP pipeline UI event log
- `gold_sensor_reliability.py` uses Python expectations; silver tables use SQL

### 5. Primary Keys & Foreign Keys

Unity Catalog informational constraints (`src/pipeline/constraints_and_tags.sql`):
- Not enforced at write time, but used by the query optimizer
- Visible in Catalog Explorer lineage view
- Support data governance and documentation

### 6. Change Data Feed

Enabled on silver tables (`delta.enableChangeDataFeed = true`):
- Downstream consumers can read incremental changes
- Supports `table_changes()` function for CDC reads

### 7. Table Comments, Column Comments & Tags

- Table/column `COMMENT` for documentation
- `SET TAGS` for domain, source, PII classification
- Visible in Catalog Explorer

### 8. Metric Views

Governed business metrics with `MEASURE()` syntax:

```sql
SELECT "Corridor Name", MEASURE("Avg Congestion Ratio"), MEASURE("Total Vehicle Count")
FROM YOUR_CATALOG.traffic_hackathon_de.traffic_congestion_metrics
GROUP BY ALL;
```

### 9. DABs (Databricks Asset Bundles)

Full CI/CD deployment:
- Parameterized `databricks.yml` with variables for catalog, schema, volume
- Pipeline + workflow definitions in `resources/`
- `deploy.sh` for one-command deployment
- Dev/prod targets

### 10. Workflow Orchestration

Databricks Workflow with task dependencies:
- Task 1: Poll Routes API (notebook)
- Task 2: Run SDP pipeline (depends on Task 1)
- Task 3: Apply constraints (depends on Task 2)
- Task 4: Deploy metric views (depends on Task 2)
- Cron schedule: every 15 minutes during business hours (configurable)

## Managed Tables vs External Tables

This hackathon uses **managed tables** to showcase features like:
- **Liquid clustering** — auto-optimizes data layout, replaces PARTITION BY + ZORDER
- **Auto-optimize** — automatic compaction and optimization

**Important for TransLink production:** TransLink's EAP data will use **Unity Catalog external tables**. The following features are **not available on external tables**:
- Liquid clustering
- Predictive optimization
- Auto-optimize (auto compaction)

These features require managed tables. When migrating to production with external tables, you will need to manage compaction and file optimization manually or via scheduled OPTIMIZE commands. Consider using managed tables where possible to take advantage of these features.

Features that **do work on external tables**:
- Primary key / foreign key constraints
- Table and column comments
- Tags
- Change Data Feed
- Auto CDC (formerly APPLY CHANGES INTO)
- Data quality expectations
- Metric views

## Validation

After deployment, run these in the SQL Editor:

```sql
-- Check table row counts
SELECT 'silver_fact_traffic_counts' AS tbl, COUNT(*) AS cnt
  FROM YOUR_CATALOG.traffic_hackathon_de.silver_fact_traffic_counts
UNION ALL SELECT 'silver_dim_corridors', COUNT(*)
  FROM YOUR_CATALOG.traffic_hackathon_de.silver_dim_corridors
UNION ALL SELECT 'silver_dim_sensors', COUNT(*)
  FROM YOUR_CATALOG.traffic_hackathon_de.silver_dim_sensors
UNION ALL SELECT 'gold_corridor_daily_summary', COUNT(*)
  FROM YOUR_CATALOG.traffic_hackathon_de.gold_corridor_daily_summary;

-- Check data quality (should see WARN violations, no DROPs on clean data)
SELECT * FROM event_log(TABLE(YOUR_CATALOG.traffic_hackathon_de.silver_fact_traffic_counts))
WHERE event_type = 'flow_progress';

-- Test a metric view
SELECT "Corridor Name", "Avg Congestion Ratio", "Total Vehicle Count"
FROM YOUR_CATALOG.traffic_hackathon_de.traffic_congestion_metrics
ORDER BY "Avg Congestion Ratio" DESC
LIMIT 10;

-- Check FK constraints
SELECT * FROM information_schema.table_constraints
WHERE constraint_schema = 'traffic_hackathon_de';
```

## Project Structure

```
data_engineering_hackathon/
├── databricks.yml                              # DABs bundle config
├── deploy.sh                                   # One-command deploy
├── README.md                                   # This file
├── PLAN.md                                     # Implementation plan
├── resources/
│   ├── traffic_pipeline.yml                    # SDP pipeline definition
│   └── traffic_workflow.yml                    # Workflow + scheduling
└── src/
    ├── data_gen/
    │   ├── corridors.json                      # 15 Vancouver corridor definitions
    │   └── generate_eap_data.py                # Synthetic EAP data generator
    ├── python_data_sources/
    │   └── google_routes_source.py             # Custom PySpark DataSource for Routes API
    ├── notebooks/
    │   └── poll_routes_api.py                  # Scheduled notebook: poll API → write to Volume
    ├── pipeline/
    │   ├── bronze_traffic_api.sql              # Auto Loader: API JSON
    │   ├── bronze_eap_fact_traffic.sql         # Auto Loader: EAP facts
    │   ├── bronze_eap_dim_corridors.sql        # Auto Loader: EAP corridors
    │   ├── bronze_eap_dim_sensors.sql          # Auto Loader: EAP sensors
    │   ├── silver_traffic_readings.sql         # MV: cleaned API data + congestion severity
    │   ├── silver_fact_traffic_counts.sql      # Auto CDC: cursor-based CDC
    │   ├── silver_dim_corridors.sql            # Auto CDC: SCD Type 1
    │   ├── silver_dim_sensors.sql              # Auto CDC: SCD Type 2
    │   ├── gold_corridor_daily_summary.sql     # API + EAP combined daily summary
    │   ├── gold_congestion_patterns.sql        # Hourly congestion patterns
    │   ├── gold_sensor_reliability.py          # Python @dlt.table — sensor quality metrics
    │   └── constraints_and_tags.sql            # PK/FK, comments, tags
    └── metric_views/
        ├── traffic_congestion_metrics.sql      # Governed congestion metrics
        ├── sensor_reliability_metrics.sql      # Governed sensor quality metrics
        └── deploy_metric_views.py              # Notebook to deploy metric views
```

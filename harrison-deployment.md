# Deployment Fix Log

## Issue: `databricks fs cp` fails with "no such directory" on UC Volumes

### Problem

The `deploy.sh` script used bare paths when uploading files to Unity Catalog Volumes:

```bash
databricks fs cp file.json /Volumes/catalog/schema/volume/config/file.json --overwrite
```

This produced the error:

```
Error: no such directory: /Volumes/catalog/schema/volume/config/file.json
```

The Databricks CLI `fs` commands require volume paths to be prefixed with `dbfs:`. Without the prefix, the CLI does not recognize the path as a UC Volume path and fails.

### Fix

Added a `DBFS_VOLUME_PATH` variable that prepends `dbfs:` to the volume path, and updated all `fs mkdir` and `fs cp` commands to use it:

```bash
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}"
DBFS_VOLUME_PATH="dbfs:${VOLUME_PATH}"

# Before (broken)
$CLI fs cp file.json "${VOLUME_PATH}/config/file.json" --overwrite

# After (working)
$CLI fs cp file.json "${DBFS_VOLUME_PATH}/config/file.json" --overwrite
```

The display/log messages continue to use the clean `/Volumes/...` path for readability.

### Reference

Databricks CLI `fs` command docs confirm the `dbfs:` prefix requirement for volume paths:
https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/reference/fs-commands

---

## Issue: `spark.dataSource` not available on serverless Environment v1

### Problem

The `poll_routes_api` notebook uses a custom Python Data Source (`GoogleRoutesDataSource`) registered via `spark.dataSource.register()`. This is the Python Data Source API introduced in PySpark for creating custom data sources in pure Python.

The workflow in `resources/traffic_workflow.yml` ran on serverless compute with `client: "1"` (Environment v1). This environment uses an older PySpark runtime (Python 3.10) where the Spark Connect session does not include the `spark.dataSource` attribute. The job failed with:

```
AttributeError: 'SparkSession' object has no attribute 'dataSource'
```

The Python Data Source API requires serverless **Environment v2** (equivalent to DBR 15.4+, Python 3.11+), which adds `spark.dataSource` to the Spark Connect session.

### Fix

Changed the environment client version from `"1"` to `"2"` in `resources/traffic_workflow.yml`:

```yaml
# Before
environments:
  - environment_key: default
    spec:
      client: "1"

# After
environments:
  - environment_key: default
    spec:
      client: "2"
```

**Important:** This change must be made in the DABs YAML file, not the Databricks UI. The bundle deploy overwrites UI changes with whatever is defined in the YAML.

### Reference

Serverless environment dependencies and configuration:
https://docs.databricks.com/aws/en/compute/serverless/dependencies

---

## Issue: Google Routes API key stored with trailing newline

### Problem

The `deploy.sh` script stored the API key in Databricks Secrets using `echo`:

```bash
echo "$API_KEY" | $CLI secrets put-secret hackathon google_routes_api_key
```

`echo` appends a newline character by default, so the stored secret became `AIzaSy...3gLE\n`. When the `poll_routes_api` notebook passed this value as the `X-Goog-Api-Key` HTTP header, the trailing newline made it an invalid header value. The Google Routes API rejected the request, and the error was captured in the `api_response_json` column:

```json
{"error": "Invalid header value b'YOUR_API_KEY_HERE\\n'"}
```

The pipeline didn't fail because the data source catches API errors and stores them in the response column. However, all route data was lost — every row contained an error instead of actual traffic data.

### Fix

Replaced `echo` with `printf '%s'` which does not append a newline:

```bash
# Before (adds trailing newline)
echo "$API_KEY" | $CLI secrets put-secret hackathon google_routes_api_key

# After (no trailing newline)
printf '%s' "$API_KEY" | $CLI secrets put-secret hackathon google_routes_api_key
```

### Cleanup

Stale error rows from previous runs persist in the bronze table since they were already ingested. The silver layer (`silver_traffic_readings.sql`) filters these out with `WHERE api_response_json NOT LIKE '%"error"%'`, so downstream gold tables are unaffected. To fully clean the bronze data, delete the old JSON files from the `traffic_api/` volume directory and rerun the pipeline with a full refresh.

---

## Issue: `ALTER TABLE` fails on materialized views and streaming tables

### Problem

The `constraints_and_tags.sql` notebook uses `ALTER TABLE` to add column comments, table comments, and tags. This fails because:

- `silver_traffic_readings` is a **materialized view** — `ALTER TABLE` does not apply to views
- `silver_fact_traffic_counts`, `silver_dim_corridors`, `silver_dim_sensors` are **streaming tables** — `ALTER TABLE` is also not supported for modifying these after creation

The job failed with:

```
[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'ALTER TABLE ... ALTER COLUMN' expects a table
but `translink_hackathon`.`traffic_hackathon_de`.`silver_traffic_readings` is a view.
```

### Correct approach for constraints and metadata

**Materialized views** — Use `ALTER MATERIALIZED VIEW` for column comments, tags, and masks:

```sql
ALTER MATERIALIZED VIEW catalog.schema.silver_traffic_readings
  ALTER COLUMN congestion_ratio COMMENT 'Ratio of actual to free-flow time';

ALTER MATERIALIZED VIEW catalog.schema.silver_traffic_readings
  SET TAGS ('domain' = 'operations');
```

**Streaming tables** — Constraints (PRIMARY KEY, FOREIGN KEY) are supported but must be declared **inline at creation time** in the `CREATE OR REFRESH STREAMING TABLE` statement, not added after the fact with `ALTER TABLE`. Inline constraints work for single-column constraints. Multi-column constraints must use the out-of-line (table-level) syntax, also at creation time:

```sql
-- Inline single-column constraint
CREATE OR REFRESH STREAMING TABLE silver_dim_corridors (
    corridor_id STRING PRIMARY KEY,
    ...
)

-- Out-of-line multi-column constraint
CREATE OR REFRESH STREAMING TABLE silver_fact_traffic_counts (
    corridor_id STRING,
    sensor_id STRING,
    count_hour TIMESTAMP,
    ...,
    CONSTRAINT pk_fact_traffic PRIMARY KEY (corridor_id, sensor_id, count_hour)
)
```

### Fix

Constraints for streaming tables need to be moved into their respective pipeline SQL definitions (`silver_dim_corridors.sql`, `silver_dim_sensors.sql`, `silver_fact_traffic_counts.sql`). Column comments and tags for materialized views need to use `ALTER MATERIALIZED VIEW` instead of `ALTER TABLE`.

### Reference

Streaming table constraint syntax:
https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table

Materialized view ALTER syntax:
https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-alter-materialized-view

---

## Investigation: total_vehicle_count Null in Gold Layer

**Finding:** `total_vehicle_count` returns null for high-congestion corridors in the `traffic_congestion_metrics` metric view, which is a passthrough from `gold_corridor_daily_summary`.

**Root Cause:** Date range mismatch between the two data sources joined in the gold layer. The `gold_corridor_daily_summary` materialized view (`src/pipeline/gold_corridor_daily_summary.sql`) performs a `FULL OUTER JOIN` between `api_daily` (live Google Routes API data) and `eap_daily` (synthetic EAP sensor data) on `corridor_id` and date:

```sql
FROM api_daily a
FULL OUTER JOIN eap_daily e
  ON a.corridor_id = e.corridor_id AND a.poll_date = e.count_date
```

The EAP synthetic data covers **Dec 2025 – Feb 2026**, but the live API polls produce rows for **March 2026**. Since there is no EAP data for March dates, the join produces no match on the EAP side, and `e.total_vehicle_count` is null for all API-polled rows.

**This is expected behavior**, not a bug in the aggregation logic. The null means "no sensor data available for this date" which is accurate. In a production deployment where the EAP data feed is continuous and covers the same date range as the API polls, this column would be populated.

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Primary Keys, Foreign Keys, Tags & Comments
-- MAGIC
-- MAGIC Unity Catalog supports **informational constraints** — PK/FK declarations
-- MAGIC that are not enforced at write time but are:
-- MAGIC - Visible in the Catalog Explorer lineage view
-- MAGIC - Used by the query optimizer for join reordering
-- MAGIC - Surfaced in data governance tools
-- MAGIC
-- MAGIC Run this notebook AFTER the pipeline has completed the first run.
-- MAGIC
-- MAGIC **Note for TransLink production:** These constraints work on both
-- MAGIC managed and external tables in Unity Catalog.
-- MAGIC
-- MAGIC **⚠️ Limitation:** Streaming tables created by `Auto CDC`
-- MAGIC do not support `ALTER TABLE` for adding PK/FK constraints or tags.
-- MAGIC The constraints and tags below apply to **materialized views** and
-- MAGIC **regular Delta tables** only. For streaming tables, document the
-- MAGIC logical keys in table/column comments instead.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Primary Keys
-- MAGIC
-- MAGIC **Note:** The statements below will fail on streaming tables created by
-- MAGIC `Auto CDC`. They are included here as reference for when
-- MAGIC TransLink's production tables are regular Delta tables (managed or external).
-- MAGIC
-- MAGIC To run these in production, create the silver tables as regular Delta tables
-- MAGIC with incremental merge logic in notebooks, rather than streaming tables.

-- COMMAND ----------

-- These will succeed on regular Delta tables but fail on streaming tables.
-- Uncomment when targeting regular Delta tables in production.

-- ALTER TABLE ${catalog}.${schema}.silver_dim_corridors
--   ADD CONSTRAINT pk_dim_corridors PRIMARY KEY (corridor_id);

-- ALTER TABLE ${catalog}.${schema}.silver_dim_sensors
--   ADD CONSTRAINT pk_dim_sensors PRIMARY KEY (sensor_id);

-- ALTER TABLE ${catalog}.${schema}.silver_fact_traffic_counts
--   ADD CONSTRAINT pk_fact_traffic PRIMARY KEY (corridor_id, sensor_id, count_hour);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Foreign Keys

-- COMMAND ----------

-- ALTER TABLE ${catalog}.${schema}.silver_fact_traffic_counts
--   ADD CONSTRAINT fk_fact_to_corridor
--   FOREIGN KEY (corridor_id) REFERENCES ${catalog}.${schema}.silver_dim_corridors (corridor_id);

-- ALTER TABLE ${catalog}.${schema}.silver_fact_traffic_counts
--   ADD CONSTRAINT fk_fact_to_sensor
--   FOREIGN KEY (sensor_id) REFERENCES ${catalog}.${schema}.silver_dim_sensors (sensor_id);

-- ALTER TABLE ${catalog}.${schema}.silver_dim_sensors
--   ADD CONSTRAINT fk_sensor_to_corridor
--   FOREIGN KEY (corridor_id) REFERENCES ${catalog}.${schema}.silver_dim_corridors (corridor_id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Table Comments & Column Tags
-- MAGIC
-- MAGIC Good metadata makes the data discoverable in Catalog Explorer
-- MAGIC and helps Genie Spaces understand the data model.

-- COMMAND ----------

-- Table-level comments (already set in pipeline, but can be updated here)
COMMENT ON TABLE ${catalog}.${schema}.silver_fact_traffic_counts IS
  'Hourly traffic counts per sensor per corridor. Incrementally merged from EAP via Auto CDC with last_modified_ts cursor column. Grain: one row per corridor × sensor × hour.';

COMMENT ON TABLE ${catalog}.${schema}.silver_dim_corridors IS
  'Corridor dimension — 15 Vancouver traffic corridors including bridges, arterials, and highways. SCD Type 1 maintained via Auto CDC.';

COMMENT ON TABLE ${catalog}.${schema}.silver_dim_sensors IS
  'Sensor dimension — traffic sensors/cameras on each corridor. SCD Type 2 tracks relocations and status changes over time.';

COMMENT ON TABLE ${catalog}.${schema}.silver_traffic_readings IS
  'Live traffic congestion readings from the Google Routes API. Polled every 15 minutes for 15 corridors. Deduplicated and enriched with congestion severity and time dimensions.';

COMMENT ON TABLE ${catalog}.${schema}.gold_corridor_daily_summary IS
  'Daily corridor performance combining live API congestion data with EAP sensor volumes. Primary analytics table for dashboards.';

-- COMMAND ----------

-- Column-level comments on key business columns
ALTER TABLE ${catalog}.${schema}.silver_traffic_readings
  ALTER COLUMN congestion_ratio COMMENT 'Ratio of actual travel time to free-flow time. Values above 1.0 indicate congestion. Above 2.0 is severe.';

ALTER TABLE ${catalog}.${schema}.silver_traffic_readings
  ALTER COLUMN congestion_severity COMMENT 'Categorical: free_flow (<1.1), light (1.1-1.3), moderate (1.3-1.6), heavy (1.6-2.0), severe (>2.0)';

ALTER TABLE ${catalog}.${schema}.silver_fact_traffic_counts
  ALTER COLUMN last_modified_ts COMMENT 'EAP cursor column — used by Auto CDC to sequence updates. Late-arriving corrections have newer timestamps.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tags (Unity Catalog)
-- MAGIC
-- MAGIC Tags help organize tables by domain, sensitivity, and ownership.

-- COMMAND ----------

-- Note: Tags on streaming tables (silver_fact_traffic_counts, silver_dim_corridors,
-- silver_dim_sensors) will fail with STREAMING_TABLE_OPERATION_NOT_ALLOWED.
-- Only materialized views and regular tables support SET TAGS.
-- Also: if your workspace has tag policies, use allowed values (e.g. 'operations'
-- instead of 'traffic' for the domain key).

ALTER TABLE ${catalog}.${schema}.silver_traffic_readings    SET TAGS ('domain' = 'operations', 'source' = 'google_api', 'pii' = 'false');
ALTER TABLE ${catalog}.${schema}.gold_corridor_daily_summary SET TAGS ('domain' = 'operations', 'tier' = 'gold', 'pii' = 'false');
ALTER TABLE ${catalog}.${schema}.gold_congestion_patterns    SET TAGS ('domain' = 'operations', 'tier' = 'gold', 'pii' = 'false');
ALTER TABLE ${catalog}.${schema}.gold_sensor_reliability     SET TAGS ('domain' = 'operations', 'tier' = 'gold', 'pii' = 'false');

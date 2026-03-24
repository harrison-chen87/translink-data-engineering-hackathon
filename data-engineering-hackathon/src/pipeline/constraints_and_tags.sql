-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Table Comments & Tags
-- MAGIC
-- MAGIC Unity Catalog metadata for governance and discoverability.
-- MAGIC Run this notebook AFTER the pipeline has completed the first run.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Table Comments

-- COMMAND ----------

-- GTFS dimension tables
COMMENT ON TABLE ${catalog}.${schema}.silver_gtfs_routes IS
  'TransLink route definitions — 242 routes (bus, SkyTrain, SeaBus). SCD Type 1 maintained via Auto CDC across weekly GTFS snapshots.';

COMMENT ON TABLE ${catalog}.${schema}.silver_gtfs_stops IS
  'TransLink bus/train stops — 8,936 stops with geographic coordinates. SCD Type 2 tracks stop changes across weekly GTFS snapshots.';

-- API tables
COMMENT ON TABLE ${catalog}.${schema}.silver_traffic_readings IS
  'Live traffic congestion readings from the Google Routes API. Polled for GTFS-derived route corridors. Deduplicated and enriched with congestion severity and time dimensions.';

-- Gold tables
COMMENT ON TABLE ${catalog}.${schema}.gold_schedule_vs_actual IS
  'Schedule vs. actual travel time per route — joins GTFS scheduled times with live API congestion data to measure transit delays.';

COMMENT ON TABLE ${catalog}.${schema}.gold_route_delay_patterns IS
  'Route-level delay patterns — which routes are consistently late, by hour of day and day type. Derived from schedule-vs-actual analysis.';

COMMENT ON TABLE ${catalog}.${schema}.gold_congestion_patterns IS
  'Hourly congestion patterns by corridor, day of week, and time period. Derived from Google Routes API readings.';

COMMENT ON TABLE ${catalog}.${schema}.gold_corridor_daily_summary IS
  'Daily corridor performance combining GTFS scheduled times with live API congestion data.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tags (Unity Catalog)
-- MAGIC
-- MAGIC **Note:** If your metastore has governed tag policies, some tag keys or
-- MAGIC values may be restricted. Tags are set via Python with error handling.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema = dbutils.widgets.get("schema")
-- MAGIC
-- MAGIC tags_by_table = {
-- MAGIC     "silver_gtfs_routes": {"domain": "transit", "source": "gtfs", "tier": "silver"},
-- MAGIC     "silver_gtfs_stops": {"domain": "transit", "source": "gtfs", "tier": "silver"},
-- MAGIC     "silver_traffic_readings": {"domain": "transit", "source": "google_api", "tier": "silver"},
-- MAGIC     "gold_schedule_vs_actual": {"domain": "transit", "tier": "gold"},
-- MAGIC     "gold_route_delay_patterns": {"domain": "transit", "tier": "gold"},
-- MAGIC     "gold_congestion_patterns": {"domain": "transit", "tier": "gold"},
-- MAGIC     "gold_corridor_daily_summary": {"domain": "transit", "tier": "gold"},
-- MAGIC }
-- MAGIC
-- MAGIC for table, tags in tags_by_table.items():
-- MAGIC     tag_str = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
-- MAGIC     try:
-- MAGIC         spark.sql(f"ALTER TABLE {catalog}.{schema}.{table} SET TAGS ({tag_str})")
-- MAGIC         print(f"  Tags set on {table}: {tags}")
-- MAGIC     except Exception as e:
-- MAGIC         print(f"  WARNING: Could not set tags on {table}: {e}")
-- MAGIC         print(f"  This may be due to governed tag policies on your metastore.")

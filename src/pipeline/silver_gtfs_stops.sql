-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: GTFS Stops (Auto CDC — SCD Type 2)
-- MAGIC
-- MAGIC All TransLink bus/train stops with full change history tracked via
-- MAGIC SCD Type 2. When a stop is renamed, relocated, or its accessibility
-- MAGIC changes, a new version is created with `__START_AT` / `__END_AT`.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_gtfs_stops (
  CONSTRAINT stop_id_not_null EXPECT (stop_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'TransLink stop definitions with SCD Type 2 change history'
TBLPROPERTIES ('quality' = 'silver');

-- COMMAND ----------

CREATE FLOW apply_gtfs_stops
AS APPLY CHANGES INTO LIVE.silver_gtfs_stops
FROM STREAM(LIVE.bronze_gtfs_stops)
KEYS (stop_id)
SEQUENCE BY gtfs_snapshot_date
COLUMNS * EXCEPT (_source_file, gtfs_snapshot_date)
STORED AS SCD TYPE 2;

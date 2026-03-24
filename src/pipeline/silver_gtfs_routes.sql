-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: GTFS Routes (Auto CDC — SCD Type 1)
-- MAGIC
-- MAGIC Current state of all TransLink routes, incrementally maintained
-- MAGIC via Auto CDC across weekly GTFS snapshots. SCD Type 1 means
-- MAGIC updates overwrite previous values — we keep the latest version.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_gtfs_routes (
  CONSTRAINT route_id_not_null EXPECT (route_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Current TransLink route definitions — SCD Type 1 via Auto CDC'
TBLPROPERTIES ('quality' = 'silver');

-- COMMAND ----------

CREATE FLOW apply_gtfs_routes
AS APPLY CHANGES INTO LIVE.silver_gtfs_routes
FROM STREAM(LIVE.bronze_gtfs_routes)
KEYS (route_id)
SEQUENCE BY gtfs_snapshot_date
COLUMNS * EXCEPT (_source_file, gtfs_snapshot_date)
STORED AS SCD TYPE 1;

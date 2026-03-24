-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: GTFS Trips
-- MAGIC
-- MAGIC Individual trip instances per route per service day.
-- MAGIC Links routes to shapes and stop_times.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_gtfs_trips
COMMENT 'Raw GTFS trip definitions linking routes to shapes and schedules'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  CAST(route_id AS STRING) AS route_id,
  CAST(service_id AS STRING) AS service_id,
  CAST(trip_id AS STRING) AS trip_id,
  trip_headsign,
  trip_short_name,
  CAST(direction_id AS INT) AS direction_id,
  block_id,
  CAST(shape_id AS STRING) AS shape_id,
  CAST(wheelchair_accessible AS INT) AS wheelchair_accessible,
  CAST(bikes_allowed AS INT) AS bikes_allowed,
  regexp_extract(_metadata.file_path, '(\\d{4}-\\d{2}-\\d{2})', 1) AS gtfs_snapshot_date,
  _metadata.file_name AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/gtfs/extracted/*/trips.txt',
  format => 'csv',
  header => true
);

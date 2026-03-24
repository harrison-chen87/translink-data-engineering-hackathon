-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: GTFS Stops
-- MAGIC
-- MAGIC All TransLink bus/train stops with geographic coordinates.
-- MAGIC Multiple weekly snapshots ingested for SCD Type 2 change tracking.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_gtfs_stops
COMMENT 'Raw GTFS stop definitions across weekly snapshots'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  CAST(stop_id AS STRING) AS stop_id,
  stop_code,
  stop_name,
  stop_desc,
  CAST(stop_lat AS DOUBLE) AS stop_lat,
  CAST(stop_lon AS DOUBLE) AS stop_lon,
  zone_id,
  stop_url,
  CAST(location_type AS INT) AS location_type,
  parent_station,
  CAST(wheelchair_boarding AS INT) AS wheelchair_boarding,
  regexp_extract(_metadata.file_path, '(\\d{4}-\\d{2}-\\d{2})', 1) AS gtfs_snapshot_date,
  _metadata.file_name AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/gtfs/extracted/*/stops.txt',
  format => 'csv',
  header => true
);

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: GTFS Stop Times
-- MAGIC
-- MAGIC Scheduled arrival and departure times at every stop for every trip.
-- MAGIC This is the largest GTFS table (~1.8M rows per snapshot) and forms
-- MAGIC the basis for calculating scheduled travel times between stops.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_gtfs_stop_times
COMMENT 'Raw GTFS scheduled stop times — arrival/departure at every stop for every trip'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  CAST(trip_id AS STRING) AS trip_id,
  arrival_time,
  departure_time,
  CAST(stop_id AS STRING) AS stop_id,
  CAST(stop_sequence AS INT) AS stop_sequence,
  stop_headsign,
  CAST(pickup_type AS INT) AS pickup_type,
  CAST(drop_off_type AS INT) AS drop_off_type,
  CAST(shape_dist_traveled AS DOUBLE) AS shape_dist_traveled,
  CAST(timepoint AS INT) AS timepoint,
  regexp_extract(_metadata.file_path, '(\\d{4}-\\d{2}-\\d{2})', 1) AS gtfs_snapshot_date,
  _metadata.file_name AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/gtfs/extracted/*/stop_times.txt',
  format => 'csv',
  header => true
);

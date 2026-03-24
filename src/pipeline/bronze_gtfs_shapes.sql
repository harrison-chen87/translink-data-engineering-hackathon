-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: GTFS Shapes
-- MAGIC
-- MAGIC GPS polylines for every route variant. Each shape is a sequence
-- MAGIC of lat/lng points that traces the route on a map.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_gtfs_shapes
COMMENT 'Raw GTFS shape points — GPS polylines for route mapping'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  CAST(shape_id AS STRING) AS shape_id,
  CAST(shape_pt_lat AS DOUBLE) AS shape_pt_lat,
  CAST(shape_pt_lon AS DOUBLE) AS shape_pt_lon,
  CAST(shape_pt_sequence AS INT) AS shape_pt_sequence,
  CAST(shape_dist_traveled AS DOUBLE) AS shape_dist_traveled,
  regexp_extract(_metadata.file_path, '(\\d{4}-\\d{2}-\\d{2})', 1) AS gtfs_snapshot_date,
  _metadata.file_name AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/gtfs/extracted/*/shapes.txt',
  format => 'csv',
  header => true
);

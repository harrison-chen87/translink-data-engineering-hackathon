-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: GTFS Routes
-- MAGIC
-- MAGIC All TransLink routes (bus, SkyTrain, SeaBus) from the GTFS feed.
-- MAGIC Multiple weekly snapshots are ingested to track route changes over time.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_gtfs_routes
COMMENT 'Raw GTFS route definitions across weekly snapshots'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  CAST(route_id AS STRING) AS route_id,
  agency_id,
  route_short_name,
  route_long_name,
  route_desc,
  CAST(route_type AS INT) AS route_type,
  route_url,
  route_color,
  route_text_color,
  regexp_extract(_metadata.file_path, '(\\d{4}-\\d{2}-\\d{2})', 1) AS gtfs_snapshot_date,
  _metadata.file_name AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/gtfs/extracted/*/routes.txt',
  format => 'csv',
  header => true
);

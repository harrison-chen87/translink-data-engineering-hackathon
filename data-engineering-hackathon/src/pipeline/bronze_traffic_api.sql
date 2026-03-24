-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: Google Routes API Traffic Readings
-- MAGIC
-- MAGIC Auto Loader ingests JSON files written by the poll_routes_api notebook.
-- MAGIC Raw data preserved as-is — no transformations, no quality filters.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_traffic_api
COMMENT 'Raw traffic readings from Google Routes API polls'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.zOrderCols' = 'corridor_id,polled_at'
)
AS SELECT
  *,
  _metadata.file_name AS _source_file,
  _metadata.file_modification_time AS _file_arrival_time
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/traffic_api/',
  format => 'json',
  header => true,
  schema => '
    corridor_id STRING,
    corridor_name STRING,
    origin_lat DOUBLE,
    origin_lng DOUBLE,
    dest_lat DOUBLE,
    dest_lng DOUBLE,
    duration_seconds INT,
    static_duration_seconds INT,
    distance_meters INT,
    congestion_ratio DOUBLE,
    api_response_json STRING,
    polled_at TIMESTAMP
  '
);

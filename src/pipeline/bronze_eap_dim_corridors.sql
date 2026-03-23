-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: EAP Dimension — Corridors
-- MAGIC
-- MAGIC Corridor metadata from the Enterprise Analytics Platform.
-- MAGIC Includes cursor column for change tracking.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_eap_dim_corridors
COMMENT 'Raw EAP corridor dimension records with cursor column'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_name AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/eap/dim_corridors/',
  format => 'json',
  schemaHints => '
    corridor_id STRING,
    corridor_name STRING,
    corridor_type STRING,
    zone STRING,
    speed_limit_kmh INT,
    num_lanes INT,
    origin_lat DOUBLE,
    origin_lng DOUBLE,
    dest_lat DOUBLE,
    dest_lng DOUBLE,
    last_modified_ts TIMESTAMP
  '
);

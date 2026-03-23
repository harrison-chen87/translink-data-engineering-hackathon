-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: EAP Fact Traffic Counts
-- MAGIC
-- MAGIC Auto Loader ingests EAP fact records from the Volume.
-- MAGIC These simulate the nightly batch exports from TransLink's
-- MAGIC Enterprise Analytics Platform (EAP) with cursor columns.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_eap_fact_traffic
COMMENT 'Raw EAP hourly traffic counts with cursor column (last_modified_ts)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_name AS _source_file,
  _metadata.file_modification_time AS _file_arrival_time
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/eap/fact_traffic_counts/',
  format => 'json',
  schemaHints => '
    corridor_id STRING,
    sensor_id STRING,
    count_hour TIMESTAMP,
    vehicle_count INT,
    avg_speed_kmh DOUBLE,
    occupancy_pct DOUBLE,
    direction STRING,
    last_modified_ts TIMESTAMP
  '
);

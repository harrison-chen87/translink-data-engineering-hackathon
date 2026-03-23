-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: EAP Dimension — Sensors
-- MAGIC
-- MAGIC Sensor/camera location metadata from the Enterprise Analytics Platform.
-- MAGIC Includes cursor column for tracking sensor relocations and status changes.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_eap_dim_sensors
COMMENT 'Raw EAP sensor dimension records with cursor column'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_name AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/eap/dim_sensors/',
  format => 'json',
  schemaHints => '
    sensor_id STRING,
    corridor_id STRING,
    sensor_type STRING,
    location_name STRING,
    lat DOUBLE,
    lng DOUBLE,
    install_date STRING,
    status STRING,
    last_modified_ts TIMESTAMP
  '
);

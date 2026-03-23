-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Dimension — Sensors (EAP — Auto CDC SCD Type 2)
-- MAGIC
-- MAGIC Sensor metadata maintained via Auto CDC with SCD Type 2.
-- MAGIC This tracks sensor relocations and status changes over time,
-- MAGIC preserving history with __START_AT / __END_AT columns.
-- MAGIC
-- MAGIC Example: when a sensor is relocated, the old record gets an end date
-- MAGIC and a new record is created with the updated location.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_dim_sensors
COMMENT 'Sensor dimension — SCD Type 2 via Auto CDC (tracks relocations and status changes)'
TBLPROPERTIES (
  'quality' = 'silver',
  'delta.enableChangeDataFeed' = 'true'
);

-- COMMAND ----------

CREATE FLOW dim_sensors_cdc
AS AUTO CDC INTO LIVE.silver_dim_sensors
FROM STREAM(LIVE.bronze_eap_dim_sensors)
KEYS (sensor_id)
SEQUENCE BY last_modified_ts
COLUMNS * EXCEPT (_source_file)
STORED AS SCD TYPE 2;

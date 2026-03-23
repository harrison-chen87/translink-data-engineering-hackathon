-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Dimension — Corridors (EAP — Auto CDC SCD Type 1)
-- MAGIC
-- MAGIC Corridor metadata maintained via Auto CDC.
-- MAGIC SCD Type 1: updates overwrite the previous version (no history kept).
-- MAGIC
-- MAGIC When a corridor is renamed or its speed limit changes, the new record
-- MAGIC arrives with a newer `last_modified_ts` and replaces the old one.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_dim_corridors
COMMENT 'Corridor dimension — SCD Type 1 via Auto CDC'
TBLPROPERTIES (
  'quality' = 'silver',
  'delta.enableChangeDataFeed' = 'true'
);

-- COMMAND ----------

CREATE FLOW dim_corridors_cdc
AS AUTO CDC INTO LIVE.silver_dim_corridors
FROM STREAM(LIVE.bronze_eap_dim_corridors)
KEYS (corridor_id)
SEQUENCE BY last_modified_ts
COLUMNS * EXCEPT (_source_file)
STORED AS SCD TYPE 1;

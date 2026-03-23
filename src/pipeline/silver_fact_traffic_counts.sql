-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Fact Traffic Counts (EAP — Auto CDC)
-- MAGIC
-- MAGIC Incrementally merges EAP fact records using Auto CDC
-- MAGIC with the `last_modified_ts` cursor column. This replaces
-- MAGIC TransLink's self-managed watermarking logic from Synapse.
-- MAGIC
-- MAGIC - Deduplicates by composite key (corridor_id, sensor_id, count_hour)
-- MAGIC - Late-arriving corrections automatically overwrite older versions
-- MAGIC
-- MAGIC **SDP value:** Auto CDC (`CREATE FLOW ... AS AUTO CDC INTO`) handles
-- MAGIC cursor tracking, deduplication, and late-arriving data automatically —
-- MAGIC no custom watermark logic or MERGE statements needed.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_fact_traffic_counts
COMMENT 'Incrementally merged EAP traffic counts via Auto CDC with cursor column'
TBLPROPERTIES (
  'quality' = 'silver',
  'delta.enableChangeDataFeed' = 'true'
);

-- COMMAND ----------

-- Auto CDC: uses last_modified_ts as the sequence (cursor) column.
-- When a correction arrives with a newer last_modified_ts for the same
-- (corridor_id, sensor_id, count_hour) key, it overwrites the old record.

CREATE FLOW fact_traffic_cdc
AS AUTO CDC INTO LIVE.silver_fact_traffic_counts
FROM STREAM(LIVE.bronze_eap_fact_traffic)
KEYS (corridor_id, sensor_id, count_hour)
SEQUENCE BY last_modified_ts
COLUMNS * EXCEPT (_source_file, _file_arrival_time)
STORED AS SCD TYPE 1;

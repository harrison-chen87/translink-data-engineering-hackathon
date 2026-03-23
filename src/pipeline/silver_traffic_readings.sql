-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Traffic Readings (Google Routes API)
-- MAGIC
-- MAGIC Cleans and enriches live traffic data from the Routes API:
-- MAGIC - Deduplicates by corridor_id + polled_at
-- MAGIC - Classifies congestion severity
-- MAGIC - Adds time dimensions (hour, day_of_week, rush_hour flag)
-- MAGIC
-- MAGIC Data quality: filters out failed API calls and null keys.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_traffic_readings
COMMENT 'Cleaned and enriched Google Routes API traffic readings with congestion classification'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY corridor_id, polled_at ORDER BY _file_arrival_time DESC) AS _rn
  FROM LIVE.bronze_traffic_api
  WHERE api_response_json NOT LIKE '%"error"%'  -- exclude failed API calls
    AND corridor_id IS NOT NULL
    AND polled_at IS NOT NULL
)
SELECT
  corridor_id,
  corridor_name,
  origin_lat,
  origin_lng,
  dest_lat,
  dest_lng,
  duration_seconds,
  static_duration_seconds,
  distance_meters,
  congestion_ratio,

  -- Congestion severity classification
  CASE
    WHEN congestion_ratio < 1.1  THEN 'free_flow'
    WHEN congestion_ratio < 1.3  THEN 'light'
    WHEN congestion_ratio < 1.6  THEN 'moderate'
    WHEN congestion_ratio < 2.0  THEN 'heavy'
    ELSE 'severe'
  END AS congestion_severity,

  -- Time dimensions
  HOUR(polled_at)          AS hour_of_day,
  DAYOFWEEK(polled_at)     AS day_of_week,
  DATE(polled_at)          AS poll_date,
  CASE
    WHEN HOUR(polled_at) BETWEEN 7 AND 9   THEN 'morning_rush'
    WHEN HOUR(polled_at) BETWEEN 10 AND 15  THEN 'midday'
    WHEN HOUR(polled_at) BETWEEN 16 AND 18  THEN 'evening_rush'
    WHEN HOUR(polled_at) BETWEEN 19 AND 21  THEN 'evening'
    ELSE 'overnight'
  END AS time_period,
  CASE
    WHEN DAYOFWEEK(polled_at) IN (1, 7) THEN 'weekend'
    ELSE 'weekday'
  END AS day_type,

  polled_at,
  _source_file

FROM deduplicated
WHERE _rn = 1;

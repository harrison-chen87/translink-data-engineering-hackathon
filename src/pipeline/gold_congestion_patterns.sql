-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: Congestion Patterns
-- MAGIC
-- MAGIC Hourly congestion patterns by corridor, day of week, and time period.
-- MAGIC Used for identifying peak congestion windows and comparing
-- MAGIC weekday vs weekend patterns across corridors.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_congestion_patterns
COMMENT 'Hourly congestion patterns by corridor, day of week, and time period'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  corridor_id,
  corridor_name,
  day_of_week,
  day_type,
  hour_of_day,
  time_period,

  COUNT(*)                            AS num_observations,
  ROUND(AVG(congestion_ratio), 3)     AS avg_congestion_ratio,
  ROUND(PERCENTILE(congestion_ratio, 0.50), 3) AS median_congestion_ratio,
  ROUND(PERCENTILE(congestion_ratio, 0.95), 3) AS p95_congestion_ratio,
  ROUND(AVG(duration_seconds), 0)     AS avg_duration_seconds,
  ROUND(AVG(distance_meters), 0)      AS avg_distance_meters,

  -- Severity distribution
  SUM(CASE WHEN congestion_severity = 'free_flow' THEN 1 ELSE 0 END) AS free_flow_count,
  SUM(CASE WHEN congestion_severity = 'light'     THEN 1 ELSE 0 END) AS light_count,
  SUM(CASE WHEN congestion_severity = 'moderate'  THEN 1 ELSE 0 END) AS moderate_count,
  SUM(CASE WHEN congestion_severity = 'heavy'     THEN 1 ELSE 0 END) AS heavy_count,
  SUM(CASE WHEN congestion_severity = 'severe'    THEN 1 ELSE 0 END) AS severe_count

FROM LIVE.silver_traffic_readings
GROUP BY
  corridor_id,
  corridor_name,
  day_of_week,
  day_type,
  hour_of_day,
  time_period;

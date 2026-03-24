-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: Route Delay Patterns
-- MAGIC
-- MAGIC Aggregates schedule-vs-actual data to show which routes are
-- MAGIC consistently late and when. Ranks routes by worst delay.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_route_delay_patterns
COMMENT 'Route-level delay patterns — which routes are late and when'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  route_id,
  route_name,
  route_type,
  hour_of_day,
  day_type,

  COUNT(*) AS sample_size,
  ROUND(AVG(delay_seconds), 0) AS avg_delay_seconds,
  ROUND(PERCENTILE(delay_seconds, 0.5), 0) AS median_delay_seconds,
  ROUND(PERCENTILE(delay_seconds, 0.95), 0) AS p95_delay_seconds,
  ROUND(MAX(delay_seconds), 0) AS max_delay_seconds,

  -- What % of observations show a delay > 60 seconds?
  ROUND(
    SUM(CASE WHEN delay_seconds > 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    1
  ) AS pct_delayed,

  -- Schedule adherence: % within 2 minutes of scheduled time
  ROUND(
    SUM(CASE WHEN ABS(delay_seconds) <= 120 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    1
  ) AS pct_on_time,

  -- Average congestion on this route at this hour
  ROUND(AVG(congestion_ratio), 3) AS avg_congestion_ratio,

  -- Dominant congestion severity
  FIRST_VALUE(congestion_severity) AS typical_severity

FROM LIVE.gold_schedule_vs_actual
WHERE delay_seconds IS NOT NULL
GROUP BY route_id, route_name, route_type, hour_of_day, day_type
HAVING COUNT(*) >= 3;  -- minimum sample size

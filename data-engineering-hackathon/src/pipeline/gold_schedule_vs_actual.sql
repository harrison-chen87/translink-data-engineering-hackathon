-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: Schedule vs. Actual Travel Time
-- MAGIC
-- MAGIC The headline table — joins GTFS scheduled travel times with live
-- MAGIC Google Routes API congestion data to show where transit is running
-- MAGIC late and by how much.
-- MAGIC
-- MAGIC **Key insight:** "The 99 B-Line averages 6 minutes late at 5pm on
-- MAGIC weekdays — worst segment is Commercial to Cambie."

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_schedule_vs_actual
COMMENT 'Schedule vs. actual travel time per route segment — the core delay analysis'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH route_scheduled AS (
  -- Average scheduled time per route × departure hour
  SELECT
    route_id,
    route_name,
    route_type,
    departure_hour,
    COUNT(*) AS scheduled_segments,
    ROUND(AVG(scheduled_segment_seconds), 0) AS avg_scheduled_seconds,
    ROUND(PERCENTILE(scheduled_segment_seconds, 0.5), 0) AS median_scheduled_seconds
  FROM LIVE.silver_gtfs_scheduled_times
  WHERE scheduled_segment_seconds > 0
    AND scheduled_segment_seconds < 7200  -- exclude outliers > 2 hours
  GROUP BY route_id, route_name, route_type, departure_hour
),
api_actual AS (
  -- Actual travel times from the Google Routes API, mapped to routes
  -- corridor_id format: route_{name}_seg_{n} → extract route name
  SELECT
    regexp_extract(corridor_id, 'route_(.+)_seg_\\d+', 1) AS route_short_name,
    corridor_name,
    hour_of_day,
    day_type,
    poll_date,
    duration_seconds AS actual_seconds,
    static_duration_seconds,
    congestion_ratio,
    congestion_severity
  FROM LIVE.silver_traffic_readings
  WHERE corridor_id LIKE 'route_%'
)
SELECT
  rs.route_id,
  rs.route_name,
  rs.route_type,
  aa.corridor_name AS segment_name,
  rs.departure_hour AS hour_of_day,
  aa.day_type,
  aa.poll_date AS report_date,

  -- Scheduled
  rs.avg_scheduled_seconds AS scheduled_seconds,
  rs.scheduled_segments,

  -- Actual (from API)
  aa.actual_seconds,
  aa.static_duration_seconds AS free_flow_seconds,
  aa.congestion_ratio,
  aa.congestion_severity,

  -- Delay metrics
  aa.actual_seconds - rs.avg_scheduled_seconds AS delay_seconds,
  ROUND((aa.actual_seconds - rs.avg_scheduled_seconds) / NULLIF(rs.avg_scheduled_seconds, 0) * 100, 1) AS delay_pct

FROM route_scheduled rs
JOIN api_actual aa
  ON rs.route_name = aa.route_short_name
  AND rs.departure_hour = aa.hour_of_day
WHERE aa.actual_seconds IS NOT NULL;

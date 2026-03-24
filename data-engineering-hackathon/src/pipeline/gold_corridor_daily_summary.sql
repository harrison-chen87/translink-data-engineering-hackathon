-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: Corridor Daily Summary (Materialized View)
-- MAGIC
-- MAGIC Daily corridor-level summary combining Google Routes API congestion
-- MAGIC data with GTFS route metadata. Primary analytics table for dashboards.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_corridor_daily_summary
COMMENT 'Daily corridor performance: API congestion data enriched with GTFS route metadata'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH api_daily AS (
  SELECT
    corridor_id,
    corridor_name,
    poll_date,
    day_type,
    COUNT(*)                          AS num_readings,
    ROUND(AVG(congestion_ratio), 3)   AS avg_congestion_ratio,
    ROUND(MAX(congestion_ratio), 3)   AS peak_congestion_ratio,
    ROUND(AVG(duration_seconds), 0)   AS avg_duration_seconds,
    ROUND(AVG(static_duration_seconds), 0) AS avg_static_duration_seconds,
    ROUND(AVG(distance_meters), 0)    AS avg_distance_meters
  FROM LIVE.silver_traffic_readings
  GROUP BY corridor_id, corridor_name, poll_date, day_type
)
SELECT
  a.corridor_id,
  a.corridor_name,
  a.poll_date AS report_date,
  a.day_type,

  -- Route metadata from GTFS (matched via corridor_id → route name)
  r.route_id,
  r.route_short_name AS route_name,
  r.route_long_name,
  CASE
    WHEN r.route_type = 1 THEN 'subway'
    WHEN r.route_type = 2 THEN 'rail'
    WHEN r.route_type = 3 THEN 'bus'
    WHEN r.route_type = 4 THEN 'ferry'
    ELSE 'other'
  END AS route_type,

  -- API congestion metrics
  a.num_readings,
  a.avg_congestion_ratio,
  a.peak_congestion_ratio,
  a.avg_duration_seconds,
  a.avg_static_duration_seconds,
  a.avg_distance_meters

FROM api_daily a
LEFT JOIN LIVE.silver_gtfs_routes r
  ON r.route_short_name = regexp_extract(a.corridor_id, 'route_(.+)_seg_\\d+', 1);

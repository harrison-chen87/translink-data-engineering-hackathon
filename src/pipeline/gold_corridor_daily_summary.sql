-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: Corridor Daily Summary
-- MAGIC
-- MAGIC Combines live Routes API congestion data with EAP sensor counts
-- MAGIC into a single daily corridor-level summary. This is the primary
-- MAGIC analytics table for dashboards and metric views.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_corridor_daily_summary
COMMENT 'Daily corridor performance: congestion from API + volume from EAP sensors'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH api_daily AS (
  SELECT
    corridor_id,
    corridor_name,
    poll_date,
    COUNT(*)                          AS num_readings,
    ROUND(AVG(congestion_ratio), 3)   AS avg_congestion_ratio,
    ROUND(MAX(congestion_ratio), 3)   AS peak_congestion_ratio,
    ROUND(AVG(duration_seconds), 0)   AS avg_duration_seconds,
    ROUND(AVG(static_duration_seconds), 0) AS avg_static_duration_seconds,
    ROUND(AVG(distance_meters), 0)    AS avg_distance_meters,
    -- Most common severity that day
    MODE(congestion_severity)         AS dominant_severity
  FROM LIVE.silver_traffic_readings
  GROUP BY corridor_id, corridor_name, poll_date
),
eap_daily AS (
  SELECT
    corridor_id,
    DATE(count_hour) AS count_date,
    SUM(vehicle_count)                AS total_vehicle_count,
    ROUND(AVG(avg_speed_kmh), 1)      AS avg_speed_kmh,
    ROUND(AVG(occupancy_pct), 1)      AS avg_occupancy_pct,
    COUNT(DISTINCT sensor_id)         AS active_sensors
  FROM LIVE.silver_fact_traffic_counts
  WHERE vehicle_count IS NOT NULL AND vehicle_count >= 0
  GROUP BY corridor_id, DATE(count_hour)
)
SELECT
  COALESCE(a.corridor_id, e.corridor_id)   AS corridor_id,
  a.corridor_name,
  COALESCE(a.poll_date, e.count_date)      AS report_date,
  c.corridor_type,
  c.zone,
  c.speed_limit_kmh,

  -- API metrics
  a.num_readings,
  a.avg_congestion_ratio,
  a.peak_congestion_ratio,
  a.avg_duration_seconds,
  a.avg_static_duration_seconds,
  a.dominant_severity,

  -- EAP metrics
  e.total_vehicle_count,
  e.avg_speed_kmh,
  e.avg_occupancy_pct,
  e.active_sensors,

  -- Derived
  ROUND(e.avg_speed_kmh / NULLIF(c.speed_limit_kmh, 0) * 100, 1) AS speed_ratio_pct

FROM api_daily a
FULL OUTER JOIN eap_daily e
  ON a.corridor_id = e.corridor_id AND a.poll_date = e.count_date
LEFT JOIN LIVE.silver_dim_corridors c
  ON COALESCE(a.corridor_id, e.corridor_id) = c.corridor_id;

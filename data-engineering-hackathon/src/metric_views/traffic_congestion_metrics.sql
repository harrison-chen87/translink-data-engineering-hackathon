-- Metric View: Traffic Congestion Metrics
-- Source: silver_traffic_readings
--
-- REFERENCE ONLY — This file is NOT executed directly by the deployment.
-- Metric views are created by the deploy_metric_views.py notebook.
-- To run this SQL manually, replace ${catalog} and ${schema} with actual values.
--
-- Query examples:
--   SELECT `Corridor Name`, `Congestion Ratio`, `Congestion Severity`
--   FROM catalog.schema.traffic_congestion_metrics
--   WHERE `Time Period` = 'evening_rush';

CREATE OR REPLACE VIEW ${catalog}.${schema}.traffic_congestion_metrics
AS SELECT
  -- Dimensions
  corridor_id,
  corridor_name    AS "Corridor Name",
  hour_of_day      AS "Hour of Day",
  day_type         AS "Day Type",
  time_period      AS "Time Period",
  congestion_severity AS "Congestion Severity",
  poll_date        AS "Report Date",

  -- Measures
  congestion_ratio       AS "Congestion Ratio",
  duration_seconds       AS "Travel Time (seconds)",
  static_duration_seconds AS "Free Flow Time (seconds)",
  distance_meters        AS "Distance (meters)"

FROM ${catalog}.${schema}.silver_traffic_readings;

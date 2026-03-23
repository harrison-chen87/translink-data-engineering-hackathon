-- Metric View: Traffic Congestion Metrics
-- Source: gold_corridor_daily_summary
--
-- Governed business metrics for traffic congestion analysis.
-- Query with MEASURE() syntax:
--   SELECT corridor_id, MEASURE("Avg Congestion Ratio"), MEASURE("Total Vehicle Count")
--   FROM catalog.schema.traffic_congestion_metrics
--   GROUP BY ALL;

CREATE OR REPLACE VIEW ${catalog}.${schema}.traffic_congestion_metrics
AS SELECT
  -- Dimensions
  corridor_id,
  corridor_name    AS "Corridor Name",
  corridor_type    AS "Corridor Type",
  zone             AS "Zone",
  report_date      AS "Report Date",
  dominant_severity AS "Dominant Severity",

  -- Measures
  avg_congestion_ratio       AS "Avg Congestion Ratio",
  peak_congestion_ratio      AS "Peak Congestion Ratio",
  avg_duration_seconds       AS "Avg Travel Time (seconds)",
  avg_static_duration_seconds AS "Avg Free Flow Time (seconds)",
  total_vehicle_count        AS "Total Vehicle Count",
  avg_speed_kmh              AS "Avg Speed (km/h)",
  speed_ratio_pct            AS "Speed Ratio (%)",
  avg_occupancy_pct          AS "Avg Occupancy (%)",
  num_readings               AS "API Readings",
  active_sensors             AS "Active Sensors"

FROM ${catalog}.${schema}.gold_corridor_daily_summary;

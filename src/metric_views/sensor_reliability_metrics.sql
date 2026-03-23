-- Metric View: Sensor Reliability Metrics
-- Source: gold_sensor_reliability
--
-- Governed metrics for monitoring sensor data quality and completeness.
-- Query with MEASURE() syntax:
--   SELECT sensor_type, MEASURE("Avg Completeness (%)"), MEASURE("Total Vehicles")
--   FROM catalog.schema.sensor_reliability_metrics
--   GROUP BY ALL;

CREATE OR REPLACE VIEW ${catalog}.${schema}.sensor_reliability_metrics
AS SELECT
  -- Dimensions
  sensor_id,
  corridor_id,
  corridor_name    AS "Corridor Name",
  sensor_type      AS "Sensor Type",
  location_name    AS "Sensor Location",
  sensor_status    AS "Sensor Status",
  report_date      AS "Report Date",

  -- Measures
  actual_readings        AS "Actual Readings",
  expected_readings      AS "Expected Readings",
  completeness_pct       AS "Completeness (%)",
  null_count_readings    AS "Null Count Readings",
  negative_count_readings AS "Negative Count Readings",
  quality_issue_pct      AS "Quality Issue (%)",
  total_vehicle_count    AS "Total Vehicles",
  avg_speed_kmh          AS "Avg Speed (km/h)"

FROM ${catalog}.${schema}.gold_sensor_reliability;

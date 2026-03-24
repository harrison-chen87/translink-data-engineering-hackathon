-- Metric View: Route Performance Metrics
-- Source: gold_route_delay_patterns
--
-- REFERENCE ONLY — This file is NOT executed directly by the deployment.
-- Metric views are created by the deploy_metric_views.py notebook.
-- To run this SQL manually, replace ${catalog} and ${schema} with actual values.
--
-- Query with MEASURE() syntax:
--   SELECT route_name, MEASURE("Avg Delay (seconds)"), MEASURE("% On Time")
--   FROM catalog.schema.route_performance_metrics
--   GROUP BY ALL;

CREATE OR REPLACE VIEW ${catalog}.${schema}.route_performance_metrics
AS SELECT
  -- Dimensions
  route_id,
  route_name       AS "Route Name",
  route_type       AS "Route Type",
  hour_of_day      AS "Hour of Day",
  day_type         AS "Day Type",

  -- Measures
  avg_delay_seconds      AS "Avg Delay (seconds)",
  median_delay_seconds   AS "Median Delay (seconds)",
  p95_delay_seconds      AS "P95 Delay (seconds)",
  pct_delayed            AS "% Delayed",
  pct_on_time            AS "% On Time",
  avg_congestion_ratio   AS "Avg Congestion Ratio",
  typical_severity       AS "Typical Severity",
  sample_size            AS "Sample Size"

FROM ${catalog}.${schema}.gold_route_delay_patterns;

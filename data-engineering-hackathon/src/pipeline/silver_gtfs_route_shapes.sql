-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: GTFS Route Shapes
-- MAGIC
-- MAGIC Route-level geographic polylines aggregated from shape points.
-- MAGIC Each row is one route variant with total distance and point count.
-- MAGIC Useful for mapping and spatial analysis.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_gtfs_route_shapes
COMMENT 'Route-level polylines with total distance — derived from GTFS shapes'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH latest_snapshot AS (
  SELECT MAX(gtfs_snapshot_date) AS max_date FROM LIVE.bronze_gtfs_shapes
),
shape_summary AS (
  SELECT
    s.shape_id,
    COUNT(*) AS num_points,
    MIN(s.shape_pt_lat) AS min_lat,
    MAX(s.shape_pt_lat) AS max_lat,
    MIN(s.shape_pt_lon) AS min_lon,
    MAX(s.shape_pt_lon) AS max_lon,
    MAX(s.shape_dist_traveled) AS total_distance_km,
    FIRST_VALUE(s.shape_pt_lat) OVER (PARTITION BY s.shape_id ORDER BY s.shape_pt_sequence) AS start_lat,
    FIRST_VALUE(s.shape_pt_lon) OVER (PARTITION BY s.shape_id ORDER BY s.shape_pt_sequence) AS start_lon
  FROM LIVE.bronze_gtfs_shapes s
  CROSS JOIN latest_snapshot ls
  WHERE s.gtfs_snapshot_date = ls.max_date
  GROUP BY s.shape_id, s.shape_pt_lat, s.shape_pt_lon, s.shape_pt_sequence
),
shape_agg AS (
  SELECT
    shape_id,
    COUNT(*) AS num_points,
    MIN(min_lat) AS min_lat,
    MAX(max_lat) AS max_lat,
    MIN(min_lon) AS min_lon,
    MAX(max_lon) AS max_lon,
    MAX(total_distance_km) AS total_distance_km,
    FIRST(start_lat) AS start_lat,
    FIRST(start_lon) AS start_lon
  FROM shape_summary
  GROUP BY shape_id
)
SELECT
  sa.shape_id,
  r.route_id,
  r.route_short_name AS route_name,
  r.route_long_name,
  sa.num_points,
  ROUND(sa.total_distance_km, 2) AS total_distance_km,
  sa.start_lat,
  sa.start_lon,
  sa.min_lat,
  sa.max_lat,
  sa.min_lon,
  sa.max_lon
FROM shape_agg sa
JOIN (
  SELECT DISTINCT route_id, shape_id
  FROM LIVE.bronze_gtfs_trips
) t ON sa.shape_id = t.shape_id
JOIN LIVE.silver_gtfs_routes r ON t.route_id = r.route_id;

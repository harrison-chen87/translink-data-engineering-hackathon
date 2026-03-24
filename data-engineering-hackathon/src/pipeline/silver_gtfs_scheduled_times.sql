-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: GTFS Scheduled Travel Times
-- MAGIC
-- MAGIC Calculates stop-to-stop scheduled travel times from GTFS stop_times.
-- MAGIC Uses `LEAD()` to get the next stop's arrival time and computes the
-- MAGIC scheduled segment duration. Joins with trips and routes to get
-- MAGIC route-level context.
-- MAGIC
-- MAGIC This is the "expected" half of the schedule-vs-reality analysis.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_gtfs_scheduled_times
COMMENT 'Stop-to-stop scheduled travel times derived from GTFS stop_times'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH latest_snapshot AS (
  -- Use only the most recent GTFS snapshot for scheduled times
  SELECT MAX(gtfs_snapshot_date) AS max_date FROM LIVE.bronze_gtfs_stop_times
),
stop_segments AS (
  SELECT
    st.trip_id,
    st.stop_id AS origin_stop_id,
    st.departure_time,
    st.stop_sequence,
    st.shape_dist_traveled AS origin_dist,
    LEAD(st.stop_id) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS dest_stop_id,
    LEAD(st.arrival_time) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS dest_arrival_time,
    LEAD(st.stop_sequence) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS dest_stop_sequence,
    LEAD(st.shape_dist_traveled) OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS dest_dist,
    st.gtfs_snapshot_date
  FROM LIVE.bronze_gtfs_stop_times st
  CROSS JOIN latest_snapshot ls
  WHERE st.gtfs_snapshot_date = ls.max_date
)
SELECT
  t.route_id,
  r.route_short_name AS route_name,
  r.route_long_name,
  CASE
    WHEN r.route_type = 1 THEN 'subway'
    WHEN r.route_type = 2 THEN 'rail'
    WHEN r.route_type = 3 THEN 'bus'
    WHEN r.route_type = 4 THEN 'ferry'
    ELSE 'other'
  END AS route_type,
  t.trip_headsign,
  t.direction_id,
  t.service_id,

  ss.origin_stop_id,
  os.stop_name AS origin_stop_name,
  os.stop_lat AS origin_lat,
  os.stop_lon AS origin_lon,
  ss.dest_stop_id,
  ds.stop_name AS dest_stop_name,
  ds.stop_lat AS dest_lat,
  ds.stop_lon AS dest_lon,

  ss.stop_sequence,
  ss.departure_time,
  ss.dest_arrival_time AS arrival_time,

  -- Parse HH:MM:SS to seconds and compute segment duration
  -- GTFS times can exceed 24:00:00 for overnight trips
  (
    CAST(SPLIT(ss.dest_arrival_time, ':')[0] AS INT) * 3600 +
    CAST(SPLIT(ss.dest_arrival_time, ':')[1] AS INT) * 60 +
    CAST(SPLIT(ss.dest_arrival_time, ':')[2] AS INT)
  ) - (
    CAST(SPLIT(ss.departure_time, ':')[0] AS INT) * 3600 +
    CAST(SPLIT(ss.departure_time, ':')[1] AS INT) * 60 +
    CAST(SPLIT(ss.departure_time, ':')[2] AS INT)
  ) AS scheduled_segment_seconds,

  -- Distance between stops (km)
  ROUND((COALESCE(ss.dest_dist, 0) - COALESCE(ss.origin_dist, 0)), 3) AS segment_distance_km,

  -- Hour bucket for the departure (for time-of-day analysis)
  CAST(SPLIT(ss.departure_time, ':')[0] AS INT) AS departure_hour,

  ss.gtfs_snapshot_date

FROM stop_segments ss
JOIN LIVE.bronze_gtfs_trips t
  ON ss.trip_id = t.trip_id AND ss.gtfs_snapshot_date = t.gtfs_snapshot_date
JOIN LIVE.silver_gtfs_routes r
  ON t.route_id = r.route_id
LEFT JOIN LIVE.silver_gtfs_stops os
  ON ss.origin_stop_id = os.stop_id
LEFT JOIN LIVE.silver_gtfs_stops ds
  ON ss.dest_stop_id = ds.stop_id
WHERE ss.dest_stop_id IS NOT NULL  -- exclude last stop of each trip
  AND ss.departure_time IS NOT NULL
  AND ss.dest_arrival_time IS NOT NULL;

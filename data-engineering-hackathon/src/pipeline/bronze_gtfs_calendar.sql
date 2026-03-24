-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: GTFS Calendar
-- MAGIC
-- MAGIC Service patterns defining which days each service_id is active.
-- MAGIC Used to determine weekday vs weekend schedules.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_gtfs_calendar
COMMENT 'Raw GTFS service calendar — weekday/weekend service patterns'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  CAST(service_id AS STRING) AS service_id,
  CAST(monday AS INT) AS monday,
  CAST(tuesday AS INT) AS tuesday,
  CAST(wednesday AS INT) AS wednesday,
  CAST(thursday AS INT) AS thursday,
  CAST(friday AS INT) AS friday,
  CAST(saturday AS INT) AS saturday,
  CAST(sunday AS INT) AS sunday,
  start_date,
  end_date,
  regexp_extract(_metadata.file_path, '(\\d{4}-\\d{2}-\\d{2})', 1) AS gtfs_snapshot_date,
  _metadata.file_name AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/gtfs/extracted/*/calendar.txt',
  format => 'csv',
  header => true
);

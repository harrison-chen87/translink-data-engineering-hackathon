# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Metric Views
# MAGIC
# MAGIC Creates metric views for governed business metrics.
# MAGIC Run this notebook after the pipeline has completed at least one full run.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Traffic Congestion Metrics

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.traffic_congestion_metrics
AS SELECT
  corridor_id,
  corridor_name    AS `Corridor Name`,
  corridor_type    AS `Corridor Type`,
  zone             AS `Zone`,
  report_date      AS `Report Date`,
  dominant_severity AS `Dominant Severity`,
  avg_congestion_ratio       AS `Avg Congestion Ratio`,
  peak_congestion_ratio      AS `Peak Congestion Ratio`,
  avg_duration_seconds       AS `Avg Travel Time (seconds)`,
  avg_static_duration_seconds AS `Avg Free Flow Time (seconds)`,
  total_vehicle_count        AS `Total Vehicle Count`,
  avg_speed_kmh              AS `Avg Speed (km/h)`,
  speed_ratio_pct            AS `Speed Ratio (%)`,
  avg_occupancy_pct          AS `Avg Occupancy (%)`,
  num_readings               AS `API Readings`,
  active_sensors             AS `Active Sensors`
FROM {catalog}.{schema}.gold_corridor_daily_summary
""")
print("Created traffic_congestion_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sensor Reliability Metrics

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.sensor_reliability_metrics
AS SELECT
  sensor_id,
  corridor_id,
  corridor_name    AS `Corridor Name`,
  sensor_type      AS `Sensor Type`,
  location_name    AS `Sensor Location`,
  sensor_status    AS `Sensor Status`,
  report_date      AS `Report Date`,
  actual_readings        AS `Actual Readings`,
  expected_readings      AS `Expected Readings`,
  completeness_pct       AS `Completeness (%)`,
  null_count_readings    AS `Null Count Readings`,
  negative_count_readings AS `Negative Count Readings`,
  quality_issue_pct      AS `Quality Issue (%)`,
  total_vehicle_count    AS `Total Vehicles`,
  avg_speed_kmh          AS `Avg Speed (km/h)`
FROM {catalog}.{schema}.gold_sensor_reliability
""")
print("Created sensor_reliability_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

views = spark.sql(f"SHOW VIEWS IN {catalog}.{schema}").filter("viewName LIKE '%metrics%'").collect()
print(f"\nMetric views deployed: {len(views)}")
for v in views:
    print(f"  - {v.viewName}")

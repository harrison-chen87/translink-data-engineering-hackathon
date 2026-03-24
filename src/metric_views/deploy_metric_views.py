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
  hour_of_day      AS `Hour of Day`,
  day_type         AS `Day Type`,
  time_period      AS `Time Period`,
  congestion_severity AS `Congestion Severity`,
  congestion_ratio AS `Congestion Ratio`,
  duration_seconds AS `Travel Time (seconds)`,
  static_duration_seconds AS `Free Flow Time (seconds)`,
  poll_date        AS `Report Date`
FROM {catalog}.{schema}.silver_traffic_readings
""")
print("Created traffic_congestion_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Route Performance Metrics

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.route_performance_metrics
AS SELECT
  route_id,
  route_name       AS `Route Name`,
  route_type       AS `Route Type`,
  hour_of_day      AS `Hour of Day`,
  day_type         AS `Day Type`,
  avg_delay_seconds      AS `Avg Delay (seconds)`,
  median_delay_seconds   AS `Median Delay (seconds)`,
  p95_delay_seconds      AS `P95 Delay (seconds)`,
  pct_delayed            AS `% Delayed`,
  pct_on_time            AS `% On Time`,
  avg_congestion_ratio   AS `Avg Congestion Ratio`,
  typical_severity       AS `Typical Severity`,
  sample_size            AS `Sample Size`
FROM {catalog}.{schema}.gold_route_delay_patterns
""")
print("Created route_performance_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

views = spark.sql(f"SHOW VIEWS IN {catalog}.{schema}").filter("viewName LIKE '%metrics%'").collect()
print(f"\nMetric views deployed: {len(views)}")
for v in views:
    print(f"  - {v.viewName}")

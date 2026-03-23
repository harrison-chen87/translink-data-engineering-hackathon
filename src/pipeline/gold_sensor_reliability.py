# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Sensor Reliability (Python DataFrame)
# MAGIC
# MAGIC **This notebook is intentionally written in Python** to demonstrate that
# MAGIC Spark Declarative Pipelines lets you freely mix SQL and Python notebooks
# MAGIC in the same pipeline. The upstream bronze and silver tables are all SQL —
# MAGIC this gold table reads from them using `dlt.read()` and builds the output
# MAGIC with PySpark DataFrames.
# MAGIC
# MAGIC **What this shows:**
# MAGIC - `@dlt.table` decorator to define a managed table
# MAGIC - `dlt.read("table_name")` to reference upstream pipeline tables
# MAGIC - `@dlt.expect_or_drop` for data quality checks in Python
# MAGIC - Standard PySpark DataFrame joins, aggregations, and window functions
# MAGIC - All of this co-existing with SQL streaming tables and Auto CDC

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sensor Reliability Table
# MAGIC
# MAGIC Tracks data completeness and quality per sensor per day.
# MAGIC Joins the SCD Type 2 sensor dimension (current version only)
# MAGIC with fact traffic counts.

# COMMAND ----------

@dlt.table(
    comment="Daily sensor data completeness and quality metrics (Python DataFrame)",
    table_properties={"quality": "gold"},
)
@dlt.expect_or_drop("valid_sensor", "sensor_id IS NOT NULL")
@dlt.expect("reasonable_completeness", "completeness_pct <= 100.0")
def gold_sensor_reliability():
    # Read upstream tables — these are SQL-defined tables in the same pipeline
    facts = dlt.read("silver_fact_traffic_counts")
    sensors = dlt.read("silver_dim_sensors")
    corridors = dlt.read("silver_dim_corridors")

    # Current sensor version only (SCD Type 2 — __END_AT IS NULL)
    current_sensors = sensors.filter(F.col("__END_AT").isNull())

    # Daily aggregation per sensor
    daily_stats = (
        facts
        .withColumn("report_date", F.to_date("count_hour"))
        .groupBy("sensor_id", "corridor_id", "report_date")
        .agg(
            F.count("*").alias("actual_readings"),
            F.lit(24).alias("expected_readings"),
            F.round(F.count("*") / 24.0 * 100, 1).alias("completeness_pct"),
            # Quality issues
            F.sum(F.when(F.col("vehicle_count").isNull(), 1).otherwise(0)).alias("null_count_readings"),
            F.sum(F.when(F.col("vehicle_count") < 0, 1).otherwise(0)).alias("negative_count_readings"),
            F.round(
                F.sum(
                    F.when(
                        F.col("vehicle_count").isNull() | (F.col("vehicle_count") < 0), 1
                    ).otherwise(0)
                ) / F.count("*") * 100,
                1
            ).alias("quality_issue_pct"),
            # Traffic stats (non-null, non-negative only)
            F.sum(F.when(F.col("vehicle_count") >= 0, F.col("vehicle_count"))).alias("total_vehicle_count"),
            F.round(
                F.avg(F.when(F.col("vehicle_count") >= 0, F.col("avg_speed_kmh"))), 1
            ).alias("avg_speed_kmh"),
        )
    )

    # Join sensor and corridor dimensions
    result = (
        daily_stats
        .join(current_sensors.select(
            "sensor_id", "sensor_type", "location_name",
            F.col("status").alias("sensor_status"),
            F.col("corridor_id").alias("sensor_corridor_id"),
        ), on="sensor_id", how="left")
        .join(corridors.select("corridor_id", "corridor_name"),
              on=F.col("sensor_corridor_id") == corridors["corridor_id"],
              how="left")
        .select(
            daily_stats["sensor_id"],
            daily_stats["corridor_id"],
            "corridor_name",
            "sensor_type",
            "location_name",
            "sensor_status",
            "report_date",
            "actual_readings",
            "expected_readings",
            "completeness_pct",
            "null_count_readings",
            "negative_count_readings",
            "quality_issue_pct",
            "total_vehicle_count",
            "avg_speed_kmh",
        )
    )

    return result

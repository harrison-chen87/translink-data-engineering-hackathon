# Databricks notebook source
# MAGIC %md
# MAGIC # Travel Time Prediction: 99 B-Line
# MAGIC
# MAGIC Predicts travel time along the **99 B-Line corridor** (Commercial-Broadway → UBC Exchange)
# MAGIC based on time of day, day of week, and seasonal patterns.
# MAGIC
# MAGIC ## How It Works
# MAGIC
# MAGIC 1. Reads historical traffic data from the **gold_corridor_daily_summary** and
# MAGIC    **silver_traffic_readings** tables built by the SDP pipeline
# MAGIC 2. Calculates congestion patterns by hour-of-day and day-of-week
# MAGIC 3. Applies seasonal adjustments (September back-to-school = higher congestion)
# MAGIC 4. Predicts travel time for a given corridor, day, and time
# MAGIC
# MAGIC ## Data Sources
# MAGIC
# MAGIC | Table | What It Provides |
# MAGIC |-------|-----------------|
# MAGIC | `silver_traffic_readings` | Per-poll congestion ratios with hour/day dimensions |
# MAGIC | `gold_corridor_daily_summary` | Daily aggregated congestion with route metadata |
# MAGIC | `silver_gtfs_routes` | Route metadata (route type, name) |

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "traffic_hackathon_de", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Hourly Congestion Profiles
# MAGIC
# MAGIC For each corridor, calculate the average congestion ratio by hour-of-day
# MAGIC and day-of-week. This gives us a "typical Monday at 9am" profile.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

hourly_profiles = (
    spark.table(f"{catalog}.{schema}.silver_traffic_readings")
    .withColumn("hour", F.hour("polled_at"))
    .withColumn("day_of_week", F.dayofweek("polled_at"))  # 1=Sun, 2=Mon, ..., 7=Sat
    .withColumn("month", F.month("polled_at"))
    .groupBy("corridor_id", "corridor_name", "day_of_week", "hour")
    .agg(
        F.avg("congestion_ratio").alias("avg_congestion_ratio"),
        F.avg("duration_seconds").alias("avg_duration_seconds"),
        F.avg("static_duration_seconds").alias("avg_static_duration_seconds"),
        F.stddev("congestion_ratio").alias("stddev_congestion_ratio"),
        F.count("*").alias("sample_count"),
    )
)

hourly_profiles.cache()
display(hourly_profiles.orderBy("corridor_id", "day_of_week", "hour"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: September Seasonal Adjustment
# MAGIC
# MAGIC September is back-to-school season in Vancouver. School traffic increases
# MAGIC congestion on arterial routes, especially during morning rush (7-9am).
# MAGIC
# MAGIC We calculate a seasonal multiplier by comparing September congestion
# MAGIC to the annual average.

# COMMAND ----------

seasonal_factors = (
    spark.table(f"{catalog}.{schema}.silver_traffic_readings")
    .withColumn("month", F.month("polled_at"))
    .withColumn("hour", F.hour("polled_at"))
    .groupBy("corridor_id", "month")
    .agg(F.avg("congestion_ratio").alias("monthly_avg_congestion"))
)

annual_avg = (
    seasonal_factors
    .groupBy("corridor_id")
    .agg(F.avg("monthly_avg_congestion").alias("annual_avg_congestion"))
)

september_adjustment = (
    seasonal_factors
    .filter(F.col("month") == 9)
    .join(annual_avg, "corridor_id")
    .withColumn(
        "seasonal_multiplier",
        F.col("monthly_avg_congestion") / F.col("annual_avg_congestion")
    )
    .select("corridor_id", "seasonal_multiplier")
)

display(september_adjustment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Predict Travel Time
# MAGIC
# MAGIC **Scenario:** Monday at 9:00 AM, non-holiday, September
# MAGIC
# MAGIC **Corridor:** 99 B-Line (Commercial-Broadway → UBC Exchange) = `corridor_16`
# MAGIC
# MAGIC If we don't have enough data for corridor_16 yet (it was just added),
# MAGIC we'll use corridor_02 (Broadway Commercial to Arbutus) as a proxy and
# MAGIC scale by distance ratio.

# COMMAND ----------

TARGET_CORRIDOR = "corridor_16"
PROXY_CORRIDOR = "corridor_02"  # Broadway (Commercial to Arbutus) — overlapping route
TARGET_DAY_OF_WEEK = 2  # Monday (1=Sun, 2=Mon)
TARGET_HOUR = 9

# 99 B-Line is ~13 km; corridor_02 is ~7.2 km (Commercial to Arbutus)
# Scale factor for extrapolating from partial corridor to full route
DISTANCE_SCALE_FACTOR = 13.0 / 7.2

# COMMAND ----------

# Check if we have data for the 99 B-Line corridor yet
bline_data_count = (
    hourly_profiles
    .filter(F.col("corridor_id") == TARGET_CORRIDOR)
    .count()
)

if bline_data_count > 0:
    print(f"Using direct data for {TARGET_CORRIDOR} ({bline_data_count} hourly profiles)")
    corridor_to_use = TARGET_CORRIDOR
    scale_factor = 1.0
else:
    print(f"No data for {TARGET_CORRIDOR} yet — using {PROXY_CORRIDOR} as proxy")
    print(f"Distance scale factor: {DISTANCE_SCALE_FACTOR:.2f}x")
    corridor_to_use = PROXY_CORRIDOR
    scale_factor = DISTANCE_SCALE_FACTOR

# COMMAND ----------

# Get the hourly profile for our target time
prediction_profile = (
    hourly_profiles
    .filter(
        (F.col("corridor_id") == corridor_to_use) &
        (F.col("day_of_week") == TARGET_DAY_OF_WEEK) &
        (F.col("hour") == TARGET_HOUR)
    )
    .collect()
)

if not prediction_profile:
    print("No data available for this corridor/time combination.")
    print("Deploy the pipeline and collect data first, then re-run this notebook.")
    dbutils.notebook.exit("NO_DATA")

profile = prediction_profile[0]

# Get September adjustment
sept_adj = (
    september_adjustment
    .filter(F.col("corridor_id") == corridor_to_use)
    .collect()
)

seasonal_mult = sept_adj[0]["seasonal_multiplier"] if sept_adj else 1.10  # Default 10% increase for September

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# Calculate predicted travel time
base_static_seconds = profile["avg_static_duration_seconds"] * scale_factor
base_congested_seconds = profile["avg_duration_seconds"] * scale_factor
september_congested_seconds = base_congested_seconds * seasonal_mult

congestion_ratio = profile["avg_congestion_ratio"] * seasonal_mult
confidence_range = profile["stddev_congestion_ratio"] * scale_factor if profile["stddev_congestion_ratio"] else 0

low_estimate = september_congested_seconds - (confidence_range * base_static_seconds)
high_estimate = september_congested_seconds + (confidence_range * base_static_seconds)

print("=" * 60)
print("TRAVEL TIME PREDICTION")
print("=" * 60)
print(f"Route:          99 B-Line (Commercial-Broadway → UBC Exchange)")
print(f"Day:            Monday (non-holiday)")
print(f"Time:           9:00 AM")
print(f"Month:          September")
print(f"")
print(f"Free-flow time: {base_static_seconds / 60:.1f} minutes")
print(f"Predicted time: {september_congested_seconds / 60:.1f} minutes")
print(f"Range:          {max(low_estimate, base_static_seconds) / 60:.1f} – {high_estimate / 60:.1f} minutes")
print(f"")
print(f"Congestion:     {congestion_ratio:.2f}x free-flow")
print(f"Sept. factor:   {seasonal_mult:.2f}x annual average")
print(f"Data source:    {corridor_to_use} ({'direct' if scale_factor == 1.0 else f'proxy, scaled {scale_factor:.2f}x'})")
print(f"Sample size:    {profile['sample_count']} observations")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Full Day Profile
# MAGIC
# MAGIC Show predicted travel time across all hours for a Monday in September.

# COMMAND ----------

from pyspark.sql.types import FloatType

monday_profile = (
    hourly_profiles
    .filter(
        (F.col("corridor_id") == corridor_to_use) &
        (F.col("day_of_week") == TARGET_DAY_OF_WEEK)
    )
    .withColumn("predicted_minutes",
        (F.col("avg_duration_seconds") * scale_factor * seasonal_mult / 60).cast(FloatType())
    )
    .withColumn("freeflow_minutes",
        (F.col("avg_static_duration_seconds") * scale_factor / 60).cast(FloatType())
    )
    .select("hour", "freeflow_minutes", "predicted_minutes", "avg_congestion_ratio", "sample_count")
    .orderBy("hour")
)

display(monday_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpreting the Results
# MAGIC
# MAGIC - **Free-flow time** = travel time with zero traffic (static_duration from Google Routes API)
# MAGIC - **Predicted time** = free-flow × congestion_ratio × seasonal_multiplier
# MAGIC - **Congestion ratio** of 1.5 means the trip takes 50% longer than free-flow
# MAGIC - **September factor** accounts for school-year traffic increase on Broadway
# MAGIC
# MAGIC The prediction improves as more data is collected. With 90 days of polling
# MAGIC at 15-minute intervals, each hour-of-day × day-of-week cell has ~90 samples.
# MAGIC
# MAGIC ### Limitations
# MAGIC
# MAGIC - This predicts **car travel time**, not bus travel time. Bus stops, passenger
# MAGIC   boarding, and dedicated bus lanes (on parts of Broadway) affect actual 99 B-Line
# MAGIC   trip duration. Add ~5-8 minutes for stops.
# MAGIC - The model assumes congestion patterns are stationary — construction, road closures,
# MAGIC   and special events are not captured.
# MAGIC - If using the proxy corridor (corridor_02), the western segment (Arbutus → UBC)
# MAGIC   may have different congestion patterns than the eastern segment.

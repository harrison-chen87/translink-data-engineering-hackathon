# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Build Route Segments for Traffic Queries
# MAGIC
# MAGIC Takes GTFS shapes (route geometries) and builds origin/destination
# MAGIC waypoint pairs along each transit route for Google Routes API queries.

# COMMAND ----------

CATALOG = "serverless_stable_ps58um_catalog"
SCHEMA = "transit_congestion_map"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read the latest shapes data
latest_feed = (
    spark.table(f"{CATALOG}.{SCHEMA}.gtfs_shapes")
    .select("feed_date").distinct()
    .orderBy(F.desc("feed_date"))
    .first()["feed_date"]
)
print(f"Using latest feed: {latest_feed}")

shapes = (
    spark.table(f"{CATALOG}.{SCHEMA}.gtfs_shapes")
    .filter(F.col("feed_date") == latest_feed)
    .orderBy("shape_id", "shape_pt_sequence")
)

print(f"Total shape points: {shapes.count()}")
print(f"Unique shapes: {shapes.select('shape_id').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample shape points to create route segments
# MAGIC
# MAGIC For each shape, take every Nth point to create origin/destination pairs.
# MAGIC This reduces API calls while still capturing congestion along the route.

# COMMAND ----------

# Sample every Nth point per shape to create segments
SAMPLE_EVERY_N = 10  # Adjust based on desired granularity vs API cost

w = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")

sampled = (
    shapes
    .withColumn("row_num", F.row_number().over(w))
    .filter((F.col("row_num") % SAMPLE_EVERY_N == 1) | (F.col("row_num") == 1))
    .select("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence")
)

# Create segment pairs: each point -> next point along the shape
w2 = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")

segments = (
    sampled
    .withColumn("next_lat", F.lead("shape_pt_lat").over(w2))
    .withColumn("next_lon", F.lead("shape_pt_lon").over(w2))
    .filter(F.col("next_lat").isNotNull())
    .select(
        "shape_id",
        F.col("shape_pt_lat").alias("origin_lat"),
        F.col("shape_pt_lon").alias("origin_lon"),
        F.col("next_lat").alias("dest_lat"),
        F.col("next_lon").alias("dest_lon"),
        "shape_pt_sequence",
    )
)

print(f"Route segments to query: {segments.count()}")

# COMMAND ----------

# Join with routes to get route info
trips = spark.table(f"{CATALOG}.{SCHEMA}.gtfs_trips").filter(F.col("feed_date") == latest_feed)
routes = spark.table(f"{CATALOG}.{SCHEMA}.gtfs_routes").filter(F.col("feed_date") == latest_feed)

# Get unique shape_id -> route mapping
shape_routes = (
    trips
    .select("shape_id", "route_id").distinct()
    .join(routes.select("route_id", "route_short_name", "route_long_name", "route_type"), "route_id")
)

segments_with_routes = segments.join(shape_routes, "shape_id")

# COMMAND ----------

# Save segments table
segments_with_routes.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.route_segments"
)

print(f"Saved {segments_with_routes.count()} segments to {CATALOG}.{SCHEMA}.route_segments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export waypoints JSON for Google Routes API DataSource
# MAGIC
# MAGIC Creates a JSON file of origin/destination pairs that the Google Routes
# MAGIC DataSource can consume in batch mode.

# COMMAND ----------

import json

waypoints = (
    segments_with_routes
    .select("origin_lat", "origin_lon", "dest_lat", "dest_lon", "shape_id", "route_short_name")
    .collect()
)

waypoints_list = [
    {
        "origin_lat": float(w["origin_lat"]),
        "origin_lon": float(w["origin_lon"]),
        "dest_lat": float(w["dest_lat"]),
        "dest_lon": float(w["dest_lon"]),
        "shape_id": w["shape_id"],
        "route_name": w["route_short_name"],
    }
    for w in waypoints
]

# Save to Volume
waypoints_path = f"/Volumes/{CATALOG}/{SCHEMA}/gtfs_data/route_waypoints.json"
with open(waypoints_path, "w") as f:
    json.dump(waypoints_list, f)

print(f"Exported {len(waypoints_list)} waypoints to {waypoints_path}")

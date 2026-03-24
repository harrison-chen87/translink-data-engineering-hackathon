# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Sync Delta → Lakebase
# MAGIC
# MAGIC Syncs routes, stops, and route segments from Delta tables to Lakebase
# MAGIC (autoscale Postgres) for sub-100ms app serving. Run after GTFS ingest.

# COMMAND ----------

CATALOG = "serverless_stable_ps58um_catalog"
SCHEMA = "transit_congestion_map"
LAKEBASE_PROJECT = "transit-cache"
LAKEBASE_ENDPOINT = "projects/transit-cache/branches/production/endpoints/primary"
LAKEBASE_HOST = "ep-round-tree-d2ped0gk.database.us-east-1.cloud.databricks.com"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Lakebase

# COMMAND ----------

import psycopg2
import psycopg2.extras
import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
username = w.current_user.me().user_name

# Generate Lakebase credential via REST API (SDK w.postgres may not be available)
cred = w.api_client.do(
    "POST",
    "/api/2.0/postgres/credentials",
    body={"endpoint": LAKEBASE_ENDPOINT},
)

pg = psycopg2.connect(
    host=LAKEBASE_HOST, port=5432,
    dbname="databricks_postgres",
    user=username,
    password=cred["token"],
    sslmode="require", connect_timeout=15,
)
pg.autocommit = True
print("Connected to Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get latest feed date

# COMMAND ----------

latest_feed = (
    spark.table(f"{CATALOG}.{SCHEMA}.gtfs_shapes")
    .select("feed_date").distinct()
    .orderBy("feed_date", ascending=False)
    .first()["feed_date"]
)
print(f"Latest GTFS feed: {latest_feed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync routes to Lakebase
# MAGIC
# MAGIC Execute the heavy JOIN + COLLECT_LIST once in Spark, then write to Postgres.

# COMMAND ----------

from pyspark.sql import functions as F

# Build route geometries in Spark (the expensive query)
routes_df = spark.sql(f"""
    SELECT DISTINCT
        s.shape_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        r.route_color,
        COLLECT_LIST(STRUCT(s.shape_pt_lat, s.shape_pt_lon, s.shape_pt_sequence)) AS points
    FROM {CATALOG}.{SCHEMA}.gtfs_shapes s
    JOIN (
        SELECT DISTINCT shape_id, route_id
        FROM {CATALOG}.{SCHEMA}.gtfs_trips
        WHERE feed_date = '{latest_feed}'
    ) t ON s.shape_id = t.shape_id
    JOIN {CATALOG}.{SCHEMA}.gtfs_routes r ON t.route_id = r.route_id
    WHERE s.feed_date = '{latest_feed}'
      AND r.feed_date = '{latest_feed}'
    GROUP BY s.shape_id, r.route_short_name, r.route_long_name, r.route_type, r.route_color
""")

routes = routes_df.collect()
print(f"Routes to sync: {len(routes)}")

# COMMAND ----------

# Write to Lakebase
cur = pg.cursor()
cur.execute("TRUNCATE TABLE routes")

batch = []
for row in routes:
    points = sorted(row["points"], key=lambda p: p[2])  # sort by sequence
    coordinates = [[float(p[1]), float(p[0])] for p in points]  # [lon, lat] for deck.gl
    batch.append((
        int(row["shape_id"]),
        row["route_short_name"],
        row["route_long_name"],
        int(row["route_type"]) if row["route_type"] is not None else None,
        row["route_color"] or "0000FF",
        json.dumps(coordinates),
        latest_feed,
    ))

psycopg2.extras.execute_batch(
    cur,
    "INSERT INTO routes (shape_id, route_short_name, route_long_name, route_type, route_color, coordinates, feed_date) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
    batch, page_size=100,
)

cur.execute("SELECT COUNT(*) FROM routes")
print(f"Routes synced: {cur.fetchone()[0]}")
cur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync stops to Lakebase

# COMMAND ----------

latest_stops_feed = (
    spark.table(f"{CATALOG}.{SCHEMA}.gtfs_stops")
    .select("feed_date").distinct()
    .orderBy("feed_date", ascending=False)
    .first()["feed_date"]
)
print(f"Latest stops feed: {latest_stops_feed}")

stops_df = spark.sql(f"""
    SELECT stop_id, stop_name, stop_lat, stop_lon, location_type
    FROM {CATALOG}.{SCHEMA}.gtfs_stops
    WHERE feed_date = '{latest_stops_feed}'
""")

stops = stops_df.collect()
print(f"Stops to sync: {len(stops)}")

cur = pg.cursor()
cur.execute("TRUNCATE TABLE stops")

batch = []
for row in stops:
    batch.append((
        row["stop_id"],
        row["stop_name"],
        float(row["stop_lat"]),
        float(row["stop_lon"]),
        int(row["location_type"]) if row["location_type"] is not None else None,
        latest_stops_feed,
    ))

psycopg2.extras.execute_batch(
    cur,
    "INSERT INTO stops (stop_id, stop_name, stop_lat, stop_lon, location_type, feed_date) "
    "VALUES (%s, %s, %s, %s, %s, %s)",
    batch, page_size=500,
)

cur.execute("SELECT COUNT(*) FROM stops")
print(f"Stops synced: {cur.fetchone()[0]}")
cur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync route segments to Lakebase

# COMMAND ----------

segments_df = spark.sql(f"""
    SELECT shape_id, origin_lat, origin_lon, dest_lat, dest_lon, route_short_name
    FROM {CATALOG}.{SCHEMA}.route_segments
""")

segments = segments_df.collect()
print(f"Segments to sync: {len(segments)}")

cur = pg.cursor()
cur.execute("TRUNCATE TABLE route_segments")

batch = []
for row in segments:
    batch.append((
        int(row["shape_id"]) if row["shape_id"] is not None else None,
        float(row["origin_lat"]),
        float(row["origin_lon"]),
        float(row["dest_lat"]),
        float(row["dest_lon"]),
        row["route_short_name"],
    ))

psycopg2.extras.execute_batch(
    cur,
    "INSERT INTO route_segments (shape_id, origin_lat, origin_lon, dest_lat, dest_lon, route_short_name) "
    "VALUES (%s, %s, %s, %s, %s, %s)",
    batch, page_size=500,
)

cur.execute("SELECT COUNT(*) FROM route_segments")
print(f"Segments synced: {cur.fetchone()[0]}")
cur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh congestion cache
# MAGIC
# MAGIC Re-query Google Routes API for all previously cached hour/day combos.

# COMMAND ----------

cur = pg.cursor()
cur.execute("SELECT cache_key, hour_of_day, day_of_week FROM congestion_cache")
cached_entries = cur.fetchall()
cur.close()
print(f"Congestion cache entries to refresh: {len(cached_entries)}")

if cached_entries:
    print("Clearing stale congestion cache — will be rebuilt on-demand by the app")
    cur = pg.cursor()
    cur.execute("TRUNCATE TABLE congestion_cache")
    cur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

cur = pg.cursor()
for table in ["routes", "stops", "route_segments", "congestion_cache"]:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"  {table}: {cur.fetchone()[0]} rows")
cur.close()
pg.close()

print(f"\nSync complete for feed_date={latest_feed}")

dbutils.notebook.exit(json.dumps({
    "feed_date": latest_feed,
    "routes": len(routes),
    "stops": len(stops),
    "segments": len(segments),
}))

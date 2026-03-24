# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest GTFS Static Data
# MAGIC
# MAGIC Downloads TransLink GTFS static zip files directly to a Volume,
# MAGIC extracts CSVs, and loads into Delta with spark.read.csv().
# MAGIC Skips feeds already ingested (idempotent).

# COMMAND ----------

CATALOG = "serverless_stable_ps58um_catalog"
SCHEMA = "transit_congestion_map"
VOLUME = "gtfs_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

import urllib.request
import zipfile
import os
import re

GTFS_DATES = [
    "2026-02-06",
    "2026-02-20",
    "2026-03-06",
    "2026-03-13",
    "2026-03-20",
]

BASE_URL = "https://gtfs-static.translink.ca/gtfs/History"
EXTRACT_DIR = f"{VOLUME_PATH}/extracted"
os.makedirs(EXTRACT_DIR, exist_ok=True)

GTFS_TABLES = [
    "agency", "calendar", "calendar_dates", "routes",
    "shapes", "stops", "stop_times", "trips", "transfers", "feed_info",
]

# COMMAND ----------

# Download and extract (skip existing)
for date in GTFS_DATES:
    filename = f"google_transit_{date}.zip"
    zip_dest = f"{VOLUME_PATH}/{filename}"
    feed_dir = f"{EXTRACT_DIR}/{date}"

    if not os.path.exists(zip_dest):
        url = f"{BASE_URL}/{date}/google_transit.zip"
        print(f"Downloading {url}")
        urllib.request.urlretrieve(url, zip_dest)

    if not os.path.exists(feed_dir) or not any(f.endswith(".txt") for f in os.listdir(feed_dir)):
        os.makedirs(feed_dir, exist_ok=True)
        print(f"Extracting {filename}")
        with zipfile.ZipFile(zip_dest, "r") as zf:
            zf.extractall(feed_dir)
    else:
        print(f"{date}: already extracted")

# COMMAND ----------

from pyspark.sql.functions import lit

for table_name in GTFS_TABLES:
    target_table = f"{CATALOG}.{SCHEMA}.gtfs_{table_name}"

    # Get already-ingested feed dates
    try:
        existing_dates = set(
            row["feed_date"]
            for row in spark.table(target_table).select("feed_date").distinct().collect()
        )
    except Exception:
        existing_dates = set()

    for date in GTFS_DATES:
        if date in existing_dates:
            continue

        csv_path = f"{EXTRACT_DIR}/{date}/{table_name}.txt"
        if not os.path.exists(csv_path):
            continue

        try:
            df = (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_path)
                .withColumn("feed_date", lit(date))
            )

            (df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target_table))

            print(f"  {table_name}/{date}: ingested")
        except Exception as e:
            print(f"  {table_name}/{date}: ERROR - {e}")

# COMMAND ----------

for table_name in GTFS_TABLES:
    target = f"{CATALOG}.{SCHEMA}.gtfs_{table_name}"
    try:
        count = spark.table(target).count()
        print(f"{target}: {count} rows")
    except Exception:
        print(f"{target}: not created")

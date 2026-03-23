# Databricks notebook source
# MAGIC %md
# MAGIC # Poll Google Routes API
# MAGIC
# MAGIC Scheduled notebook that:
# MAGIC 1. Registers the GoogleRoutesDataSource
# MAGIC 2. Polls all 15 Vancouver corridors for live traffic data
# MAGIC 3. Writes results as JSON to a Unity Catalog Volume
# MAGIC
# MAGIC The SDP pipeline then picks up these JSON files via Auto Loader.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("volume", "traffic_data", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Google Routes Data Source

# COMMAND ----------

# MAGIC %run ../python_data_sources/google_routes_source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Poll all corridors

# COMMAND ----------

api_key = dbutils.secrets.get(scope="hackathon", key="google_routes_api_key")
corridors_path = f"/Volumes/{catalog}/{schema}/{volume}/config/corridors.json"

df = (spark.read
    .format("google_routes")
    .option("api_key", api_key)
    .option("corridors_path", corridors_path)
    .load()
)

display(df)
print(f"Polled {df.count()} corridors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Volume as JSON (for Auto Loader)

# COMMAND ----------

from datetime import datetime

# Partition by date for efficient Auto Loader pickup
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
output_path = f"/Volumes/{catalog}/{schema}/{volume}/traffic_api/poll_{timestamp}.json"

(df.write
    .mode("append")
    .json(f"/Volumes/{catalog}/{schema}/{volume}/traffic_api/")
)

print(f"Wrote {df.count()} rows to traffic_api/ in volume")

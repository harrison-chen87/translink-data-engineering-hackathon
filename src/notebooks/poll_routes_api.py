# Databricks notebook source
# MAGIC %md
# MAGIC # Poll Google Routes API
# MAGIC
# MAGIC Polls the Google Routes API for live traffic data across configured
# MAGIC corridor segments and writes results as JSON to a Volume for Auto Loader pickup.
# MAGIC
# MAGIC Works on all serverless runtimes — uses the Python Data Source API if
# MAGIC available, otherwise falls back to direct API calls.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("volume", "traffic_data", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
corridors_path = f"{volume_path}/config/corridors.json"
output_path = f"{volume_path}/traffic_api/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get API Key

# COMMAND ----------

scope_name = f"{catalog}_{schema}"

try:
    api_key = dbutils.secrets.get(scope=scope_name, key="google_routes_api_key")
    print(f"API key loaded from secrets scope '{scope_name}'")
except Exception as e:
    print(f"WARNING: No API key found in scope '{scope_name}'. Skipping API polling.")
    print(f"  Set it with: echo 'KEY' | databricks secrets put-secret {scope_name} google_routes_api_key")
    dbutils.notebook.exit("NO_API_KEY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try Python Data Source API, Fall Back to Direct Calls

# COMMAND ----------

import json
import os

# Load corridors
with open(corridors_path, "r") as f:
    corridors = json.load(f)
print(f"Loaded {len(corridors)} corridor segments")

# COMMAND ----------

# Try to use the Python Data Source API (requires newer runtime)
datasource_available = False
try:
    # %run would have registered the data source if the runtime supports it
    spark.dataSource
    datasource_available = True
    print("Python Data Source API available — using spark.read.format('google_routes')")
except AttributeError:
    print("Python Data Source API not available — using direct API calls")

# COMMAND ----------

if datasource_available:
    # Use the registered Python Data Source
    exec(open(f"/Workspace{dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0]}/../python_data_sources/google_routes_source.py").read(), globals()) if False else None

    try:
        df = (spark.read
            .format("google_routes")
            .option("api_key", api_key)
            .option("corridors_path", corridors_path)
            .load()
        )
        df.write.mode("append").json(output_path)
        print(f"Polled {df.count()} corridors via Data Source API")
    except Exception as e:
        print(f"Data Source failed ({e}), falling back to direct API calls")
        datasource_available = False

# COMMAND ----------

if not datasource_available:
    # Direct API calls — works on any runtime
    import urllib.request
    from datetime import datetime, timezone

    ROUTES_API_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"

    results = []
    errors = 0

    for c in corridors:
        origin = c["origin"]
        dest = c["destination"]

        request_body = json.dumps({
            "origin": {"location": {"latLng": {"latitude": origin["lat"], "longitude": origin["lng"]}}},
            "destination": {"location": {"latLng": {"latitude": dest["lat"], "longitude": dest["lng"]}}},
            "travelMode": "DRIVE",
            "routingPreference": "TRAFFIC_AWARE",
        }).encode("utf-8")

        req = urllib.request.Request(
            ROUTES_API_URL,
            data=request_body,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": api_key,
                "X-Goog-FieldMask": "routes.duration,routes.staticDuration,routes.distanceMeters",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            route = data.get("routes", [{}])[0]
            duration_str = route.get("duration", "0s")
            static_str = route.get("staticDuration", "0s")

            results.append({
                "corridor_id": c["corridor_id"],
                "corridor_name": c["corridor_name"],
                "origin_lat": origin["lat"],
                "origin_lng": origin["lng"],
                "dest_lat": dest["lat"],
                "dest_lng": dest["lng"],
                "duration_seconds": int(duration_str.rstrip("s")),
                "static_duration_seconds": int(static_str.rstrip("s")),
                "distance_meters": route.get("distanceMeters", 0),
                "congestion_ratio": round(
                    int(duration_str.rstrip("s")) / max(int(static_str.rstrip("s")), 1), 4
                ),
                "api_response_json": json.dumps(data),
                "polled_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            errors += 1
            results.append({
                "corridor_id": c["corridor_id"],
                "corridor_name": c["corridor_name"],
                "origin_lat": origin["lat"],
                "origin_lng": origin["lng"],
                "dest_lat": dest["lat"],
                "dest_lng": dest["lng"],
                "duration_seconds": None,
                "static_duration_seconds": None,
                "distance_meters": None,
                "congestion_ratio": None,
                "api_response_json": json.dumps({"error": str(e)}),
                "polled_at": datetime.now(timezone.utc).isoformat(),
            })

    # Write results as JSON directly to volume (no Spark needed)
    if results:
        os.makedirs(output_path, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_file = f"{output_path}/poll_{timestamp}.json"
        with open(out_file, "w") as f:
            for row in results:
                f.write(json.dumps(row) + "\n")
        print(f"Polled {len(results)} corridors ({errors} errors), wrote to {out_file}")
    else:
        print("No results to write")

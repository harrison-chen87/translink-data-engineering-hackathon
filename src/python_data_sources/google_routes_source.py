# Databricks notebook source
# MAGIC %md
# MAGIC # Google Routes API — Custom PySpark Data Source
# MAGIC
# MAGIC A batch-mode PySpark DataSource that polls the Google Routes API
# MAGIC for live traffic data across configured Vancouver corridors.
# MAGIC
# MAGIC Requires DBR 15.4+ or serverless compute.
# MAGIC
# MAGIC ## Usage
# MAGIC ```python
# MAGIC spark.dataSource.register(GoogleRoutesDataSource)
# MAGIC df = (spark.read.format("google_routes")
# MAGIC     .option("api_key", dbutils.secrets.get("hackathon", "google_routes_api_key"))
# MAGIC     .option("corridors_path", "/Volumes/catalog/schema/volume/corridors.json")
# MAGIC     .load())
# MAGIC ```

# COMMAND ----------

import json
import urllib.request
from datetime import datetime, timezone

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

class GoogleRoutesDataSource(DataSource):
    """PySpark DataSource that reads live traffic data from the Google Routes API."""

    @classmethod
    def name(cls):
        return "google_routes"

    def schema(self):
        return StructType([
            StructField("corridor_id", StringType(), False),
            StructField("corridor_name", StringType(), False),
            StructField("origin_lat", DoubleType(), False),
            StructField("origin_lng", DoubleType(), False),
            StructField("dest_lat", DoubleType(), False),
            StructField("dest_lng", DoubleType(), False),
            StructField("duration_seconds", IntegerType(), True),
            StructField("static_duration_seconds", IntegerType(), True),
            StructField("distance_meters", IntegerType(), True),
            StructField("congestion_ratio", DoubleType(), True),
            StructField("api_response_json", StringType(), True),
            StructField("polled_at", TimestampType(), False),
        ])

    def reader(self, schema):
        return GoogleRoutesReader(self.options)


class GoogleRoutesReader(DataSourceReader):
    """Reads from the Google Routes API for each configured corridor."""

    ROUTES_API_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"

    def __init__(self, options):
        self.api_key = options.get("api_key", "")
        self.corridors_path = options.get("corridors_path", "")
        # Allow passing corridors as inline JSON (for testing without Volumes)
        self.corridors_json = options.get("corridors_json", "")

    def read(self, partition):
        # Load corridor definitions
        if self.corridors_json:
            corridors = json.loads(self.corridors_json)
        elif self.corridors_path:
            with open(self.corridors_path) as f:
                corridors = json.load(f)
        else:
            raise ValueError("Must provide either 'corridors_path' or 'corridors_json' option")

        polled_at = datetime.now(timezone.utc)

        for corridor in corridors:
            row = self._poll_corridor(corridor, polled_at)
            yield tuple(row.values())

    def _poll_corridor(self, corridor, polled_at):
        """Call the Routes API for a single corridor and return a row dict."""
        request_body = {
            "origin": {
                "location": {
                    "latLng": {
                        "latitude": corridor["origin"]["lat"],
                        "longitude": corridor["origin"]["lng"],
                    }
                }
            },
            "destination": {
                "location": {
                    "latLng": {
                        "latitude": corridor["destination"]["lat"],
                        "longitude": corridor["destination"]["lng"],
                    }
                }
            },
            "travelMode": "DRIVE",
            "routingPreference": "TRAFFIC_AWARE",
            "units": "METRIC",
        }

        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "routes.duration,routes.staticDuration,routes.distanceMeters",
        }

        duration_seconds = None
        static_duration_seconds = None
        distance_meters = None
        congestion_ratio = None
        response_json = None

        try:
            req = urllib.request.Request(
                self.ROUTES_API_URL,
                data=json.dumps(request_body).encode("utf-8"),
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                response_json = resp.read().decode("utf-8")
                data = json.loads(response_json)

            if "routes" in data and len(data["routes"]) > 0:
                route = data["routes"][0]
                # duration comes as "123s" string
                duration_seconds = _parse_duration(route.get("duration"))
                static_duration_seconds = _parse_duration(route.get("staticDuration"))
                distance_meters = route.get("distanceMeters")

                if duration_seconds and static_duration_seconds and static_duration_seconds > 0:
                    congestion_ratio = round(duration_seconds / static_duration_seconds, 4)

        except Exception as e:
            # Store the error in the response field; don't fail the whole batch
            response_json = json.dumps({"error": str(e)})

        return {
            "corridor_id": corridor["corridor_id"],
            "corridor_name": corridor["corridor_name"],
            "origin_lat": corridor["origin"]["lat"],
            "origin_lng": corridor["origin"]["lng"],
            "dest_lat": corridor["destination"]["lat"],
            "dest_lng": corridor["destination"]["lng"],
            "duration_seconds": duration_seconds,
            "static_duration_seconds": static_duration_seconds,
            "distance_meters": distance_meters,
            "congestion_ratio": congestion_ratio,
            "api_response_json": response_json,
            "polled_at": polled_at,
        }


def _parse_duration(duration_str):
    """Parse Google Routes API duration string ('123s') to int seconds."""
    if not duration_str:
        return None
    return int(duration_str.rstrip("s"))

# COMMAND ----------

# Register the data source so it can be used with spark.read.format("google_routes")
spark.dataSource.register(GoogleRoutesDataSource)

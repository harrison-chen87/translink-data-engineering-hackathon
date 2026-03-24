# Databricks notebook source
# MAGIC %md
# MAGIC # Google Routes API — Custom PySpark Data Source
# MAGIC
# MAGIC A PySpark DataSource that polls the Google Routes API for live traffic
# MAGIC data across configured Vancouver corridors. Supports both batch and
# MAGIC streaming reads.
# MAGIC
# MAGIC Requires DBR 15.4+ or serverless compute.
# MAGIC
# MAGIC ## Authentication
# MAGIC
# MAGIC The API key is managed via a **Unity Catalog Connection** (`google_routes_api`).
# MAGIC This provides UC-level governance (permissions, audit, cross-workspace portability).
# MAGIC
# MAGIC Retrieve the key from the connection, then pass it as an option:
# MAGIC ```python
# MAGIC # Get API key from UC Connection
# MAGIC conn = spark.sql("DESCRIBE CONNECTION google_routes_api").collect()
# MAGIC api_key = next(r["value"] for r in conn if r["key"] == "bearer_token")
# MAGIC ```
# MAGIC
# MAGIC Or pass it directly from secrets (legacy fallback):
# MAGIC ```python
# MAGIC api_key = dbutils.secrets.get("scope", "google_routes_api_key")
# MAGIC ```
# MAGIC
# MAGIC ## Batch Usage
# MAGIC ```python
# MAGIC spark.dataSource.register(GoogleRoutesDataSource)
# MAGIC df = (spark.read.format("google_routes")
# MAGIC     .option("api_key", api_key)
# MAGIC     .option("corridors_path", "/Volumes/catalog/schema/volume/corridors.json")
# MAGIC     .load())
# MAGIC ```
# MAGIC
# MAGIC ## Streaming Usage
# MAGIC ```python
# MAGIC spark.dataSource.register(GoogleRoutesDataSource)
# MAGIC df = (spark.readStream.format("google_routes")
# MAGIC     .option("api_key", api_key)
# MAGIC     .option("corridors_path", "/Volumes/catalog/schema/volume/corridors.json")
# MAGIC     .option("poll_interval_seconds", "900")  # 15 minutes
# MAGIC     .load())
# MAGIC ```

# COMMAND ----------

import json
import time
import urllib.request
from datetime import datetime, timezone

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    InputPartition,
)

try:
    from pyspark.sql.datasource import DataSourceStreamReader
except ImportError:
    DataSourceStreamReader = None
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

GOOGLE_ROUTES_SCHEMA = StructType([
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


class GoogleRoutesDataSource(DataSource):
    """PySpark DataSource for live traffic data from the Google Routes API.

    Supports batch reads (spark.read) and streaming reads (spark.readStream).
    """

    @classmethod
    def name(cls):
        return "google_routes"

    def schema(self):
        return GOOGLE_ROUTES_SCHEMA

    def reader(self, schema):
        return GoogleRoutesBatchReader(self.options, schema)

    if DataSourceStreamReader is not None:
        def streamReader(self, schema):
            return GoogleRoutesStreamReader(self.options, schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Reader Logic

# COMMAND ----------

class GoogleRoutesReader:
    """Base reader with shared API polling logic.

    Handles corridor loading, API calls, retry with exponential backoff,
    and response parsing. Batch and stream readers inherit from this
    alongside their respective PySpark interfaces.
    """

    ROUTES_API_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"

    def __init__(self, options, schema):
        self.api_key = options.get("api_key", "")
        self.corridors_path = options.get("corridors_path", "")
        self.corridors_json = options.get("corridors_json", "")
        self.max_retries = int(options.get("max_retries", "3"))
        self.initial_backoff = float(options.get("initial_backoff", "1.0"))

    def _load_corridors(self):
        """Load corridor definitions from JSON file or inline string."""
        if self.corridors_json:
            return json.loads(self.corridors_json)
        elif self.corridors_path:
            with open(self.corridors_path) as f:
                return json.load(f)
        else:
            raise ValueError(
                "Must provide either 'corridors_path' or 'corridors_json' option"
            )

    def _poll_all_corridors(self):
        """Poll all corridors and yield one tuple per corridor."""
        corridors = self._load_corridors()
        polled_at = datetime.now(timezone.utc)

        for corridor in corridors:
            row = self._poll_corridor_with_retry(corridor, polled_at)
            yield tuple(row.values())

    def _poll_corridor_with_retry(self, corridor, polled_at):
        """Poll a single corridor with exponential backoff on transient errors."""
        last_error = None

        for attempt in range(self.max_retries + 1):
            try:
                return self._poll_corridor(corridor, polled_at)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries and self._is_retryable(e):
                    backoff = min(
                        self.initial_backoff * (2 ** attempt), 30.0
                    )
                    time.sleep(backoff)
                else:
                    break

        # All retries exhausted or non-retryable — store error, don't fail batch
        return {
            "corridor_id": corridor["corridor_id"],
            "corridor_name": corridor["corridor_name"],
            "origin_lat": corridor["origin"]["lat"],
            "origin_lng": corridor["origin"]["lng"],
            "dest_lat": corridor["destination"]["lat"],
            "dest_lng": corridor["destination"]["lng"],
            "duration_seconds": None,
            "static_duration_seconds": None,
            "distance_meters": None,
            "congestion_ratio": None,
            "api_response_json": json.dumps({"error": str(last_error)}),
            "polled_at": polled_at,
        }

    def _is_retryable(self, error):
        """Retry on timeouts and 5xx/429 HTTP errors."""
        if isinstance(error, (TimeoutError, ConnectionError, OSError)):
            return True
        if isinstance(error, urllib.request.HTTPError):
            return error.code == 429 or error.code >= 500
        return False

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

        req = urllib.request.Request(
            self.ROUTES_API_URL,
            data=json.dumps(request_body).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            response_json = resp.read().decode("utf-8")
            data = json.loads(response_json)

        duration_seconds = None
        static_duration_seconds = None
        distance_meters = None
        congestion_ratio = None

        if "routes" in data and len(data["routes"]) > 0:
            route = data["routes"][0]
            duration_seconds = _parse_duration(route.get("duration"))
            static_duration_seconds = _parse_duration(route.get("staticDuration"))
            distance_meters = route.get("distanceMeters")

            if duration_seconds and static_duration_seconds and static_duration_seconds > 0:
                congestion_ratio = round(duration_seconds / static_duration_seconds, 4)

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

# MAGIC %md
# MAGIC ## Batch Reader

# COMMAND ----------

class GoogleRoutesBatchReader(GoogleRoutesReader, DataSourceReader):
    """Batch reader — polls all corridors once per spark.read.load() call."""

    def read(self, partition):
        yield from self._poll_all_corridors()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Reader

# COMMAND ----------

class PollPartition(InputPartition):
    """Partition representing a single poll cycle."""

    def __init__(self, poll_id):
        self.poll_id = poll_id


if DataSourceStreamReader is not None:
    class GoogleRoutesStreamReader(GoogleRoutesReader, DataSourceStreamReader):
        """Streaming reader — polls all corridors each micro-batch.

        Each call to latestOffset() increments the poll counter. Spark calls
        read() for each new offset, which triggers a fresh API poll. Use
        poll_interval_seconds on the streaming query's trigger to control
        how often polls happen:

            (spark.readStream.format("google_routes")
                .option("api_key", key)
                .option("corridors_path", path)
                .load()
                .writeStream
                .trigger(processingTime="15 minutes")
                .toTable("bronze_traffic_api"))
        """

        def __init__(self, options, schema):
            super().__init__(options, schema)
            self._current_poll_id = 0

        def initialOffset(self):
            return {"poll_id": 0}

        def latestOffset(self):
            self._current_poll_id += 1
            return {"poll_id": self._current_poll_id}

        def partitions(self, start, end):
            return [PollPartition(end["poll_id"])]

        def read(self, partition):
            yield from self._poll_all_corridors()

        def commit(self, end):
            pass


# COMMAND ----------

# Register the data source so it can be used with spark.read.format("google_routes")
# spark.dataSource requires serverless runtime with Python Data Source API support
try:
    spark.dataSource.register(GoogleRoutesDataSource)
    print("Registered 'google_routes' data source")
except AttributeError:
    print("WARNING: spark.dataSource not available on this runtime. "
          "Python Data Source API requires a newer serverless environment. "
          "The poll_routes_api notebook will fall back to direct API calls.")

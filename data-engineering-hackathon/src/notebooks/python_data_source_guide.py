# Databricks notebook source
# MAGIC %md
# MAGIC # Python Data Source API — Deep Dive
# MAGIC
# MAGIC This notebook explains the **PySpark Python Data Source API**: what it is,
# MAGIC why you need it, and how this project uses it to ingest live traffic data
# MAGIC from the Google Routes API.
# MAGIC
# MAGIC **Audience:** Data engineers migrating from Synapse, SSIS, or other ETL tools
# MAGIC who need to pull data from REST APIs into Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Is a Python Data Source?
# MAGIC
# MAGIC A Python Data Source is a **custom Spark connector** written in pure Python.
# MAGIC It lets you teach Spark how to read from (or write to) any external system —
# MAGIC APIs, databases, message queues, proprietary file formats — using the same
# MAGIC `spark.read.format("your_name").load()` interface you'd use for Parquet, JSON, or Delta.
# MAGIC
# MAGIC ### The Problem It Solves
# MAGIC
# MAGIC Without a Python Data Source, ingesting from a REST API typically looks like this:
# MAGIC
# MAGIC ```python
# MAGIC # The old way — manual, fragile, not composable
# MAGIC import requests
# MAGIC
# MAGIC response = requests.get("https://api.example.com/data")
# MAGIC data = response.json()
# MAGIC
# MAGIC # Manually create a DataFrame
# MAGIC df = spark.createDataFrame(data, schema=my_schema)
# MAGIC
# MAGIC # Write to a table
# MAGIC df.write.mode("append").saveAsTable("bronze_api_data")
# MAGIC ```
# MAGIC
# MAGIC **Problems with this approach:**
# MAGIC
# MAGIC | Problem | Impact |
# MAGIC |---------|--------|
# MAGIC | All data flows through the **driver** | Single point of failure; driver OOM on large payloads |
# MAGIC | No **retry logic** built in | Transient API failures kill the entire notebook |
# MAGIC | No **streaming** support | Can't use `spark.readStream` — you're stuck with batch |
# MAGIC | Not **composable** | Can't chain with Auto Loader, SDP, or other Spark sources |
# MAGIC | **Schema is manual** | Must define and maintain schema separately from the connector |
# MAGIC | No **partitioned reads** | Can't parallelize across multiple API endpoints/pages |
# MAGIC
# MAGIC ### The Solution
# MAGIC
# MAGIC A Python Data Source wraps all of this into a reusable, Spark-native connector:
# MAGIC
# MAGIC ```python
# MAGIC # The new way — composable, retryable, streamable
# MAGIC df = spark.read.format("google_routes") \
# MAGIC     .option("api_key", key) \
# MAGIC     .option("corridors_path", path) \
# MAGIC     .load()
# MAGIC ```
# MAGIC
# MAGIC The same connector works for batch and streaming with no code changes on the
# MAGIC consumer side.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture: The Two-Class Contract
# MAGIC
# MAGIC Every Python Data Source implements two classes:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────┐
# MAGIC │  DataSource (Registration)                               │
# MAGIC │  ├── name()     → "google_routes"                        │
# MAGIC │  ├── schema()   → StructType([...])                      │
# MAGIC │  ├── reader()   → returns a DataSourceReader (batch)     │
# MAGIC │  └── streamReader() → returns a DataSourceStreamReader   │
# MAGIC └──────────────────────────────────────────────────────────┘
# MAGIC          │                        │
# MAGIC          ▼                        ▼
# MAGIC ┌─────────────────┐    ┌──────────────────────┐
# MAGIC │ DataSourceReader │    │ DataSourceStreamReader│
# MAGIC │ (Batch)          │    │ (Streaming)           │
# MAGIC │                  │    │                       │
# MAGIC │ read(partition)  │    │ initialOffset()       │
# MAGIC │  → yield tuples  │    │ latestOffset()        │
# MAGIC │                  │    │ partitions(start,end) │
# MAGIC └─────────────────┘    │ read(partition)        │
# MAGIC                        │ commit(end)            │
# MAGIC                        └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **DataSource** = "What is this connector called, what shape is its data, and
# MAGIC who does the actual reading?"
# MAGIC
# MAGIC **Reader** = "How do I actually fetch the data?"
# MAGIC
# MAGIC This separation means Spark handles serialization, parallelism, and scheduling —
# MAGIC you just implement the data fetching logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our Implementation: Google Routes Data Source
# MAGIC
# MAGIC **File:** `src/python_data_sources/google_routes_source.py`
# MAGIC
# MAGIC We use a **flat single-level inheritance** pattern — the recommended approach
# MAGIC for PySpark Data Sources. This avoids serialization issues when Spark ships
# MAGIC reader instances to executor processes.
# MAGIC
# MAGIC ```
# MAGIC GoogleRoutesReader (base — shared API logic)
# MAGIC     │
# MAGIC     ├── GoogleRoutesBatchReader(GoogleRoutesReader, DataSourceReader)
# MAGIC     │     └── read() → polls all corridors once
# MAGIC     │
# MAGIC     └── GoogleRoutesStreamReader(GoogleRoutesReader, DataSourceStreamReader)
# MAGIC           ├── initialOffset() → {"poll_id": 0}
# MAGIC           ├── latestOffset()  → increments poll counter
# MAGIC           ├── partitions()    → single partition per poll
# MAGIC           └── read()          → polls all corridors (fresh each micro-batch)
# MAGIC ```
# MAGIC
# MAGIC ### Why Flat Inheritance?
# MAGIC
# MAGIC PySpark **serializes** reader instances to ship them from the driver to executors.
# MAGIC Deep class hierarchies and abstract base classes break Python's `pickle`
# MAGIC serialization. Flat mixin-style inheritance (base class + PySpark interface)
# MAGIC keeps serialization clean and debugging simple.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Design Decisions
# MAGIC
# MAGIC ### 1. Retry with Exponential Backoff
# MAGIC
# MAGIC The Google Routes API can return transient errors (rate limits, server errors).
# MAGIC Our reader retries with exponential backoff:
# MAGIC
# MAGIC ```python
# MAGIC def _poll_corridor_with_retry(self, corridor, polled_at):
# MAGIC     for attempt in range(self.max_retries + 1):
# MAGIC         try:
# MAGIC             return self._poll_corridor(corridor, polled_at)
# MAGIC         except Exception as e:
# MAGIC             if attempt < self.max_retries and self._is_retryable(e):
# MAGIC                 backoff = min(self.initial_backoff * (2 ** attempt), 30.0)
# MAGIC                 time.sleep(backoff)
# MAGIC             else:
# MAGIC                 break
# MAGIC     # Return error row instead of failing the batch
# MAGIC     return {..., "api_response_json": json.dumps({"error": str(last_error)})}
# MAGIC ```
# MAGIC
# MAGIC | Attempt | Wait | Total elapsed |
# MAGIC |---------|------|---------------|
# MAGIC | 1 | 0s | 0s |
# MAGIC | 2 | 1s | 1s |
# MAGIC | 3 | 2s | 3s |
# MAGIC | 4 | 4s | 7s |
# MAGIC
# MAGIC **Retryable errors:** timeouts, connection errors, HTTP 429 (rate limit), HTTP 5xx.
# MAGIC **Non-retryable:** HTTP 4xx (bad request, auth failure) — these fail immediately.
# MAGIC
# MAGIC **Graceful degradation:** Even after exhausting retries, the reader returns a row
# MAGIC with the error in `api_response_json` rather than failing the entire batch.
# MAGIC This means 14 out of 15 corridors still get data even if one consistently fails.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Standard Library Only
# MAGIC
# MAGIC The reader uses `urllib.request` (Python standard library) instead of the
# MAGIC popular `requests` package. Why?
# MAGIC
# MAGIC > **Every package you import must be available on all executor nodes**, not just
# MAGIC > the driver. The standard library is always available. External packages require
# MAGIC > cluster-level installation or `%pip install` — which adds complexity and
# MAGIC > potential version conflicts.
# MAGIC
# MAGIC For a simple REST API call, `urllib.request` is sufficient. For more complex
# MAGIC connectors (OAuth2, pagination, connection pooling), you might need `requests` —
# MAGIC but always prefer the standard library when it's adequate.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Batch vs. Streaming: When to Use Which
# MAGIC
# MAGIC | Mode | How It Works | Use When |
# MAGIC |------|-------------|----------|
# MAGIC | **Batch** | `spark.read` → poll once → write JSON to Volume → Auto Loader picks up | Scheduled workflows, hackathon demos, simple setups |
# MAGIC | **Streaming** | `spark.readStream` → continuous polling → writes directly to Delta | Production ingestion, low-latency requirements, simpler pipeline |
# MAGIC
# MAGIC **Batch flow:**
# MAGIC ```
# MAGIC Notebook (scheduled) → spark.read.format("google_routes") → JSON in Volume → Auto Loader → Bronze
# MAGIC ```
# MAGIC
# MAGIC **Streaming flow:**
# MAGIC ```
# MAGIC spark.readStream.format("google_routes") → Delta table (direct) → downstream pipeline
# MAGIC ```
# MAGIC
# MAGIC The streaming path eliminates the Volume intermediary and the Auto Loader step.
# MAGIC The tradeoff is that it requires a long-running cluster or a streaming job,
# MAGIC while batch can run on ephemeral compute via a scheduled workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Error Rows vs. Exceptions
# MAGIC
# MAGIC When an API call fails, we **don't throw an exception** from `read()`.
# MAGIC Instead, we return a row with null metrics and the error in `api_response_json`:
# MAGIC
# MAGIC ```python
# MAGIC return {
# MAGIC     "corridor_id": corridor["corridor_id"],
# MAGIC     "duration_seconds": None,          # No data
# MAGIC     "congestion_ratio": None,           # No data
# MAGIC     "api_response_json": '{"error": "timeout"}',  # Error captured
# MAGIC     "polled_at": polled_at,
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Why?** An exception in `read()` kills the entire Spark task. If one corridor
# MAGIC out of 16 has a transient issue, you'd lose all 16 rows. Error rows preserve
# MAGIC the successful data and make failures visible in the silver layer's data quality
# MAGIC expectations:
# MAGIC
# MAGIC ```sql
# MAGIC -- Silver layer catches these via expectations
# MAGIC CONSTRAINT valid_duration EXPECT (duration_seconds > 0) ON VIOLATION WARN
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live Demo: Using the Data Source
# MAGIC
# MAGIC Register the data source, then read from it. This works on any cluster
# MAGIC with DBR 15.4+ or serverless compute.

# COMMAND ----------

# MAGIC %run ../python_data_sources/google_routes_source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch Read
# MAGIC
# MAGIC Polls all corridors once and returns a DataFrame.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "traffic_hackathon_de", "Schema")
dbutils.widgets.text("volume", "traffic_data", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

# Get API key from UC Connection (primary) or secrets (fallback)
try:
    conn_details = spark.sql("DESCRIBE CONNECTION `google_routes_api`").collect()
    conn_options = {row["key"]: row["value"] for row in conn_details}
    api_key = conn_options.get("bearer_token", "")
    if not api_key:
        raise ValueError("bearer_token not found")
    print("API key loaded from UC Connection 'google_routes_api'")
except Exception as e:
    scope_name = f"{catalog}_{schema}"
    api_key = dbutils.secrets.get(scope=scope_name, key="google_routes_api_key")
    print(f"Fallback: API key loaded from secrets scope '{scope_name}'")

corridors_path = f"/Volumes/{catalog}/{schema}/{volume}/config/corridors.json"

df = (spark.read
    .format("google_routes")
    .option("api_key", api_key)
    .option("corridors_path", corridors_path)
    .load()
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect the Schema
# MAGIC
# MAGIC The schema is defined in the `DataSource.schema()` method. Spark enforces it
# MAGIC automatically — if `read()` yields a tuple that doesn't match, you get a
# MAGIC clear error.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Analysis
# MAGIC
# MAGIC Which corridors are most congested right now?

# COMMAND ----------

from pyspark.sql import functions as F

display(
    df.select("corridor_name", "duration_seconds", "static_duration_seconds", "congestion_ratio")
    .withColumn("delay_seconds", F.col("duration_seconds") - F.col("static_duration_seconds"))
    .orderBy(F.col("congestion_ratio").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building Your Own Data Source
# MAGIC
# MAGIC To create a new Python Data Source for a different API, follow this template:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.datasource import DataSource, DataSourceReader
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# MAGIC
# MAGIC class MyApiDataSource(DataSource):
# MAGIC     @classmethod
# MAGIC     def name(cls):
# MAGIC         return "my_api"
# MAGIC
# MAGIC     def schema(self):
# MAGIC         return StructType([
# MAGIC             StructField("id", StringType(), False),
# MAGIC             StructField("value", IntegerType(), True),
# MAGIC         ])
# MAGIC
# MAGIC     def reader(self, schema):
# MAGIC         return MyApiReader(self.options, schema)
# MAGIC
# MAGIC
# MAGIC class MyApiReader(DataSourceReader):
# MAGIC     def __init__(self, options, schema):
# MAGIC         self.url = options.get("url", "")
# MAGIC
# MAGIC     def read(self, partition):
# MAGIC         import urllib.request, json
# MAGIC         with urllib.request.urlopen(self.url) as resp:
# MAGIC             data = json.loads(resp.read())
# MAGIC         for item in data:
# MAGIC             yield (item["id"], item["value"])
# MAGIC
# MAGIC
# MAGIC spark.dataSource.register(MyApiDataSource)
# MAGIC df = spark.read.format("my_api").option("url", "https://...").load()
# MAGIC ```
# MAGIC
# MAGIC ### Checklist for Production Data Sources
# MAGIC
# MAGIC | Requirement | Why | Our Implementation |
# MAGIC |------------|-----|-------------------|
# MAGIC | Retry with backoff | APIs have transient failures | `_poll_corridor_with_retry()` |
# MAGIC | Error rows, not exceptions | Don't lose good data because of one bad call | Error stored in `api_response_json` |
# MAGIC | Standard library imports | Must work on all executors | `urllib.request` instead of `requests` |
# MAGIC | Flat inheritance | PySpark serializes readers via pickle | `GoogleRoutesBatchReader(GoogleRoutesReader, DataSourceReader)` |
# MAGIC | Configurable via options | Callers pass API keys, paths, timeouts | `self.options.get("api_key")` |
# MAGIC | Schema declared upfront | Spark needs it before reading | `schema()` returns `StructType` |
# MAGIC | Streaming support | Enables continuous ingestion | `GoogleRoutesStreamReader` with offset tracking |

# Databricks notebook source
# MAGIC %md
# MAGIC # Acquire GTFS Data
# MAGIC
# MAGIC Downloads TransLink GTFS data directly from the public feed into the
# MAGIC hackathon volume. Runs as the first workflow task — no local download needed.
# MAGIC
# MAGIC **Source:** `https://gtfs-static.translink.ca/gtfs/History/{date}/google_transit.zip`

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "traffic_hackathon_de", "Schema")
dbutils.widgets.text("volume", "traffic_data", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

GTFS_BASE_URL = "https://gtfs-static.translink.ca/gtfs/History"
GTFS_SNAPSHOTS = ["2026-03-20", "2026-03-13", "2026-03-06"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download and Extract GTFS Snapshots

# COMMAND ----------

import os
import urllib.request
import zipfile
import tempfile

downloaded = 0
skipped = 0

for snapshot_date in GTFS_SNAPSHOTS:
    extract_dir = f"{volume_path}/gtfs/extracted/{snapshot_date}"

    # Skip if already extracted
    try:
        files = os.listdir(extract_dir)
        if len(files) > 5:  # a valid GTFS extract has ~15 files
            print(f"  {snapshot_date}: already extracted ({len(files)} files) — skipping")
            skipped += 1
            continue
    except FileNotFoundError:
        pass

    url = f"{GTFS_BASE_URL}/{snapshot_date}/google_transit.zip"
    print(f"  {snapshot_date}: downloading from {url}...")

    try:
        # Download to temp file, then extract directly to volume
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            urllib.request.urlretrieve(url, tmp.name)
            tmp_path = tmp.name

        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(tmp_path, "r") as zf:
            zf.extractall(extract_dir)

        os.unlink(tmp_path)

        file_count = len(os.listdir(extract_dir))
        print(f"  {snapshot_date}: extracted {file_count} files to {extract_dir}")
        downloaded += 1

    except Exception as e:
        print(f"  WARNING: Failed to download {snapshot_date}: {e}")
        print(f"  The GTFS URL may have changed. Check:")
        print(f"  https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources")

print(f"\nDone: {downloaded} downloaded, {skipped} already present")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

gtfs_base = f"{volume_path}/gtfs/extracted"
snapshots = sorted(os.listdir(gtfs_base))
print(f"GTFS snapshots in volume: {snapshots}")

for s in snapshots:
    files = os.listdir(f"{gtfs_base}/{s}")
    txt_files = [f for f in files if f.endswith(".txt")]
    print(f"  {s}: {len(txt_files)} files — {', '.join(sorted(txt_files)[:5])}...")

if not snapshots:
    raise RuntimeError("No GTFS data available. Check network connectivity and GTFS URL.")

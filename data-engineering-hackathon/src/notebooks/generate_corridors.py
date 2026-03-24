# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Route Corridors from GTFS
# MAGIC
# MAGIC Reads GTFS route data from the volume and generates a `corridors.json`
# MAGIC file for the Google Routes API polling notebook. Selects ~25 high-ridership
# MAGIC routes and creates 3 segments per route.
# MAGIC
# MAGIC Falls back to a default set of 16 corridors if GTFS data is unavailable.

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
# MAGIC ## Load GTFS Routes and Shapes

# COMMAND ----------

import os
import csv
import json
import math
from collections import defaultdict

# Find latest GTFS snapshot
gtfs_base = f"{volume_path}/gtfs/extracted"
try:
    snapshots = sorted(os.listdir(gtfs_base), reverse=True)
    latest = snapshots[0] if snapshots else None
except FileNotFoundError:
    latest = None

if not latest:
    print("WARNING: No GTFS data found — using default corridors")
    dbutils.notebook.exit("NO_GTFS_DATA")

print(f"Using GTFS snapshot: {latest}")
gtfs_dir = f"{gtfs_base}/{latest}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Corridor Segments from Route Shapes

# COMMAND ----------

# Load routes
routes = {}
with open(f"{gtfs_dir}/routes.txt", "r") as f:
    for row in csv.DictReader(f):
        routes[row["route_id"]] = row

# Load shapes — group points by shape_id
shapes = defaultdict(list)
with open(f"{gtfs_dir}/shapes.txt", "r") as f:
    for row in csv.DictReader(f):
        shapes[row["shape_id"]].append({
            "lat": float(row["shape_pt_lat"]),
            "lon": float(row["shape_pt_lon"]),
            "seq": int(row["shape_pt_sequence"]),
            "dist": float(row.get("shape_dist_traveled", 0) or 0),
        })

# Sort shape points by sequence
for sid in shapes:
    shapes[sid].sort(key=lambda p: p["seq"])

# Load trips — map route_id → shape_ids
route_shapes = defaultdict(set)
with open(f"{gtfs_dir}/trips.txt", "r") as f:
    for row in csv.DictReader(f):
        if row.get("shape_id"):
            route_shapes[row["route_id"]].add(row["shape_id"])

print(f"Loaded {len(routes)} routes, {len(shapes)} shapes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select High-Ridership Routes

# COMMAND ----------

ROUTE_TYPE_LABELS = {0: "tram", 1: "subway", 2: "rail", 3: "bus", 4: "ferry"}

# Priority routes by short name
PRIORITY = {"099", "99", "R1", "R2", "R3", "R4", "R5",
            "009", "9", "014", "14", "025", "25", "041", "41", "049", "49",
            "003", "3", "008", "8", "019", "19", "020", "20"}

route_by_name = {r["route_short_name"]: r for r in routes.values()}

# Select priority routes first, then fill by shape count (proxy for service frequency)
selected = []
for name in PRIORITY:
    if name in route_by_name:
        r = route_by_name[name]
        if r["route_id"] in route_shapes and r["route_id"] not in [s["route_id"] for s in selected]:
            selected.append(r)

# Fill to 25
remaining = sorted(
    [r for r in routes.values()
     if r["route_id"] in route_shapes
     and r["route_id"] not in [s["route_id"] for s in selected]],
    key=lambda r: -len(route_shapes[r["route_id"]])
)
for r in remaining:
    if len(selected) >= 25:
        break
    selected.append(r)

print(f"Selected {len(selected)} routes:")
for r in selected:
    print(f"  {r['route_short_name']:>5} | {r['route_long_name'][:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Corridor Segments

# COMMAND ----------

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat, dlon = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

corridors = []
SEGMENTS_PER_ROUTE = 3

for route in selected:
    rid = route["route_id"]
    name = route["route_short_name"]
    long_name = route.get("route_long_name", "")
    rtype = ROUTE_TYPE_LABELS.get(int(route.get("route_type", 3)), "bus")

    # Pick the longest shape for this route
    best_shape = max(route_shapes[rid], key=lambda sid: len(shapes.get(sid, [])))
    pts = shapes.get(best_shape, [])
    if len(pts) < 4:
        continue

    # Pick evenly spaced segments
    step = max(1, len(pts) // (SEGMENTS_PER_ROUTE + 1))
    for i in range(SEGMENTS_PER_ROUTE):
        idx_start = step * (i)
        idx_end = min(step * (i + 1), len(pts) - 1)
        if idx_start >= idx_end:
            continue

        origin = pts[idx_start]
        dest = pts[idx_end]
        dist = haversine_km(origin["lat"], origin["lon"], dest["lat"], dest["lon"])

        if dist < 0.3:  # skip very short segments
            continue

        seg_num = i + 1
        corridors.append({
            "corridor_id": f"route_{name}_seg_{seg_num}",
            "corridor_name": f"{name} {long_name} (segment {seg_num})" if long_name else f"Route {name} (segment {seg_num})",
            "corridor_type": rtype,
            "route_id": rid,
            "route_short_name": name,
            "zone": "Metro Vancouver",
            "speed_limit_kmh": 50 if rtype == "bus" else 80,
            "num_lanes": 2,
            "origin": {"lat": origin["lat"], "lng": origin["lon"]},
            "destination": {"lat": dest["lat"], "lng": dest["lon"]},
            "distance_km": round(dist, 2),
        })

print(f"\nGenerated {len(corridors)} corridor segments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Volume

# COMMAND ----------

config_dir = f"{volume_path}/config"
os.makedirs(config_dir, exist_ok=True)

output_path = f"{config_dir}/corridors.json"
with open(output_path, "w") as f:
    json.dump(corridors, f, indent=2)

print(f"Written {len(corridors)} corridors to {output_path}")

# Summary by type
type_counts = defaultdict(int)
for c in corridors:
    type_counts[c["corridor_type"]] += 1
for t, count in sorted(type_counts.items()):
    print(f"  {t}: {count} segments")

"""
Generate synthetic EAP (Enterprise Analytics Platform) data for the hackathon.

Produces JSON files that simulate TransLink's normalized EAP Delta tables:
  - dim_corridors   — corridor metadata (15 corridors)
  - dim_sensors     — sensor/camera locations (~60 sensors, 4 per corridor)
  - fact_traffic_counts — hourly traffic counts (~500K rows, 90 days)

Includes seeded data quality issues and late-arriving/updated records
so participants can practice Auto CDC with cursor columns.

Usage:
    python generate_eap_data.py [--output-dir /tmp/eap_data] [--days 90]
"""

import argparse
import json
import os
import random
from datetime import datetime, timedelta
from uuid import uuid4

# ---------------------------------------------------------------------------
# Corridor definitions (shared with corridors.json)
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_corridors():
    with open(os.path.join(SCRIPT_DIR, "corridors.json")) as f:
        return json.load(f)

# ---------------------------------------------------------------------------
# Sensor types by corridor type
# ---------------------------------------------------------------------------

SENSOR_TYPES = {
    "bridge": ["loop_detector", "radar", "camera", "piezo"],
    "arterial": ["loop_detector", "camera", "bluetooth", "radar"],
    "highway": ["loop_detector", "radar", "camera", "microwave"],
}

DIRECTIONS = ["northbound", "southbound", "eastbound", "westbound"]

# ---------------------------------------------------------------------------
# Dimension generators
# ---------------------------------------------------------------------------

def generate_dim_corridors(corridors, base_ts):
    """Generate corridor dimension records."""
    rows = []
    for c in corridors:
        rows.append({
            "corridor_id": c["corridor_id"],
            "corridor_name": c["corridor_name"],
            "corridor_type": c["corridor_type"],
            "zone": c["zone"],
            "speed_limit_kmh": c["speed_limit_kmh"],
            "num_lanes": c["num_lanes"],
            "origin_lat": c["origin"]["lat"],
            "origin_lng": c["origin"]["lng"],
            "dest_lat": c["destination"]["lat"],
            "dest_lng": c["destination"]["lng"],
            "last_modified_ts": base_ts.isoformat(),
        })
    return rows


def generate_dim_corridors_updates(corridors, update_ts):
    """Generate a few corridor updates (name changes, speed limit changes)."""
    updates = []
    # Simulate: Pattullo Bridge renamed after replacement
    updates.append({
        "corridor_id": "corridor_15",
        "corridor_name": "New Pattullo Bridge (New West to Surrey)",
        "corridor_type": "bridge",
        "zone": "surrey",
        "speed_limit_kmh": 70,  # new bridge has higher limit
        "num_lanes": 4,
        "origin_lat": 49.2110,
        "origin_lng": -122.8912,
        "dest_lat": 49.2020,
        "dest_lng": -122.8912,
        "last_modified_ts": update_ts.isoformat(),
    })
    # Simulate: Broadway speed limit reduced due to construction
    updates.append({
        "corridor_id": "corridor_02",
        "corridor_name": "Broadway (Commercial to Arbutus)",
        "corridor_type": "arterial",
        "zone": "vancouver_west",
        "speed_limit_kmh": 40,  # reduced for construction
        "num_lanes": 4,
        "origin_lat": 49.2575,
        "origin_lng": -123.0680,
        "dest_lat": 49.2641,
        "dest_lng": -123.1557,
        "last_modified_ts": update_ts.isoformat(),
    })
    return updates


def generate_dim_sensors(corridors, base_ts):
    """Generate 4 sensors per corridor."""
    rows = []
    sensor_num = 1
    for c in corridors:
        sensor_types = SENSOR_TYPES.get(c["corridor_type"], ["loop_detector", "camera", "radar", "bluetooth"])
        for i, stype in enumerate(sensor_types):
            # Interpolate lat/lng along the corridor
            frac = (i + 1) / (len(sensor_types) + 1)
            lat = c["origin"]["lat"] + frac * (c["destination"]["lat"] - c["origin"]["lat"])
            lng = c["origin"]["lng"] + frac * (c["destination"]["lng"] - c["origin"]["lng"])

            rows.append({
                "sensor_id": f"sensor_{sensor_num:04d}",
                "corridor_id": c["corridor_id"],
                "sensor_type": stype,
                "location_name": f"{c['corridor_name']} - {stype.replace('_', ' ').title()} {i+1}",
                "lat": round(lat, 6),
                "lng": round(lng, 6),
                "install_date": (base_ts - timedelta(days=random.randint(365, 3650))).strftime("%Y-%m-%d"),
                "status": "active",
                "last_modified_ts": base_ts.isoformat(),
            })
            sensor_num += 1
    return rows


def generate_dim_sensors_updates(sensors, update_ts):
    """Generate sensor updates: relocations, status changes."""
    updates = []
    # Sensor relocated
    s = dict(sensors[10])  # pick one sensor
    s["lat"] = s["lat"] + 0.001  # slight position change
    s["lng"] = s["lng"] - 0.001
    s["location_name"] = s["location_name"] + " (Relocated)"
    s["last_modified_ts"] = update_ts.isoformat()
    updates.append(s)

    # Sensor decommissioned
    s2 = dict(sensors[25])
    s2["status"] = "decommissioned"
    s2["last_modified_ts"] = update_ts.isoformat()
    updates.append(s2)

    # Sensor back online after maintenance
    s3 = dict(sensors[40])
    s3["status"] = "active"
    s3["last_modified_ts"] = update_ts.isoformat()
    updates.append(s3)

    return updates


# ---------------------------------------------------------------------------
# Fact generator
# ---------------------------------------------------------------------------

def _traffic_volume(hour, day_of_week, corridor_type):
    """Simulate realistic hourly traffic volume."""
    # Base volume by corridor type
    base = {"bridge": 800, "arterial": 400, "highway": 1200}.get(corridor_type, 500)

    # Time-of-day multiplier
    if 7 <= hour <= 9:
        tod_mult = 2.5 + random.uniform(-0.3, 0.3)  # morning rush
    elif 16 <= hour <= 18:
        tod_mult = 2.8 + random.uniform(-0.3, 0.3)  # evening rush
    elif 10 <= hour <= 15:
        tod_mult = 1.5 + random.uniform(-0.2, 0.2)  # midday
    elif 6 <= hour <= 7 or 19 <= hour <= 21:
        tod_mult = 1.2 + random.uniform(-0.2, 0.2)  # shoulders
    else:
        tod_mult = 0.3 + random.uniform(-0.1, 0.1)  # overnight

    # Weekend discount
    if day_of_week >= 5:
        tod_mult *= 0.6

    volume = int(base * tod_mult * random.uniform(0.85, 1.15))
    return max(0, volume)


def _avg_speed(hour, day_of_week, speed_limit, corridor_type):
    """Simulate average speed (lower during rush hour)."""
    if 7 <= hour <= 9 or 16 <= hour <= 18:
        ratio = random.uniform(0.4, 0.7)
    elif 10 <= hour <= 15:
        ratio = random.uniform(0.65, 0.85)
    else:
        ratio = random.uniform(0.8, 0.95)

    if day_of_week >= 5:
        ratio = min(ratio + 0.15, 0.98)

    return round(speed_limit * ratio, 1)


def generate_fact_traffic_counts(corridors, sensors, num_days, start_date):
    """Generate hourly traffic counts for all sensors over num_days."""
    rows = []
    sensor_corridor_map = {}
    for s in sensors:
        sensor_corridor_map[s["sensor_id"]] = s["corridor_id"]

    corridor_map = {c["corridor_id"]: c for c in corridors}

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        day_of_week = current_date.weekday()
        batch_ts = current_date + timedelta(hours=23, minutes=59)  # nightly batch

        for sensor in sensors:
            corridor = corridor_map[sensor["corridor_id"]]
            direction = random.choice(DIRECTIONS[:2])  # pick two primary directions

            for hour in range(24):
                count_hour = current_date + timedelta(hours=hour)
                vehicle_count = _traffic_volume(hour, day_of_week, corridor["corridor_type"])
                avg_speed = _avg_speed(hour, day_of_week, corridor["speed_limit_kmh"], corridor["corridor_type"])
                occupancy_pct = round(min(100, vehicle_count / 20 + random.uniform(-5, 5)), 1)

                row = {
                    "corridor_id": sensor["corridor_id"],
                    "sensor_id": sensor["sensor_id"],
                    "count_hour": count_hour.isoformat(),
                    "vehicle_count": vehicle_count,
                    "avg_speed_kmh": avg_speed,
                    "occupancy_pct": max(0, occupancy_pct),
                    "direction": direction,
                    "last_modified_ts": batch_ts.isoformat(),
                }

                # --- Seed data quality issues ---

                # ~2% null vehicle counts
                if random.random() < 0.02:
                    row["vehicle_count"] = None

                # ~0.3% negative counts (sensor glitch)
                if random.random() < 0.003:
                    row["vehicle_count"] = -abs(random.randint(1, 50))

                # ~0.5% duplicate records (will be in a separate batch)
                # handled below

                rows.append(row)

    return rows


def generate_fact_updates(corridors, sensors, rows, update_date):
    """Generate corrected/late-arriving fact records (~5% of a day's data)."""
    updates = []
    corridor_map = {c["corridor_id"]: c for c in corridors}
    update_ts = update_date + timedelta(hours=23, minutes=59)

    # Pick ~5% of rows from the last 7 days and create corrections
    recent_rows = [r for r in rows if r["count_hour"] and
                   update_date - timedelta(days=7) <= datetime.fromisoformat(r["count_hour"]) < update_date]

    sample_size = min(len(recent_rows), int(len(recent_rows) * 0.05))
    sampled = random.sample(recent_rows, sample_size) if recent_rows else []

    for r in sampled:
        corrected = dict(r)
        # Fix: corrected vehicle count (was miscalibrated)
        if corrected["vehicle_count"] is not None:
            corrected["vehicle_count"] = max(0, corrected["vehicle_count"] + random.randint(-20, 20))
        else:
            corrected["vehicle_count"] = random.randint(50, 500)
        corrected["last_modified_ts"] = update_ts.isoformat()
        updates.append(corrected)

    return updates


def generate_duplicate_facts(rows, rate=0.005):
    """Generate exact duplicate records (~0.5%)."""
    sample_size = int(len(rows) * rate)
    return random.sample(rows, min(sample_size, len(rows)))


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------

def write_json_batches(rows, output_dir, prefix, batch_size=10000):
    """Write rows as JSON files in batches."""
    os.makedirs(output_dir, exist_ok=True)
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        fname = os.path.join(output_dir, f"{prefix}_{i // batch_size + 1:04d}.json")
        with open(fname, "w") as f:
            for row in batch:
                f.write(json.dumps(row) + "\n")
    print(f"  Wrote {len(rows)} rows to {output_dir} ({(len(rows) - 1) // batch_size + 1} files)")


def write_json_single(rows, output_dir, filename):
    """Write rows as a single JSONL file."""
    os.makedirs(output_dir, exist_ok=True)
    fpath = os.path.join(output_dir, filename)
    with open(fpath, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    print(f"  Wrote {len(rows)} rows to {fpath}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic EAP data")
    parser.add_argument("--output-dir", default="/tmp/eap_data", help="Output directory")
    parser.add_argument("--days", type=int, default=90, help="Number of days of history")
    args = parser.parse_args()

    corridors = load_corridors()
    start_date = datetime(2025, 12, 1)
    base_ts = datetime(2025, 11, 30, 23, 59, 0)
    update_date = start_date + timedelta(days=args.days - 1)
    update_ts = update_date + timedelta(hours=12)

    print("Generating EAP synthetic data...")
    print(f"  Period: {start_date.date()} to {(start_date + timedelta(days=args.days - 1)).date()}")
    print(f"  Output: {args.output_dir}")

    # --- Dimensions ---
    print("\n[1/6] Generating dim_corridors...")
    dim_corridors = generate_dim_corridors(corridors, base_ts)
    write_json_single(dim_corridors, os.path.join(args.output_dir, "dim_corridors"), "initial_load.json")

    dim_corridors_updates = generate_dim_corridors_updates(corridors, update_ts)
    write_json_single(dim_corridors_updates, os.path.join(args.output_dir, "dim_corridors"), "updates.json")

    print("\n[2/6] Generating dim_sensors...")
    dim_sensors = generate_dim_sensors(corridors, base_ts)
    write_json_single(dim_sensors, os.path.join(args.output_dir, "dim_sensors"), "initial_load.json")

    dim_sensors_updates = generate_dim_sensors_updates(dim_sensors, update_ts)
    write_json_single(dim_sensors_updates, os.path.join(args.output_dir, "dim_sensors"), "updates.json")

    # --- Facts ---
    print(f"\n[3/6] Generating fact_traffic_counts ({args.days} days)...")
    fact_rows = generate_fact_traffic_counts(corridors, dim_sensors, args.days, start_date)
    write_json_batches(fact_rows, os.path.join(args.output_dir, "fact_traffic_counts"), "batch")

    print("\n[4/6] Generating fact corrections (late-arriving)...")
    fact_updates = generate_fact_updates(corridors, dim_sensors, fact_rows, update_date)
    write_json_batches(fact_updates, os.path.join(args.output_dir, "fact_traffic_counts"), "corrections")

    print("\n[5/6] Generating duplicate records...")
    dupes = generate_duplicate_facts(fact_rows)
    write_json_batches(dupes, os.path.join(args.output_dir, "fact_traffic_counts"), "duplicates")

    # --- Summary ---
    total_fact = len(fact_rows) + len(fact_updates) + len(dupes)
    print(f"\n[6/6] Summary:")
    print(f"  dim_corridors:      {len(dim_corridors)} initial + {len(dim_corridors_updates)} updates")
    print(f"  dim_sensors:        {len(dim_sensors)} initial + {len(dim_sensors_updates)} updates")
    print(f"  fact_traffic_counts: {len(fact_rows)} initial + {len(fact_updates)} corrections + {len(dupes)} duplicates = {total_fact} total")
    print(f"\nDone! Data written to {args.output_dir}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Generate corridor definitions from GTFS route data.

Reads route_waypoints.json and routes.txt from a GTFS data directory,
selects high-ridership routes, and outputs a corridors.json file in the
format expected by the Google Routes Python Data Source.

Usage:
    python3 generate_route_corridors.py \
        --waypoints-path /path/to/route_waypoints.json \
        --routes-path /path/to/routes.txt \
        --output-path /path/to/corridors.json \
        [--max-routes 25] \
        [--segments-per-route 3]
"""

import argparse
import csv
import json
import math
import sys
from collections import defaultdict


# High-priority routes to always include (by route_short_name)
PRIORITY_ROUTES = {
    # RapidBus
    "R1", "R2", "R3", "R4", "R5",
    # B-Lines
    "099", "99",
    # Major bus routes
    "009", "9", "014", "14", "025", "25", "041", "41", "049", "49",
    "003", "3", "008", "8", "019", "19", "020", "20",
    # Night bus
    "N19", "N9", "N10", "N15", "N17", "N22", "N24",
}

# Route type labels from GTFS spec
ROUTE_TYPE_LABELS = {
    0: "tram",
    1: "subway",
    2: "rail",
    3: "bus",
    4: "ferry",
    715: "on_demand",
}


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_routes(routes_path):
    """Load routes.txt into a dict keyed by route_id."""
    routes = {}
    with open(routes_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            routes[row["route_id"]] = row
    return routes


def load_waypoints(waypoints_path):
    """Load route_waypoints.json and group by route_name."""
    with open(waypoints_path, "r") as f:
        waypoints = json.load(f)

    by_route = defaultdict(list)
    for wp in waypoints:
        by_route[wp["route_name"]].append(wp)
    return by_route


def select_segments(waypoints, max_segments=3):
    """Select representative segments from a route's waypoints.

    Picks start, middle, and end segments to cover the route.
    Filters out very short segments (< 0.5 km).
    """
    # Filter to segments with meaningful distance
    valid = [
        wp for wp in waypoints
        if haversine_km(wp["origin_lat"], wp["origin_lon"],
                        wp["dest_lat"], wp["dest_lon"]) >= 0.5
    ]

    if not valid:
        return waypoints[:max_segments]

    if len(valid) <= max_segments:
        return valid

    # Pick evenly spaced segments
    step = len(valid) / max_segments
    return [valid[int(i * step)] for i in range(max_segments)]


def main():
    parser = argparse.ArgumentParser(description="Generate corridors from GTFS data")
    parser.add_argument("--waypoints-path", required=True, help="Path to route_waypoints.json")
    parser.add_argument("--routes-path", required=True, help="Path to routes.txt")
    parser.add_argument("--output-path", required=True, help="Output corridors.json path")
    parser.add_argument("--max-routes", type=int, default=25, help="Max routes to include")
    parser.add_argument("--segments-per-route", type=int, default=3, help="Segments per route")
    args = parser.parse_args()

    print(f"Loading routes from {args.routes_path}...")
    routes = load_routes(args.routes_path)

    print(f"Loading waypoints from {args.waypoints_path}...")
    waypoints_by_route = load_waypoints(args.waypoints_path)

    # Build route lookup by short name
    route_by_name = {}
    for rid, r in routes.items():
        name = r["route_short_name"]
        route_by_name[name] = r

    # Select routes: priority first, then by number of waypoints (proxy for ridership)
    selected_names = []

    # Priority routes that exist in the data
    for name in PRIORITY_ROUTES:
        if name in waypoints_by_route and name in route_by_name:
            if name not in selected_names:
                selected_names.append(name)

    # Fill remaining slots with routes that have the most waypoints
    remaining = sorted(
        [(name, len(wps)) for name, wps in waypoints_by_route.items()
         if name not in selected_names and name in route_by_name],
        key=lambda x: -x[1]
    )
    for name, _ in remaining:
        if len(selected_names) >= args.max_routes:
            break
        selected_names.append(name)

    print(f"Selected {len(selected_names)} routes: {', '.join(selected_names)}")

    # Generate corridor definitions
    corridors = []
    for route_name in selected_names:
        route_info = route_by_name[route_name]
        route_type = ROUTE_TYPE_LABELS.get(int(route_info.get("route_type", 3)), "bus")
        long_name = route_info.get("route_long_name", "")
        segments = select_segments(waypoints_by_route[route_name], args.segments_per_route)

        for i, seg in enumerate(segments, 1):
            corridor_id = f"route_{route_name}_seg_{i}"
            dist = haversine_km(
                seg["origin_lat"], seg["origin_lon"],
                seg["dest_lat"], seg["dest_lon"]
            )

            corridors.append({
                "corridor_id": corridor_id,
                "corridor_name": f"{route_name} {long_name} (segment {i})" if long_name else f"Route {route_name} (segment {i})",
                "corridor_type": route_type,
                "route_id": route_info["route_id"],
                "route_short_name": route_name,
                "zone": "Metro Vancouver",
                "speed_limit_kmh": 50 if route_type == "bus" else 80,
                "num_lanes": 2,
                "origin": {
                    "lat": seg["origin_lat"],
                    "lng": seg["origin_lon"]
                },
                "destination": {
                    "lat": seg["dest_lat"],
                    "lng": seg["dest_lon"]
                },
                "distance_km": round(dist, 2)
            })

    # Write output
    with open(args.output_path, "w") as f:
        json.dump(corridors, f, indent=2)

    print(f"\nGenerated {len(corridors)} corridor segments from {len(selected_names)} routes")
    print(f"Written to {args.output_path}")

    # Summary by route type
    type_counts = defaultdict(int)
    for c in corridors:
        type_counts[c["corridor_type"]] += 1
    for t, count in sorted(type_counts.items()):
        print(f"  {t}: {count} segments")


if __name__ == "__main__":
    main()

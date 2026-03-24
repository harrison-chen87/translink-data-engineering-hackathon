# GTFS Integration Spec — TransLink Data Engineering Hackathon

## Goal

Replace the synthetic EAP data with real TransLink GTFS data. Every table in the
pipeline uses real transit data that joins cleanly with live Google Routes API results.
The demo becomes: "here's your actual transit network analyzed on Databricks."

---

## What Changes

### Removed (synthetic)
- `src/data_gen/generate_eap_data.py` — no longer needed
- `src/data_gen/corridors.json` — replaced with GTFS-derived corridors
- `bronze_eap_fact_traffic.sql` — replaced by `bronze_gtfs_stop_times.sql`
- `bronze_eap_dim_corridors.sql` — replaced by `bronze_gtfs_routes.sql`
- `bronze_eap_dim_sensors.sql` — replaced by `bronze_gtfs_stops.sql`
- `silver_fact_traffic_counts.sql` — replaced by `silver_gtfs_scheduled_times.sql`
- `silver_dim_corridors.sql` — replaced by `silver_gtfs_routes.sql`
- `silver_dim_sensors.sql` — replaced by `silver_gtfs_stops.sql`
- `gold_sensor_reliability.py` — replaced by `gold_route_delay_patterns.sql`

### Kept (unchanged)
- `bronze_traffic_api.sql` — Auto Loader for Routes API JSON (same)
- `silver_traffic_readings.sql` — MV cleaning API data (same)
- `gold_congestion_patterns.sql` — hourly congestion patterns (same, reads from silver_traffic_readings)
- Python Data Source (`google_routes_source.py`) — same code, different corridors file
- `poll_routes_api.py` — same notebook, reads corridors from volume

### Replaced / Rewritten
- `gold_corridor_daily_summary.sql` — rewrite to join GTFS scheduled times with API actuals
- `constraints_and_tags.sql` — update table names and comments
- `deploy.sh` — remove synthetic data gen, add GTFS data copy
- `resources/traffic_pipeline.yml` — swap notebook references
- `resources/traffic_workflow.yml` — no changes needed (same task structure)
- `README.md` — full rewrite of data model section

---

## Data Available

**Source:** `/Volumes/serverless_stable_ps58um_catalog/transit_congestion_map/gtfs_data/`

| File | Rows | Description |
|------|------|-------------|
| `stops.txt` | 8,936 | Bus/train stops with lat/lng |
| `routes.txt` | 242 | All TransLink routes (bus=3, SkyTrain=1, rail=2, SeaBus=4) |
| `trips.txt` | 62,543 | Individual trip instances per route per service day |
| `stop_times.txt` | 1,833,726 | Scheduled arrival/departure at every stop for every trip |
| `shapes.txt` | 220,157 | GPS polylines for every route variant |
| `calendar.txt` | ~20 | Service patterns (weekday/weekend, date ranges) |
| `route_waypoints.json` | 21,444 | Pre-computed origin→dest segments for Routes API |
| 5 weekly GTFS zips | — | Snapshots: 2026-02-06, 02-20, 03-06, 03-13, 03-20 |

---

## Architecture

```
                                       GTFS Files (5 weekly snapshots)
                                         │
                  ┌──────────────────────┼──────────────────────┐
                  │                      │                      │
                  ▼                      ▼                      ▼
         bronze_gtfs_routes     bronze_gtfs_stops     bronze_gtfs_stop_times
         bronze_gtfs_trips      bronze_gtfs_shapes    bronze_gtfs_calendar
         (Streaming Tables via Auto Loader on CSV)
                  │
                  ▼
         silver_gtfs_routes          (Auto CDC SCD1 — route metadata across snapshots)
         silver_gtfs_stops           (Auto CDC SCD2 — stop changes/relocations)
         silver_gtfs_scheduled_times (MV — stop-to-stop scheduled travel times)
         silver_gtfs_route_shapes    (MV — route polylines for mapping)

Google Routes API ──► Python Data Source ──► Volume (JSON) ──► Auto Loader
                                                                    │
                                                                    ▼
                                                          bronze_traffic_api
                                                                    │
                                                                    ▼
                                                    silver_traffic_readings (MV)
                  │                                                 │
                  └──────────────────┬──────────────────────────────┘
                                     │
                                     ▼
                              Gold (joins GTFS + API)
                                gold_schedule_vs_actual     (scheduled vs real travel time)
                                gold_route_delay_patterns   (which routes are late, when)
                                gold_congestion_patterns    (kept — hourly congestion from API)
                                     │
                                     ▼
                              Metric Views
                                route_performance_metrics
                                traffic_congestion_metrics
```

---

## Mapping: Synthetic → GTFS (same patterns, real data)

| Pattern | Synthetic (old) | GTFS (new) | Why it's better |
|---------|----------------|------------|-----------------|
| Auto CDC SCD Type 1 | `dim_corridors` — 16 fake corridors with 2 seeded updates | `routes` — 242 real routes, actual changes across 5 weekly snapshots | Real schedule changes, not manufactured ones |
| Auto CDC SCD Type 2 | `dim_sensors` — 64 fake sensors with 3 seeded relocations | `stops` — 8,936 real stops, actual changes across snapshots | Real stop additions/relocations |
| Cursor-based CDC | `fact_traffic_counts` — 500K fake hourly counts | `stop_times` — 1.8M real scheduled stop times | Real schedule data that joins with API results |
| Data quality | Seeded nulls, negatives, duplicates in synthetic data | Natural GTFS issues: missing stop_times, duplicate trip_ids, inconsistent shape_dist | Real data quality issues, not manufactured ones |
| Gold aggregation | `gold_corridor_daily_summary` — fake counts + real congestion | `gold_schedule_vs_actual` — real schedule + real congestion | Everything is real and joins cleanly |

---

## Implementation Plan

### Phase 1: GTFS Bronze Ingestion

**6 new streaming tables** in `src/pipeline/`:

1. **`bronze_gtfs_routes.sql`**
   ```sql
   CREATE OR REFRESH STREAMING TABLE bronze_gtfs_routes
   AS SELECT *, _metadata.file_path,
     regexp_extract(_metadata.file_path, '(\\d{4}-\\d{2}-\\d{2})', 1) AS gtfs_snapshot_date
   FROM STREAM read_files(
     '${volume_path}/gtfs/extracted/*/routes.txt',
     format => 'csv', header => true
   );
   ```
   - Partition by snapshot date via file path extraction
   - All 5 snapshots ingested for SCD tracking

2. **`bronze_gtfs_stops.sql`** — Same pattern for stops.txt
3. **`bronze_gtfs_trips.sql`** — Same pattern for trips.txt
4. **`bronze_gtfs_stop_times.sql`** — Same pattern for stop_times.txt (1.8M rows)
5. **`bronze_gtfs_shapes.sql`** — Same pattern for shapes.txt
6. **`bronze_gtfs_calendar.sql`** — Same pattern for calendar.txt

**Teaching moment:** Auto Loader on nested directory structures with metadata extraction.

### Phase 2: GTFS Silver Layer

7. **`silver_gtfs_routes.sql`** — Auto CDC SCD Type 1
   ```sql
   CREATE OR REFRESH STREAMING TABLE silver_gtfs_routes;
   CREATE FLOW apply_gtfs_routes
   AS APPLY CHANGES INTO silver_gtfs_routes
   FROM STREAM(bronze_gtfs_routes)
   KEYS (route_id)
   SEQUENCE BY gtfs_snapshot_date
   STORED AS SCD TYPE 1;
   ```
   - Replaces synthetic `silver_dim_corridors`
   - Real route changes tracked across 5 snapshots
   - Data quality: `EXPECT (route_id IS NOT NULL) ON VIOLATION DROP ROW`

8. **`silver_gtfs_stops.sql`** — Auto CDC SCD Type 2
   ```sql
   CREATE OR REFRESH STREAMING TABLE silver_gtfs_stops;
   CREATE FLOW apply_gtfs_stops
   AS APPLY CHANGES INTO silver_gtfs_stops
   FROM STREAM(bronze_gtfs_stops)
   KEYS (stop_id)
   SEQUENCE BY gtfs_snapshot_date
   STORED AS SCD TYPE 2;
   ```
   - Replaces synthetic `silver_dim_sensors`
   - Real stop additions/changes tracked with `__START_AT` / `__END_AT`
   - Data quality: `EXPECT (stop_lat BETWEEN 48 AND 50) ON VIOLATION WARN`

9. **`silver_gtfs_scheduled_times.sql`** — Materialized View
   - Replaces synthetic `silver_fact_traffic_counts`
   - Calculate stop-to-stop travel time from stop_times:
     ```sql
     LEAD(arrival_time) OVER (PARTITION BY trip_id ORDER BY stop_sequence)
       - departure_time AS scheduled_segment_seconds
     ```
   - Join with trips → routes to get route_id, route_name
   - Join with stops to get origin/destination lat/lng
   - Aggregate to: route × segment × hour_of_day → avg scheduled time
   - Data quality: `EXPECT (scheduled_segment_seconds > 0) ON VIOLATION WARN`

10. **`silver_gtfs_route_shapes.sql`** — Materialized View
    - Aggregate shape points into route-level polylines
    - Calculate total route length from shape_dist_traveled
    - One row per (route_id, shape_id) with point array

### Phase 3: Route-Based Corridors

11. **`src/data_gen/generate_route_corridors.py`** — New script
    - Reads `route_waypoints.json` and `routes.txt` from the GTFS volume
    - Selects ~25 high-ridership routes:
      ```
      99 B-Line, R4, R5 (RapidBus)
      9, 14, 25, 41, 49 (major bus)
      Expo Line feeders
      Key bridges (Lions Gate, Knight St, Alex Fraser, etc.)
      ```
    - For each route: picks 3-5 representative segments (endpoint pairs)
    - Outputs `corridors.json` in the **exact same format** as current:
      ```json
      {
        "corridor_id": "route_099_seg_1",
        "corridor_name": "99 B-Line: Commercial-Broadway to Cambie",
        "corridor_type": "rapidbus",
        "origin": {"lat": 49.2627, "lng": -123.0694},
        "destination": {"lat": 49.2660, "lng": -123.1149},
        ...
      }
      ```
    - The Python Data Source and poll_routes_api.py work unchanged

    **Fallback:** Keep current `corridors.json` as `corridors_default.json`.
    deploy.sh tries to generate GTFS corridors; if it fails, uses the default.

### Phase 4: Schedule vs. Reality Gold Tables

12. **`gold_schedule_vs_actual.sql`** — Materialized View (the star table)
    - Joins `silver_gtfs_scheduled_times` with `silver_traffic_readings`
    - Match logic: corridor_id in API data → route_id in GTFS
      (the corridor_id format from Phase 3 embeds route_id)
    - Columns:
      ```
      route_id, route_name, route_type, segment_name,
      hour_of_day, day_type (weekday/weekend),
      scheduled_seconds, actual_seconds,
      delay_seconds, delay_pct,
      congestion_ratio, congestion_severity,
      report_date
      ```
    - This is the "so what" — real schedule + real traffic = real delay analysis

13. **`gold_route_delay_patterns.sql`** — Materialized View
    - Replaces `gold_sensor_reliability.py`
    - Aggregate schedule_vs_actual by route + hour:
      ```
      route_name, hour_of_day, day_type,
      avg_delay_seconds, p95_delay_seconds,
      pct_trips_delayed, worst_segment,
      sample_size
      ```
    - "The 99 B-Line averages 6 minutes late at 5pm on weekdays,
       worst segment is Commercial to Cambie"

### Phase 5: Updated Metric Views + Tags

14. **`src/metric_views/route_performance_metrics.sql`** — New
    - Dimensions: route_name, route_type, hour_of_day, day_type
    - Measures: Avg Delay, P95 Delay, % On Time, Schedule Adherence

15. Update `traffic_congestion_metrics.sql` — adjust to new gold table columns

16. Update `constraints_and_tags.sql`:
    - Comments on new GTFS tables
    - Tags: domain=transit, source=gtfs, tier=bronze/silver/gold
    - PK/FK reference for production (route_id, stop_id keys)

### Phase 6: Deploy + Docs

17. **`deploy.sh`** changes:
    - Remove synthetic data generation step
    - Add: copy GTFS data from source volume to hackathon volume
    - Add: run `generate_route_corridors.py` to create corridors.json
    - Add: `--gtfs-source` flag (path to GTFS volume, for customer portability)
    - Fallback: if no GTFS source, use bundled `corridors_default.json`

18. **`resources/traffic_pipeline.yml`** — Swap notebook references:
    - Remove: bronze_eap_*, silver_dim_*, silver_fact_*, gold_sensor_reliability
    - Add: bronze_gtfs_*, silver_gtfs_*, gold_schedule_vs_actual, gold_route_delay_patterns

19. **`README.md`** — Full update:
    - New architecture diagram
    - GTFS data model explanation
    - Updated validation queries
    - "Schedule vs. reality" as the headline use case

---

## Files Summary

### New (13 files)
```
src/pipeline/bronze_gtfs_routes.sql
src/pipeline/bronze_gtfs_stops.sql
src/pipeline/bronze_gtfs_trips.sql
src/pipeline/bronze_gtfs_stop_times.sql
src/pipeline/bronze_gtfs_shapes.sql
src/pipeline/bronze_gtfs_calendar.sql
src/pipeline/silver_gtfs_routes.sql
src/pipeline/silver_gtfs_stops.sql
src/pipeline/silver_gtfs_scheduled_times.sql
src/pipeline/silver_gtfs_route_shapes.sql
src/pipeline/gold_schedule_vs_actual.sql
src/pipeline/gold_route_delay_patterns.sql
src/data_gen/generate_route_corridors.py
```

### Modified (7 files)
```
deploy.sh
resources/traffic_pipeline.yml
src/pipeline/constraints_and_tags.sql
src/metric_views/traffic_congestion_metrics.sql
src/metric_views/deploy_metric_views.py
src/data_gen/corridors.json              → generated from GTFS
README.md
```

### Removed (4 files)
```
src/data_gen/generate_eap_data.py        → replaced by GTFS ingestion
src/pipeline/bronze_eap_fact_traffic.sql → replaced by bronze_gtfs_stop_times.sql
src/pipeline/bronze_eap_dim_corridors.sql→ replaced by bronze_gtfs_routes.sql
src/pipeline/bronze_eap_dim_sensors.sql  → replaced by bronze_gtfs_stops.sql
src/pipeline/silver_fact_traffic_counts.sql → replaced by silver_gtfs_scheduled_times.sql
src/pipeline/silver_dim_corridors.sql    → replaced by silver_gtfs_routes.sql
src/pipeline/silver_dim_sensors.sql      → replaced by silver_gtfs_stops.sql
src/pipeline/gold_sensor_reliability.py  → replaced by gold_route_delay_patterns.sql
src/metric_views/sensor_reliability_metrics.sql → replaced by route_performance_metrics.sql
```

### Kept unchanged (5 files)
```
src/pipeline/bronze_traffic_api.sql
src/pipeline/silver_traffic_readings.sql
src/pipeline/gold_congestion_patterns.sql
src/python_data_sources/google_routes_source.py
src/notebooks/poll_routes_api.py
```

---

## Customer Portability

For a customer deploying this in their own environment:

1. **GTFS data** — deploy.sh downloads directly from TransLink's public GTFS feed:
   ```
   https://gtfs-static.translink.ca/gtfs/History/YYYY-MM-DD/google_transit.zip
   ```
   No authentication required. The script downloads the latest + recent history,
   extracts, and uploads to the hackathon volume. Customer just needs `curl` and `unzip`.

   For non-TransLink customers, deploy.sh accepts `--gtfs-url` to point at any
   transit agency's GTFS feed (standard format across all agencies worldwide).

2. **Google API key** — Customer provides their own (same as before)

3. **No synthetic data** — Everything is derived from GTFS + live API. No confusion
   about what's real vs. fake.

4. **Fallback** — If GTFS download fails (network, URL change), `corridors_default.json`
   (the current 16 corridors) still works. The GTFS bronze/silver tables will just be empty.

---

## Phasing / Priority

| Phase | Effort | What you get |
|-------|--------|-------------|
| 1. Bronze GTFS | Low — 6 boilerplate SQL files | Foundation |
| 2. Silver GTFS | Medium — window functions, Auto CDC | Real dimensions + scheduled times |
| 3. Route corridors | Medium — Python script | Real routes in API polling |
| 4. Gold schedule vs actual | Medium — multi-source join | The demo headline |
| 5. Metric views + tags | Low — copy patterns | Dashboard-ready |
| 6. Deploy + docs | Low — script updates | Customer-ready |

Build order: Phase 1 → 2 → 3 → 4 → 5 → 6 (sequential — each depends on prior)

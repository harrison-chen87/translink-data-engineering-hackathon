# Filter — Feature Spec

## Overview

The filter input supports two modes: **route filter** and **vehicle filter**.
The system auto-detects which mode to use based on whether the input matches a
known route or a vehicle ID. In both cases, the entire map focuses on the
relevant data and all other data is hidden.

The app currently operates in **Live mode only**. Explorer mode is disabled
(hidden) pending further development.

---

## UI layout

The controls panel contains (top to bottom):

1. **Live clock** — current Pacific time, auto-refreshing indicator
2. **Layer toggles** — Congestion, Routes, Stops, Frequency, Top 10
3. **Filter input** — text field with clear button and info line
4. **Quick help** — clickable route chips (099, 014, 049, 025), instructions
   for vehicle filtering, and brief descriptions of each toggle
5. **Legend** — congestion colors (green/yellow/red) and vehicle dot indicator

### Quick help panel

- Clickable route chips apply the filter instantly on click
- Suggested routes: 099 (B-Line), 014, 049, 025 (high-traffic routes)
- Explains vehicle ID filtering: "type a vehicle ID from a bus dot"
- Briefly describes each toggle button's function

---

## Filter input

- Text input at the top of the controls panel
- Case-insensitive, debounced at 500ms
- Clear button (x) resets the filter and restores the full map
- Info line below the input shows match summary

### Auto-detection logic

The filter determines mode based on the input value:

1. **Route mode**: input matches a `route_short_name` in the routes data
   (e.g. `099`, `014`, `C23`). Checked client-side against loaded `routesData`.
2. **Vehicle mode**: input matches a `vehicle.id` from the live vehicles feed
   (e.g. `19045`, `16832`). Resolves the vehicle's current route and applies
   that as the route filter.
3. **Ambiguous**: if the input matches both a route and a vehicle ID, route
   takes priority — routes are the more useful filter.
4. **No match**: if the input matches neither, show empty state with info
   line "No matching route or vehicle".

The info line reflects the mode:
- Route mode: `Route 099`
- Vehicle mode: `Vehicle 19045 | Route 099 | 32 km/h`

---

## Layer behavior when filtered by route

When a route is entered (e.g. `099`), every layer scopes to that route:

### Routes layer

| Unfiltered | Filtered |
|---|---|
| All routes rendered at normal opacity | Only the matching route rendered, thicker line, full opacity |

- Match on `route_short_name` (case-insensitive)
- Non-matching routes are hidden, not dimmed
- **Status: implemented** — client-side filtering on `routesData`

### Congestion layer

| Unfiltered | Filtered |
|---|---|
| All congestion segments from bus speeds + Google API | Only congestion segments along the filtered route |

- Backend `/api/congestion?route=099&live=true` filters
  `_route_painted_congestion` to segments attributed to the filtered route
- Arterial corridors are excluded (they are not route-specific)
- **Status: implemented** — server-side route filtering

### Stops layer

| Unfiltered | Filtered |
|---|---|
| All stops (when toggled on) | Only stops served by the filtered route |

- Backend `/api/stops?route=099` returns stops that appear in `stop_times`
  for the filtered route
- Re-fetched from the backend whenever the filter changes or the layer is
  toggled on, ensuring the route parameter is always current
- **Status: implemented** — server-side JOIN on `stop_times.route_short_name`

### Frequency layer (heatmap)

| Unfiltered | Filtered |
|---|---|
| Frequency heatmap for all stops | Frequency heatmap for stops on the filtered route only |

- Backend `/api/frequency?hour=8&route=099` returns frequency data only for
  stops served by the filtered route via subquery on `stop_times`
- Re-fetched from the backend whenever the filter changes or the layer is
  toggled on
- Heatmap renders only those stops
- **Status: implemented** — server-side filtering via `stop_frequency` JOIN

### Live vehicles

| Unfiltered | Filtered by route | Filtered by vehicle |
|---|---|---|
| All vehicles as red dots with route labels | All vehicles on the filtered route | Only the single filtered vehicle |

- Route filter: client-side filter on `vehicle.route` matching the filter text
- Vehicle filter: client-side filter on `vehicle.id` (exact match)
- Route labels still shown next to each dot
- **Status: implemented** — client-side filtering on `liveVehicles`

### Top corridors panel

| Unfiltered | Filtered |
|---|---|
| Top 10 corridors by trip count | Hidden (not relevant to single-route view) |

- Corridors panel is hidden when any filter is active
- Corridors toggle button is disabled/dimmed
- **Status: implemented**

---

## Vehicle filter

When a vehicle ID is entered (e.g. `19045`), the system resolves the vehicle's
current route from the live vehicles data and applies it as a route filter for
all server-side queries (congestion, stops, frequency). The only difference
from a route filter is that only the single filtered vehicle is rendered — not
all vehicles on that route.

### Resolution flow

1. Find the vehicle in `liveVehicles` by matching `vehicle.id`
2. Read the vehicle's `route` field (e.g. `099`)
3. Set `filterRoute = '099'` for all API calls (congestion, stops, frequency)
4. Set `filterVehicleId = '19045'` so only that vehicle is rendered

**Status: implemented** — `resolveFilter()` auto-detects route vs vehicle,
sets `filterRoute` and `filterVehicleId`. All API calls use `filterRoute`.
`matchesFilter()` shows only the specific vehicle when `filterVehicleId` is set.

### What the vehicle sees

All layers show the same data as if the vehicle's route was filtered:

| Layer | Data shown |
|---|---|
| **Congestion** | Bus-speed congestion for the vehicle's route |
| **Stops** | Stops served by the vehicle's route |
| **Frequency** | Frequency heatmap for the vehicle's route stops |
| **Routes** | The vehicle's route line |
| **Live vehicles** | Only the filtered vehicle (not route siblings) |
| **Info line** | `Vehicle 19045 \| Route 099 \| 32 km/h` |
| **Stats bar** | `Vehicle 19045 \| Route 099 \| 43 segments \| 28 stops` |

### Live mode behavior

- Vehicle position updates every 10s with the live refresh cycle
- If the vehicle disappears from the feed (end of service, GPS dropout), the
  route filter stays active based on the last known route
- If the vehicle changes route (trip change), the next `resolveFilter()` call
  (on live refresh) would need to detect this — **not yet implemented**

### Future enhancements (not yet implemented)

- Auto-pan viewport to center on the filtered vehicle
- Auto-follow: viewport tracks the vehicle as it moves
- Visual distinction (larger dot, white ring) for the filtered vehicle

---

## Stats bar

When filtered, the stats bar reflects the filtered state:

| State | Format |
|---|---|
| Unfiltered | `LIVE \| 342 segments, 12 congested \| 156 vehicles \| 1054 routes` |
| Route filter | `Route 099 \| 43 segments \| 28 stops` |
| Vehicle filter | `Vehicle 19045 \| Route 099 \| 43 segments \| 28 stops` |

**Status: implemented**

---

## Backend API

All endpoints accept an optional `route` query parameter:

| Endpoint | `route` behavior | Status |
|---|---|---|
| `/api/congestion` | Filters `_route_painted_congestion` by route; excludes arterials | Implemented |
| `/api/stops` | JOINs `stops` with `stop_times` on route | Implemented |
| `/api/frequency` | Subquery filters `stop_frequency` to route's stops | Implemented |
| `/api/live-vehicles` | No change — client-side filtering | N/A |

Vehicle filtering requires no backend changes — the client resolves the
vehicle's route and passes it to the existing `?route=X` parameters.

---

## Cache strategy

| Cache key pattern | Scope |
|---|---|
| `{hour}_{day}` | All-route congestion |
| `{hour}_{day}_r_{route}` | Single-route congestion |
| `{hour}_{day}_q{quarter}` | 15-min granularity (live mode) |

Route-specific cache entries follow the same 24h TTL eviction as other entries.

---

## Edge cases

| Case | Behavior |
|---|---|
| Route not found (no matching waypoints) | Show empty congestion, info line says "No matching route or vehicle" |
| Filter text matches multiple routes (e.g. `1` matches `001`, `010`) | All matching routes shown — substring filter |
| Vehicle disappears from feed | Route filter stays active from last known route |
| Vehicle changes route mid-session | All layers update to new route on next refresh |
| Input matches both a route and vehicle ID | Route filter takes priority |
| Outside service hours (no live vehicles) | Vehicle filter cannot resolve — info shows "No matching route or vehicle" |
| Toggle stops/frequency while filtered | Re-fetches with current route param |

---

## Explorer mode (disabled)

Explorer mode (historical congestion by hour/day with play animation and trip
planner) is currently hidden pending further development. The mode switch is
hidden but the code is preserved. When re-enabled:

- Route filter passes to `/api/congestion?route=X` with hour/day params
- Play button animates hour-by-hour with route-filtered congestion
- Trip planner remains unaffected by filter
- Vehicle filter prompts user to switch to Live mode

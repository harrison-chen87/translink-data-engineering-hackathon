#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Lakebase Setup Script for Transit Congestion Map
#
# Creates and configures a Lakebase autoscale project via REST API,
# creates required tables, and sets up app service principal access.
#
# Usage (standalone):
#   ./setup_lakebase.sh --profile <profile> --project <name>
#
# Usage (from deploy.sh — all vars passed via environment):
#   Called automatically when `databricks lakebase` CLI is unavailable
#   or Lakebase is not ACTIVE after the timeout period.
#
# Required environment variables (when called from deploy.sh):
#   PROFILE_FLAG, LAKEBASE_PROJECT, SECRETS_SCOPE, USERNAME
#
# Exports on success:
#   LAKEBASE_HOST, LAKEBASE_ENDPOINT
# ---------------------------------------------------------------------------

# Allow standalone usage with flags
if [[ "${1:-}" == "--profile" ]]; then
    _PROFILE="${2:-}"
    PROFILE_FLAG="--profile ${_PROFILE}"
    shift 2
fi
if [[ "${1:-}" == "--project" ]]; then
    LAKEBASE_PROJECT="${2:-transit-cache}"
    shift 2
fi

LAKEBASE_PROJECT="${LAKEBASE_PROJECT:-transit-cache}"
LAKEBASE_ENDPOINT="projects/${LAKEBASE_PROJECT}/branches/production/endpoints/primary"

# ---------------------------------------------------------------------------
# Step 1: Create the Lakebase project (idempotent)
# ---------------------------------------------------------------------------
_create_project() {
    echo "  Creating Lakebase project '${LAKEBASE_PROJECT}' via REST API..."
    local RESULT
    RESULT=$(databricks api post "/api/2.0/postgres/projects?project_id=${LAKEBASE_PROJECT}" \
        ${PROFILE_FLAG:-} --json '{"pg_version":"17"}' 2>&1) || true

    if echo "$RESULT" | grep -q "already exists"; then
        echo "  Project already exists."
    elif echo "$RESULT" | grep -q "done"; then
        echo "  Project creation initiated."
    else
        echo "  Project creation response: $RESULT"
    fi
}

# ---------------------------------------------------------------------------
# Step 2: Wait for project to become ACTIVE
# ---------------------------------------------------------------------------
_wait_for_active() {
    echo "  Waiting for Lakebase to become ACTIVE..."
    for i in $(seq 1 30); do
        local STATUS
        STATUS=$(databricks api get \
            "/api/2.0/postgres/projects/${LAKEBASE_PROJECT}/branches/production/endpoints" \
            ${PROFILE_FLAG:-} 2>/dev/null \
            | python3 -c "
import sys, json
data = json.load(sys.stdin)
endpoints = data.get('endpoints', [])
for ep in endpoints:
    state = ep.get('status', {}).get('current_state', '')
    if state == 'ACTIVE':
        print('ACTIVE')
        break
else:
    print('PROVISIONING')
" 2>/dev/null || echo "NOT_FOUND")

        if [ "$STATUS" = "ACTIVE" ]; then
            echo "  Lakebase project is ACTIVE."
            return 0
        fi

        if [ "$i" -eq 30 ]; then
            echo "  WARNING: Lakebase not ACTIVE after 5 minutes."
            return 1
        fi

        printf "  Waiting... (%d/30)\r" "$i"
        sleep 10
    done
}

# ---------------------------------------------------------------------------
# Step 3: Extract endpoint hostname
# ---------------------------------------------------------------------------
_get_endpoint_host() {
    LAKEBASE_HOST=$(databricks api get \
        "/api/2.0/postgres/projects/${LAKEBASE_PROJECT}/branches/production/endpoints" \
        ${PROFILE_FLAG:-} 2>/dev/null \
        | python3 -c "
import sys, json
data = json.load(sys.stdin)
endpoints = data.get('endpoints', [])
for ep in endpoints:
    host = ep.get('status', {}).get('hosts', {}).get('host', '')
    if host:
        print(host)
        break
" 2>/dev/null || echo "")

    if [ -z "$LAKEBASE_HOST" ]; then
        echo "  ERROR: Could not detect Lakebase endpoint hostname."
        return 1
    fi

    echo "  Lakebase host: ${LAKEBASE_HOST}"
    echo "  Lakebase endpoint: ${LAKEBASE_ENDPOINT}"
    export LAKEBASE_HOST
    export LAKEBASE_ENDPOINT
}

# ---------------------------------------------------------------------------
# Step 4: Create tables
# ---------------------------------------------------------------------------
_create_tables() {
    echo "  Creating Lakebase tables..."

    local TOKEN
    TOKEN=$(databricks api post /api/2.0/postgres/credentials \
        ${PROFILE_FLAG:-} --json "{\"endpoint\": \"${LAKEBASE_ENDPOINT}\"}" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])" 2>/dev/null || echo "")

    if [ -z "$TOKEN" ]; then
        echo "  WARNING: Could not get Lakebase credentials. Skipping local table creation."
        echo "  Run notebook '00_create_lakebase_tables' inside the workspace instead."
        return 0
    fi

    local PSQL_OUTPUT
    PSQL_OUTPUT=$(PGPASSWORD="$TOKEN" psql \
        "host=${LAKEBASE_HOST} port=5432 dbname=databricks_postgres user=${USERNAME:-$(whoami)} sslmode=require" \
        -c "
CREATE TABLE IF NOT EXISTS routes (
    shape_id INTEGER, route_short_name TEXT, route_long_name TEXT,
    route_type INTEGER, route_color TEXT, coordinates TEXT, feed_date TEXT
);
CREATE TABLE IF NOT EXISTS stops (
    stop_id TEXT, stop_name TEXT, stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION, location_type INTEGER, feed_date TEXT
);
CREATE TABLE IF NOT EXISTS route_segments (
    shape_id INTEGER, origin_lat DOUBLE PRECISION, origin_lon DOUBLE PRECISION,
    dest_lat DOUBLE PRECISION, dest_lon DOUBLE PRECISION, route_short_name TEXT
);
CREATE TABLE IF NOT EXISTS congestion_cache (
    cache_key TEXT PRIMARY KEY, hour_of_day INTEGER, day_of_week INTEGER,
    segments_json TEXT, last_accessed_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS stop_times (
    trip_id TEXT, arrival_time TEXT, departure_time TEXT, stop_id TEXT,
    stop_sequence INTEGER, route_id TEXT, route_short_name TEXT, shape_id TEXT
);
CREATE TABLE IF NOT EXISTS stop_frequency (
    stop_id TEXT, stop_name TEXT, stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION, route_short_name TEXT,
    trips_per_day INTEGER, peak_hour INTEGER, peak_trips INTEGER
);
" 2>&1) || true

    if echo "$PSQL_OUTPUT" | grep -qi "public access is not allowed\|connection refused\|could not connect\|authorization failed"; then
        echo "  WARNING: Cannot connect to Lakebase locally (private link/IP ACL)."
        echo "  Run notebook '00_create_lakebase_tables' inside the workspace instead."
    else
        echo "  Tables created."
    fi
}

# ---------------------------------------------------------------------------
# Step 5: Configure app service principal access
#
# Call this AFTER the app has been deployed (app SP must exist).
# Usage: setup_lakebase.sh --configure-app-access --app-name <name>
# ---------------------------------------------------------------------------
_configure_app_access() {
    local APP_NAME="${1:-transit-congestion-map}"
    local NATIVE_PASSWORD="${2:-transit-app-$(date +%s | tail -c 6)}"

    echo "  Configuring app service principal access..."

    # Get the app's service principal ID
    local APP_INFO
    APP_INFO=$(databricks apps get "${APP_NAME}" ${PROFILE_FLAG:-} 2>/dev/null || echo "{}")
    local SP_ID
    SP_ID=$(echo "$APP_INFO" | python3 -c "
import sys, json
info = json.load(sys.stdin)
print(info.get('service_principal_client_id', ''))
" 2>/dev/null || echo "")

    if [ -z "$SP_ID" ]; then
        echo "  ERROR: Could not get app service principal ID."
        return 1
    fi
    echo "  App SP: ${SP_ID}"

    # Grant READ on secrets scope
    databricks secrets put-acl "${SECRETS_SCOPE:-transit_congestion_map}" \
        "$SP_ID" READ ${PROFILE_FLAG:-} 2>/dev/null || true
    echo "  Secrets ACL granted."

    # Grant CAN_MANAGE on Lakebase database-project
    local PROJ_UID
    PROJ_UID=$(databricks api get "/api/2.0/postgres/projects/${LAKEBASE_PROJECT}" \
        ${PROFILE_FLAG:-} 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['uid'])" 2>/dev/null || echo "")

    if [ -n "$PROJ_UID" ]; then
        databricks api patch "/api/2.0/permissions/database-projects/${PROJ_UID}" \
            ${PROFILE_FLAG:-} --json "{
            \"access_control_list\": [{
                \"service_principal_name\": \"${SP_ID}\",
                \"permission_level\": \"CAN_MANAGE\"
            }]
        }" >/dev/null 2>&1 || true
        echo "  Lakebase project permissions granted."
    fi

    # Create Postgres role with native password
    local TOKEN
    TOKEN=$(databricks api post /api/2.0/postgres/credentials \
        ${PROFILE_FLAG:-} --json "{\"endpoint\": \"${LAKEBASE_ENDPOINT}\"}" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])" 2>/dev/null || echo "")

    if [ -n "$TOKEN" ]; then
        local PSQL_OUTPUT
        PSQL_OUTPUT=$(PGPASSWORD="$TOKEN" psql \
            "host=${LAKEBASE_HOST} port=5432 dbname=databricks_postgres user=${USERNAME:-$(whoami)} sslmode=require" \
            -c "
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${SP_ID}') THEN
        CREATE ROLE \"${SP_ID}\" WITH LOGIN PASSWORD '${NATIVE_PASSWORD}';
    ELSE
        ALTER ROLE \"${SP_ID}\" WITH PASSWORD '${NATIVE_PASSWORD}';
    END IF;
END
\$\$;
GRANT ALL ON ALL TABLES IN SCHEMA public TO \"${SP_ID}\";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"${SP_ID}\";
GRANT USAGE ON SCHEMA public TO \"${SP_ID}\";
" 2>&1) || true

        if echo "$PSQL_OUTPUT" | grep -qi "public access is not allowed\|connection refused\|could not connect\|authorization failed"; then
            echo "  WARNING: Cannot create Postgres role locally (private link/IP ACL)."
            echo "  Run this SQL inside the workspace via a notebook:"
            echo "    CREATE ROLE \"${SP_ID}\" WITH LOGIN PASSWORD '${NATIVE_PASSWORD}';"
            echo "    GRANT ALL ON ALL TABLES IN SCHEMA public TO \"${SP_ID}\";"
        else
            echo "  Postgres role configured."
        fi
    else
        echo "  WARNING: Could not get Lakebase credentials. Skipping Postgres role creation."
        echo "  The app will use OAuth auth (works if Lakebase is provisioned, not autoscale)."
    fi

    # Export the password so deploy.sh can inject it into app.yaml
    export LAKEBASE_NATIVE_PASSWORD="$NATIVE_PASSWORD"
    echo "  Native password set."
}

# ---------------------------------------------------------------------------
# Main: run all setup steps (or individual step via flag)
# ---------------------------------------------------------------------------
case "${1:-all}" in
    --configure-app-access)
        shift
        _configure_app_access "${1:-transit-congestion-map}" "${2:-}"
        ;;
    all|--setup)
        _create_project
        _wait_for_active
        _get_endpoint_host
        _create_tables
        echo ""
        echo "  Lakebase setup complete."
        echo "  Host: ${LAKEBASE_HOST}"
        echo "  Run './setup_lakebase.sh --configure-app-access <app-name>' after app deployment."
        ;;
    *)
        echo "Usage: $0 [--setup | --configure-app-access <app-name> [password]]"
        echo "       $0 --profile <profile> --project <name> [--setup]"
        exit 1
        ;;
esac

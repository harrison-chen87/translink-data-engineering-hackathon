#!/usr/bin/env bash
set -euo pipefail

# Transit Congestion Map — Automated Deployment Script
# Deploys the full stack to a customer's Databricks workspace.
# Run from: can-hunter/customer_prototypes/translink/transit-congestion-map/

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="${SCRIPT_DIR}/src"
STAGING_DIR="${SCRIPT_DIR}/.deploy-staging"

echo "============================================"
echo "  Transit Congestion Map — Deployment"
echo "============================================"
echo ""

# -----------------------------------------------
# 1. Collect configuration
# -----------------------------------------------

read -rp "Databricks CLI profile [DEFAULT]: " DB_PROFILE
DB_PROFILE="${DB_PROFILE:-DEFAULT}"

PROFILE_FLAG=""
if [ "$DB_PROFILE" != "DEFAULT" ]; then
    PROFILE_FLAG="--profile ${DB_PROFILE}"
fi

# Verify CLI authentication
echo ""
echo "Verifying Databricks CLI authentication..."
if ! CURRENT_USER=$(databricks current-user me $PROFILE_FLAG 2>/dev/null); then
    echo "ERROR: Could not authenticate with Databricks CLI."
    echo "Run: databricks auth login --profile ${DB_PROFILE}"
    exit 1
fi
USERNAME=$(echo "$CURRENT_USER" | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
echo "Authenticated as: ${USERNAME}"

echo ""
read -rp "Unity Catalog name (must already exist): " CATALOG
if [ -z "$CATALOG" ]; then
    echo "ERROR: Catalog name is required."
    exit 1
fi

SCHEMA="transit_congestion_map"
VOLUME="gtfs_data"
SECRETS_SCOPE="transit_congestion_map"
LAKEBASE_PROJECT="transit-cache"
WORKSPACE_PATH="/Workspace/Users/${USERNAME}/transit-congestion-map"

echo ""
echo "Enter your API keys (they will be stored in Databricks Secrets only):"
read -rp "Google Routes API key: " GOOGLE_API_KEY
if [ -z "$GOOGLE_API_KEY" ]; then
    echo "ERROR: Google Routes API key is required."
    exit 1
fi

read -rp "TransLink API key: " TRANSLINK_API_KEY
if [ -z "$TRANSLINK_API_KEY" ]; then
    echo "ERROR: TransLink API key is required."
    exit 1
fi

echo ""
echo "--- Configuration Summary ---"
echo "  Profile:        ${DB_PROFILE}"
echo "  User:           ${USERNAME}"
echo "  Catalog:        ${CATALOG}"
echo "  Schema:         ${CATALOG}.${SCHEMA}"
echo "  Volume:         ${CATALOG}.${SCHEMA}.${VOLUME}"
echo "  Secrets scope:  ${SECRETS_SCOPE}"
echo "  Lakebase DB:    ${LAKEBASE_PROJECT}"
echo "  Workspace path: ${WORKSPACE_PATH}"
echo "-----------------------------"
echo ""
read -rp "Proceed? (y/N): " CONFIRM
if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

# -----------------------------------------------
# 2. Create infrastructure
# -----------------------------------------------

echo ""
echo "[1/6] Creating schema and volume..."

# Find a SQL warehouse for executing statements
WAREHOUSE_ID=$(
  databricks warehouses list $PROFILE_FLAG --output json 2>/dev/null \
  | python3 -c "
import sys, json
wh = json.load(sys.stdin)
running = [w for w in wh if w.get('state') == 'RUNNING']
print((running or wh or [{}])[0].get('id', ''))
" 2>/dev/null || echo ""
)

if [[ -z "$WAREHOUSE_ID" ]]; then
    echo "  ERROR: No SQL warehouses found. Create one first or run these manually:"
    echo "    CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA};"
    echo "    CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.${VOLUME};"
    exit 1
fi

echo "  Using warehouse: $WAREHOUSE_ID"

# Start warehouse if needed
databricks warehouses start "$WAREHOUSE_ID" --wait $PROFILE_FLAG >/dev/null 2>&1 || true

# Create schema via SQL Statement Execution API
RESULT=$(databricks api post /api/2.0/sql/statements $PROFILE_FLAG --json "{
  \"warehouse_id\": \"${WAREHOUSE_ID}\",
  \"statement\": \"CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}\",
  \"wait_timeout\": \"120s\"
}" 2>&1) || true
STATE=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
if [[ "$STATE" != "SUCCEEDED" ]]; then
    echo "  WARNING: Schema creation returned state: $STATE"
fi

# Create volume via SQL Statement Execution API
RESULT=$(databricks api post /api/2.0/sql/statements $PROFILE_FLAG --json "{
  \"warehouse_id\": \"${WAREHOUSE_ID}\",
  \"statement\": \"CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.${VOLUME}\",
  \"wait_timeout\": \"120s\"
}" 2>&1) || true
STATE=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
if [[ "$STATE" != "SUCCEEDED" ]]; then
    echo "  WARNING: Volume creation returned state: $STATE"
fi

echo "  Schema and volume created."

echo ""
echo "[2/6] Creating secrets scope and storing API keys..."
# Create scope (ignore error if it already exists)
databricks secrets create-scope ${SECRETS_SCOPE} $PROFILE_FLAG 2>/dev/null || true
databricks secrets put-secret ${SECRETS_SCOPE} google_routes_api_key \
    --string-value "${GOOGLE_API_KEY}" $PROFILE_FLAG
databricks secrets put-secret ${SECRETS_SCOPE} translink_api_key \
    --string-value "${TRANSLINK_API_KEY}" $PROFILE_FLAG
echo "  Secrets stored."

echo ""
echo "[3/6] Creating Lakebase database..."
# Create Lakebase database (ignore error if it already exists)
databricks lakebase databases create ${LAKEBASE_PROJECT} $PROFILE_FLAG 2>/dev/null || true

echo "  Waiting for Lakebase to become ACTIVE..."
for i in $(seq 1 30); do
    DB_STATUS=$(databricks lakebase databases get ${LAKEBASE_PROJECT} $PROFILE_FLAG 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
    if [ "$DB_STATUS" = "ACTIVE" ]; then
        echo "  Lakebase database is ACTIVE."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  WARNING: Lakebase not yet ACTIVE after 5 minutes. Continuing — it may be ready by pipeline step 04."
    fi
    sleep 10
done

# Get Lakebase endpoint details
LAKEBASE_INFO=$(databricks lakebase databases get ${LAKEBASE_PROJECT} $PROFILE_FLAG 2>/dev/null || echo "{}")
LAKEBASE_ENDPOINT=$(echo "$LAKEBASE_INFO" | python3 -c "
import sys, json
info = json.load(sys.stdin)
# Build endpoint path from database name
name = info.get('name', '${LAKEBASE_PROJECT}')
print(f'projects/{name}/branches/production/endpoints/primary')
" 2>/dev/null || echo "projects/${LAKEBASE_PROJECT}/branches/production/endpoints/primary")

LAKEBASE_HOST=$(echo "$LAKEBASE_INFO" | python3 -c "
import sys, json
info = json.load(sys.stdin)
ep = info.get('endpoint', {})
print(ep.get('host', ''))
" 2>/dev/null || echo "")

if [ -z "$LAKEBASE_HOST" ]; then
    # Try to extract from endpoints list
    LAKEBASE_HOST=$(databricks lakebase endpoints list --database ${LAKEBASE_PROJECT} $PROFILE_FLAG 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
endpoints = data if isinstance(data, list) else data.get('endpoints', [])
for ep in endpoints:
    host = ep.get('host', '') or ep.get('hostname', '')
    if host:
        print(host)
        break
" 2>/dev/null || echo "")
fi

if [ -z "$LAKEBASE_HOST" ]; then
    echo "  WARNING: Could not auto-detect Lakebase host."
    read -rp "  Enter Lakebase endpoint hostname manually: " LAKEBASE_HOST
fi

echo "  Lakebase endpoint: ${LAKEBASE_ENDPOINT}"
echo "  Lakebase host:     ${LAKEBASE_HOST}"

# -----------------------------------------------
# 3. Prepare staged copies with customer config
# -----------------------------------------------

echo ""
echo "[4/6] Preparing notebooks and app with your configuration..."
rm -rf "${STAGING_DIR}"
mkdir -p "${STAGING_DIR}/pipeline" "${STAGING_DIR}/app"

# Patch pipeline notebooks: replace hardcoded catalog/schema/secrets/lakebase
for nb in 01_ingest_gtfs 02_build_route_segments 03_sync_to_lakebase; do
    sed \
        -e "s|serverless_stable_ps58um_catalog|${CATALOG}|g" \
        -e "s|\"hackathon\"|\"${SECRETS_SCOPE}\"|g" \
        -e "s|scope=\"hackathon\"|scope=\"${SECRETS_SCOPE}\"|g" \
        -e "s|\"transit_congestion_map\"|\"${SECRETS_SCOPE}\"|g" \
        -e "s|LAKEBASE_PROJECT = \"transit-cache\"|LAKEBASE_PROJECT = \"${LAKEBASE_PROJECT}\"|g" \
        -e "s|LAKEBASE_ENDPOINT = \"projects/transit-cache/branches/production/endpoints/primary\"|LAKEBASE_ENDPOINT = \"${LAKEBASE_ENDPOINT}\"|g" \
        -e "s|ep-round-tree-d2ped0gk.database.us-east-1.cloud.databricks.com|${LAKEBASE_HOST}|g" \
        "${SRC_DIR}/pipeline/${nb}.py" > "${STAGING_DIR}/pipeline/${nb}.py"
done

# Patch app.yaml (scope, lakebase, secrets scope env var)
sed \
    -e "s|scope: transit_congestion_map|scope: ${SECRETS_SCOPE}|g" \
    -e "s|value: transit-cache|value: ${LAKEBASE_PROJECT}|g" \
    -e "s|value: projects/transit-cache/branches/production/endpoints/primary|value: ${LAKEBASE_ENDPOINT}|g" \
    -e "s|ep-round-tree-d2ped0gk.database.us-east-1.cloud.databricks.com|${LAKEBASE_HOST}|g" \
    -e "s|value: transit_congestion_map|value: ${SECRETS_SCOPE}|g" \
    "${SRC_DIR}/app/app.yaml" > "${STAGING_DIR}/app/app.yaml"

# Copy app source files (no patching needed — they read from env vars)
cp "${SRC_DIR}/app/main.py" "${STAGING_DIR}/app/main.py"
cp "${SRC_DIR}/app/requirements.txt" "${STAGING_DIR}/app/requirements.txt"
cp -r "${SRC_DIR}/app/static" "${STAGING_DIR}/app/static" 2>/dev/null || true
# If index.html is directly in app/, copy it
if [ -f "${SRC_DIR}/app/static/index.html" ]; then
    true  # already copied above
elif [ -f "${SRC_DIR}/app/index.html" ]; then
    cp "${SRC_DIR}/app/index.html" "${STAGING_DIR}/app/index.html"
fi

echo "  Staged files ready."

# -----------------------------------------------
# 4. Upload pipeline notebooks
# -----------------------------------------------

echo ""
echo "[5/6] Uploading notebooks and running pipeline..."

databricks workspace mkdirs "${WORKSPACE_PATH}/pipeline" $PROFILE_FLAG

for nb in 01_ingest_gtfs 02_build_route_segments 03_sync_to_lakebase; do
    databricks workspace import \
        "${WORKSPACE_PATH}/pipeline/${nb}" \
        --file "${STAGING_DIR}/pipeline/${nb}.py" \
        --format SOURCE --language PYTHON --overwrite \
        $PROFILE_FLAG
    echo "  Uploaded ${nb}"
done

# Upload app source
databricks workspace mkdirs "${WORKSPACE_PATH}/app" $PROFILE_FLAG
databricks workspace import \
    "${WORKSPACE_PATH}/app/main.py" \
    --file "${STAGING_DIR}/app/main.py" \
    --format AUTO --overwrite $PROFILE_FLAG
databricks workspace import \
    "${WORKSPACE_PATH}/app/requirements.txt" \
    --file "${STAGING_DIR}/app/requirements.txt" \
    --format AUTO --overwrite $PROFILE_FLAG
databricks workspace import \
    "${WORKSPACE_PATH}/app/app.yaml" \
    --file "${STAGING_DIR}/app/app.yaml" \
    --format AUTO --overwrite $PROFILE_FLAG

if [ -d "${STAGING_DIR}/app/static" ]; then
    databricks workspace mkdirs "${WORKSPACE_PATH}/app/static" $PROFILE_FLAG
    for f in "${STAGING_DIR}/app/static/"*; do
        fname=$(basename "$f")
        databricks workspace import \
            "${WORKSPACE_PATH}/app/static/${fname}" \
            --file "$f" \
            --format AUTO --overwrite $PROFILE_FLAG
    done
fi

echo ""
echo "  Running pipeline (this takes ~10-15 minutes)..."
echo ""

for nb in 01_ingest_gtfs 02_build_route_segments 03_sync_to_lakebase; do
    echo "  Running ${nb}..."
    RUN_OUTPUT=$(databricks jobs submit --json '{
        "run_name": "transit-deploy-'"${nb}"'",
        "tasks": [{
            "task_key": "'"${nb}"'",
            "notebook_task": {
                "notebook_path": "'"${WORKSPACE_PATH}/pipeline/${nb}"'"
            },
            "environment_key": "default"
        }],
        "environments": [{
            "environment_key": "default",
            "spec": { "client": "1" }
        }]
    }' $PROFILE_FLAG --wait 2>&1)

    if echo "$RUN_OUTPUT" | grep -q '"result_state": "SUCCESS"\|SUCCESS'; then
        echo "  ${nb}: SUCCESS"
    else
        echo "  ${nb}: WARNING — check run output for errors"
        echo "  $RUN_OUTPUT" | tail -5
    fi
done

# -----------------------------------------------
# 5. Deploy the app
# -----------------------------------------------

echo ""
echo "[6/6] Deploying Databricks App..."
databricks apps deploy transit-congestion-map \
    --source-code-path "${WORKSPACE_PATH}/app" \
    $PROFILE_FLAG

echo ""
echo "============================================"
echo "  Deployment complete!"
echo "============================================"
echo ""
echo "Your app is deploying. Check status with:"
echo "  databricks apps get transit-congestion-map $PROFILE_FLAG"
echo ""
echo "Once running, open the app URL shown in the output above."
echo ""

# Clean up staging
rm -rf "${STAGING_DIR}"

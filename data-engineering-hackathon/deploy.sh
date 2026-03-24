#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# TransLink Data Engineering Hackathon — Deploy Script
#
# Usage:
#   ./deploy.sh --profile <profile> --catalog <catalog> [options]
#
# This script:
#   1. Creates the UC schema and volume
#   2. Stores the Google API key in workspace secrets
#   3. Deploys the DABs bundle (pipeline + workflow)
#   4. Runs the workflow (which downloads GTFS data, generates corridors,
#      polls the API, runs the pipeline, and applies tags/metrics)
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Defaults
PROFILE=""
CATALOG=""
SCHEMA="traffic_hackathon_de"
VOLUME="traffic_data"
TARGET=""
API_KEY=""
SKIP_PIPELINE=false

usage() {
  cat <<EOF
Usage: $0 --profile <profile> --catalog <catalog> [options]

Required:
  --profile         Databricks CLI profile name
  --catalog         Unity Catalog catalog name

Optional:
  --schema          Schema name (default: traffic_hackathon_de)
  --volume          Volume name (default: traffic_data)
  --target          DABs target (default: none)
  --api-key         Google Routes API key (will be stored in Databricks Secrets)
  --skip-pipeline   Skip pipeline run (deploy only)
EOF
  exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --profile)        PROFILE="$2"; shift 2 ;;
    --catalog)        CATALOG="$2"; shift 2 ;;
    --schema)         SCHEMA="$2"; shift 2 ;;
    --volume)         VOLUME="$2"; shift 2 ;;
    --target)         TARGET="$2"; shift 2 ;;
    --api-key)        API_KEY="$2"; shift 2 ;;
    --skip-pipeline)  SKIP_PIPELINE=true; shift ;;
    -h|--help)        usage ;;
    *)                echo "Unknown option: $1"; usage ;;
  esac
done

[[ -z "$PROFILE" ]] && { echo "ERROR: --profile is required"; usage; }
[[ -z "$CATALOG" ]] && { echo "ERROR: --catalog is required"; usage; }

CLI="databricks --profile=$PROFILE"
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}"

echo "============================================="
echo " TransLink Data Engineering Hackathon Deploy"
echo "============================================="
echo "Profile:  $PROFILE"
echo "Catalog:  $CATALOG"
echo "Schema:   $SCHEMA"
echo "Volume:   $VOLUME"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Create schema and volume
# ---------------------------------------------------------------------------
echo "[1/4] Creating schema and volume..."

# Find a SQL warehouse
WAREHOUSE_ID=$(
  $CLI warehouses list --output json 2>/dev/null \
  | python3 -c "
import sys, json
wh = json.load(sys.stdin)
running = [w for w in wh if w.get('state') == 'RUNNING']
print((running or wh or [{}])[0].get('id', ''))
" 2>/dev/null || echo ""
)

if [[ -z "$WAREHOUSE_ID" ]]; then
  echo "  WARNING: No SQL warehouses found."
  echo "  Create the schema and volume manually:"
  echo "    CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA};"
  echo "    CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.${VOLUME};"
else
  echo "  Using warehouse: $WAREHOUSE_ID"

  # Start warehouse if needed
  $CLI warehouses start "$WAREHOUSE_ID" --wait >/dev/null 2>&1 || true

  # Create schema
  RESULT=$($CLI api post /api/2.0/sql/statements --json "{
    \"warehouse_id\": \"${WAREHOUSE_ID}\",
    \"statement\": \"CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}\",
    \"wait_timeout\": \"30s\"
  }" 2>&1) || true
  STATE=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
  [[ "$STATE" == "SUCCEEDED" ]] && echo "  Schema: ${CATALOG}.${SCHEMA} ✓" || echo "  WARNING: Schema creation: $STATE"

  # Create volume
  RESULT=$($CLI api post /api/2.0/sql/statements --json "{
    \"warehouse_id\": \"${WAREHOUSE_ID}\",
    \"statement\": \"CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.${VOLUME}\",
    \"wait_timeout\": \"30s\"
  }" 2>&1) || true
  STATE=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
  [[ "$STATE" == "SUCCEEDED" ]] && echo "  Volume: ${VOLUME_PATH} ✓" || echo "  WARNING: Volume creation: $STATE"
fi

# ---------------------------------------------------------------------------
# Step 2: Store API key in workspace secrets
# ---------------------------------------------------------------------------
echo ""
echo "[2/4] Configuring API credentials..."

SCOPE_NAME="${CATALOG}_${SCHEMA}"

if [[ -n "$API_KEY" ]]; then
  $CLI secrets create-scope "$SCOPE_NAME" 2>/dev/null || true
  echo "$API_KEY" | $CLI secrets put-secret "$SCOPE_NAME" google_routes_api_key
  echo "  Stored API key in secrets scope '${SCOPE_NAME}' ✓"
else
  echo "  No --api-key provided. GTFS pipeline will work without it,"
  echo "  but live API polling will fail until a key is set:"
  echo "    databricks secrets create-scope ${SCOPE_NAME} --profile=$PROFILE"
  echo "    echo 'YOUR_KEY' | databricks secrets put-secret ${SCOPE_NAME} google_routes_api_key --profile=$PROFILE"
fi

# ---------------------------------------------------------------------------
# Step 3: Deploy DABs bundle
# ---------------------------------------------------------------------------
echo ""
echo "[3/4] Deploying DABs bundle..."

TARGET_FLAG=""
[[ -n "$TARGET" ]] && TARGET_FLAG="--target=$TARGET"

cd "$SCRIPT_DIR"
databricks bundle deploy \
  --profile="$PROFILE" \
  $TARGET_FLAG \
  --var="catalog=${CATALOG}" \
  --var="schema=${SCHEMA}" \
  --var="volume=${VOLUME}" \
  --var="warehouse_id=${WAREHOUSE_ID}"

echo "  Bundle deployed ✓"

# ---------------------------------------------------------------------------
# Step 4: Run the workflow
# ---------------------------------------------------------------------------
if [[ "$SKIP_PIPELINE" == "false" ]]; then
  echo ""
  echo "[4/4] Running the workflow..."
  echo "  This will: download GTFS → generate corridors → poll API → run pipeline → apply tags"
  echo ""

  databricks bundle run traffic_workflow \
    --profile="$PROFILE" \
    $TARGET_FLAG \
    --var="catalog=${CATALOG}" \
    --var="schema=${SCHEMA}" \
    --var="volume=${VOLUME}" \
    --var="warehouse_id=${WAREHOUSE_ID}" \
    --no-wait

  echo "  Workflow complete ✓"
else
  echo ""
  echo "[4/4] Skipping workflow run (--skip-pipeline)"
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "============================================="
echo " Deployment complete!"
echo "============================================="
echo ""
echo "Next steps:"
echo "  1. Check pipeline status in the Databricks UI"
echo "  2. Run validation queries (see README.md)"
echo "  3. Enable the workflow schedule for continuous polling"
echo ""

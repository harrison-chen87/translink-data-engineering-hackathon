#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# TransLink Data Engineering Hackathon — Deploy Script
#
# Usage:
#   ./deploy.sh --profile <profile> --catalog <catalog> [options]
#
# This script:
#   1. Generates synthetic EAP data locally
#   2. Creates the UC schema, volume, and secrets scope
#   3. Uploads data + corridor config to the volume
#   4. Stores the Google API key as a Databricks secret
#   5. Deploys the DABs bundle (pipeline + workflow)
#   6. Runs the pipeline
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Defaults
PROFILE=""
CATALOG=""
SCHEMA="traffic_hackathon_de"
VOLUME="traffic_data"
TARGET=""
API_KEY=""
DATA_DIR="/tmp/eap_data"
DAYS=90
SKIP_DATA_GEN=false
SKIP_UPLOAD=false
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
  --data-dir        Local directory for generated data (default: /tmp/eap_data)
  --days            Number of days of synthetic history (default: 90)
  --skip-data-gen   Skip synthetic data generation
  --skip-upload     Skip data upload to volume
  --skip-pipeline   Skip pipeline run
EOF
  exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --profile)      PROFILE="$2"; shift 2 ;;
    --catalog)      CATALOG="$2"; shift 2 ;;
    --schema)       SCHEMA="$2"; shift 2 ;;
    --volume)       VOLUME="$2"; shift 2 ;;
    --target)       TARGET="$2"; shift 2 ;;
    --api-key)      API_KEY="$2"; shift 2 ;;
    --data-dir)     DATA_DIR="$2"; shift 2 ;;
    --days)         DAYS="$2"; shift 2 ;;
    --skip-data-gen)  SKIP_DATA_GEN=true; shift ;;
    --skip-upload)    SKIP_UPLOAD=true; shift ;;
    --skip-pipeline)  SKIP_PIPELINE=true; shift ;;
    -h|--help)      usage ;;
    *)              echo "Unknown option: $1"; usage ;;
  esac
done

[[ -z "$PROFILE" ]] && { echo "ERROR: --profile is required"; usage; }
[[ -z "$CATALOG" ]] && { echo "ERROR: --catalog is required"; usage; }

CLI="databricks --profile=$PROFILE"
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}"
DBFS_VOLUME_PATH="dbfs:${VOLUME_PATH}"

echo "============================================="
echo " TransLink Data Engineering Hackathon Deploy"
echo "============================================="
echo "Profile:  $PROFILE"
echo "Catalog:  $CATALOG"
echo "Schema:   $SCHEMA"
echo "Volume:   $VOLUME"
echo "Data dir: $DATA_DIR"
echo "Days:     $DAYS"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Generate synthetic EAP data
# ---------------------------------------------------------------------------
if [[ "$SKIP_DATA_GEN" == "false" ]]; then
  echo "[1/6] Generating synthetic EAP data..."
  python3 "${SCRIPT_DIR}/src/data_gen/generate_eap_data.py" \
    --output-dir "$DATA_DIR" \
    --days "$DAYS"
else
  echo "[1/6] Skipping data generation (--skip-data-gen)"
fi

# ---------------------------------------------------------------------------
# Step 2: Create schema and volume
# ---------------------------------------------------------------------------
echo ""
echo "[2/6] Creating schema and volume..."

$CLI api post /api/2.0/sql/statements \
  --json "{
    \"warehouse_id\": \"$($CLI warehouses list --output json | python3 -c "import sys,json; wh=json.load(sys.stdin); print(wh[0]['id'] if wh else '')" 2>/dev/null || echo "")\",
    \"statement\": \"CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}\",
    \"wait_timeout\": \"30s\"
  }" > /dev/null 2>&1 || echo "  Schema may already exist (or create manually)"

$CLI api post /api/2.0/sql/statements \
  --json "{
    \"warehouse_id\": \"$($CLI warehouses list --output json | python3 -c "import sys,json; wh=json.load(sys.stdin); print(wh[0]['id'] if wh else '')" 2>/dev/null || echo "")\",
    \"statement\": \"CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.${VOLUME}\",
    \"wait_timeout\": \"30s\"
  }" > /dev/null 2>&1 || echo "  Volume may already exist (or create manually)"

echo "  Schema: ${CATALOG}.${SCHEMA}"
echo "  Volume: ${VOLUME_PATH}"

# ---------------------------------------------------------------------------
# Step 3: Upload data to volume
# ---------------------------------------------------------------------------
if [[ "$SKIP_UPLOAD" == "false" ]]; then
  echo ""
  echo "[3/6] Uploading data to volume..."

  # Upload corridor config
  $CLI fs mkdir "${DBFS_VOLUME_PATH}/config" 2>/dev/null || true
  $CLI fs cp "${SCRIPT_DIR}/src/data_gen/corridors.json" "${DBFS_VOLUME_PATH}/config/corridors.json" --overwrite
  echo "  Uploaded corridors.json to ${VOLUME_PATH}/config/"

  # Upload EAP data
  for table_dir in fact_traffic_counts dim_corridors dim_sensors; do
    if [[ -d "${DATA_DIR}/${table_dir}" ]]; then
      $CLI fs mkdir "${DBFS_VOLUME_PATH}/eap/${table_dir}" 2>/dev/null || true
      for f in "${DATA_DIR}/${table_dir}"/*.json; do
        $CLI fs cp "$f" "${DBFS_VOLUME_PATH}/eap/${table_dir}/$(basename "$f")" --overwrite
      done
      echo "  Uploaded ${table_dir}/"
    fi
  done
else
  echo "[3/6] Skipping upload (--skip-upload)"
fi

# ---------------------------------------------------------------------------
# Step 4: Store API key in Databricks Secrets
# ---------------------------------------------------------------------------
echo ""
echo "[4/6] Configuring secrets..."

if [[ -n "$API_KEY" ]]; then
  # Create scope if it doesn't exist
  $CLI secrets create-scope hackathon 2>/dev/null || true
  printf '%s' "$API_KEY" | $CLI secrets put-secret hackathon google_routes_api_key
  echo "  Stored Google API key in secrets scope 'hackathon'"
else
  echo "  No --api-key provided. Set it later with:"
  echo "    databricks secrets create-scope hackathon --profile=$PROFILE"
  echo "    echo 'YOUR_KEY' | databricks secrets put-secret hackathon google_routes_api_key --profile=$PROFILE"
fi

# ---------------------------------------------------------------------------
# Step 5: Deploy DABs bundle
# ---------------------------------------------------------------------------
echo ""
echo "[5/6] Deploying DABs bundle..."

TARGET_FLAG=""
[[ -n "$TARGET" ]] && TARGET_FLAG="--target=$TARGET"

cd "$SCRIPT_DIR"
databricks bundle deploy \
  --profile="$PROFILE" \
  $TARGET_FLAG \
  --var="catalog=${CATALOG}" \
  --var="schema=${SCHEMA}" \
  --var="volume=${VOLUME}"

echo "  Bundle deployed"

# ---------------------------------------------------------------------------
# Step 6: Run the pipeline
# ---------------------------------------------------------------------------
if [[ "$SKIP_PIPELINE" == "false" ]]; then
  echo ""
  echo "[6/6] Running the pipeline..."

  databricks bundle run traffic_workflow \
    --profile="$PROFILE" \
    $TARGET_FLAG \
    --var="catalog=${CATALOG}" \
    --var="schema=${SCHEMA}" \
    --var="volume=${VOLUME}"

  echo "  Pipeline run started"
else
  echo "[6/6] Skipping pipeline run (--skip-pipeline)"
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

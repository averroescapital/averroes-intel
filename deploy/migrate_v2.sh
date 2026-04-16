#!/bin/bash
# =============================================================================
# Averroes Portfolio Intel — v2 migration runbook
# =============================================================================
# One-shot migration from legacy pipeline (SQL-based silver) to the new
# era-based pipeline (Python-based silver via silver_gold_v2 module).
#
# Run from repo root:
#    bash deploy/migrate_v2.sh
#
# Prereqs:
#   - gcloud CLI authenticated (`gcloud auth login` + `gcloud auth application-default login`)
#   - bq CLI installed
#   - Active project set: `gcloud config set project averroes-portfolio-intel`
#   - All 16 MA files present in a local folder (default: ./raw_ma_files/)
# =============================================================================

set -euo pipefail

PROJECT_ID="averroes-portfolio-intel"
REGION="europe-west2"
BUCKET="${PROJECT_ID}-portfolio-data"
PORTCO_ID="portco-alpha"
MA_FILES_DIR="${MA_FILES_DIR:-./raw_ma_files}"
GCS_PREFIX="${PORTCO_ID}/ma-files"

# export PATH to pick up gcloud if installed via Homebrew on macOS
export PATH="/opt/homebrew/share/google-cloud-sdk/bin:${PATH}"

echo "================================================================"
echo " 1. Apply v2 BigQuery schemas"
echo "================================================================"
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false \
  < bq_schemas/silver_gold_v2.sql
echo "  ✓ silver.kpi_long created"
echo "  ✓ gold.kpi_monthly_v2 created"
echo "  ✓ gold.revenue_ltm_by_bl, gold.tech_arr_split, gold.modules_live_view views created"

echo
echo "================================================================"
echo " 2. Run local backfill (parse 16 files -> CSVs)"
echo "================================================================"
python3 functions/ingest/backfill.py
echo "  ✓ dashboard/silver_kpi_long.csv + dashboard/gold_kpi_monthly.csv written"

echo
echo "================================================================"
echo " 3. Load backfill CSVs into BigQuery (silver + gold v2)"
echo "================================================================"
python3 deploy/load_bq_backfill.py
echo "  ✓ silver.kpi_long populated for ${PORTCO_ID}"
echo "  ✓ gold.kpi_monthly_v2 populated for ${PORTCO_ID}"

echo
echo "================================================================"
echo " 4. Upload raw MA files to GCS for future event-triggered ingest"
echo "================================================================"
if [[ -d "${MA_FILES_DIR}" ]]; then
  for f in "${MA_FILES_DIR}"/*.xlsx; do
    [[ -e "$f" ]] || { echo "  (no .xlsx in ${MA_FILES_DIR})"; break; }
    fname=$(basename "$f")
    # Skip re-upload if already there and the event would re-trigger ingest
    echo "  Uploading ${fname}..."
    gsutil cp "$f" "gs://${BUCKET}/${GCS_PREFIX}/${fname}"
  done
  echo "  ✓ Raw files now in gs://${BUCKET}/${GCS_PREFIX}/"
else
  echo "  WARNING: ${MA_FILES_DIR} not found."
  echo "  Skipping file upload. You can upload manually with:"
  echo "    gsutil cp <yourfiles>/*.xlsx gs://${BUCKET}/${GCS_PREFIX}/"
fi

echo
echo "================================================================"
echo " 5. Redeploy portfolio-data-ingest Cloud Function with new parser"
echo "================================================================"
# Copy taxonomy if the config dir exists (legacy)
if [[ -f config/kpi_taxonomy.yaml ]]; then
  cp config/kpi_taxonomy.yaml functions/ingest/ 2>/dev/null || true
fi

gcloud functions deploy portfolio-data-ingest \
  --gen2 \
  --runtime=python311 \
  --region="${REGION}" \
  --source=functions/ingest \
  --entry-point=process_file \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=${BUCKET}" \
  --set-env-vars="GCP_PROJECT=${PROJECT_ID}" \
  --set-secrets="GEMINI_API_KEY=projects/${PROJECT_ID}/secrets/gemini_api_key:latest" \
  --memory=512MB \
  --timeout=300

echo
echo "================================================================"
echo " 6. Quick sanity check in BigQuery"
echo "================================================================"
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=pretty \
"SELECT FORMAT_DATE('%Y-%m', period) AS month, era,
        ROUND(revenue_total_actual, 1)      AS revenue_total,
        ROUND(tech_arr, 1)                  AS tech_arr,
        ROUND(ecommerce_arr, 1)             AS ecom_arr,
        ROUND(ems_arr, 1)                   AS ems_arr,
        total_headcount
 FROM \`${PROJECT_ID}.gold.kpi_monthly_v2\`
 WHERE portco_id = '${PORTCO_ID}'
 ORDER BY period"

echo
echo "================================================================"
echo " ✅ Migration complete."
echo "================================================================"
echo "  - silver.kpi_long     (long-format, unit-normalised + derived)"
echo "  - gold.kpi_monthly_v2 (dashboard-ready wide)"
echo "  - gold.revenue_ltm_by_bl, gold.tech_arr_split, gold.modules_live_view"
echo
echo "  Next:"
echo "  - Drop a new MA file in gs://${BUCKET}/${PORTCO_ID}/ma-files/"
echo "    and check the function logs:"
echo "    gcloud functions logs read portfolio-data-ingest --region=${REGION} --gen2 --limit=50"
echo "  - Update dashboard/pe_app.py to read from gold.kpi_monthly_v2 (see dashboard/pages/era_view.py)"

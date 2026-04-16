#!/bin/bash
export PATH=/opt/homebrew/share/google-cloud-sdk/bin:$PATH
export CLOUDSDK_CONFIG=~/.gcloud_config_dir
# averroes-portfolio-intel deployment script
# Run this script to deploy BigQuery schemas and Cloud Functions to GCP.

PROJECT_ID="averroes-portfolio-intel"
REGION="europe-west2"
BUCKET_NAME="$PROJECT_ID-portfolio-data"

echo "Deploying BigQuery Schemas..."
# Ensure schemas are executed. A quick script could be run.
bq query --use_legacy_sql=false < bq_schemas/schemas.sql
bq query --use_legacy_sql=false < bq_schemas/gold_views.sql
# v2 schemas: silver.kpi_long + gold.kpi_monthly_v2 + new views
bq query --use_legacy_sql=false < bq_schemas/silver_gold_v2.sql

echo "Preparing functions..."
# Copy taxonomy file into function folders so it deploys together
cp config/kpi_taxonomy.yaml functions/ingest/
cp config/kpi_taxonomy.yaml functions/anomaly_detect/

echo "Deploying ingest function..."
gcloud functions deploy portfolio-data-ingest \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=functions/ingest \
  --entry-point=process_file \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=$BUCKET_NAME" \
  --set-env-vars=GCP_PROJECT=$PROJECT_ID \
  --set-secrets=GEMINI_API_KEY=projects/$PROJECT_ID/secrets/gemini_api_key:latest \
  --memory=512MB \
  --timeout=300

echo "Deploying anomaly detection function..."
gcloud functions deploy portfolio-anomaly-detect \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=functions/anomaly_detect \
  --entry-point=detect_anomalies \
  --trigger-http \
  --set-env-vars=GCP_PROJECT=$PROJECT_ID \
  --set-secrets=GEMINI_API_KEY=projects/$PROJECT_ID/secrets/gemini_api_key:latest \
  --memory=512MB \
  --timeout=300

echo "Creating Cloud Scheduler trigger for Anomaly Detect..."
gcloud scheduler jobs create http trigger-anomaly-detect \
  --schedule="0 7 * * *" \
  --uri="$(gcloud functions describe portfolio-anomaly-detect --region=$REGION --gen2 --format='value(serviceConfig.uri)')" \
  --http-method=POST \
  --oidc-service-account-email="YOUR_SERVICE_ACCOUNT_EMAIL@$PROJECT_ID.iam.gserviceaccount.com" \
  --location=$REGION

echo "Done!"

# Averroes Capital Portfolio Intelligence Platform

This repository contains the full source code and infrastructure scripts to deploy the Averroes Capital Portfolio Intelligence Platform onto Google Cloud Platform.

## Architecture

![Architecture](https://via.placeholder.com/800x400?text=Metabase+%3C-+BigQuery+%3C-+Cloud+Functions+%3C-+GCS+Bucket)

- **Storage**: GCS Bucket (`averroes-portfolio-intel-portfolio-data`)
- **Ingestion**: Event-triggered Cloud Function uses Pandas & Gemini to parse Excel, CSV, and PDF into normalized JSON logic, mapping variables using fuzzy-matching to the standard taxonomy.
- **Data Warehouse**: BigQuery (`bronze`, `silver`, `gold`, `alerts`). Gold views compute MoM, YoY, RAG statuses.
- **Anomaly Detection**: Scheduled Cloud Function (cron) queries Gold layer. Uses statistical anomalies, rule-based anomaly detection, and Gemini 2.0 Pro to create executive summaries and insight generation.
- **BI**: Metabase dashboards

## Repository Structure
- `bq_schemas/` - SQL schemas and views for Bronze, Silver, Gold, and Alerts layers.
- `config/kpi_taxonomy.yaml` - Core configuration holding KPI naming standard aliases, expected behaviors, RAG rules, and anomaly rules.
- `functions/ingest/` - Cloud Function source for parsing incoming documents and loading them into Bronze/Silver.
- `functions/anomaly_detect/` - Cloud Function source for generating automated alerts and AI executive commentaries.
- `generate_sample_data.py` - Script to generate 30-months of sample data specifically modeled with a built-in anomaly in Sept 2024 for Portco Alpha.
- `deploy.sh` - Simple deployment sequence.

## Steps to Deploy

### 1. Prerequisites
- `gcloud` CLI installed and authenticated to your GCP project `averroes-portfolio-intel`.
- `bq` CLI installed.
- Ensure all relevant datasets exist (`bronze`, `silver`, `gold`, `alerts`).
- Ensure the GCS bucket exists (`averroes-portfolio-intel-portfolio-data`).
- Store your Gemini API key in Secret Manager:
  ```bash
  echo -n "YOUR_API_KEY" | gcloud secrets create gemini_api_key --data-file=-
  ```
- Ensure Cloud Functions, Run, Secret Manager, storage, and build APIs are enabled.

### 2. Generate Sample Data
Run the included python script to build 30 months of synthetic metrics data.
```bash
pip install pandas openpyxl
python generate_sample_data.py
```
This generates `.xlsx` and `.csv` files inside the `sample_data/` directory appropriately arranged per PortCo. 

### 3. Deploy
Execute the deployment script to pipe the DDL into BigQuery and upload your Cloud Functions.
```bash
./deploy.sh
```

### 4. Test Ingestion flow
Upload one of the generated files locally up to the Google Storage bucket:
```bash
gsutil cp sample_data/portco-alpha/* gs://averroes-portfolio-intel-portfolio-data/portco-alpha/
```
Wait 2 minutes and check `averroes-portfolio-intel.bronze.raw_submissions` and `averroes-portfolio-intel.silver.normalised_kpis` inside the BQ Console!

### 5. Test Anomaly Detection
Once data is placed and Gold Views compute, you can trigger anomaly detection either via Cloud Scheduler or directly with the local command:
```bash
gcloud functions call portfolio-anomaly-detect --region=europe-west2 --gen2
```
Look inside `alerts.anomaly_flags` to spot the generated Sept 2024 churn alert!

### 6. Set up Metabase
1. Connect Metabase to your GCP BigQuery using a Google Service Account JSON Key configured with BigQuery Viewer permissions.
2. Form your dashboards leveraging `gold.portfolio_rollup` and `gold.monthly_kpis`!

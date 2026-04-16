# Averroes Capital Portfolio Intelligence Platform

## Project Overview

You are building a PE portfolio monitoring dashboard for a GP fund (Averroes Capital). The dashboard ingests monthly management accounts (Excel files) from portfolio companies into BigQuery (GCP), computes KPIs, and displays them via a custom Streamlit dashboard (`app.py` & `pe_app.py`).

**Project Directory:**  
`/Users/ishuratna/.gemini/antigravity/scratch/averroes-portfolio-intel`

**Dashboard Subdirectory:**  
`/Users/ishuratna/.gemini/antigravity/scratch/averroes-portfolio-intel/dashboard`

## 1. System Architecture

The tech stack comprises:
- **Cloud Storage (GCS):** Receives raw PortCo Excel/CSV submissions.
- **Data Warehouse (BigQuery):** Multi-layered dataset structure mapping raw inputs to computed KPIs.
- **Compute (Cloud Functions):** Event-driven functions parsing file uploads into BigQuery.
- **Frontend (Streamlit):** Executive-grade UI built using Python, pandas, and plotly.
- **Anomaly Detection:** Rule-based checks generating alerts for metrics like cash thresholds, revenue variance, etc.

### Data Flow
1. **Raw Ingestion (Bronze Layer):** Management accounts (`MAfileFeb26.xlsx`, `KPITracker.xlsx`) are uploaded to GCS. Cloud Functions parse files into `bronze.ma_raw`, storing line items as simple rows.
2. **Normalization (Silver Layer):** The raw data is structured into specialized tables (`silver.pnl`, `silver.financial_kpis`, `silver.cash_flow`, `silver.covenants`). This is unified across multiple portfolio companies.
3. **KPI Computation (Gold Layer):** SQL views (`gold.kpi_monthly`, `gold.kpi_quarterly`) compute complex LTM (Last Twelve Months), YTD (Year-to-Date), margins, and growth percentiles.
4. **Visualization:** Dashboards read straight from the Gold Layer to render views.

## 2. Codebase Structure

### Root Directory
- `README.md`: High-level overview and GCP deployment instructions.
- `deploy.sh`: Script to pipe DDL into BigQuery and upload Cloud Functions.
- `generate_sample_data.py`: Generates 30-months of synthetic metric data to test anomalies.
- `kpi_taxonomy.yaml`: Central taxonomy for standard aliases, RAG behaviors, and expectations.

### `bq_schemas/` (BigQuery SQL)
- `schemas.sql` & `pe_schemas.sql`: Defines Bronze, Silver, and Gold structures.
- `gold_views.sql`: The aggregation logic mapping normalized data into dashboard-ready KPI rows.

### `dashboard/` (Frontend)
- `app.py`: The **primary** application. It implements the "Averroes Portfolio Portal" with a premium "deep navy and white" executive aesthetic. Features a robust fallback mechanism that auto-generates realistic sample data if BigQuery is disconnected. It contains 7 distinct views: Executive Summary, Revenue & ARR, Profitability, Cash, People, Product, and Risk.
- `pe_app.py`: An older prototype built early in the process mapping specifically to the KPI tracker logic.

### `functions/` (Serverless Compute)
- Contains scripts to process ingestion (`functions/ingest/`) and compute anomaly alerts via Gemini (`functions/anomaly_detect/`).

## 3. Workflows

### Editing the Dashboard UI
1. Open `/Users/ishuratna/.gemini/antigravity/scratch/averroes-portfolio-intel/dashboard/app.py`.
2. The UI uses standard Streamlit components but overrides CSS at the top of the file for a custom, premium aesthetic.
3. To add new metrics, trace them from the `safe()` helper function which pulls straight from the BigQuery query payload.

### Running the App Locally
```bash
cd /Users/ishuratna/.gemini/antigravity/scratch/averroes-portfolio-intel/dashboard
python3 -m venv .venv
source .venv/bin/activate
pip install streamlit pandas plotly google-cloud-bigquery db-dtypes
streamlit run app.py
```

### Adding a New Portfolio Company
1. Add their unique `portco_id` to Google Cloud and verify standard mapping in the pipeline.
2. The Streamlit dashboard will automatically pick up the new ID via `portcos = df_raw["portco_id"].unique()` and add it to the sidebar dropdown. 

### Triggering Ingestion Manually
If you need to test Excel parsing locally or manually trigger an update without GCP:
```bash
gsutil cp sample_data/portco-alpha/* gs://averroes-portfolio-intel-portfolio-data/portco-alpha/
```

## 4. Key Metrics Monitored
- **Standard KPIs:** Total Revenue, Gross Margin, Contribution Margin, Adjusted EBITDA, Net Working Capital, Cash Burn, Free Cash Conversion.
- **SaaS Specific:** Tech ARR, MRR, Revenue Churn, Net Revenue Retention, Rule of 40, S&M Efficiency.
- **PortCo Specific (e.g. Alpha):** Modules Sold/Live, Properties Activated, Geo-specific Revenue.
- **Debt & Risk:** GL Revenue Covenant, EBITDA Covenant, and GP Guard Rails (95% of Budget threshold).

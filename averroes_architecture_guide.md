# Averroes Portfolio Intelligence: End-to-End System Architecture & Technical Workflow

**Document Purpose:** This document serves as the definitive reference guide for the Averroes Portfolio Intelligence Platform. It details the end-to-end data lifecycle—from the ingestion of raw Management Accounts (MAs) in Excel to executive-level dashboard visualizations and AI-driven data exploration.

---

## 1. High-Level System Architecture Overview

The platform is designed to transition unstructured, portfolio-company-specific Excel data into a highly structured, auditable, and actionable intelligence layer. 

The architecture consists of four primary pillars:
1.  **Automated Ingestion Pipeline:** Cloud-native pipeline to intercept, route, and parse Excel files.
2.  **Medallion Data Lakehouse (BigQuery):** A three-tiered storage architecture separating raw audit data from business logic and reporting metrics.
3.  **Executive Dashboard Engine:** A Streamlit-based web application providing real-time, interactive board-level reporting and automated anomaly detection.
4.  **AI Data Analyst (LLM Integration):** A Gemini-powered natural language-to-SQL engine capable of traversing both raw and consolidated data layers.

---

## 2. Detailed Technical Workflow

### Phase 1: Data Ingestion & Intelligent Routing
**Goal:** Ingest raw Management Account (MA) files without manual intervention and apply company-specific parsing rules.

1.  **File Upload (The Trigger):** A user or automated process uploads a monthly management account file (e.g., `MAfileFeb26.xlsx`) to a designated Google Cloud Storage (GCS) Bucket: `gs://averroes-portfolio-intel-portfolio-data`.
2.  **Cloud Function Execution:** A Google Cloud Function (`functions/ingest/main.py`) is triggered instantaneously upon file creation in the bucket.
3.  **Intelligent Routing:** The Cloud Function inspects the file path (e.g., `portco-alpha/MAfileFeb26.xlsx`) to extract = `portco_id`.
4.  **Strategy Pattern Parsing:** Instead of a monolithic script, the system utilizes a Strategy Pattern. The main parser (`ma_parser.py`) routes the file to a bespoke "sub-parser" tailored to the specific portfolio company's business model (e.g., `functions/ingest/parsers/alpha_parser.py` tailored for Hospitality SaaS).
5.  **Extraction Logic:** The active parser uses the `openpyxl` Python library to tear down the Excel workbook. It navigates across 36+ sheets, extracting 104 distinct Key Performance Indicators (KPIs). It maps specific cell coordinates to uniform variables, handling errors, empty cells, and data type conversions automatically.

### Phase 2: Medallion Architecture Storage (Google BigQuery)
**Goal:** Store data with absolute audibility, progressing from raw extractions to clean, dashboard-ready metrics.

All parsed data is transacted into Google BigQuery using the `google-cloud-bigquery` library.

*   **Bronze Layer (`bronze.raw_management_accounts`):**
    *   **Purpose:** The single source of truth and auditability.
    *   **What it holds:** Every single cell extracted from the Excel file is logged here as an individual row.
    *   **Data Structure:** `sheet_name`, `row_label`, `column_label`, `value`, `source_cell` (e.g., 'P&L Summary', 'Total Revenue', 'Actual', 150000, 'B18').
*   **Silver Layer (`silver.monthly_pnl`, `silver.headcount`):**
    *   **Purpose:** Normalization and standardization.
    *   **What it holds:** Intermediate tables where jagged data structures from different companies are cleansed, transformed, and mapped into standardized internal taxonomies.
*   **Gold Layer (`gold.kpi_monthly`):**
    *   **Purpose:** Live executive reporting.
    *   **What it holds:** A massive, wide table containing 104 fully calculated and consolidated KPIs. It contains exactly one row per portfolio company per month.
    *   **Key Data Points:** `total_arr`, `tech_gross_margin_pct`, `ebitda_actual`, `cash_runway_months`, `revenue_churn_pct`, `rule_of_40`, etc.

### Phase 3: The Executive Dashboard
**Goal:** Surface the Gold Layer data visually for immediate Private Equity analysis.

Hosted on **Streamlit Community Cloud** (`dashboard/pe_app.py`), this UI provides an instant strategic snapshot.

1.  **Secure Connection:** Uses Streamlit Secrets (`gcp_service_account`) to securely authenticate with GCP and pull live data from the Gold Layer.
2.  **Resilience Mechanism:** If BigQuery is unreachable, the system automatically falls back to an encrypted, localized `gold_phase1_data.csv`.
3.  **Anomaly Detection Engine (PE Red Flags):** An intelligent function processes the localized payload before rendering to scan for structural business risks. This function acts as a programmatic investment committee.
    *   *Ruleset Examples:*
        *   If `cash_runway_months < 12` ➔ Triggers a Liquidity Alert.
        *   If `revenue_churn_pct > 5` ➔ Triggers a Churn Wave Alert.
        *   If `tech_gross_margin_pct < 75` ➔ Triggers a COGS/Scalability Alert.
        *   If `rule_of_40 < 0.20` ➔ Triggers an Efficiency Erosion Alert.
    *   *Output:* A highly visible expander at the top of the dashboard specifying the severity (🟡 WARNING or 🔴 CRITICAL) and actionable directives.
4.  **Visual Renderings:** Employs `plotly` to render complex ARR waterfalls, debtor aging bar charts, and product module movement heatmaps.

### Phase 4: AI Data Analyst (Cross-Layer Intelligence)
**Goal:** Empower users to perform ad-hoc, highly technical data analysis using only natural language.

1.  **The Interface:** A dedicated chat interface built in Streamlit (`dashboard/pages/1_🤖_AI_Data_Analyst.py`).
2.  **LLM Configuration:** Uses the `google-generativeai` package to connect to the **Gemini 2.5 Flash** model. API Keys are securely managed via Streamlit Secrets.
3.  **Prompt Engineering & Context Injection:** Before the user's question is sent to the LLM, the system constructs a dense "System Prompt". This prompt fully describes the schemas for both the **Gold Layer** (104 columns) and the **Bronze Layer**.
4.  **Strategic LLM Inference:** When a user asks a question, the LLM determines the required data depth:
    *   *High-Level Query ("Which company has the worst Rule of 40?"):* The LLM targets the Gold Layer.
    *   *Audit Query ("Where exactly did the EBITDA number come from for Portco Alpha in Feb?"):* The LLM targets the Bronze Layer, fetching specific cell configurations.
5.  **Code Execution (Text-to-SQL):** The LLM outputs a raw Google Standard SQL query. The Streamlit backend securely passes this query to the BigQuery Client.
6.  **Results Delivery:** BigQuery executes the synthesized SQL and returns the dataset. The backend converts this to a Pandas DataFrame and renders the exact tabular answer in the chat UI, alongside the SQL query used.

---

## 3. Summary of Technologies Used
*   **Infrastructure & Data Warehouse**: Google Cloud Storage (GCS), Google Cloud Functions, Google BigQuery.
*   **Backend Automation & Parsing**: Python 3.x, Pandas, Openpyxl.
*   **Frontend & Visualization**: Streamlit, Plotly, Streamlit Community Cloud.
*   **Artificial Intelligence**: Google Gemini 2.5 Flash API.

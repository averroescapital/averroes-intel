# Averroes Portfolio Intel — Project Brief

**Purpose of this file:** This is the living source of truth for any AI/assistant (or human) picking up the project cold. Read this first, then CHANGELOG.md. Update whenever non-trivial work lands.

**Last updated:** 2026-04-16

---

## 1. One-line summary

PE-grade portfolio monitoring platform for Averroes Capital. Ingests monthly Management Accounts (Excel) from portfolio companies into a GCP medallion warehouse (bronze/silver/gold), computes ~100 KPIs, surfaces them in a Streamlit dashboard for the IC/GP team, runs anomaly detection, and exposes a Gemini-powered natural-language data analyst over the full stack.

**Primary audience:** Internal IC / GP team (Averroes).
**Primary user (right now):** Ishu (analyst/ops).

---

## 2. Current phase

**V2 migration near-complete.** Era-based Python parsers are done (all three eras), dashboard is cut over to `gold.kpi_monthly_v2`, Customer Numbers + Covenant sheets are parsed. Remaining: run backfill (`deploy/migrate_v2.sh`) to populate BQ, verify dashboard against live data, then decommission legacy `gold.kpi_monthly`.

---

## 3. Portfolio company roster

| Portco | Stage | Entry | Entry ARR | Cadence | Status |
|---|---|---|---|---|---|
| **Portco Alpha** | Series B | 2023-01 | £2.8M | Monthly (day 10) | **Live** — only portco with a working parser + real data |
| Portco Beta | Series A | 2023-07 | £1.5M | Monthly (day 15) | Config only, no parser |
| Portco Gamma | Series B | 2024-01 | £4.2M | Quarterly (day 20) | Config only, no parser |

Only **Alpha** has a parser (`deploy/parsers/alpha_parser.py`) and live data. Beta/Gamma exist as taxonomy entries but are not yet onboarded.

### Alpha business model
Hospitality SaaS with three business lines:
- **Ecommerce** (`ecommerce`) — success fees + setup fees + payment fees
- **EMS** (`ems`) — subscription + setup + hardware
- **Services** (`services`)

Tech ARR = Ecommerce ARR + EMS ARR (Services is project-based, not recurring).
Currency: GBP. All monetary figures stored in £k unless suffixed otherwise.

---

## 4. Infrastructure

- **GitHub:** `github.com/averroescapital/averroes-intel` (public repo, 34+ commits)
- **GitHub org:** `averroescapital`
- **GCP project:** `averroes-portfolio-intel`
- **Region:** `europe-west2`
- **gcloud auth:** Run `gcloud auth login` + `gcloud config set project averroes-portfolio-intel` before any deploy/backfill commands
- **GCS bucket:** `averroes-portfolio-intel-portfolio-data`
- **Ingest path:** `gs://{bucket}/portco-alpha/ma-files/*.xlsx`
- **Cloud Function:** `portfolio-data-ingest` (gen2, python311, 512MB, 300s timeout)
  - Triggered by `google.cloud.storage.object.v1.finalized`
  - Entry point: `process_file` in `functions/ingest/`
  - Secret: `gemini_api_key` (from Secret Manager)
- **Anomaly function:** `portfolio-anomaly-detect` (scheduled, generates alerts + Gemini exec commentary)
- **BI frontend:** Streamlit Community Cloud (`dashboard/app.py` and `dashboard/pe_app.py`)
  - Pages: `1_🤖_AI_Data_Analyst.py`, `2_📊_Era_View.py`
  - Auth: `gcp_service_account` via Streamlit Secrets
  - Resilience: falls back to local `gold_phase1_data.csv` if BQ unreachable

---

## 5. Era model (important)

Alpha's MA files evolved over time. The v2 parser branches by era because sheet availability and schema differ dramatically. 16 MA files total, spanning 2024-11 → 2026-02.

| Era | Months | Sheets available | Parser behaviour |
|---|---|---|---|
| **Era 1** | 2024-11 → 2025-10 (12 files) | P&L Summary + Headcount only (~15-23 sheets). `P&L Summary ` has trailing-space drift. | Extract top-line revenue/cost totals + headcount. No BL split, no KPI sheet, no waterfall. |
| **Era 2** | 2025-11 → 2025-12 (2 files) | + P&L Detail + Balance Sheet (~32 sheets) | Unlocks BL-level revenue/contribution (ecommerce/ems/services) via P&L Detail. Balance sheet → cash, NWC, net debt. |
| **Era 3** | 2026-01 → 2026-02 (2 files) | + Financial KPIs + Revenue Waterfall (~36 sheets) | Full KPI suite + revenue waterfall bridge (wf_* columns). |

Sheet-name drift to watch: `P&L Summary ` (trailing space) in Era 1 files. Parser must tolerate this.

Canonical P&L Detail row layout (Era 2+): R5-R8 Revenue (Ecom/EMS/Services/Total) → R9-R12 Direct Costs → R13-R16 Staff Costs → R17-R20 Direct Contribution → overheads.

---

## 6. Data model

### Bronze — `bronze.raw_management_accounts`
Every extracted cell as a row. Schema: `sheet_name, row_label, column_label, value, source_cell`. Single source of truth for audit.

### Silver — `silver.kpi_long` (v2, long-format)
```
portco_id, period (DATE, 1st of month), kpi (canonical name), value FLOAT64,
value_type ('actual'|'budget'|'prior_year'|'ytd_actual'|'ytd_budget'),
business_line ('ecommerce'|'ems'|'services'|'total'|NULL),
era ('era1'|'era2'|'era3'),
source_file, source_sheet, source_cell, ingested_at
```
Partitioned by month, clustered on `portco_id, kpi`. Replaces legacy `silver.normalised_kpis`.

### Gold — `gold.kpi_monthly_v2` (dashboard-ready wide)
One row per portco × period. Key groups:
- **Tech MRR/ARR:** `tech_mrr_actual/budget/prior_year/ytd_actual`, `tech_arr`, `ecommerce_arr`, `ems_arr`, `ecommerce_mrr_actual`, `ems_mrr_actual`
- **Revenue by BL:** `revenue_{ecommerce|ems|services}_{actual|budget|prior_year}`, `revenue_total_*`, `revenue_total_ytd_*`, `revenue_yoy_growth_pct`, `revenue_vs_budget_pct`
- **Ecommerce breakdown:** `revenue_ecom_{success_fees|setup_fees|payment_fees}`
- **EMS breakdown:** `revenue_ems_{subscription|setup|hardware}`
- **Contribution & direct costs:** `contribution_{ecommerce|ems|services|total}`, `direct_costs_{ecommerce|ems|services}`
- **P&L bottom line:** `total_overheads`, `ebitda_{actual|budget|prior_year}`, `ebitda_less_capex`
- **Balance sheet:** `cash_balance`, `net_working_capital`, `net_debt`, `cash_burn_monthly`
- **Headcount:** `total_headcount`
- **Modules (Era 2+):** `modules_live_total`, `modules_live_{ecommerce|ems|services}`, `modules_pipeline`
- **Financial KPIs (Era 3):** `arpc_actual`, `tech_gross_margin_pct`, `rule_of_40`, `revenue_churn_pct`
- **Metadata:** `currency`, `data_source`, `era`, `fy`, `fy_quarter`, `fy_month_num`, `computed_at`

Partitioned by month, clustered on `portco_id`.

### Gold — `gold.kpi_monthly` (legacy, ~160 cols)
Preserved untouched for archaeology. Populated by KPITracker ingest (FY23–FY26) + old MA MERGE logic. Has `wf_*` Revenue Waterfall Bridge columns added via `bq_schemas/alter_add_wf_columns.sql` (wf_revenue_start, wf_one_off_prev, wf_one_off_ytd, wf_recurring_growth, wf_arr_ytg, wf_weighted_pipeline, wf_budget_assumptions, wf_revenue_gap, wf_revenue_end). Dashboard cutover to `kpi_monthly_v2` is pending.

### Gold views
- `gold.revenue_ltm_by_bl` — trailing 12-month revenue per BL
- `gold.tech_arr_split` — Tech ARR = Ecommerce + EMS, with share %
- `gold.modules_live_view` — Era 2+ module counts per BL

### Customer Numbers sheet (parsed as of 2026-04-15)
The "Customer Numbers" cross-tab sheet (present in Era 3 files, 36 sheets) contains per-BL property/module counts, revenue, and ARPC across all months. The parser now extracts ALL populated month-columns (backfilling from Aug 2025 → current). KPIs emitted: `MODULES_LIVE_ECOMMERCE/_EMS/_SERVICES/_TOTAL`, `MODULES_BUDGET_*`, `CUSTOMER_REVENUE_*`, `ARPC_*`, and geo-level property counts (`PROPERTIES_UK_*`, `PROPERTIES_IRELAND_*`, `PROPERTIES_ITALY_*`, `PROPERTIES_SPAIN_UAE_*`). Revenue values are converted from absolute £ to £k to match the rest of the pipeline.

The gold columns `modules_live_ecommerce / _ems / _services` will now populate automatically on the next backfill run — no schema change needed.

### Covenant sheets (parsed as of 2026-04-16)
Two covenant sheets are now parsed from Era 2+ files:

**GL Covenants:** ARR covenant (actual vs covenant totals, threshold 0.9, ratio), Interest Cover (interest charge, EBITDA, ratio), Debt Service Ratio, Cash Minimum Balance. Labels in cols B/C; total row identified by absence of labels + both D/E populated.

**Averroes Guard Rails:** Five compliance blocks — Revenue (95% of budget), MRR, Contribution (85% of budget), EBITDA less Capex, Cash Balance. Each block has covenant YTD, actual YTD, and ratio. Section headers in col B; stop at 'KPIs' section (different layout). Dashboard View 7 maps Guard Rails KPIs to actual-vs-covenant charts with dynamic RAG status (green >1.0, amber 0.95-1.0, red <0.95).

22 new columns added to `gold.kpi_monthly_v2` (`gl_arr_actual/_covenant/_ratio/_threshold`, `gl_interest_cover_ratio`, `gl_debt_service_ratio`, `gl_cash_min_balance`, `gr_revenue/mrr/contribution/ebitda_capex/cash_*`).

---

## 7. KPI taxonomy (see `kpi_taxonomy.yaml`)

~25 canonical KPIs across 7 categories. Each has name, display_name, unit, benchmark, benchmark_rule, frequency, aliases.

- **Revenue:** arr, new_arr, expansion_arr, churn_arr, net_new_arr, arr_growth_rate
- **Retention:** nrr (benchmark 110), grr (90), logo_churn_rate (<5)
- **Unit Economics:** gross_margin (>70), cac, cac_payback (<12), ltv_cac (>3)
- **Efficiency:** net_burn, runway (>18), rule_of_40 (>40), ebitda_margin (path to 20)
- **GTM:** pipeline_coverage (>3x), quota_attainment (>70), win_rate (>30), sales_cycle
- **Customer:** nps (>50), csat (>85), customer_health_score (>70)
- **People:** total_fte, arr_per_fte (>150), enps (>30), regrettable_attrition (<10)

---

## 8. Anomaly / RAG rules

Defined in `kpi_taxonomy.yaml` → `anomaly_rules`. **Red** (action required) and **amber** (investigate) severities.

Red triggers: NRR drops >2pp MoM; pipeline <3x for 2+ months; runway <12 months; ≥2 KPIs deteriorating simultaneously.
Amber triggers: Rule of 40 <30 for 3+ months; gross margin falling while ARR rising (COGS creep); NPS <40 for 2+ months; win rate down while pipeline up.

Each rule has a prescribed action (e.g., "Churn wave incoming in 60-90 days. Pull churn cohort data immediately.").

Dashboard red-flag logic (in `pe_app.py`) runs a programmatic IC pre-check:
- cash_runway_months < 12 → Liquidity Alert
- revenue_churn_pct > 5 → Churn Wave Alert
- tech_gross_margin_pct < 75 → COGS/Scalability Alert
- rule_of_40 < 0.20 → Efficiency Erosion Alert

---

## 9. Repo layout

```
averroes-portfolio-intel/
├── PROJECT_BRIEF.md            ← this file
├── CHANGELOG.md                ← session-by-session log
├── README.md                   ← deploy instructions
├── averroes_architecture_guide.md
├── portfolio_analytics_documentation.md
├── kpi_taxonomy.yaml           ← canonical KPIs + anomaly rules
├── deploy.sh                   ← v1 deploy
├── generate_sample_data.py     ← 30 months synthetic w/ Sept 2024 anomaly
├── trigger_local.py
├── Alpha_Management_Accounts_2025.xlsx
│
├── bq_schemas/
│   ├── schemas.sql             ← v1 bronze/silver/gold
│   ├── pe_schemas.sql
│   ├── phase1_schema.sql
│   ├── gold_views.sql
│   ├── alter_add_wf_columns.sql ← wf_* waterfall bridge cols on legacy gold.kpi_monthly
│   └── silver_gold_v2.sql      ← v2: silver.kpi_long + gold.kpi_monthly_v2 + 3 views
│
├── deploy/
│   ├── migrate_v2.sh           ← one-shot v1→v2 migration runbook
│   ├── Dockerfile
│   ├── load_bq_backfill.py     ← loads CSVs into BQ v2 tables
│   ├── ma_parser.py
│   ├── phase1_parser.py
│   ├── pe_app.py
│   ├── parsers/
│   │   └── alpha_parser.py     ← Alpha-specific strategy parser
│   ├── requirements.txt
│   └── averroes-dashboard-deploy.zip
│
├── functions/
│   ├── ingest/                 ← Cloud Function: GCS → bronze/silver
│   └── anomaly_detect/         ← Cloud Function: gold → alerts + Gemini commentary
│
├── dashboard/
│   ├── app.py                  ← "Averroes Portfolio Portal" (primary, 7 views)
│   ├── pe_app.py               ← earlier KPI-tracker-shaped prototype
│   ├── pages/
│   │   ├── 1_🤖_AI_Data_Analyst.py
│   │   └── 2_📊_Era_View.py
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── capture_live.py
│   ├── gold_dummy.py
│   ├── gold_*.csv              ← fallback payloads
│   ├── silver_kpi_long.csv     ← local backfill output
│   └── temp_data/
│
├── config/
│   └── kpi_taxonomy.yaml       ← duplicated here for legacy function packaging
│
├── analysis/
│   ├── canonical_schema.json
│   ├── profile_detail.csv
│   ├── profile_ma_files.py     ← script that audits all 16 MA files for sheet presence / drift
│   └── profile_report.md       ← human-readable output; THE reference for era boundaries
│
└── sample_data/                ← synthetic portco files for testing
```

Dashboard has 7 views in `app.py`: Executive Summary, Revenue & ARR, Profitability, Cash, People, Product, Risk.

---

## 10. How to run things

**V2 migration (the command we just discussed):**
```bash
cd <repo-root>
bash deploy/migrate_v2.sh
```
Prereqs: `gcloud auth login` + `gcloud auth application-default login`, `gcloud config set project averroes-portfolio-intel`, 16 MA files in `./raw_ma_files/` (override with `MA_FILES_DIR=...`).
Steps: applies v2 SQL → runs `functions/ingest/backfill.py` → loads CSVs via `deploy/load_bq_backfill.py` → uploads MA files to GCS → redeploys ingest function → runs sanity SELECT.

**Dashboard locally:**
```bash
cd dashboard
python3 -m venv .venv && source .venv/bin/activate
pip install streamlit pandas plotly google-cloud-bigquery db-dtypes
streamlit run app.py
```

**Manual ingest trigger:**
```bash
gsutil cp sample_data/portco-alpha/*.xlsx gs://averroes-portfolio-intel-portfolio-data/portco-alpha/ma-files/
gcloud functions logs read portfolio-data-ingest --region=europe-west2 --gen2 --limit=50
```

**Manual anomaly detection:**
```bash
gcloud functions call portfolio-anomaly-detect --region=europe-west2 --gen2
```

---

## 11. Known fragilities / TODO

- **Parser brittleness across eras.** Cell coordinates shift between eras; `P&L Summary ` trailing space in Era 1 caused parser misses — keep tolerant lookups.
- **~~Customer Numbers sheet not parsed.~~** DONE (2026-04-16). Both `deploy/parsers/alpha_parser.py` and `functions/ingest/parsers/era3_parser.py` now extract per-BL modules + revenue + ARPC + geo from this sheet. Needs backfill run to populate gold.
- **~~Dashboard still points at legacy `gold.kpi_monthly`.~~** DONE (2026-04-16). `app.py` now queries `gold.kpi_monthly_v2` with a `harmonize_v2_columns()` compat layer. Era View Module 5 shows per-BL stacked bars.
- **~~Covenant data not parsed (View 7 showing zeros).~~** DONE (2026-04-16). Both parser codepaths now extract 23 KPIs from `GL Covenants` (ARR covenant ratio, Interest Cover, Debt Service, Cash Min) and `Averroes Guard Rails` (Revenue/MRR/Contribution/EBITDA-less-Capex/Cash — each with covenant, actual, ratio). Dashboard compat layer maps these to View 7 with dynamic RAG status. Gold v2 schema includes 22 new covenant columns. Needs backfill run.
- **`gold_phase1_data.csv` fallback drifts** from live BQ — needs a refresh cadence or a script.
- **Beta / Gamma onboarding** requires net-new parser classes in `deploy/parsers/`.
- **AI Analyst context** — if we add many more columns, the Gemini system prompt will need condensation.

---

## 12. Working conventions

- **Living brief.** This file is updated every session where non-trivial work happens (new schema, new parser behaviour, new dashboard view, new known-issue). Previous states live in git.
- **Changelog.** `CHANGELOG.md` holds a dated entry per session with "what changed + why + any new TODOs".
- **Strategy-pattern parsing.** All portco-specific logic lives in `deploy/parsers/<portco>_parser.py`. `ma_parser.py` is the dispatcher.
- **Bronze is immutable.** Any fix goes through silver/gold. Re-parsing is cheap; destroying bronze is forbidden.
- **Canonical KPI names.** Defined in `kpi_taxonomy.yaml`; never silently invent new KPI strings in silver.

---

## 13. Glossary

- **MA** — Management Accounts (monthly Excel file from portco finance team)
- **Portco** — Portfolio company
- **BL** — Business Line (ecommerce / ems / services for Alpha)
- **IC** — Investment Committee
- **GP / LP** — General Partner (Averroes) / Limited Partner
- **Medallion** — bronze (raw) → silver (normalized) → gold (reporting) data architecture
- **LTM / YTD / YoY / MoM** — Last-Twelve-Months / Year-to-Date / Year-over-Year / Month-over-Month
- **RAG** — Red/Amber/Green health indicator
- **Era 1/2/3** — Tiers of MA file completeness (see §5)
- **ARR / MRR / NRR / GRR** — Annual/Monthly Recurring Revenue; Net/Gross Revenue Retention
- **wf_\*** — Revenue Waterfall Bridge columns on legacy `gold.kpi_monthly`

# Averroes Portfolio Intel — Changelog

Session-by-session log. Most recent first. Pair with `PROJECT_BRIEF.md` (the living overview).

Format per entry:
```
## YYYY-MM-DD — short headline
**What:** the change in 1-2 lines.
**Why:** motivation / context.
**Files touched:** list
**Follow-ups:** any new TODOs that came out of this.
```

---

## 2026-04-28 — FY24 support, Journey Analytics, Cloud Function hardening (v3.0)
**What:** Major expansion: added FY24 (Nov 2023–Oct 2024) support via era1 parser rewrite, new Journey Analytics dashboard page, Cloud Function file filter fix, Reprocess GCS Files buttons on all pages, system artifact updated to v3.0.0. Total MA files: 28 (up from 16).
**Why:** FY24 files uploaded to GCS weren't being processed (file filter too restrictive + parser couldn't handle FY24 layout). Journey Analytics requested for investor-deck-style trailing 12-month views.
**Changes:**
1. **Era 1 parser rewrite** (`functions/ingest/parsers/era1_parser.py`, 297 lines): P&L section (R5-R18) hardcoded (stable). Bottom section (R19+) now uses label scanning via `_BOTTOM_LABEL_MAP` to handle FY24 compact, FY24 extended, and FY25 layouts. Added `_has_prior_year_column()` to detect Col 10 as Variance (FY24) vs Prior Year (FY25). Ecommerce/EMS P&L also label-scanned.
2. **Era router fallback** (`parsers/router.py`, 52 lines): Added broader fallback — checks for any P&L/Summary-like sheet before defaulting to era3. Added sheet name logging.
3. **Cloud Function filter** (`functions/ingest/main.py`): Relaxed from "Management Accounts"/"MAfile" check to any `.xlsx/.xls/.xlsm`. Added temp/hidden file skip (`~$`, `.`). Added try/except with traceback around parse. Added download size + period extraction logging.
4. **Journey Analytics page** (`pages/3_📈_Journey_Analytics.py`, 709 lines): Investor-deck-style trailing 12-month charts with month selector dropdown. Sections: Revenue & ARR, Direct Contribution, EBITDA & Cash, summary comparison table. `.fillna(0)` guards on all `.astype(int)` calls.
5. **Reprocess GCS Files button** on all pages (`pe_app.py`, `Era_View.py`, `Journey_Analytics.py`): Copies blobs in-place to re-trigger Cloud Function without re-uploading.
6. **requirements.txt**: Added `google-cloud-storage>=2.14.0` for Reprocess button on Streamlit Community Cloud.
7. **common.py**: Fixed missing "may" in `period_from_filename` regex.
8. **silver_gold_v2.py**: `SERVICES_ARR` only computed when `SERVICES_MRR` exists in source (era3 only). Reverted attempted derivation from revenue.
9. **System artifact** updated to v3.0.0 (1040 lines).
**Files touched:**
- `functions/ingest/parsers/era1_parser.py` — full rewrite
- `functions/ingest/parsers/router.py` — fallback + logging
- `functions/ingest/parsers/common.py` — regex fix
- `functions/ingest/main.py` — file filter + error handling
- `functions/ingest/silver_gold_v2.py` — Services ARR guard
- `pages/3_📈_Journey_Analytics.py` — new file
- `pages/2_📊_Era_View.py` — sidebar buttons
- `pe_app.py` — sidebar buttons
- `requirements.txt` — google-cloud-storage added
- `Averroes_Portfolio_Intel_System_Artifact.html` — v3.0.0
**Tested against:** All 28 MA files (12 FY24 + 12 FY25 + 2 Era 2 + 2 Era 3) parse correctly.
**Follow-ups:**
- Decommission legacy `gold.kpi_monthly` table
- Onboard Portco Beta / Gamma
- Consider adding NRR/GRR charts once retention data is available
- Add covenant trend forecasting (6-month forward from GL Covenants cols F+)

---

## 2026-04-16 — GL Covenants + Averroes Guard Rails parser (View 7 goes live)
**What:** Added covenant parsing to both parser codepaths (Cloud Function + local backfill). Extracts 23 KPIs from two sheets: GL Covenants (ARR covenant ratio, Interest Cover, Debt Service Ratio, Cash Min Balance) and Averroes Guard Rails (Revenue/MRR/Contribution/EBITDA-less-Capex/Cash — each with covenant, actual, and ratio). Updated the dashboard compat layer to map these KPIs to View 7 columns with dynamic RAG status (green/amber/red based on ratio thresholds). Added 22 covenant columns to the gold v2 SQL schema.
**Why:** View 7 (Covenants & Risk) was showing zero placeholders because no parser read the covenant sheets. Now it displays real compliance data with actual-vs-covenant tracking, headroom analysis, and breach detection.
**KPIs added (GL Covenants):**
- `GL_ARR_ACTUAL/COVENANT/RATIO/THRESHOLD` — ARR covenant compliance
- `GL_INTEREST_COVER_INTEREST/EBITDA/RATIO` — Interest coverage
- `GL_DEBT_SERVICE_RATIO` — Debt service ratio
- `GL_CASH_MIN_BALANCE` — Cash minimum covenant
**KPIs added (Averroes Guard Rails):**
- `GR_REVENUE_ACTUAL_YTD/COVENANT_YTD/RATIO` — Revenue vs covenant (95% of budget)
- `GR_MRR_ACTUAL/COVENANT/RATIO` — MRR covenant compliance
- `GR_CONTRIBUTION_ACTUAL_YTD/COVENANT_YTD/RATIO` — Contribution (85% of budget)
- `GR_EBITDA_CAPEX_ACTUAL_YTD/COVENANT_YTD/RATIO` — EBITDA less Capex
- `GR_CASH_ACTUAL/COVENANT/RATIO` — Cash balance covenant
**Files touched:**
- `functions/ingest/parsers/schema.py` — registered 24 new covenant KPIs (era2+era3)
- `functions/ingest/parsers/era2_parser.py` — added `parse_gl_covenants()` + `parse_guard_rails_covenants()`
- `functions/ingest/parsers/era3_parser.py` — wired covenant parsers into era3 `parse()`
- `deploy/parsers/alpha_parser.py` — added sections 7 (GL Covenants) + 8 (Guard Rails) with matching logic
- `dashboard/app.py` — `harmonize_v2_columns()` maps Guard Rails KPIs to View 7 columns, dynamic RAG status
- `bq_schemas/silver_gold_v2.sql` — added 22 covenant columns to `gold.kpi_monthly_v2`
**Tested against:** `functions/ingest/local_MAfileFeb26.xlsx` (Feb 2026, Era 3). Both parsers produce 23 covenant KPIs with matching values and no duplicates.
**Follow-ups:**
- Re-run backfill to populate silver/gold with covenant data
- Verify View 7 renders correctly against live BQ data
- Consider adding covenant trend charts (6-month forecast from GL Covenants cols F+)

---

## 2026-04-16 — Dashboard cutover to gold.kpi_monthly_v2
**What:** Switched the primary dashboard (`app.py`) from legacy `gold.kpi_monthly` (~160 cols) to the era-based `gold.kpi_monthly_v2`. Added a `harmonize_v2_columns()` compat layer that derives all legacy column names so the 7 views continue working unchanged. Upgraded Era View Module 5 to show per-BL stacked bars (Ecommerce/EMS/Services) instead of totals only.
**Why:** The v2 schema is the target for all new parser work. Running two parallel gold tables was causing drift — the legacy table wasn't getting Customer Numbers, waterfall, or other v2 enhancements. Cutover means a single source of truth.
**What changed in the views:**
- All 7 views: now use v2 data via the compat layer
- Product Metrics (View 6): `modules_live_ecommerce/_ems/_services` populated from Customer Numbers
- Module 5 (Era View): stacked bar chart by BL with per-BL summary cards + MoM deltas
- Covenants & Risk (View 7): gracefully degrades (zero placeholders) since covenant columns don't exist in v2 yet
- Sample data generator aligned with v2 column names + era tagging
**Files touched:**
- `dashboard/app.py` — `harmonize_v2_columns()`, `generate_sample_data()` rewritten for v2, `load_data()` → `gold.kpi_monthly_v2`
- `dashboard/pages/2_📊_Era_View.py` — Module 5 upgraded to per-BL stacked bars with fallback, raw data table expanded
**Follow-ups:**
- Parse `GL Covenants` + `Averroes Guard Rails` sheets → populate covenant columns so View 7 shows real data
- Verify all views against live BQ data once backfill completes
- Consider removing the legacy `gold.kpi_monthly` table and `pe_app.py` once confirmed stable

---

## 2026-04-16 — Customer Numbers parser extension (per-BL modules go live)
**What:** Extended both parser codepaths to read the "Customer Numbers" cross-tab sheet from Era 3 MA files. Extracts all populated month-columns (backfills Aug 2025 → current) so a single Era 3 file surfaces 7 months of history.
**Why:** Module 5 in the dashboard only showed Live vs Pipeline totals. The gold columns (`modules_live_ecommerce/_ems/_services`) existed but were empty because no parser read the source sheet. Now they will populate on the next backfill.
**KPIs added:**
- `MODULES_LIVE_ECOMMERCE/EMS/SERVICES/TOTAL` — actual property counts per BL
- `MODULES_BUDGET_ECOMMERCE/EMS/SERVICES/TOTAL` — budget property counts
- `CUSTOMER_REVENUE_ECOMMERCE/EMS/SERVICES/TOTAL` — actual revenue per BL (£ → £k)
- `ARPC_ECOMMERCE/EMS/SERVICES/TOTAL` — average revenue per customer per BL
- `PROPERTIES_{UK|IRELAND|ITALY|SPAIN_UAE}_{ECOM|EMS|SERVICES|TOTAL}` — geo-level counts
**Files touched:**
- `deploy/parsers/alpha_parser.py` — added section 6 (Customer Numbers), tolerant `_find_sheet` helper, fixed `reporting_period` scoping
- `functions/ingest/parsers/era3_parser.py` — added `parse_customer_numbers()` with `_CUST_*` mapping tables, called at end of `parse()`
- `functions/ingest/parsers/schema.py` — registered all new KPIs in `KPI_CATALOG`
- `PROJECT_BRIEF.md` — updated schema gap section + fragilities
**Tested against:** `functions/ingest/local_MAfileFeb26.xlsx` (Feb 2026, Era 3). Both `deploy/parsers` and `functions/ingest/parsers` produce correct output: Ecom 499, EMS 414, Services 148, Total 722 for Feb 2026.
**Follow-ups:**
- Re-run backfill (`python3 functions/ingest/backfill.py`) to populate silver/gold with new KPIs
- Dashboard Module 5 view should now show per-BL stacks once gold refreshes
- Consider adding `MODULES_LIVE_TOTAL` to the existing `gold.modules_live_view` WHERE clause if needed

---

## 2026-04-15 — Established PROJECT_BRIEF.md + CHANGELOG.md
**What:** Created a persistent project brief (`PROJECT_BRIEF.md`) and this changelog to retain full context across assistant sessions. Synthesised from README, architecture guide, portfolio analytics doc, `silver_gold_v2.sql`, `migrate_v2.sh`, `kpi_taxonomy.yaml`, and `analysis/profile_report.md`.
**Why:** Re-deriving project context every session was wasting time; Ishu wants one place that stays current.
**Files touched:**
- `PROJECT_BRIEF.md` (new) — full platform overview, era model, schemas, run commands, fragilities
- `CHANGELOG.md` (new) — this file
**Follow-ups:**
- Extend Alpha parser to read "Customer Numbers" sheet → unlocks per-BL modules in Module 5 (columns already in `gold.kpi_monthly_v2`).
- Finish dashboard cutover from legacy `gold.kpi_monthly` to `gold.kpi_monthly_v2`.
- Put a refresh cadence on `dashboard/gold_phase1_data.csv` fallback so it doesn't drift.
- Onboard Portco Beta (parser + taxonomy already partially configured).

---

## Pre-brief history (reconstructed from repo state)

These are the major milestones visible from the codebase before this brief existed:

- **Phase 1:** Initial pipeline — GCS + Cloud Functions + BigQuery medallion, `phase1_parser.py` + `phase1_schema.sql`, `gold_phase1_data.csv` as fallback.
- **KPITracker ingest:** Populated legacy `gold.kpi_monthly` with ~160 columns of FY23-FY26 historical data.
- **Revenue Waterfall Bridge:** `wf_*` columns added to legacy gold via `alter_add_wf_columns.sql`.
- **Streamlit dashboard v1:** `pe_app.py` (earlier, KPI-tracker shaped), then `app.py` (primary, 7-view executive portal with BQ→CSV fallback).
- **AI Data Analyst page:** Gemini 2.5 Flash text-to-SQL chat over bronze + gold (`dashboard/pages/1_🤖_AI_Data_Analyst.py`).
- **Era profiling:** `analysis/profile_ma_files.py` audited all 16 Alpha MA files → identified Era 1/2/3 boundaries based on sheet availability.
- **V2 schema & migration:** `bq_schemas/silver_gold_v2.sql` defines long-format `silver.kpi_long` + wide `gold.kpi_monthly_v2` + 3 derived views. `deploy/migrate_v2.sh` is the one-shot runbook. Alpha-specific `deploy/parsers/alpha_parser.py` implements Strategy Pattern dispatch from `ma_parser.py`.
- **Current work:** V2 cutover in progress — schemas applied, backfill script ready, dashboard partial migration via `dashboard/pages/2_📊_Era_View.py`.

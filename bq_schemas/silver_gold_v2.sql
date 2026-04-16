-- =====================================================================
-- Averroes Portfolio Intel - Silver + Gold v2 schemas (era-based parser)
-- =====================================================================
-- Target project: averroes-portfolio-intel
-- Run:   bq query --use_legacy_sql=false < bq_schemas/silver_gold_v2.sql
-- =====================================================================

-- Datasets (no-op if exist)
CREATE SCHEMA IF NOT EXISTS `averroes-portfolio-intel.silver`
  OPTIONS (location = 'europe-west2');
CREATE SCHEMA IF NOT EXISTS `averroes-portfolio-intel.gold`
  OPTIONS (location = 'europe-west2');

-- ---------------------------------------------------------------------
-- SILVER: long-format, unit-normalized KPI rows
-- ---------------------------------------------------------------------
-- Drop & recreate — v2 schema replaces the old silver.normalised_kpis
DROP TABLE IF EXISTS `averroes-portfolio-intel.silver.kpi_long`;
CREATE TABLE `averroes-portfolio-intel.silver.kpi_long` (
  portco_id      STRING   NOT NULL,
  period         DATE     NOT NULL,           -- first-of-month
  kpi            STRING   NOT NULL,           -- canonical KPI name (see schema.py)
  value          FLOAT64,
  value_type     STRING,                      -- 'actual' | 'budget' | 'prior_year' | 'ytd_actual' | 'ytd_budget'
  business_line  STRING,                      -- 'ecommerce' | 'ems' | 'services' | 'total' | NULL
  era            STRING,                      -- 'era1' | 'era2' | 'era3'
  source_file    STRING,
  source_sheet   STRING,
  source_cell    STRING,
  ingested_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE_TRUNC(period, MONTH)
CLUSTER BY portco_id, kpi;

-- ---------------------------------------------------------------------
-- GOLD: dashboard-ready wide table (one row per portco x period)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS `averroes-portfolio-intel.gold.kpi_monthly_v2`;
CREATE TABLE `averroes-portfolio-intel.gold.kpi_monthly_v2` (
  portco_id                      STRING   NOT NULL,
  period                         DATE     NOT NULL,
  fy                             STRING,
  fy_quarter                     STRING,
  fy_month_num                   INT64,

  -- Tech MRR (authoritative, £k)
  tech_mrr_actual                FLOAT64,
  tech_mrr_budget                FLOAT64,
  tech_mrr_prior_year            FLOAT64,
  tech_mrr_ytd_actual            FLOAT64,

  -- Services MRR (from Financial KPIs, £k)
  services_mrr_actual            FLOAT64,
  services_mrr_budget            FLOAT64,
  services_mrr_prior_year        FLOAT64,
  services_mrr_ytd_actual        FLOAT64,

  -- Business-line MRR/ARR (derived)
  ecommerce_mrr_actual           FLOAT64,
  ems_mrr_actual                 FLOAT64,
  tech_arr                       FLOAT64,
  ecommerce_arr                  FLOAT64,
  ems_arr                        FLOAT64,
  services_arr                   FLOAT64,

  -- Revenues by business line (actual, budget, PY) in £k
  revenue_ecommerce_actual       FLOAT64,
  revenue_ecommerce_budget       FLOAT64,
  revenue_ecommerce_prior_year   FLOAT64,
  revenue_ems_actual             FLOAT64,
  revenue_ems_budget             FLOAT64,
  revenue_ems_prior_year         FLOAT64,
  revenue_services_actual        FLOAT64,
  revenue_services_budget        FLOAT64,
  revenue_services_prior_year    FLOAT64,
  revenue_total_actual           FLOAT64,
  revenue_total_budget           FLOAT64,
  revenue_total_prior_year       FLOAT64,
  revenue_total_ytd_actual       FLOAT64,
  revenue_total_ytd_budget       FLOAT64,
  revenue_yoy_growth_pct         FLOAT64,
  revenue_vs_budget_pct          FLOAT64,

  -- Ecommerce breakdown
  revenue_ecom_success_fees      FLOAT64,
  revenue_ecom_setup_fees        FLOAT64,
  revenue_ecom_payment_fees      FLOAT64,

  -- EMS breakdown
  revenue_ems_subscription       FLOAT64,
  revenue_ems_setup              FLOAT64,
  revenue_ems_hardware           FLOAT64,

  -- Direct contribution (actual, budget, PY)
  contribution_ecommerce         FLOAT64,
  contribution_ecommerce_budget  FLOAT64,
  contribution_ecommerce_py      FLOAT64,
  contribution_ems               FLOAT64,
  contribution_ems_budget        FLOAT64,
  contribution_ems_py            FLOAT64,
  contribution_services          FLOAT64,
  contribution_services_budget   FLOAT64,
  contribution_services_py       FLOAT64,
  contribution_total             FLOAT64,
  contribution_total_budget      FLOAT64,
  contribution_total_py          FLOAT64,
  contribution_total_ytd_actual  FLOAT64,
  contribution_total_ytd_budget  FLOAT64,

  -- Direct costs (actual + budget)
  direct_costs_ecommerce         FLOAT64,
  direct_costs_ecommerce_budget  FLOAT64,
  direct_costs_ems               FLOAT64,
  direct_costs_ems_budget        FLOAT64,
  direct_costs_services          FLOAT64,
  direct_costs_services_budget   FLOAT64,

  -- P&L bottom line
  total_overheads                FLOAT64,
  total_overheads_budget         FLOAT64,
  total_overheads_py             FLOAT64,
  ebitda_actual                  FLOAT64,
  ebitda_budget                  FLOAT64,
  ebitda_prior_year              FLOAT64,
  ebitda_less_capex              FLOAT64,
  ebitda_less_capex_budget       FLOAT64,

  -- YTD from P&L Summary
  revenue_total_ytd_py           FLOAT64,
  ebitda_ytd_py                  FLOAT64,

  -- Balance sheet
  cash_balance                   FLOAT64,
  cash_balance_bs_budget         FLOAT64,
  cash_balance_bs_prior          FLOAT64,
  net_working_capital            FLOAT64,
  nwc_budget                     FLOAT64,
  nwc_prior_month                FLOAT64,
  net_debt                       FLOAT64,
  cash_burn_monthly              FLOAT64,
  capex                          FLOAT64,
  capex_budget                   FLOAT64,

  -- Headcount (total + segments + payroll)
  total_headcount                FLOAT64,
  headcount_budget               FLOAT64,
  headcount_prior_year           FLOAT64,
  headcount_ecommerce            FLOAT64,
  headcount_ems                  FLOAT64,
  headcount_services             FLOAT64,
  headcount_central              FLOAT64,
  gross_payroll                  FLOAT64,
  gross_payroll_budget           FLOAT64,
  revenue_per_employee           FLOAT64,

  -- Modules (Era 2+)
  modules_live_total             FLOAT64,
  modules_live_ecommerce         FLOAT64,
  modules_live_ems               FLOAT64,
  modules_live_services          FLOAT64,
  modules_pipeline               FLOAT64,

  -- EBITDA YTD (cumulative within FY)
  ebitda_ytd_actual              FLOAT64,
  ebitda_ytd_budget              FLOAT64,

  -- Financial KPIs (Era 3)
  arpc_actual                    FLOAT64,
  arpc_budget                    FLOAT64,
  arpc_ytd_actual                FLOAT64,
  arpc_ytd_budget                FLOAT64,

  tech_gross_margin_pct          FLOAT64,
  tech_gross_margin_budget_pct   FLOAT64,
  tech_gross_margin_prior_pct    FLOAT64,
  tech_gross_margin_ytd_pct      FLOAT64,
  tech_gross_margin_ytd_budget_pct FLOAT64,

  ebitda_margin_pct              FLOAT64,
  ebitda_margin_budget_pct       FLOAT64,
  ebitda_margin_prior_pct        FLOAT64,
  ebitda_margin_ytd_pct          FLOAT64,
  ebitda_margin_ytd_budget_pct   FLOAT64,

  cash_balance_budget            FLOAT64,
  cash_balance_prior_month       FLOAT64,

  free_cash_conversion_month     FLOAT64,
  free_cash_conversion_budget    FLOAT64,
  free_cash_conversion_ytd       FLOAT64,

  indicative_ev                  FLOAT64,
  sm_efficiency                  FLOAT64,
  ytd_revenue_growth_pct         FLOAT64,
  ytd_revenue_growth_budget      FLOAT64,
  time_to_value_days             FLOAT64,

  rule_of_40                     FLOAT64,
  revenue_churn_pct              FLOAT64,
  revenue_churn_target           FLOAT64,

  -- Revenue Waterfall (Era 3)
  wf_revenue_start               FLOAT64,
  wf_one_off_prev                FLOAT64,
  wf_one_off_ytd                 FLOAT64,
  wf_recurring_growth            FLOAT64,
  wf_arr_ytg                     FLOAT64,
  wf_weighted_pipeline           FLOAT64,
  wf_budget_assumptions          FLOAT64,
  wf_revenue_gap                 FLOAT64,
  wf_revenue_end                 FLOAT64,

  -- GL Covenants (Era 2+)
  gl_arr_actual                  FLOAT64,
  gl_arr_covenant                FLOAT64,
  gl_arr_ratio                   FLOAT64,
  gl_arr_threshold               FLOAT64,
  gl_interest_cover_ratio        FLOAT64,
  gl_debt_service_ratio          FLOAT64,
  gl_cash_min_balance            FLOAT64,

  -- Averroes Guard Rails (Era 2+)
  gr_revenue_actual_ytd          FLOAT64,
  gr_revenue_covenant_ytd        FLOAT64,
  gr_revenue_ratio               FLOAT64,
  gr_mrr_actual                  FLOAT64,
  gr_mrr_covenant                FLOAT64,
  gr_mrr_ratio                   FLOAT64,
  gr_contribution_actual_ytd     FLOAT64,
  gr_contribution_covenant_ytd   FLOAT64,
  gr_contribution_ratio          FLOAT64,
  gr_ebitda_capex_actual_ytd     FLOAT64,
  gr_ebitda_capex_covenant_ytd   FLOAT64,
  gr_ebitda_capex_ratio          FLOAT64,
  gr_cash_actual                 FLOAT64,
  gr_cash_covenant               FLOAT64,
  gr_cash_ratio                  FLOAT64,

  -- Metadata
  currency                       STRING,
  data_source                    STRING,
  era                            STRING,
  computed_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE_TRUNC(period, MONTH)
CLUSTER BY portco_id;

-- ---------------------------------------------------------------------
-- NOTE on gold.kpi_monthly (legacy table)
-- ---------------------------------------------------------------------
-- The original `gold.kpi_monthly` is a real table (~160 columns) populated
-- by the KPITracker ingest (FY23-FY26 historical) plus the old MA MERGE logic.
-- We keep it untouched for archaeology. The dashboard will be cut over to
-- read `gold.kpi_monthly_v2` directly (see dashboard/pe_app.py).

-- ---------------------------------------------------------------------
-- Useful derived view: LTM (last-twelve-months) revenues by business line
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `averroes-portfolio-intel.gold.revenue_ltm_by_bl` AS
SELECT
  portco_id,
  period,
  SUM(revenue_ecommerce_actual) OVER w AS ltm_revenue_ecommerce,
  SUM(revenue_ems_actual)       OVER w AS ltm_revenue_ems,
  SUM(revenue_services_actual)  OVER w AS ltm_revenue_services,
  SUM(revenue_total_actual)     OVER w AS ltm_revenue_total
FROM `averroes-portfolio-intel.gold.kpi_monthly_v2`
WINDOW w AS (
  PARTITION BY portco_id
  ORDER BY UNIX_DATE(period)
  RANGE BETWEEN 335 PRECEDING AND CURRENT ROW  -- ~12 months of days
);

-- ---------------------------------------------------------------------
-- Tech ARR split view — makes the "Tech ARR = Ecommerce ARR + EMS ARR"
-- identity trivially queryable from the dashboard.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `averroes-portfolio-intel.gold.tech_arr_split` AS
SELECT
  portco_id,
  period,
  era,
  tech_arr,
  ecommerce_arr,
  ems_arr,
  SAFE_DIVIDE(ecommerce_arr, tech_arr) AS ecommerce_share,
  SAFE_DIVIDE(ems_arr,       tech_arr) AS ems_share
FROM `averroes-portfolio-intel.gold.kpi_monthly_v2`
WHERE tech_arr IS NOT NULL;

-- ---------------------------------------------------------------------
-- Live modules view (Era 2+): monthly + by business line
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `averroes-portfolio-intel.gold.modules_live_view` AS
SELECT
  portco_id,
  period,
  era,
  modules_live_total,
  modules_live_ecommerce,
  modules_live_ems,
  modules_live_services,
  modules_pipeline
FROM `averroes-portfolio-intel.gold.kpi_monthly_v2`
WHERE modules_live_total IS NOT NULL;

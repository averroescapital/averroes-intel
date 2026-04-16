-- =============================================================
-- PHASE 1: PE KPI DASHBOARD - BIGQUERY SCHEMA
-- Aligned to Phase 1 KPI Dictionary (Sections A-E)
-- Location: europe-west2
-- Currency: GBP, £k unless stated
-- FY: Nov-Oct (Q1=Nov-Jan, Q2=Feb-Apr, Q3=May-Jul, Q4=Aug-Oct)
-- =============================================================

-- BRONZE LAYER (raw parsed cells from MA file)
CREATE TABLE IF NOT EXISTS `averroes-portfolio-intel.bronze.raw_management_accounts` (
  ingestion_id STRING NOT NULL,
  portco_id STRING NOT NULL,
  file_name STRING NOT NULL,
  sheet_name STRING NOT NULL,
  reporting_period DATE NOT NULL,
  row_label STRING,
  column_label STRING,
  value FLOAT64,
  value_type STRING,          -- 'actual', 'budget', 'prior_year', 'ytd_actual', 'ytd_budget'
  currency STRING DEFAULT 'GBP',
  unit STRING DEFAULT 'thousands',
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  source_cell STRING
);

-- GOLD LAYER (single wide table, one row per portco per month)
CREATE TABLE IF NOT EXISTS `averroes-portfolio-intel.gold.kpi_monthly` (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  fy STRING,                   -- 'FY26', 'FY25'
  fy_quarter STRING,           -- 'Q1', 'Q2', 'Q3', 'Q4'
  fy_month_num INT64,          -- 1-12 (1=Nov, 4=Feb, etc.)

  -- =============================================
  -- SECTION A: ARR / MRR / SCALE
  -- =============================================
  -- MRR
  tech_mrr_actual FLOAT64,           -- £ (not £k)
  tech_mrr_budget FLOAT64,
  tech_mrr_prior_year FLOAT64,
  services_mrr_actual FLOAT64,
  services_mrr_budget FLOAT64,
  services_mrr_prior_year FLOAT64,
  total_mrr_actual FLOAT64,          -- tech + services

  -- MRR YTD
  tech_mrr_ytd_actual FLOAT64,
  tech_mrr_ytd_budget FLOAT64,
  tech_mrr_ytd_prior_year FLOAT64,
  services_mrr_ytd_actual FLOAT64,
  services_mrr_ytd_budget FLOAT64,
  services_mrr_ytd_prior_year FLOAT64,

  -- ARR (= MRR * 12)
  tech_arr FLOAT64,                  -- £ (full)
  services_arr FLOAT64,
  total_arr FLOAT64,

  -- CARR (Contracted ARR = ARR + signed-not-live pipeline)
  carr FLOAT64,
  implementation_backlog FLOAT64,     -- CARR - ARR

  -- Revenue
  revenue_ecommerce_actual FLOAT64,   -- £k
  revenue_ecommerce_budget FLOAT64,
  revenue_ems_actual FLOAT64,
  revenue_ems_budget FLOAT64,
  revenue_services_actual FLOAT64,
  revenue_services_budget FLOAT64,
  revenue_total_actual FLOAT64,
  revenue_total_budget FLOAT64,
  revenue_total_prior_year FLOAT64,

  -- Revenue YTD
  revenue_total_ytd_actual FLOAT64,
  revenue_total_ytd_budget FLOAT64,
  revenue_total_ytd_prior_year FLOAT64,

  -- Revenue growth
  revenue_yoy_growth_pct FLOAT64,
  revenue_vs_budget_pct FLOAT64,
  revenue_ytd_growth_pct FLOAT64,

  -- Net New ARR (new + expansion - churn)
  new_arr FLOAT64,
  expansion_arr FLOAT64,
  churn_arr FLOAT64,
  net_new_arr FLOAT64,

  -- ACV (average contract value from KPI data sheet)
  acv FLOAT64,

  -- =============================================
  -- SECTION B: UNIT ECONOMICS / ARPC / MARGINS
  -- =============================================
  -- ARPC (Average Revenue Per Customer)
  arpc_actual FLOAT64,
  arpc_budget FLOAT64,
  arpc_ytd_actual FLOAT64,
  arpc_ytd_budget FLOAT64,

  -- Gross Margin
  direct_costs_total FLOAT64,
  gross_profit_ecommerce FLOAT64,
  gross_profit_ems FLOAT64,
  gross_profit_services FLOAT64,
  gross_profit_total FLOAT64,

  tech_gross_margin_pct FLOAT64,      -- month
  tech_gross_margin_ytd_pct FLOAT64,  -- YTD
  tech_gross_margin_prior_pct FLOAT64,
  tech_gross_margin_budget_pct FLOAT64,

  -- Direct Contribution
  contribution_ecommerce FLOAT64,
  contribution_ems FLOAT64,
  contribution_services FLOAT64,
  contribution_total FLOAT64,
  contribution_margin_pct FLOAT64,

  -- EBITDA
  total_overheads FLOAT64,
  ebitda_actual FLOAT64,
  ebitda_budget FLOAT64,
  ebitda_prior_year FLOAT64,
  ebitda_margin_pct FLOAT64,          -- month
  ebitda_margin_ytd_pct FLOAT64,      -- YTD
  ebitda_margin_budget_pct FLOAT64,
  ebitda_margin_prior_pct FLOAT64,

  capex FLOAT64,
  ebitda_less_capex FLOAT64,

  -- EBITDA YTD
  ebitda_ytd_actual FLOAT64,
  ebitda_ytd_budget FLOAT64,

  -- CAC / LTV (calculated from KPI data sheet)
  cac FLOAT64,                        -- Customer Acquisition Cost
  cac_payback_months FLOAT64,         -- CAC / monthly ARPC
  ltv FLOAT64,                        -- Lifetime Value
  ltv_cac_ratio FLOAT64,              -- LTV / CAC

  -- S&M Efficiency
  sm_efficiency FLOAT64,              -- TCV / Sales Cost
  sm_efficiency_ytd FLOAT64,

  -- Revenue per employee
  revenue_per_employee FLOAT64,
  payroll_pct_revenue FLOAT64,

  -- =============================================
  -- SECTION C: RETENTION / CHURN
  -- =============================================
  -- Revenue Churn
  revenue_churn_pct FLOAT64,          -- monthly churn rate
  revenue_churn_target FLOAT64,

  -- NRR / GRR (Net/Gross Revenue Retention)
  nrr_pct FLOAT64,                    -- (1 + expansion - churn) / opening
  grr_pct FLOAT64,                    -- (1 - churn) / opening

  -- Logo churn
  logo_churn_count INT64,
  logo_churn_pct FLOAT64,

  -- Customer concentration
  top5_customer_pct FLOAT64,          -- % revenue from top 5

  -- NPS
  nps_score FLOAT64,

  -- =============================================
  -- SECTION D: PRODUCT USAGE / MODULES
  -- =============================================
  -- Modules Sold (cumulative)
  modules_sold_total INT64,
  modules_sold_rooms INT64,
  modules_sold_tables INT64,
  modules_sold_spa INT64,
  modules_sold_retail INT64,
  modules_sold_events INT64,
  modules_sold_vouchers INT64,

  -- Modules Live (cumulative)
  modules_live_total INT64,
  modules_live_rooms INT64,
  modules_live_tables INT64,
  modules_live_spa INT64,
  modules_live_retail INT64,
  modules_live_events INT64,
  modules_live_vouchers INT64,

  -- Monthly flow
  modules_sold_month INT64,
  modules_live_month INT64,
  modules_churn INT64,

  -- Properties
  properties_sold INT64,
  properties_live INT64,
  properties_ecommerce INT64,
  properties_ems INT64,
  properties_services INT64,

  -- Revenue per live module
  revenue_per_live_module FLOAT64,

  -- Time to Value
  time_to_value_days FLOAT64,
  time_to_value_excl_blocked FLOAT64,

  -- =============================================
  -- SECTION E: CAPITAL EFFICIENCY / CASH / NWC
  -- =============================================
  -- Cash
  cash_balance FLOAT64,               -- £ (full, not £k)
  cash_balance_prior_month FLOAT64,
  cash_balance_budget FLOAT64,
  cash_burn_monthly FLOAT64,          -- monthly change
  cash_runway_months FLOAT64,

  -- NWC
  net_working_capital FLOAT64,        -- £ (full)
  nwc_prior_month FLOAT64,
  nwc_budget FLOAT64,

  -- Free Cash Conversion
  free_cash_conversion_month FLOAT64,
  free_cash_conversion_ytd FLOAT64,
  free_cash_conversion_budget FLOAT64,

  -- AR Aging (from Accounts Receivable sheet)
  ar_current FLOAT64,
  ar_30_days FLOAT64,
  ar_60_days FLOAT64,
  ar_90_plus_days FLOAT64,
  ar_total FLOAT64,

  -- =============================================
  -- COMPOSITE / VALUATION
  -- =============================================
  rule_of_40 FLOAT64,                 -- ARR growth % + EBITDA margin %
  indicative_ev FLOAT64,              -- Tech ARR * 7.7 + Services ARR * 1

  -- =============================================
  -- PEOPLE
  -- =============================================
  total_headcount FLOAT64,
  headcount_budget FLOAT64,
  headcount_ecommerce FLOAT64,
  headcount_ems FLOAT64,
  headcount_services FLOAT64,
  headcount_central FLOAT64,
  gross_payroll FLOAT64,
  gross_payroll_budget FLOAT64,

  -- =============================================
  -- COVENANTS
  -- =============================================
  gl_revenue_actual_cumulative FLOAT64,
  gl_revenue_target_cumulative FLOAT64,
  gl_revenue_headroom_pct FLOAT64,
  gl_revenue_breach BOOLEAN,
  gl_ebitda_actual_cumulative FLOAT64,
  gl_ebitda_target_cumulative FLOAT64,
  gl_ebitda_headroom_pct FLOAT64,
  gl_ebitda_breach BOOLEAN,

  -- =============================================
  -- REVENUE WATERFALL BRIDGE (Strategic Planning)
  -- =============================================
  wf_revenue_start      FLOAT64,
  wf_one_off_prev       FLOAT64,
  wf_one_off_ytd        FLOAT64,
  wf_recurring_growth   FLOAT64,
  wf_arr_ytg            FLOAT64,
  wf_weighted_pipeline  FLOAT64,
  wf_budget_assumptions FLOAT64,
  wf_revenue_gap        FLOAT64,
  wf_revenue_end        FLOAT64,

  -- =============================================
  -- METADATA
  -- =============================================
  currency STRING DEFAULT 'GBP',
  data_source STRING DEFAULT 'ma_parser',
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

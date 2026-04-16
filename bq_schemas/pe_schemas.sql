-- SECTION 2: BIGQUERY SCHEMA

-- BRONZE LAYER
CREATE TABLE IF NOT EXISTS bronze.raw_management_accounts (
  ingestion_id STRING NOT NULL,
  portco_id STRING NOT NULL,
  file_name STRING NOT NULL,
  sheet_name STRING NOT NULL,
  reporting_period DATE NOT NULL,
  row_label STRING,
  column_label STRING,
  value FLOAT64,
  value_type STRING, -- 'actual', 'budget', 'prior_year', 'variance', 'ytd'
  currency STRING DEFAULT 'GBP',
  unit STRING DEFAULT 'thousands',
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  source_cell STRING
);

-- SILVER LAYER
CREATE TABLE IF NOT EXISTS silver.monthly_pnl (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  segment STRING NOT NULL, -- 'ecommerce', 'ems', 'services', 'international', 'total'
  -- Revenue
  revenue_actual FLOAT64,
  revenue_budget FLOAT64,
  revenue_prior_year FLOAT64,
  -- Cost breakdown
  direct_costs_actual FLOAT64,
  direct_costs_budget FLOAT64,
  staff_costs_actual FLOAT64,
  staff_costs_budget FLOAT64,
  -- Profitability
  gross_profit_actual FLOAT64,
  gross_profit_budget FLOAT64,
  gross_margin_pct FLOAT64,
  direct_contribution_actual FLOAT64,
  direct_contribution_budget FLOAT64,
  contribution_margin_pct FLOAT64,
  -- Group level (segment='total' only)
  overheads_actual FLOAT64,
  overheads_budget FLOAT64,
  ebitda_actual FLOAT64,
  ebitda_budget FLOAT64,
  ebitda_prior_year FLOAT64,
  ebitda_margin_pct FLOAT64,
  capex_actual FLOAT64,
  ebitda_less_capex FLOAT64,
  ebitda_less_capex_margin_pct FLOAT64,
  pat_actual FLOAT64,
  currency STRING DEFAULT 'GBP'
);

CREATE TABLE IF NOT EXISTS silver.revenue_detail (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  revenue_line STRING NOT NULL, -- 'tech1', 'onejourney', 'gifted', 'management_systems', 'other_tech', 'agency', 'fulfilment'
  segment STRING NOT NULL, -- 'ecommerce', 'ems', 'services'
  amount FLOAT64,
  currency STRING DEFAULT 'GBP'
);

CREATE TABLE IF NOT EXISTS silver.monthly_overheads (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  category STRING NOT NULL, -- 'net_central_staff', 'office', 'premises', 'legal', 'marketing', 'finance', 'recruitment', 'it_software', 'travel', 'people', 'other'
  actual FLOAT64,
  budget FLOAT64,
  prior_year FLOAT64,
  currency STRING DEFAULT 'GBP'
);

CREATE TABLE IF NOT EXISTS silver.balance_sheet (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  line_item STRING NOT NULL,
  category STRING NOT NULL,
  actual FLOAT64,
  budget FLOAT64,
  prior_month FLOAT64,
  currency STRING DEFAULT 'GBP'
);

CREATE TABLE IF NOT EXISTS silver.cash_flow (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  category STRING NOT NULL,
  line_item STRING NOT NULL,
  actual FLOAT64,
  budget FLOAT64,
  currency STRING DEFAULT 'GBP'
);

CREATE TABLE IF NOT EXISTS silver.headcount (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  segment STRING NOT NULL, -- 'ecommerce', 'ems', 'services', 'central'
  team STRING NOT NULL,
  actual_fte FLOAT64,
  budget_fte FLOAT64,
  prior_year_fte FLOAT64
);

CREATE TABLE IF NOT EXISTS silver.arr_analysis (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  -- MRR by product
  mrr_onejourney_live FLOAT64,
  mrr_gifted FLOAT64,
  mrr_premier FLOAT64,
  mrr_onejourney_sold FLOAT64,
  mrr_total_live FLOAT64,
  mrr_total_sold FLOAT64,
  -- MRR by segment
  mrr_ecommerce_live FLOAT64,
  mrr_ems_live FLOAT64,
  mrr_ecommerce_sold FLOAT64,
  mrr_ems_sold FLOAT64,
  -- ARR (= MRR * 12)
  arr_onejourney_live FLOAT64,
  arr_gifted FLOAT64,
  arr_premier FLOAT64,
  arr_onejourney_sold FLOAT64,
  arr_total_live FLOAT64,
  arr_total_sold FLOAT64,
  arr_ecommerce_live FLOAT64,
  arr_ems_live FLOAT64,
  arr_ecommerce_sold FLOAT64,
  arr_ems_sold FLOAT64,
  -- Derived
  revenue_per_live_module FLOAT64,
  currency STRING DEFAULT 'GBP'
);

CREATE TABLE IF NOT EXISTS silver.modules (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  -- Modules Sold (cumulative)
  sold_rooms INT64,
  sold_tables INT64,
  sold_spa INT64,
  sold_reservations INT64,
  sold_retail INT64,
  sold_events INT64,
  sold_golf INT64,
  sold_vouchers INT64,
  sold_total_pre_vouchers INT64,
  sold_total_post_vouchers INT64,
  -- Modules Live (cumulative)
  live_rooms INT64,
  live_tables INT64,
  live_spa INT64,
  live_reservations INT64,
  live_retail INT64,
  live_events INT64,
  live_golf INT64,
  live_vouchers INT64,
  live_total_pre_vouchers INT64,
  live_total_post_vouchers INT64,
  -- Monthly changes
  sold_per_month_pre_vouchers INT64,
  sold_per_month_post_vouchers INT64,
  live_per_month_pre_vouchers INT64,
  live_per_month_post_vouchers INT64,
  -- Churn
  churn_modules INT64,
  -- Properties
  properties_sold INT64,
  properties_ecommerce INT64,
  properties_ems INT64,
  properties_services INT64,
  properties_live INT64
);

CREATE TABLE IF NOT EXISTS silver.customers (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  segment STRING,
  customer_count INT64,
  new_customers INT64,
  churned_customers INT64,
  arpu FLOAT64,
  currency STRING DEFAULT 'GBP'
);

CREATE TABLE IF NOT EXISTS silver.revenue_waterfall (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  component STRING NOT NULL,
  amount FLOAT64,
  currency STRING DEFAULT 'GBP'
);

CREATE TABLE IF NOT EXISTS silver.ar_ap (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  type STRING NOT NULL,
  counterparty STRING,
  age_bucket STRING,
  amount FLOAT64,
  currency STRING DEFAULT 'GBP'
);

CREATE TABLE IF NOT EXISTS silver.covenants (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  covenant_source STRING NOT NULL, -- 'growth_lending', 'averroes_guard_rails'
  metric STRING NOT NULL, -- 'revenue', 'ebitda'
  actual_monthly FLOAT64,
  target_monthly FLOAT64,
  actual_cumulative FLOAT64,
  target_cumulative FLOAT64,
  headroom FLOAT64,
  headroom_pct FLOAT64,
  breach BOOLEAN,
  currency STRING DEFAULT 'GBP'
);

-- GOLD LAYER
CREATE TABLE IF NOT EXISTS gold.kpi_monthly (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,

  -- ========== REVENUE ==========
  total_revenue FLOAT64,
  total_revenue_budget FLOAT64,
  total_group_revenue FLOAT64,
  revenue_vs_budget_pct FLOAT64,
  revenue_vs_budget_variance FLOAT64,
  ecommerce_revenue FLOAT64,
  ems_revenue FLOAT64,
  services_revenue FLOAT64,
  international_revenue FLOAT64,
  tech_revenue FLOAT64, -- ecommerce + ems
  tech1_revenue FLOAT64,
  onejourney_revenue FLOAT64,
  gifted_revenue FLOAT64,
  af_revenue FLOAT64, -- agency + fulfilment

  -- Revenue time views
  ltm_revenue_total FLOAT64,
  ltm_revenue_tech FLOAT64,
  ltm_revenue_ecommerce FLOAT64,
  ltm_revenue_ems FLOAT64,
  ltm_revenue_services FLOAT64,
  run_rate_revenue_total FLOAT64,
  run_rate_revenue_tech FLOAT64,
  run_rate_revenue_services FLOAT64,
  ytd_revenue FLOAT64,
  ytd_revenue_budget FLOAT64,
  ytd_revenue_growth FLOAT64,
  ytd_revenue_budget FLOAT64,
  ytg_revenue_vs_budget FLOAT64,
  ltm_vs_budget_variation FLOAT64,

  -- Revenue growth rates
  yoy_tech_monthly_growth FLOAT64,
  yoy_services_monthly_growth FLOAT64,
  yoy_total_ltm_growth FLOAT64,
  yoy_tech_ltm_growth FLOAT64,
  yoy_ecommerce_ltm_growth FLOAT64,
  yoy_ems_ltm_growth FLOAT64,
  yoy_services_ltm_growth FLOAT64,

  -- ========== ARR ==========
  tech_mrr_live FLOAT64,
  tech_mrr_sold FLOAT64,
  tech_arr_live FLOAT64,
  tech_arr_sold FLOAT64,
  ecommerce_arr_live FLOAT64,
  ems_arr_live FLOAT64,
  ecommerce_arr_sold FLOAT64,
  ems_arr_sold FLOAT64,
  revenue_per_live_module FLOAT64,
  yoy_tech_arr_growth FLOAT64,
  yoy_ecommerce_arr_growth FLOAT64,

  -- ========== PROFITABILITY ==========
  -- Gross profit
  direct_costs_total FLOAT64,
  gross_profit_total FLOAT64,
  gross_profit_tech FLOAT64,
  gross_profit_af FLOAT64,
  gross_profit_ecommerce FLOAT64,
  gross_profit_ems FLOAT64,
  gross_profit_services FLOAT64,
  gross_margin_total_pct FLOAT64,
  gross_margin_tech_pct FLOAT64,
  gross_margin_af_pct FLOAT64,
  tech_gross_margin_month FLOAT64,
  gross_margin_ecommerce_pct FLOAT64,
  gross_margin_ems_pct FLOAT64,
  gross_margin_services_pct FLOAT64,

  -- Contribution
  contribution_total FLOAT64,
  contribution_tech FLOAT64,
  contribution_af FLOAT64,
  contribution_ecommerce FLOAT64,
  contribution_ems FLOAT64,
  contribution_services FLOAT64,
  contribution_margin_total_pct FLOAT64,
  contribution_margin_tech_pct FLOAT64,
  contribution_margin_af_pct FLOAT64,
  contribution_margin_ecommerce_pct FLOAT64,
  contribution_margin_ems_pct FLOAT64,
  contribution_margin_services_pct FLOAT64,

  -- EBITDA
  total_overheads FLOAT64,
  overhead_ratio FLOAT64,
  adjusted_ebitda FLOAT64,
  adjusted_ebitda_budget FLOAT64,
  adjusted_ebitda_margin FLOAT64,
  ebitda_less_capex FLOAT64,
  ebitda_less_capex_margin FLOAT64,
  ltm_ebitda FLOAT64,
  run_rate_ebitda FLOAT64,
  ebitda_margin_month FLOAT64,
  ytd_ebitda FLOAT64,
  ytd_ebitda_budget FLOAT64,
  ytg_ebitda FLOAT64,
  yoy_ltm_ebitda_growth FLOAT64,
  pat FLOAT64,
  pat_margin FLOAT64,

  -- ========== CASH ==========
  cash_balance FLOAT64,
  cash_burn FLOAT64, -- monthly change
  cash_runway_months FLOAT64,
  net_working_capital FLOAT64,
  free_cash_conversion FLOAT64,

  -- ========== EFFICIENCY ==========
  sm_efficiency FLOAT64,
  revenue_per_employee FLOAT64,
  payroll_pct_revenue FLOAT64,
  sm_efficiency FLOAT64,
  rule_of_40_score FLOAT64,
  time_to_value_days FLOAT64,

  -- ========== PEOPLE ==========
  total_headcount FLOAT64,
  headcount_budget FLOAT64,
  headcount_variance FLOAT64,
  ecommerce_headcount FLOAT64,
  ems_headcount FLOAT64,
  services_headcount FLOAT64,
  central_headcount FLOAT64,

  -- ========== MODULES (Non-Financial) ==========
  modules_sold_pre_vouchers INT64,
  modules_sold_post_vouchers INT64,
  modules_live_pre_vouchers INT64,
  modules_live_post_vouchers INT64,
  modules_churn INT64,
  sold_per_month_pre_vouchers INT64,
  sold_per_month_post_vouchers INT64,
  live_per_month_pre_vouchers INT64,
  live_per_month_post_vouchers INT64,
  -- Module breakdown (sold cumulative)
  sold_rooms INT64,
  sold_tables INT64,
  sold_spa INT64,
  sold_retail INT64,
  sold_events INT64,
  sold_vouchers INT64,
  -- Module breakdown (live cumulative)
  live_rooms INT64,
  live_tables INT64,
  live_spa INT64,
  live_retail INT64,
  live_events INT64,
  live_vouchers INT64,

  -- Properties
  properties_sold INT64,
  properties_live INT64,
  properties_ecommerce INT64,
  properties_ems INT64,
  properties_services INT64,

  -- Module growth rates
  yoy_sold_modules_growth FLOAT64, -- pre-vouchers
  yoy_live_modules_growth FLOAT64, -- pre-vouchers
  yoy_properties_growth FLOAT64,

  -- ========== CUSTOMERS ==========
  total_customers INT64,
  arpu FLOAT64,
  arpc FLOAT64,
  revenue_churn_pct FLOAT64,
  indicative_ev FLOAT64,

  -- ========== COVENANTS ==========
  gl_revenue_actual_cumulative FLOAT64,
  gl_revenue_covenant_cumulative FLOAT64,
  gl_revenue_headroom_pct FLOAT64,
  gl_revenue_breach BOOLEAN,
  gl_ebitda_actual_cumulative FLOAT64,
  gl_ebitda_covenant_cumulative FLOAT64,
  gl_ebitda_headroom_pct FLOAT64,
  gl_ebitda_breach BOOLEAN,
  averroes_revenue_rag STRING, -- 'green', 'amber', 'red'
  averroes_ebitda_rag STRING,

  -- ========== METADATA ==========
  currency STRING DEFAULT 'GBP',
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS gold.kpi_quarterly (
  portco_id STRING NOT NULL,
  quarter STRING NOT NULL, -- 'Q1 FY26', 'Q2 FY26', etc.
  quarter_end_date DATE NOT NULL,
  tech_revenue FLOAT64,
  af_revenue FLOAT64,
  total_revenue FLOAT64,
  gross_profit FLOAT64,
  gross_margin_pct FLOAT64,
  contribution FLOAT64,
  contribution_margin_pct FLOAT64,
  adjusted_ebitda FLOAT64,
  ebitda_margin_pct FLOAT64,
  cash_balance FLOAT64,
  modules_sold INT64,
  modules_live INT64,
  properties INT64,
  customers INT64,
  currency STRING DEFAULT 'GBP',
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ALERTS
CREATE TABLE IF NOT EXISTS alerts.anomaly_flags (
  portco_id STRING NOT NULL,
  period DATE NOT NULL,
  alert_type STRING NOT NULL,
  severity STRING NOT NULL, -- 'critical', 'warning', 'info'
  metric STRING NOT NULL,
  description STRING,
  current_value FLOAT64,
  threshold_value FLOAT64,
  deviation_pct FLOAT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

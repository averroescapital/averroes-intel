-- ============================================================
-- ALTER TABLE: Add wf_* (Revenue Waterfall Bridge) columns
-- to gold.kpi_monthly in BigQuery
--
-- Run once in BigQuery console or via bq CLI:
--   bq query --use_legacy_sql=false < alter_add_wf_columns.sql
-- ============================================================

ALTER TABLE `averroes-portfolio-intel.gold.kpi_monthly`
ADD COLUMN IF NOT EXISTS wf_revenue_start      FLOAT64,
ADD COLUMN IF NOT EXISTS wf_one_off_prev        FLOAT64,
ADD COLUMN IF NOT EXISTS wf_one_off_ytd         FLOAT64,
ADD COLUMN IF NOT EXISTS wf_recurring_growth    FLOAT64,
ADD COLUMN IF NOT EXISTS wf_arr_ytg             FLOAT64,
ADD COLUMN IF NOT EXISTS wf_weighted_pipeline   FLOAT64,
ADD COLUMN IF NOT EXISTS wf_budget_assumptions  FLOAT64,
ADD COLUMN IF NOT EXISTS wf_revenue_gap         FLOAT64,
ADD COLUMN IF NOT EXISTS wf_revenue_end         FLOAT64;

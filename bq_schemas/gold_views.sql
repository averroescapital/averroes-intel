-- This file contains queries to generate the gold layer tables based on the silver layer.
-- In a real environment, these would be scheduled queries or DBT models.

-- 1. gold.monthly_kpis
CREATE OR REPLACE VIEW `averroes-portfolio-intel.gold.monthly_kpis` AS
WITH current_and_prev AS (
  SELECT
    portco_id,
    period,
    kpi_name,
    kpi_category,
    value,
    unit,
    benchmark_value,
    normalised_at,
    LAG(value) OVER (PARTITION BY portco_id, kpi_name ORDER BY period) as prev_month_value,
    LAG(value, 12) OVER (PARTITION BY portco_id, kpi_name ORDER BY period) as prev_year_value,
    AVG(value) OVER (PARTITION BY portco_id, kpi_name ORDER BY period ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as trend_3m_avg,
    AVG(value) OVER (PARTITION BY portco_id, kpi_name ORDER BY period ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING) as prev_3m_avg
  FROM `averroes-portfolio-intel.silver.normalised_kpis`
  WHERE is_latest = TRUE
)
SELECT
  portco_id,
  period,
  kpi_name,
  kpi_category,
  value,
  unit,
  benchmark_value,
  (value - prev_month_value) AS mom_delta,
  SAFE_DIVIDE((value - prev_month_value), prev_month_value) * 100 AS mom_delta_pct,
  CASE
    WHEN trend_3m_avg > prev_3m_avg * 1.02 THEN 'up'
    WHEN trend_3m_avg < prev_3m_avg * 0.98 THEN 'down'
    ELSE 'flat'
  END AS trend_3m,
  trend_3m_avg,
  (value - prev_year_value) AS yoy_delta,
  SAFE_DIVIDE((value - prev_year_value), prev_year_value) * 100 AS yoy_delta_pct,
  -- Simple RAG status calculation
  CASE
    WHEN benchmark_value IS NULL THEN 'info'
    WHEN value >= benchmark_value THEN 'green'
    WHEN value >= benchmark_value * 0.9 THEN 'amber'
    ELSE 'red'
  END AS rag_status,
  SAFE_DIVIDE((value - benchmark_value), benchmark_value) * 100 AS vs_benchmark_pct,
  CURRENT_TIMESTAMP() AS computed_at
FROM current_and_prev;

-- 2. gold.arr_bridge
CREATE OR REPLACE VIEW `averroes-portfolio-intel.gold.arr_bridge` AS
WITH arr_components AS (
  SELECT
    portco_id,
    period,
    MAX(CASE WHEN kpi_name = 'arr' THEN value END) as closing_arr,
    MAX(CASE WHEN kpi_name = 'new_arr' THEN value END) as new_arr,
    MAX(CASE WHEN kpi_name = 'expansion_arr' THEN value END) as expansion_arr,
    MAX(CASE WHEN kpi_name = 'churn_arr' THEN value END) as churn_arr
  FROM `averroes-portfolio-intel.silver.normalised_kpis`
  WHERE kpi_category = 'Revenue' AND is_latest = TRUE
  GROUP BY portco_id, period
)
SELECT
  portco_id,
  period,
  LAG(closing_arr) OVER (PARTITION BY portco_id ORDER BY period) AS opening_arr,
  new_arr,
  expansion_arr,
  churn_arr,
  IFNULL(new_arr, 0) + IFNULL(expansion_arr, 0) - IFNULL(churn_arr, 0) AS net_new_arr,
  closing_arr,
  SAFE_DIVIDE((LAG(closing_arr) OVER (PARTITION BY portco_id ORDER BY period) + IFNULL(expansion_arr, 0) - IFNULL(churn_arr, 0)), 
               LAG(closing_arr) OVER (PARTITION BY portco_id ORDER BY period)) * 100 AS implied_nrr,
  SAFE_DIVIDE(expansion_arr, churn_arr) AS expansion_churn_ratio,
  CURRENT_TIMESTAMP() AS computed_at
FROM arr_components;

-- 3. gold.portfolio_rollup
CREATE OR REPLACE VIEW `averroes-portfolio-intel.gold.portfolio_rollup` AS
SELECT
  period,
  kpi_name,
  kpi_category,
  MAX(CASE WHEN portco_id = 'portco-alpha' THEN value END) AS portco_alpha_value,
  MAX(CASE WHEN portco_id = 'portco-alpha' THEN rag_status END) AS portco_alpha_rag,
  MAX(CASE WHEN portco_id = 'portco-beta' THEN value END) AS portco_beta_value,
  MAX(CASE WHEN portco_id = 'portco-beta' THEN rag_status END) AS portco_beta_rag,
  MAX(CASE WHEN portco_id = 'portco-gamma' THEN value END) AS portco_gamma_value,
  MAX(CASE WHEN portco_id = 'portco-gamma' THEN rag_status END) AS portco_gamma_rag,
  AVG(value) AS portfolio_avg,
  MAX(benchmark_value) AS benchmark_value,
  CURRENT_TIMESTAMP() AS computed_at
FROM `averroes-portfolio-intel.gold.monthly_kpis`
GROUP BY period, kpi_name, kpi_category;

-- bronze.raw_submissions
CREATE TABLE IF NOT EXISTS bronze.raw_submissions (
  submission_id STRING NOT NULL,
  portco_id STRING NOT NULL,
  period STRING NOT NULL,           -- YYYY-MM
  source_file STRING NOT NULL,      -- original filename
  source_type STRING NOT NULL,      -- 'excel', 'csv', 'pdf'
  raw_data JSON NOT NULL,           -- full extracted content as JSON
  parser_version STRING,
  parser_confidence FLOAT64,
  gaps_flagged ARRAY<STRING>,       -- KPIs that could not be extracted
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  ingested_by STRING DEFAULT 'system'
);

-- silver.normalised_kpis
CREATE TABLE IF NOT EXISTS silver.normalised_kpis (
  portco_id STRING NOT NULL,
  period STRING NOT NULL,           -- YYYY-MM
  kpi_name STRING NOT NULL,         -- standardised name from taxonomy
  kpi_category STRING NOT NULL,     -- Revenue, Retention, Unit Economics, etc.
  value FLOAT64,
  unit STRING NOT NULL,             -- 'gbp_k', 'pct', 'months', 'score', 'x', 'count', 'gbp', 'days'
  benchmark_value FLOAT64,
  source_submission_id STRING,
  normalised_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  is_latest BOOL DEFAULT TRUE       -- for handling restatements
);

-- gold.monthly_kpis
CREATE TABLE IF NOT EXISTS gold.monthly_kpis (
  portco_id STRING NOT NULL,
  period STRING NOT NULL,
  kpi_name STRING NOT NULL,
  kpi_category STRING NOT NULL,
  value FLOAT64,
  unit STRING,
  benchmark_value FLOAT64,
  mom_delta FLOAT64,                -- month-over-month change
  mom_delta_pct FLOAT64,            -- MoM as percentage
  trend_3m STRING,                  -- 'up', 'down', 'flat'
  trend_3m_avg FLOAT64,             -- 3-month rolling average
  yoy_delta FLOAT64,                -- year-over-year change
  yoy_delta_pct FLOAT64,
  rag_status STRING,                -- 'green', 'amber', 'red'
  vs_benchmark_pct FLOAT64,         -- % deviation from benchmark
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

-- gold.arr_bridge
CREATE TABLE IF NOT EXISTS gold.arr_bridge (
  portco_id STRING NOT NULL,
  period STRING NOT NULL,
  opening_arr FLOAT64,
  new_arr FLOAT64,
  expansion_arr FLOAT64,
  churn_arr FLOAT64,
  net_new_arr FLOAT64,
  closing_arr FLOAT64,
  implied_nrr FLOAT64,
  expansion_churn_ratio FLOAT64,
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

-- gold.portfolio_rollup
CREATE TABLE IF NOT EXISTS gold.portfolio_rollup (
  period STRING NOT NULL,
  kpi_name STRING NOT NULL,
  kpi_category STRING NOT NULL,
  -- One column per portco (dynamic in queries, but for the base table:)
  portco_alpha_value FLOAT64,
  portco_alpha_rag STRING,
  portco_beta_value FLOAT64,
  portco_beta_rag STRING,
  portco_gamma_value FLOAT64,
  portco_gamma_rag STRING,
  portfolio_avg FLOAT64,
  benchmark_value FLOAT64,
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

-- gold.data_health
CREATE TABLE IF NOT EXISTS gold.data_health (
  portco_id STRING NOT NULL,
  period STRING NOT NULL,
  expected_cadence STRING,          -- 'monthly' or 'quarterly'
  submission_status STRING,         -- 'received', 'overdue', 'partial'
  kpis_received INT64,
  kpis_expected INT64,
  gaps ARRAY<STRING>,               -- list of missing KPI names
  last_submission_date TIMESTAMP,
  days_overdue INT64,
  checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

-- alerts.anomaly_flags
CREATE TABLE IF NOT EXISTS alerts.anomaly_flags (
  alert_id STRING NOT NULL,
  portco_id STRING NOT NULL,
  period STRING NOT NULL,
  kpi_name STRING NOT NULL,
  alert_type STRING NOT NULL,       -- 'statistical', 'rule_based', 'correlated'
  severity STRING NOT NULL,         -- 'red', 'amber', 'info'
  rule_name STRING,                 -- e.g., 'nrr_drop_2pp', 'pipeline_below_3x'
  current_value FLOAT64,
  threshold_value FLOAT64,
  z_score FLOAT64,                  -- for statistical alerts
  description STRING NOT NULL,      -- human-readable alert text
  consecutive_months INT64,         -- how many months this condition has persisted
  is_acknowledged BOOL DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

-- alerts.ai_commentary
CREATE TABLE IF NOT EXISTS alerts.ai_commentary (
  commentary_id STRING NOT NULL,
  portco_id STRING NOT NULL,        -- 'all' for cross-portco commentary
  period STRING NOT NULL,
  commentary_type STRING NOT NULL,  -- 'executive_summary', 'anomaly_detail', 'cross_portco'
  commentary_text STRING NOT NULL,
  model_used STRING,
  generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
);

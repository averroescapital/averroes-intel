import os
import json
import uuid
import re
import pandas as pd
import yaml
from datetime import datetime
import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import openpyxl
from ma_parser import ingest_ma_to_bronze

# Initialize clients
project_id = os.environ.get('GCP_PROJECT', 'averroes-portfolio-intel')
bq_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)

@functions_framework.cloud_event
def process_file(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"Processing PE Ingestion: gs://{bucket_name}/{file_name}")

    # 1. Extract portco_id and path
    parts = file_name.split('/')
    if len(parts) >= 2:
        portco_id = parts[0]
    else:
        print("Skipping file not in a portco directory.")
        return

    # 2. Download content
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_bytes()

    # 3. Bronze Ingestion Strategy
    if "MAfile" in file_name:
        bronze_rows = ingest_ma_to_bronze(file_content, file_name, portco_id)
        if bronze_rows:
            table_id = f"{project_id}.bronze.raw_management_accounts"
            # Attempt cleanup if no streaming lock, else use Load Job with specific disposition
            try:
                bq_client.query(f"DELETE FROM `{table_id}` WHERE file_name = {repr(file_name)}").result()
            except:
                print("Skipping bronze cleanup due to streaming lock - appending instead.")
            
            # Use Load Job (Job based avoids buffer issues)
            # Fetch existing schema to avoid mode mismatch (REQUIRED vs NULLABLE)
            table_ref = bq_client.get_table(table_id)
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND", 
                schema=table_ref.schema,
                autodetect=False
            )
            bq_client.load_table_from_json(bronze_rows, table_id, job_config=job_config).result()
            print(f"Loaded {len(bronze_rows)} rows to bronze.")
                
            # TRIGGER SILVER TRANSFORMATION
            transform_all_silver(portco_id, file_name)
    
    elif "KPITracker" in file_name:
        print("Master KPI Tracker detected. Ingesting 3-year time-series master data...")
        ingest_master_tracker(file_content, file_name, portco_id)

def transform_all_silver(portco_id, file_name):
    print(f"Running Silver core transformation for {portco_id} ({file_name})...")

    # 1. Monthly P&L - pivot bronze row_labels into segment rows
    pnl_query = f"""
        WITH b AS (
            SELECT
                reporting_period AS period,
                row_label,
                column_label AS type,
                value
            FROM `{project_id}.bronze.raw_management_accounts`
            WHERE file_name = {repr(file_name)}
              AND sheet_name = 'P&L Detail'
        ),
        pivot AS (
            SELECT
                period,
                MAX(CASE WHEN row_label='TOTAL_REVENUE'            AND type='actual' THEN value END) AS tot_rev_a,
                MAX(CASE WHEN row_label='TOTAL_REVENUE'            AND type='budget' THEN value END) AS tot_rev_b,
                MAX(CASE WHEN row_label='ECOMMERCE_REVENUE'        AND type='actual' THEN value END) AS eco_a,
                MAX(CASE WHEN row_label='ECOMMERCE_REVENUE'        AND type='budget' THEN value END) AS eco_b,
                MAX(CASE WHEN row_label='EMS_REVENUE'              AND type='actual' THEN value END) AS ems_a,
                MAX(CASE WHEN row_label='EMS_REVENUE'              AND type='budget' THEN value END) AS ems_b,
                MAX(CASE WHEN row_label='SERVICES_REVENUE'         AND type='actual' THEN value END) AS ser_a,
                MAX(CASE WHEN row_label='SERVICES_REVENUE'         AND type='budget' THEN value END) AS ser_b,
                MAX(CASE WHEN row_label='TOTAL_DIRECT_CONTRIBUTION' AND type='actual' THEN value END) AS cont_a,
                MAX(CASE WHEN row_label='EBITDA'                   AND type='actual' THEN value END) AS eb_a,
                MAX(CASE WHEN row_label='EBITDA'                   AND type='budget' THEN value END) AS eb_b,
                MAX(CASE WHEN row_label='TOTAL_OVERHEADS'          AND type='actual' THEN value END) AS oh_a
            FROM b
            GROUP BY 1
        )
        SELECT {repr(portco_id)}, period, 'ecommerce', eco_a,   eco_b,   NULL, NULL,   NULL, NULL, NULL FROM pivot WHERE period IS NOT NULL
        UNION ALL
        SELECT {repr(portco_id)}, period, 'ems',       ems_a,   ems_b,   NULL, NULL,   NULL, NULL, NULL FROM pivot WHERE period IS NOT NULL
        UNION ALL
        SELECT {repr(portco_id)}, period, 'services',  ser_a,   ser_b,   NULL, NULL,   NULL, NULL, NULL FROM pivot WHERE period IS NOT NULL
        UNION ALL
        SELECT {repr(portco_id)}, period, 'total',     tot_rev_a, tot_rev_b, NULL, cont_a, eb_a, eb_b, oh_a FROM pivot WHERE period IS NOT NULL
    """
    bq_client.query(f"DELETE FROM `{project_id}.silver.monthly_pnl` WHERE portco_id = {repr(portco_id)}").result()
    bq_client.query(f"""
        INSERT INTO `{project_id}.silver.monthly_pnl`
          (portco_id, period, segment, revenue_actual, revenue_budget,
           gross_profit_actual, direct_contribution_actual,
           ebitda_actual, ebitda_budget, overheads_actual)
        {pnl_query}
    """).result()
    print("Populated silver.monthly_pnl.")

    # 2. Headcount - team rows from Headcount sheet
    hc_query = f"""
        INSERT INTO `{project_id}.silver.headcount`
          (portco_id, period, segment, team, actual_fte, budget_fte)
        SELECT
            {repr(portco_id)},
            reporting_period,
            CASE
                WHEN row_label LIKE 'Ecommerce%' THEN 'ecommerce'
                WHEN row_label LIKE 'EMS%'       THEN 'ems'
                WHEN row_label LIKE 'Services%'  THEN 'services'
                ELSE 'central'
            END AS segment,
            row_label AS team,
            MAX(CASE WHEN column_label='actual' THEN value END) AS actual_fte,
            MAX(CASE WHEN column_label='budget' THEN value END) AS budget_fte
        FROM `{project_id}.bronze.raw_management_accounts`
        WHERE file_name = {repr(file_name)}
          AND sheet_name = 'Headcount'
        GROUP BY 2, 3, 4
    """
    bq_client.query(f"DELETE FROM `{project_id}.silver.headcount` WHERE portco_id = {repr(portco_id)}").result()
    bq_client.query(hc_query).result()
    print("Populated silver.headcount.")

    # 3. Gold Rollup
    update_gold_kpis(portco_id)

def ingest_master_tracker(file_content, file_name, portco_id):
    import io
    import pandas as pd
    from datetime import datetime
    import numpy as np
    
    print(f"Ingesting Master KPI Tracker: {file_name}")
    df = pd.read_excel(io.BytesIO(file_content), sheet_name='Monthly Actuals', header=None)
    
    # 1. Map Time Series Columns (Strict column ranges based on SPEC)
    col_to_period = {}
    
    # FY23 (G-R = 6 to 17)
    for i, c in enumerate(range(6, 18)):
        col_to_period[c] = f"2022-{11+i:02d}-01" if 11+i <= 12 else f"2023-{i-1:02d}-01"
        
    # FY24 (V-AG = 21 to 32)
    for i, c in enumerate(range(21, 33)):
        col_to_period[c] = f"2023-{11+i:02d}-01" if 11+i <= 12 else f"2024-{i-1:02d}-01"
        
    # FY25 (AK-AV = 36 to 47)
    for i, c in enumerate(range(36, 48)):
        col_to_period[c] = f"2024-{11+i:02d}-01" if 11+i <= 12 else f"2025-{i-1:02d}-01"
        
    # FY26 (AZ-BK = 51 to 62)
    for i, c in enumerate(range(51, 63)):
        col_to_period[c] = f"2025-{11+i:02d}-01" if 11+i <= 12 else f"2026-{i-1:02d}-01"

    print(f"Mapped {len(col_to_period)} columns (G-R, V-AG, AK-AV, AZ-BK).")

    # 2. Comprehensive KPI Mapping (Authoritative Specs)
    kpi_rows = {
        9: "tech1_revenue",
        10: "onejourney_revenue",
        11: "gifted_revenue",
        14: "total_revenue", 
        21: "total_group_revenue",
        23: "ecommerce_revenue",
        24: "ems_revenue",
        25: "services_revenue",
        50: "direct_costs_total",
        64: "gross_profit_total",
        70: "gross_margin_total_pct",
        86: "contribution_total",
        92: "contribution_margin_total_pct",
        104: "total_overheads",
        107: "adjusted_ebitda",
        108: "adjusted_ebitda_margin",
        122: "cash_balance",
        138: "tech_mrr_live",
        144: "tech_arr_live",
        156: "ecommerce_arr_live",
        157: "ems_arr_live",
        222: "modules_sold_pre_vouchers",
        235: "modules_live_pre_vouchers",
        247: "modules_churn",
        254: "properties_live"
    }
    
    gold_rows = []
    for row_idx, kpi_name in kpi_rows.items():
        # Sheet maps row 9 to df.iloc[8]
        actual_row = row_idx - 1
        for col_idx, period in col_to_period.items():
            val = df.iloc[actual_row, col_idx]
            if isinstance(val, (int, float)):
                gold_rows.append({
                    "portco_id": portco_id,
                    "period": period,
                    "kpi_name": kpi_name,
                    "value": float(val)
                })

    # For this demo, we'll insert into a temp gold table or update gold.kpi_monthly
    print(f"Extracted {len(gold_rows)} historical data points from Tracker.")
    
    if gold_rows:
        # Since gold.kpi_monthly has a wide schema, we'll need to pivot or use a flattened approach
        # For now, we'll populate a simplified version of the gold table
        table_id = f"{project_id}.gold.kpi_monthly"
        
        import numpy as np
        # Pivot the extracted data into Wide format for BigQuery
        pivoted_data = {}
        for row in gold_rows:
            p = row['period']
            if p not in pivoted_data: pivoted_data[p] = {"portco_id": portco_id, "period": p}
            val = row['value']
            # Clean NaN/Inf values for BigQuery
            if pd.isna(val) or np.isinf(val):
                val = None
            pivoted_data[p][row['kpi_name']] = val
            
        rows_to_insert = list(pivoted_data.values())
        print(f"Prepared {len(rows_to_insert)} rows for gold.kpi_monthly insertion.")
        
        # Use Load Job with autodetection, WRITE_TRUNCATE is fine if we self-heal missing cols
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True, 
        )
        
        load_job = bq_client.load_table_from_json(rows_to_insert, table_id, job_config=job_config)
        load_job.result() # Wait for completion
        print(f"Successfully REFRESHED {len(rows_to_insert)} historical periods in gold.kpi_monthly.")
    
def update_gold_kpis(portco_id):
    print(f"Merging Gold KPIs for {portco_id}...")

    merge_query = f"""
        MERGE `{project_id}.gold.kpi_monthly` T
        USING (
            WITH b AS (
                SELECT
                    reporting_period AS period,
                    row_label,
                    column_label AS type,
                    MAX(value) AS val
                FROM `{project_id}.bronze.raw_management_accounts`
                WHERE portco_id = {repr(portco_id)}
                GROUP BY 1, 2, 3
            ),
            pnl AS (
                SELECT
                    period,
                    -- Revenue
                    MAX(CASE WHEN row_label='TOTAL_REVENUE'             AND type='actual'     THEN val END) AS rev_a,
                    MAX(CASE WHEN row_label='TOTAL_REVENUE'             AND type='budget'     THEN val END) AS rev_b,
                    MAX(CASE WHEN row_label='TOTAL_REVENUE'             AND type='prior_year' THEN val END) AS rev_py,
                    MAX(CASE WHEN row_label='TOTAL_REVENUE_YTD'         AND type='actual'     THEN val END) AS rev_ytd_a,
                    MAX(CASE WHEN row_label='ECOMMERCE_REVENUE'         AND type='actual'     THEN val END) AS eco_a,
                    MAX(CASE WHEN row_label='ECOMMERCE_REVENUE'         AND type='budget'     THEN val END) AS eco_b,
                    MAX(CASE WHEN row_label='EMS_REVENUE'               AND type='actual'     THEN val END) AS ems_a,
                    MAX(CASE WHEN row_label='EMS_REVENUE'               AND type='budget'     THEN val END) AS ems_b,
                    MAX(CASE WHEN row_label='SERVICES_REVENUE'          AND type='actual'     THEN val END) AS ser_a,
                    MAX(CASE WHEN row_label='SERVICES_REVENUE'          AND type='budget'     THEN val END) AS ser_b,
                    -- Costs & Profit
                    MAX(CASE WHEN row_label='TOTAL_DIRECT_COSTS'        AND type='actual'     THEN val END) AS dct_a,
                    MAX(CASE WHEN row_label='TOTAL_DIRECT_CONTRIBUTION' AND type='actual'     THEN val END) AS cont_a,
                    MAX(CASE WHEN row_label='TOTAL_OVERHEADS'           AND type='actual'     THEN val END) AS oh_a,
                    MAX(CASE WHEN row_label='EBITDA'                    AND type='actual'     THEN val END) AS eb_a,
                    MAX(CASE WHEN row_label='EBITDA'                    AND type='budget'     THEN val END) AS eb_b,
                    MAX(CASE WHEN row_label='EBITDA'                    AND type='prior_year' THEN val END) AS eb_py,
                    -- Cash & KPIs
                    MAX(CASE WHEN row_label='CASH_BALANCE'              AND type='actual'     THEN val END) AS cash,
                    MAX(CASE WHEN row_label='TECH_MRR'                  AND type='actual'     THEN val END) AS tech_mrr,
                    MAX(CASE WHEN row_label='TECH_MRR'                  AND type='budget'     THEN val END) AS tech_mrr_b,
                    MAX(CASE WHEN row_label='TECH_MRR'                  AND type='prior_year' THEN val END) AS tech_mrr_py,
                    MAX(CASE WHEN row_label='ARPC'                      AND type='actual'     THEN val END) AS arpc_a,
                    MAX(CASE WHEN row_label='ARPC'                      AND type='budget'     THEN val END) AS arpc_b,
                    MAX(CASE WHEN row_label='TECH_GROSS_MARGIN_MONTH'   AND type='actual'     THEN val END) AS tech_gm,
                    MAX(CASE WHEN row_label='NET_WORKING_CAPITAL'       AND type='actual'     THEN val END) AS nwc,
                    -- Waterfall
                    MAX(CASE WHEN row_label='WF_REVENUE_START'          AND type='actual'     THEN val END) AS wf_start,
                    MAX(CASE WHEN row_label='WF_ONE_OFF_PREV'           AND type='actual'     THEN val END) AS wf_one_off_prev,
                    MAX(CASE WHEN row_label='WF_ONE_OFF_YTD'            AND type='actual'     THEN val END) AS wf_one_off_ytd,
                    MAX(CASE WHEN row_label='WF_RECURRING_GROWTH'       AND type='actual'     THEN val END) AS wf_rec,
                    MAX(CASE WHEN row_label='WF_ARR_TO_GO'              AND type='actual'     THEN val END) AS wf_arr,
                    MAX(CASE WHEN row_label='WF_WEIGHTED_PIPELINE'      AND type='actual'     THEN val END) AS wf_pipe,
                    MAX(CASE WHEN row_label='WF_BUDGET_ASSUMPTIONS'     AND type='actual'     THEN val END) AS wf_budget,
                    MAX(CASE WHEN row_label='WF_REVENUE_END'            AND type='actual'     THEN val END) AS wf_end
                FROM b
                GROUP BY 1
            ),
            hc AS (
                SELECT
                    period,
                    SUM(actual_fte) AS hc_a,
                    SUM(budget_fte) AS hc_b
                FROM `{project_id}.silver.headcount`
                WHERE portco_id = {repr(portco_id)}
                GROUP BY 1
            )
            SELECT
                p.period,
                p.rev_a,    p.rev_b,    p.rev_py,   p.rev_ytd_a,
                p.eco_a,    p.eco_b,
                p.ems_a,    p.ems_b,
                p.ser_a,    p.ser_b,
                p.dct_a,    p.cont_a,   p.oh_a,
                p.eb_a,     p.eb_b,     p.eb_py,
                p.cash,
                p.tech_mrr, p.tech_mrr_b, p.tech_mrr_py,
                p.arpc_a,   p.arpc_b,
                p.tech_gm,  p.nwc,
                COALESCE(h.hc_a, 0) AS hc_a,
                COALESCE(h.hc_b, 0) AS hc_b,
                p.wf_start, p.wf_one_off_prev, p.wf_one_off_ytd,
                p.wf_rec,   p.wf_arr,   p.wf_pipe, p.wf_budget, p.wf_end
            FROM pnl p
            LEFT JOIN hc h ON p.period = h.period
            WHERE p.period IS NOT NULL
        ) S
        ON T.portco_id = {repr(portco_id)} AND T.period = S.period
        WHEN MATCHED THEN UPDATE SET
            revenue_total_actual       = COALESCE(S.rev_a,   T.revenue_total_actual),
            revenue_total_budget       = COALESCE(S.rev_b,   T.revenue_total_budget),
            revenue_total_prior_year   = COALESCE(S.rev_py,  T.revenue_total_prior_year),
            revenue_total_ytd_actual   = COALESCE(S.rev_ytd_a, T.revenue_total_ytd_actual),
            revenue_ecommerce_actual   = COALESCE(S.eco_a,   T.revenue_ecommerce_actual),
            revenue_ecommerce_budget   = COALESCE(S.eco_b,   T.revenue_ecommerce_budget),
            revenue_ems_actual         = COALESCE(S.ems_a,   T.revenue_ems_actual),
            revenue_ems_budget         = COALESCE(S.ems_b,   T.revenue_ems_budget),
            revenue_services_actual    = COALESCE(S.ser_a,   T.revenue_services_actual),
            revenue_services_budget    = COALESCE(S.ser_b,   T.revenue_services_budget),
            direct_costs_total         = COALESCE(S.dct_a,   T.direct_costs_total),
            contribution_total         = COALESCE(S.cont_a,  T.contribution_total),
            total_overheads            = COALESCE(S.oh_a,    T.total_overheads),
            ebitda_actual              = COALESCE(S.eb_a,    T.ebitda_actual),
            ebitda_budget              = COALESCE(S.eb_b,    T.ebitda_budget),
            ebitda_prior_year          = COALESCE(S.eb_py,   T.ebitda_prior_year),
            cash_balance               = COALESCE(S.cash,    T.cash_balance),
            tech_mrr_actual            = COALESCE(S.tech_mrr,    T.tech_mrr_actual),
            tech_mrr_budget            = COALESCE(S.tech_mrr_b,  T.tech_mrr_budget),
            tech_mrr_prior_year        = COALESCE(S.tech_mrr_py, T.tech_mrr_prior_year),
            arpc_actual                = COALESCE(S.arpc_a,  T.arpc_actual),
            arpc_budget                = COALESCE(S.arpc_b,  T.arpc_budget),
            tech_gross_margin_pct      = COALESCE(S.tech_gm, T.tech_gross_margin_pct),
            net_working_capital        = COALESCE(S.nwc,     T.net_working_capital),
            total_headcount            = COALESCE(S.hc_a,    T.total_headcount),
            headcount_budget           = COALESCE(S.hc_b,    T.headcount_budget),
            wf_revenue_start           = COALESCE(S.wf_start,        T.wf_revenue_start),
            wf_one_off_prev            = COALESCE(S.wf_one_off_prev,  T.wf_one_off_prev),
            wf_one_off_ytd             = COALESCE(S.wf_one_off_ytd,   T.wf_one_off_ytd),
            wf_recurring_growth        = COALESCE(S.wf_rec,  T.wf_recurring_growth),
            wf_arr_ytg                 = COALESCE(S.wf_arr,  T.wf_arr_ytg),
            wf_weighted_pipeline       = COALESCE(S.wf_pipe, T.wf_weighted_pipeline),
            wf_budget_assumptions      = COALESCE(S.wf_budget, T.wf_budget_assumptions),
            wf_revenue_end             = COALESCE(S.wf_end,  T.wf_revenue_end)
        WHEN NOT MATCHED THEN
            INSERT (portco_id, period,
                    revenue_total_actual, revenue_total_budget, revenue_total_prior_year, revenue_total_ytd_actual,
                    revenue_ecommerce_actual, revenue_ecommerce_budget,
                    revenue_ems_actual, revenue_ems_budget,
                    revenue_services_actual, revenue_services_budget,
                    direct_costs_total, contribution_total, total_overheads,
                    ebitda_actual, ebitda_budget, ebitda_prior_year,
                    cash_balance,
                    tech_mrr_actual, tech_mrr_budget, tech_mrr_prior_year,
                    arpc_actual, arpc_budget, tech_gross_margin_pct, net_working_capital,
                    total_headcount, headcount_budget,
                    wf_revenue_start, wf_one_off_prev, wf_one_off_ytd,
                    wf_recurring_growth, wf_arr_ytg, wf_weighted_pipeline,
                    wf_budget_assumptions, wf_revenue_end)
            VALUES ({repr(portco_id)}, S.period,
                    S.rev_a,  S.rev_b,  S.rev_py, S.rev_ytd_a,
                    S.eco_a,  S.eco_b,
                    S.ems_a,  S.ems_b,
                    S.ser_a,  S.ser_b,
                    S.dct_a,  S.cont_a, S.oh_a,
                    S.eb_a,   S.eb_b,   S.eb_py,
                    S.cash,
                    S.tech_mrr, S.tech_mrr_b, S.tech_mrr_py,
                    S.arpc_a, S.arpc_b, S.tech_gm, S.nwc,
                    S.hc_a,   S.hc_b,
                    S.wf_start, S.wf_one_off_prev, S.wf_one_off_ytd,
                    S.wf_rec,   S.wf_arr, S.wf_pipe,
                    S.wf_budget, S.wf_end)
    """
    bq_client.query(merge_query).result()
    print("Gold KPIs merged successfully.")

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
    # This function will pull from bronze.raw_management_accounts 
    # to populate silver.monthly_pnl, silver.headcount, etc.
    print(f"Running Silver core transformation for {portco_id} ({file_name})...")
    
    # 1. Monthly PNL Transformation
    pnl_query = f"""
        INSERT INTO `{project_id}.silver.monthly_pnl` (portco_id, period, segment, revenue_actual, revenue_budget, gross_profit_actual, direct_contribution_actual, ebitda_actual, ebitda_budget, overheads_actual)
        WITH raw as (
            SELECT 
                reporting_period as period,
                LOWER(sheet_name) as sheet,
                LOWER(row_label) as item,
                column_label as type,
                value
            FROM `{project_id}.bronze.raw_management_accounts`
            WHERE file_name = {repr(file_name)}
        )
        SELECT 
            {repr(portco_id)} as portco_id,
            period,
            CASE 
                WHEN sheet LIKE '%ecommerce%' THEN 'ecommerce'
                WHEN sheet LIKE '%ems%' THEN 'ems'
                WHEN sheet LIKE '%services%' THEN 'services'
                ELSE 'total'
            END as segment,
            MAX(CASE WHEN item LIKE '%total revenue%' OR item LIKE '%revenue%' THEN CASE WHEN type='actual' THEN value END END) as rev_a,
            MAX(CASE WHEN item LIKE '%total revenue%' OR item LIKE '%revenue%' THEN CASE WHEN type='budget' THEN value END END) as rev_b,
            MAX(CASE WHEN item LIKE '%gross profit%' THEN CASE WHEN type='actual' THEN value END END) as gp_a,
            MAX(CASE WHEN item LIKE '%direct contribution%' THEN CASE WHEN type='actual' THEN value END END) as cont_a,
            MAX(CASE WHEN item LIKE '%ebitda%' THEN CASE WHEN type='actual' THEN value END END) as eb_a,
            MAX(CASE WHEN item LIKE '%ebitda%' THEN CASE WHEN type='budget' THEN value END END) as eb_b,
            MAX(CASE WHEN item LIKE '%overheads%' THEN CASE WHEN type='actual' THEN value END END) as oh_a
        FROM raw
        GROUP BY 1, 2, 3
    """
    bq_client.query(f"DELETE FROM `{project_id}.silver.monthly_pnl` WHERE portco_id = {repr(portco_id)}").result()
    bq_client.query(pnl_query).result()
    print("Populated silver.monthly_pnl.")

    # 2. Headcount Transformation
    hc_query = f"""
        INSERT INTO `{project_id}.silver.headcount` (portco_id, period, segment, team, actual_fte, budget_fte)
        SELECT 
            portco_id,
            reporting_period,
            CASE 
                WHEN row_label LIKE '%Ecommerce%' THEN 'ecommerce'
                WHEN row_label LIKE '%EMS%' THEN 'ems'
                WHEN row_label LIKE '%Services%' THEN 'services'
                ELSE 'central'
            END as segment,
            row_label as team,
            MAX(CASE WHEN column_label='actual' THEN value END) as act,
            MAX(CASE WHEN column_label='budget' THEN value END) as bud
        FROM `{project_id}.bronze.raw_management_accounts`
        WHERE file_name = {repr(file_name)} AND sheet_name='Headcount'
        GROUP BY 1, 2, 3, 4
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
    cols = ["total_revenue_budget", "adjusted_ebitda_budget", "headcount_budget", "total_headcount", 
            "ecommerce_revenue", "ems_revenue", "services_revenue", "gross_profit_total", "contribution_total",
            "arpc", "sm_efficiency", "ytd_revenue_growth", "tech_gross_margin_month", "ebitda_margin_month",
            "net_working_capital", "free_cash_conversion", "revenue_churn_pct", "time_to_value_days", "indicative_ev", "rule_of_40_score"]
    for col in cols:
        try:
           bq_client.query(f"ALTER TABLE `{project_id}.gold.kpi_monthly` ADD COLUMN IF NOT EXISTS {col} FLOAT64").result()
        except: pass

    # This maps silver tables into gold.kpi_monthly (MERGE mode)
    print(f"Merging Gold KPIs for {portco_id} (Monthly P&L rollup)...")
    
    merge_query = f"""
        MERGE `{project_id}.gold.kpi_monthly` T
        USING (
            WITH raw_pnl as (
                SELECT 
                    reporting_period,
                    row_label,
                    column_label as type,
                    MAX(value) as val
                FROM `{project_id}.bronze.raw_management_accounts`
                WHERE portco_id = {repr(portco_id)}
                GROUP BY 1, 2, 3
            ),
            pivoted as (
                SELECT 
                    reporting_period as period,
                    MAX(CASE WHEN row_label = 'TOTAL_REVENUE' AND type='actual' THEN val END) as rev_a,
                    MAX(CASE WHEN row_label = 'TOTAL_REVENUE' AND type='budget' THEN val END) as rev_b,
                    MAX(CASE WHEN row_label = 'GROSS_PROFIT' AND type='actual' THEN val END) as gp_a,
                    MAX(CASE WHEN row_label = 'DIRECT_CONTRIBUTION' AND type='actual' THEN val END) as cont_a,
                    MAX(CASE WHEN row_label = 'ADJUSTED_EBITDA' AND type='actual' THEN val END) as eb_a,
                    MAX(CASE WHEN row_label = 'ADJUSTED_EBITDA' AND type='budget' THEN val END) as eb_b,
                    MAX(CASE WHEN row_label = 'CASH_AT_BANK' AND type='actual' THEN val END) as cash
                FROM raw_pnl
                GROUP BY 1
            ),
            silver_details as (
                SELECT 
                    period,
                    MAX(CASE WHEN segment='ecommerce' THEN revenue_actual END) as rev_eco,
                    MAX(CASE WHEN segment='ems' THEN revenue_actual END) as rev_ems,
                    MAX(CASE WHEN segment='services' THEN revenue_actual END) as rev_ser,
                    MAX(CASE WHEN segment='total' THEN revenue_budget END) as s_rev_b,
                    MAX(CASE WHEN segment='total' THEN ebitda_budget END) as s_eb_b
                FROM `{project_id}.silver.monthly_pnl`
                WHERE portco_id = {repr(portco_id)}
                GROUP BY 1
            ),
            financial_kpis as (
                SELECT
                    reporting_period as period,
                    MAX(CASE WHEN row_label = 'ARPC' THEN value END) as arpc,
                    MAX(CASE WHEN row_label = 'SM_EFFICIENCY' THEN value END) as sm_eff,
                    MAX(CASE WHEN row_label = 'YTD_REVENUE_GROWTH' THEN value END) as ytd_grow,
                    MAX(CASE WHEN row_label = 'TECH_GROSS_MARGIN_MONTH' THEN value END) as tech_gm,
                    MAX(CASE WHEN row_label = 'EBITDA_MARGIN_MONTH' THEN value END) as eb_m,
                    MAX(CASE WHEN row_label = 'NET_WORKING_CAPITAL' THEN value END) as nwc,
                    MAX(CASE WHEN row_label = 'FREE_CASH_CONVERSION' THEN value END) as fcc,
                    MAX(CASE WHEN row_label = 'REVENUE_CHURN_PCT' THEN value END) as churn,
                    MAX(CASE WHEN row_label = 'TIME_TO_VALUE_DAYS' THEN value END) as ttv,
                    MAX(CASE WHEN row_label = 'INDICATIVE_EV' THEN value END) as ev
                FROM `{project_id}.bronze.raw_management_accounts`
                WHERE portco_id = {repr(portco_id)} AND sheet_name = 'Financial KPIs'
                GROUP BY 1
            )
            SELECT 
                p.period,
                p.rev_a,
                COALESCE(p.rev_b, s.s_rev_b) as rev_b,
                p.gp_a,
                p.cont_a,
                p.eb_a,
                COALESCE(p.eb_b, s.s_eb_b) as eb_b,
                p.cash,
                s.rev_eco,
                s.rev_ems,
                s.rev_ser,
                (SELECT SUM(actual_fte) FROM `{project_id}.silver.headcount` h WHERE h.portco_id = {repr(portco_id)} AND h.period = p.period) as hc,
                (SELECT SUM(budget_fte) FROM `{project_id}.silver.headcount` h WHERE h.portco_id = {repr(portco_id)} AND h.period = p.period) as hc_b,
                f.arpc, f.sm_eff, f.ytd_grow, f.tech_gm, f.eb_m, f.nwc, f.fcc, f.churn, f.ttv, f.ev,
                COALESCE(f.ytd_grow, 0) + COALESCE(f.eb_m, 0) as r40
            FROM pivoted p
            LEFT JOIN silver_details s ON p.period = s.period
            LEFT JOIN financial_kpis f ON p.period = f.period
        ) S
        ON T.portco_id = {repr(portco_id)} AND T.period = S.period
        WHEN MATCHED THEN
            UPDATE SET
                revenue_total_actual = COALESCE(S.rev_a, T.revenue_total_actual),
                total_revenue_budget = COALESCE(S.rev_b, T.total_revenue_budget),
                gross_profit_total = COALESCE(S.gp_a, T.gross_profit_total),
                contribution_total = COALESCE(S.cont_a, T.contribution_total),
                ebitda_actual = COALESCE(S.eb_a, T.ebitda_actual),
                adjusted_ebitda_budget = COALESCE(S.eb_b, T.adjusted_ebitda_budget),
                ecommerce_revenue = COALESCE(S.rev_eco, T.ecommerce_revenue),
                ems_revenue = COALESCE(S.rev_ems, T.ems_revenue),
                services_revenue = COALESCE(S.rev_ser, T.services_revenue),
                total_headcount = COALESCE(S.hc, T.total_headcount),
                headcount_budget = COALESCE(S.hc_b, T.headcount_budget),
                cash_balance = COALESCE(S.cash, T.cash_balance),
                arpc = S.arpc,
                sm_efficiency = S.sm_eff,
                ytd_revenue_growth = S.ytd_grow,
                tech_gross_margin_month = S.tech_gm,
                ebitda_margin_month = S.eb_m,
                net_working_capital = S.nwc,
                free_cash_conversion = S.fcc,
                revenue_churn_pct = S.churn,
                time_to_value_days = S.ttv,
                indicative_ev = S.ev,
                rule_of_40_score = S.r40
        WHEN NOT MATCHED THEN
            INSERT (portco_id, period, revenue_total_actual, total_revenue_budget, gross_profit_total, contribution_total, ebitda_actual, adjusted_ebitda_budget, ecommerce_revenue, ems_revenue, services_revenue, total_headcount, headcount_budget, cash_balance, arpc, sm_efficiency, ytd_revenue_growth, tech_gross_margin_month, ebitda_margin_month, net_working_capital, free_cash_conversion, revenue_churn_pct, time_to_value_days, indicative_ev, rule_of_40_score)
            VALUES ({repr(portco_id)}, S.period, S.rev_a, S.rev_b, S.gp_a, S.cont_a, S.eb_a, S.eb_b, S.rev_eco, S.rev_ems, S.rev_ser, S.hc, S.hc_b, S.cash, S.arpc, S.sm_eff, S.ytd_grow, S.tech_gm, S.eb_m, S.nwc, S.fcc, S.churn, S.ttv, S.ev, S.r40)
    """
    bq_client.query(merge_query).result()
    print("Gold KPIs merged (Standardized P&L mapping).")

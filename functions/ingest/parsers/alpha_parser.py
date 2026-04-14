import io
import openpyxl
import pandas as pd
from datetime import datetime
import re

def parse_date(val):
    if isinstance(val, datetime):
        return val.replace(day=1).date().isoformat()
    return "2026-02-01" # Specific fallback for Portco Alpha MA Feb

def parse_alpha_ma(file_content, file_name):
    """
    Advanced Parser for Portco Alpha (Hospitality SaaS).
    Extracts segmented ARR, CARR, Tech GM%, NRR/GRR, and NWC details.
    """
    wb = openpyxl.load_workbook(io.BytesIO(file_content), data_only=True)
    rows_to_insert = []
    
    # --- 1. SEGMENTED REVENUE (P&L Detail) ---
    if 'P&L Detail' in wb.sheetnames:
        ws = wb['P&L Detail']
        reporting_period = parse_date(ws.cell(row=3, column=2).value)
        
        mapping = {
            ' Ecommerce Revenue': 'tech_mrr',
            ' EMS Revenue': 'services_mrr',
            ' Services Revenue': 'other_services_mrr',
            'TOTAL REVENUE': 'total_revenue',
            'Gross Profit': 'total_gross_profit',
            'EBITDA': 'ebitda'
        }
        
        for r in range(4, 100):
            label = str(ws.cell(row=r, column=1).value).strip()
            if label in mapping:
                val = ws.cell(row=r, column=2).value # Actual
                if isinstance(val, (int, float)):
                    rows_to_insert.append({
                        "period": reporting_period,
                        "kpi": mapping[label],
                        "value": float(val),
                        "sheet": "P&L Detail"
                    })

    # --- 2. TECH SPECIFIC MARGIN (Tech P&L) ---
    if 'Ecommerce P&L' in wb.sheetnames:
        ws = wb['Ecommerce P&L']
        for r in range(4, 50):
            label = str(ws.cell(row=r, column=1).value).strip().lower()
            if 'gross margin' in label:
                val = ws.cell(row=r, column=2).value
                if isinstance(val, (int, float)):
                    rows_to_insert.append({
                        "period": reporting_period,
                        "kpi": "tech_gross_margin_pct",
                        "value": float(val) * 100 if val <= 1 else float(val),
                        "sheet": "Ecommerce P&L"
                    })

    # --- 3. RETENTION & HEALTH (Financial KPIs) ---
    if 'Financial KPIs' in wb.sheetnames:
        ws = wb['Financial KPIs']
        kpi_map = [
            (3, 4, "carr"),              # Contracted ARR
            (4, 4, "live_arr"),          # Billing ARR
            (6, 4, "nrr_pct"),           # Net Retention
            (7, 4, "grr_pct"),           # Gross Retention
            (10, 4, "customer_concentration_pct"), # Top 10 focus
            (12, 4, "nps"),              # Customer satisfaction
        ]
        for r, c, kpi in kpi_map:
            val = ws.cell(row=r, column=c).value
            if isinstance(val, (int, float)):
                rows_to_insert.append({
                    "period": reporting_period,
                    "kpi": kpi,
                    "value": float(val),
                    "sheet": "Financial KPIs"
                })

    # --- 4. MODULES & LTV:CAC (Hospitality Metrics) ---
    if 'Hospitality Metrics' in wb.sheetnames:
        ws = wb['Hospitality Metrics']
        h_map = [
            (5, 2, "rooms_module_delta"),
            (6, 2, "spa_module_delta"),
            (7, 2, "f_b_module_delta"),
            (8, 2, "vouchers_module_delta"),
            (15, 2, "ltv_cac_ratio"),
            (16, 2, "cac_payback_months")
        ]
        for r, c, kpi in h_map:
            val = ws.cell(row=r, column=c).value
            if isinstance(val, (int, float)):
                rows_to_insert.append({
                    "period": reporting_period,
                    "kpi": kpi,
                    "value": float(val),
                    "sheet": "Hospitality Metrics"
                })

    # --- 5. NWC & DEBTOR AGING (Balance Sheet) ---
    if 'Balance Sheet' in wb.sheetnames:
        ws = wb['Balance Sheet']
        for r in range(4, 60):
            label = str(ws.cell(row=r, column=1).value).strip().lower()
            if 'debtors' in label:
                # Assuming Debtors Aging is in columns D, E, F (30, 60, 90+)
                rows_to_insert.append({"period": reporting_period, "kpi": "debtors_30d", "value": float(ws.cell(row=r, column=4).value or 0)})
                rows_to_insert.append({"period": reporting_period, "kpi": "debtors_60d", "value": float(ws.cell(row=r, column=5).value or 0)})
                rows_to_insert.append({"period": reporting_period, "kpi": "debtors_90d", "value": float(ws.cell(row=r, column=6).value or 0)})

    return rows_to_insert

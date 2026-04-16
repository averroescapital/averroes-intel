"""
Production parser entrypoint for Portco Alpha (Journey Hospitality SaaS).

This module is a thin compatibility wrapper around the era-based router. The
router inspects sheet signatures and dispatches to era1_parser / era2_parser /
era3_parser as appropriate.

Output row shape (consumed by ma_parser.ingest_ma_to_bronze):
    {
        'period':         'YYYY-MM-01',
        'kpi':            canonical KPI name (see schema.py),
        'value':          float,
        'value_type':     'actual' | 'budget' | 'prior_year' | 'ytd_actual' | 'ytd_budget',
        'business_line':  None | 'ecommerce' | 'ems' | 'services' | 'total',
        'sheet':          source sheet name,
        'source_cell':    e.g. 'B28',
        'era':            'era1' | 'era2' | 'era3',
    }
"""
import io
import openpyxl
from .router import parse as _route_parse


def parse_alpha_ma(file_content, file_name):
    """Parse a Portco Alpha MA workbook. Returns list of canonical rows."""
    wb = openpyxl.load_workbook(io.BytesIO(file_content), data_only=True)
    try:
        rows, era = _route_parse(wb, file_name=file_name)
        print(f"[alpha_parser] {file_name}: era={era}, rows={len(rows)}")
        return rows
    finally:
        wb.close()

"""
Shared utilities for era-specific parsers.

Every era parser returns rows shaped like:
    {period, kpi, value, value_type, business_line, sheet, source_cell}

These are then normalized by ma_parser.py into bronze rows.
"""
from datetime import datetime, date
import re


def parse_date(val):
    """Normalise any date value to first-of-month ISO string.

    Accepts: datetime, date, or None. Returns 'YYYY-MM-01' or None.
    """
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val.replace(day=1).strftime('%Y-%m-%d')
    return None


def period_from_filename(file_name):
    """Best-effort period extraction from filename.

    Handles patterns like:
      'FY25 Management Accounts - June 25.xlsx'        -> 2025-06-01
      'FY26 Management Accounts - November 25.xlsx'    -> 2025-11-01  (FY26 starts Nov 25)
      '4. FY26 Management Accounts - February 26.xlsx' -> 2026-02-01
      'MAfileJan26.xlsx'                               -> 2026-01-01
    """
    if not file_name:
        return None
    name = file_name.lower()

    months = {
        'january':1,'jan':1,
        'february':2,'feb':2,
        'march':3,'mar':3,
        'april':4,'apr':4,
        'may':5,
        'june':6,'jun':6,
        'july':7,'jul':7,
        'august':8,'aug':8,
        'september':9,'sep':9,'sept':9,
        'october':10,'oct':10,
        'november':11,'nov':11,
        'december':12,'dec':12,
    }
    # match "<month> <yy>" where yy = 2-digit year
    m = re.search(r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\s*[- ]?\s*(\d{2})\b', name)
    if m:
        mo = months[m.group(1)]
        yr = 2000 + int(m.group(2))
        return f"{yr:04d}-{mo:02d}-01"
    return None


def safe_number(v):
    """Coerce a cell value to float, or None if not numeric."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    return None


def find_sheet(wb, target_name):
    """Find a sheet by name allowing trailing/leading whitespace and case drift.

    Returns the actual sheetname string, or None.
    """
    target_n = target_name.strip().lower()
    for s in wb.sheetnames:
        if s.strip().lower() == target_n:
            return s
    return None


def row(period, kpi, value, value_type='actual', business_line=None, sheet=None, source_cell=None):
    """Build a canonical row.

    business_line:  None | 'ecommerce' | 'ems' | 'services' | 'total' | 'central'
    value_type:     'actual' | 'budget' | 'prior_year' | 'ytd_actual' | 'ytd_budget'
    """
    return {
        'period': period,
        'kpi': kpi,
        'value': float(value) if value is not None else None,
        'value_type': value_type,
        'business_line': business_line,
        'sheet': sheet,
        'source_cell': source_cell,
    }


def scan_label_rows(ws, label_col=1, max_row=200):
    """Yield (row_num, stripped_label) for rows where col `label_col` has a non-empty string."""
    for r in range(1, min(max_row, ws.max_row) + 1):
        v = ws.cell(r, label_col).value
        if v is None:
            continue
        s = str(v).strip()
        if s:
            yield r, s


def get_cell(ws, r, c):
    """Return value + coordinate string for debugging."""
    v = ws.cell(r, c).value
    return v, ws.cell(r, c).coordinate

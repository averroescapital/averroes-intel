"""
Era 1 parser: covers FY24 (Nov 2023 – Oct 2024) AND FY25 (Nov 2024 – Oct 2025)

Both fiscal years share the 'Summary ' sheet format but with layout drift:

  FY24 early (Nov 23 – ~May 24):  compact — no EBITDA Less Capex row,
      Tech MRR / Cash / NWC / Net Debt / Cash Burn start at R20.
      Col 10 = Variance (£), NOT Prior Year.

  FY24 late  (Jun 24 – Oct 24):   extended — Capitalised Development + EBITDA Less
      Capex rows inserted, Tech MRR at R28, balance sheet at R29+.
      Col 10 = Variance (£), NOT Prior Year.

  FY25       (Nov 24 – Oct 25):   same extended layout but adds LTM Tech MRR row
      between Tech MRR and Cash. Col 10 = Prior Year.

Strategy: P&L section (R5-R18) is stable across all layouts → hardcoded.
Bottom section (EBITDA Less Capex, Tech MRR, Cash, etc.) → label-scanned.
Prior-year column → detected from R3 header text.

Sheet layout:
  - 'Summary '  (trailing space!)    -> BL revenue, direct contrib, Tech MRR, Cash
  - 'Ecommerce P&L'                  -> Success/SetUp/Payment fees breakdown
  - 'EMS P&L'                        -> Subscription breakdown
  - 'Services P&L'                   -> revenue breakdown
  - 'Headcount'                      -> wide format (teams x month columns)
  - 'P&L Summary'                    -> month-level P&L (without BL split)
"""
from .common import find_sheet, safe_number, parse_date, period_from_filename, row


# Column layout in 'Summary ' sheet — stable across FY24 & FY25
SUMMARY_COL_ACTUAL      = 2
SUMMARY_COL_BUDGET      = 3
SUMMARY_COL_YTD_ACTUAL  = 6
SUMMARY_COL_YTD_BUDGET  = 7
SUMMARY_COL_PRIOR_YEAR  = 10  # only valid if header confirms it


def _has_prior_year_column(ws):
    """Check R3 C10 header to see if col 10 is Prior Year or Variance."""
    header = str(ws.cell(3, SUMMARY_COL_PRIOR_YEAR).value or '').strip().lower()
    # FY25 files have 'PY' or 'Prior Year'; FY24 files have 'Variance'
    return 'prior' in header or header == 'py'


def _emit(rows, period, kpi, sheet, ws, r, business_line=None,
          skip_ytd=False, skip_py=False):
    """Emit actual/budget/YTD/prior-year rows from a 'Summary ' layout row."""
    actual = safe_number(ws.cell(r, SUMMARY_COL_ACTUAL).value)
    budget = safe_number(ws.cell(r, SUMMARY_COL_BUDGET).value)
    ytd_a  = safe_number(ws.cell(r, SUMMARY_COL_YTD_ACTUAL).value)
    ytd_b  = safe_number(ws.cell(r, SUMMARY_COL_YTD_BUDGET).value)
    py     = safe_number(ws.cell(r, SUMMARY_COL_PRIOR_YEAR).value)

    if actual is not None:
        rows.append(row(period, kpi, actual, 'actual', business_line, sheet,
                        ws.cell(r, SUMMARY_COL_ACTUAL).coordinate))
    if budget is not None:
        rows.append(row(period, kpi, budget, 'budget', business_line, sheet,
                        ws.cell(r, SUMMARY_COL_BUDGET).coordinate))
    if not skip_ytd and ytd_a is not None:
        rows.append(row(period, kpi, ytd_a, 'ytd_actual', business_line, sheet,
                        ws.cell(r, SUMMARY_COL_YTD_ACTUAL).coordinate))
    if not skip_ytd and ytd_b is not None:
        rows.append(row(period, kpi, ytd_b, 'ytd_budget', business_line, sheet,
                        ws.cell(r, SUMMARY_COL_YTD_BUDGET).coordinate))
    if not skip_py and py is not None:
        rows.append(row(period, kpi, py, 'prior_year', business_line, sheet,
                        ws.cell(r, SUMMARY_COL_PRIOR_YEAR).coordinate))


# Label → KPI mapping for the bottom section (scanned, not hardcoded)
_BOTTOM_LABEL_MAP = {
    'capitalised development':  'CAPEX',
    'ebitda less capex':        'EBITDA_LESS_CAPEX',
    'tech mrr':                 'TECH_MRR',
    'ltm tech mrr':             'LTM_TECH_MRR',
    'cash on hand':             'CASH_ON_HAND',
    'cash at bank':             'CASH_ON_HAND',
    'net working capital':      'NET_WORKING_CAPITAL',
    'net debt':                 'NET_DEBT',
    'cash burn':                'CASH_BURN',
}

# KPIs where YTD / Prior Year don't make sense (snapshot values)
_BOTTOM_SKIP_YTD = {'TECH_MRR', 'LTM_TECH_MRR', 'CASH_ON_HAND',
                    'NET_WORKING_CAPITAL', 'NET_DEBT', 'CASH_BURN', 'CAPEX'}


def _parse_summary(wb, period_hint, rows):
    """Extract BL revenue, costs, contribution, Tech MRR, Cash from 'Summary '."""
    sheet_name = find_sheet(wb, 'Summary')
    if not sheet_name:
        return period_hint
    ws = wb[sheet_name]

    # Period lives in Row 2, Col 2 as end-of-month date
    period = parse_date(ws.cell(2, 2).value) or period_hint
    if period is None:
        return None

    # Detect whether col 10 is Prior Year
    has_py = _has_prior_year_column(ws)
    global_skip_py = not has_py

    # ------- P&L SECTION (R5-R18) — stable across all layouts -------

    # Business-line revenue
    _emit(rows, period, 'REVENUE_ECOMMERCE', sheet_name, ws, 5,
          business_line='ecommerce', skip_py=global_skip_py)
    _emit(rows, period, 'REVENUE_EMS',       sheet_name, ws, 6,
          business_line='ems', skip_py=global_skip_py)
    _emit(rows, period, 'REVENUE_SERVICES',  sheet_name, ws, 7,
          business_line='services', skip_py=global_skip_py)
    _emit(rows, period, 'REVENUE_TOTAL',     sheet_name, ws, 8,
          business_line='total', skip_py=global_skip_py)

    # Direct + Staff Costs
    _emit(rows, period, 'DIRECT_COSTS_ECOMMERCE', sheet_name, ws, 9,
          business_line='ecommerce', skip_py=global_skip_py)
    _emit(rows, period, 'DIRECT_COSTS_EMS',       sheet_name, ws, 10,
          business_line='ems', skip_py=global_skip_py)
    _emit(rows, period, 'DIRECT_COSTS_SERVICES',  sheet_name, ws, 11,
          business_line='services', skip_py=global_skip_py)

    # Direct Contribution
    _emit(rows, period, 'DIRECT_CONTRIBUTION_ECOMMERCE', sheet_name, ws, 13,
          business_line='ecommerce', skip_py=global_skip_py)
    _emit(rows, period, 'DIRECT_CONTRIBUTION_EMS',       sheet_name, ws, 14,
          business_line='ems', skip_py=global_skip_py)
    _emit(rows, period, 'DIRECT_CONTRIBUTION_SERVICES',  sheet_name, ws, 15,
          business_line='services', skip_py=global_skip_py)
    _emit(rows, period, 'DIRECT_CONTRIBUTION_TOTAL',     sheet_name, ws, 16,
          business_line='total', skip_py=global_skip_py)

    # P&L bottom-line (R17 Overheads, R18 EBITDA — stable)
    _emit(rows, period, 'TOTAL_OVERHEADS', sheet_name, ws, 17,
          skip_py=global_skip_py)
    _emit(rows, period, 'EBITDA',          sheet_name, ws, 18,
          skip_py=global_skip_py)

    # ------- BOTTOM SECTION (R19+) — label-scanned for layout resilience -------
    for r in range(19, min(45, ws.max_row) + 1):
        label_raw = ws.cell(r, 1).value
        if label_raw is None:
            continue
        label = str(label_raw).strip().lower()
        kpi = _BOTTOM_LABEL_MAP.get(label)
        if kpi is None:
            continue

        skip_ytd = kpi in _BOTTOM_SKIP_YTD
        _emit(rows, period, kpi, sheet_name, ws, r,
              skip_ytd=skip_ytd, skip_py=(global_skip_py or skip_ytd))

    return period


# Ecommerce P&L — breakdown
# Col 5 = Actual. Row positions vary slightly; scan by label for resilience.
def _parse_ecommerce_pnl(wb, period, rows):
    sn = find_sheet(wb, 'Ecommerce P&L')
    if not sn or period is None:
        return
    ws = wb[sn]

    label_map = {
        'success fees': 'REVENUE_ECOM_SUCCESS_FEES',
        'setup fees':   'REVENUE_ECOM_SETUP_FEES',
        'set up fees':  'REVENUE_ECOM_SETUP_FEES',
        'payment fees': 'REVENUE_ECOM_PAYMENT_FEES',
    }
    for r in range(1, min(40, ws.max_row) + 1):
        label = str(ws.cell(r, 1).value or '').strip().lower()
        kpi = label_map.get(label)
        if kpi is None:
            # Also try partial matching
            for key, kpi_name in label_map.items():
                if key in label:
                    kpi = kpi_name
                    break
        if kpi:
            v = safe_number(ws.cell(r, 5).value)
            if v is not None:
                rows.append(row(period, kpi, v, 'actual', 'ecommerce', sn,
                                ws.cell(r, 5).coordinate))


# EMS P&L — subscription breakdown
def _parse_ems_pnl(wb, period, rows):
    sn = find_sheet(wb, 'EMS P&L')
    if not sn or period is None:
        return
    ws = wb[sn]

    label_map = {
        'total subscription':  'REVENUE_EMS_SUBSCRIPTION',
        'subscription':        'REVENUE_EMS_SUBSCRIPTION',
        'set up':              'REVENUE_EMS_SETUP',
        'setup':               'REVENUE_EMS_SETUP',
        'hardware':            'REVENUE_EMS_HARDWARE',
    }
    for r in range(1, min(40, ws.max_row) + 1):
        raw = str(ws.cell(r, 1).value or '').strip()
        label = raw.lower()
        kpi = label_map.get(label)
        if kpi is None:
            for key, kpi_name in label_map.items():
                if key in label and 'total' not in label:
                    kpi = kpi_name
                    break
            # Prefer 'total subscription' over bare 'subscription'
            if label == 'total subscription':
                kpi = 'REVENUE_EMS_SUBSCRIPTION'
        if kpi:
            v = safe_number(ws.cell(r, 5).value)
            if v is not None:
                rows.append(row(period, kpi, v, 'actual', 'ems', sn,
                                ws.cell(r, 5).coordinate))


# Headcount — Era 1 layout is WIDE: teams in col B, date headers in row 2 across cols.
def _parse_headcount_era1(wb, period, rows):
    sn = find_sheet(wb, 'Headcount')
    if not sn or period is None:
        return
    ws = wb[sn]
    target_ym = period[:7]  # "YYYY-MM"

    # Find target column by scanning row 2 headers
    target_col = None
    for c in range(3, ws.max_column + 1):
        v = ws.cell(2, c).value
        if hasattr(v, 'strftime') and v.strftime('%Y-%m') == target_ym:
            target_col = c
            break
    if target_col is None:
        # Fallback: last dated header <= target (closest past month)
        best = None
        for c in range(3, ws.max_column + 1):
            v = ws.cell(2, c).value
            if hasattr(v, 'strftime'):
                ym = v.strftime('%Y-%m')
                if ym <= target_ym and (best is None or ym > best[1]):
                    best = (c, ym)
        if best:
            target_col = best[0]
    if target_col is None:
        return

    # Determine counts-block end
    block_end = 22
    for r in range(4, min(35, ws.max_row) + 1):
        a = ws.cell(r, 1).value
        if a and str(a).strip().lower().startswith('gross payroll'):
            block_end = r
            break

    total = 0.0
    seen_value = False
    for r in range(4, block_end):
        team = ws.cell(r, 2).value
        if not team:
            continue
        label = str(team).strip()
        if not label or label.lower() in {'total', 'grand total'}:
            continue
        v = safe_number(ws.cell(r, target_col).value)
        if v is None or v == 0:
            continue
        rows.append(row(period, f'HEADCOUNT_{label.upper().replace(" ", "_")}',
                        v, 'actual', None, sn, ws.cell(r, target_col).coordinate))
        total += v
        seen_value = True

    if seen_value:
        rows.append(row(period, 'HEADCOUNT_TOTAL', round(total, 1),
                        'actual', None, sn, None))


def parse(wb, file_name=None):
    """Parse an Era 1 / FY24 file into canonical rows."""
    rows = []
    period_hint = period_from_filename(file_name)
    period = _parse_summary(wb, period_hint, rows)

    # Some files have no date cell in Summary — fall back to filename
    if period is None:
        period = period_hint
    if period is None:
        return rows  # unusable

    _parse_ecommerce_pnl(wb, period, rows)
    _parse_ems_pnl(wb, period, rows)
    _parse_headcount_era1(wb, period, rows)
    return rows

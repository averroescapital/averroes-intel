"""
Era 1 parser: Nov 2024 - Oct 2025 (LEGACY format)

Sheet layout available:
  - 'Summary '  (trailing space!)    -> business-line revenue, direct contrib, Tech MRR, Cash
  - 'Ecommerce P&L'                  -> Success/SetUp/Payment fees breakdown
  - 'EMS P&L'                        -> Subscription breakdown (Spa/Salon/Salonlite/College)
  - 'Services P&L'                   -> revenue breakdown
  - 'Headcount'                      -> wide format (teams x month columns)
  - 'P&L Summary'                    -> month-level P&L (without business-line split)

Missing: P&L Detail, Financial KPIs, Balance Sheet (consolidated), Revenue Waterfall

The 'Summary ' sheet is the richest single source for this era:
  R 2 col B: period (end-of-month date)
  R 5-R 7  : Ecommerce / EMS / Services revenue (actual / budget / variance / % / YTD-actual / YTD-budget / ... / PY / PY-variance / PY-variance% / YE-budget-var / YE-budget-var%)
  R 8      : Total Revenue
  R 9-R11  : Direct costs by business line (negative)
  R12      : Total Direct & Staff Costs
  R13-R15  : Direct Contribution by business line
  R28      : Tech MRR (Actual / Budget / Variance / %)
  R29      : LTM Tech MRR
  R30      : Cash on hand
  R31      : Net Working Capital
  R32      : Net Debt
  R33      : Cash Burn
"""
from .common import find_sheet, safe_number, parse_date, period_from_filename, row


# Column layout in 'Summary ' sheet (confirmed Nov-2024 + Apr-2025 + Oct-2025)
SUMMARY_COL_ACTUAL      = 2
SUMMARY_COL_BUDGET      = 3
SUMMARY_COL_YTD_ACTUAL  = 6
SUMMARY_COL_YTD_BUDGET  = 7
SUMMARY_COL_PRIOR_YEAR  = 10


def _emit(rows, period, kpi, sheet, ws, r, business_line=None, skip_ytd=False, skip_py=False):
    """Helper that emits actual/budget/YTD/prior-year rows from a 'Summary ' layout row."""
    actual = safe_number(ws.cell(r, SUMMARY_COL_ACTUAL).value)
    budget = safe_number(ws.cell(r, SUMMARY_COL_BUDGET).value)
    ytd_a  = safe_number(ws.cell(r, SUMMARY_COL_YTD_ACTUAL).value)
    ytd_b  = safe_number(ws.cell(r, SUMMARY_COL_YTD_BUDGET).value)
    py     = safe_number(ws.cell(r, SUMMARY_COL_PRIOR_YEAR).value)

    if actual is not None:
        rows.append(row(period, kpi, actual, 'actual', business_line, sheet, ws.cell(r, SUMMARY_COL_ACTUAL).coordinate))
    if budget is not None:
        rows.append(row(period, kpi, budget, 'budget', business_line, sheet, ws.cell(r, SUMMARY_COL_BUDGET).coordinate))
    if not skip_ytd and ytd_a is not None:
        rows.append(row(period, kpi, ytd_a, 'ytd_actual', business_line, sheet, ws.cell(r, SUMMARY_COL_YTD_ACTUAL).coordinate))
    if not skip_ytd and ytd_b is not None:
        rows.append(row(period, kpi, ytd_b, 'ytd_budget', business_line, sheet, ws.cell(r, SUMMARY_COL_YTD_BUDGET).coordinate))
    if not skip_py and py is not None:
        rows.append(row(period, kpi, py, 'prior_year', business_line, sheet, ws.cell(r, SUMMARY_COL_PRIOR_YEAR).coordinate))


def _parse_summary(wb, period_hint, rows):
    """Extract business-line revenue, costs, contribution, Tech MRR, Cash from 'Summary '."""
    sheet_name = find_sheet(wb, 'Summary')
    if not sheet_name:
        return period_hint
    ws = wb[sheet_name]

    # Period lives in Row 2, Col 2 as end-of-month date
    period = parse_date(ws.cell(2, 2).value) or period_hint
    if period is None:
        return None

    # Business-line revenue
    _emit(rows, period, 'REVENUE_ECOMMERCE', sheet_name, ws, 5, business_line='ecommerce')
    _emit(rows, period, 'REVENUE_EMS',       sheet_name, ws, 6, business_line='ems')
    _emit(rows, period, 'REVENUE_SERVICES',  sheet_name, ws, 7, business_line='services')
    _emit(rows, period, 'REVENUE_TOTAL',     sheet_name, ws, 8, business_line='total')

    # Direct + Staff Costs (rows 9-11 contain combined direct+staff costs by business line in Era 1)
    _emit(rows, period, 'DIRECT_COSTS_ECOMMERCE', sheet_name, ws, 9,  business_line='ecommerce')
    _emit(rows, period, 'DIRECT_COSTS_EMS',       sheet_name, ws, 10, business_line='ems')
    _emit(rows, period, 'DIRECT_COSTS_SERVICES',  sheet_name, ws, 11, business_line='services')

    # Direct Contribution
    _emit(rows, period, 'DIRECT_CONTRIBUTION_ECOMMERCE', sheet_name, ws, 13, business_line='ecommerce')
    _emit(rows, period, 'DIRECT_CONTRIBUTION_EMS',       sheet_name, ws, 14, business_line='ems')
    _emit(rows, period, 'DIRECT_CONTRIBUTION_SERVICES',  sheet_name, ws, 15, business_line='services')
    _emit(rows, period, 'DIRECT_CONTRIBUTION_TOTAL',     sheet_name, ws, 16, business_line='total')

    # P&L bottom-line (from 'Summary ' — R17 Overheads, R18 EBITDA, R20 EBITDA less capex)
    _emit(rows, period, 'TOTAL_OVERHEADS',    sheet_name, ws, 17)
    _emit(rows, period, 'EBITDA',             sheet_name, ws, 18)
    _emit(rows, period, 'EBITDA_LESS_CAPEX',  sheet_name, ws, 20)

    # Tech MRR (authoritative)
    _emit(rows, period, 'TECH_MRR',     sheet_name, ws, 28, skip_ytd=True, skip_py=True)
    _emit(rows, period, 'LTM_TECH_MRR', sheet_name, ws, 29, skip_ytd=True, skip_py=True)

    # Balance sheet snapshots
    _emit(rows, period, 'CASH_ON_HAND',        sheet_name, ws, 30, skip_ytd=True, skip_py=True)
    _emit(rows, period, 'NET_WORKING_CAPITAL', sheet_name, ws, 31, skip_ytd=True, skip_py=True)
    _emit(rows, period, 'NET_DEBT',            sheet_name, ws, 32, skip_ytd=True, skip_py=True)
    _emit(rows, period, 'CASH_BURN',           sheet_name, ws, 33, skip_ytd=True, skip_py=True)

    return period


# Ecommerce P&L — breakdown (layout confirmed Nov 2024 + Oct 2025)
# Col 5 = Actual. R16=Success, R17=SetUp, R18=Payment, R19=TotalRevenue
def _parse_ecommerce_pnl(wb, period, rows):
    sn = find_sheet(wb, 'Ecommerce P&L')
    if not sn or period is None:
        return
    ws = wb[sn]
    for r, kpi in [(16, 'REVENUE_ECOM_SUCCESS_FEES'),
                   (17, 'REVENUE_ECOM_SETUP_FEES'),
                   (18, 'REVENUE_ECOM_PAYMENT_FEES')]:
        v = safe_number(ws.cell(r, 5).value)
        if v is not None:
            rows.append(row(period, kpi, v, 'actual', 'ecommerce', sn, ws.cell(r, 5).coordinate))


# EMS P&L — subscription breakdown (layout confirmed Nov 2024 + Oct 2025)
# R15=Spa, R16=Salon, R17=Salonlite, R18=College, R19=TotalSubscription, R20=SetUp,
# R21=Memberships, R22=Hardware, R23=Partner, R24=TotalRevenue
def _parse_ems_pnl(wb, period, rows):
    sn = find_sheet(wb, 'EMS P&L')
    if not sn or period is None:
        return
    ws = wb[sn]
    for r, kpi, bl in [(19, 'REVENUE_EMS_SUBSCRIPTION', 'ems'),
                       (20, 'REVENUE_EMS_SETUP',        'ems'),
                       (22, 'REVENUE_EMS_HARDWARE',     'ems')]:
        v = safe_number(ws.cell(r, 5).value)
        if v is not None:
            rows.append(row(period, kpi, v, 'actual', bl, sn, ws.cell(r, 5).coordinate))


# Headcount — Era 1 layout is WIDE: teams in col B (with merged-cell gaps), date
# headers in row 2 across columns C..AJ (can extend to col 36+ as the sheet grows
# historically). There are TWO blocks: counts (R4..~R19) and Gross Payroll (R23+).
# For Era 1 we emit the headcount for the column matching the file's reporting
# period. HEADCOUNT_TOTAL is computed as the sum of numeric rows within the
# counts block (robust across layout drift where an explicit 'Total' row is not
# reliably placed).
def _parse_headcount_era1(wb, period, rows):
    sn = find_sheet(wb, 'Headcount')
    if not sn or period is None:
        return
    ws = wb[sn]
    target_ym = period[:7]  # "YYYY-MM"

    # Find target column by scanning the ENTIRE row-2 width (can go up to col 36+)
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

    # Determine counts-block end: first row whose col A label starts with
    # 'Gross Payroll' marks the start of the next block.
    block_end = 22  # conservative default
    for r in range(4, min(35, ws.max_row) + 1):
        a = ws.cell(r, 1).value
        if a and str(a).strip().lower().startswith('gross payroll'):
            block_end = r
            break

    # Strategy: only count rows where col B has an explicit team label.
    # Rationale: across Era 1 files the layout has a 'Total' row embedded mid-block
    # with no label (e.g. R19 in Nov-24 = 119). Counting unlabeled rows
    # double-counts. Labelled-only sum may slightly undercount if merged cells
    # drop labels (Apr 25+), but will never inflate the total.
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
    """Parse an Era 1 file into canonical rows."""
    rows = []
    period_hint = period_from_filename(file_name)
    period = _parse_summary(wb, period_hint, rows)

    # Jun/Jul 2025 have no date cell in Summary — fall back to filename
    if period is None:
        period = period_hint
    if period is None:
        return rows  # unusable

    _parse_ecommerce_pnl(wb, period, rows)
    _parse_ems_pnl(wb, period, rows)
    _parse_headcount_era1(wb, period, rows)
    return rows

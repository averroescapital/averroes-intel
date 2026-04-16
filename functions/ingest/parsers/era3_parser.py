"""
Era 3 parser: Jan 2026+ (CANONICAL format, Feb 2026 reference)

Inherits Era 2's P&L Detail / Balance Sheet / Guard Rails / Cosmo / Headcount
parsers and ADDS:
  - 'Financial KPIs'     -> Tech MRR (authoritative), ARPC, Rule of 40, Revenue Churn, Gross Margin
  - 'Revenue Waterfall'  -> ARR Growth, pipeline components

Layout of Financial KPIs (Feb 2026 confirmed):
  R1 C4  = period date (end-of-month)
  R3 col B = 'TECH MRR - CURRENT MONTH' / col F = 'TECH MRR - YTD' / col J = 'SERVICES MRR - CURRENT MONTH'
  R4 col B = 'Prior year'    C = value   F-label + G-value   J-label + K-value
  R5 col B = 'Budget'        C = value   ...
  R6 col B = 'Actual'        C = value   ...   col D = delta/margin
  R67 col F = 'RULE OF 40'
  R67 col J = 'REVENUE CHURN'
  R68 col F = 'ARR Growth'

Tech MRR is in RAW £ (not thousands) in Financial KPIs — caller must normalize
if needed.
"""
from .common import find_sheet, safe_number, parse_date, period_from_filename, row
from . import era2_parser


def _block(ws, label_row, label_col, value_col, kpi, rows, period, sheet_name):
    """Read the Prior-year / Budget / Actual block that sits at (label_row+1..+3, value_col)."""
    # Prior year
    v = safe_number(ws.cell(label_row + 1, value_col).value)
    if v is not None:
        rows.append(row(period, kpi, v, 'prior_year', None, sheet_name, ws.cell(label_row + 1, value_col).coordinate))
    # Budget
    v = safe_number(ws.cell(label_row + 2, value_col).value)
    if v is not None:
        rows.append(row(period, kpi, v, 'budget', None, sheet_name, ws.cell(label_row + 2, value_col).coordinate))
    # Actual
    v = safe_number(ws.cell(label_row + 3, value_col).value)
    if v is not None:
        rows.append(row(period, kpi, v, 'actual', None, sheet_name, ws.cell(label_row + 3, value_col).coordinate))


_FIN_KPI_BLOCKS = {
    'TECH MRR - CURRENT MONTH':                'TECH_MRR',
    'TECH MRR - YTD':                          'LTM_TECH_MRR',
    'SERVICES MRR - CURRENT MONTH':            'SERVICES_MRR',
    'SERVICES MRR - YTD':                      'LTM_SERVICES_MRR',
    'AVE REVENUE PER CUSTOMER - CURRENT MONTH':'ARPC',
    'AVE REVENUE PER CUSTOMER - YTD':          'ARPC_YTD',
    'YTD REVENUE GROWTH':                      'YTD_REVENUE_GROWTH',
    'S&M EFFICIENCY':                          'SM_EFFICIENCY',
    'TECH GROSS MARGIN (MONTH)':               'TECH_GROSS_MARGIN',
    'TECH GROSS MARGIN (YTD)':                 'TECH_GROSS_MARGIN_YTD',
    'EBITDA MARGIN (MONTH)':                   'EBITDA_MARGIN',
    'EBITDA MARGIN (YTD)':                     'EBITDA_MARGIN_YTD',
    # NWC: skip — Balance Sheet provides it in £k; Financial KPIs is raw £
    'CASH':                                    'CASH_FIN_KPI',
    'FREE CASH CONVERSION (MONTH)':            'FREE_CASH_CONVERSION',
    'FREE CASH CONVERSION (YTD)':              'FREE_CASH_CONVERSION_YTD',
    'INDICATIVE EV':                           'INDICATIVE_EV',
    'REVENUE CHURN':                           'REVENUE_CHURN',
    'TIME TO VALUE (DAYS)':                    'TIME_TO_VALUE',
}


def parse_financial_kpis(wb, rows):
    sn = find_sheet(wb, 'Financial KPIs')
    if not sn:
        return None
    ws = wb[sn]
    period = parse_date(ws.cell(1, 4).value)
    if period is None:
        return None

    # Scan section header rows (3, 19, 35, 51, 67) for block labels.
    # Each block: header row, then +1=Prior Year, +2=Budget, +3=Actual.
    # Value column is always one to the right of the label column.
    for header_row in (3, 19, 35, 51, 67):
        for c in range(2, min(ws.max_column, 20) + 1):
            label = str(ws.cell(header_row, c).value or '').strip().upper()
            if not label:
                continue
            kpi = _FIN_KPI_BLOCKS.get(label)
            if kpi is None:
                continue
            value_col = c + 1

            if kpi == 'INDICATIVE_EV':
                # Indicative EV is a single value, not a PY/Budget/Actual block.
                v = safe_number(ws.cell(header_row + 2, value_col).value)
                if v is not None:
                    rows.append(row(period, kpi, v, 'actual', None, sn,
                                    ws.cell(header_row + 2, value_col).coordinate))
                continue

            if kpi == 'SM_EFFICIENCY':
                # Non-standard: header+2 row has label "YTD" and value is the actual
                v = safe_number(ws.cell(header_row + 2, value_col).value)
                if v is not None:
                    rows.append(row(period, kpi, v, 'actual', None, sn,
                                    ws.cell(header_row + 2, value_col).coordinate))
                continue

            if kpi == 'TIME_TO_VALUE':
                # Non-standard: target(+2), actual(+3), excl.blocked(+4)
                _block(ws, header_row, c, value_col, kpi, rows, period, sn)
                # Also extract "Excluding blocked" at header+4
                v = safe_number(ws.cell(header_row + 4, value_col).value)
                if v is not None:
                    rows.append(row(period, 'TIME_TO_VALUE_EXCL_BLOCKED', v, 'actual',
                                    None, sn, ws.cell(header_row + 4, value_col).coordinate))
                continue

            # Standard PY/Budget/Actual block
            _block(ws, header_row, c, value_col, kpi, rows, period, sn)

    # Rule of 40 has a non-standard layout (components, not PY/Budget/Actual).
    # Scan rows 67-75 for the specific component labels.
    for r in range(67, min(76, ws.max_row + 1)):
        for c in range(2, min(ws.max_column, 15) + 1):
            label = str(ws.cell(r, c).value or '').strip().upper()
            if label == 'RULE OF 40':
                v = safe_number(ws.cell(r, c + 1).value)
                if v is not None:
                    rows.append(row(period, 'RULE_OF_40', v, 'actual', None, sn,
                                    ws.cell(r, c + 1).coordinate))
            elif label == 'ARR GROWTH':
                v = safe_number(ws.cell(r, c + 1).value)
                if v is not None:
                    rows.append(row(period, 'ARR_GROWTH', v, 'actual', None, sn,
                                    ws.cell(r, c + 1).coordinate))

    return period


# ---------------------------------------------------------------------------
# Revenue Waterfall (Era 3 only)
# R2 "Revenue (start)", R5 "FY26 YTD Recurring Growth", R6 "FY26 ARR YTG"
# R10/R40/R58 etc. — various components.
# Col 2 = value for each named row.
# ---------------------------------------------------------------------------
import re as _re

# Waterfall labels use FY-specific prefixes (e.g. "FY25 Revenue", "FY26 One-off YTD").
# We match by keyword patterns so labels work across any FY.
_WATERFALL_PATTERNS = [
    (_re.compile(r'^fy\d{2}\s+revenue$'),                'WF_REVENUE_START'),
    (_re.compile(r'one.?off$'),                           'WF_ONE_OFF_PREV'),
    (_re.compile(r'one.?off\s+(revenue\s+)?ytd'),         'WF_ONE_OFF_YTD'),
    (_re.compile(r'ytd\s+recurring\s+growth'),            'WF_RECURRING_GROWTH'),
    (_re.compile(r'arr\s+ytg'),                           'WF_ARR_TO_GO'),
    (_re.compile(r'weighted\s+pipeline'),                 'WF_WEIGHTED_PIPELINE'),
    (_re.compile(r'budget\s+assumption'),                 'WF_BUDGET_ASSUMPTIONS'),
    (_re.compile(r'revenue\s+gap'),                       'WF_REVENUE_GAP'),
    (_re.compile(r'^fy\d{2}\s+budget$'),                  'WF_REVENUE_END'),
]


def parse_revenue_waterfall(wb, period, rows):
    sn = find_sheet(wb, 'Revenue Waterfall')
    if not sn or period is None:
        return
    ws = wb[sn]
    for r in range(1, min(20, ws.max_row) + 1):
        label = str(ws.cell(r, 1).value or '').strip().lower()
        if not label:
            continue
        for pattern, kpi in _WATERFALL_PATTERNS:
            if pattern.search(label):
                v = safe_number(ws.cell(r, 2).value)
                if v is not None:
                    rows.append(row(period, kpi, v, 'actual', None, sn, ws.cell(r, 2).coordinate))
                break


# ---------------------------------------------------------------------------
# Customer Numbers (Era 3 — cross-tab, all months in one sheet)
#
# Layout (Feb 2026 confirmed):
#   R2: end-of-month dates across cols B..Y (C26 = YTD)
#   R3: first-of-month dates (use this for period)
#   R4:  "ACTUAL PROPERTIES"
#     R5: Ecom   R6: EMS   R7: Services   R8: Total
#   R10: "ACTUAL REVENUE" (absolute £ — convert to £k)
#     R11: Ecom  R12: EMS  R13: Services  R14: Total
#   R16: "AVE REVENUE PER CUSTOMER"
#     R17: Ecom  R18: EMS  R19: Services  R20: Total
#   R22: "BUDGET PROPERTIES"
#     R23: Ecom  R24: EMS  R25: Services  R26: Total
#   R28+: Geo breakdown (UK / Ireland / Italy / Spain-UAE)
#
# We extract ALL populated month-columns (not just the reporting period)
# so that the backfill run surfaces historical customer numbers. Dedup
# is handled downstream in silver (latest ingested_at wins).
# ---------------------------------------------------------------------------

# Mapping: (row_number, canonical_kpi, business_line)
_CUST_ACTUAL_PROPS = [
    (5,  'MODULES_LIVE_ECOMMERCE', 'ecommerce'),
    (6,  'MODULES_LIVE_EMS',       'ems'),
    (7,  'MODULES_LIVE_SERVICES',  'services'),
    (8,  'MODULES_LIVE_TOTAL',     'total'),
]

_CUST_BUDGET_PROPS = [
    (23, 'MODULES_BUDGET_ECOMMERCE', 'ecommerce'),
    (24, 'MODULES_BUDGET_EMS',       'ems'),
    (25, 'MODULES_BUDGET_SERVICES',  'services'),
    (26, 'MODULES_BUDGET_TOTAL',     'total'),
]

_CUST_ACTUAL_REV = [
    (11, 'CUSTOMER_REVENUE_ECOMMERCE', 'ecommerce'),
    (12, 'CUSTOMER_REVENUE_EMS',       'ems'),
    (13, 'CUSTOMER_REVENUE_SERVICES',  'services'),
    (14, 'CUSTOMER_REVENUE_TOTAL',     'total'),
]

_CUST_ARPC = [
    (17, 'ARPC_ECOMMERCE', 'ecommerce'),
    (18, 'ARPC_EMS',       'ems'),
    (19, 'ARPC_SERVICES',  'services'),
    (20, 'ARPC_TOTAL',     'total'),
]

_CUST_GEO_BLOCKS = {
    'uk':        {'ecom': 29, 'ems': 30, 'services': 31, 'total': 32},
    'ireland':   {'ecom': 35, 'ems': 36, 'services': 37, 'total': 38},
    'italy':     {'ecom': 41, 'ems': 42, 'services': 43, 'total': 44},
    'spain_uae': {'ecom': 47, 'ems': 48, 'services': 49, 'total': 50},
}


def parse_customer_numbers(wb, rows):
    """Extract per-BL property counts, revenue, ARPC, and geo breakdown
    from the 'Customer Numbers' cross-tab sheet."""
    sn = find_sheet(wb, 'Customer Numbers')
    if not sn:
        return
    ws = wb[sn]

    # Build column → period map from R3 (first-of-month dates)
    col_periods = {}
    for c in range(2, ws.max_column + 1):
        period = parse_date(ws.cell(3, c).value)
        if period:
            col_periods[c] = period

    if not col_periods:
        return

    def _extract(mapping, value_type='actual', convert_to_k=False):
        for row_num, kpi, bl in mapping:
            for c, period in col_periods.items():
                v = safe_number(ws.cell(row_num, c).value)
                if v is not None and v != 0:
                    if convert_to_k:
                        v = round(v / 1000.0, 5)
                    rows.append(row(
                        period, kpi, v, value_type, bl, sn,
                        ws.cell(row_num, c).coordinate
                    ))

    # Actual property/module counts
    _extract(_CUST_ACTUAL_PROPS, 'actual')

    # Budget property counts
    _extract(_CUST_BUDGET_PROPS, 'budget')

    # Actual revenue per BL (absolute £ → £k)
    _extract(_CUST_ACTUAL_REV, 'actual', convert_to_k=True)

    # Average revenue per customer per BL
    _extract(_CUST_ARPC, 'actual')

    # Geo breakdown
    for geo, bl_rows in _CUST_GEO_BLOCKS.items():
        geo_mapping = [
            (r, f'PROPERTIES_{geo.upper()}_{bl_key.upper()}',
             bl_key if bl_key != 'total' else 'total')
            for bl_key, r in bl_rows.items()
        ]
        _extract(geo_mapping, 'actual')


# ---------------------------------------------------------------------------
# P&L Summary (Era 3) — extra KPIs not in P&L Detail
#
# Layout (Feb 2026):
#   Col A=Label  B=Actual  C=Budget  D=Variance  E=PY  F=PY Var
#   Col H=YTD Actual  I=YTD Budget  K=YTD PY
#   R5-7  : Revenue by BL (ecom/ems/services)
#   R8    : Total Revenue
#   R9-11 : Direct & Staff Costs by BL
#   R12   : Total Direct & Staff Costs
#   R13-15: Direct Contribution by BL
#   R16   : Total Direct Contribution
#   R17   : Total Overheads
#   R18   : EBITDA
#   R19   : Capitalised Development (= Capex)
#   R20   : EBITDA Less Capex
# Sheet name has trailing space: 'P&L Summary '
# ---------------------------------------------------------------------------
_PNL_SUMMARY_MAP = {
    # (row, kpi, business_line)  —  for contribution by BL, cost by BL
    13: ('DIRECT_CONTRIBUTION_ECOMMERCE', 'ecommerce'),
    14: ('DIRECT_CONTRIBUTION_EMS',       'ems'),
    15: ('DIRECT_CONTRIBUTION_SERVICES',  'services'),
    16: ('DIRECT_CONTRIBUTION_TOTAL',     'total'),
    19: ('CAPITALISED_DEV',               None),
    20: ('EBITDA_LESS_CAPEX',             None),
}


def parse_pnl_summary(wb, period, rows):
    """Extract budget/PY/YTD data from P&L Summary that P&L Detail doesn't have."""
    sn = find_sheet(wb, 'P&L Summary')
    if not sn or period is None:
        return
    ws = wb[sn]

    # Contribution: budget (C) and prior_year (E) for BL breakdown
    for r, (kpi, bl) in _PNL_SUMMARY_MAP.items():
        for c, vt in [(2, 'actual'), (3, 'budget'), (5, 'prior_year')]:
            v = safe_number(ws.cell(r, c).value)
            if v is not None:
                rows.append(row(period, kpi, v, vt, bl, sn, ws.cell(r, c).coordinate))
        # YTD actual (col G=7) and YTD budget (col H=8) and YTD PY (col J=10)
        # Note: col I=9 is YTD Budget Variance, col K=11 is YTD PY Variance
        for c, vt in [(7, 'ytd_actual'), (8, 'ytd_budget'), (10, 'ytd_prior_year')]:
            v = safe_number(ws.cell(r, c).value)
            if v is not None:
                rows.append(row(period, kpi, v, vt, bl, sn, ws.cell(r, c).coordinate))

    # Revenue YTD by BL (rows 5-8)
    _rev_rows = [
        (5, 'REVENUE_ECOMMERCE', 'ecommerce'),
        (6, 'REVENUE_EMS',       'ems'),
        (7, 'REVENUE_SERVICES',  'services'),
        (8, 'REVENUE_TOTAL',     'total'),
    ]
    for r, kpi, bl in _rev_rows:
        for c, vt in [(7, 'ytd_actual'), (8, 'ytd_budget'), (10, 'ytd_prior_year')]:
            v = safe_number(ws.cell(r, c).value)
            if v is not None:
                rows.append(row(period, kpi, v, vt, bl, sn, ws.cell(r, c).coordinate))

    # EBITDA YTD (row 18)
    for c, vt in [(7, 'ytd_actual'), (8, 'ytd_budget'), (10, 'ytd_prior_year')]:
        v = safe_number(ws.cell(18, c).value)
        if v is not None:
            rows.append(row(period, 'EBITDA', v, vt, None, sn, ws.cell(18, c).coordinate))

    # Total Overheads budget/PY (row 17)
    for c, vt in [(3, 'budget'), (5, 'prior_year')]:
        v = safe_number(ws.cell(17, c).value)
        if v is not None:
            rows.append(row(period, 'TOTAL_OVERHEADS', v, vt, None, sn, ws.cell(17, c).coordinate))

    # NOTE: Rows 9-11 in P&L Summary are "Direct & Staff Costs" (combined),
    # not just direct costs. P&L Detail already provides the correct split
    # at rows 9-11 there. Do NOT extract costs from P&L Summary to avoid conflict.


# ---------------------------------------------------------------------------
# Cash Flow (Era 3)
# Layout:  R6=EBITDA  R18=Capex  R26=Net Cash Flow  R28=Opening  R29=Closing
# Col C=Actual  Col D=Budget  Col G=YTD
# ---------------------------------------------------------------------------
def parse_cash_flow(wb, period, rows):
    sn = find_sheet(wb, 'Cash Flow')
    if not sn or period is None:
        return
    ws = wb[sn]
    _cf_map = [
        (6,  'CF_EBITDA'),
        (18, 'CAPEX'),
        (26, 'NET_CASH_FLOW'),
    ]
    for r, kpi in _cf_map:
        for c, vt in [(3, 'actual'), (4, 'budget')]:
            v = safe_number(ws.cell(r, c).value)
            if v is not None:
                rows.append(row(period, kpi, v, vt, None, sn, ws.cell(r, c).coordinate))
        # YTD (col G=7)
        v = safe_number(ws.cell(r, 7).value)
        if v is not None:
            rows.append(row(period, kpi, v, 'ytd_actual', None, sn, ws.cell(r, 7).coordinate))

    # Cash burn = Net Cash Flow actual (R26 C3)
    ncf = safe_number(ws.cell(26, 3).value)
    if ncf is not None:
        # Cash burn is the negative of net cash flow (positive = burning)
        rows.append(row(period, 'CASH_BURN', -ncf, 'actual', None, sn, ws.cell(26, 3).coordinate))


def parse_kpi_data(wb, period, rows):
    """Extract S&M cost and TCV from 'KPI data' sheet.
    Layout: R1 headers (Q1/Q2/Q3/Q4/YTD), R6=TCV, R8=Sales cost. Col F=YTD."""
    sn = find_sheet(wb, 'KPI data')
    if not sn or period is None:
        return
    ws = wb[sn]
    # YTD Sales cost in col 6 (F), row 8
    sales_cost_ytd = safe_number(ws.cell(8, 6).value)
    if sales_cost_ytd is not None:
        rows.append(row(period, 'SM_COST_YTD', sales_cost_ytd, 'actual', None, sn, ws.cell(8, 6).coordinate))
    # YTD TCV in col 6, row 6
    tcv_ytd = safe_number(ws.cell(6, 6).value)
    if tcv_ytd is not None:
        rows.append(row(period, 'TCV_YTD', tcv_ytd, 'actual', None, sn, ws.cell(6, 6).coordinate))


def parse(wb, file_name=None):
    rows = []
    # Financial KPIs is the authoritative source for Tech MRR in Era 3
    period = parse_financial_kpis(wb, rows)

    # P&L Detail for revenue breakdown
    if period is None:
        period = era2_parser.parse_pnl_detail(wb, rows)
    else:
        era2_parser.parse_pnl_detail(wb, rows)

    if period is None:
        period = period_from_filename(file_name)
    if period is None:
        return rows

    era2_parser.parse_business_line_pnls(wb, period, rows)
    # Only fall back to Guard Rails MRR if Financial KPIs didn't emit TECH_MRR
    has_tech_mrr = any(r['kpi'] == 'TECH_MRR' for r in rows)
    if not has_tech_mrr:
        era2_parser.parse_guard_rails(wb, period, rows)

    era2_parser.parse_balance_sheet(wb, period, rows)
    era2_parser.parse_cosmo_portal(wb, period, rows)
    era2_parser.parse_headcount(wb, period, rows)
    era2_parser.parse_gl_covenants(wb, period, rows)
    era2_parser.parse_guard_rails_covenants(wb, period, rows)
    parse_pnl_summary(wb, period, rows)
    parse_cash_flow(wb, period, rows)
    parse_revenue_waterfall(wb, period, rows)
    parse_customer_numbers(wb, rows)
    parse_kpi_data(wb, period, rows)
    return rows

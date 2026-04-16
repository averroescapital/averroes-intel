"""
Era 2 parser: Nov 2025 - Dec 2025 (TRANSITION format)

New sheets vs Era 1:
  - 'P&L Detail'        -> business-line revenue split (R5-R7) + costs + contribution
  - 'Balance Sheet'     -> cash, NWC
  - 'Averroes Guard Rails' -> Tech MRR (R38 Month, R39 YTD)
  - 'GL Covenants'      -> ARR reference (R2)
  - 'Cosmo Portal Upload' -> ARR, Total Modules, churn (R114, R123, etc.)

Still missing: Financial KPIs, Revenue Waterfall

Retained from Era 1: Ecommerce P&L, EMS P&L, Services P&L, Headcount (but now narrow layout)
"""
from .common import find_sheet, safe_number, parse_date, period_from_filename, row


# ---------------------------------------------------------------------------
# P&L Detail — Era 2/3 canonical structure
# Period in R3C2 (first of month)
# Col 2 = Actual, Col 3 = Budget, Col 5 = Prior Year, Col 7 = YTD Actual
# Row labels in col A:
#   R5  = Ecommerce       (revenue)
#   R6  = EMS
#   R7  = Services
#   R8  = Total Revenue
#   R9  = Ecommerce       (direct costs)
#   R10 = EMS
#   R11 = Services
#   R12 = Total Direct Costs
#   R13 = Ecommerce       (staff costs)
#   R14 = EMS
#   R15 = Services
#   R16 = Total Staff Costs
#   R17 = Ecommerce       (direct contribution)
#   R18 = EMS
#   R19 = Services
#   R20 = Total Direct Contribution
# ---------------------------------------------------------------------------
PNL_DETAIL_MAP = [
    # (row, kpi, business_line)
    (5,  'REVENUE_ECOMMERCE',              'ecommerce'),
    (6,  'REVENUE_EMS',                    'ems'),
    (7,  'REVENUE_SERVICES',               'services'),
    (8,  'REVENUE_TOTAL',                  'total'),
    (9,  'DIRECT_COSTS_ECOMMERCE',         'ecommerce'),
    (10, 'DIRECT_COSTS_EMS',               'ems'),
    (11, 'DIRECT_COSTS_SERVICES',          'services'),
    (13, 'STAFF_COSTS_ECOMMERCE',          'ecommerce'),
    (14, 'STAFF_COSTS_EMS',                'ems'),
    (15, 'STAFF_COSTS_SERVICES',           'services'),
    (17, 'DIRECT_CONTRIBUTION_ECOMMERCE',  'ecommerce'),
    (18, 'DIRECT_CONTRIBUTION_EMS',        'ems'),
    (19, 'DIRECT_CONTRIBUTION_SERVICES',   'services'),
    (20, 'DIRECT_CONTRIBUTION_TOTAL',      'total'),
]


def parse_pnl_detail(wb, rows):
    sn = find_sheet(wb, 'P&L Detail')
    if not sn:
        return None
    ws = wb[sn]
    period = parse_date(ws.cell(3, 2).value)
    if period is None:
        return None

    for r, kpi, bl in PNL_DETAIL_MAP:
        actual = safe_number(ws.cell(r, 2).value)
        budget = safe_number(ws.cell(r, 3).value)
        prior  = safe_number(ws.cell(r, 5).value)
        ytd    = safe_number(ws.cell(r, 7).value)
        if actual is not None:
            rows.append(row(period, kpi, actual, 'actual',     bl, sn, ws.cell(r, 2).coordinate))
        if budget is not None:
            rows.append(row(period, kpi, budget, 'budget',     bl, sn, ws.cell(r, 3).coordinate))
        if prior is not None:
            rows.append(row(period, kpi, prior,  'prior_year', bl, sn, ws.cell(r, 5).coordinate))
        if ytd is not None:
            rows.append(row(period, kpi, ytd,    'ytd_actual', bl, sn, ws.cell(r, 7).coordinate))

    # Row after Total Direct Contribution often has EBITDA-related items
    # Scan a few rows below row 20 for labels like EBITDA / Overheads
    for r in range(21, min(50, ws.max_row) + 1):
        label = str(ws.cell(r, 1).value or '').strip().lower()
        if label == 'ebitda' or label == 'adjusted ebitda':
            for val_col, vt in [(2, 'actual'), (3, 'budget'), (5, 'prior_year'), (7, 'ytd_actual')]:
                v = safe_number(ws.cell(r, val_col).value)
                if v is not None:
                    rows.append(row(period, 'EBITDA', v, vt, None, sn, ws.cell(r, val_col).coordinate))
        elif 'total overhead' in label:
            v = safe_number(ws.cell(r, 2).value)
            if v is not None:
                rows.append(row(period, 'TOTAL_OVERHEADS', v, 'actual', None, sn, ws.cell(r, 2).coordinate))

    return period


# ---------------------------------------------------------------------------
# Ecommerce / EMS P&L — same layout as Era 1
# ---------------------------------------------------------------------------
def parse_business_line_pnls(wb, period, rows):
    # Ecommerce P&L
    sn = find_sheet(wb, 'Ecommerce P&L')
    if sn:
        ws = wb[sn]
        for r, kpi in [(16, 'REVENUE_ECOM_SUCCESS_FEES'),
                       (17, 'REVENUE_ECOM_SETUP_FEES'),
                       (18, 'REVENUE_ECOM_PAYMENT_FEES')]:
            v = safe_number(ws.cell(r, 5).value)
            if v is not None:
                rows.append(row(period, kpi, v, 'actual', 'ecommerce', sn, ws.cell(r, 5).coordinate))

    # EMS P&L
    sn = find_sheet(wb, 'EMS P&L')
    if sn:
        ws = wb[sn]
        # Era 2 col layout same as Era 1: col 5 = actual
        # Subscription is normally at R18 or R19, SetUp R19 or R20. Scan safely.
        for r, kpi, bl in [(19, 'REVENUE_EMS_SUBSCRIPTION', 'ems'),
                           (20, 'REVENUE_EMS_SETUP',        'ems'),
                           (22, 'REVENUE_EMS_HARDWARE',     'ems')]:
            v = safe_number(ws.cell(r, 5).value)
            if v is not None:
                rows.append(row(period, kpi, v, 'actual', bl, sn, ws.cell(r, 5).coordinate))


# ---------------------------------------------------------------------------
# Averroes Guard Rails — Tech MRR in Era 2
# R38 = Tech MRR (Month), R39 = Tech MRR (YTD)
# R47 in some variants (Era 3). We scan labels.
# ---------------------------------------------------------------------------
def parse_guard_rails(wb, period, rows):
    sn = find_sheet(wb, 'Averroes Guard Rails')
    if not sn or period is None:
        return
    ws = wb[sn]
    for r in range(1, min(80, ws.max_row) + 1):
        label = str(ws.cell(r, 2).value or '').strip().lower()
        if label == 'tech mrr (month)' or label == 'mrr':
            # Actual is usually col 3, budget col 4 (pattern varies — take first numeric)
            for c in range(3, 8):
                v = safe_number(ws.cell(r, c).value)
                if v is not None:
                    rows.append(row(period, 'TECH_MRR', v, 'actual', None, sn, ws.cell(r, c).coordinate))
                    break
        elif label == 'tech mrr (ytd)':
            for c in range(3, 8):
                v = safe_number(ws.cell(r, c).value)
                if v is not None:
                    rows.append(row(period, 'LTM_TECH_MRR', v, 'actual', None, sn, ws.cell(r, c).coordinate))
                    break


# ---------------------------------------------------------------------------
# Balance Sheet (Era 2/3)
# Period in R3C3
# Labels in col 2 (col B). Actual in col 3, Budget in col 4.
# ---------------------------------------------------------------------------
def parse_balance_sheet(wb, period, rows):
    sn = find_sheet(wb, 'Balance Sheet')
    if not sn or period is None:
        return
    ws = wb[sn]
    for r in range(1, min(200, ws.max_row) + 1):
        label = str(ws.cell(r, 2).value or '').strip().lower()
        if label in {'cash', 'cash at bank', 'cash and cash equivalents'}:
            v = safe_number(ws.cell(r, 3).value)
            if v is not None:
                rows.append(row(period, 'CASH_ON_HAND', v, 'actual', None, sn, ws.cell(r, 3).coordinate))
        elif 'net debt' in label:
            v = safe_number(ws.cell(r, 3).value)
            if v is not None:
                rows.append(row(period, 'NET_DEBT', v, 'actual', None, sn, ws.cell(r, 3).coordinate))
        elif 'working capital' in label and 'net' in label:
            v = safe_number(ws.cell(r, 3).value)
            if v is not None:
                rows.append(row(period, 'NET_WORKING_CAPITAL', v, 'actual', None, sn, ws.cell(r, 3).coordinate))


# ---------------------------------------------------------------------------
# Cosmo Portal Upload — KPIs R112+
# Header row R113 = Nov/Dec/Jan/Feb/Mar (month short names)
# R114 = ARR
# R115 = Headcount
# R121 = Ecommerce Annual Churn %
# R122 = EMS Annual Churn %
# R123 = Total Modules
# R124 = Pipeline Modules
# ---------------------------------------------------------------------------
def parse_cosmo_portal(wb, period, rows):
    sn = find_sheet(wb, 'Cosmo Portal Upload')
    if not sn or period is None:
        return
    ws = wb[sn]
    # Find header row by scanning for 'Nov','Dec','Jan','Feb','Mar' in row 113
    header_row = None
    for r in range(110, min(120, ws.max_row) + 1):
        if str(ws.cell(r, 2).value or '').strip().lower() == 'nov':
            header_row = r
            break
    if header_row is None:
        return

    # Map short-month -> column index
    month_map = {}
    month_abbrev = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
    for c in range(2, min(10, ws.max_column) + 1):
        v = str(ws.cell(header_row, c).value or '').strip().lower()
        if v in month_abbrev:
            month_map[month_abbrev[v]] = c

    target_month = int(period[5:7])
    col = month_map.get(target_month)
    if col is None:
        return

    # Row labels -> KPI mapping (label in col A, row numbers relative to header)
    kpi_rows = {
        'arr':                             ('ARR_DIRECT',    None),
        'headcount':                       ('HEADCOUNT_TOTAL', None),
        'ecommerce annual churn %':        ('ECOMMERCE_CHURN_PCT', 'ecommerce'),
        'ems annual churn %':              ('EMS_CHURN_PCT', 'ems'),
        'total modules':                   ('TOTAL_MODULES', None),
        'number of modules in sales pipeline': ('MODULES_PIPELINE', None),
    }
    for r in range(header_row + 1, min(header_row + 20, ws.max_row) + 1):
        label = str(ws.cell(r, 1).value or '').strip().lower()
        if label in kpi_rows:
            kpi_name, bl = kpi_rows[label]
            v = safe_number(ws.cell(r, col).value)
            if v is not None:
                # Special: if kpi_name is ARR_DIRECT, don't overwrite TECH_ARR (derived)
                # Instead emit as ARR_COVENANT for cross-check purposes
                if kpi_name == 'ARR_DIRECT':
                    rows.append(row(period, 'ARR_COVENANT', v, 'actual', None, sn, ws.cell(r, col).coordinate))
                else:
                    rows.append(row(period, kpi_name, v, 'actual', bl, sn, ws.cell(r, col).coordinate))


# ---------------------------------------------------------------------------
# Headcount (Era 2/3 narrow layout)
# Period in R3C3. Col 2 = team, Col 3 = Actual, Col 4 = Budget, Col 6 = PY
# ---------------------------------------------------------------------------
def parse_headcount(wb, period, rows):
    sn = find_sheet(wb, 'Headcount')
    if not sn or period is None:
        return
    ws = wb[sn]
    # Confirm narrow layout by checking R3C3 is a date
    from datetime import datetime as _dt, date as _d
    r3c3 = ws.cell(3, 3).value
    if not isinstance(r3c3, (_dt, _d)):
        return  # probably Era 1 wide layout — handled elsewhere

    for r in range(4, min(50, ws.max_row) + 1):
        team = ws.cell(r, 2).value
        if not team:
            continue
        team_label = str(team).strip()
        if not team_label:
            continue
        actual = safe_number(ws.cell(r, 3).value)
        if team_label.lower() in {'total', 'grand total'}:
            if actual is not None:
                rows.append(row(period, 'HEADCOUNT_TOTAL', actual, 'actual', None, sn, ws.cell(r, 3).coordinate))
            continue
        if actual is not None:
            key = f"HEADCOUNT_{team_label.upper().replace(' ', '_')}"
            rows.append(row(period, key, actual, 'actual', None, sn, ws.cell(r, 3).coordinate))


# ---------------------------------------------------------------------------
# GL Covenants (Era 2+)
#
# Layout (Feb 2026 confirmed):
#   ARR section (rows ~2-13):
#     R2 header "ARR Covenant"
#     R11 C4 = Actual ARR (e.g. 9676579.8)
#     R11 C5 = Covenant ARR (e.g. 10416045.3)
#     R12 C4 = Threshold (e.g. 0.9)
#     R13 C4 = Ratio (Actual/Covenant, e.g. 0.929)
#   Interest Cover (rows ~21-26):
#     R24 C4 = Interest expense
#     R25 C4 = EBITDA
#     R26 C4 = Ratio (Interest/EBITDA)
#   Debt Service Ratio (rows ~30-35):
#     R35 C4 = Ratio
#   Cash Minimum Balance (row ~38):
#     R38 C4 = Cash min
#
# We use label-scanning for resilience.
# ---------------------------------------------------------------------------
def parse_gl_covenants(wb, period, rows):
    """Parse GL Covenants sheet.

    Layout (Feb 2026 confirmed):
      R2  B='ARR'                         <- section header
      R5-R10: line items (Ecommerce/EMS/Services actual+covenant)
      R11 D=actual total  E=covenant total <- summary row (no label in A/B/C)
      R12 C='Covenant'  D=0.9            <- threshold
      R13 C='Actual'    D=0.929          <- ratio (actual/covenant)
      R21 B='Interest Cover'              <- section header
      R24 C='Interest Charge' D=value
      R25 C='Adjusted EBITDA' D=value
      R26 C='Interest Cover'  D=ratio
      R30 B='Debt Service Ratio Cover'    <- section header
      R35 C='Debt Service Ratio' D=ratio
      R38 B='Cash Minimum Balance'        <- section header
      R41 C='Covenant' D=500000
      R42 C='Cash'     D=value

    Labels appear in cols A, B, OR C. We check all three.
    """
    sn = find_sheet(wb, 'GL Covenants')
    if not sn or period is None:
        return
    ws = wb[sn]

    section = None
    arr_total_found = False

    for r in range(1, min(60, ws.max_row) + 1):
        lab_a = str(ws.cell(r, 1).value or '').strip().lower()
        lab_b = str(ws.cell(r, 2).value or '').strip().lower()
        lab_c = str(ws.cell(r, 3).value or '').strip().lower()
        combined = f"{lab_a} {lab_b} {lab_c}"

        # Section detection (label in B is the primary pattern)
        if lab_b == 'arr' or 'arr covenant' in combined:
            section = 'arr'
            arr_total_found = False
            continue
        elif 'interest cover' in lab_b and 'debt' not in combined:
            section = 'interest'
            continue
        elif 'debt service' in lab_b or 'debt service' in combined:
            section = 'debt'
            continue
        elif 'cash minimum' in lab_b or 'minimum cash' in combined or 'cash min' in combined:
            section = 'cash_min'
            continue

        if section == 'arr':
            # Total row: no labels in A/B/C, both D and E populated (large values)
            if not arr_total_found:
                v = safe_number(ws.cell(r, 4).value)
                v2 = safe_number(ws.cell(r, 5).value)
                # Total row has no labels and both actual+covenant in D+E
                has_label = bool(lab_a or lab_b or lab_c)
                if not has_label and v is not None and v2 is not None and v > 1000:
                    rows.append(row(period, 'GL_ARR_ACTUAL', v, 'actual', None, sn, ws.cell(r, 4).coordinate))
                    rows.append(row(period, 'GL_ARR_COVENANT', v2, 'actual', None, sn, ws.cell(r, 5).coordinate))
                    arr_total_found = True
                    continue
            # After total: threshold and ratio in col C labels
            if 'covenant' in lab_c:
                v = safe_number(ws.cell(r, 4).value)
                if v is not None and v < 2:  # threshold ~0.9
                    rows.append(row(period, 'GL_ARR_THRESHOLD', v, 'actual', None, sn, ws.cell(r, 4).coordinate))
            elif 'actual' in lab_c:
                v = safe_number(ws.cell(r, 4).value)
                if v is not None and v < 2:  # ratio ~0.929
                    rows.append(row(period, 'GL_ARR_RATIO', v, 'actual', None, sn, ws.cell(r, 4).coordinate))

        elif section == 'interest':
            if 'interest' in lab_c and ('charge' in lab_c or 'expense' in lab_c or 'cost' in lab_c):
                v = safe_number(ws.cell(r, 4).value)
                if v is not None:
                    rows.append(row(period, 'GL_INTEREST_COVER_INTEREST', v, 'actual', None, sn, ws.cell(r, 4).coordinate))
            elif 'ebitda' in lab_c:
                v = safe_number(ws.cell(r, 4).value)
                if v is not None:
                    rows.append(row(period, 'GL_INTEREST_COVER_EBITDA', v, 'actual', None, sn, ws.cell(r, 4).coordinate))
            elif 'interest cover' in lab_c:
                v = safe_number(ws.cell(r, 4).value)
                if v is not None and abs(v) < 100:  # ratio
                    rows.append(row(period, 'GL_INTEREST_COVER_RATIO', v, 'actual', None, sn, ws.cell(r, 4).coordinate))

        elif section == 'debt':
            if 'debt service ratio' in lab_c or 'ratio' in lab_c:
                v = safe_number(ws.cell(r, 4).value)
                if v is not None:
                    rows.append(row(period, 'GL_DEBT_SERVICE_RATIO', v, 'actual', None, sn, ws.cell(r, 4).coordinate))

        elif section == 'cash_min':
            if 'covenant' in lab_c:
                v = safe_number(ws.cell(r, 4).value)
                if v is not None:
                    rows.append(row(period, 'GL_CASH_MIN_BALANCE', v, 'actual', None, sn, ws.cell(r, 4).coordinate))


# ---------------------------------------------------------------------------
# Averroes Guard Rails — Covenant compliance (Era 2+)
#
# Layout (Feb 2026 confirmed):
#   Revenue block:     R5 covenant YTD, R6 actual YTD, R7 ratio (95% threshold)
#   MRR block:         R13 covenant,    R14 actual,    R15 ratio
#   Contribution:      R21 covenant,    R22 actual,    R23 ratio (85% threshold)
#   EBITDA less Capex: R29 covenant,    R30 actual,    R31 ratio
#   Cash Balance:      R37 covenant,    R38 actual,    R39 ratio
#
# Values are in C4 (absolute £ in some blocks, £k in others — we store raw).
# We use label-scanning for resilience across eras.
# ---------------------------------------------------------------------------
def parse_guard_rails_covenants(wb, period, rows):
    """Parse full Averroes Guard Rails for covenant compliance KPIs.

    This is separate from parse_guard_rails() which only extracts Tech MRR.

    Layout (Feb 2026 confirmed):
      Section headers in col B: 'Revenue', 'MRR', 'Contribution',
          'EBITDA less Capex', 'Cash Balance', 'KPIs' (stop).
      Within each section, the triplet is:
        - Covenant: C='Covenant', D=value
        - Actual:   C='Actual' or C='Cash', D=value
        - Ratio:    no label, D=value (small number < 10)
    """
    sn = find_sheet(wb, 'Averroes Guard Rails')
    if not sn or period is None:
        return
    ws = wb[sn]

    # Ordered block definitions: (header_keyword, covenant_kpi, actual_kpi, ratio_kpi)
    block_defs = [
        ('revenue',      'GR_REVENUE_COVENANT_YTD',      'GR_REVENUE_ACTUAL_YTD',      'GR_REVENUE_RATIO'),
        ('mrr',          'GR_MRR_COVENANT',              'GR_MRR_ACTUAL',              'GR_MRR_RATIO'),
        ('contribution', 'GR_CONTRIBUTION_COVENANT_YTD', 'GR_CONTRIBUTION_ACTUAL_YTD', 'GR_CONTRIBUTION_RATIO'),
        ('ebitda',       'GR_EBITDA_CAPEX_COVENANT_YTD', 'GR_EBITDA_CAPEX_ACTUAL_YTD', 'GR_EBITDA_CAPEX_RATIO'),
        ('cash',         'GR_CASH_COVENANT',             'GR_CASH_ACTUAL',             'GR_CASH_RATIO'),
    ]

    current_block = None
    current_kpis = None  # (covenant, actual, ratio)
    block_values = 0  # count of numeric values seen in current block

    for r in range(1, min(55, ws.max_row) + 1):
        lab_b = str(ws.cell(r, 2).value or '').strip().lower()
        lab_c = str(ws.cell(r, 3).value or '').strip().lower()

        # Detect section headers (col B only — this is the consistent pattern)
        if lab_b and not lab_c:
            # Stop at KPIs section (different layout)
            if lab_b == 'kpis':
                break
            for keyword, *kpis in block_defs:
                if keyword in lab_b:
                    current_block = keyword
                    current_kpis = kpis
                    block_values = 0
                    break
            continue

        if current_block is None or current_kpis is None:
            continue

        # Within a block: extract from col D
        v = safe_number(ws.cell(r, 4).value)
        if v is None:
            continue

        if block_values < 3:
            rows.append(row(period, current_kpis[block_values], v, 'actual', None, sn, ws.cell(r, 4).coordinate))
            block_values += 1
            if block_values >= 3:
                current_block = None


def parse(wb, file_name=None):
    rows = []
    period = parse_pnl_detail(wb, rows)
    if period is None:
        period = period_from_filename(file_name)
    if period is None:
        return rows

    parse_business_line_pnls(wb, period, rows)
    parse_guard_rails(wb, period, rows)
    parse_balance_sheet(wb, period, rows)
    parse_cosmo_portal(wb, period, rows)
    parse_headcount(wb, period, rows)
    parse_gl_covenants(wb, period, rows)
    parse_guard_rails_covenants(wb, period, rows)
    return rows

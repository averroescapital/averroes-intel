"""
QA Structure Checks — validates Excel file structure BEFORE parsing.

Runs after era detection, before silver/gold write. Non-blocking: logs findings
to bronze.qa_results but never prevents data from flowing through.

Three categories of structure check:
  1. SHEET PRESENCE — expected sheets exist for the detected era
  2. LABEL ANCHORS — key cells contain the expected text the parser will use
  3. PERIOD CELLS  — date/period cells are populated and valid

Each check produces a QAResult dict:
  {file_name, portco_id, period, era, check_category, check_name,
   severity ('info'|'warning'|'error'), sheet, cell, expected, actual, message}
"""
import datetime
import uuid
from parsers.common import find_sheet, parse_date, safe_number

# =========================================================================
# 1. SHEET PRESENCE — per-era expected sheet lists
# =========================================================================

# Sheets required per era (the parser will silently produce no rows if missing)
ERA_REQUIRED_SHEETS = {
    'era1': ['Summary'],
    'era2': ['Summary', 'P&L Detail', 'Balance Sheet'],
    'era3': ['Summary', 'P&L Detail', 'Balance Sheet', 'Financial KPIs'],
}

# Sheets the parser will opportunistically read (warning if missing, not error)
ERA_OPTIONAL_SHEETS = {
    'era1': ['Ecommerce P&L', 'EMS P&L', 'Headcount'],
    'era2': ['Ecommerce P&L', 'EMS P&L', 'Headcount',
             'Averroes Guard Rails', 'GL Covenants', 'Cosmo Portal Upload'],
    'era3': ['Ecommerce P&L', 'EMS P&L', 'Headcount',
             'Averroes Guard Rails', 'GL Covenants', 'Cosmo Portal Upload',
             'Revenue Waterfall', 'Customer Numbers', 'Cash Flow',
             'KPI data', 'P&L Summary'],
}


def _check_sheets(wb, era):
    """Check sheet presence for the given era."""
    results = []

    for sheet in ERA_REQUIRED_SHEETS.get(era, []):
        found = find_sheet(wb, sheet)
        if found is None:
            results.append({
                'check_category': 'sheet_presence',
                'check_name': 'required_sheet_missing',
                'severity': 'error',
                'sheet': sheet,
                'cell': None,
                'expected': f"Sheet '{sheet}' must exist for {era}",
                'actual': 'MISSING',
                'message': f"Required sheet '{sheet}' not found. Parser will produce incomplete data for this era.",
            })
        else:
            results.append({
                'check_category': 'sheet_presence',
                'check_name': 'required_sheet_present',
                'severity': 'info',
                'sheet': found,
                'cell': None,
                'expected': sheet,
                'actual': found,
                'message': f"Required sheet '{found}' present.",
            })

    for sheet in ERA_OPTIONAL_SHEETS.get(era, []):
        found = find_sheet(wb, sheet)
        if found is None:
            results.append({
                'check_category': 'sheet_presence',
                'check_name': 'optional_sheet_missing',
                'severity': 'warning',
                'sheet': sheet,
                'cell': None,
                'expected': f"Sheet '{sheet}' expected for {era}",
                'actual': 'MISSING',
                'message': f"Optional sheet '{sheet}' not found. Some KPIs will be empty.",
            })

    return results


# =========================================================================
# 2. LABEL ANCHORS — verify key cells the parsers depend on
# =========================================================================

def _norm(val):
    """Normalize a cell value to lowercase stripped string."""
    if val is None:
        return ''
    return str(val).strip().lower()


# Each anchor: (sheet_name, row, col, expected_substring, description)
# The check passes if the expected_substring is found in the cell text.

ERA1_LABEL_ANCHORS = [
    # Summary P&L section (R5-R18 hardcoded)
    ('Summary', 5, 1, 'ecommerce',       'Summary R5: Ecommerce revenue label'),
    ('Summary', 6, 1, 'ems',             'Summary R6: EMS revenue label'),
    ('Summary', 7, 1, 'service',         'Summary R7: Services revenue label'),
    ('Summary', 8, 1, 'total',           'Summary R8: Total revenue label'),
    ('Summary', 18, 1, 'ebitda',         'Summary R18: EBITDA label'),
    # Bottom section uses label scanning so we just check a few known ones exist
    # in the R19-R44 range — not tied to a specific row
]

ERA2_LABEL_ANCHORS = [
    # P&L Detail — hardcoded rows
    ('P&L Detail', 5, 1, 'ecommerce',    'P&L Detail R5: Ecommerce revenue'),
    ('P&L Detail', 6, 1, 'ems',          'P&L Detail R6: EMS revenue'),
    ('P&L Detail', 7, 1, 'service',      'P&L Detail R7: Services revenue'),
    ('P&L Detail', 8, 1, 'total',        'P&L Detail R8: Total revenue'),
    ('P&L Detail', 17, 1, 'ecommerce',   'P&L Detail R17: Ecommerce contribution'),
    ('P&L Detail', 20, 1, 'total',       'P&L Detail R20: Total contribution'),
]

ERA3_LABEL_ANCHORS = [
    # Financial KPIs — block header scanning
    # We check that R3 has at least one block header
    ('Financial KPIs', 3, 2, 'tech mrr',  'Financial KPIs R3B: Tech MRR block header'),
]


def _check_label_anchors(wb, era):
    """Verify key label cells contain expected text."""
    results = []

    anchors = list(ERA1_LABEL_ANCHORS)  # era1 anchors apply to all eras
    if era in ('era2', 'era3'):
        anchors.extend(ERA2_LABEL_ANCHORS)
    if era == 'era3':
        anchors.extend(ERA3_LABEL_ANCHORS)

    for (sheet_target, row_num, col_num, expected_sub, desc) in anchors:
        sheet_name = find_sheet(wb, sheet_target)
        if sheet_name is None:
            # Already flagged in sheet presence check
            continue
        ws = wb[sheet_name]
        actual_val = _norm(ws.cell(row_num, col_num).value)

        if expected_sub.lower() in actual_val:
            results.append({
                'check_category': 'label_anchor',
                'check_name': 'label_match',
                'severity': 'info',
                'sheet': sheet_name,
                'cell': ws.cell(row_num, col_num).coordinate,
                'expected': expected_sub,
                'actual': actual_val[:80],
                'message': f"{desc} — OK ('{actual_val[:40]}')",
            })
        else:
            results.append({
                'check_category': 'label_anchor',
                'check_name': 'label_mismatch',
                'severity': 'error',
                'sheet': sheet_name,
                'cell': ws.cell(row_num, col_num).coordinate,
                'expected': expected_sub,
                'actual': actual_val[:80] if actual_val else '(empty)',
                'message': (f"{desc} — MISMATCH. Expected text containing "
                            f"'{expected_sub}', got '{actual_val[:40] or '(empty)'}'. "
                            f"Parser may extract wrong values from this row."),
            })

    # Era 1 specific: check bottom section has at least some expected labels
    if era == 'era1':
        sheet_name = find_sheet(wb, 'Summary')
        if sheet_name:
            ws = wb[sheet_name]
            expected_bottom = {'tech mrr', 'cash on hand', 'cash at bank', 'ebitda'}
            found_bottom = set()
            for r in range(19, min(45, ws.max_row) + 1):
                label = _norm(ws.cell(r, 1).value)
                for eb in expected_bottom:
                    if eb in label:
                        found_bottom.add(eb)
            missing_bottom = expected_bottom - found_bottom
            # At least "tech mrr" and one cash label should be present
            if 'tech mrr' not in found_bottom:
                results.append({
                    'check_category': 'label_anchor',
                    'check_name': 'bottom_label_missing',
                    'severity': 'error',
                    'sheet': sheet_name,
                    'cell': 'R19-R44',
                    'expected': 'tech mrr',
                    'actual': f"Found: {found_bottom or 'none'}",
                    'message': ("Summary bottom section: 'Tech MRR' label not found in R19-R44. "
                                "Tech MRR will not be extracted."),
                })
            if 'cash on hand' not in found_bottom and 'cash at bank' not in found_bottom:
                results.append({
                    'check_category': 'label_anchor',
                    'check_name': 'bottom_label_missing',
                    'severity': 'error',
                    'sheet': sheet_name,
                    'cell': 'R19-R44',
                    'expected': 'cash on hand / cash at bank',
                    'actual': f"Found: {found_bottom or 'none'}",
                    'message': ("Summary bottom section: No cash label found in R19-R44. "
                                "Cash balance will not be extracted."),
                })

    return results


# =========================================================================
# 3. PERIOD CELLS — verify date/period values are populated and valid
# =========================================================================

def _check_period_cells(wb, era, filename_period):
    """Check that period/date cells are populated and parse to valid dates."""
    results = []

    # Era 1: Summary R2C2 has end-of-month date
    sheet_name = find_sheet(wb, 'Summary')
    if sheet_name:
        ws = wb[sheet_name]
        period_val = ws.cell(2, 2).value
        parsed = parse_date(period_val)
        if parsed is None:
            results.append({
                'check_category': 'period_cell',
                'check_name': 'period_missing',
                'severity': 'error',
                'sheet': sheet_name,
                'cell': 'B2',
                'expected': 'Valid date',
                'actual': str(period_val)[:40] if period_val else '(empty)',
                'message': f"Summary B2: No valid date found. Parser will fall back to filename period ({filename_period}).",
            })
        else:
            results.append({
                'check_category': 'period_cell',
                'check_name': 'period_valid',
                'severity': 'info',
                'sheet': sheet_name,
                'cell': 'B2',
                'expected': 'Valid date',
                'actual': parsed,
                'message': f"Summary B2: Period = {parsed}",
            })

    # Era 2/3: P&L Detail R3C2 has first-of-month date
    if era in ('era2', 'era3'):
        sheet_name = find_sheet(wb, 'P&L Detail')
        if sheet_name:
            ws = wb[sheet_name]
            period_val = ws.cell(3, 2).value
            parsed = parse_date(period_val)
            if parsed is None:
                results.append({
                    'check_category': 'period_cell',
                    'check_name': 'period_missing',
                    'severity': 'warning',
                    'sheet': sheet_name,
                    'cell': 'B3',
                    'expected': 'Valid date',
                    'actual': str(period_val)[:40] if period_val else '(empty)',
                    'message': "P&L Detail B3: No valid date. Parser may use fallback period.",
                })

    # Era 3: Financial KPIs R1C4 has end-of-month date
    if era == 'era3':
        sheet_name = find_sheet(wb, 'Financial KPIs')
        if sheet_name:
            ws = wb[sheet_name]
            period_val = ws.cell(1, 4).value
            parsed = parse_date(period_val)
            if parsed is None:
                results.append({
                    'check_category': 'period_cell',
                    'check_name': 'period_missing',
                    'severity': 'error',
                    'sheet': sheet_name,
                    'cell': 'D1',
                    'expected': 'Valid date',
                    'actual': str(period_val)[:40] if period_val else '(empty)',
                    'message': "Financial KPIs D1: No valid date. This is the authoritative period source for era3.",
                })

    # Era 1: Check col-10 header (Prior Year vs Variance detection)
    sheet_name = find_sheet(wb, 'Summary')
    if sheet_name:
        ws = wb[sheet_name]
        col10_header = _norm(ws.cell(3, 10).value)
        if col10_header:
            is_py = 'prior' in col10_header or col10_header == 'py'
            is_var = 'variance' in col10_header or 'var' in col10_header
            if is_py:
                results.append({
                    'check_category': 'period_cell',
                    'check_name': 'col10_type',
                    'severity': 'info',
                    'sheet': sheet_name,
                    'cell': 'J3',
                    'expected': 'Prior Year or Variance',
                    'actual': col10_header,
                    'message': f"Summary J3: Column 10 = Prior Year ('{col10_header}'). PY values will be extracted.",
                })
            elif is_var:
                results.append({
                    'check_category': 'period_cell',
                    'check_name': 'col10_type',
                    'severity': 'info',
                    'sheet': sheet_name,
                    'cell': 'J3',
                    'expected': 'Prior Year or Variance',
                    'actual': col10_header,
                    'message': f"Summary J3: Column 10 = Variance ('{col10_header}'). PY extraction skipped (FY24 format).",
                })
            else:
                results.append({
                    'check_category': 'period_cell',
                    'check_name': 'col10_unknown',
                    'severity': 'warning',
                    'sheet': sheet_name,
                    'cell': 'J3',
                    'expected': 'Prior Year or Variance',
                    'actual': col10_header,
                    'message': f"Summary J3: Unrecognised column 10 header ('{col10_header}'). Parser may misinterpret values.",
                })

    return results


# =========================================================================
# 4. PARSED ROW COUNT — verify parser actually extracted KPIs
# =========================================================================

def _check_parsed_output(parsed_rows, era):
    """Check that parsing produced a reasonable number of rows."""
    results = []
    n = len(parsed_rows)

    # Minimum expected row counts per era
    min_rows = {'era1': 10, 'era2': 20, 'era3': 40}
    expected_min = min_rows.get(era, 10)

    if n == 0:
        results.append({
            'check_category': 'parsed_output',
            'check_name': 'zero_rows',
            'severity': 'error',
            'sheet': None,
            'cell': None,
            'expected': f'>= {expected_min} rows',
            'actual': '0',
            'message': "Parser produced ZERO rows. File may be corrupt or in an unrecognised format.",
        })
    elif n < expected_min:
        results.append({
            'check_category': 'parsed_output',
            'check_name': 'low_row_count',
            'severity': 'warning',
            'sheet': None,
            'cell': None,
            'expected': f'>= {expected_min} rows',
            'actual': str(n),
            'message': f"Parser produced only {n} rows (expected >= {expected_min} for {era}). Some sheets may not have been read.",
        })
    else:
        results.append({
            'check_category': 'parsed_output',
            'check_name': 'row_count_ok',
            'severity': 'info',
            'sheet': None,
            'cell': None,
            'expected': f'>= {expected_min} rows',
            'actual': str(n),
            'message': f"Parser produced {n} rows. Looks healthy for {era}.",
        })

    # Check critical KPIs were extracted
    kpi_set = {r.get('kpi') for r in parsed_rows}
    critical_kpis = {
        'era1': ['REVENUE_TOTAL', 'EBITDA', 'TECH_MRR'],
        'era2': ['REVENUE_TOTAL', 'EBITDA', 'TECH_MRR', 'CASH_ON_HAND'],
        'era3': ['REVENUE_TOTAL', 'EBITDA', 'TECH_MRR', 'CASH_ON_HAND', 'ARPC'],
    }
    for kpi in critical_kpis.get(era, []):
        if kpi not in kpi_set:
            results.append({
                'check_category': 'parsed_output',
                'check_name': 'critical_kpi_missing',
                'severity': 'error',
                'sheet': None,
                'cell': None,
                'expected': kpi,
                'actual': 'NOT FOUND in parsed rows',
                'message': f"Critical KPI '{kpi}' was not extracted. Dashboard will show gaps for this metric.",
            })

    return results


# =========================================================================
# MAIN ORCHESTRATOR
# =========================================================================

def run_qa_checks(wb, era, parsed_rows, file_name, portco_id, filename_period):
    """
    Run all QA structure checks. Returns list of QA result dicts.

    Args:
        wb: openpyxl Workbook
        era: 'era1', 'era2', or 'era3'
        parsed_rows: list of dicts from the parser
        file_name: source filename
        portco_id: portfolio company ID
        filename_period: period string from filename (fallback)
    """
    qa_run_id = str(uuid.uuid4())[:12]
    now = datetime.datetime.utcnow().isoformat()

    # Detect period from parsed data
    periods = [r.get('period') for r in parsed_rows if r.get('period')]
    period = periods[0] if periods else filename_period

    all_results = []
    all_results.extend(_check_sheets(wb, era))
    all_results.extend(_check_label_anchors(wb, era))
    all_results.extend(_check_period_cells(wb, era, filename_period))
    all_results.extend(_check_parsed_output(parsed_rows, era))

    # Stamp metadata on every result
    for r in all_results:
        r['qa_run_id'] = qa_run_id
        r['file_name'] = file_name
        r['portco_id'] = portco_id
        r['period'] = period
        r['era'] = era
        r['checked_at'] = now

    # Summary log
    errors = sum(1 for r in all_results if r['severity'] == 'error')
    warnings = sum(1 for r in all_results if r['severity'] == 'warning')
    infos = sum(1 for r in all_results if r['severity'] == 'info')
    print(f"[QA] {file_name}: {len(all_results)} checks — "
          f"{errors} errors, {warnings} warnings, {infos} info")

    if errors > 0:
        for r in all_results:
            if r['severity'] == 'error':
                print(f"[QA] ERROR: {r['message']}")

    return all_results

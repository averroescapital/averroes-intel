"""
Era router: detects which era an MA file belongs to and dispatches to the
appropriate parser.

Era signatures:
  era1:  has 'Summary ' (trailing space) OR 'Summary' + NO 'P&L Detail'
  era2:  has 'P&L Detail' AND NO 'Financial KPIs'
  era3:  has 'Financial KPIs'
"""
from .common import find_sheet
from . import era1_parser, era2_parser, era3_parser


def detect_era(wb):
    """Return 'era1', 'era2', or 'era3' based on sheet signatures."""
    has_pnl_detail    = find_sheet(wb, 'P&L Detail') is not None
    has_financial_kpi = find_sheet(wb, 'Financial KPIs') is not None
    has_summary       = find_sheet(wb, 'Summary') is not None  # matches 'Summary ' too

    if has_financial_kpi:
        return 'era3'
    if has_pnl_detail:
        return 'era2'
    if has_summary:
        return 'era1'
    # fallback: if none match, treat as era3 so the canonical parser attempts extraction
    return 'era3'


def parse(wb, file_name=None):
    """Main entrypoint used by alpha_parser / ma_parser."""
    era = detect_era(wb)
    if era == 'era1':
        rows = era1_parser.parse(wb, file_name=file_name)
    elif era == 'era2':
        rows = era2_parser.parse(wb, file_name=file_name)
    else:
        rows = era3_parser.parse(wb, file_name=file_name)
    # Stamp the era on every row for bronze traceability
    for r in rows:
        r['era'] = era
    return rows, era

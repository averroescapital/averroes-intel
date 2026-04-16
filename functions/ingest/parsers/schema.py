"""
Canonical KPI schema for Portco Alpha (Journey Hospitality).

This is the authoritative list of metrics our pipeline emits into bronze.
Every era parser produces a SUBSET of this list depending on what the file
contains. Downstream silver/gold transforms assume these exact KPI names.

Source of truth: Feb 2026 file structure.

Business rule (from Nov 2024 verification):
    TECH_MRR = Ecommerce (Success + Payment Fees) + EMS (Total Subscription)
             = Ecommerce Total Revenue - Set Up Fees + EMS Total Subscription
We do NOT recompute — we READ Tech MRR from the authoritative source cell
in each era. Dashboard displays both Total Revenue and Tech MRR so the
difference (non-recurring) is visible.
"""

# ---------------------------------------------------------------------------
# KPI CATALOG
# ---------------------------------------------------------------------------
# Fields:
#   key               canonical KPI name
#   business_line     None | 'ecommerce' | 'ems' | 'services' | 'total' | 'central'
#   availability      set of eras that produce this KPI: {'era1','era2','era3'}
#   description
# ---------------------------------------------------------------------------

KPI_CATALOG = {
    # ---------- Revenue (business-line) ----------
    'REVENUE_ECOMMERCE':              {'business_line': 'ecommerce', 'availability': {'era1','era2','era3'}},
    'REVENUE_EMS':                    {'business_line': 'ems',       'availability': {'era1','era2','era3'}},
    'REVENUE_SERVICES':               {'business_line': 'services',  'availability': {'era1','era2','era3'}},
    'REVENUE_TOTAL':                  {'business_line': 'total',     'availability': {'era1','era2','era3'}},

    # ---------- Ecommerce breakdown ----------
    'REVENUE_ECOM_SUCCESS_FEES':      {'business_line': 'ecommerce', 'availability': {'era1','era2','era3'}},
    'REVENUE_ECOM_SETUP_FEES':        {'business_line': 'ecommerce', 'availability': {'era1','era2','era3'}},
    'REVENUE_ECOM_PAYMENT_FEES':      {'business_line': 'ecommerce', 'availability': {'era1','era2','era3'}},

    # ---------- EMS breakdown ----------
    'REVENUE_EMS_SUBSCRIPTION':       {'business_line': 'ems',       'availability': {'era1','era2','era3'}},
    'REVENUE_EMS_SETUP':              {'business_line': 'ems',       'availability': {'era1','era2','era3'}},
    'REVENUE_EMS_HARDWARE':           {'business_line': 'ems',       'availability': {'era1','era2','era3'}},

    # ---------- Costs (business-line) ----------
    'DIRECT_COSTS_ECOMMERCE':         {'business_line': 'ecommerce', 'availability': {'era1','era2','era3'}},
    'DIRECT_COSTS_EMS':               {'business_line': 'ems',       'availability': {'era1','era2','era3'}},
    'DIRECT_COSTS_SERVICES':          {'business_line': 'services',  'availability': {'era1','era2','era3'}},
    'STAFF_COSTS_ECOMMERCE':          {'business_line': 'ecommerce', 'availability': {'era1','era2','era3'}},
    'STAFF_COSTS_EMS':                {'business_line': 'ems',       'availability': {'era1','era2','era3'}},
    'STAFF_COSTS_SERVICES':           {'business_line': 'services',  'availability': {'era1','era2','era3'}},

    # ---------- Direct Contribution (business-line) ----------
    'DIRECT_CONTRIBUTION_ECOMMERCE':  {'business_line': 'ecommerce', 'availability': {'era1','era2','era3'}},
    'DIRECT_CONTRIBUTION_EMS':        {'business_line': 'ems',       'availability': {'era1','era2','era3'}},
    'DIRECT_CONTRIBUTION_SERVICES':   {'business_line': 'services',  'availability': {'era1','era2','era3'}},
    'DIRECT_CONTRIBUTION_TOTAL':      {'business_line': 'total',     'availability': {'era1','era2','era3'}},

    # ---------- Recurring Revenue / ARR ----------
    # Excel is the source of truth; we READ these, never recompute.
    'TECH_MRR':                       {'business_line': None, 'availability': {'era1','era2','era3'}},
    'TECH_ARR':                       {'business_line': None, 'availability': {'era1','era2','era3'}},  # derived = MRR*12 in silver
    'LTM_TECH_MRR':                   {'business_line': None, 'availability': {'era1','era2','era3'}},
    'SERVICES_MRR':                   {'business_line': 'services', 'availability': {'era3'}},

    # Derived in silver: REVENUE_NON_RECURRING = REVENUE_TOTAL - TECH_MRR

    # ---------- P&L bottom-line ----------
    'TOTAL_OVERHEADS':                {'business_line': None, 'availability': {'era2','era3'}},
    'EBITDA':                         {'business_line': None, 'availability': {'era1','era2','era3'}},
    'GROSS_PROFIT':                   {'business_line': None, 'availability': {'era2','era3'}},

    # ---------- Balance Sheet / Cash ----------
    'CASH_ON_HAND':                   {'business_line': None, 'availability': {'era1','era2','era3'}},
    'NET_WORKING_CAPITAL':            {'business_line': None, 'availability': {'era1','era3'}},
    'NET_DEBT':                       {'business_line': None, 'availability': {'era1','era2','era3'}},
    'CASH_BURN':                      {'business_line': None, 'availability': {'era1'}},

    # ---------- Financial KPIs (Era 3 only) ----------
    'ARPC':                           {'business_line': None, 'availability': {'era3'}},
    'TECH_GROSS_MARGIN':              {'business_line': None, 'availability': {'era3'}},
    'RULE_OF_40':                     {'business_line': None, 'availability': {'era3'}},
    'REVENUE_CHURN':                  {'business_line': None, 'availability': {'era3'}},
    'ARR_GROWTH':                     {'business_line': None, 'availability': {'era3'}},

    # ---------- Modules ----------
    # Total Modules: direct from Cosmo Portal Upload (Oct 2025+)
    'TOTAL_MODULES':                  {'business_line': None, 'availability': {'era1_late','era2','era3'}},
    # Module counts by type: computed from Key Asset Data (Oct 2025+)
    'MODULES_LIVE_ECOMMERCE':         {'business_line': 'ecommerce', 'availability': {'era1_late','era2','era3'}},
    'MODULES_LIVE_EMS':               {'business_line': 'ems',       'availability': {'era1_late','era2','era3'}},
    'MODULES_LIVE_SERVICES':          {'business_line': 'services',  'availability': {'era1_late','era2','era3'}},
    'MODULES_PIPELINE':               {'business_line': None, 'availability': {'era1_late','era2','era3'}},

    # ---------- Modules (from Customer Numbers sheet) ----------
    'MODULES_LIVE_TOTAL':             {'business_line': 'total',     'availability': {'era3'}},
    'MODULES_BUDGET_ECOMMERCE':       {'business_line': 'ecommerce', 'availability': {'era3'}},
    'MODULES_BUDGET_EMS':             {'business_line': 'ems',       'availability': {'era3'}},
    'MODULES_BUDGET_SERVICES':        {'business_line': 'services',  'availability': {'era3'}},
    'MODULES_BUDGET_TOTAL':           {'business_line': 'total',     'availability': {'era3'}},

    # ---------- Customer Revenue / ARPC per BL ----------
    'CUSTOMER_REVENUE_ECOMMERCE':     {'business_line': 'ecommerce', 'availability': {'era3'}},
    'CUSTOMER_REVENUE_EMS':           {'business_line': 'ems',       'availability': {'era3'}},
    'CUSTOMER_REVENUE_SERVICES':      {'business_line': 'services',  'availability': {'era3'}},
    'CUSTOMER_REVENUE_TOTAL':         {'business_line': 'total',     'availability': {'era3'}},
    'ARPC_ECOMMERCE':                 {'business_line': 'ecommerce', 'availability': {'era3'}},
    'ARPC_EMS':                       {'business_line': 'ems',       'availability': {'era3'}},
    'ARPC_SERVICES':                  {'business_line': 'services',  'availability': {'era3'}},
    'ARPC_TOTAL':                     {'business_line': 'total',     'availability': {'era3'}},

    # ---------- Geo-level property counts ----------
    'PROPERTIES_UK_ECOM':             {'business_line': 'ecommerce', 'availability': {'era3'}},
    'PROPERTIES_UK_EMS':              {'business_line': 'ems',       'availability': {'era3'}},
    'PROPERTIES_UK_SERVICES':         {'business_line': 'services',  'availability': {'era3'}},
    'PROPERTIES_UK_TOTAL':            {'business_line': 'total',     'availability': {'era3'}},
    'PROPERTIES_IRELAND_ECOM':        {'business_line': 'ecommerce', 'availability': {'era3'}},
    'PROPERTIES_IRELAND_EMS':         {'business_line': 'ems',       'availability': {'era3'}},
    'PROPERTIES_IRELAND_SERVICES':    {'business_line': 'services',  'availability': {'era3'}},
    'PROPERTIES_IRELAND_TOTAL':       {'business_line': 'total',     'availability': {'era3'}},
    'PROPERTIES_ITALY_ECOM':          {'business_line': 'ecommerce', 'availability': {'era3'}},
    'PROPERTIES_ITALY_EMS':           {'business_line': 'ems',       'availability': {'era3'}},
    'PROPERTIES_ITALY_SERVICES':      {'business_line': 'services',  'availability': {'era3'}},
    'PROPERTIES_ITALY_TOTAL':         {'business_line': 'total',     'availability': {'era3'}},
    'PROPERTIES_SPAIN_UAE_ECOM':      {'business_line': 'ecommerce', 'availability': {'era3'}},
    'PROPERTIES_SPAIN_UAE_EMS':       {'business_line': 'ems',       'availability': {'era3'}},
    'PROPERTIES_SPAIN_UAE_SERVICES':  {'business_line': 'services',  'availability': {'era3'}},
    'PROPERTIES_SPAIN_UAE_TOTAL':     {'business_line': 'total',     'availability': {'era3'}},

    # ---------- GL Covenants ----------
    'GL_ARR_ACTUAL':                  {'business_line': None, 'availability': {'era2','era3'}},
    'GL_ARR_COVENANT':                {'business_line': None, 'availability': {'era2','era3'}},
    'GL_ARR_RATIO':                   {'business_line': None, 'availability': {'era2','era3'}},
    'GL_ARR_THRESHOLD':               {'business_line': None, 'availability': {'era2','era3'}},
    'GL_INTEREST_COVER_INTEREST':     {'business_line': None, 'availability': {'era2','era3'}},
    'GL_INTEREST_COVER_EBITDA':       {'business_line': None, 'availability': {'era2','era3'}},
    'GL_INTEREST_COVER_RATIO':        {'business_line': None, 'availability': {'era2','era3'}},
    'GL_DEBT_SERVICE_RATIO':          {'business_line': None, 'availability': {'era2','era3'}},
    'GL_CASH_MIN_BALANCE':            {'business_line': None, 'availability': {'era2','era3'}},

    # ---------- Averroes Guard Rails (YTD actuals vs covenants) ----------
    'GR_REVENUE_ACTUAL_YTD':          {'business_line': None, 'availability': {'era2','era3'}},
    'GR_REVENUE_COVENANT_YTD':        {'business_line': None, 'availability': {'era2','era3'}},
    'GR_REVENUE_RATIO':               {'business_line': None, 'availability': {'era2','era3'}},
    'GR_MRR_ACTUAL':                  {'business_line': None, 'availability': {'era2','era3'}},
    'GR_MRR_COVENANT':                {'business_line': None, 'availability': {'era2','era3'}},
    'GR_MRR_RATIO':                   {'business_line': None, 'availability': {'era2','era3'}},
    'GR_CONTRIBUTION_ACTUAL_YTD':     {'business_line': None, 'availability': {'era2','era3'}},
    'GR_CONTRIBUTION_COVENANT_YTD':   {'business_line': None, 'availability': {'era2','era3'}},
    'GR_CONTRIBUTION_RATIO':          {'business_line': None, 'availability': {'era2','era3'}},
    'GR_EBITDA_CAPEX_ACTUAL_YTD':     {'business_line': None, 'availability': {'era2','era3'}},
    'GR_EBITDA_CAPEX_COVENANT_YTD':   {'business_line': None, 'availability': {'era2','era3'}},
    'GR_EBITDA_CAPEX_RATIO':          {'business_line': None, 'availability': {'era2','era3'}},
    'GR_CASH_ACTUAL':                 {'business_line': None, 'availability': {'era2','era3'}},
    'GR_CASH_COVENANT':               {'business_line': None, 'availability': {'era2','era3'}},
    'GR_CASH_RATIO':                  {'business_line': None, 'availability': {'era2','era3'}},

    # ---------- Churn ----------
    'ECOMMERCE_CHURN_PCT':            {'business_line': 'ecommerce', 'availability': {'era1_late','era2','era3'}},
    'EMS_CHURN_PCT':                  {'business_line': 'ems',       'availability': {'era1_late','era2','era3'}},

    # ---------- Headcount ----------
    'HEADCOUNT_TOTAL':                {'business_line': None, 'availability': {'era1','era2','era3'}},
    # Per-team HEADCOUNT_<TEAM> are also emitted but kept dynamic
}


def is_known_kpi(key):
    return key in KPI_CATALOG


def kpis_for_era(era):
    """Return set of KPI keys expected for a given era."""
    return {k for k, v in KPI_CATALOG.items() if era in v['availability']}

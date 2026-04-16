import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime
import os
import io
import sys

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Averroes Capital - Portfolio KPI Dashboard",
    layout="wide",
    page_icon="📊"
)

# --- APP REBOOT TOGGLE ---
APP_VERSION = "2.0.3 - Live Cache Buster Free Cash Multip"

PROJECT_ID = "averroes-portfolio-intel"

# ============================================================
# DESIGN SYSTEM - Executive Theme (Navy & White)
# ============================================================
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [data-testid="stAppViewContainer"] {
        background-color: #fcfcfc !important;
        font-family: 'Inter', sans-serif;
    }

    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #0f172a;
        margin-bottom: 0px;
    }
    .sub-header {
        font-size: 0.9rem;
        color: #64748b;
        margin-bottom: 30px;
    }
    .kpi-section-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #0f172a;
        border-bottom: 2px solid #0ea5e9;
        padding-bottom: 8px;
        margin-top: 20px;
        margin-bottom: 25px;
    }

    div[data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        padding: 20px !important;
        border-radius: 4px;
    }
    [data-testid="stMetricValue"] {
        color: #0f172a !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.75rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #64748b !important;
    }

    .status-badge {
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.7rem;
        font-weight: 700;
        text-transform: uppercase;
    }
    .status-on-track { background-color: #dcfce7; color: #166534; }
    .status-watch { background-color: #fef9c3; color: #854d0e; }
    .status-below { background-color: #fee2e2; color: #991b1b; }

    /* ---- PRINT STYLES ---- */
    @media print {
        [data-testid="stSidebar"],
        [data-testid="stToolbar"],
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"],
        header, footer,
        .stButton, .stDownloadButton,
        iframe { display: none !important; }

        html, body, [data-testid="stAppViewContainer"],
        [data-testid="stMain"], [data-testid="block-container"] {
            background-color: #ffffff !important;
            margin: 0 !important;
            padding: 0 !important;
            width: 100% !important;
        }

        [data-testid="stMetric"] {
            border: 1px solid #e2e8f0 !important;
            page-break-inside: avoid;
        }

        .kpi-section-title { page-break-after: avoid; }
    }
    </style>
""", unsafe_allow_html=True)


# ============================================================
# HELPER FUNCTIONS
# ============================================================
def fmt_gbp_k(val, decimals=2):
    """Format value knowing the base unit is already in thousands."""
    if pd.isna(val): return "—"
    raw_val = val * 1000
    abs_val = abs(raw_val)
    if abs_val >= 100_000:
        return f"£{raw_val / 1_000_000:,.{decimals}f} M"
    elif abs_val >= 10_000:
        return f"£{raw_val / 1_000:,.0f} k"
    else:
        return f"£{raw_val:,.0f}"

def fmt_gbp(val, decimals=2):
    """Format as full £ value dynamically scaled to M or k."""
    if pd.isna(val): return "—"
    abs_val = abs(val)
    if abs_val >= 100_000:
        return f"£{val / 1_000_000:,.{decimals}f} M"
    elif abs_val >= 10_000:
        return f"£{val / 1_000:,.0f} k"
    else:
        return f"£{val:,.0f}"

def fmt_pct(val, decimals=1):
    """Format as percentage."""
    if pd.isna(val): return "—"
    return f"{val:,.{decimals}f}%"

def fmt_num(val, decimals=2):
    """Format number dynamically scaled to M or k."""
    if pd.isna(val): return "—"
    abs_val = abs(val)
    if abs_val >= 100_000:
        return f"{val / 1_000_000:,.{decimals}f} M"
    elif abs_val >= 10_000:
        return f"{val / 1_000:,.0f} k"
    else:
        return f"{val:,.0f}"

def fmt_months(val, decimals=1):
    if pd.isna(val): return "—"
    return f"{val:,.{decimals}f} mo"

def delta_pct(actual, comparator):
    """Calculate delta as percentage string for st.metric."""
    if pd.isna(actual) or pd.isna(comparator) or comparator == 0:
        return None
    delta = ((actual - comparator) / abs(comparator)) * 100
    return f"{delta:+.1f}%"

def rag_status(actual, budget, higher_is_better=True):
    """Return RAG status based on actual vs budget."""
    if pd.isna(actual) or pd.isna(budget) or budget == 0:
        return "grey"
    ratio = actual / budget if higher_is_better else budget / actual
    if ratio >= 0.95: return "green"
    elif ratio >= 0.85: return "amber"
    else: return "red"



def get_anomalies(row):
    """Detect critical PE red flags and return a list of actionable alerts."""
    alerts = []
    
    # 1. Cash Runway (Critical Liquidity)
    runway = row.get('cash_runway_months')
    if pd.notna(runway) and runway < 12:
        severity = "🔴 CRITICAL" if runway < 6 else "🟡 WARNING"
        alerts.append({
            "level": severity,
            "metric": "Cash Runway",
            "message": f"Runway is only {runway:.1f} months. Cash balance £{row.get('cash_balance', 0):,.0f} vs monthly burn.",
            "action": "Immediate board trigger. Review liquidity and bridge funding options."
        })

    # 2. Tech Gross Margin Erosion (Scalability Risk)
    tgm = row.get('tech_gross_margin_pct')
    if pd.notna(tgm) and tgm < 75:
        alerts.append({
            "level": "🟡 WARNING",
            "metric": "Tech Gross Margin",
            "message": f"Margin at {tgm:.1f}% is below SaaS benchmark (80%+).",
            "action": "COGS creep detected. Audit hosting and support staffing levels."
        })

    # 3. Revenue Retention (Churn Wave)
    nrr = row.get('revenue_churn_pct') # In this schema churn is stored; check if NRR is derived
    if pd.notna(nrr) and nrr > 5:
        alerts.append({
            "level": "🔴 CRITICAL",
            "metric": "Monthly Churn",
            "message": f"Revenue churn spiked to {nrr:.1f}% this month.",
            "action": "Churn wave incoming. Pull cohort data and review Top 10 customer health."
        })

    # 4. Rule of 40 (Efficiency Drift)
    r40 = row.get('rule_of_40')
    if pd.notna(r40) and r40 < 0.20:
        alerts.append({
            "level": "🟡 WARNING",
            "metric": "Rule of 40",
            "message": f"Efficiency score ({r40:.2f}) fallen below 20%.",
            "action": "Growth or profitability is structurally broken. Diagnose root cause."
        })

    return alerts

# ============================================================
# V2 COLUMN HARMONIZATION
# ============================================================

def harmonize_v2_columns(df):
    """Map gold.kpi_monthly_v2 columns to legacy names used by pe_app views."""
    def _safe(col, default=0):
        return df[col] if col in df.columns else default

    # ARR
    df["total_arr"] = _safe("tech_arr")
    df["carr"] = _safe("tech_arr")

    # Services MRR (not in v2 — use zero)
    df["services_mrr_actual"] = _safe("services_mrr_actual", 0)
    df["services_mrr_budget"] = 0
    df["services_mrr_ytd_actual"] = 0
    df["services_mrr_ytd_budget"] = 0

    # Revenue aliases
    df["revenue_total_budget"] = _safe("revenue_total_budget")
    df["revenue_yoy_growth_pct"] = _safe("revenue_yoy_growth_pct")

    # Profitability
    rev = df["revenue_total_actual"].replace(0, np.nan) if "revenue_total_actual" in df.columns else 1
    df["ebitda_budget"] = _safe("ebitda_budget")
    df["ebitda_margin_pct"] = (_safe("ebitda_actual") / rev * 100).fillna(0) if "ebitda_actual" in df.columns else 0
    df["ebitda_margin_budget_pct"] = (_safe("ebitda_budget") / rev * 100).fillna(0) if "ebitda_budget" in df.columns else 0
    df["ebitda_margin_prior_pct"] = (_safe("ebitda_prior_year") / rev * 100).fillna(0) if "ebitda_prior_year" in df.columns else 0

    # Contribution
    df["contribution_margin_pct"] = 0
    if "contribution_total" in df.columns:
        df["contribution_margin_pct"] = (_safe("contribution_total") / rev * 100).clip(0, 100).fillna(0)

    # Direct costs
    dc_ecom = _safe("direct_costs_ecommerce")
    dc_ems = _safe("direct_costs_ems")
    dc_services = _safe("direct_costs_services")
    df["direct_costs_total"] = dc_ecom + dc_ems + dc_services

    # Tech gross margin
    df["tech_gross_margin_pct"] = _safe("tech_gross_margin_pct")
    df["tech_gross_margin_budget_pct"] = 0
    df["tech_gross_margin_prior_pct"] = 0

    # Cash
    df["cash_balance"] = _safe("cash_balance")
    ebitda_abs = df["ebitda_actual"].abs().replace(0, np.nan) if "ebitda_actual" in df.columns else 1
    df["cash_runway_months"] = (_safe("cash_balance") / ebitda_abs).fillna(0) if "cash_balance" in df.columns else 0

    # Capex
    df["capex"] = 0

    # ARPC
    df["arpc_actual"] = _safe("arpc_actual")
    df["arpc_budget"] = 0

    # S&M / other efficiency
    df["sm_efficiency"] = 0

    # Waterfall (era3 only)
    for wf_col in ['wf_revenue_start', 'wf_one_off_prev', 'wf_one_off_ytd', 'wf_recurring_growth',
                    'wf_arr_ytg', 'wf_weighted_pipeline', 'wf_budget_assumptions', 'wf_revenue_gap', 'wf_revenue_end']:
        if wf_col not in df.columns:
            df[wf_col] = 0

    # Modules
    df["modules_live_total"] = _safe("modules_live_total")
    df["modules_live_ecommerce"] = _safe("modules_live_ecommerce")
    df["modules_live_ems"] = _safe("modules_live_ems")
    df["modules_pipeline"] = _safe("modules_pipeline")

    # Headcount
    df["total_headcount"] = _safe("total_headcount")

    # Covenants
    df["gl_revenue_actual_cumulative"] = _safe("gr_revenue_actual_ytd")
    df["gl_revenue_covenant_cumulative"] = _safe("gr_revenue_covenant_ytd")
    df["gl_ebitda_actual_cumulative"] = _safe("gr_ebitda_capex_actual_ytd")
    df["gl_ebitda_covenant_cumulative"] = _safe("gr_ebitda_capex_covenant_ytd")

    return df


# ============================================================
# DATA LOADING
# ============================================================

BUCKET_NAME = f"{PROJECT_ID}-portfolio-data"
INGEST_PATH = os.path.join(os.path.dirname(__file__), "..", "functions", "ingest")


def _get_gcp_credentials():
    """Return (credentials, project_id) using st.secrets SA or ADC."""
    if "gcp_service_account" in st.secrets:
        from google.oauth2 import service_account
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        return creds, PROJECT_ID
    return None, PROJECT_ID


@st.cache_data(ttl=600)
def load_from_gcs():
    """
    GCS fallback: list every portco-*/MAfile*.xlsx in the portfolio-data bucket,
    download and parse each one with phase1_parser, return combined DataFrame.
    Falls back gracefully if GCS is unreachable.
    """
    try:
        from google.cloud import storage as gcs

        creds, project = _get_gcp_credentials()
        client = gcs.Client(project=project, credentials=creds) if creds else gcs.Client(project=project)
        bucket = client.bucket(BUCKET_NAME)

        # Add ingest path so we can import phase1_parser
        ingest_abs = os.path.abspath(INGEST_PATH)
        if ingest_abs not in sys.path:
            sys.path.insert(0, ingest_abs)
        from phase1_parser import parse_ma_file

        rows = []
        blobs = list(client.list_blobs(BUCKET_NAME))
        ma_blobs = [b for b in blobs if "MAfile" in b.name and b.name.endswith(".xlsx")]

        if not ma_blobs:
            print("GCS: no MAfile*.xlsx objects found in bucket.")
            return pd.DataFrame(), "gcs_no_files"

        for blob in ma_blobs:
            # Derive portco_id from path prefix  (e.g. portco-alpha/MAfileJan26.xlsx)
            parts = blob.name.split("/")
            portco_id = parts[0] if len(parts) >= 2 else "portco-alpha"
            print(f"GCS: parsing {blob.name} for {portco_id}")
            try:
                file_bytes = blob.download_as_bytes()
                parsed = parse_ma_file(file_bytes, portco_id)
                rows.append(parsed)
            except Exception as parse_err:
                print(f"GCS: failed to parse {blob.name}: {parse_err}")

        if not rows:
            return pd.DataFrame(), "gcs_parse_failed"

        df_gcs = pd.DataFrame(rows)
        df_gcs['period'] = pd.to_datetime(df_gcs['period']).dt.tz_localize(None).dt.normalize()
        # Keep latest parse per portco+period
        if 'computed_at' in df_gcs.columns:
            df_gcs = df_gcs.sort_values('computed_at').drop_duplicates(
                subset=['portco_id', 'period'], keep='last'
            )
        else:
            df_gcs = df_gcs.drop_duplicates(subset=['portco_id', 'period'], keep='last')

        print(f"GCS: loaded {len(df_gcs)} rows from {len(ma_blobs)} MA files.")
        return df_gcs, "gcs_fallback"

    except Exception as e:
        print(f"GCS fallback failed: {e}")
        return pd.DataFrame(), "gcs_error"


@st.cache_data(ttl=600)
def load_data():
    """
    Data loading priority:
      1. BigQuery gold.kpi_monthly_v2  (live, single source of truth)
      2. GCS bucket MA files        (parses all MAfile*.xlsx on the fly)
      3. Local gold_phase1_data.csv (emergency static fallback)
    """
    # --- 1. BigQuery ---
    try:
        from google.cloud import bigquery
        creds, project = _get_gcp_credentials()
        client = bigquery.Client(project=project, credentials=creds) if creds else bigquery.Client(project=project)

        query = f"SELECT * FROM `{PROJECT_ID}.gold.kpi_monthly_v2` ORDER BY period ASC"
        df_bq = client.query(query).to_dataframe()

        if not df_bq.empty:
            df_bq['period'] = pd.to_datetime(df_bq['period']).dt.tz_localize(None).dt.normalize()
            if 'computed_at' in df_bq.columns:
                df_bq = df_bq.sort_values('computed_at').drop_duplicates(
                    subset=['portco_id', 'period'], keep='last'
                )
            return df_bq, "connected"
    except Exception as e:
        print(f"BigQuery unavailable: {e}")

    # --- 2. GCS bucket (parse MA files directly) ---
    df_gcs, gcs_status = load_from_gcs()
    if not df_gcs.empty:
        return df_gcs, gcs_status

    # --- 3. Local CSV (emergency only) ---
    csv_path = os.path.join(os.path.dirname(__file__), "gold_phase1_data.csv")
    try:
        df_csv = pd.read_csv(csv_path)
        df_csv['period'] = pd.to_datetime(df_csv['period']).dt.tz_localize(None).dt.normalize()
        if not df_csv.empty:
            return df_csv, "csv_fallback"
    except Exception as csv_e:
        print(f"CSV load warning: {csv_e}")

    return pd.DataFrame(), "error"


df_raw, data_status = load_data()
if not df_raw.empty:
    df_raw = harmonize_v2_columns(df_raw)

# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.title("📊 Averroes Capital")
st.sidebar.markdown("Portfolio Intelligence Platform")
st.sidebar.markdown("---")

if df_raw.empty:
    st.error("No data available. Check BigQuery connection or gold_phase1_data.csv.")
    st.stop()

portco_list = sorted(df_raw['portco_id'].unique())
selected_portco = st.sidebar.selectbox("Select Portfolio Company", portco_list)
pc_df = df_raw[df_raw['portco_id'] == selected_portco].copy()

if pc_df.empty:
    st.warning(f"No data for {selected_portco}")
    st.stop()

# Period selector
periods = sorted(pc_df['period'].unique())
latest_period = periods[-1]
selected_period = st.sidebar.selectbox(
    "Reporting Period",
    periods,
    index=len(periods) - 1,
    format_func=lambda x: pd.Timestamp(x).strftime('%B %Y')
)

row = pc_df[pc_df['period'] == selected_period].iloc[-1]

st.sidebar.markdown("---")
import streamlit.components.v1 as components
status_label = {
    "connected":        "🟢 BigQuery Live",
    "gcs_fallback":     "🟡 GCS Fallback",
    "gcs_no_files":     "🔴 GCS – No MA Files",
    "gcs_parse_failed": "🔴 GCS – Parse Error",
    "gcs_error":        "🔴 GCS Unavailable",
    "csv_fallback":     "🟠 CSV Fallback (static)",
    "error":            "🔴 No Data",
}.get(data_status, "⚪ Unknown")
st.sidebar.caption(f"Data: {status_label}")
_fy_month = row.get('fy_month_num', 0)
_fy_month_int = int(_fy_month) if pd.notna(_fy_month) else 0
st.sidebar.caption(f"Period: {row.get('fy', '')} {row.get('fy_quarter', '')} (Month {_fy_month_int})")
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

# ============================================================
# HEADER
# ============================================================
display_name = selected_portco.replace("portco-", "").title()
period_str = pd.Timestamp(selected_period).strftime('%B %Y')

alerts = get_anomalies(row)

# Header
st.markdown(f'<div class="main-header">{display_name} — Monthly Board Pack</div>', unsafe_allow_html=True)
st.markdown(f'<div class="sub-header">{row.get("fy", "")} {row.get("fy_quarter", "")} | {period_str} | Currency: GBP</div>', unsafe_allow_html=True)
st.markdown("---")

# ============================================================
# ANOMALY & RISK ALERTS
# ============================================================
if alerts:
    with st.expander("🚨 CRITICAL PORTFOLIO ALERTS DETECTED", expanded=True):
        for a in alerts:
            col_l, col_r = st.columns([1, 4])
            with col_l:
                st.markdown(f"**{a['level']}**")
                st.caption(a['metric'])
            with col_r:
                st.markdown(a['message'])
                st.info(f"**Recommended Action:** {a['action']}")
        st.markdown("---")

# ============================================================
# EXECUTIVE SUMMARY - Top-line KPIs
# ============================================================
st.markdown('<div class="kpi-section-title">Executive Summary</div>', unsafe_allow_html=True)

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.metric(
        "Total ARR",
        fmt_gbp(row.get('total_arr')),
        delta_pct(row.get('total_arr'), row.get('total_arr') * 0.9) if pd.notna(row.get('total_arr')) else None
    )
with c2:
    st.metric(
        "Monthly Revenue",
        fmt_gbp_k(row.get('revenue_total_actual')),
        delta_pct(row.get('revenue_total_actual'), row.get('revenue_total_budget'))
    )
with c3:
    st.metric(
        "EBITDA",
        fmt_gbp_k(row.get('ebitda_actual')),
        delta_pct(row.get('ebitda_actual'), row.get('ebitda_budget'))
    )
with c4:
    st.metric(
        "Cash Balance",
        fmt_gbp(row.get('cash_balance')),
    )
with c5:
    st.metric(
        "Rule of 40",
        fmt_pct(row.get('rule_of_40') * 100 if pd.notna(row.get('rule_of_40')) else None),
    )


# ============================================================
# SECTION A: ARR / MRR / Scale
# ============================================================
st.markdown('<div class="kpi-section-title">A. ARR / MRR / Scale</div>', unsafe_allow_html=True)

# Metrics calculation for Section A
tech_mrr_month = row.get('tech_mrr_actual', 0) or 0
serv_mrr_month = row.get('services_mrr_actual', 0) or 0

# --- YTD: Use explicit cumulative MRR columns from the database ---
tech_mrr_ytd = row.get('tech_mrr_ytd_actual', 0) or 0
serv_mrr_ytd = row.get('services_mrr_ytd_actual', 0) or 0
total_mrr_ytd = tech_mrr_ytd + serv_mrr_ytd

tech_arr_annual = tech_mrr_month * 12
serv_arr_annual = serv_mrr_month * 12
total_arr_annual = tech_arr_annual + serv_arr_annual

# --- Row 1: Monthly Momentum ---
st.markdown("### 1. Monthly Recurring Revenue (Current)")
m1, m2, m3 = st.columns(3)
with m1:
    st.metric("Tech MRR (Month)", fmt_gbp(tech_mrr_month), delta_pct(tech_mrr_month, row.get('tech_mrr_budget')))
with m2:
    st.metric("Services MRR (Month)", fmt_gbp(serv_mrr_month), delta_pct(serv_mrr_month, row.get('services_mrr_budget')))
with m3:
    st.metric("Total Group MRR (Month)", fmt_gbp(tech_mrr_month + serv_mrr_month))

# --- Row 2: YTD Cumulative MRR ---
st.markdown("### 2. Cumulative MRR (YTD)")
y1, y2, y3 = st.columns(3)
with y1:
    st.metric("Tech MRR (YTD Cumulative)", fmt_gbp(tech_mrr_ytd))
with y2:
    st.metric("Services MRR (YTD Cumulative)", fmt_gbp(serv_mrr_ytd))
with y3:
    st.metric("Total Group MRR (YTD Cumulative)", fmt_gbp(total_mrr_ytd))

# --- Row 3: Strategic ARR (Annualized) ---
st.markdown("### 3. Annualized ARR (Run Rate)")
a1, a2, a3 = st.columns(3)
with a1:
    st.metric("Tech ARR (Annualized)", fmt_gbp(tech_arr_annual))
with a2:
    st.metric("Services ARR (Annualized)", fmt_gbp(serv_arr_annual))
with a3:
    st.metric("Total Group ARR (Annualized)", fmt_gbp(total_arr_annual))

# --- Row 4: Secondary Metrics ---
st.markdown("---")
s1, s2, s3 = st.columns(3)
with s1:
    st.metric("CARR", fmt_gbp(row.get('carr')))
with s2:
    st.metric("Rev YoY Growth", fmt_pct(row.get('revenue_yoy_growth_pct')))
with s3:
    st.metric("Tech Gross Margin", fmt_pct(row.get('tech_gross_margin_pct')))


# # Strategic Revenue Waterfall Bridge
st.markdown("---")
st.markdown("**Revenue Waterfall Bridge (£k) — FY25 to FY26 Budget**")

wf_labels = ['FY25 Start', 'FY25 One-off', 'FY26 One-off YTD', 'Recur. Growth', 'ARR YTG', 'Weighted Pipe', 'H2 Assumptions', 'Gap', 'FY26 Budget']
wf_vals = [
    row.get('wf_revenue_start') or 0,
    row.get('wf_one_off_prev') or 0,
    row.get('wf_one_off_ytd') or 0,
    row.get('wf_recurring_growth') or 0,
    row.get('wf_arr_ytg') or 0,
    row.get('wf_weighted_pipeline') or 0,
    row.get('wf_budget_assumptions') or 0,
    row.get('wf_revenue_gap') or 0,
    row.get('wf_revenue_end') or 0,
]
wf_measures = ['absolute', 'relative', 'relative', 'relative', 'relative', 'relative', 'relative', 'relative', 'total']

fig_wf = go.Figure(go.Waterfall(
    orientation="v",
    measure=wf_measures,
    x=wf_labels,
    y=wf_vals,
    connector={"line": {"color": "#e2e8f0"}},
    increasing={"marker": {"color": "#22c55e"}},
    decreasing={"marker": {"color": "#ef4444"}},
    totals={"marker": {"color": "#0f172a"}},
    text=[fmt_gbp_k(v) if v != 0 else "—" for v in wf_vals],
    textposition="outside"
))
fig_wf.update_layout(
    height=400, margin=dict(l=40, r=20, t=10, b=40),
    plot_bgcolor='white', font=dict(family='Inter', size=11),
    showlegend=False
)
st.plotly_chart(fig_wf, use_container_width=True)

# Secondary Bar Chart
st.markdown("**Revenue Category Breakdown (£k) — Actual vs Budget**")
rev_cats = ['Ecommerce', 'EMS', 'Services']
rev_actual = [row.get('revenue_ecommerce_actual', 0), row.get('revenue_ems_actual', 0), row.get('revenue_services_actual', 0)]
rev_budget = [row.get('revenue_ecommerce_budget', 0), row.get('revenue_ems_budget', 0), row.get('revenue_services_budget', 0)]

fig_rev = go.Figure()
fig_rev.add_trace(go.Bar(name='Actual', x=rev_cats, y=rev_actual, marker_color='#0ea5e9'))
fig_rev.add_trace(go.Bar(name='Budget', x=rev_cats, y=rev_budget, marker_color='#cbd5e1'))
fig_rev.update_layout(
    barmode='group', height=300,
    margin=dict(l=40, r=20, t=20, b=40),
    legend=dict(orientation='h', y=1.12),
    yaxis_title='£k',
    plot_bgcolor='white',
    font=dict(family='Inter')
)
st.plotly_chart(fig_rev, use_container_width=True)


# ============================================================
# SECTION B: Unit Economics / Margins
# ============================================================
st.markdown('<div class="kpi-section-title">B. Unit Economics / ARPC / Margins</div>', unsafe_allow_html=True)

b1, b2, b3, b4 = st.columns(4)
with b1:
    st.metric("ARPC (Actual)", fmt_gbp(row.get('arpc_actual')),
              delta_pct(row.get('arpc_actual'), row.get('arpc_budget')))
with b2:
    st.metric("S&M Efficiency", fmt_pct(row.get('sm_efficiency', 0) * 100 if pd.notna(row.get('sm_efficiency')) else None))
with b3:
    st.metric("Tech Gross Margin", fmt_pct(row.get('tech_gross_margin_pct')))
with b4:
    st.metric("EBITDA Margin", fmt_pct(row.get('ebitda_margin_pct')))

b5, b6, b7, b8 = st.columns(4)
with b5:
    st.metric("Contribution Margin", fmt_pct(row.get('contribution_margin_pct')))
with b6:
    st.metric("Direct Costs", fmt_gbp_k(row.get('direct_costs_total')))
with b7:
    st.metric("Total Overheads", fmt_gbp_k(row.get('total_overheads')))
with b8:
    st.metric("Capex", fmt_gbp_k(row.get('capex')))


# Margin comparison chart
st.markdown("**Margin Comparison — Actual vs Budget vs Prior Year**")
margin_labels = ['Tech GM%', 'EBITDA Margin%', 'Contribution Margin%']
margin_actual = [row.get('tech_gross_margin_pct', 0), row.get('ebitda_margin_pct', 0), row.get('contribution_margin_pct', 0)]
margin_budget = [row.get('tech_gross_margin_budget_pct', 0), row.get('ebitda_margin_budget_pct', 0), 0]
margin_py = [row.get('tech_gross_margin_prior_pct', 0), row.get('ebitda_margin_prior_pct', 0), 0]

fig_margin = go.Figure()
fig_margin.add_trace(go.Bar(name='Actual', x=margin_labels, y=margin_actual, marker_color='#0ea5e9'))
fig_margin.add_trace(go.Bar(name='Budget', x=margin_labels, y=margin_budget, marker_color='#cbd5e1'))
fig_margin.add_trace(go.Bar(name='Prior Year', x=margin_labels, y=margin_py, marker_color='#fbbf24'))
fig_margin.update_layout(
    barmode='group', height=300,
    margin=dict(l=40, r=20, t=20, b=40),
    legend=dict(orientation='h', y=1.12),
    yaxis_title='%',
    plot_bgcolor='white',
    font=dict(family='Inter')
)
st.plotly_chart(fig_margin, use_container_width=True)


# ============================================================
# SECTION C: P&L Waterfall
# ============================================================
st.markdown('<div class="kpi-section-title">P&L Waterfall (£k)</div>', unsafe_allow_html=True)

waterfall_labels = ['Revenue', 'Direct Costs', 'Gross Profit', 'Overheads', 'EBITDA', 'Capex', 'EBITDA less Capex']
waterfall_values = [
    row.get('revenue_total_actual', 0),
    -abs(row.get('direct_costs_total', 0)),
    row.get('gross_profit_total', 0) if pd.notna(row.get('gross_profit_total')) else (row.get('revenue_total_actual', 0) - abs(row.get('direct_costs_total', 0))),
    row.get('total_overheads', 0),
    row.get('ebitda_actual', 0),
    row.get('capex', 0),
    row.get('ebitda_less_capex', 0)
]
waterfall_measures = ['absolute', 'relative', 'total', 'relative', 'total', 'relative', 'total']

fig_waterfall = go.Figure(go.Waterfall(
    name="P&L", orientation="v",
    measure=waterfall_measures,
    x=waterfall_labels,
    y=waterfall_values,
    connector={"line": {"color": "#e2e8f0"}},
    increasing={"marker": {"color": "#0ea5e9"}},
    decreasing={"marker": {"color": "#ef4444"}},
    totals={"marker": {"color": "#0f172a"}},
    textposition="outside",
    text=[fmt_gbp_k(v) for v in waterfall_values]
))
fig_waterfall.update_layout(
    height=350, margin=dict(l=40, r=20, t=20, b=40),
    plot_bgcolor='white', font=dict(family='Inter'),
    showlegend=False
)
st.plotly_chart(fig_waterfall, use_container_width=True)


# ============================================================
# SECTION C: Retention / Churn
# ============================================================
st.markdown('<div class="kpi-section-title">C. Retention / Churn</div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
with c1:
    churn_val = row.get('revenue_churn_pct')
    target_val = row.get('revenue_churn_target')
    churn_display = fmt_pct(churn_val * 100 if pd.notna(churn_val) else None)
    target_display = f"Target: {fmt_pct(target_val * 100 if pd.notna(target_val) else None)}"
    st.metric("Revenue Churn %", churn_display, target_display)
with c2:
    st.metric("CAC", fmt_gbp(row.get('cac')))
with c3:
    st.metric("CAC Payback", fmt_months(row.get('cac_payback_months')))
with c4:
    st.metric("LTV:CAC Ratio", fmt_num(row.get('ltv_cac_ratio'), 2))


# ============================================================
# SECTION D: Product Usage / Properties
# ============================================================
st.markdown('<div class="kpi-section-title">D. Product Usage / Modules</div>', unsafe_allow_html=True)

d1, d2, d3, d4 = st.columns(4)
with d1:
    st.metric("Properties Live", fmt_num(row.get('properties_live'), 0))
with d2:
    st.metric("Properties - Ecommerce", fmt_num(row.get('properties_ecommerce'), 0))
with d3:
    st.metric("Properties - EMS", fmt_num(row.get('properties_ems'), 0))
with d4:
    st.metric("Properties - Services", fmt_num(row.get('properties_services'), 0))

d5, d6, d7, d8 = st.columns(4)
with d5:
    st.metric("Time to Value (days)", fmt_num(row.get('time_to_value_days'), 0))
with d6:
    st.metric("TTV excl. Blocked", fmt_num(row.get('time_to_value_excl_blocked'), 0))
with d7:
    st.metric("Indicative EV", fmt_gbp(row.get('indicative_ev')))
with d8:
    st.metric("Implementation Backlog", fmt_num(row.get('implementation_backlog'), 0))


# ============================================================
# SECTION E: Capital Efficiency / Cash / NWC
# ============================================================
st.markdown('<div class="kpi-section-title">E. Capital Efficiency / Cash / NWC</div>', unsafe_allow_html=True)

e1, e2, e3, e4 = st.columns(4)
with e1:
    st.metric("Cash Balance", fmt_gbp(row.get('cash_balance')),
              delta_pct(row.get('cash_balance'), row.get('cash_balance_budget')))
with e2:
    st.metric("Cash Burn (Monthly)", fmt_gbp(row.get('cash_burn_monthly')))
with e3:
    st.metric("Cash Runway", fmt_months(row.get('cash_runway_months')))
with e4:
    st.metric("NWC", fmt_gbp(row.get('net_working_capital')),
              delta_pct(row.get('net_working_capital'), row.get('nwc_budget')))

e5, e6, e7, e8 = st.columns(4)
with e5:
    st.metric("Free Cash Conv (Month)", fmt_pct(row.get('free_cash_conversion_month')))
with e6:
    st.metric("Free Cash Conv (YTD)", fmt_pct(row.get('free_cash_conversion_ytd')))
with e7:
    st.metric("Free Cash Conv (Budget)", fmt_pct(row.get('free_cash_conversion_budget')))
with e8:
    st.metric("Cash vs Budget", fmt_gbp(row.get('cash_balance_budget')))


# Cash bridge chart
st.markdown("**Cash Bridge**")
cash_labels = ['Opening Cash', 'EBITDA', 'Capex', 'NWC Change', 'Closing Cash']
opening_cash = row.get('cash_balance_prior_month', 0) or 0
ebitda = row.get('ebitda_actual', 0) or 0
capex_val = row.get('capex', 0) or 0
nwc_change = (row.get('net_working_capital', 0) or 0) - (row.get('nwc_prior_month', 0) or 0)
closing_cash = row.get('cash_balance', 0) or 0

# Scale to full £ for cash
fig_cash = go.Figure(go.Waterfall(
    name="Cash", orientation="v",
    measure=['absolute', 'relative', 'relative', 'relative', 'total'],
    x=cash_labels,
    y=[opening_cash, ebitda * 1000, capex_val * 1000, nwc_change, closing_cash],
    connector={"line": {"color": "#e2e8f0"}},
    increasing={"marker": {"color": "#22c55e"}},
    decreasing={"marker": {"color": "#ef4444"}},
    totals={"marker": {"color": "#0f172a"}},
    textposition="outside",
    text=[fmt_gbp(v) for v in [opening_cash, ebitda * 1000, capex_val * 1000, nwc_change, closing_cash]]
))
fig_cash.update_layout(
    height=350, margin=dict(l=40, r=20, t=20, b=40),
    plot_bgcolor='white', font=dict(family='Inter'),
    showlegend=False, yaxis_title='£'
)
st.plotly_chart(fig_cash, use_container_width=True)


# ============================================================
# PEOPLE SECTION
# ============================================================
st.markdown('<div class="kpi-section-title">People / Headcount</div>', unsafe_allow_html=True)

p1, p2, p3, p4 = st.columns(4)
with p1:
    st.metric("Total Headcount", fmt_num(row.get('total_headcount'), 1),
              delta_pct(row.get('total_headcount'), row.get('headcount_budget')))
with p2:
    st.metric("Gross Payroll", fmt_gbp_k(row.get('gross_payroll') / 1000 if pd.notna(row.get('gross_payroll')) else None),
              delta_pct(row.get('gross_payroll'), row.get('gross_payroll_budget')))
with p3:
    st.metric("Revenue per Employee", fmt_gbp_k(row.get('revenue_per_employee') / 1000 if pd.notna(row.get('revenue_per_employee')) else None))
with p4:
    st.metric("Payroll % Revenue", fmt_pct(row.get('payroll_pct_revenue')))

p5, p6, p7, p8 = st.columns(4)
with p5:
    st.metric("HC - Ecommerce", fmt_num(row.get('headcount_ecommerce'), 1))
with p6:
    st.metric("HC - EMS", fmt_num(row.get('headcount_ems'), 1))
with p7:
    st.metric("HC - Services", fmt_num(row.get('headcount_services'), 1))
with p8:
    st.metric("HC - Central", fmt_num(row.get('headcount_central'), 1))


# ============================================================
# YTD COMPARISON TABLE
# ============================================================
st.markdown('<div class="kpi-section-title">YTD Summary — Actual vs Budget vs Prior Year</div>', unsafe_allow_html=True)

ytd_data = {
    'Metric': [
        'Revenue (£k)', 'Tech MRR (£)', 'Services MRR (£)',
        'Tech Gross Margin %', 'EBITDA (£k)', 'EBITDA Margin %',
        'Cash Balance (£)', 'Headcount'
    ],
    'YTD Actual': [
        row.get('revenue_total_ytd_actual'),
        row.get('tech_mrr_ytd_actual'),
        row.get('services_mrr_ytd_actual'),
        row.get('tech_gross_margin_ytd_pct'),
        row.get('ebitda_ytd_actual'),
        row.get('ebitda_margin_ytd_pct'),
        row.get('cash_balance'),
        row.get('total_headcount')
    ],
    'YTD Budget': [
        row.get('revenue_total_ytd_budget'),
        row.get('tech_mrr_ytd_budget'),
        row.get('services_mrr_ytd_budget'),
        None,
        row.get('ebitda_ytd_budget'),
        None,
        row.get('cash_balance_budget'),
        row.get('headcount_budget')
    ],
    'YTD Prior Year': [
        row.get('revenue_total_ytd_prior_year'),
        row.get('tech_mrr_ytd_prior_year'),
        row.get('services_mrr_ytd_prior_year'),
        None,
        None,
        None,
        None,
        None
    ]
}

ytd_df = pd.DataFrame(ytd_data)

# Format nicely
def format_ytd_val(val):
    if pd.isna(val) or val is None:
        return "—"
    return f"{val:,.1f}"

for col in ['YTD Actual', 'YTD Budget', 'YTD Prior Year']:
    ytd_df[col] = ytd_df[col].apply(format_ytd_val)

st.dataframe(ytd_df, use_container_width=True, hide_index=True)


# ============================================================
# FOOTER
# ============================================================
st.markdown("---")
st.caption(f"Averroes Capital | Portfolio Intelligence Platform | Phase 1 KPI Dashboard | Generated {datetime.now().strftime('%d %b %Y %H:%M')}")

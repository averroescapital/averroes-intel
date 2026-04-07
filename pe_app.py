import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime
import os
import io

# PDF generation
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.enums import TA_LEFT, TA_CENTER

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

# ============================================================
# EXEC SUMMARY — KPI TABLE BUILDER
# ============================================================
def build_kpi_table(row):
    """Returns list of dicts: {kpi, actual, budget, var_pct, prior_year, unit}"""
    def safe(col): return row.get(col)
    def var(a, b):
        if pd.isna(a) or pd.isna(b) or b == 0: return None
        return ((a - b) / abs(b)) * 100

    table = [
        {"kpi": "Total ARR",            "actual": safe('total_arr'),               "budget": None,                              "prior_year": None,                             "unit": "gbp"},
        {"kpi": "Monthly Revenue",       "actual": safe('revenue_total_actual'),    "budget": safe('revenue_total_budget'),      "prior_year": safe('revenue_total_prior_year'), "unit": "gbp_k"},
        {"kpi": "Tech MRR (Month)",      "actual": safe('tech_mrr_actual'),         "budget": safe('tech_mrr_budget'),           "prior_year": safe('tech_mrr_prior_year'),      "unit": "gbp"},
        {"kpi": "Services MRR (Month)",  "actual": safe('services_mrr_actual'),     "budget": safe('services_mrr_budget'),       "prior_year": safe('services_mrr_prior_year'),  "unit": "gbp"},
        {"kpi": "EBITDA",                "actual": safe('ebitda_actual'),           "budget": safe('ebitda_budget'),             "prior_year": safe('ebitda_prior_year'),         "unit": "gbp_k"},
        {"kpi": "EBITDA Margin %",       "actual": safe('ebitda_margin_pct'),       "budget": safe('ebitda_margin_budget_pct'),  "prior_year": safe('ebitda_margin_prior_pct'),   "unit": "pct"},
        {"kpi": "Tech Gross Margin %",   "actual": safe('tech_gross_margin_pct'),   "budget": safe('tech_gross_margin_budget_pct'), "prior_year": safe('tech_gross_margin_prior_pct'), "unit": "pct"},
        {"kpi": "Cash Balance",          "actual": safe('cash_balance'),            "budget": safe('cash_balance_budget'),       "prior_year": None,                             "unit": "gbp"},
        {"kpi": "Cash Runway (months)",  "actual": safe('cash_runway_months'),      "budget": None,                              "prior_year": None,                             "unit": "num"},
        {"kpi": "Rule of 40",            "actual": (safe('rule_of_40') or 0) * 100,"budget": None,                              "prior_year": None,                             "unit": "pct"},
        {"kpi": "Revenue YoY Growth %",  "actual": safe('revenue_yoy_growth_pct'),  "budget": None,                              "prior_year": None,                             "unit": "pct"},
        {"kpi": "ARPC",                  "actual": safe('arpc_actual'),             "budget": safe('arpc_budget'),               "prior_year": None,                             "unit": "gbp"},
        {"kpi": "Revenue Churn %",       "actual": (safe('revenue_churn_pct') or 0)*100, "budget": None,                         "prior_year": None,                             "unit": "pct"},
        {"kpi": "Headcount",             "actual": safe('total_headcount'),         "budget": safe('headcount_budget'),          "prior_year": None,                             "unit": "num"},
    ]
    for r in table:
        r["var_pct"] = var(r["actual"], r["budget"])
    return table

def fmt_kpi_val(val, unit):
    if val is None or (isinstance(val, float) and pd.isna(val)): return "—"
    if unit == "gbp":     return fmt_gbp(val)
    if unit == "gbp_k":   return fmt_gbp_k(val)
    if unit == "pct":     return fmt_pct(val)
    if unit == "num":     return fmt_num(val, 1)
    return str(round(val, 1))

# ============================================================
# EXEC SUMMARY — GEMINI COMMENTARY
# ============================================================
def generate_commentary(portco, period_str, kpi_table, alerts):
    """Call Gemini to generate PE-style exec commentary. Cached in session_state."""
    cache_key = f"exec_commentary_{portco}_{period_str}"
    if cache_key in st.session_state:
        return st.session_state[cache_key]

    try:
        import google.generativeai as genai
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-2.5-flash')

        kpi_lines = "\n".join([
            f"  - {r['kpi']}: Actual={fmt_kpi_val(r['actual'], r['unit'])}"
            + (f", Budget={fmt_kpi_val(r['budget'], r['unit'])}" if r['budget'] is not None else "")
            + (f", Var={r['var_pct']:+.1f}%" if r['var_pct'] is not None else "")
            + (f", PY={fmt_kpi_val(r['prior_year'], r['unit'])}" if r['prior_year'] is not None else "")
            for r in kpi_table
        ])

        alert_lines = "\n".join([f"  - [{a['level']}] {a['metric']}: {a['message']}" for a in alerts]) if alerts else "  None"

        prompt = f"""You are a data analyst at Averroes Capital, a Private Equity firm.
Write a concise executive briefing for the Managing Director / GP.

Portfolio Company: {portco}
Reporting Period: {period_str}

Key Performance Indicators:
{kpi_lines}

Active Alerts:
{alert_lines}

Instructions:
- Write exactly 3 short paragraphs (4-6 sentences each)
- Paragraph 1: Overall trading momentum and revenue performance
- Paragraph 2: Profitability, margins, and cash position
- Paragraph 3: Key risks, alerts, and one clear bottom-line recommendation
- Tone: Direct, data-driven, PE-style. No fluff. Reference specific numbers.
- Do not use bullet points. Prose only.
- Do not use headers or bold.
"""
        resp = model.generate_content(prompt)
        commentary = resp.text.strip()
        st.session_state[cache_key] = commentary
        return commentary

    except Exception as e:
        fallback = f"Executive commentary unavailable: {e}"
        st.session_state[cache_key] = fallback
        return fallback


# ============================================================
# EXEC SUMMARY — PDF GENERATOR
# ============================================================
def generate_exec_pdf(portco, period_str, commentary, kpi_table, alerts):
    """Generate a PDF exec summary using reportlab. Returns bytes."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=20*mm, rightMargin=20*mm,
                            topMargin=20*mm, bottomMargin=20*mm)

    styles = getSampleStyleSheet()
    navy = colors.HexColor('#0f172a')
    blue = colors.HexColor('#0ea5e9')
    light_grey = colors.HexColor('#f8fafc')
    mid_grey = colors.HexColor('#64748b')
    red = colors.HexColor('#dc2626')
    amber = colors.HexColor('#d97706')
    green = colors.HexColor('#16a34a')

    title_style = ParagraphStyle('Title', fontName='Helvetica-Bold', fontSize=18, textColor=navy, spaceAfter=2*mm)
    sub_style   = ParagraphStyle('Sub',   fontName='Helvetica',      fontSize=9,  textColor=mid_grey, spaceAfter=6*mm)
    body_style  = ParagraphStyle('Body',  fontName='Helvetica',      fontSize=9,  textColor=navy, leading=14, spaceAfter=4*mm)
    section_style = ParagraphStyle('Sec', fontName='Helvetica-Bold', fontSize=10, textColor=navy, spaceBefore=6*mm, spaceAfter=3*mm)
    alert_style = ParagraphStyle('Alert', fontName='Helvetica',      fontSize=8,  textColor=navy, leading=12)

    story = []

    # Header
    story.append(Paragraph("Averroes Capital", ParagraphStyle('Brand', fontName='Helvetica-Bold', fontSize=10, textColor=blue)))
    story.append(Paragraph(f"{portco} — Executive Summary", title_style))
    story.append(Paragraph(f"Reporting Period: {period_str} | Confidential | Generated {datetime.now().strftime('%d %b %Y')}", sub_style))
    story.append(HRFlowable(width="100%", thickness=1, color=blue, spaceAfter=5*mm))

    # Commentary
    story.append(Paragraph("Executive Commentary", section_style))
    for para in commentary.split('\n\n'):
        if para.strip():
            story.append(Paragraph(para.strip(), body_style))

    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#e2e8f0'), spaceAfter=4*mm))

    # KPI Table
    story.append(Paragraph("Key Performance Indicators", section_style))
    table_data = [["KPI", "Actual", "Budget", "Var %", "Prior Year"]]
    for r in kpi_table:
        var_str = f"{r['var_pct']:+.1f}%" if r['var_pct'] is not None else "—"
        table_data.append([
            r['kpi'],
            fmt_kpi_val(r['actual'], r['unit']),
            fmt_kpi_val(r['budget'], r['unit']),
            var_str,
            fmt_kpi_val(r['prior_year'], r['unit']),
        ])

    col_widths = [55*mm, 30*mm, 30*mm, 22*mm, 30*mm]
    t = Table(table_data, colWidths=col_widths, repeatRows=1)

    table_style = TableStyle([
        ('BACKGROUND',  (0,0), (-1,0),  navy),
        ('TEXTCOLOR',   (0,0), (-1,0),  colors.white),
        ('FONTNAME',    (0,0), (-1,0),  'Helvetica-Bold'),
        ('FONTSIZE',    (0,0), (-1,0),  8),
        ('ALIGN',       (1,0), (-1,-1), 'RIGHT'),
        ('ALIGN',       (0,0), (0,-1),  'LEFT'),
        ('FONTNAME',    (0,1), (-1,-1), 'Helvetica'),
        ('FONTSIZE',    (0,1), (-1,-1), 8),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, light_grey]),
        ('GRID',        (0,0), (-1,-1), 0.3, colors.HexColor('#e2e8f0')),
        ('TOPPADDING',  (0,0), (-1,-1), 3),
        ('BOTTOMPADDING',(0,0),(-1,-1), 3),
    ])
    # Colour var% column: red if negative, green if positive
    for i, r in enumerate(kpi_table, start=1):
        if r['var_pct'] is not None:
            col = green if r['var_pct'] >= 0 else red
            table_style.add('TEXTCOLOR', (3, i), (3, i), col)
            table_style.add('FONTNAME',  (3, i), (3, i), 'Helvetica-Bold')
    t.setStyle(table_style)
    story.append(t)

    # Alerts
    if alerts:
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#e2e8f0'), spaceBefore=6*mm, spaceAfter=4*mm))
        story.append(Paragraph("Critical Portfolio Alerts", section_style))
        for a in alerts:
            colour = red if "CRITICAL" in a['level'] else amber
            story.append(Paragraph(
                f"<font color='#{colour.hexval()[2:]}'>■</font> <b>{a['metric']}</b>: {a['message']} <i>Action: {a['action']}</i>",
                alert_style
            ))
            story.append(Spacer(1, 2*mm))
    else:
        story.append(Spacer(1, 4*mm))
        story.append(Paragraph("No critical alerts this period.", body_style))

    # Footer
    story.append(Spacer(1, 6*mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#e2e8f0')))
    story.append(Paragraph("Averroes Capital | Portfolio Intelligence Platform | Confidential",
                            ParagraphStyle('Footer', fontName='Helvetica', fontSize=7, textColor=mid_grey, alignment=TA_CENTER, spaceBefore=3*mm)))

    doc.build(story)
    buf.seek(0)
    return buf.getvalue()


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
# DATA LOADING
# ============================================================
@st.cache_data(ttl=600)
def load_data():
    """Load from BigQuery, fall back to local CSV."""
    try:
        from google.cloud import bigquery
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        else:
            client = bigquery.Client(project=PROJECT_ID)

        query = f"SELECT * FROM `{PROJECT_ID}.gold.kpi_monthly` ORDER BY period ASC"
        df_bq = client.query(query).to_dataframe()

        if not df_bq.empty:
            df_bq['period'] = pd.to_datetime(df_bq['period'])
            return df_bq, "connected"
    except Exception as e:
        print(f"BigQuery Connection Issue: {e}")

    # Fallback to local CSV
    try:
        csv_path = os.path.join(os.path.dirname(__file__), "gold_phase1_data.csv")
        df = pd.read_csv(csv_path)
        df['period'] = pd.to_datetime(df['period'])
        return df, "csv_fallback"
    except Exception as e2:
        print(f"CSV fallback failed: {e2}")
        return pd.DataFrame(), "error"


df_raw, data_status = load_data()

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
status_label = "🟢 BigQuery Live" if data_status == "connected" else "🟡 CSV Fallback"
st.sidebar.caption(f"Data: {status_label}")
st.sidebar.caption(f"Period: {row.get('fy', '')} {row.get('fy_quarter', '')} (Month {int(row.get('fy_month_num', 0))})")

# ============================================================
# HEADER
# ============================================================
display_name = selected_portco.replace("portco-", "").title()
period_str = pd.Timestamp(selected_period).strftime('%B %Y')

st.markdown(f'<div class="main-header">{display_name} — Monthly Board Pack</div>', unsafe_allow_html=True)
st.markdown(f'<div class="sub-header">{row.get("fy", "")} {row.get("fy_quarter", "")} | {period_str} | Currency: GBP</div>', unsafe_allow_html=True)
# ============================================================
# EXEC SUMMARY SECTION
# ============================================================
kpi_table   = build_kpi_table(row)
alerts      = get_anomalies(row)

with st.spinner("Generating executive summary…"):
    commentary = generate_commentary(display_name, period_str, kpi_table, alerts)

pdf_bytes = generate_exec_pdf(display_name, period_str, commentary, kpi_table, alerts)

st.markdown('<div class="kpi-section-title">📋 Executive Summary</div>', unsafe_allow_html=True)

# Download button (top right)
dl_col, _ = st.columns([1, 4])
with dl_col:
    st.download_button(
        label="⬇ Download PDF",
        data=pdf_bytes,
        file_name=f"exec_summary_{display_name}_{period_str.replace(' ', '_')}.pdf",
        mime="application/pdf",
        use_container_width=True
    )

# Commentary
st.markdown(
    f'<div style="background:#f8fafc;border-left:4px solid #0ea5e9;padding:18px 22px;border-radius:4px;font-size:0.92rem;line-height:1.7;color:#0f172a;margin-bottom:16px;">{commentary.replace(chr(10), "<br><br>")}</div>',
    unsafe_allow_html=True
)

# KPI snapshot table
st.markdown("**Key KPI Snapshot**")
kpi_df_data = []
for r in kpi_table:
    var_str = f"{r['var_pct']:+.1f}%" if r['var_pct'] is not None else "—"
    kpi_df_data.append({
        "KPI":        r["kpi"],
        "Actual":     fmt_kpi_val(r["actual"],     r["unit"]),
        "Budget":     fmt_kpi_val(r["budget"],     r["unit"]),
        "Var %":      var_str,
        "Prior Year": fmt_kpi_val(r["prior_year"], r["unit"]),
    })
kpi_snapshot_df = pd.DataFrame(kpi_df_data)
st.dataframe(kpi_snapshot_df, use_container_width=True, hide_index=True)

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
    text=[fmt_gbp_k(v / 1000) if v != 0 else "—" for v in wf_vals],
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

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Averroes Portfolio Portal",
    layout="wide",
    page_icon="https://averroescapital.com/favicon.ico",
    initial_sidebar_state="expanded"
)

# ============================================================
# DESIGN SYSTEM - Clean White Executive Theme
# ============================================================
COLORS = {
    "primary": "#0f172a",
    "accent_blue": "#0ea5e9",
    "accent_purple": "#6366f1",
    "accent_green": "#059669",
    "accent_teal": "#10b981",
    "accent_red": "#ef4444",
    "accent_amber": "#f59e0b",
    "muted": "#64748b",
    "border": "#e2e8f0",
    "bg": "#ffffff",
    "bg_subtle": "#f8fafc",
}

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color=COLORS["primary"]),
    margin=dict(l=20, r=20, t=50, b=20),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis=dict(gridcolor="#f1f5f9", showline=True, linecolor=COLORS["border"]),
    yaxis=dict(gridcolor="#f1f5f9", showline=True, linecolor=COLORS["border"]),
)

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [data-testid="stAppViewContainer"] {
        background-color: #ffffff !important;
        font-family: 'Inter', sans-serif;
        color: #1e293b;
    }
    [data-testid="stSidebar"] {
        background-color: #f8fafc;
        border-right: 1px solid #e2e8f0;
    }
    div[data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        padding: 16px 20px !important;
        border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }
    [data-testid="stMetricValue"] {
        color: #0f172a !important;
        font-size: 1.6rem !important;
        font-weight: 600 !important;
    }
    [data-testid="stMetricDelta"] { font-size: 0.8rem !important; }
    [data-testid="stMetricLabel"] {
        font-size: 0.7rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        color: #64748b !important;
    }
    h1, h2, h3, h4 { color: #0f172a; font-weight: 600; }
    h1 { font-size: 1.75rem !important; }
    .plotly-graph-div { background-color: transparent !important; }
    div[data-testid="stExpander"] {
        border: 1px solid #e2e8f0;
        border-radius: 12px;
    }
    .rag-green { color: #059669; font-weight: 600; }
    .rag-amber { color: #f59e0b; font-weight: 600; }
    .rag-red { color: #ef4444; font-weight: 600; }
    </style>
""", unsafe_allow_html=True)

# ============================================================
# DATA LOADING - BigQuery with Sample Fallback
# ============================================================
PROJECT_ID = "averroes-portfolio-intel"

def generate_sample_data():
    """Generate realistic PE portfolio sample data for testing."""
    np.random.seed(42)
    periods = pd.date_range("2025-04-01", "2026-02-01", freq="MS")
    rows = []
    for p in periods:
        month_idx = (p - periods[0]).days / 30
        base_rev = 420 + month_idx * 8 + np.random.normal(0, 12)
        ecomm = base_rev * 0.45 + np.random.normal(0, 5)
        ems = base_rev * 0.30 + np.random.normal(0, 4)
        services = base_rev * 0.25 + np.random.normal(0, 3)
        direct_costs = base_rev * 0.35 + np.random.normal(0, 4)
        gp = base_rev - direct_costs
        contribution = gp * 0.72 + np.random.normal(0, 3)
        overheads = 85 + month_idx * 1.5 + np.random.normal(0, 5)
        ebitda = contribution - overheads
        cash = 1800 - month_idx * 30 + np.random.normal(0, 40)
        headcount = 52 + int(month_idx * 0.8)
        tech_arr_live = 3200 + month_idx * 85 + np.random.normal(0, 30)
        tech_arr_sold = tech_arr_live * 1.12
        mrr_live = tech_arr_live / 12
        mrr_sold = tech_arr_sold / 12
        budget_rev = base_rev * 1.05
        budget_ebitda = ebitda * 1.08
        rows.append({
            "portco_id": "portco-alpha",
            "period": p,
            # Revenue
            "total_revenue": round(base_rev, 1),
            "total_revenue_budget": round(budget_rev, 1),
            "total_group_revenue": round(base_rev * 1.02, 1),
            "revenue_vs_budget_pct": round((base_rev / budget_rev - 1), 3),
            "revenue_vs_budget_variance": round(base_rev - budget_rev, 1),
            "ecommerce_revenue": round(ecomm, 1),
            "ems_revenue": round(ems, 1),
            "services_revenue": round(services, 1),
            "tech_revenue": round(ecomm + ems, 1),
            "tech1_revenue": round(ecomm * 0.6, 1),
            "onejourney_revenue": round(ecomm * 0.3, 1),
            "gifted_revenue": round(ecomm * 0.1, 1),
            "af_revenue": round(services * 0.5, 1),
            # ARR
            "tech_mrr_live": round(mrr_live, 1),
            "tech_mrr_sold": round(mrr_sold, 1),
            "tech_arr_live": round(tech_arr_live, 1),
            "tech_arr_sold": round(tech_arr_sold, 1),
            "ecommerce_arr_live": round(tech_arr_live * 0.55, 1),
            "ems_arr_live": round(tech_arr_live * 0.45, 1),
            "revenue_per_live_module": round(np.random.uniform(2.8, 3.5), 2),
            # Profitability
            "direct_costs_total": round(direct_costs, 1),
            "gross_profit_total": round(gp, 1),
            "gross_margin_total_pct": round(gp / base_rev * 100, 1),
            "contribution_total": round(contribution, 1),
            "contribution_margin_total_pct": min(round(contribution / base_rev * 100, 1), 100),
            "total_overheads": round(overheads, 1),
            "overhead_ratio": round(overheads / base_rev * 100, 1),
            "adjusted_ebitda": round(ebitda, 1),
            "adjusted_ebitda_budget": round(budget_ebitda, 1),
            "adjusted_ebitda_margin": round(ebitda / base_rev * 100, 1),
            "ebitda_margin_month": round(ebitda / base_rev * 100, 1),
            "tech_gross_margin_month": round((ecomm + ems - direct_costs * 0.6) / (ecomm + ems) * 100, 1),
            "pat": round(ebitda * 0.75, 1),
            "pat_margin": round(ebitda * 0.75 / base_rev * 100, 1),
            # Cash
            "cash_balance": round(cash, 0),
            "cash_burn": round(-30 + np.random.normal(0, 10), 1),
            "cash_runway_months": round(cash / max(abs(ebitda), 1), 1),
            "net_working_capital": round(350 + np.random.normal(0, 20), 1),
            "free_cash_conversion": round(np.random.uniform(0.6, 0.9), 2),
            # Efficiency
            "arpc": round(base_rev * 1000 / max(headcount, 1) * 12 / 300, 0),
            "sm_efficiency": round(np.random.uniform(0.8, 1.4), 2),
            "revenue_per_employee": round(base_rev / headcount, 1),
            "payroll_pct_revenue": round(np.random.uniform(38, 48), 1),
            "rule_of_40_score": round(15 + ebitda / base_rev * 100, 1),
            "revenue_churn_pct": round(np.random.uniform(1.5, 4.5), 1),
            "ytd_revenue_growth": round(10 + month_idx * 0.8, 1),
            "time_to_value_days": round(np.random.uniform(25, 45), 0),
            "indicative_ev": round(tech_arr_live * 8.5 * 1000, 0),
            # People
            "total_headcount": headcount,
            "headcount_budget": headcount + 3,
            "headcount_variance": -3,
            "ecommerce_headcount": int(headcount * 0.35),
            "ems_headcount": int(headcount * 0.25),
            "services_headcount": int(headcount * 0.20),
            "central_headcount": int(headcount * 0.20),
            # Modules
            "modules_sold_pre_vouchers": 180 + int(month_idx * 5),
            "modules_live_pre_vouchers": 160 + int(month_idx * 4),
            "modules_churn": int(np.random.uniform(1, 4)),
            "sold_per_month_pre_vouchers": int(np.random.uniform(3, 8)),
            "live_per_month_pre_vouchers": int(np.random.uniform(2, 6)),
            "properties_sold": 95 + int(month_idx * 2),
            "properties_live": 88 + int(month_idx * 1.8),
            # Covenants
            "gl_revenue_actual_cumulative": round(sum([420 + i * 8 for i in range(int(month_idx) + 1)]), 1),
            "gl_revenue_covenant_cumulative": round(sum([400 + i * 7.5 for i in range(int(month_idx) + 1)]), 1),
            "gl_revenue_headroom_pct": round(np.random.uniform(3, 12), 1),
            "gl_revenue_breach": False,
            "gl_ebitda_actual_cumulative": round(sum([(420 + i * 8) * 0.12 for i in range(int(month_idx) + 1)]), 1),
            "gl_ebitda_covenant_cumulative": round(sum([(400 + i * 7.5) * 0.10 for i in range(int(month_idx) + 1)]), 1),
            "gl_ebitda_headroom_pct": round(np.random.uniform(5, 18), 1),
            "gl_ebitda_breach": False,
            "averroes_revenue_rag": "green" if month_idx < 8 else "amber",
            "averroes_ebitda_rag": "green",
            # LTM
            "ltm_revenue_total": round(base_rev * 12, 1),
            "ltm_ebitda": round(ebitda * 12, 1),
            "run_rate_revenue_total": round(base_rev * 12, 1),
            "run_rate_ebitda": round(ebitda * 12, 1),
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def load_data():
    """Try BigQuery first, fall back to sample data."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        query = f"SELECT * FROM `{PROJECT_ID}.gold.kpi_monthly` ORDER BY period ASC"
        df = client.query(query).to_dataframe()
        if not df.empty:
            return df, "bigquery"
    except Exception:
        pass
    return generate_sample_data(), "sample"


df_raw, data_source = load_data()

# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.image("https://averroescapital.com/wp-content/uploads/2021/04/averroes-capital-logo.png", width=180)
st.sidebar.markdown("<br>", unsafe_allow_html=True)

VIEWS = [
    "Executive Summary",
    "Revenue & ARR Deep Dive",
    "Profitability & Contribution",
    "Cash & Balance Sheet",
    "People & Efficiency",
    "Product Metrics",
    "Covenants & Risk",
]
view = st.sidebar.radio("Navigation", VIEWS, label_visibility="collapsed")

if df_raw.empty:
    st.warning("No data found. Please check ingestion pipeline.")
    st.stop()

portcos = df_raw["portco_id"].unique()
selected_portco = st.sidebar.selectbox("Portfolio Company", portcos)
df = df_raw[df_raw["portco_id"] == selected_portco].sort_values("period").copy()

if data_source == "sample":
    st.sidebar.caption("Using sample data (BigQuery unavailable)")
else:
    st.sidebar.caption("Connected to BigQuery")

st.sidebar.markdown("---")
st.sidebar.caption(f"Last refresh: {datetime.now().strftime('%H:%M %d %b %Y')}")

# ============================================================
# HELPERS
# ============================================================
df_actuals = df[df["total_revenue"] > 0].sort_values("period")
if df_actuals.empty:
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest
else:
    latest = df_actuals.iloc[-1]
    prev_candidates = df_actuals.iloc[:-1]
    prev = prev_candidates.iloc[-1] if len(prev_candidates) > 0 else latest

latest_period = latest["period"]
if isinstance(latest_period, pd.Timestamp):
    period_label = latest_period.strftime("%b %Y")
else:
    period_label = str(latest_period)


def safe(key, row=None):
    r = row if row is not None else latest
    v = r.get(key, 0) if isinstance(r, dict) else r[key] if key in r.index else 0
    return 0 if pd.isna(v) else v


def mom(current, previous):
    if previous and not pd.isna(previous) and previous != 0:
        return ((current / previous) - 1) * 100
    return 0


def rag_dot(status):
    colors = {"green": "#059669", "amber": "#f59e0b", "red": "#ef4444"}
    c = colors.get(str(status).lower(), "#94a3b8")
    return f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:{c};margin-right:6px"></span>'


def make_chart(fig, height=400):
    fig.update_layout(**CHART_LAYOUT, height=height)
    return fig


# ============================================================
# HEADER
# ============================================================
st.title(view)
st.markdown(f"**{selected_portco.replace('-', ' ').title()}** | {period_label} Monthly Performance Report")
st.markdown("---")

# ============================================================
# VIEW 1: EXECUTIVE SUMMARY
# ============================================================
if view == "Executive Summary":
    # KPI Cards Row 1
    c1, c2, c3, c4, c5 = st.columns(5)

    rev = safe("total_revenue")
    rev_b = safe("total_revenue_budget")
    rev_delta = f"{mom(rev, safe('total_revenue', prev)):+.1f}% MoM"
    if rev_b > 0:
        rev_delta += f" | {((rev/rev_b)-1)*100:+.1f}% vs Bud"
    c1.metric("Total Revenue (GBP k)", f"£{rev:,.0f}", rev_delta)

    eb = safe("adjusted_ebitda")
    c2.metric("Adj. EBITDA (GBP k)", f"£{eb:,.1f}", f"{mom(eb, safe('adjusted_ebitda', prev)):+.1f}% MoM")

    cash = safe("cash_balance")
    cash_chg = cash - safe("cash_balance", prev)
    c3.metric("Cash Balance (GBP k)", f"£{cash:,.0f}", f"£{cash_chg:+,.0f}k {'Burn' if cash_chg < 0 else 'Gen'}")

    arr = safe("tech_arr_live")
    c4.metric("Tech ARR Live (GBP k)", f"£{arr:,.0f}", f"{mom(arr, safe('tech_arr_live', prev)):+.1f}% MoM")

    hc = safe("total_headcount")
    c5.metric("Headcount", f"{hc:.0f}", f"{hc - safe('headcount_budget'):.0f} vs Budget")

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row
    t1, t2 = st.columns(2)
    with t1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["period"], y=df["total_revenue"], name="Actual",
                                 line=dict(color=COLORS["accent_blue"], width=3)))
        if "total_revenue_budget" in df.columns:
            fig.add_trace(go.Scatter(x=df["period"], y=df["total_revenue_budget"], name="Budget",
                                     line=dict(dash="dot", color=COLORS["muted"], width=1.5)))
        fig = make_chart(fig)
        fig.update_layout(title="Revenue vs Budget (GBP k)")
        st.plotly_chart(fig, use_container_width=True)

    with t2:
        colors = [COLORS["accent_green"] if v >= 0 else COLORS["accent_red"] for v in df["adjusted_ebitda"]]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df["period"], y=df["adjusted_ebitda"], marker_color=colors, name="EBITDA"))
        fig = make_chart(fig)
        fig.update_layout(title="Monthly Adj. EBITDA (GBP k)")
        st.plotly_chart(fig, use_container_width=True)

    # Quick Summary Table
    with st.expander("Key Metrics Snapshot", expanded=False):
        metrics = {
            "Gross Margin": f"{safe('gross_margin_total_pct'):.1f}%",
            "Contribution Margin": f"{safe('contribution_margin_total_pct'):.1f}%",
            "EBITDA Margin": f"{safe('ebitda_margin_month'):.1f}%",
            "Rule of 40": f"{safe('rule_of_40_score'):.1f}",
            "Cash Runway": f"{safe('cash_runway_months'):.0f} months",
            "Revenue per Employee": f"£{safe('revenue_per_employee'):.1f}k",
            "S&M Efficiency": f"{safe('sm_efficiency'):.2f}x",
            "Revenue Churn": f"{safe('revenue_churn_pct'):.1f}%",
        }
        cols = st.columns(4)
        for i, (k, v) in enumerate(metrics.items()):
            cols[i % 4].metric(k, v)


# ============================================================
# VIEW 2: REVENUE & ARR DEEP DIVE
# ============================================================
elif view == "Revenue & ARR Deep Dive":
    # Top-line metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Revenue", f"£{safe('total_revenue'):,.0f}k")
    c2.metric("Tech Revenue", f"£{safe('tech_revenue'):,.0f}k")
    c3.metric("Tech ARR Live", f"£{safe('tech_arr_live'):,.0f}k")
    c4.metric("YTD Revenue Growth", f"{safe('ytd_revenue_growth'):.1f}%")
    st.markdown("---")

    # Segment Mix
    t1, t2 = st.columns(2)
    with t1:
        seg_cols = ["ecommerce_revenue", "ems_revenue", "services_revenue"]
        available = [c for c in seg_cols if c in df.columns]
        if available:
            fig = go.Figure()
            color_map = {"ecommerce_revenue": COLORS["accent_blue"],
                         "ems_revenue": COLORS["accent_purple"],
                         "services_revenue": COLORS["accent_teal"]}
            for col in available:
                fig.add_trace(go.Scatter(
                    x=df["period"], y=df[col], name=col.replace("_revenue", "").title(),
                    stackgroup="one", line=dict(width=0.5),
                    fillcolor=color_map.get(col, COLORS["muted"])
                ))
            fig = make_chart(fig)
            fig.update_layout(title="Revenue by Segment (GBP k)")
            st.plotly_chart(fig, use_container_width=True)

    with t2:
        # ARR Trend
        arr_cols = [c for c in ["tech_arr_live", "tech_arr_sold"] if c in df.columns]
        if arr_cols:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["period"], y=df["tech_arr_live"], name="ARR Live",
                                     line=dict(color=COLORS["accent_blue"], width=3)))
            fig.add_trace(go.Scatter(x=df["period"], y=df["tech_arr_sold"], name="ARR Sold",
                                     line=dict(color=COLORS["accent_purple"], width=2, dash="dash")))
            fig = make_chart(fig)
            fig.update_layout(title="Tech ARR: Live vs Sold (GBP k)")
            st.plotly_chart(fig, use_container_width=True)

    # Product Detail
    st.subheader("Product-Level Revenue")
    prod_cols = [c for c in ["tech1_revenue", "onejourney_revenue", "gifted_revenue"] if c in df.columns]
    if prod_cols:
        fig = go.Figure()
        for col in prod_cols:
            fig.add_trace(go.Scatter(x=df["period"], y=df[col],
                                     name=col.replace("_revenue", "").replace("_", " ").title(),
                                     mode="lines+markers"))
        fig = make_chart(fig, height=350)
        fig.update_layout(title="Product Revenue Breakdown (GBP k)")
        st.plotly_chart(fig, use_container_width=True)

    # Revenue per Module
    if "revenue_per_live_module" in df.columns:
        st.metric("Revenue per Live Module (GBP k)", f"£{safe('revenue_per_live_module'):.2f}k")


# ============================================================
# VIEW 3: PROFITABILITY & CONTRIBUTION
# ============================================================
elif view == "Profitability & Contribution":
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gross Margin", f"{safe('gross_margin_total_pct'):.1f}%")
    c2.metric("Contribution Margin", f"{safe('contribution_margin_total_pct'):.1f}%")
    c3.metric("EBITDA Margin", f"{safe('ebitda_margin_month'):.1f}%")
    c4.metric("PAT Margin", f"{safe('pat_margin'):.1f}%")
    st.markdown("---")

    # Margin Waterfall
    t1, t2 = st.columns(2)
    with t1:
        df["cont_margin"] = (df["contribution_total"] / df["total_revenue"].replace(0, 1) * 100).clip(0, 100)
        df["gm_pct"] = (df["gross_profit_total"] / df["total_revenue"].replace(0, 1) * 100).clip(0, 100)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["period"], y=df["gm_pct"], name="Gross Margin %",
                                 line=dict(color=COLORS["accent_blue"], width=3)))
        fig.add_trace(go.Scatter(x=df["period"], y=df["cont_margin"], name="Contribution Margin %",
                                 line=dict(color=COLORS["accent_green"], width=3)))
        fig.add_trace(go.Scatter(x=df["period"], y=df.get("ebitda_margin_month", df["adjusted_ebitda"] / df["total_revenue"].replace(0,1) * 100),
                                 name="EBITDA Margin %",
                                 line=dict(color=COLORS["accent_purple"], width=3)))
        fig = make_chart(fig, height=450)
        fig.update_layout(title="Margin Stack Trend (%)", yaxis_range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)

    with t2:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df["period"], y=df["gross_profit_total"], name="Gross Profit",
                             marker_color=COLORS["accent_blue"]))
        fig.add_trace(go.Bar(x=df["period"], y=df["contribution_total"], name="Contribution",
                             marker_color=COLORS["accent_green"]))
        fig.add_trace(go.Bar(x=df["period"], y=df["adjusted_ebitda"], name="EBITDA",
                             marker_color=COLORS["accent_purple"]))
        fig = make_chart(fig, height=450)
        fig.update_layout(title="Profitability Waterfall (GBP k)", barmode="group")
        st.plotly_chart(fig, use_container_width=True)

    # Overhead Analysis
    st.subheader("Cost Structure")
    o1, o2, o3 = st.columns(3)
    o1.metric("Direct Costs", f"£{safe('direct_costs_total'):,.0f}k")
    o2.metric("Total Overheads", f"£{safe('total_overheads'):,.0f}k")
    o3.metric("Overhead Ratio", f"{safe('overhead_ratio'):.1f}%")

    if "total_overheads" in df.columns:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["period"], y=df["total_overheads"], name="Overheads",
                                 fill="tozeroy", line=dict(color=COLORS["accent_red"], width=2),
                                 fillcolor="rgba(239, 68, 68, 0.05)"))
        fig = make_chart(fig, height=300)
        fig.update_layout(title="Monthly Overheads (GBP k)")
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
# VIEW 4: CASH & BALANCE SHEET
# ============================================================
elif view == "Cash & Balance Sheet":
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cash Balance", f"£{safe('cash_balance'):,.0f}k")
    burn = safe("cash_burn")
    c2.metric("Monthly Burn/Gen", f"£{burn:+,.0f}k", "Burn" if burn < 0 else "Generation")
    c3.metric("Runway", f"{safe('cash_runway_months'):.0f} months")
    c4.metric("Free Cash Conversion", f"{safe('free_cash_conversion'):.0%}")
    st.markdown("---")

    # Cash Trend
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["period"], y=df["cash_balance"], name="Cash Balance",
                             fill="tozeroy", line=dict(color=COLORS["accent_blue"], width=3),
                             fillcolor="rgba(14, 165, 233, 0.08)"))
    # Runway threshold line
    if safe("adjusted_ebitda") < 0:
        danger_line = abs(safe("adjusted_ebitda")) * 6  # 6 months runway
        fig.add_hline(y=danger_line, line_dash="dash", line_color=COLORS["accent_red"],
                      annotation_text="6-Month Runway Threshold")
    fig = make_chart(fig, height=450)
    fig.update_layout(title="Cash Balance Evolution (GBP k)")
    st.plotly_chart(fig, use_container_width=True)

    t1, t2 = st.columns(2)
    with t1:
        if "net_working_capital" in df.columns:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["period"], y=df["net_working_capital"],
                                 marker_color=COLORS["accent_teal"], name="NWC"))
            fig = make_chart(fig, height=350)
            fig.update_layout(title="Net Working Capital (GBP k)")
            st.plotly_chart(fig, use_container_width=True)

    with t2:
        if "free_cash_conversion" in df.columns:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["period"], y=df["free_cash_conversion"],
                                     mode="lines+markers", line=dict(color=COLORS["accent_green"], width=2),
                                     name="FCF Conversion"))
            fig.add_hline(y=0.7, line_dash="dash", line_color=COLORS["muted"],
                          annotation_text="Target: 70%")
            fig = make_chart(fig, height=350)
            fig.update_layout(title="Free Cash Conversion (x)")
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# VIEW 5: PEOPLE & EFFICIENCY
# ============================================================
elif view == "People & Efficiency":
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ARPC", f"£{safe('arpc'):,.0f}")
    c2.metric("S&M Efficiency", f"{safe('sm_efficiency'):.2f}x")
    c3.metric("Revenue Churn", f"{safe('revenue_churn_pct'):.1f}%")
    c4.metric("Rule of 40", f"{safe('rule_of_40_score'):.1f}")
    st.markdown("---")

    t1, t2 = st.columns(2)
    with t1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["period"], y=df["total_headcount"], name="Actual",
                                 mode="lines+markers", line=dict(color=COLORS["accent_purple"], width=3)))
        if "headcount_budget" in df.columns:
            fig.add_trace(go.Scatter(x=df["period"], y=df["headcount_budget"], name="Budget",
                                     line=dict(dash="dot", color=COLORS["muted"])))
        fig = make_chart(fig)
        fig.update_layout(title="Headcount: Actual vs Budget")
        st.plotly_chart(fig, use_container_width=True)

    with t2:
        df["rev_per_emp"] = df["total_revenue"] / df["total_headcount"].replace(0, 1)
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df["period"], y=df["rev_per_emp"],
                             marker_color=COLORS["accent_blue"], name="Rev/Employee"))
        fig = make_chart(fig)
        fig.update_layout(title="Revenue per Employee (GBP k)")
        st.plotly_chart(fig, use_container_width=True)

    # Headcount breakdown
    st.subheader("Team Composition")
    hc_cols = {"ecommerce_headcount": "E-Commerce", "ems_headcount": "EMS",
               "services_headcount": "Services", "central_headcount": "Central"}
    available_hc = {k: v for k, v in hc_cols.items() if k in df.columns}
    if available_hc:
        fig = go.Figure()
        for col, label in available_hc.items():
            fig.add_trace(go.Bar(x=df["period"], y=df[col], name=label))
        fig = make_chart(fig, height=350)
        fig.update_layout(title="Headcount by Department", barmode="stack")
        st.plotly_chart(fig, use_container_width=True)

    # Efficiency metrics
    st.subheader("Efficiency Ratios")
    e1, e2, e3 = st.columns(3)
    e1.metric("Payroll % Revenue", f"{safe('payroll_pct_revenue'):.1f}%")
    e2.metric("Time to Value", f"{safe('time_to_value_days'):.0f} days")
    e3.metric("Indicative EV", f"£{safe('indicative_ev')/1e6:.1f}M")


# ============================================================
# VIEW 6: PRODUCT METRICS
# ============================================================
elif view == "Product Metrics":
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Modules Sold", f"{safe('modules_sold_pre_vouchers'):,.0f}")
    c2.metric("Modules Live", f"{safe('modules_live_pre_vouchers'):,.0f}")
    c3.metric("Properties Live", f"{safe('properties_live'):,.0f}")
    c4.metric("Module Churn", f"{safe('modules_churn'):,.0f}")
    st.markdown("---")

    t1, t2 = st.columns(2)
    with t1:
        mod_cols = {"modules_sold_pre_vouchers": "Sold", "modules_live_pre_vouchers": "Live"}
        available_mod = {k: v for k, v in mod_cols.items() if k in df.columns}
        if available_mod:
            fig = go.Figure()
            for col, label in available_mod.items():
                fig.add_trace(go.Scatter(x=df["period"], y=df[col], name=label,
                                         mode="lines+markers",
                                         line=dict(width=3)))
            fig = make_chart(fig)
            fig.update_layout(title="Module Growth: Sold vs Live")
            st.plotly_chart(fig, use_container_width=True)

    with t2:
        prop_cols = {"properties_sold": "Sold", "properties_live": "Live"}
        available_prop = {k: v for k, v in prop_cols.items() if k in df.columns}
        if available_prop:
            fig = go.Figure()
            for col, label in available_prop.items():
                fig.add_trace(go.Scatter(x=df["period"], y=df[col], name=label,
                                         mode="lines+markers", line=dict(width=3)))
            fig = make_chart(fig)
            fig.update_layout(title="Property Growth: Sold vs Live")
            st.plotly_chart(fig, use_container_width=True)

    # Monthly velocity
    st.subheader("Monthly Velocity")
    v1, v2 = st.columns(2)
    with v1:
        if "sold_per_month_pre_vouchers" in df.columns:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["period"], y=df["sold_per_month_pre_vouchers"],
                                 marker_color=COLORS["accent_blue"], name="Sold/Month"))
            fig = make_chart(fig, height=300)
            fig.update_layout(title="Modules Sold per Month")
            st.plotly_chart(fig, use_container_width=True)
    with v2:
        if "live_per_month_pre_vouchers" in df.columns:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["period"], y=df["live_per_month_pre_vouchers"],
                                 marker_color=COLORS["accent_green"], name="Live/Month"))
            fig = make_chart(fig, height=300)
            fig.update_layout(title="Modules Going Live per Month")
            st.plotly_chart(fig, use_container_width=True)

    # Conversion funnel
    if "modules_sold_pre_vouchers" in df.columns and "modules_live_pre_vouchers" in df.columns:
        conv_rate = safe("modules_live_pre_vouchers") / max(safe("modules_sold_pre_vouchers"), 1) * 100
        st.metric("Sold-to-Live Conversion", f"{conv_rate:.1f}%")


# ============================================================
# VIEW 7: COVENANTS & RISK
# ============================================================
elif view == "Covenants & Risk":
    st.subheader("Growth Lending Covenants")

    # RAG Status
    rev_rag = safe("averroes_revenue_rag")
    eb_rag = safe("averroes_ebitda_rag")
    r1, r2 = st.columns(2)
    r1.markdown(f"### Revenue Covenant {rag_dot(rev_rag)} {'PASS' if rev_rag != 'red' else 'BREACH'}", unsafe_allow_html=True)
    r2.markdown(f"### EBITDA Covenant {rag_dot(eb_rag)} {'PASS' if eb_rag != 'red' else 'BREACH'}", unsafe_allow_html=True)

    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Revenue Actual (Cum)", f"£{safe('gl_revenue_actual_cumulative'):,.0f}k")
    c2.metric("Revenue Covenant (Cum)", f"£{safe('gl_revenue_covenant_cumulative'):,.0f}k")
    c3.metric("EBITDA Actual (Cum)", f"£{safe('gl_ebitda_actual_cumulative'):,.0f}k")
    c4.metric("EBITDA Covenant (Cum)", f"£{safe('gl_ebitda_covenant_cumulative'):,.0f}k")

    st.markdown("<br>", unsafe_allow_html=True)

    # Headroom Charts
    t1, t2 = st.columns(2)
    with t1:
        if "gl_revenue_actual_cumulative" in df.columns:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["period"], y=df["gl_revenue_actual_cumulative"],
                                     name="Actual", line=dict(color=COLORS["accent_blue"], width=3)))
            fig.add_trace(go.Scatter(x=df["period"], y=df["gl_revenue_covenant_cumulative"],
                                     name="Covenant", line=dict(color=COLORS["accent_red"], width=2, dash="dash")))
            fig = make_chart(fig)
            fig.update_layout(title="Revenue: Actual vs Covenant (Cumulative, GBP k)")
            st.plotly_chart(fig, use_container_width=True)

    with t2:
        if "gl_ebitda_actual_cumulative" in df.columns:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["period"], y=df["gl_ebitda_actual_cumulative"],
                                     name="Actual", line=dict(color=COLORS["accent_blue"], width=3)))
            fig.add_trace(go.Scatter(x=df["period"], y=df["gl_ebitda_covenant_cumulative"],
                                     name="Covenant", line=dict(color=COLORS["accent_red"], width=2, dash="dash")))
            fig = make_chart(fig)
            fig.update_layout(title="EBITDA: Actual vs Covenant (Cumulative, GBP k)")
            st.plotly_chart(fig, use_container_width=True)

    # Headroom
    st.subheader("Headroom Analysis")
    h1, h2 = st.columns(2)
    rev_hr = safe("gl_revenue_headroom_pct")
    eb_hr = safe("gl_ebitda_headroom_pct")
    h1.metric("Revenue Headroom", f"{rev_hr:.1f}%",
              delta="Comfortable" if rev_hr > 5 else "Tight",
              delta_color="normal" if rev_hr > 5 else "inverse")
    h2.metric("EBITDA Headroom", f"{eb_hr:.1f}%",
              delta="Comfortable" if eb_hr > 5 else "Tight",
              delta_color="normal" if eb_hr > 5 else "inverse")

    # Risk flags
    st.subheader("Risk Flags")
    risks = []
    if safe("cash_runway_months") < 12:
        risks.append(("Cash runway below 12 months", "red"))
    if safe("revenue_churn_pct") > 5:
        risks.append(("Revenue churn above 5%", "amber"))
    if safe("rule_of_40_score") < 30:
        risks.append(("Rule of 40 below 30 for 3+ months", "amber"))
    if rev_hr < 5:
        risks.append(("Revenue covenant headroom tight (<5%)", "red"))
    if eb_hr < 5:
        risks.append(("EBITDA covenant headroom tight (<5%)", "red"))

    if risks:
        for desc, severity in risks:
            st.markdown(f"{rag_dot(severity)} **{desc}**", unsafe_allow_html=True)
    else:
        st.success("No active risk flags. All metrics within acceptable ranges.")

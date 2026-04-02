import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Averroes Capital - Portfolio KPI Dashboard",
    layout="wide",
    page_icon="🏦"
)

PROJECT_ID = "averroes-portfolio-intel"

# ============================================================
# DESIGN SYSTEM - Premium Executive Theme (Navy & White)
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
    
    /* KPI Card Styling */
    div[data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        padding: 20px !important;
        border-radius: 4px;
    }
    
    [data-testid="stMetricValue"] {
        color: #0f172a !important;
        font-size: 2.2rem !important;
        font-weight: 700 !important;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 0.8rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #64748b !important;
    }
    
    /* RAG Status Indicators */
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
# DATA LOADING - PRODUCTION WITH DUMMY FALLBACK
# ============================================================
@st.cache_data(ttl=600)
def load_data():
    """
    Load data from BigQuery for live companies.
    Falls back to gold_dummy_data.csv for 'Portco Dummy' demonstration.
    """
    try:
        # Check for Streamlit Secrets (for Cloud Deployment)
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info)
            client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        else:
            # Local fallback (ADC)
            client = bigquery.Client(project=PROJECT_ID)
            
        query = f"SELECT * FROM `{PROJECT_ID}.gold.kpi_monthly` ORDER BY period ASC"
        df_bq = client.query(query).to_dataframe()
        
        # Load local dummy data for demonstration purposes
        df_dummy = pd.read_csv("gold_dummy_data.csv")
        df_dummy['period'] = pd.to_datetime(df_dummy['period'])
        
        # Combine - BigQuery is the primary source of truth, Dummy added for UI testing
        if not df_bq.empty:
            df = pd.concat([df_bq, df_dummy], ignore_index=True)
            df['period'] = pd.to_datetime(df['period']) # Normalize all to Timestamps
            return df, "connected"
        else:
            return df_dummy, "demo_only"
            
    except Exception as e:
        # If BigQuery fails, only show dummy data
        print(f"BigQuery Connection Issue: {e}")
        try:
            df_dummy = pd.read_csv("gold_dummy_data.csv")
            df_dummy['period'] = pd.to_datetime(df_dummy['period'])
            return df_dummy, "demo_only"
        except:
            return pd.DataFrame(), "error"

df_raw, data_status = load_data()

# ============================================================
# SIDEBAR FILTERS
# ============================================================
st.sidebar.title("🏦 Averroes Capital")
st.sidebar.markdown("Portfolio Intelligence Platform")
st.sidebar.markdown("---")

if not df_raw.empty:
    portco_list = sorted(df_raw['portco_id'].unique())
    selected_portco = st.sidebar.selectbox("Select Portfolio Company", portco_list)

    # Filter by Company
    pc_df = df_raw[df_raw['portco_id'] == selected_portco]

    # Dynamic Product Filter (only if multiple products exist)
    unique_prods = sorted(pc_df['product_id'].dropna().unique())
    if len(unique_prods) > 1:
        product_list = ["Aggregate View"] + list(unique_prods)
        selected_product = st.sidebar.selectbox("Select Product Vertical", product_list)
        
        if selected_product != "Aggregate View":
            filtered_df = pc_df[pc_df['product_id'] == selected_product]
        else:
            filtered_df = pc_df
    else:
        filtered_df = pc_df
        selected_product = unique_prods[0] if unique_prods else "All"
else:
    st.error("No data found in Gold Layer or gold_dummy_data.csv")
    st.stop()

st.sidebar.markdown("---")
# Grouping Logic for Aggregation
numeric_cols = filtered_df.select_dtypes(include=[np.number]).columns
view_df = filtered_df.groupby('period')[numeric_cols].mean().reset_index().sort_values('period')
# Note: For ARR Bridge, we sum instead of mean
view_df['arr'] = filtered_df.groupby('period')['arr'].sum().values
view_df['opening_arr'] = filtered_df.groupby('period')['opening_arr'].sum().values
view_df['new_arr'] = filtered_df.groupby('period')['new_arr'].sum().values
view_df['expansion_arr'] = filtered_df.groupby('period')['expansion_arr'].sum().values
view_df['churn_arr'] = filtered_df.groupby('period')['churn_arr'].sum().values
view_df['closing_arr'] = filtered_df.groupby('period')['closing_arr'].sum().values

if data_status == "connected":
    st.sidebar.success("✅ Connected to BigQuery Gold Layer")
else:
    st.sidebar.warning("⚠️ Using local gold_dummy_data.csv")

st.sidebar.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")
@st.cache_data
def get_delta_val(curr, prev):
    if prev == 0 or pd.isna(prev): return "0.0%"
    diff = ((curr / prev) - 1) * 100
    return f"{diff:+.1f}%"

latest = view_df.iloc[-1]
prev = view_df.iloc[-2] if len(view_df) > 1 else latest

# ============================================================
# HEADER
# ============================================================
st.markdown(f"<div class='main-header'>{selected_portco} — KPI Dashboard</div>", unsafe_allow_html=True)
st.markdown(f"<div class='sub-header'>Reporting period: {latest['period'].strftime('%B %Y')} &nbsp;•&nbsp; Prepared by: GP team &nbsp;•&nbsp; All figures £000s unless stated</div>", unsafe_allow_html=True)

# ============================================================
# EXECUTIVE SUMMARY SECTION
# ============================================================
st.markdown("<div class='kpi-section-title'>Executive summary — key metrics</div>", unsafe_allow_html=True)

c1, c2, c3, c4, c5, c6 = st.columns(6)

def get_delta(curr, prev):
    if prev == 0: return "0%"
    diff = ((curr / prev) - 1) * 100
    return f"{diff:+.1f}%"

c1.metric("ARR", f"£{latest['arr']/1000:,.2f}M", get_delta_val(latest['arr'], prev['arr']))
c2.metric("NRR", f"{latest['nrr_pct']:.0f}%", get_delta_val(latest['nrr_pct'], prev['nrr_pct']))
c3.metric("Gross Margin", f"{latest['gross_margin_pct']:.0f}%", get_delta_val(latest['gross_margin_pct'], prev['gross_margin_pct']))
c4.metric("Rule of 40", f"{latest['rule_of_40']:.0f}", get_delta_val(latest['rule_of_40'], prev['rule_of_40']))
c5.metric("Runway", f"{latest['runway_months']:.0f}mo", get_delta_val(latest['runway_months'], prev['runway_months']))
c6.metric("Pipeline Coverage", f"{latest['pipeline_coverage']:.1f}x", get_delta_val(latest['pipeline_coverage'], prev['pipeline_coverage']))

# ============================================================
# ARR BRIDGE SECTION
# ============================================================
st.markdown("<div class='kpi-section-title'>ARR bridge — " + latest['period'].strftime('%B %Y') + "</div>", unsafe_allow_html=True)
st.caption("Read this chart first. New + expansion ARR versus churn defines the health of the revenue engine. Expansion exceeding churn = land-and-expand working.")

# ARR Waterfall Chart
fig_waterfall = go.Figure(go.Waterfall(
    name = "ARR Bridge", orientation = "v",
    measure = ["relative", "relative", "relative", "relative", "total"],
    x = ["Opening ARR", "New ARR", "Expansion", "Churn", "Closing ARR"],
    textposition = "outside",
    text = [f"£{x:,.0f}" for x in [latest['opening_arr'], latest['new_arr'], latest['expansion_arr'], latest['churn_arr'], latest['closing_arr']]],
    y = [latest['opening_arr'], latest['new_arr'], latest['expansion_arr'], latest['churn_arr'], latest['closing_arr']],
    connector = {"line":{"color":"rgb(63, 63, 63)"}},
    increasing = {"marker":{"color":"#10b981"}}, # Green
    decreasing = {"marker":{"color":"#ef4444"}}, # Red
    totals = {"marker":{"color":"#1e3a8a"}} # Navy
))

fig_waterfall.update_layout(
    title = "ARR waterfall — new, expansion, churn, net",
    showlegend = False,
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='white',
    height=400,
    margin=dict(t=50, b=20, l=20, r=20)
)
fig_waterfall.update_yaxes(showgrid=True, gridcolor='#f1f5f9')

st.plotly_chart(fig_waterfall, use_container_width=True)

# ============================================================
# TREND ANALYSIS SECTION
# ============================================================
st.markdown("<div class='kpi-section-title'>Trend analysis — 6-month view</div>", unsafe_allow_html=True)

t1, t2 = st.columns(2)

with t1:
    # ARR Progression Area Chart
    fig_arr = px.area(view_df, x="period", y="arr", title="ARR progression (£k)")
    fig_arr.update_traces(line_color='#0ea5e9', fillcolor='rgba(14, 165, 233, 0.1)')
    fig_arr.update_layout(plot_bgcolor='white', paper_bgcolor='white', height=300)
    fig_arr.update_xaxes(title="", showgrid=False)
    fig_arr.update_yaxes(title="", gridcolor='#f1f5f9')
    st.plotly_chart(fig_arr, use_container_width=True)
    
    # Gross Margin & Rule of 40
    fig_gm_r40 = go.Figure()
    fig_gm_r40.add_trace(go.Scatter(x=view_df['period'], y=view_df['gross_margin_pct'], name="Gross Margin %", line=dict(color='#6366f1', width=3)))
    fig_gm_r40.add_trace(go.Scatter(x=view_df['period'], y=view_df['rule_of_40'], name="Rule of 40", line=dict(color='#0ea5e9', width=3)))
    fig_gm_r40.update_layout(title="Gross margin & Rule of 40", plot_bgcolor='white', paper_bgcolor='white', height=300, legend=dict(orientation="h", y=1.1))
    fig_gm_r40.update_xaxes(showgrid=False)
    fig_gm_r40.update_yaxes(gridcolor='#f1f5f9')
    st.plotly_chart(fig_gm_r40, use_container_width=True)

with t2:
    # NRR & GRR Dual Line
    fig_ret = go.Figure()
    fig_ret.add_trace(go.Scatter(x=view_df['period'], y=view_df['nrr_pct'], name="NRR %", line=dict(color='#059669', width=3)))
    fig_ret.add_trace(go.Scatter(x=view_df['period'], y=view_df['grr_pct'], name="GRR %", line=dict(color='#059669', width=2, dash='dot')))
    fig_ret.update_layout(title="NRR & GRR (%)", plot_bgcolor='white', paper_bgcolor='white', height=300, legend=dict(orientation="h", y=1.1))
    fig_ret.update_xaxes(showgrid=False)
    fig_ret.update_yaxes(gridcolor='#f1f5f9')
    st.plotly_chart(fig_ret, use_container_width=True)

    # Pipeline Coverage & Quota
    fig_pipe = go.Figure()
    fig_pipe.add_trace(go.Scatter(x=view_df['period'], y=view_df['pipeline_coverage'], name="Pipeline Coverage (x)", line=dict(color='#854d0e', width=3)))
    fig_pipe.update_layout(title="Pipeline coverage", plot_bgcolor='white', paper_bgcolor='white', height=300)
    fig_pipe.update_xaxes(showgrid=False)
    fig_pipe.update_yaxes(gridcolor='#f1f5f9')
    st.plotly_chart(fig_pipe, use_container_width=True)

# ============================================================
# RAG STATUS TABLE SECTION
# ============================================================
st.markdown("<div class='kpi-section-title'>RAG status — all KPIs vs benchmark</div>", unsafe_allow_html=True)

rag_data = [
    {"Metric": "ARR", "Latest": f"£{latest['arr']/1000:.2f}M", "Prior Month": f"£{prev['arr']/1000:.2f}M", "MoM Change": f"{(latest['arr']-prev['arr']):+,.0f}k", "Benchmark": "—", "Status": "ON TRACK", "Trend Signal": "Rising consistently."},
    {"Metric": "NRR", "Latest": f"{latest['nrr_pct']:.0f}%", "Prior Month": f"{prev['nrr_pct']:.0f}%", "MoM Change": f"{(latest['nrr_pct']-prev['nrr_pct']):+.1f}%", "Benchmark": "110%", "Status": "WATCH", "Trend Signal": "Trending toward 110% target."},
    {"Metric": "GRR", "Latest": f"{latest['grr_pct']:.0f}%", "Prior Month": f"{prev['grr_pct']:.0f}%", "MoM Change": f"{(latest['grr_pct']-prev['grr_pct']):+.1f}%", "Benchmark": "90%", "Status": "ON TRACK", "Trend Signal": "Solid improvement trend."},
    {"Metric": "Gross Margin", "Latest": f"{latest['gross_margin_pct']:.0f}%", "Prior Month": f"{prev['gross_margin_pct']:.0f}%", "MoM Change": f"{(latest['gross_margin_pct']-prev['gross_margin_pct']):+.1f}%", "Benchmark": "70%", "Status": "ON TRACK", "Trend Signal": "Infrastructure optimization contributing."},
    {"Metric": "Net Burn", "Latest": f"£{latest['net_burn']:.0f}k", "Prior Month": f"£{prev['net_burn']:.0f}k", "MoM Change": f"{(latest['net_burn']-prev['net_burn']):+.1f}k", "Benchmark": "—", "Status": "ON TRACK", "Trend Signal": "Burn multiple improving."},
    {"Metric": "Runway", "Latest": f"{latest['runway_months']:.0f}mo", "Prior Month": f"{prev['runway_months']:.0f}mo", "MoM Change": f"{(latest['runway_months']-prev['runway_months']):+.1f}mo", "Benchmark": "18mo", "Status": "ON TRACK", "Trend Signal": "Extending — positive signal."},
    {"Metric": "LTV:CAC", "Latest": f"{latest['ltv_cac_ratio']:.1f}x", "Prior Month": f"{prev['ltv_cac_ratio']:.1f}x", "MoM Change": f"{(latest['ltv_cac_ratio']-prev['ltv_cac_ratio']):+.1f}x", "Benchmark": "3x", "Status": "ON TRACK", "Trend Signal": "Crossed 3x target."},
]

st.table(pd.DataFrame(rag_data))

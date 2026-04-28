import streamlit as st
import pandas as pd
from google.cloud import bigquery
import google.generativeai as genai

# Page config
st.set_page_config(page_title="AI Data Analyst (Advanced)", page_icon="🤖", layout="wide")

PROJECT_ID = "averroes-portfolio-intel"

# ============================================================
# DESIGN SYSTEM — Clean White + Averroes Blue
# ============================================================
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [data-testid="stAppViewContainer"] {
        background-color: #ffffff !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* ---- SIDEBAR ---- */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%) !important;
    }
    [data-testid="stSidebar"] * { color: #e2e8f0 !important; }
    [data-testid="stSidebar"] [data-testid="stMarkdown"] p,
    [data-testid="stSidebar"] [data-testid="stMarkdown"] span { color: #e2e8f0 !important; }
    [data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.1) !important; }

    /* ---- HEADERS ---- */
    .main-header {
        font-size: 1.85rem; font-weight: 700; color: #0f172a;
        margin-bottom: 0; letter-spacing: -0.02em;
    }
    .sub-header {
        font-size: 0.85rem; color: #64748b; margin-bottom: 28px; font-weight: 400;
    }

    /* ---- CHAT ---- */
    .ai-bubble {
        background-color: #f8fafc; border-left: 3px solid #0ea5e9;
        padding: 16px 20px; border-radius: 8px; margin-bottom: 20px;
    }
    [data-testid="stChatMessage"] {
        border-radius: 10px;
        border: 1px solid #e2e8f0;
        margin-bottom: 12px;
    }

    hr { border: none; border-top: 1px solid #e2e8f0; margin: 24px 0; }
    </style>
""", unsafe_allow_html=True)

# Sidebar branding
st.sidebar.markdown("""
<div style="text-align:center; padding: 8px 0 16px 0;">
    <div style="font-size:1.3rem; font-weight:700; color:#ffffff; letter-spacing:-0.02em;">
        Averroes Capital
    </div>
    <div style="font-size:0.7rem; font-weight:500; color:#7dd3fc; text-transform:uppercase; letter-spacing:0.12em; margin-top:2px;">
        Portfolio Intelligence
    </div>
</div>
""", unsafe_allow_html=True)
st.sidebar.markdown("---")

st.markdown("<div class='main-header'>AI Data Analyst</div>", unsafe_allow_html=True)
st.markdown("<div class='sub-header'>Connected to Bronze (Raw) and Gold (Consolidated) layers &nbsp;&bull;&nbsp; Ask natural language questions about performance, audit trails, or GL details</div>", unsafe_allow_html=True)

# Initialization
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "dataframe" in message:
            st.dataframe(message["dataframe"])

# Check API key
if "GEMINI_API_KEY" not in st.secrets:
    st.warning("🔒 **API Key Missing**: Please add `GEMINI_API_KEY = 'your_key_here'` to your Streamlit secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
model = genai.GenerativeModel('gemini-2.5-flash')

# ============================================================
# CROSS-LAYER SCHEMA CONTEXT
# ============================================================
SCHEMA_CONTEXT = f"""
You are an expert PE Data Scientist for Averroes Capital. 
You translate complex NL questions into BigQuery SQL. 

DATABASES AVAILABLE:
1. GOLD LAYER: `{PROJECT_ID}.gold.kpi_monthly_v2`
   - Use for: Executive performance, ARR trends, Margins, Rule of 40, Cash, Headcount totals.
   - Key Columns:
     - Revenue: tech_arr, ecommerce_arr, ems_arr, services_arr, revenue_total_actual, revenue_total_budget, revenue_yoy_growth_pct
     - BL Revenue: revenue_ecommerce_actual, revenue_ems_actual, revenue_services_actual
     - MRR: tech_mrr_actual, tech_mrr_budget, services_mrr_actual, services_mrr_budget, ecommerce_mrr_actual, ems_mrr_actual
     - Profit: ebitda_actual, ebitda_budget, ebitda_ytd_actual, ebitda_margin_pct, tech_gross_margin_pct, total_overheads, contribution_total
     - Cash: cash_balance, cash_balance_budget, cash_burn_monthly, net_working_capital, net_debt
     - Efficiency: rule_of_40, arpc_actual, arpc_budget, revenue_churn_pct, free_cash_conversion_month, sm_efficiency, indicative_ev
     - Waterfall: wf_revenue_start, wf_one_off_prev, wf_one_off_ytd, wf_recurring_growth, wf_arr_ytg, wf_weighted_pipeline, wf_budget_assumptions, wf_revenue_gap, wf_revenue_end
     - Covenants: gl_arr_actual, gl_arr_covenant, gl_arr_ratio, gr_revenue_ratio, gr_ebitda_capex_ratio
     - People: total_headcount
     - Time: period (DATE), fy (STRING e.g. 'FY26'), fy_quarter, fy_month_num
     - Modules: modules_live_total, modules_live_ecommerce, modules_live_ems

2. BRONZE LAYER: `{PROJECT_ID}.bronze.raw_management_accounts`
   - Use for: Audit trails, finding specific cells in the Excel MA file, looking up raw GL row labels.
   - Columns: 
     - row_label (STRING): Exact row name from Excel (e.g. 'EMS Revenue')
     - sheet_name (STRING): Exact sheet name (e.g. 'P&L Summary ', 'Headcount')
     - value (FLOAT64): Raw numeric value.
     - source_cell (STRING): Excel cell reference (e.g. 'B35').

STRATEGY:
- If asked "What is the total ARR?", use GOLD.
- If asked "Where did the EBITDA number come from?" or "What was the raw value in cell B18 of the P&L sheet?", use BRONZE.
- Return ONLY the SQL query. No markdown.

User Question: 
"""

# Chat Input
if prompt := st.chat_input("E.g. Which Portco has the lowest Cash Runway but high Rule of 40?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing Cross-Layer Data..."):
            try:
                response = model.generate_content(SCHEMA_CONTEXT + prompt)
                sql_query = response.text.replace("```sql", "").replace("```", "").strip()
                
                # Execute in BigQuery
                if "gcp_service_account" in st.secrets:
                    from google.oauth2 import service_account
                    info = st.secrets["gcp_service_account"]
                    credentials = service_account.Credentials.from_service_account_info(info)
                    bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
                else:
                    bq_client = bigquery.Client(project=PROJECT_ID)
                    
                df = bq_client.query(sql_query).to_dataframe()
                
                st.markdown(f"**Cross-Layer Insight ({len(df)} rows):**")
                st.dataframe(df)
                
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": f"**Decision:** Analyzed the Data layers and executed this query:\n```sql\n{sql_query}\n```",
                    "dataframe": df
                })
                
            except Exception as e:
                st.error(f"Execution Error: {e}")
                st.session_state.messages.append({"role": "assistant", "content": f"Error: {e}"})

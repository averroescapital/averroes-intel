import streamlit as st
import pandas as pd
from google.cloud import bigquery
import google.generativeai as genai

# Page config
st.set_page_config(page_title="AI Data Analyst (Advanced)", page_icon="🤖", layout="wide")

PROJECT_ID = "averroes-portfolio-intel"

# ============================================================
# DESIGN SYSTEM
# ============================================================
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [data-testid="stAppViewContainer"] {
        background-color: #fcfcfc !important;
        font-family: 'Inter', sans-serif;
    }
    .main-header { font-size: 2.2rem; font-weight: 700; color: #0f172a; margin-bottom: 0px; }
    .sub-header { font-size: 0.9rem; color: #64748b; margin-bottom: 30px; }
    .ai-bubble { background-color: #f8fafc; border-left: 4px solid #0ea5e9; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
    </style>
""", unsafe_allow_html=True)

st.markdown("<div class='main-header'>🤖 Enterprise AI Data Analyst</div>", unsafe_allow_html=True)
st.markdown("<div class='sub-header'>Connected to **Bronze (Raw)** and **Gold (Consolidated)** layers. Ask Natural Language questions about performance, audit trails, or GL details.</div>", unsafe_allow_html=True)

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
1. GOLD LAYER: `{PROJECT_ID}.gold.kpi_monthly`
   - Use for: Executive performance, ARR trends, Margins, Rule of 40, Cash, Headcount totals.
   - Key Columns (104 available): 
     - Revenue: total_arr, tech_arr, services_arr, revenue_total_actual, revenue_yoy_growth_pct
     - Profit: ebitda_actual, ebitda_margin_pct, tech_gross_margin_pct, total_overheads
     - Cash/Efficiency: cash_balance, cash_runway_months, rule_of_40, sm_efficiency
     - Retention: revenue_churn_pct, nrr_pct, grr_pct, top5_customer_pct
     - People: total_headcount, headcount_ecommerce, headcount_central, gross_payroll
     - Time: period (DATE), fy (STRING e.g. 'FY26'), fy_quarter, fy_month_num

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

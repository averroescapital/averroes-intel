import streamlit as st
import pandas as pd
from google.cloud import bigquery
import google.generativeai as genai

# Page config
st.set_page_config(page_title="AI Data Analyst", page_icon="🤖", layout="wide")

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
    </style>
""", unsafe_allow_html=True)

st.markdown("<div class='main-header'>🤖 AI Data Analyst</div>", unsafe_allow_html=True)
st.markdown("<div class='sub-header'>Ask natural language questions about your portfolio performance. The AI translates your query into BigQuery SQL and fetches live results.</div>", unsafe_allow_html=True)

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
    st.warning("🔒 **API Key Missing**: Please add `GEMINI_API_KEY = 'your_key_here'` to your Streamlit secrets to enable the AI Analyst.")
    st.stop()

genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
model = genai.GenerativeModel('gemini-2.5-flash')

# BigQuery Schema Context
SCHEMA_CONTEXT = f"""
You are an expert Google BigQuery data analyst working for a Private Equity firm. 
You must translate the user's natural language question into a valid BigQuery SQL query.
Return ONLY the raw SQL query, without any markdown formatting (no ```sql). Do not include any explanations.

The table is `{PROJECT_ID}.gold.kpi_monthly`.
Here is the schema of the table:
- period (DATE): The month of the reporting period.
- portco_id (STRING): Name of the portfolio company (e.g. 'Portco Alpha', 'Portco Dummy')
- product_id (STRING): Name of the product grouping.
- arr (FLOAT64): Annual Recurring Revenue
- opening_arr (FLOAT64)
- new_arr (FLOAT64)
- expansion_arr (FLOAT64)
- churn_arr (FLOAT64)
- closing_arr (FLOAT64)
- nrr_pct (FLOAT64): Net Retention Rate Percentage
- grr_pct (FLOAT64): Gross Retention Rate Percentage
- gross_margin_pct (FLOAT64)
- rule_of_40 (FLOAT64)
- runway_months (FLOAT64)
- pipeline_coverage (FLOAT64)
- quota_attainment_pct (FLOAT64)
- ltv_cac_ratio (FLOAT64)
- net_burn (FLOAT64)
- nps (FLOAT64)

User Question: 
"""

# Chat Input
if prompt := st.chat_input("E.g., What is the total ARR and average Net Burn for Portco Dummy grouped by product?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing request and generating query..."):
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
                
                st.markdown(f"**Result ({len(df)} rows):**")
                st.dataframe(df)
                
                # Append to history
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": f"**Executed Query:**\n```sql\n{sql_query}\n```",
                    "dataframe": df
                })
                
            except Exception as e:
                st.error(f"Error executing query: {e}")
                st.session_state.messages.append({"role": "assistant", "content": f"Error: {e}"})

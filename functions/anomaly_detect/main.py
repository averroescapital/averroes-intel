import os
import uuid
import yaml
import json
import pandas as pd
from datetime import datetime
import functions_framework
from google.cloud import bigquery
import google.generativeai as genai

project_id = os.environ.get('GCP_PROJECT', 'averroes-portfolio-intel')
bq_client = bigquery.Client(project=project_id)
genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))

# Load taxonomy and rules
try:
    with open('kpi_taxonomy.yaml', 'r') as f:
        TAXONOMY = yaml.safe_load(f)
except Exception as e:
    print(f"Error loading taxonomy: {e}")
    TAXONOMY = {'anomaly_rules': [], 'portcos': []}

def detect_statistical_anomalies(df):
    alerts = []
    # For each portco and KPI
    # df should be sorted by period ascending
    for (portco, kpi), group in df.groupby(['portco_id', 'kpi_name']):
        if len(group) < 6:
            continue
        
        # Calculate rolling metrics over last 6 months
        rolling_mean = group['value'].rolling(window=6).mean()
        rolling_std = group['value'].rolling(window=6).std()
        
        # Just check the latest row
        latest_row = group.iloc[-1]
        latest_mean = rolling_mean.iloc[-1]
        latest_std = rolling_std.iloc[-1]
        
        if pd.isna(latest_std) or latest_std == 0:
            continue
            
        z_score = (latest_row['value'] - latest_mean) / latest_std
        
        if abs(z_score) > 2.0:
            alerts.append({
                "alert_id": str(uuid.uuid4()),
                "portco_id": portco,
                "period": latest_row['period'],
                "kpi_name": kpi,
                "alert_type": "statistical",
                "severity": "info" if abs(z_score) < 3 else "amber",
                "rule_name": "statistical_anomaly",
                "current_value": float(latest_row['value']),
                "threshold_value": float(latest_mean + (2 * latest_std if z_score > 0 else -2 * latest_std)),
                "z_score": float(z_score),
                "description": f"Statistical anomaly: Value {latest_row['value']} deviates significantly from 6-month average ({latest_mean:.2f})",
                "consecutive_months": 1,
            })
    return alerts


def evaluate_rules(df):
    alerts = []
    rules = TAXONOMY.get('anomaly_rules', [])
    
    # Needs to track consecutive months, for simplicity just checking current condition
    # To truly support `consecutive_months`, we would need window functions.
    for rule in rules:
        rule_kpi = rule['kpi']
        condition = rule['condition'] # "mom_delta < -2", "value < 12", "value < 30 AND consecutive_months >= 3"
        
        # very simplified eval engine
        for (portco, kpi), group in df.groupby(['portco_id', 'kpi_name']):
            if rule_kpi != 'any' and kpi != rule_kpi:
                continue
                
            latest = group.iloc[-1]
            try:
                # We need to parse condition safely
                val = float(latest['value'])
                mom_delta = float(latest['mom_delta']) if pd.notna(latest['mom_delta']) else 0
                
                # Manual parsing of simple rules given in prompt
                condition_met = False
                if "mom_delta < -2" in condition and mom_delta < -2:
                    condition_met = True
                elif "value < 3 AND consecutive_months >= 2" in condition and val < 3:
                     # Simulating consecutive months
                     if len(group) >= 2 and group.iloc[-2]['value'] < 3:
                         condition_met = True
                elif "value < 12" in condition and val < 12:
                     condition_met = True
                elif "value < 30" in condition and val < 30:
                     if len(group) >= 3 and group.iloc[-2]['value'] < 30 and group.iloc[-3]['value'] < 30:
                          condition_met = True
                # etc... for a real app, use eval() on a safe dict or proper AST parser
                
                if condition_met:
                     alerts.append({
                        "alert_id": str(uuid.uuid4()),
                        "portco_id": portco,
                        "period": latest['period'],
                        "kpi_name": kpi,
                        "alert_type": "rule_based",
                        "severity": rule['severity'],
                        "rule_name": rule['name'],
                        "current_value": float(val),
                        "threshold_value": None,
                        "z_score": None,
                        "description": rule['description'],
                        "consecutive_months": 1 # simplified
                    })
            except Exception as e:
                pass
                
    return alerts

def generate_ai_commentary(portco_id, period, portco_df, alerts):
    model = genai.GenerativeModel('gemini-2.0-pro-exp-02-05') # Using a pro model for reasoning
    
    json_data = portco_df[['kpi_name', 'value', 'mom_delta', 'trend_3m']].to_json(orient="records")
    alerts_data = json.dumps([{k: v for k, v in a.items() if k in ['kpi_name', 'severity', 'description', 'current_value']} for a in alerts])
    
    portco_name = next((p['display_name'] for p in TAXONOMY.get('portcos', []) if p['id'] == portco_id), portco_id)
    
    prompt = f"""
You are a private equity portfolio analyst at Averroes Capital.
Review this month's KPI data for {portco_name} and generate:

1. EXECUTIVE SUMMARY (3-4 sentences): What happened this month? Is the business
   on track vs the investment thesis? What would a GP want to know in 30 seconds?

2. ANOMALY COMMENTARY: For each flagged anomaly below, provide PE-specific context.
   What does this mean for the investment? What should the GP ask the CFO?

3. TREND SIGNALS: What 3-month trends are most important right now?
   Are any leading indicators (pipeline, NPS) predicting future problems?

Data: {json_data}
Alerts: {alerts_data}

Write in concise, direct language. No jargon. Specific numbers only.
"""
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Gemini API error: {e}")
        return "Failed to generate AI commentary."

@functions_framework.http
def detect_anomalies(request):
    """HTTP Cloud Function to be triggered by Cloud Scheduler."""
    
    # 1. Fetch data from Gold Layer
    query = f"""
        SELECT portco_id, period, kpi_name, value, mom_delta, trend_3m
        FROM `{project_id}.gold.monthly_kpis`
        ORDER BY period ASC
    """
    df = bq_client.query(query).to_dataframe()
    
    if df.empty:
        return 'No data found in gold.monthly_kpis', 200

    # Execute Layers A and B
    stat_alerts = detect_statistical_anomalies(df)
    rule_alerts = evaluate_rules(df)
    
    all_alerts = stat_alerts + rule_alerts
    
    # Filter only alerts for the latest period for each portco to insert
    # In a full batch process, you'd only want to process new data or truncate/load active alerts.
    
    latest_period_df = df.groupby('portco_id').last().reset_index()
    latest_periods = latest_period_df.set_index('portco_id')['period'].to_dict()
    
    filtered_alerts = [a for a in all_alerts if a['period'] == latest_periods.get(a['portco_id'])]
    
    # Execute Layer C (AI Narrative)
    commentaries = []
    for portco_id, period in latest_periods.items():
        portco_df = df[(df['portco_id'] == portco_id) & (df['period'] == period)]
        portco_alerts = [a for a in filtered_alerts if a['portco_id'] == portco_id]
        
        narrative = generate_ai_commentary(portco_id, period, portco_df, portco_alerts)
        
        commentaries.append({
            "commentary_id": str(uuid.uuid4()),
            "portco_id": portco_id,
            "period": period,
            "commentary_type": "executive_summary",
            "commentary_text": narrative,
            "model_used": "gemini-2.0-pro"
        })
        
    # Write to BigQuery
    if filtered_alerts:
        bq_client.insert_rows_json(f"{project_id}.alerts.anomaly_flags", filtered_alerts)
        print(f"Inserted {len(filtered_alerts)} anomaly alerts.")
        
    if commentaries:
        bq_client.insert_rows_json(f"{project_id}.alerts.ai_commentary", commentaries)
        print(f"Inserted {len(commentaries)} AI commentaries.")

    return f"Processed {len(filtered_alerts)} alerts and {len(commentaries)} commentaries.", 200

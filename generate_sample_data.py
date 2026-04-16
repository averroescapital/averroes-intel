import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def generate_sample_data(portco_id, start_date, end_date, entry_arr, is_clean=True, name_mapping=None):
    dates = pd.date_range(start=start_date, end=end_date, freq='ME') # Month end
    periods = [d.strftime('%Y-%m') for d in dates]
    
    data = []
    current_arr = entry_arr
    for period in periods:
        
        # Base values
        new_arr = current_arr * np.random.uniform(0.01, 0.05)
        expansion_arr = current_arr * np.random.uniform(0.005, 0.02)
        
        # Introduce the required anomaly
        if portco_id == 'portco-alpha' and period == '2024-09':
            churn_arr = current_arr * 0.035 # 3.5% spike
        else:
            churn_arr = current_arr * np.random.uniform(0.005, 0.015)
            
        current_arr = current_arr + new_arr + expansion_arr - churn_arr
        
        row = {
            'ARR': current_arr,
            'New ARR': new_arr,
            'Expansion ARR': expansion_arr,
            'Churn ARR': churn_arr,
            'Net New ARR': new_arr + expansion_arr - churn_arr,
            'Gross Margin %': np.random.uniform(65, 80),
            'CAC (£)': np.random.uniform(5000, 15000),
            'EBITDA Margin %': np.random.uniform(-10, 20),
            'Pipeline Coverage (x)': np.random.uniform(2.5, 4.0),
            'Win Rate %': np.random.uniform(20, 40),
            'NPS': np.random.uniform(30, 60),
            'Total Headcount': int(current_arr / 150),
            'Runway (months)': np.random.uniform(10, 24)
        }
        
        if not is_clean and name_mapping:
            messy_row = {name_mapping.get(k, k): v for k, v in row.items()}
            data.append(messy_row)
        else:
            data.append(row)
            
    df = pd.DataFrame(data)
    df.insert(0, 'Period', periods)
    return df

os.makedirs('sample_data/portco-alpha', exist_ok=True)
os.makedirs('sample_data/portco-beta', exist_ok=True)
os.makedirs('sample_data/portco-gamma', exist_ok=True)

# Portco Alpha (Clean Excel, 30 months)
alpha_df = generate_sample_data('portco-alpha', '2023-01-01', '2025-06-30', 2800)
alpha_df.to_excel('sample_data/portco-alpha/Portco_Alpha_KPI_Monthly_Jan2023_Jun2025.xlsx', index=False)

# Portco Beta (Clean Excel, 24 months)
beta_clean_df = generate_sample_data('portco-beta', '2023-07-01', '2025-06-30', 1500)
beta_clean_df.to_excel('sample_data/portco-beta/Portco_Beta_KPI_Monthly_Jul2023_Jun2025.xlsx', index=False)

# Portco Beta (Messy CSV, to test alias mapping)
mapping = {
    'ARR': 'Annual Run Rate',
    'Gross Margin %': 'GM%',
    'Total Headcount': 'FTE',
    'Win Rate %': 'Close Rate'
}
beta_messy_df = generate_sample_data('portco-beta', '2025-07-01', '2025-12-31', 1500 * 1.5, is_clean=False, name_mapping=mapping)
beta_messy_df.to_csv('sample_data/portco-beta/Beta_Monthly_Report_Messy_Format.csv', index=False)

print("Sample data generated!")

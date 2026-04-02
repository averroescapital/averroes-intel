import pandas as pd
import numpy as np
from google.cloud import bigquery
import pandas_gbq

PROJECT_ID = "averroes-portfolio-intel"

def generate_portco_dummy_gold():
    """Generates a dummy gold layer dataset for testing and demonstration purposes."""
    np.random.seed(42)
    periods = pd.date_range("2025-01-01", "2025-06-01", freq="MS")
    portcos = ["Portco Dummy"]
    products = {
        "Portco Dummy": ["SaaS Product A", "SaaS Product B", "Enterprise Tier"]
    }
    
    rows = []
    for pc in portcos:
        for prod in products[pc]:
            # Baseline values for Portco Dummy
            arr_base = 1500 # GBPk
            nrr_base = 108
            gr_base = 70
            
            for i, p in enumerate(periods):
                # Monthly growth Factor for dummy data
                month_factor = 1 + (i * 0.02) 
                
                arr = arr_base * month_factor + np.random.normal(0, 20)
                opening_arr = arr / (1.02)
                new_arr = arr * 0.03
                expansion = arr * 0.015
                churn = - (arr * 0.025)
                
                rows.append({
                    "portco_id": pc,
                    "product_id": prod,
                    "period": p,
                    "arr": round(arr, 1),
                    "opening_arr": round(opening_arr, 1),
                    "new_arr": round(new_arr, 1),
                    "expansion_arr": round(expansion, 1),
                    "churn_arr": round(churn, 1),
                    "closing_arr": round(arr, 1),
                    "nrr_pct": round(nrr_base + np.random.normal(0, 1.5), 1),
                    "grr_pct": round(90 + np.random.normal(0, 1), 1),
                    "gross_margin_pct": round(gr_base + np.random.normal(0, 1), 1),
                    "rule_of_40": round(35 + (i * 1.5), 1),
                    "runway_months": 24 - i,
                    "pipeline_coverage": round(3.3 + np.random.normal(0, 0.1), 2),
                    "quota_attainment_pct": round(72 + np.random.normal(0, 3), 1),
                    "ltv_cac_ratio": round(3.4 + np.random.normal(0, 0.1), 2),
                    "net_burn": round(260 - (i * 12), 1),
                    "nps": 51 + i
                })
    
    df = pd.DataFrame(rows)
    # Save to a dedicated CSV file locally
    df.to_csv("gold_dummy_data.csv", index=False)
    
    # Upload to GCP BigQuery (Gold Layer)
    try:
        print(f"Uploading dummy data to {PROJECT_ID}.gold.kpi_monthly...")
        pandas_gbq.to_gbq(
            df, 
            destination_table='gold.kpi_monthly', 
            project_id=PROJECT_ID, 
            if_exists='append' # Append so we don't overwrite real data
        )
        print("Successfully uploaded Portco Dummy to BigQuery.")
    except Exception as e:
        print(f"Error uploading to BigQuery: {e}")
        
    return df

if __name__ == "__main__":
    generate_portco_dummy_gold()
    print("Gold dummy data file created: gold_dummy_data.csv")

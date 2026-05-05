"""
refresh_gold_csv.py — Export gold.kpi_monthly_v2 from BigQuery to local CSV fallback.

Usage:
    python scripts/refresh_gold_csv.py

Writes to: gold_kpi_monthly.csv (repo root, used by pe_app.py + Journey page fallback)

Authentication: uses Application Default Credentials
    gcloud auth application-default login
    gcloud config set project averroes-portfolio-intel
"""

import os
import sys
import pandas as pd

PROJECT_ID = "averroes-portfolio-intel"
GOLD_TABLE = f"{PROJECT_ID}.gold.kpi_monthly_v2"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
CSV_OUT = os.path.join(REPO_ROOT, "gold_kpi_monthly.csv")


def main():
    from google.cloud import bigquery

    print(f"Querying {GOLD_TABLE} ...")
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT * FROM `{GOLD_TABLE}` ORDER BY portco_id, period ASC"
    df = client.query(query).to_dataframe()

    if df.empty:
        print("ERROR: query returned 0 rows. CSV not updated.")
        sys.exit(1)

    # Normalise period to date-only string
    df["period"] = pd.to_datetime(df["period"]).dt.strftime("%Y-%m-%d")

    # Dedup by (portco_id, period), keep latest computed_at
    if "computed_at" in df.columns:
        df = (df.sort_values("computed_at")
                .drop_duplicates(subset=["portco_id", "period"], keep="last"))

    df = df.sort_values(["portco_id", "period"]).reset_index(drop=True)
    df.to_csv(CSV_OUT, index=False)

    periods = sorted(df["period"].unique())
    print(f"Wrote {len(df)} rows to {CSV_OUT}")
    print(f"Periods: {periods[0]} → {periods[-1]} ({len(periods)} months)")
    print(f"Portcos: {', '.join(df['portco_id'].unique())}")


if __name__ == "__main__":
    main()

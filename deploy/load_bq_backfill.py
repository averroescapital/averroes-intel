"""
Loads backfilled silver + gold CSVs into BigQuery.

Prerequisites:
  - silver.kpi_long + gold.kpi_monthly_v2 tables created
    (run:  bq query --use_legacy_sql=false < bq_schemas/silver_gold_v2.sql)
  - Python env has: pip install google-cloud-bigquery pandas pyarrow
  - Authenticated:  gcloud auth application-default login

Run:
  python deploy/load_bq_backfill.py
"""
import os
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "averroes-portfolio-intel"
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard")
SILVER_CSV = os.path.join(DASHBOARD_DIR, "silver_kpi_long.csv")
GOLD_CSV   = os.path.join(DASHBOARD_DIR, "gold_kpi_monthly.csv")

SILVER_TABLE = f"{PROJECT_ID}.silver.kpi_long"
GOLD_TABLE   = f"{PROJECT_ID}.gold.kpi_monthly_v2"

PORTCO_ID = "portco-alpha"


def load_silver(client: bigquery.Client):
    print(f"Loading silver from {SILVER_CSV} -> {SILVER_TABLE}")
    df = pd.read_csv(SILVER_CSV)
    df["period"] = pd.to_datetime(df["period"]).dt.date
    df["portco_id"] = PORTCO_ID
    # Re-order cols to match table DDL
    cols = ["portco_id", "period", "kpi", "value", "value_type",
            "business_line", "era", "source_file", "source_sheet", "source_cell"]
    df = df[cols]

    # Delete existing rows for this portco before re-loading (idempotent)
    client.query(
        f"DELETE FROM `{SILVER_TABLE}` WHERE portco_id = '{PORTCO_ID}'"
    ).result()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("portco_id",     "STRING", mode="REQUIRED"),
            bigquery.SchemaField("period",        "DATE",   mode="REQUIRED"),
            bigquery.SchemaField("kpi",           "STRING", mode="REQUIRED"),
            bigquery.SchemaField("value",         "FLOAT64"),
            bigquery.SchemaField("value_type",    "STRING"),
            bigquery.SchemaField("business_line", "STRING"),
            bigquery.SchemaField("era",           "STRING"),
            bigquery.SchemaField("source_file",   "STRING"),
            bigquery.SchemaField("source_sheet",  "STRING"),
            bigquery.SchemaField("source_cell",   "STRING"),
        ],
    )
    job = client.load_table_from_dataframe(df, SILVER_TABLE, job_config=job_config)
    job.result()
    print(f"  ✓ {len(df):,} silver rows loaded.")


def load_gold(client: bigquery.Client):
    print(f"Loading gold from {GOLD_CSV} -> {GOLD_TABLE}")
    df = pd.read_csv(GOLD_CSV)
    df["period"] = pd.to_datetime(df["period"]).dt.date
    if "portco_id" not in df.columns:
        df["portco_id"] = PORTCO_ID
    # computed_at should be timestamp
    if "computed_at" in df.columns:
        df["computed_at"] = pd.to_datetime(df["computed_at"], errors="coerce")

    # Delete existing rows for this portco
    client.query(
        f"DELETE FROM `{GOLD_TABLE}` WHERE portco_id = '{PORTCO_ID}'"
    ).result()

    # Use WRITE_APPEND + autodetect schema to accept any extra columns tolerantly
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    # Pull explicit schema from existing table so types match
    table_ref = client.get_table(GOLD_TABLE)
    job_config.schema = table_ref.schema

    # Drop df columns that aren't in the table, and add NULL columns for those missing
    table_cols = {f.name for f in table_ref.schema}
    for col in list(df.columns):
        if col not in table_cols:
            df = df.drop(columns=[col])
    for col in table_cols:
        if col not in df.columns:
            df[col] = None
    df = df[[f.name for f in table_ref.schema]]

    job = client.load_table_from_dataframe(df, GOLD_TABLE, job_config=job_config)
    job.result()
    print(f"  ✓ {len(df):,} gold rows loaded.")


def main():
    client = bigquery.Client(project=PROJECT_ID)
    load_silver(client)
    load_gold(client)
    print("\nAll done. Sample query:")
    print(f"  bq query --use_legacy_sql=false 'SELECT period, era, revenue_total_actual, tech_arr FROM `{GOLD_TABLE}` ORDER BY period'")


if __name__ == "__main__":
    main()

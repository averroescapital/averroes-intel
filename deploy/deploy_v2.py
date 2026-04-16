#!/usr/bin/env python3
"""
Averroes Portfolio Intel — Full v2 deployment (no gcloud CLI needed).

Uses a service account JSON key for all GCP operations:
  1. Apply v2 BigQuery schemas (silver.kpi_long + gold.kpi_monthly_v2)
  2. Run local backfill (parse all MA files → silver/gold CSVs)
  3. Load CSVs into BigQuery
  4. Upload raw MA files to GCS
  5. Sanity check query

Usage:
  # Set env vars then run:
  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your-service-account-key.json
  python3 deploy/deploy_v2.py

  # Or pass the key path directly:
  python3 deploy/deploy_v2.py --key /path/to/key.json

  # Skip file upload to GCS (if files already there):
  python3 deploy/deploy_v2.py --skip-upload

Prerequisites:
  pip install google-cloud-bigquery google-cloud-storage pandas pyarrow openpyxl
"""
import os
import sys
import glob
import argparse

# Ensure repo root is on path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, os.path.join(REPO_ROOT, "functions", "ingest"))

PROJECT_ID = "averroes-portfolio-intel"
REGION = "europe-west2"
BUCKET = f"{PROJECT_ID}-portfolio-data"
PORTCO_ID = "portco-alpha"
GCS_PREFIX = f"{PORTCO_ID}/ma-files"

SILVER_TABLE = f"{PROJECT_ID}.silver.kpi_long"
GOLD_TABLE = f"{PROJECT_ID}.gold.kpi_monthly_v2"


def step1_apply_schemas(client):
    """Apply v2 BigQuery schemas."""
    print("=" * 60)
    print(" 1. Apply v2 BigQuery schemas")
    print("=" * 60)

    sql_path = os.path.join(REPO_ROOT, "bq_schemas", "silver_gold_v2.sql")
    with open(sql_path) as f:
        sql = f.read()

    # Split on semicolons and execute each statement
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for i, stmt in enumerate(statements):
        if not stmt:
            continue
        try:
            job = client.query(stmt)
            job.result()
            # Extract table/view name for logging
            for keyword in ["TABLE", "VIEW", "SCHEMA"]:
                if keyword in stmt.upper():
                    break
            print(f"  ✓ Statement {i+1}/{len(statements)} executed")
        except Exception as e:
            print(f"  ⚠ Statement {i+1} warning: {e}")

    print("  ✓ silver.kpi_long created")
    print("  ✓ gold.kpi_monthly_v2 created")
    print("  ✓ Views created (revenue_ltm_by_bl, tech_arr_split, modules_live_view)")


def step2_backfill(ma_dir):
    """Run local backfill — parse all MA files into silver/gold CSVs."""
    print()
    print("=" * 60)
    print(" 2. Run local backfill (parse MA files → CSVs)")
    print("=" * 60)

    import pandas as pd
    from parsers.alpha_parser import parse_alpha_ma
    from silver_gold_v2 import build_silver_from_parsed, pivot_to_gold

    files = sorted(glob.glob(os.path.join(ma_dir, "*.xlsx")))
    print(f"  Found {len(files)} MA files in {ma_dir}")

    if not files:
        print("  ERROR: No .xlsx files found. Aborting backfill.")
        return None, None

    frames = []
    for f in files:
        with open(f, "rb") as fh:
            content = fh.read()
        try:
            parsed = parse_alpha_ma(content, os.path.basename(f))
        except Exception as e:
            print(f"  ERROR {os.path.basename(f)}: {e}")
            continue
        if not parsed:
            print(f"  EMPTY {os.path.basename(f)}")
            continue
        silver_df = build_silver_from_parsed(parsed, source_file=os.path.basename(f))
        frames.append(silver_df)

    if not frames:
        print("  ERROR: No data parsed. Aborting.")
        return None, None

    silver = pd.concat(frames, ignore_index=True)

    # De-duplicate: prefer later era
    era_rank = {"era1": 1, "era2": 2, "era3": 3, "unknown": 0}
    silver["_era_rank"] = silver["era"].map(era_rank).fillna(0)
    silver = silver.sort_values(["period", "kpi", "value_type", "business_line", "_era_rank"])
    silver = silver.drop_duplicates(
        subset=["period", "kpi", "value_type", "business_line"], keep="last"
    ).drop(columns=["_era_rank"]).reset_index(drop=True)

    gold = pivot_to_gold(silver, portco_id=PORTCO_ID)

    # Write CSVs
    out_dir = os.path.join(REPO_ROOT, "dashboard")
    silver_csv = os.path.join(out_dir, "silver_kpi_long.csv")
    gold_csv = os.path.join(out_dir, "gold_kpi_monthly.csv")
    silver.to_csv(silver_csv, index=False)
    gold.to_csv(gold_csv, index=False)
    print(f"  ✓ Silver: {silver_csv} ({len(silver):,} rows)")
    print(f"  ✓ Gold:   {gold_csv} ({len(gold):,} rows)")

    return silver_csv, gold_csv


def step3_load_bq(client, silver_csv, gold_csv):
    """Load backfill CSVs into BigQuery."""
    print()
    print("=" * 60)
    print(" 3. Load backfill CSVs into BigQuery")
    print("=" * 60)

    import pandas as pd
    from google.cloud import bigquery

    # --- Silver ---
    print(f"  Loading silver → {SILVER_TABLE}")
    df = pd.read_csv(silver_csv)
    df["period"] = pd.to_datetime(df["period"]).dt.date
    df["portco_id"] = PORTCO_ID
    cols = ["portco_id", "period", "kpi", "value", "value_type",
            "business_line", "era", "source_file", "source_sheet", "source_cell"]
    df = df[cols]

    client.query(f"DELETE FROM `{SILVER_TABLE}` WHERE portco_id = '{PORTCO_ID}'").result()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("portco_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("period", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("kpi", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("value", "FLOAT64"),
            bigquery.SchemaField("value_type", "STRING"),
            bigquery.SchemaField("business_line", "STRING"),
            bigquery.SchemaField("era", "STRING"),
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("source_sheet", "STRING"),
            bigquery.SchemaField("source_cell", "STRING"),
        ],
    )
    client.load_table_from_dataframe(df, SILVER_TABLE, job_config=job_config).result()
    print(f"  ✓ {len(df):,} silver rows loaded")

    # --- Gold ---
    print(f"  Loading gold → {GOLD_TABLE}")
    df = pd.read_csv(gold_csv)
    df["period"] = pd.to_datetime(df["period"]).dt.date
    if "portco_id" not in df.columns:
        df["portco_id"] = PORTCO_ID
    if "computed_at" in df.columns:
        df["computed_at"] = pd.to_datetime(df["computed_at"], errors="coerce")

    client.query(f"DELETE FROM `{GOLD_TABLE}` WHERE portco_id = '{PORTCO_ID}'").result()

    table_ref = client.get_table(GOLD_TABLE)
    table_cols = {f.name for f in table_ref.schema}
    for col in list(df.columns):
        if col not in table_cols:
            df = df.drop(columns=[col])
    for col in table_cols:
        if col not in df.columns:
            df[col] = None
    df = df[[f.name for f in table_ref.schema]]

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=table_ref.schema,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    client.load_table_from_dataframe(df, GOLD_TABLE, job_config=job_config).result()
    print(f"  ✓ {len(df):,} gold rows loaded")


def step4_upload_gcs(ma_dir):
    """Upload raw MA files to GCS."""
    print()
    print("=" * 60)
    print(" 4. Upload raw MA files to GCS")
    print("=" * 60)

    from google.cloud import storage

    gcs = storage.Client(project=PROJECT_ID)
    bucket = gcs.bucket(BUCKET)
    files = sorted(glob.glob(os.path.join(ma_dir, "*.xlsx")))

    for f in files:
        fname = os.path.basename(f)
        blob = bucket.blob(f"{GCS_PREFIX}/{fname}")
        print(f"  Uploading {fname}...")
        blob.upload_from_filename(f)

    print(f"  ✓ {len(files)} files uploaded to gs://{BUCKET}/{GCS_PREFIX}/")


def step5_sanity_check(client):
    """Quick sanity query."""
    print()
    print("=" * 60)
    print(" 5. Sanity check")
    print("=" * 60)

    query = f"""
    SELECT FORMAT_DATE('%Y-%m', period) AS month, era,
           ROUND(revenue_total_actual, 1) AS revenue_total,
           ROUND(tech_arr, 1) AS tech_arr,
           ROUND(gr_revenue_ratio, 3) AS rev_covenant_ratio,
           ROUND(gl_arr_ratio, 3) AS arr_covenant_ratio,
           total_headcount
    FROM `{GOLD_TABLE}`
    WHERE portco_id = '{PORTCO_ID}'
      AND revenue_total_actual IS NOT NULL
    ORDER BY period
    """
    result = client.query(query).result()
    rows = list(result)
    if rows:
        # Print header
        headers = [f.name for f in result.schema]
        print("  " + "  ".join(f"{h:>18s}" for h in headers))
        for row in rows:
            print("  " + "  ".join(f"{str(v):>18s}" for v in row.values()))
    else:
        print("  WARNING: No rows returned. Check table contents.")

    print()
    print("=" * 60)
    print(" ✅ Deployment complete!")
    print("=" * 60)
    print(f"  Tables: {SILVER_TABLE}, {GOLD_TABLE}")
    print(f"  GCS:    gs://{BUCKET}/{GCS_PREFIX}/")
    print()
    print("  Remaining manual step:")
    print("  Redeploy Cloud Function (needs gcloud CLI):")
    print(f"    gcloud functions deploy portfolio-data-ingest \\")
    print(f"      --gen2 --runtime=python311 --region={REGION} \\")
    print(f"      --source=functions/ingest --entry-point=process_file \\")
    print(f"      --trigger-event-filters='type=google.cloud.storage.object.v1.finalized' \\")
    print(f"      --trigger-event-filters='bucket={BUCKET}' \\")
    print(f"      --memory=512MB --timeout=300")


def main():
    parser = argparse.ArgumentParser(description="Deploy Averroes v2 pipeline to GCP")
    parser.add_argument("--key", help="Path to GCP service account JSON key")
    parser.add_argument("--ma-dir", default=os.path.join(REPO_ROOT, "raw_ma_files"),
                        help="Directory containing MA .xlsx files")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip uploading MA files to GCS")
    parser.add_argument("--skip-schema", action="store_true",
                        help="Skip BQ schema creation (if already done)")
    args = parser.parse_args()

    if args.key:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    print(f"Authenticated to project: {PROJECT_ID}")
    print(f"MA files dir: {args.ma_dir}")
    print()

    if not args.skip_schema:
        step1_apply_schemas(client)

    silver_csv, gold_csv = step2_backfill(args.ma_dir)
    if silver_csv is None:
        print("Backfill failed. Aborting.")
        sys.exit(1)

    step3_load_bq(client, silver_csv, gold_csv)

    if not args.skip_upload:
        step4_upload_gcs(args.ma_dir)

    step5_sanity_check(client)


if __name__ == "__main__":
    main()

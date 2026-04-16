"""
Cloud Function: portfolio-data-ingest

GCS-triggered. On every new file in gs://<bucket>/<portco_id>/...
  1. Parse Excel via parsers.alpha_parser (era-based router)
  2. Write raw KPI rows to bronze.raw_management_accounts
  3. Build silver (long, normalised, derived) via silver_gold_v2.build_silver_from_parsed
  4. Write silver rows to silver.kpi_long (per-file idempotent)
  5. Pivot silver → gold via silver_gold_v2.pivot_to_gold
  6. Upsert gold into gold.kpi_monthly_v2 (per-portco, per-period)
     (gold.kpi_monthly is a view pointed at v2, so dashboard sees the new data automatically)
"""
import os
import io
import functions_framework
import pandas as pd
from google.cloud import bigquery, storage

from parsers.alpha_parser import parse_alpha_ma
from silver_gold_v2 import build_silver_from_parsed, pivot_to_gold

PROJECT_ID = os.environ.get("GCP_PROJECT", "averroes-portfolio-intel")
BRONZE_TABLE = f"{PROJECT_ID}.bronze.raw_management_accounts"
SILVER_TABLE = f"{PROJECT_ID}.silver.kpi_long"
GOLD_TABLE   = f"{PROJECT_ID}.gold.kpi_monthly_v2"

bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)


@functions_framework.cloud_event
def process_file(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name   = data["name"]

    print(f"[ingest] processing gs://{bucket_name}/{file_name}")

    parts = file_name.split("/")
    if len(parts) < 2:
        print("[ingest] skipping: not in a portco_id/ folder")
        return
    portco_id = parts[0]

    # Only process MA files for now
    basename = os.path.basename(file_name)
    if "Management Accounts" not in basename and "MAfile" not in basename:
        print(f"[ingest] skipping non-MA file: {basename}")
        return

    blob = storage_client.bucket(bucket_name).blob(file_name)
    file_bytes = blob.download_as_bytes()

    # --- 1. Parse ---
    parsed_rows = parse_alpha_ma(file_bytes, basename)
    if not parsed_rows:
        print(f"[ingest] parser returned no rows for {basename}")
        return
    era = parsed_rows[0].get("era", "unknown")
    print(f"[ingest] parsed {len(parsed_rows)} rows (era={era})")

    # --- 2. Bronze ---
    write_bronze(parsed_rows, portco_id, basename)

    # --- 3 & 4. Silver ---
    silver_df = build_silver_from_parsed(parsed_rows, source_file=basename)
    silver_df.insert(0, "portco_id", portco_id)
    write_silver(silver_df, portco_id, basename)

    # --- 5 & 6. Gold ---
    gold_df = pivot_to_gold(silver_df, portco_id=portco_id)
    write_gold(gold_df, portco_id)

    print(f"[ingest] done: bronze={len(parsed_rows)} silver={len(silver_df)} gold={len(gold_df)}")


# ---------------------------------------------------------------------------
# BRONZE
# ---------------------------------------------------------------------------
def write_bronze(parsed_rows, portco_id, file_name):
    """Append parsed KPI rows to bronze, idempotent per file_name."""
    # Shape rows to bronze schema
    bronze_rows = []
    for r in parsed_rows:
        bronze_rows.append({
            "portco_id":        portco_id,
            "file_name":        file_name,
            "sheet_name":       r.get("sheet"),
            "reporting_period": _iso_date(r.get("period")),
            "row_label":        r.get("kpi"),
            "column_label":     r.get("value_type", "actual"),
            "value":            _safe_float(r.get("value")),
            "business_line":    r.get("business_line"),
            "era":              r.get("era"),
            "source_cell":      r.get("source_cell"),
        })

    # Idempotent: delete existing rows for this file before re-load
    _delete(f"DELETE FROM `{BRONZE_TABLE}` WHERE file_name = @f",
            [bigquery.ScalarQueryParameter("f", "STRING", file_name)])

    # Try to match existing table schema (preserve legacy columns)
    try:
        table_ref = bq_client.get_table(BRONZE_TABLE)
        existing_fields = {f.name for f in table_ref.schema}
        # Drop keys our row has but the table doesn't
        bronze_rows = [{k: v for k, v in row.items() if k in existing_fields}
                       for row in bronze_rows]
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=table_ref.schema,
            autodetect=False,
        )
    except Exception as e:
        print(f"[bronze] schema fetch failed, using autodetect: {e}")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,
        )

    bq_client.load_table_from_json(bronze_rows, BRONZE_TABLE, job_config=job_config).result()
    print(f"[bronze] wrote {len(bronze_rows)} rows to {BRONZE_TABLE}")


# ---------------------------------------------------------------------------
# SILVER  (silver.kpi_long)
# ---------------------------------------------------------------------------
def write_silver(silver_df: pd.DataFrame, portco_id: str, file_name: str):
    if silver_df.empty:
        print("[silver] empty, skip")
        return

    # Idempotent: delete rows from this source_file before re-load
    _delete(
        f"DELETE FROM `{SILVER_TABLE}` "
        f"WHERE portco_id = @p AND source_file = @f",
        [bigquery.ScalarQueryParameter("p", "STRING", portco_id),
         bigquery.ScalarQueryParameter("f", "STRING", file_name)],
    )

    # Also clear any derived rows for the periods this file covers (source_file='derived')
    periods = [p.strftime("%Y-%m-%d") for p in silver_df["period"].unique()]
    if periods:
        _delete(
            f"DELETE FROM `{SILVER_TABLE}` "
            f"WHERE portco_id = @p AND source_file = 'derived' "
            f"AND period IN UNNEST(@periods)",
            [bigquery.ScalarQueryParameter("p", "STRING", portco_id),
             bigquery.ArrayQueryParameter("periods", "DATE", periods)],
        )

    # Normalise dtypes
    df = silver_df.copy()
    df["period"] = pd.to_datetime(df["period"]).dt.date
    df["value"]  = df["value"].astype(float)

    try:
        table_ref = bq_client.get_table(SILVER_TABLE)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=table_ref.schema,
            autodetect=False,
        )
    except Exception as e:
        print(f"[silver] schema fetch failed, using autodetect: {e}")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,
        )

    bq_client.load_table_from_dataframe(df, SILVER_TABLE, job_config=job_config).result()
    print(f"[silver] wrote {len(df)} rows to {SILVER_TABLE}")


# ---------------------------------------------------------------------------
# GOLD  (gold.kpi_monthly_v2)
# ---------------------------------------------------------------------------
def write_gold(gold_df: pd.DataFrame, portco_id: str):
    if gold_df.empty:
        print("[gold] empty, skip")
        return

    periods = [p.strftime("%Y-%m-%d") for p in gold_df["period"].unique()]

    # Idempotent: delete the portco/period slice
    _delete(
        f"DELETE FROM `{GOLD_TABLE}` "
        f"WHERE portco_id = @p AND period IN UNNEST(@periods)",
        [bigquery.ScalarQueryParameter("p", "STRING", portco_id),
         bigquery.ArrayQueryParameter("periods", "DATE", periods)],
    )

    df = gold_df.copy()
    df["period"] = pd.to_datetime(df["period"]).dt.date

    try:
        table_ref = bq_client.get_table(GOLD_TABLE)
        schema = table_ref.schema
        allowed = {f.name for f in schema}
        df = df[[c for c in df.columns if c in allowed]]
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=schema,
            autodetect=False,
        )
    except Exception as e:
        print(f"[gold] schema fetch failed, using autodetect: {e}")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,
        )

    bq_client.load_table_from_dataframe(df, GOLD_TABLE, job_config=job_config).result()
    print(f"[gold] wrote {len(df)} rows to {GOLD_TABLE}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _delete(sql: str, params):
    try:
        bq_client.query(
            sql, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result()
    except Exception as e:
        # Streaming buffer lock or table-missing tolerated
        print(f"[delete] skipped ({e.__class__.__name__}): {e}")


def _iso_date(p):
    if p is None:
        return None
    if hasattr(p, "strftime"):
        return p.strftime("%Y-%m-%d")
    return str(p)


def _safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        if pd.isna(f):
            return None
        return f
    except Exception:
        return None

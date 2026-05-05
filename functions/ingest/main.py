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
import openpyxl
from google.cloud import bigquery, storage

from parsers.router import parse as route_parse
from parsers.common import period_from_filename
from silver_gold_v2 import build_silver_from_parsed, pivot_to_gold
from qa_checks import run_qa_checks

PROJECT_ID = os.environ.get("GCP_PROJECT", "averroes-portfolio-intel")
BRONZE_TABLE = f"{PROJECT_ID}.bronze.raw_management_accounts"
SILVER_TABLE = f"{PROJECT_ID}.silver.kpi_long"
GOLD_TABLE   = f"{PROJECT_ID}.gold.kpi_monthly_v2"
QA_TABLE     = f"{PROJECT_ID}.bronze.qa_results"

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

    basename = os.path.basename(file_name)

    # Only process Excel files
    if not basename.lower().endswith((".xlsx", ".xls", ".xlsm")):
        print(f"[ingest] skipping non-Excel file: {basename}")
        return

    # Skip temp/hidden files (Excel lock files, macOS resource forks)
    if basename.startswith(("~$", ".")):
        print(f"[ingest] skipping temp/hidden file: {basename}")
        return

    blob = storage_client.bucket(bucket_name).blob(file_name)
    file_bytes = blob.download_as_bytes()
    print(f"[ingest] downloaded {len(file_bytes):,} bytes from gs://{bucket_name}/{file_name}")

    # --- 1. Parse ---
    try:
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    except Exception as e:
        print(f"[ingest] ERROR opening workbook {basename}: {e.__class__.__name__}: {e}")
        import traceback; traceback.print_exc()
        return

    try:
        parsed_rows, era = route_parse(wb, file_name=basename)
        print(f"[ingest] parsed {len(parsed_rows)} rows (era={era})")
    except Exception as e:
        print(f"[ingest] ERROR parsing {basename}: {e.__class__.__name__}: {e}")
        import traceback; traceback.print_exc()
        wb.close()
        return

    if not parsed_rows:
        print(f"[ingest] parser returned no rows for {basename} — file may have an unrecognised layout")
        wb.close()
        return

    periods = sorted(set(r.get("period", "?") for r in parsed_rows))
    print(f"[ingest] periods={periods}")

    # --- 1b. QA Structure Checks (non-blocking) ---
    try:
        filename_period = period_from_filename(basename)
        qa_results = run_qa_checks(wb, era, parsed_rows, basename, portco_id, filename_period)
        write_qa_results(qa_results, portco_id)
    except Exception as e:
        print(f"[QA] ERROR (non-blocking): {e}")
    finally:
        wb.close()

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
# QA RESULTS  (bronze.qa_results)
# ---------------------------------------------------------------------------
def write_qa_results(qa_results, portco_id):
    """Write QA check results to BigQuery. Creates table if it doesn't exist."""
    if not qa_results:
        return

    # Ensure table exists (idempotent DDL)
    _ensure_qa_table()

    file_name = qa_results[0].get('file_name', '')

    # Idempotent: delete previous QA results for this file
    _delete(
        f"DELETE FROM `{QA_TABLE}` WHERE file_name = @f",
        [bigquery.ScalarQueryParameter("f", "STRING", file_name)],
    )

    # Shape rows to BQ schema
    rows = []
    for r in qa_results:
        rows.append({
            'qa_run_id':      r.get('qa_run_id'),
            'file_name':      r.get('file_name'),
            'portco_id':      r.get('portco_id', portco_id),
            'period':         _iso_date(r.get('period')),
            'era':            r.get('era'),
            'check_category': r.get('check_category'),
            'check_name':     r.get('check_name'),
            'severity':       r.get('severity'),
            'sheet':          r.get('sheet'),
            'cell':           r.get('cell'),
            'expected':       str(r.get('expected', ''))[:500],
            'actual':         str(r.get('actual', ''))[:500],
            'message':        str(r.get('message', ''))[:1000],
            'checked_at':     r.get('checked_at'),
        })

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    bq_client.load_table_from_json(rows, QA_TABLE, job_config=job_config).result()
    errors = sum(1 for r in qa_results if r['severity'] == 'error')
    warnings = sum(1 for r in qa_results if r['severity'] == 'warning')
    print(f"[QA] wrote {len(rows)} results to {QA_TABLE} ({errors} errors, {warnings} warnings)")


def _ensure_qa_table():
    """Create bronze.qa_results if it doesn't exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{QA_TABLE}` (
        qa_run_id STRING,
        file_name STRING,
        portco_id STRING,
        period DATE,
        era STRING,
        check_category STRING,
        check_name STRING,
        severity STRING,
        sheet STRING,
        cell STRING,
        expected STRING,
        actual STRING,
        message STRING,
        checked_at STRING
    )
    """
    try:
        bq_client.query(ddl).result()
    except Exception as e:
        print(f"[QA] DDL skipped: {e}")


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

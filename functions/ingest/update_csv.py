"""
update_csv.py — Refresh gold_phase1_data.csv from GCS.

Usage:
    python update_csv.py                   # pulls ALL MAfile*.xlsx from GCS bucket
    python update_csv.py --local path.xlsx # parse a single local file instead

The script:
  1. Lists every portco-*/MAfile*.xlsx blob in the GCS bucket
  2. Downloads and parses each one via phase1_parser.parse_ma_file()
  3. Writes the combined result to dashboard/gold_phase1_data.csv,
     deduplicating by (portco_id, period) and keeping the latest parse.

Authentication: uses Application Default Credentials (gcloud auth application-default login)
or GOOGLE_APPLICATION_CREDENTIALS env var.
"""

import argparse
import os
import sys
import pandas as pd
from phase1_parser import parse_ma_file

PROJECT_ID   = "averroes-portfolio-intel"
BUCKET_NAME  = f"{PROJECT_ID}-portfolio-data"

# Resolve paths relative to this script so it works from any cwd
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT   = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
CSV_OUT     = os.path.join(REPO_ROOT, "dashboard", "gold_phase1_data.csv")


def parse_from_gcs():
    """Download and parse all MAfile*.xlsx blobs from the GCS bucket."""
    from google.cloud import storage as gcs
    client  = gcs.Client(project=PROJECT_ID)
    bucket  = client.bucket(BUCKET_NAME)
    blobs   = list(client.list_blobs(BUCKET_NAME))
    ma_blobs = [b for b in blobs if "MAfile" in b.name and b.name.endswith(".xlsx")]

    if not ma_blobs:
        print(f"No MAfile*.xlsx objects found in gs://{BUCKET_NAME}/")
        return []

    rows = []
    for blob in sorted(ma_blobs, key=lambda b: b.name):
        parts     = blob.name.split("/")
        portco_id = parts[0] if len(parts) >= 2 else "portco-alpha"
        print(f"  Parsing gs://{BUCKET_NAME}/{blob.name}  [{portco_id}]")
        try:
            file_bytes = blob.download_as_bytes()
            parsed     = parse_ma_file(file_bytes, portco_id)
            rows.append(parsed)
        except Exception as e:
            print(f"  WARNING: failed to parse {blob.name}: {e}")

    return rows


def parse_local(file_path, portco_id="portco-alpha"):
    """Parse a single local MA file."""
    print(f"  Parsing local file: {file_path}  [{portco_id}]")
    return [parse_ma_file(file_path, portco_id)]


def build_and_save(rows):
    """Merge parsed rows into the CSV, dedup by (portco_id, period)."""
    if not rows:
        print("Nothing to write.")
        return

    df_new = pd.DataFrame(rows)
    df_new['period'] = pd.to_datetime(df_new['period']).dt.normalize()

    # Load existing CSV if present so we don't lose previously parsed rows
    if os.path.exists(CSV_OUT):
        df_existing = pd.read_csv(CSV_OUT)
        df_existing['period'] = pd.to_datetime(df_existing['period']).dt.normalize()
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    # Keep latest parse per portco+period
    if 'computed_at' in df_combined.columns:
        df_combined = (df_combined
                       .sort_values('computed_at')
                       .drop_duplicates(subset=['portco_id', 'period'], keep='last'))
    else:
        df_combined = df_combined.drop_duplicates(subset=['portco_id', 'period'], keep='last')

    df_combined = df_combined.sort_values(['portco_id', 'period']).reset_index(drop=True)
    df_combined.to_csv(CSV_OUT, index=False)

    periods = sorted(df_combined['period'].dt.strftime('%b %Y').unique())
    print(f"\nWrote {len(df_combined)} rows to {CSV_OUT}")
    print(f"Periods now in CSV: {', '.join(periods)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh gold_phase1_data.csv from GCS or a local file.")
    parser.add_argument("--local",    help="Path to a local MAfile*.xlsx to parse instead of GCS")
    parser.add_argument("--portco",   default="portco-alpha", help="portco_id for --local mode")
    args = parser.parse_args()

    if args.local:
        rows = parse_local(args.local, args.portco)
    else:
        print(f"Connecting to gs://{BUCKET_NAME} ...")
        rows = parse_from_gcs()

    build_and_save(rows)

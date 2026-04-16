"""
Backfill script — parses all 16 historical MA files and produces:
  - silver CSV (long-format, unit-normalized, derived metrics)
  - gold CSV   (wide-format, dashboard-ready)

Uses silver_gold_v2 as the single source of truth for derivation logic
(shared with the Cloud Function main.py).
"""
import os
import glob
import sys
import pandas as pd

HERE = os.path.dirname(__file__)
sys.path.insert(0, HERE)

from parsers.alpha_parser import parse_alpha_ma  # noqa: E402
from silver_gold_v2 import build_silver_from_parsed, pivot_to_gold  # noqa: E402


UPLOADS_DIR = "/sessions/dazzling-upbeat-hopper/mnt/uploads"
OUTPUT_DIR  = "/sessions/dazzling-upbeat-hopper/mnt/averroes-portfolio-intel/dashboard"
PORTCO_ID   = "portco-alpha"


def load_all_files():
    patterns = [os.path.join(UPLOADS_DIR, "*Management Accounts*.xlsx")]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    return sorted(set(files))


def build_silver_all():
    """Parse every MA file, build silver for each, then union and de-dup by era rank."""
    files = load_all_files()
    print(f"Parsing {len(files)} files...")
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
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # De-duplicate across files sharing a period: prefer the later era
    era_rank = {"era1": 1, "era2": 2, "era3": 3, "unknown": 0}
    df["_era_rank"] = df["era"].map(era_rank).fillna(0)
    df = df.sort_values(["period", "kpi", "value_type", "business_line", "_era_rank"])
    df = df.drop_duplicates(
        subset=["period", "kpi", "value_type", "business_line"], keep="last"
    ).drop(columns=["_era_rank"]).reset_index(drop=True)

    df = df.sort_values(["period", "kpi", "business_line", "value_type"]).reset_index(drop=True)
    return df


def main():
    silver = build_silver_all()
    silver_out = os.path.join(OUTPUT_DIR, "silver_kpi_long.csv")
    silver.to_csv(silver_out, index=False)
    print(f"\nSilver written: {silver_out}  ({len(silver)} rows)")

    gold = pivot_to_gold(silver, portco_id=PORTCO_ID)
    gold_out = os.path.join(OUTPUT_DIR, "gold_kpi_monthly.csv")
    gold.to_csv(gold_out, index=False)
    print(f"Gold written:   {gold_out}  ({len(gold)} rows)")

    print("\n=== Gold summary by period ===")
    cols = ["period", "era", "revenue_total_actual",
            "revenue_ecommerce_actual", "revenue_ems_actual", "revenue_services_actual",
            "tech_mrr_actual", "tech_arr", "ecommerce_arr", "ems_arr",
            "contribution_total", "modules_live_total", "total_headcount"]
    show = gold[cols].copy()
    show["period"] = show["period"].dt.strftime("%Y-%m")
    print(show.to_string(index=False))


if __name__ == "__main__":
    main()

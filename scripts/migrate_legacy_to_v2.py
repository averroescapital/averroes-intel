"""
migrate_legacy_to_v2.py — Compare gold.kpi_monthly (legacy) vs gold.kpi_monthly_v2
and backfill any missing data into v2.

Usage:
    python3 scripts/migrate_legacy_to_v2.py --dry-run    # audit only, no writes
    python3 scripts/migrate_legacy_to_v2.py              # actually backfill

After successful migration:
    bq rm -t averroes-portfolio-intel:gold.kpi_monthly

Authentication: gcloud auth application-default login
"""

import argparse
import sys
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "averroes-portfolio-intel"
LEGACY_TABLE = f"{PROJECT_ID}.gold.kpi_monthly"
V2_TABLE = f"{PROJECT_ID}.gold.kpi_monthly_v2"

# Column mapping: legacy name → v2 name
# Only columns where the name differs between schemas.
# Columns with identical names in both tables are auto-matched.
COLUMN_MAP = {
    # Legacy → V2
    "tech_mrr_live": "tech_mrr_actual",
    "tech_arr_live": "tech_arr",
    "ecommerce_arr_live": "ecommerce_arr",
    "ems_arr_live": "ems_arr",
    "total_revenue": "revenue_total_actual",
    "total_revenue_budget": "revenue_total_budget",
    "ecommerce_revenue": "revenue_ecommerce_actual",
    "ems_revenue": "revenue_ems_actual",
    "services_revenue": "revenue_services_actual",
    "adjusted_ebitda": "ebitda_actual",
    "adjusted_ebitda_budget": "ebitda_budget",
    "adjusted_ebitda_margin": "ebitda_margin_pct",
    "cash_burn": "cash_burn_monthly",
    "rule_of_40_score": "rule_of_40",
    "arpc": "arpc_actual",
    "gl_revenue_actual_cumulative": "gr_revenue_actual_ytd",
    "gl_revenue_covenant_cumulative": "gr_revenue_covenant_ytd",
    "gl_ebitda_actual_cumulative": "gr_ebitda_capex_actual_ytd",
    "gl_ebitda_covenant_cumulative": "gr_ebitda_capex_covenant_ytd",
}

# Columns in legacy that have NO equivalent in v2 — we'll report them but can't migrate.
# Most were never populated by the parser anyway.
UNMAPPABLE = {
    "total_mrr_actual", "total_arr", "carr", "implementation_backlog",
    "new_arr", "expansion_arr", "churn_arr", "net_new_arr", "acv",
    "nrr_pct", "grr_pct", "logo_churn_count", "logo_churn_pct",
    "top5_customer_pct", "nps_score", "cac", "cac_payback_months",
    "ltv", "ltv_cac_ratio",
    "modules_sold_total", "modules_sold_rooms", "modules_sold_tables",
    "modules_sold_spa", "modules_sold_retail", "modules_sold_events",
    "modules_sold_vouchers", "modules_live_rooms", "modules_live_tables",
    "modules_live_spa", "modules_live_retail", "modules_live_events",
    "modules_live_vouchers", "modules_sold_month", "modules_live_month",
    "modules_churn", "modules_sold_pre_vouchers", "modules_sold_post_vouchers",
    "modules_live_pre_vouchers", "modules_live_post_vouchers",
    "sold_per_month_pre_vouchers", "sold_per_month_post_vouchers",
    "live_per_month_pre_vouchers", "live_per_month_post_vouchers",
    "sold_rooms", "sold_tables", "sold_spa", "sold_retail", "sold_events",
    "sold_vouchers", "live_rooms", "live_tables", "live_spa", "live_retail",
    "live_events", "live_vouchers",
    "properties_sold", "properties_live", "properties_ecommerce",
    "properties_ems", "properties_services",
    "revenue_per_live_module", "cash_runway_months",
    "ar_current", "ar_30_days", "ar_60_days", "ar_90_plus_days", "ar_total",
    "gross_margin_total_pct", "gross_margin_tech_pct", "gross_margin_af_pct",
    "gross_margin_ecommerce_pct", "gross_margin_ems_pct", "gross_margin_services_pct",
    "gross_profit_total", "gross_profit_tech", "gross_profit_af",
    "gross_profit_ecommerce", "gross_profit_ems", "gross_profit_services",
    "contribution_margin_total_pct", "contribution_margin_tech_pct",
    "contribution_margin_af_pct", "contribution_margin_ecommerce_pct",
    "contribution_margin_ems_pct", "contribution_margin_services_pct",
    "ltm_revenue_total", "ltm_revenue_tech", "ltm_revenue_ecommerce",
    "ltm_revenue_ems", "ltm_revenue_services",
    "run_rate_revenue_total", "run_rate_revenue_tech", "run_rate_revenue_services",
    "ltm_ebitda", "run_rate_ebitda",
    "yoy_tech_monthly_growth", "yoy_services_monthly_growth",
    "yoy_total_ltm_growth", "yoy_tech_ltm_growth", "yoy_ecommerce_ltm_growth",
    "yoy_ems_ltm_growth", "yoy_services_ltm_growth", "yoy_tech_arr_growth",
    "yoy_ecommerce_arr_growth", "yoy_sold_modules_growth", "yoy_live_modules_growth",
    "yoy_properties_growth", "yoy_ltm_ebitda_growth",
    "tech_revenue", "international_revenue", "tech1_revenue",
    "onejourney_revenue", "gifted_revenue", "af_revenue",
    "overhead_ratio", "pat", "pat_margin",
    "ytd_revenue", "ytd_revenue_budget", "ytd_revenue_growth",
    "ytg_revenue_vs_budget", "ltm_vs_budget_variation",
    "ytg_ebitda",
    "tech_gross_margin_month", "ebitda_margin_month",
    "averroes_revenue_rag", "averroes_ebitda_rag",
    "gl_revenue_headroom_pct", "gl_revenue_breach",
    "gl_ebitda_headroom_pct", "gl_ebitda_breach",
    "direct_costs_total", "total_customers",
    "total_group_revenue", "revenue_vs_budget_variance",
    "contribution_total", "contribution_tech", "contribution_af",
    "sm_efficiency", "payroll_pct_revenue", "revenue_per_employee",
    "free_cash_conversion",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Audit only — don't write to v2")
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT_ID)

    # ── Step 1: Check legacy table exists ──
    try:
        legacy_info = client.get_table(LEGACY_TABLE)
        print(f"Legacy table: {legacy_info.num_rows:,} rows, "
              f"{len(legacy_info.schema)} columns")
    except Exception as e:
        print(f"Legacy table not found: {e}")
        print("Nothing to migrate. You're clean.")
        return

    # ── Step 2: Load both tables ──
    print(f"\nLoading legacy ({LEGACY_TABLE}) ...")
    df_legacy = client.query(f"SELECT * FROM `{LEGACY_TABLE}`").to_dataframe()
    print(f"  → {len(df_legacy)} rows, {len(df_legacy.columns)} columns")

    print(f"Loading v2 ({V2_TABLE}) ...")
    df_v2 = client.query(f"SELECT * FROM `{V2_TABLE}`").to_dataframe()
    print(f"  → {len(df_v2)} rows, {len(df_v2.columns)} columns")

    if df_legacy.empty:
        print("\nLegacy table is empty. Safe to DROP.")
        return

    # Normalise periods
    df_legacy["period"] = pd.to_datetime(df_legacy["period"]).dt.normalize()
    if not df_v2.empty:
        df_v2["period"] = pd.to_datetime(df_v2["period"]).dt.normalize()

    # ── Step 3: Compare period coverage ──
    legacy_keys = set(zip(df_legacy["portco_id"], df_legacy["period"]))
    v2_keys = set(zip(df_v2["portco_id"], df_v2["period"])) if not df_v2.empty else set()

    only_in_legacy = legacy_keys - v2_keys
    only_in_v2 = v2_keys - legacy_keys
    in_both = legacy_keys & v2_keys

    print(f"\n── Period Coverage ──")
    print(f"  Legacy only: {len(only_in_legacy)} rows")
    print(f"  V2 only:     {len(only_in_v2)} rows")
    print(f"  In both:     {len(in_both)} rows")

    if only_in_legacy:
        periods = sorted(set(p for _, p in only_in_legacy))
        print(f"\n  Legacy-only periods ({len(periods)}):")
        for p in periods:
            portcos = [pc for pc, pp in only_in_legacy if pp == p]
            print(f"    {p.strftime('%Y-%m-%d')} — {', '.join(portcos)}")

    # ── Step 4: Check for non-null data in unmappable columns ──
    print(f"\n── Unmappable columns (legacy-only, no v2 equivalent) ──")
    populated_unmappable = {}
    for col in sorted(UNMAPPABLE):
        if col in df_legacy.columns:
            non_null = df_legacy[col].notna().sum()
            if non_null > 0:
                populated_unmappable[col] = non_null
    if populated_unmappable:
        print(f"  {len(populated_unmappable)} columns have data that CANNOT be migrated:")
        for col, count in sorted(populated_unmappable.items(), key=lambda x: -x[1]):
            sample = df_legacy[col].dropna().head(3).tolist()
            print(f"    {col}: {count} non-null rows (sample: {sample})")
    else:
        print("  None have data — nothing lost.")

    # ── Step 5: Build rows to backfill (legacy-only periods) ──
    if not only_in_legacy:
        print(f"\n✅ No legacy-only rows to migrate. V2 has full coverage.")
    else:
        print(f"\n── Building backfill for {len(only_in_legacy)} legacy-only rows ──")

        # Get v2 column names
        v2_cols = set(df_v2.columns) if not df_v2.empty else {f.name for f in client.get_table(V2_TABLE).schema}

        # Filter legacy to only-in-legacy rows
        mask = df_legacy.apply(lambda r: (r["portco_id"], r["period"]) in only_in_legacy, axis=1)
        df_migrate = df_legacy[mask].copy()

        # Rename columns per mapping
        rename = {k: v for k, v in COLUMN_MAP.items() if k in df_migrate.columns}
        df_migrate = df_migrate.rename(columns=rename)

        # Keep only columns that exist in v2
        keep = [c for c in df_migrate.columns if c in v2_cols]
        drop = [c for c in df_migrate.columns if c not in v2_cols and c not in UNMAPPABLE]
        df_migrate = df_migrate[keep]

        if drop:
            print(f"  Dropped {len(drop)} columns not in v2 schema: {drop[:10]}...")

        # Add required metadata
        if "era" not in df_migrate.columns or df_migrate["era"].isna().all():
            df_migrate["era"] = "legacy"
        if "data_source" not in df_migrate.columns:
            df_migrate["data_source"] = "legacy_migration"

        # Normalise dtypes
        df_migrate["period"] = df_migrate["period"].dt.date

        non_null_cols = [c for c in df_migrate.columns
                         if c not in ("portco_id", "period", "era", "data_source", "currency", "computed_at")
                         and df_migrate[c].notna().any()]
        print(f"  {len(df_migrate)} rows, {len(non_null_cols)} columns with data")
        print(f"  Columns with data: {', '.join(sorted(non_null_cols)[:20])}...")

        if args.dry_run:
            print(f"\n🔍 DRY RUN — would insert {len(df_migrate)} rows into {V2_TABLE}")
            print("  Run without --dry-run to execute.")
        else:
            print(f"\n  Inserting {len(df_migrate)} rows into {V2_TABLE} ...")
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[f for f in client.get_table(V2_TABLE).schema],
                autodetect=False,
            )
            client.load_table_from_dataframe(df_migrate, V2_TABLE, job_config=job_config).result()
            print(f"  ✅ Inserted {len(df_migrate)} rows successfully.")

    # ── Step 6: Check overlapping rows for missing data ──
    if in_both:
        print(f"\n── Checking {len(in_both)} overlapping rows for data gaps ──")
        v2_col_set = set(df_v2.columns)
        gaps_found = 0
        for col_legacy, col_v2 in COLUMN_MAP.items():
            if col_legacy not in df_legacy.columns or col_v2 not in v2_col_set:
                continue
            # For overlapping rows, check if legacy has data where v2 is null
            for portco, period in list(in_both)[:5]:  # sample check
                leg_val = df_legacy[(df_legacy["portco_id"] == portco) &
                                    (df_legacy["period"] == period)][col_legacy].values
                v2_val = df_v2[(df_v2["portco_id"] == portco) &
                               (df_v2["period"] == period)][col_v2].values
                if len(leg_val) and len(v2_val):
                    if pd.notna(leg_val[0]) and pd.isna(v2_val[0]):
                        gaps_found += 1
                        if gaps_found <= 10:
                            print(f"  GAP: {portco} {period.strftime('%Y-%m')} "
                                  f"{col_legacy}={leg_val[0]} but {col_v2}=NULL in v2")
        if gaps_found == 0:
            print("  No gaps found — v2 has equal or better data for all overlapping rows.")
        else:
            print(f"\n  ⚠️  Found {gaps_found} gap(s) in overlapping rows.")
            print("  These are likely from KPITracker historical ingest.")
            print("  Consider running a targeted UPDATE to fill these.")

    # ── Summary ──
    print(f"\n{'='*60}")
    print(f"MIGRATION SUMMARY")
    print(f"{'='*60}")
    if only_in_legacy:
        if args.dry_run:
            print(f"  ⬜ {len(only_in_legacy)} legacy-only rows ready to migrate (run without --dry-run)")
        else:
            print(f"  ✅ {len(only_in_legacy)} legacy-only rows migrated to v2")
    else:
        print(f"  ✅ No legacy-only rows — v2 has full period coverage")
    if populated_unmappable:
        print(f"  ⚠️  {len(populated_unmappable)} unmappable columns have data (see above)")
        print(f"     These are legacy-only metrics not in the v2 schema.")
        print(f"     If needed, add columns to v2 via ALTER TABLE.")
    else:
        print(f"  ✅ No data loss from unmappable columns")
    print()
    if not args.dry_run and not only_in_legacy:
        print(f"  Safe to DROP legacy table:")
        print(f"    bq rm -t {PROJECT_ID}:gold.kpi_monthly")
    elif not args.dry_run:
        print(f"  Re-run with --dry-run to verify, then DROP:")
        print(f"    bq rm -t {PROJECT_ID}:gold.kpi_monthly")


if __name__ == "__main__":
    main()

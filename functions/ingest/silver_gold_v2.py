"""
Shared silver+gold transform logic used by both the backfill CLI and the Cloud Function.

Input: parsed KPI rows (list of dicts) from parsers.alpha_parser.parse_alpha_ma
Output:
  - silver_df:  long-format, unit-normalized, with derived metrics
  - gold_df:    wide-format, one row per period

Unit conventions:
  - Era 1/2 Tech MRR : £k (as stored in source)
  - Era 3 Tech MRR   : raw £ → normalised to £k here (÷1000)
  - Revenue, DC, EBITDA, cash, NWC : £k
  - Headcount : persons
"""
import pandas as pd

RAW_POUND_KPIS = {"TECH_MRR", "LTM_TECH_MRR", "SERVICES_MRR", "LTM_SERVICES_MRR"}


def normalize_unit(kpi: str, value: float, era: str) -> float:
    """Normalize MRR to £k (Era 3 source is raw £)."""
    if kpi in RAW_POUND_KPIS and era == "era3":
        return value / 1000.0
    return value


def build_silver_from_parsed(parsed_rows: list, source_file: str) -> pd.DataFrame:
    """Turn parser output into a normalised silver DataFrame with derived metrics."""
    if not parsed_rows:
        return pd.DataFrame()

    era = parsed_rows[0].get("era", "unknown")
    rows = []
    for r in parsed_rows:
        v = r.get("value")
        if v is None:
            continue
        kpi = r["kpi"]
        v_norm = normalize_unit(kpi, v, era)
        rows.append({
            "period":        r["period"],
            "kpi":           kpi,
            "value":         v_norm,
            "value_type":    r.get("value_type", "actual"),
            "business_line": r.get("business_line"),
            "era":           era,
            "source_file":   source_file,
            "source_sheet":  r.get("sheet"),
            "source_cell":   r.get("source_cell"),
        })

    df = pd.DataFrame(rows)
    df["period"] = pd.to_datetime(df["period"]).dt.normalize()
    df = df.sort_values(["period", "kpi", "value_type"]).reset_index(drop=True)
    df = df.drop_duplicates(
        subset=["period", "kpi", "value_type", "business_line"], keep="last"
    ).reset_index(drop=True)

    # --- Derived metrics per period ---
    derived_rows = []
    for period, g in df[df["value_type"] == "actual"].groupby("period"):
        lookup = {(r["kpi"], r["business_line"]): r["value"] for _, r in g.iterrows()}
        era_for_period = g["era"].iloc[0] if len(g) else "unknown"

        def get(kpi, bl=None):
            return lookup.get((kpi, bl))

        # Ecommerce MRR = Success Fees + Payment Fees (excl SetUp)
        ecom_success = get("REVENUE_ECOM_SUCCESS_FEES", "ecommerce")
        ecom_payment = get("REVENUE_ECOM_PAYMENT_FEES", "ecommerce")
        if ecom_success is not None and ecom_payment is not None:
            ecom_mrr = ecom_success + ecom_payment
            derived_rows.append({"period": period, "kpi": "ECOMMERCE_MRR",
                                 "value": ecom_mrr, "value_type": "actual",
                                 "business_line": "ecommerce", "era": era_for_period,
                                 "source_file": "derived", "source_sheet": "derived",
                                 "source_cell": "SUCCESS_FEES + PAYMENT_FEES"})
            derived_rows.append({"period": period, "kpi": "ECOMMERCE_ARR",
                                 "value": ecom_mrr * 12, "value_type": "actual",
                                 "business_line": "ecommerce", "era": era_for_period,
                                 "source_file": "derived", "source_sheet": "derived",
                                 "source_cell": "ECOMMERCE_MRR * 12"})

        # EMS MRR = back-solved from authoritative Tech MRR to keep identity
        tech_mrr_for_split = get("TECH_MRR", None)
        if tech_mrr_for_split is not None and ecom_success is not None and ecom_payment is not None:
            ems_mrr = tech_mrr_for_split - (ecom_success + ecom_payment)
            derived_rows.append({"period": period, "kpi": "EMS_MRR",
                                 "value": ems_mrr, "value_type": "actual",
                                 "business_line": "ems", "era": era_for_period,
                                 "source_file": "derived", "source_sheet": "derived",
                                 "source_cell": "TECH_MRR - ECOMMERCE_MRR"})
            derived_rows.append({"period": period, "kpi": "EMS_ARR",
                                 "value": ems_mrr * 12, "value_type": "actual",
                                 "business_line": "ems", "era": era_for_period,
                                 "source_file": "derived", "source_sheet": "derived",
                                 "source_cell": "EMS_MRR * 12"})

        # Preserve raw EMS Subscription line for reference
        ems_sub = get("REVENUE_EMS_SUBSCRIPTION", "ems")
        if ems_sub is not None:
            derived_rows.append({"period": period, "kpi": "EMS_SUBSCRIPTION_REVENUE",
                                 "value": ems_sub, "value_type": "actual",
                                 "business_line": "ems", "era": era_for_period,
                                 "source_file": "derived", "source_sheet": "derived",
                                 "source_cell": "REVENUE_EMS_SUBSCRIPTION"})

        # Tech ARR = Tech MRR * 12
        if tech_mrr_for_split is not None:
            derived_rows.append({"period": period, "kpi": "TECH_ARR",
                                 "value": tech_mrr_for_split * 12, "value_type": "actual",
                                 "business_line": None, "era": era_for_period,
                                 "source_file": "derived", "source_sheet": "derived",
                                 "source_cell": "TECH_MRR * 12"})

        # Services ARR = Services MRR * 12
        # Silver stores all MRR values in £k (normalize_unit converts era3 raw £ → £k).
        # For era1/era2, SERVICES_MRR doesn't exist in the MA files.
        # Derive it from REVENUE_SERVICES (£k) using the observed ratio (0.765)
        # from era3 months where both are available.
        # MRR captures recurring revenue only; ~76.5% of total services revenue.
        SERVICES_MRR_TO_REVENUE_RATIO = 0.765
        services_mrr = get("SERVICES_MRR", None)
        if services_mrr is None:
            # Fallback: estimate from services revenue (already in £k)
            services_rev_k = get("REVENUE_SERVICES", "services")
            if services_rev_k is not None:
                services_mrr = services_rev_k * SERVICES_MRR_TO_REVENUE_RATIO
                derived_rows.append({"period": period, "kpi": "SERVICES_MRR",
                                     "value": services_mrr, "value_type": "actual",
                                     "business_line": "services", "era": era_for_period,
                                     "source_file": "derived", "source_sheet": "derived",
                                     "source_cell": f"REVENUE_SERVICES * {SERVICES_MRR_TO_REVENUE_RATIO}"})
        if services_mrr is not None:
            derived_rows.append({"period": period, "kpi": "SERVICES_ARR",
                                 "value": services_mrr * 12, "value_type": "actual",
                                 "business_line": "services", "era": era_for_period,
                                 "source_file": "derived", "source_sheet": "derived",
                                 "source_cell": "SERVICES_MRR * 12"})

    if derived_rows:
        df_derived = pd.DataFrame(derived_rows)
        df_derived["period"] = pd.to_datetime(df_derived["period"]).dt.normalize()
        df = pd.concat([df, df_derived], ignore_index=True)

    df = df.sort_values(["period", "kpi", "business_line", "value_type"]).reset_index(drop=True)
    return df


def _div_k(v):
    """Convert raw £ to £k (divide by 1000). Returns None if input is None."""
    return round(v / 1000.0, 5) if v is not None else None


def pivot_to_gold(silver: pd.DataFrame, portco_id: str = "portco-alpha") -> pd.DataFrame:
    """Pivot long silver → wide gold. FY runs Nov→Oct."""
    if silver.empty:
        return pd.DataFrame()

    periods = sorted(silver["period"].unique())
    gold_rows = []

    for period in periods:
        p = silver[silver["period"] == period]
        era = p["era"].iloc[0] if len(p) else "unknown"

        def pick(kpi, value_type="actual", business_line=None):
            q = p[(p["kpi"] == kpi) & (p["value_type"] == value_type)]
            if business_line is not None:
                q = q[q["business_line"] == business_line]
            if len(q) == 0:
                return None
            return float(q["value"].iloc[0])

        ts = pd.Timestamp(period)
        month = ts.month
        # FY runs Nov-Oct
        if month >= 11:
            fy = ts.year + 1
            fy_month = month - 10
        else:
            fy = ts.year
            fy_month = month + 2
        fy_quarter = f"Q{((fy_month - 1) // 3) + 1}"

        row = {
            "portco_id": portco_id,
            "period": ts.strftime("%Y-%m-%d"),
            "fy": f"FY{fy % 100:02d}",
            "fy_quarter": fy_quarter,
            "fy_month_num": fy_month,

            "tech_mrr_actual":     pick("TECH_MRR", "actual"),
            "tech_mrr_budget":     pick("TECH_MRR", "budget"),
            "tech_mrr_prior_year": pick("TECH_MRR", "prior_year"),
            "tech_mrr_ytd_actual": pick("LTM_TECH_MRR", "actual"),

            "services_mrr_actual":     pick("SERVICES_MRR", "actual"),
            "services_mrr_budget":     pick("SERVICES_MRR", "budget"),
            "services_mrr_prior_year": pick("SERVICES_MRR", "prior_year"),
            "services_mrr_ytd_actual": pick("LTM_SERVICES_MRR", "actual"),

            "ecommerce_mrr_actual": pick("ECOMMERCE_MRR", "actual", "ecommerce"),
            "ems_mrr_actual":       pick("EMS_MRR", "actual", "ems"),
            "tech_arr":             pick("TECH_ARR", "actual"),
            "ecommerce_arr":        pick("ECOMMERCE_ARR", "actual", "ecommerce"),
            "ems_arr":              pick("EMS_ARR", "actual", "ems"),
            "services_arr":         pick("SERVICES_ARR", "actual", "services"),

            "revenue_ecommerce_actual":     pick("REVENUE_ECOMMERCE", "actual", "ecommerce"),
            "revenue_ecommerce_budget":     pick("REVENUE_ECOMMERCE", "budget", "ecommerce"),
            "revenue_ecommerce_prior_year": pick("REVENUE_ECOMMERCE", "prior_year", "ecommerce"),
            "revenue_ems_actual":           pick("REVENUE_EMS", "actual", "ems"),
            "revenue_ems_budget":           pick("REVENUE_EMS", "budget", "ems"),
            "revenue_ems_prior_year":       pick("REVENUE_EMS", "prior_year", "ems"),
            "revenue_services_actual":      pick("REVENUE_SERVICES", "actual", "services"),
            "revenue_services_budget":      pick("REVENUE_SERVICES", "budget", "services"),
            "revenue_services_prior_year":  pick("REVENUE_SERVICES", "prior_year", "services"),
            "revenue_total_actual":         pick("REVENUE_TOTAL", "actual", "total"),
            "revenue_total_budget":         pick("REVENUE_TOTAL", "budget", "total"),
            "revenue_total_prior_year":     pick("REVENUE_TOTAL", "prior_year", "total"),
            "revenue_total_ytd_actual":     pick("REVENUE_TOTAL", "ytd_actual", "total"),
            "revenue_total_ytd_budget":     pick("REVENUE_TOTAL", "ytd_budget", "total"),

            "revenue_ecom_success_fees": pick("REVENUE_ECOM_SUCCESS_FEES", "actual", "ecommerce"),
            "revenue_ecom_setup_fees":   pick("REVENUE_ECOM_SETUP_FEES", "actual", "ecommerce"),
            "revenue_ecom_payment_fees": pick("REVENUE_ECOM_PAYMENT_FEES", "actual", "ecommerce"),

            "revenue_ems_subscription": pick("REVENUE_EMS_SUBSCRIPTION", "actual", "ems"),
            "revenue_ems_setup":        pick("REVENUE_EMS_SETUP", "actual", "ems"),
            "revenue_ems_hardware":     pick("REVENUE_EMS_HARDWARE", "actual", "ems"),

            "contribution_ecommerce":       pick("DIRECT_CONTRIBUTION_ECOMMERCE", "actual", "ecommerce"),
            "contribution_ecommerce_budget":pick("DIRECT_CONTRIBUTION_ECOMMERCE", "budget", "ecommerce"),
            "contribution_ecommerce_py":    pick("DIRECT_CONTRIBUTION_ECOMMERCE", "prior_year", "ecommerce"),
            "contribution_ems":             pick("DIRECT_CONTRIBUTION_EMS", "actual", "ems"),
            "contribution_ems_budget":      pick("DIRECT_CONTRIBUTION_EMS", "budget", "ems"),
            "contribution_ems_py":          pick("DIRECT_CONTRIBUTION_EMS", "prior_year", "ems"),
            "contribution_services":        pick("DIRECT_CONTRIBUTION_SERVICES", "actual", "services"),
            "contribution_services_budget": pick("DIRECT_CONTRIBUTION_SERVICES", "budget", "services"),
            "contribution_services_py":     pick("DIRECT_CONTRIBUTION_SERVICES", "prior_year", "services"),
            "contribution_total":           pick("DIRECT_CONTRIBUTION_TOTAL", "actual", "total"),
            "contribution_total_budget":    pick("DIRECT_CONTRIBUTION_TOTAL", "budget", "total"),
            "contribution_total_py":        pick("DIRECT_CONTRIBUTION_TOTAL", "prior_year", "total"),

            "direct_costs_ecommerce":       pick("DIRECT_COSTS_ECOMMERCE", "actual", "ecommerce"),
            "direct_costs_ecommerce_budget":pick("DIRECT_COSTS_ECOMMERCE", "budget", "ecommerce"),
            "direct_costs_ems":             pick("DIRECT_COSTS_EMS", "actual", "ems"),
            "direct_costs_ems_budget":      pick("DIRECT_COSTS_EMS", "budget", "ems"),
            "direct_costs_services":        pick("DIRECT_COSTS_SERVICES", "actual", "services"),
            "direct_costs_services_budget": pick("DIRECT_COSTS_SERVICES", "budget", "services"),

            "total_overheads":        pick("TOTAL_OVERHEADS", "actual"),
            "total_overheads_budget": pick("TOTAL_OVERHEADS", "budget"),
            "total_overheads_py":     pick("TOTAL_OVERHEADS", "prior_year"),
            "ebitda_actual":          pick("EBITDA", "actual"),
            "ebitda_budget":          pick("EBITDA", "budget"),
            "ebitda_prior_year":      pick("EBITDA", "prior_year"),
            "ebitda_less_capex":      pick("EBITDA_LESS_CAPEX", "actual"),
            "ebitda_less_capex_budget": pick("EBITDA_LESS_CAPEX", "budget"),

            # YTD from P&L Summary (era3)
            "revenue_total_ytd_py":          pick("REVENUE_TOTAL", "ytd_prior_year", "total"),
            "ebitda_ytd_actual":             pick("EBITDA", "ytd_actual"),
            "ebitda_ytd_budget":             pick("EBITDA", "ytd_budget"),
            "ebitda_ytd_py":                 pick("EBITDA", "ytd_prior_year"),
            "contribution_total_ytd_actual": pick("DIRECT_CONTRIBUTION_TOTAL", "ytd_actual", "total"),
            "contribution_total_ytd_budget": pick("DIRECT_CONTRIBUTION_TOTAL", "ytd_budget", "total"),

            "cash_balance":           pick("CASH_ON_HAND", "actual"),
            "cash_balance_bs_budget": pick("CASH_ON_HAND", "budget"),
            "cash_balance_bs_prior":  pick("CASH_ON_HAND", "prior_year"),
            "net_working_capital":    pick("NET_WORKING_CAPITAL", "actual"),
            "nwc_budget":             pick("NET_WORKING_CAPITAL", "budget"),
            "nwc_prior_month":        pick("NET_WORKING_CAPITAL", "prior_year"),
            "net_debt":               pick("NET_DEBT", "actual"),
            "cash_burn_monthly":      pick("CASH_BURN", "actual"),
            "capex":                  pick("CAPEX", "actual"),
            "capex_budget":           pick("CAPEX", "budget"),

            "total_headcount":     pick("HEADCOUNT_TOTAL", "actual"),
            "headcount_budget":    pick("HEADCOUNT_TOTAL", "budget"),
            "headcount_prior_year":pick("HEADCOUNT_TOTAL", "prior_year"),
            "headcount_ecommerce": pick("HEADCOUNT_ECOMMERCE", "actual", "ecommerce"),
            "headcount_ems":       pick("HEADCOUNT_EMS", "actual", "ems"),
            "headcount_services":  pick("HEADCOUNT_SERVICES", "actual", "services"),
            "headcount_central":   pick("HEADCOUNT_CENTRAL", "actual", "central"),
            "gross_payroll":       pick("GROSS_PAYROLL", "actual"),
            "gross_payroll_budget":pick("GROSS_PAYROLL", "budget"),
            "revenue_per_employee":pick("REVENUE_PER_EMPLOYEE", "actual"),

            "modules_live_total":     pick("MODULES_LIVE_TOTAL", "actual", "total"),
            "modules_live_ecommerce": pick("MODULES_LIVE_ECOMMERCE", "actual", "ecommerce"),
            "modules_live_ems":       pick("MODULES_LIVE_EMS",       "actual", "ems"),
            "modules_live_services":  pick("MODULES_LIVE_SERVICES",  "actual", "services"),
            "modules_pipeline":       pick("MODULES_PIPELINE", "actual"),

            "arpc_actual":           pick("ARPC", "actual"),
            "arpc_budget":           pick("ARPC", "budget"),
            "arpc_ytd_actual":       pick("ARPC_YTD", "actual"),
            "arpc_ytd_budget":       pick("ARPC_YTD", "budget"),

            "tech_gross_margin_pct":        pick("TECH_GROSS_MARGIN", "actual"),
            "tech_gross_margin_budget_pct":  pick("TECH_GROSS_MARGIN", "budget"),
            "tech_gross_margin_prior_pct":   pick("TECH_GROSS_MARGIN", "prior_year"),
            "tech_gross_margin_ytd_pct":     pick("TECH_GROSS_MARGIN_YTD", "actual"),
            "tech_gross_margin_ytd_budget_pct": pick("TECH_GROSS_MARGIN_YTD", "budget"),

            "ebitda_margin_pct":            pick("EBITDA_MARGIN", "actual"),
            "ebitda_margin_budget_pct":     pick("EBITDA_MARGIN", "budget"),
            "ebitda_margin_prior_pct":      pick("EBITDA_MARGIN", "prior_year"),
            "ebitda_margin_ytd_pct":        pick("EBITDA_MARGIN_YTD", "actual"),
            "ebitda_margin_ytd_budget_pct": pick("EBITDA_MARGIN_YTD", "budget"),

            # CASH_FIN_KPI is in raw £ from Financial KPIs; convert to £k
            "cash_balance_budget":         _div_k(pick("CASH_FIN_KPI", "budget")),
            "cash_balance_prior_month":    _div_k(pick("CASH_FIN_KPI", "prior_year")),

            "free_cash_conversion_month":  pick("FREE_CASH_CONVERSION", "actual"),
            "free_cash_conversion_budget": pick("FREE_CASH_CONVERSION", "budget"),
            "free_cash_conversion_ytd":    pick("FREE_CASH_CONVERSION_YTD", "actual"),

            "indicative_ev":             _div_k(pick("INDICATIVE_EV", "actual")),
            "sm_efficiency":             pick("SM_EFFICIENCY", "actual"),
            "sm_cost_ytd":               pick("SM_COST_YTD", "actual"),
            "tcv_ytd":                   pick("TCV_YTD", "actual"),
            "ytd_revenue_growth_pct":    pick("YTD_REVENUE_GROWTH", "actual"),
            "ytd_revenue_growth_budget": pick("YTD_REVENUE_GROWTH", "budget"),
            "time_to_value_days":        pick("TIME_TO_VALUE", "actual"),
            "time_to_value_excl_blocked": pick("TIME_TO_VALUE_EXCL_BLOCKED", "actual"),

            "rule_of_40":            pick("RULE_OF_40", "actual"),
            "revenue_churn_pct":     pick("REVENUE_CHURN", "actual"),
            "revenue_churn_target":  pick("REVENUE_CHURN", "budget"),

            # GL Covenants (Era 2+)
            "gl_arr_actual":           pick("GL_ARR_ACTUAL", "actual"),
            "gl_arr_covenant":         pick("GL_ARR_COVENANT", "actual"),
            "gl_arr_ratio":            pick("GL_ARR_RATIO", "actual"),
            "gl_arr_threshold":        pick("GL_ARR_THRESHOLD", "actual"),
            "gl_interest_cover_ratio": pick("GL_INTEREST_COVER_RATIO", "actual"),
            "gl_debt_service_ratio":   pick("GL_DEBT_SERVICE_RATIO", "actual"),
            "gl_cash_min_balance":     pick("GL_CASH_MIN_BALANCE", "actual"),

            # Averroes Guard Rails (Era 2+)
            "gr_revenue_actual_ytd":        pick("GR_REVENUE_ACTUAL_YTD", "actual"),
            "gr_revenue_covenant_ytd":      pick("GR_REVENUE_COVENANT_YTD", "actual"),
            "gr_revenue_ratio":             pick("GR_REVENUE_RATIO", "actual"),
            "gr_mrr_actual":                pick("GR_MRR_ACTUAL", "actual"),
            "gr_mrr_covenant":              pick("GR_MRR_COVENANT", "actual"),
            "gr_mrr_ratio":                 pick("GR_MRR_RATIO", "actual"),
            "gr_contribution_actual_ytd":   pick("GR_CONTRIBUTION_ACTUAL_YTD", "actual"),
            "gr_contribution_covenant_ytd": pick("GR_CONTRIBUTION_COVENANT_YTD", "actual"),
            "gr_contribution_ratio":        pick("GR_CONTRIBUTION_RATIO", "actual"),
            "gr_ebitda_capex_actual_ytd":   pick("GR_EBITDA_CAPEX_ACTUAL_YTD", "actual"),
            "gr_ebitda_capex_covenant_ytd": pick("GR_EBITDA_CAPEX_COVENANT_YTD", "actual"),
            "gr_ebitda_capex_ratio":        pick("GR_EBITDA_CAPEX_RATIO", "actual"),
            "gr_cash_actual":               pick("GR_CASH_ACTUAL", "actual"),
            "gr_cash_covenant":             pick("GR_CASH_COVENANT", "actual"),
            "gr_cash_ratio":                pick("GR_CASH_RATIO", "actual"),

            # Revenue Waterfall (Era 3)
            "wf_revenue_start":       pick("WF_REVENUE_START", "actual"),
            "wf_one_off_prev":        pick("WF_ONE_OFF_PREV", "actual"),
            "wf_one_off_ytd":         pick("WF_ONE_OFF_YTD", "actual"),
            "wf_recurring_growth":    pick("WF_RECURRING_GROWTH", "actual"),
            "wf_arr_ytg":             pick("WF_ARR_TO_GO", "actual"),
            "wf_weighted_pipeline":   pick("WF_WEIGHTED_PIPELINE", "actual"),
            "wf_budget_assumptions":  pick("WF_BUDGET_ASSUMPTIONS", "actual"),
            "wf_revenue_gap":         pick("WF_REVENUE_GAP", "actual"),
            "wf_revenue_end":         pick("WF_REVENUE_END", "actual"),

            "currency":    "GBP",
            "data_source": f"ma_parser:{era}",
            "era":         era,
            "computed_at": pd.Timestamp.utcnow().isoformat(),
        }

        # Derived pct metrics
        if row["revenue_total_actual"] and row["revenue_total_prior_year"]:
            row["revenue_yoy_growth_pct"] = round(
                (row["revenue_total_actual"] / row["revenue_total_prior_year"] - 1) * 100, 2)
        else:
            row["revenue_yoy_growth_pct"] = None
        if row["revenue_total_actual"] and row["revenue_total_budget"]:
            row["revenue_vs_budget_pct"] = round(
                (row["revenue_total_actual"] / row["revenue_total_budget"] - 1) * 100, 2)
        else:
            row["revenue_vs_budget_pct"] = None

        gold_rows.append(row)

    gold = pd.DataFrame(gold_rows)
    gold["period"] = pd.to_datetime(gold["period"]).dt.normalize()
    gold = gold.sort_values("period").reset_index(drop=True)

    # Compute cumulative YTD fields within each FY (fallback if P&L Summary didn't provide)
    if not gold.empty and "fy" in gold.columns:
        for col_actual, col_ytd in [
            ("ebitda_actual", "ebitda_ytd_actual"),
            ("ebitda_budget", "ebitda_ytd_budget"),
        ]:
            if col_actual in gold.columns:
                cumulative = gold.groupby("fy")[col_actual].cumsum()
                # Only fill where P&L Summary didn't already provide the value
                if col_ytd in gold.columns:
                    gold[col_ytd] = gold[col_ytd].fillna(cumulative)
                else:
                    gold[col_ytd] = cumulative

    return gold

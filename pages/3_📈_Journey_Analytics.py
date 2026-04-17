"""
Journey Analytics — Trailing 12-Month Lookback
===============================================
Investor-deck-style charts for Portco Alpha (Journey Hospitality).
Auto-selects the most recent 12 months from gold.kpi_monthly_v2.

Charts:
  1. Monthly Revenue by Business Line (stacked bars + total markers)
  2. LTM Revenue by Business Line (stacked bars + total markers)
  3. ARR Trend (stacked: Tech ARR + Services ARR)
  4. Ecommerce Live Modules (bar chart + placeholder pie for module types)
  5. Direct Contribution by BL — YoY comparison (current month vs prior year)
"""
import os
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go

PROJECT_ID = "averroes-portfolio-intel"
PORTCO_ID = "portco-alpha"

st.set_page_config(page_title="Journey Analytics", page_icon="📈", layout="wide")

# ============================================================
# DESIGN SYSTEM — Investor Deck Theme (Navy + Green)
# ============================================================
NAVY = "#0f172a"
BLUE = "#1e3a5f"
TECH_COLOR = "#2563eb"      # Blue for Tech Revenues
SERVICES_COLOR = "#84cc16"  # Green for Services
EMS_COLOR = "#8b5cf6"       # Purple for EMS (when split from Tech)
ECOM_COLOR = "#0ea5e9"      # Cyan for Ecommerce
TOTAL_COLOR = "#64748b"     # Grey for totals
ACCENT_GREEN = "#65a30d"    # Module bars
PY_COLOR = "#cbd5e1"        # Prior year comparison (light grey)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [data-testid="stAppViewContainer"] {
    background-color: #fcfcfc !important;
    font-family: 'Inter', sans-serif;
}
.main-header { font-size: 2.2rem; font-weight: 700; color: #0f172a; margin-bottom: 0; }
.sub-header  { font-size: 0.9rem; color: #64748b; margin-bottom: 24px; }
.section-h   { font-size: 1.35rem; font-weight: 600; color: #0f172a;
               border-bottom: 2px solid #0ea5e9; padding-bottom: 6px;
               margin-top: 36px; margin-bottom: 6px; }
.section-sh  { font-size: 0.85rem; color: #64748b; margin-bottom: 14px; }
.growth-badge {
    display: inline-block; padding: 8px 16px; border-radius: 50%;
    background: #1e3a5f; color: white; font-size: 1.4rem; font-weight: 700;
    text-align: center; min-width: 70px; line-height: 1.2;
}
.growth-label { font-size: 0.75rem; color: #64748b; text-align: center; margin-top: 4px; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# DATA LOADING
# ============================================================
@st.cache_data(ttl=600)
def load_v2():
    """Load gold.kpi_monthly_v2, BQ first then CSV fallback."""
    try:
        from google.cloud import bigquery
        creds = None
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"])
        client = bigquery.Client(project=PROJECT_ID, credentials=creds) if creds \
            else bigquery.Client(project=PROJECT_ID)
        q = f"""
            SELECT * FROM `{PROJECT_ID}.gold.kpi_monthly_v2`
            WHERE portco_id = '{PORTCO_ID}'
            ORDER BY period
        """
        df = client.query(q).to_dataframe()
        if len(df):
            df["period"] = pd.to_datetime(df["period"]).dt.tz_localize(None).dt.normalize()
            return df, "bigquery"
    except Exception as e:
        pass

    # Fallback: local CSV
    for csv_try in [
        os.path.join(os.path.dirname(__file__), "..", "dashboard", "gold_kpi_monthly.csv"),
        os.path.join(os.path.dirname(__file__), "..", "gold_kpi_monthly.csv"),
    ]:
        if os.path.exists(csv_try):
            df = pd.read_csv(csv_try)
            df["period"] = pd.to_datetime(df["period"]).dt.normalize()
            return df, "local_csv"
    return pd.DataFrame(), "error"


df_all, source = load_v2()
if df_all.empty:
    st.error("No data found. Check BigQuery connection or local CSV.")
    st.stop()

df_all = df_all.sort_values("period").reset_index(drop=True)

# ============================================================
# MONTH SELECTOR + TRAILING 12-MONTH FILTER
# ============================================================
all_periods = sorted(df_all["period"].unique())

st.markdown("<div class='main-header'>Journey Hospitality — Trailing 12-Month Analytics</div>",
            unsafe_allow_html=True)

# Month picker — formatted as "Mar-26", defaulting to latest
period_labels = {p: p.strftime("%b-%y") for p in all_periods}
sel_col, info_col = st.columns([2, 4])
with sel_col:
    selected = st.selectbox(
        "Select month (end of window)",
        options=all_periods,
        index=len(all_periods) - 1,
        format_func=lambda p: period_labels[p],
    )

latest = selected
cutoff = latest - pd.DateOffset(months=11)
df = df_all[df_all["period"].between(cutoff, latest)].copy().reset_index(drop=True)
df["period_label"] = df["period"].dt.strftime("%b-%y")

# Also get the prior-year row for YoY comparison
py_period = latest - pd.DateOffset(months=12)
py_row = df_all[df_all["period"] == py_period]
has_py = not py_row.empty

# Latest row
last = df.iloc[-1]

with info_col:
    st.markdown(
        f"<div class='sub-header' style='margin-top:28px;'>"
        f"Showing {df['period'].min():%b %Y} to {df['period'].max():%b %Y} "
        f"({len(df)} months) &nbsp;|&nbsp; Source: <b>{source}</b>"
        f"</div>",
        unsafe_allow_html=True,
    )


# ============================================================
# HELPER: Growth % calculation
# ============================================================
def safe_int_text(series):
    """Convert a numeric series to int strings for chart labels, NaN → ''."""
    return series.fillna(0).fillna(0).round(0).astype(int)


def yoy_growth(current, prior):
    """Return YoY growth % or None."""
    if pd.isna(current) or pd.isna(prior) or prior == 0:
        return None
    return round((current / prior - 1) * 100)


# ============================================================
# 1. MONTHLY REVENUE BY BUSINESS LINE
# ============================================================
st.markdown("<div class='section-h'>1. Monthly Revenues by Business Line (£k)</div>",
            unsafe_allow_html=True)
st.markdown("<div class='section-sh'>Tech Revenues (Ecommerce + EMS) stacked with Services. "
            "Grey dots = Total Group Revenue.</div>", unsafe_allow_html=True)

# Compute Tech = Total - Services (matching the investor deck split)
df["tech_revenue"] = df["revenue_ecommerce_actual"].fillna(0) + df["revenue_ems_actual"].fillna(0)
df["services_revenue"] = df["revenue_services_actual"].fillna(0)

chart_col, badge_col = st.columns([5, 1])

with chart_col:
    fig1 = go.Figure()

    # Tech revenue (blue)
    fig1.add_bar(
        x=df["period_label"], y=df["tech_revenue"],
        name="Tech Revenues", marker_color=TECH_COLOR,
        text=df["tech_revenue"].fillna(0).round(0).astype(int),
        textposition="inside", textfont=dict(color="white", size=10),
    )
    # Services revenue (green)
    fig1.add_bar(
        x=df["period_label"], y=df["services_revenue"],
        name="Service Revenues", marker_color=SERVICES_COLOR,
        text=df["services_revenue"].fillna(0).round(0).astype(int),
        textposition="inside", textfont=dict(color=NAVY, size=10),
    )
    # Total group (grey dots)
    fig1.add_trace(go.Scatter(
        x=df["period_label"], y=df["revenue_total_actual"],
        mode="markers+text", name="Total Group Revenues",
        marker=dict(color=TOTAL_COLOR, size=8),
        text=df["revenue_total_actual"].fillna(0).round(0).astype(int),
        textposition="top center", textfont=dict(size=10, color=TOTAL_COLOR),
    ))

    fig1.update_layout(
        barmode="stack", height=420,
        yaxis=dict(title="£k", showgrid=True, gridcolor="#f1f5f9"),
        xaxis=dict(tickangle=0),
        plot_bgcolor="white",
        margin=dict(l=40, r=20, t=30, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        font=dict(family="Inter"),
    )
    st.plotly_chart(fig1, use_container_width=True)

with badge_col:
    if has_py:
        py_tech = (py_row.iloc[0].get("revenue_ecommerce_actual", 0) or 0) + \
                  (py_row.iloc[0].get("revenue_ems_actual", 0) or 0)
        tech_growth = yoy_growth(last.get("tech_revenue"), py_tech)
        if tech_growth is not None:
            st.markdown(f"""
            <div style="text-align:center; margin-top:60px;">
                <div class="growth-badge">{tech_growth}%</div>
                <div class="growth-label">{last['period']:%b-%y} Monthly Tech<br>Revenue Growth<br>vs {py_period:%b-%y}</div>
            </div>
            """, unsafe_allow_html=True)


# ============================================================
# 2. LTM REVENUE BY BUSINESS LINE
# ============================================================
st.markdown("<div class='section-h'>2. LTM Revenues by Business Line (£k)</div>",
            unsafe_allow_html=True)
st.markdown("<div class='section-sh'>Trailing 12-month cumulative revenue. "
            "Tech Revenues stacked with Services. Grey dots = Total Group.</div>", unsafe_allow_html=True)

# Compute LTM using the full dataset (need 12 months of history)
ltm = df_all.sort_values("period").copy()
for col in ("revenue_ecommerce_actual", "revenue_ems_actual",
            "revenue_services_actual", "revenue_total_actual"):
    ltm[f"ltm_{col}"] = ltm[col].rolling(window=12, min_periods=1).sum()

ltm["ltm_tech"] = ltm["ltm_revenue_ecommerce_actual"].fillna(0) + \
                   ltm["ltm_revenue_ems_actual"].fillna(0)
ltm["ltm_services"] = ltm["ltm_revenue_services_actual"].fillna(0)

# Filter to display window
ltm_display = ltm[ltm["period"].between(cutoff, latest)].copy().reset_index(drop=True)
ltm_display["period_label"] = ltm_display["period"].dt.strftime("%b-%y")

chart_col2, badge_col2 = st.columns([5, 1])

with chart_col2:
    fig2 = go.Figure()

    fig2.add_bar(
        x=ltm_display["period_label"], y=ltm_display["ltm_tech"],
        name="Tech Revenues", marker_color=TECH_COLOR,
        text=ltm_display["ltm_tech"].fillna(0).round(0).astype(int),
        textposition="inside", textfont=dict(color="white", size=10),
    )
    fig2.add_bar(
        x=ltm_display["period_label"], y=ltm_display["ltm_services"],
        name="Service Revenues", marker_color=SERVICES_COLOR,
        text=ltm_display["ltm_services"].fillna(0).round(0).astype(int),
        textposition="inside", textfont=dict(color=NAVY, size=10),
    )
    fig2.add_trace(go.Scatter(
        x=ltm_display["period_label"], y=ltm_display["ltm_revenue_total_actual"],
        mode="markers+text", name="Total Group Revenues",
        marker=dict(color=TOTAL_COLOR, size=8),
        text=ltm_display["ltm_revenue_total_actual"].fillna(0).round(0).astype(int),
        textposition="top center", textfont=dict(size=10, color=TOTAL_COLOR),
    ))

    fig2.update_layout(
        barmode="stack", height=420,
        yaxis=dict(title="£k (trailing 12m)", showgrid=True, gridcolor="#f1f5f9"),
        xaxis=dict(tickangle=0),
        plot_bgcolor="white",
        margin=dict(l=40, r=20, t=30, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        font=dict(family="Inter"),
    )
    st.plotly_chart(fig2, use_container_width=True)

with badge_col2:
    # LTM growth badges
    ltm_latest = ltm_display.iloc[-1] if not ltm_display.empty else None
    # Get LTM from 12 months ago
    ltm_py = ltm[ltm["period"] == py_period]
    if ltm_latest is not None and not ltm_py.empty:
        tech_ltm_growth = yoy_growth(ltm_latest["ltm_tech"], ltm_py.iloc[0].get("ltm_tech", 0))
        total_ltm_growth = yoy_growth(
            ltm_latest["ltm_revenue_total_actual"],
            ltm_py.iloc[0].get("ltm_revenue_total_actual", 0)
        )
        if tech_ltm_growth is not None:
            st.markdown(f"""
            <div style="text-align:center; margin-top:40px;">
                <div class="growth-badge">{tech_ltm_growth}%</div>
                <div class="growth-label">{last['period']:%b-%y} Tech<br>Revenue Growth<br>vs {py_period:%b-%y}</div>
            </div>
            """, unsafe_allow_html=True)
        if total_ltm_growth is not None:
            st.markdown(f"""
            <div style="text-align:center; margin-top:20px;">
                <div class="growth-badge">{total_ltm_growth}%</div>
                <div class="growth-label">{last['period']:%b-%y} Group<br>Revenue Growth<br>vs {py_period:%b-%y}</div>
            </div>
            """, unsafe_allow_html=True)


# ============================================================
# 3. ARR TREND (STACKED)
# ============================================================
st.markdown("<div class='section-h'>3. ARR by Component (£k)</div>",
            unsafe_allow_html=True)
st.markdown("<div class='section-sh'>Tech ARR (Ecommerce + EMS) stacked with Services ARR. "
            "Grey dots = Total ARR.</div>", unsafe_allow_html=True)

# Compute Services ARR if not in gold
if "services_arr" not in df.columns or df["services_arr"].isna().all():
    df["services_arr_display"] = df.get("services_mrr_actual", pd.Series(dtype=float)).fillna(0) * 12
else:
    df["services_arr_display"] = df["services_arr"].fillna(0)

df["tech_arr_display"] = df["tech_arr"].fillna(0)
df["total_arr_display"] = df["tech_arr_display"] + df["services_arr_display"]

chart_col3, badge_col3 = st.columns([5, 1])

with chart_col3:
    fig3 = go.Figure()

    fig3.add_bar(
        x=df["period_label"], y=df["tech_arr_display"],
        name="Tech ARR", marker_color=TECH_COLOR,
        text=df["tech_arr_display"].fillna(0).round(0).astype(int),
        textposition="inside", textfont=dict(color="white", size=10),
    )
    fig3.add_bar(
        x=df["period_label"], y=df["services_arr_display"],
        name="Services ARR", marker_color=SERVICES_COLOR,
        text=df["services_arr_display"].fillna(0).round(0).astype(int),
        textposition="inside", textfont=dict(color=NAVY, size=10),
    )
    fig3.add_trace(go.Scatter(
        x=df["period_label"], y=df["total_arr_display"],
        mode="markers+text", name="Total ARR",
        marker=dict(color=TOTAL_COLOR, size=8),
        text=df["total_arr_display"].fillna(0).round(0).astype(int),
        textposition="top center", textfont=dict(size=10, color=TOTAL_COLOR),
    ))

    fig3.update_layout(
        barmode="stack", height=420,
        yaxis=dict(title="£k ARR", showgrid=True, gridcolor="#f1f5f9"),
        xaxis=dict(tickangle=0),
        plot_bgcolor="white",
        margin=dict(l=40, r=20, t=30, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        font=dict(family="Inter"),
    )
    st.plotly_chart(fig3, use_container_width=True)

with badge_col3:
    if has_py:
        py_tech_arr = py_row.iloc[0].get("tech_arr", 0) or 0
        py_serv_arr = py_row.iloc[0].get("services_arr", 0) or 0
        py_total_arr = py_tech_arr + py_serv_arr
        arr_growth = yoy_growth(last.get("total_arr_display"), py_total_arr)
        if arr_growth is not None:
            st.markdown(f"""
            <div style="text-align:center; margin-top:60px;">
                <div class="growth-badge">{arr_growth}%</div>
                <div class="growth-label">{last['period']:%b-%y} ARR<br>Growth vs<br>{py_period:%b-%y}</div>
            </div>
            """, unsafe_allow_html=True)


# ============================================================
# 4. ECOMMERCE LIVE MODULES
# ============================================================
st.markdown("<div class='section-h'>4. Ecommerce Live Module Growth</div>",
            unsafe_allow_html=True)

mod_col = "modules_live_ecommerce"
mod_total_col = "modules_live_total"

# Use ecommerce modules if available, else fall back to total
if mod_col in df.columns and df[mod_col].notna().any():
    mod_series = df[mod_col]
    mod_label = "Ecommerce Live Modules"
elif mod_total_col in df.columns and df[mod_total_col].notna().any():
    mod_series = df[mod_total_col]
    mod_label = "Total Live Modules"
else:
    mod_series = None
    mod_label = None

if mod_series is not None and mod_series.notna().any():
    mod_data = df[mod_series.notna()].copy()
    mod_values = mod_series[mod_series.notna()]

    chart_col4, pie_col4 = st.columns([3, 2])

    with chart_col4:
        st.markdown(f"<div class='section-sh'>Live Modules Number &mdash; {mod_label}</div>",
                    unsafe_allow_html=True)

        # Growth badge
        if has_py and mod_col in py_row.columns:
            py_mods = py_row.iloc[0].get(mod_col)
            if py_mods is None or pd.isna(py_mods):
                py_mods = py_row.iloc[0].get(mod_total_col)
            if pd.notna(py_mods) and py_mods > 0:
                mod_growth = yoy_growth(mod_values.iloc[-1], py_mods)
                if mod_growth is not None:
                    st.markdown(f"""
                    <div style="margin-bottom: 12px;">
                        <span class="growth-badge" style="font-size:1rem; padding:6px 12px; border-radius:24px;">{mod_growth}%</span>
                        <span style="color:#64748b; font-size:0.8rem; margin-left:8px;">
                            {last['period']:%b-%y} Total Live Module Growth vs {py_period:%b-%y} ({int(py_mods)})
                        </span>
                    </div>
                    """, unsafe_allow_html=True)

        fig4 = go.Figure()
        fig4.add_bar(
            x=mod_data["period_label"],
            y=mod_values.values,
            name=mod_label,
            marker_color=ACCENT_GREEN,
            text=mod_values.fillna(0).round(0).astype(int).values,
            textposition="outside",
            textfont=dict(size=10, color=NAVY),
        )
        fig4.update_layout(
            height=380,
            yaxis=dict(title="# Modules", showgrid=True, gridcolor="#f1f5f9"),
            xaxis=dict(tickangle=0),
            plot_bgcolor="white",
            margin=dict(l=40, r=20, t=30, b=40),
            showlegend=False,
            font=dict(family="Inter"),
        )
        st.plotly_chart(fig4, use_container_width=True)

    with pie_col4:
        st.markdown("<div class='section-sh'>Live Modules Number by Type</div>",
                    unsafe_allow_html=True)
        st.markdown("""
        <div style="background: #f8fafc; border: 1px dashed #cbd5e1; border-radius: 8px;
                    padding: 40px 24px; text-align: center; margin-top: 20px;">
            <p style="color: #64748b; font-size: 0.95rem; margin-bottom: 8px;">
                <strong>Module Type Breakdown</strong>
            </p>
            <p style="color: #94a3b8; font-size: 0.82rem;">
                Module types (Vouchers, Spa, Retail, Rooms, Tables) are not yet
                parsed from the MA files.<br><br>
                <em>TODO: Add module-type extraction to the Customer Numbers parser
                to enable this pie chart.</em>
            </p>
        </div>
        """, unsafe_allow_html=True)

else:
    st.info("No module data available yet (requires Era 2+ data with Customer Numbers sheet).")


# ============================================================
# 5. DIRECT CONTRIBUTION — YoY COMPARISON
# ============================================================
st.markdown("<div class='section-h'>5. Direct Contribution Analysis (£k)</div>",
            unsafe_allow_html=True)
st.markdown("<div class='section-sh'>Year-over-year comparison of direct contribution by business line. "
            "Contribution = Revenue - Direct &amp; Staff Costs.</div>", unsafe_allow_html=True)

# Build comparison data: latest month vs prior year
latest_label = f"{last['period']:%b-%y}"
py_label = f"{py_period:%b-%y}" if has_py else "Prior Year"

# Contribution values
bl_names = ["Ecommerce", "EMS", "Services"]
bl_rev_cols = ["revenue_ecommerce_actual", "revenue_ems_actual", "revenue_services_actual"]
bl_dc_cols = ["contribution_ecommerce", "contribution_ems", "contribution_services"]

current_rev = [last.get(c, 0) or 0 for c in bl_rev_cols]
current_dc = [last.get(c, 0) or 0 for c in bl_dc_cols]
current_total_rev = sum(current_rev)
current_total_dc = sum(current_dc)

if has_py:
    py_r = py_row.iloc[0]
    prior_rev = [py_r.get(c, 0) or 0 for c in bl_rev_cols]
    prior_dc = [py_r.get(c, 0) or 0 for c in bl_dc_cols]
    prior_total_rev = sum(prior_rev)
    prior_total_dc = sum(prior_dc)
else:
    prior_rev = [0, 0, 0]
    prior_dc = [0, 0, 0]
    prior_total_rev = 0
    prior_total_dc = 0

# ---- Summary table + charts side by side ----
table_col, charts_col = st.columns([2, 3])

with table_col:
    st.markdown(f"**Financial Breakdown (£k)**")

    rows_data = []
    for i, bl in enumerate(bl_names):
        rows_data.append({"": bl, latest_label: f"{current_rev[i]:,.0f}", py_label: f"{prior_rev[i]:,.0f}"})
    rows_data.append({
        "": "**Total Revenue**",
        latest_label: f"**{current_total_rev:,.0f}**",
        py_label: f"**{prior_total_rev:,.0f}**",
    })

    # Costs row
    current_costs = [abs((current_rev[i]) - (current_dc[i])) for i in range(3)]
    prior_costs = [abs((prior_rev[i]) - (prior_dc[i])) for i in range(3)]
    for i, bl in enumerate(bl_names):
        rows_data.append({"": bl, latest_label: f"({current_costs[i]:,.0f})", py_label: f"({prior_costs[i]:,.0f})"})
    rows_data.append({
        "": "**Total Direct & Staff Costs**",
        latest_label: f"**({sum(current_costs):,.0f})**",
        py_label: f"**({sum(prior_costs):,.0f})**",
    })

    # Contribution with margins
    for i, bl in enumerate(bl_names):
        margin_curr = (current_dc[i] / current_rev[i] * 100) if current_rev[i] else 0
        margin_py = (prior_dc[i] / prior_rev[i] * 100) if prior_rev[i] else 0
        rows_data.append({
            "": bl,
            latest_label: f"{current_dc[i]:,.0f}",
            f"% ({latest_label})": f"{margin_curr:.0f}%",
            py_label: f"{prior_dc[i]:,.0f}",
            f"% ({py_label})": f"{margin_py:.0f}%",
        })

    total_margin_curr = (current_total_dc / current_total_rev * 100) if current_total_rev else 0
    total_margin_py = (prior_total_dc / prior_total_rev * 100) if prior_total_rev else 0
    rows_data.append({
        "": "**Total Direct Contribution**",
        latest_label: f"**{current_total_dc:,.0f}**",
        f"% ({latest_label})": f"**{total_margin_curr:.0f}%**",
        py_label: f"**{prior_total_dc:,.0f}**",
        f"% ({py_label})": f"**{total_margin_py:.0f}%**",
    })

    summary_df = pd.DataFrame(rows_data)
    st.dataframe(summary_df, hide_index=True, use_container_width=True)

with charts_col:
    # Three side-by-side DC comparison charts
    c1, c2, c3 = st.columns(3)

    bl_colors = [ECOM_COLOR, EMS_COLOR, SERVICES_COLOR]

    for idx, (col_widget, bl) in enumerate(zip([c1, c2, c3], bl_names)):
        with col_widget:
            st.markdown(f"<div style='text-align:center; font-weight:600; color:{NAVY}; "
                        f"font-size:0.85rem;'>{bl} Direct Contribution</div>",
                        unsafe_allow_html=True)

            dc_curr = current_dc[idx]
            dc_py = prior_dc[idx]
            rev_curr = current_rev[idx]
            rev_py = prior_rev[idx]
            margin_curr = (dc_curr / rev_curr * 100) if rev_curr else 0
            margin_py = (dc_py / rev_py * 100) if rev_py else 0

            fig_dc = go.Figure()

            # Revenue bars (lighter, background)
            fig_dc.add_bar(
                x=[py_label, latest_label],
                y=[rev_py, rev_curr],
                name="Revenue",
                marker_color=["#e2e8f0", "#e2e8f0"],
                text=[f"{rev_py:,.0f}", f"{rev_curr:,.0f}"],
                textposition="outside",
                textfont=dict(size=9, color=TOTAL_COLOR),
            )
            # DC bars (colored, overlaid)
            fig_dc.add_bar(
                x=[py_label, latest_label],
                y=[dc_py, dc_curr],
                name="Direct Contribution",
                marker_color=[PY_COLOR, bl_colors[idx]],
                text=[f"{dc_py:,.0f}", f"{dc_curr:,.0f}"],
                textposition="inside",
                textfont=dict(size=11, color="white" if idx != 1 else NAVY),
            )
            # Margin % as scatter points
            fig_dc.add_trace(go.Scatter(
                x=[py_label, latest_label],
                y=[margin_py, margin_curr],
                mode="markers+text",
                name="DC Margin %",
                marker=dict(color=NAVY, size=10),
                text=[f"{margin_py:.0f}%", f"{margin_curr:.0f}%"],
                textposition="bottom center",
                textfont=dict(size=10),
                yaxis="y2",
            ))

            fig_dc.update_layout(
                barmode="overlay",
                height=300,
                showlegend=False,
                yaxis=dict(showgrid=False, showticklabels=False),
                yaxis2=dict(overlaying="y", side="right", showgrid=False,
                            showticklabels=False, range=[0, 100]),
                xaxis=dict(tickfont=dict(size=10)),
                plot_bgcolor="white",
                margin=dict(l=10, r=10, t=10, b=40),
                font=dict(family="Inter"),
            )
            st.plotly_chart(fig_dc, use_container_width=True)


# ============================================================
# MONTHLY MoM TREND TABLE (collapsible)
# ============================================================
st.markdown("---")
with st.expander("📋 Underlying Data — Last 12 Months"):
    display_cols = [
        "period", "era",
        "revenue_ecommerce_actual", "revenue_ems_actual", "revenue_services_actual",
        "revenue_total_actual",
        "tech_arr", "services_arr",
        "contribution_ecommerce", "contribution_ems", "contribution_services",
        "contribution_total",
        "modules_live_ecommerce", "modules_live_ems", "modules_live_services",
        "modules_live_total",
        "ebitda_actual", "total_headcount",
    ]
    show = df[[c for c in display_cols if c in df.columns]].copy()
    show["period"] = df["period"].dt.strftime("%Y-%m")

    # Rename for readability
    rename_map = {
        "revenue_ecommerce_actual": "Rev Ecom (£k)",
        "revenue_ems_actual": "Rev EMS (£k)",
        "revenue_services_actual": "Rev Svc (£k)",
        "revenue_total_actual": "Rev Total (£k)",
        "tech_arr": "Tech ARR (£k)",
        "services_arr": "Svc ARR (£k)",
        "contribution_ecommerce": "DC Ecom (£k)",
        "contribution_ems": "DC EMS (£k)",
        "contribution_services": "DC Svc (£k)",
        "contribution_total": "DC Total (£k)",
        "modules_live_ecommerce": "Mods Ecom",
        "modules_live_ems": "Mods EMS",
        "modules_live_services": "Mods Svc",
        "modules_live_total": "Mods Total",
        "ebitda_actual": "EBITDA (£k)",
        "total_headcount": "Headcount",
    }
    show = show.rename(columns=rename_map)
    st.dataframe(show, hide_index=True, use_container_width=True)

st.caption(f"Journey Analytics | Averroes Capital Portfolio Intelligence | "
           f"Data as of {last['period']:%d %b %Y}")

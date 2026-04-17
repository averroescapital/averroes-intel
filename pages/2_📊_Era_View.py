"""
Era View — Portco Alpha (Journey Hospitality)
=============================================
Reads `gold.kpi_monthly_v2` (the era-based parser pipeline).

Six dashboard modules:
  1. Revenue by Business Line (monthly)
  2. LTM Revenue by Business Line
  3. Tech ARR Split (Tech = Ecommerce + EMS)
  4. Live Modules — monthly total
  5. Live Modules — by type (Era 2+)
  6. Direct Contribution analysis (by BL)
"""
import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

PROJECT_ID = "averroes-portfolio-intel"
PORTCO_ID  = "portco-alpha"

st.set_page_config(page_title="Era View — Portco Alpha", page_icon="📊", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [data-testid="stAppViewContainer"] {
    background-color: #fcfcfc !important;
    font-family: 'Inter', sans-serif;
}
.main-header { font-size: 2.2rem; font-weight: 700; color: #0f172a; margin-bottom: 0px; }
.sub-header  { font-size: 0.9rem; color: #64748b; margin-bottom: 30px; }
.section-h   { font-size: 1.35rem; font-weight: 600; color: #0f172a; margin-top: 28px; margin-bottom: 6px; }
.section-sh  { font-size: 0.85rem; color: #64748b; margin-bottom: 14px; }
</style>
""", unsafe_allow_html=True)

st.markdown("<div class='main-header'>📊 Portco Alpha — Era View</div>", unsafe_allow_html=True)
st.markdown("<div class='sub-header'>Era-based parser (Nov 2024 – Feb 2026). Source: <b>gold.kpi_monthly_v2</b>.</div>", unsafe_allow_html=True)


@st.cache_data(ttl=600)
def load_v2():
    """Load gold.kpi_monthly_v2 from BigQuery, fall back to local CSV for preview."""
    # 1. BigQuery
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
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
        st.info(f"BigQuery unavailable ({e.__class__.__name__}); using local CSV.")

    # 2. Local CSV
    csv_path = os.path.join(os.path.dirname(__file__), "..", "gold_kpi_monthly.csv")
    df = pd.read_csv(csv_path)
    df["period"] = pd.to_datetime(df["period"]).dt.normalize()
    return df, "local_csv"


df, source = load_v2()

# ---- Sidebar: Refresh + Reprocess ----
st.sidebar.markdown("### Data Controls")
st.sidebar.caption(f"Source: **{source}**")
if st.sidebar.button("🔄 Refresh Data", use_container_width=True,
                     help="Clear cache and re-query BigQuery"):
    st.cache_data.clear()
    st.rerun()

if st.sidebar.button("⚙️ Reprocess GCS Files", use_container_width=True,
                     help="Re-upload files on GCS to trigger the Cloud Function"):
    _reprocess_status = st.sidebar.empty()
    try:
        from google.cloud import storage as _gcs
        from google.oauth2 import service_account as _sa
        _creds = None
        if "gcp_service_account" in st.secrets:
            _creds = _sa.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"])
        _client = _gcs.Client(project=PROJECT_ID, credentials=_creds) \
            if _creds else _gcs.Client(project=PROJECT_ID)
        _bucket = _client.bucket(f"{PROJECT_ID}-portfolio-data")
        _prefix = f"{PORTCO_ID}/ma-files/"
        _blobs = list(_bucket.list_blobs(prefix=_prefix))
        _xlsx = [b for b in _blobs if b.name.lower().endswith(".xlsx")
                 and not b.name.split("/")[-1].startswith("~$")]
        if not _xlsx:
            _reprocess_status.warning("No .xlsx files found on GCS.")
        else:
            _reprocess_status.info(f"Re-triggering {len(_xlsx)} files...")
            for _b in _xlsx:
                _bucket.copy_blob(_b, _bucket, _b.name)
            _reprocess_status.success(
                f"Re-triggered {len(_xlsx)} files. Data will refresh in ~1-2 min. "
                f"Hit **Refresh Data** after that.")
    except Exception as _e:
        _reprocess_status.error(f"GCS error: {_e}")

st.sidebar.markdown("---")

if df.empty:
    st.error("No data found in gold.kpi_monthly_v2.")
    st.stop()

st.caption(f"📡 Data source: **{source}** · {len(df)} months · "
           f"{df['period'].min():%b %Y} → {df['period'].max():%b %Y}")

df = df.sort_values("period").reset_index(drop=True)
df["period_label"] = df["period"].dt.strftime("%b %y")

# ============================================================
# MODULE 1 — Revenue by BL (monthly, stacked)
# ============================================================
st.markdown("<div class='section-h'>1. Monthly Revenue by Business Line</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sh'>Stacked — Ecommerce + EMS + Services = Total. In £k.</div>", unsafe_allow_html=True)

fig1 = go.Figure()
fig1.add_bar(x=df["period_label"], y=df["revenue_ecommerce_actual"],
             name="Ecommerce", marker_color="#0ea5e9")
fig1.add_bar(x=df["period_label"], y=df["revenue_ems_actual"],
             name="EMS", marker_color="#8b5cf6")
fig1.add_bar(x=df["period_label"], y=df["revenue_services_actual"],
             name="Services", marker_color="#f59e0b")
fig1.add_trace(go.Scatter(x=df["period_label"], y=df["revenue_total_actual"],
                          mode="lines+markers", name="Total (actual)",
                          line=dict(color="#0f172a", width=2)))
fig1.update_layout(barmode="stack", height=380,
                   yaxis_title="£k", margin=dict(l=20, r=20, t=10, b=20),
                   legend=dict(orientation="h", yanchor="bottom", y=1.02))
st.plotly_chart(fig1, use_container_width=True)

# ============================================================
# MODULE 2 — LTM Revenue by BL
# ============================================================
st.markdown("<div class='section-h'>2. LTM Revenue by Business Line</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sh'>Trailing 12-month revenue, computed month-by-month. Needs ≥12 data points to be meaningful.</div>", unsafe_allow_html=True)

ltm = df.copy()
for col in ("revenue_ecommerce_actual", "revenue_ems_actual",
            "revenue_services_actual", "revenue_total_actual"):
    ltm[f"ltm_{col}"] = ltm[col].rolling(window=12, min_periods=1).sum()

fig2 = go.Figure()
fig2.add_trace(go.Scatter(x=ltm["period_label"], y=ltm["ltm_revenue_ecommerce_actual"],
                          mode="lines+markers", name="Ecommerce LTM",
                          line=dict(color="#0ea5e9", width=2)))
fig2.add_trace(go.Scatter(x=ltm["period_label"], y=ltm["ltm_revenue_ems_actual"],
                          mode="lines+markers", name="EMS LTM",
                          line=dict(color="#8b5cf6", width=2)))
fig2.add_trace(go.Scatter(x=ltm["period_label"], y=ltm["ltm_revenue_services_actual"],
                          mode="lines+markers", name="Services LTM",
                          line=dict(color="#f59e0b", width=2)))
fig2.add_trace(go.Scatter(x=ltm["period_label"], y=ltm["ltm_revenue_total_actual"],
                          mode="lines+markers", name="Total LTM",
                          line=dict(color="#0f172a", width=2, dash="dot")))
fig2.update_layout(height=380, yaxis_title="£k (trailing 12m)",
                   margin=dict(l=20, r=20, t=10, b=20),
                   legend=dict(orientation="h", yanchor="bottom", y=1.02))
st.plotly_chart(fig2, use_container_width=True)

# ============================================================
# MODULE 3 — Tech ARR Split
# ============================================================
st.markdown("<div class='section-h'>3. Tech ARR Split</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sh'>Tech ARR = Ecommerce ARR + EMS ARR (identity enforced by back-solving EMS from Tech MRR).</div>", unsafe_allow_html=True)

c1, c2 = st.columns([3, 2])
with c1:
    fig3 = go.Figure()
    fig3.add_bar(x=df["period_label"], y=df["ecommerce_arr"],
                 name="Ecommerce ARR", marker_color="#0ea5e9")
    fig3.add_bar(x=df["period_label"], y=df["ems_arr"],
                 name="EMS ARR", marker_color="#8b5cf6")
    fig3.add_trace(go.Scatter(x=df["period_label"], y=df["tech_arr"],
                              mode="lines+markers", name="Tech ARR",
                              line=dict(color="#0f172a", width=2)))
    fig3.update_layout(barmode="stack", height=380,
                       yaxis_title="£k ARR",
                       margin=dict(l=20, r=20, t=10, b=20),
                       legend=dict(orientation="h", yanchor="bottom", y=1.02))
    st.plotly_chart(fig3, use_container_width=True)

with c2:
    last = df.iloc[-1]
    st.markdown("**Latest month:** " + last["period"].strftime("%b %Y"))
    st.metric("Tech ARR", f"£{last['tech_arr']:,.0f}k")
    st.metric("Ecommerce ARR", f"£{last['ecommerce_arr']:,.0f}k",
              delta=f"{last['ecommerce_arr'] / last['tech_arr'] * 100:,.1f}% of Tech")
    st.metric("EMS ARR", f"£{last['ems_arr']:,.0f}k",
              delta=f"{last['ems_arr'] / last['tech_arr'] * 100:,.1f}% of Tech")

# ============================================================
# MODULE 4 — Live Modules (monthly total)
# ============================================================
st.markdown("<div class='section-h'>4. Live Modules — Monthly Total</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sh'>Available Era 2 onwards (Nov 2025+). Sum of all live instances.</div>", unsafe_allow_html=True)

mod = df[df["modules_live_total"].notna()].copy()
if mod.empty:
    st.info("No module data available yet (Era 2+).")
else:
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=mod["period_label"], y=mod["modules_live_total"],
                              mode="lines+markers+text",
                              text=mod["modules_live_total"].astype(int).astype(str),
                              textposition="top center",
                              name="Live modules",
                              line=dict(color="#10b981", width=3)))
    fig4.update_layout(height=320, yaxis_title="# modules live",
                       margin=dict(l=20, r=20, t=10, b=20))
    st.plotly_chart(fig4, use_container_width=True)

# ============================================================
# MODULE 5 — Modules: Live by Business Line (Era 2+)
# ============================================================
st.markdown("<div class='section-h'>5. Live Modules — by Business Line</div>", unsafe_allow_html=True)

# Check if per-BL columns are populated
has_bl = all(c in mod.columns for c in ("modules_live_ecommerce", "modules_live_ems", "modules_live_services")) and \
         mod[["modules_live_ecommerce", "modules_live_ems", "modules_live_services"]].notna().any().all() if not mod.empty else False

if mod.empty:
    st.info("No module data available yet (Era 2+).")
elif has_bl:
    st.markdown("<div class='section-sh'>Stacked by BL — Ecommerce + EMS + Services = Total. Pipeline shown as dotted line.</div>", unsafe_allow_html=True)

    fig5 = go.Figure()
    fig5.add_bar(x=mod["period_label"], y=mod["modules_live_ecommerce"],
                 name="Ecommerce", marker_color="#0ea5e9")
    fig5.add_bar(x=mod["period_label"], y=mod["modules_live_ems"],
                 name="EMS", marker_color="#8b5cf6")
    fig5.add_bar(x=mod["period_label"], y=mod["modules_live_services"],
                 name="Services", marker_color="#f59e0b")
    fig5.add_trace(go.Scatter(x=mod["period_label"], y=mod["modules_live_total"],
                              mode="lines+markers", name="Total",
                              line=dict(color="#0f172a", width=2)))
    if "modules_pipeline" in mod.columns and mod["modules_pipeline"].notna().any():
        fig5.add_trace(go.Scatter(x=mod["period_label"], y=mod["modules_pipeline"],
                                  mode="lines+markers", name="Pipeline",
                                  line=dict(color="#ef4444", width=2, dash="dot")))
    fig5.update_layout(barmode="stack", height=400, yaxis_title="# modules / properties",
                       margin=dict(l=20, r=20, t=10, b=20),
                       legend=dict(orientation="h", yanchor="bottom", y=1.02))
    st.plotly_chart(fig5, use_container_width=True)

    # Per-BL summary cards
    last_mod = mod.iloc[-1]
    prev_mod = mod.iloc[-2] if len(mod) > 1 else None
    cols_summary = st.columns(5)
    for i, (col, label) in enumerate([
        ("modules_live_ecommerce", "Ecommerce"),
        ("modules_live_ems", "EMS"),
        ("modules_live_services", "Services"),
        ("modules_live_total", "Total"),
        ("modules_pipeline", "Pipeline"),
    ]):
        val = last_mod.get(col)
        if pd.notna(val):
            delta_str = None
            if prev_mod is not None and pd.notna(prev_mod.get(col)):
                delta_str = f"{int(val - prev_mod[col]):+,} MoM"
            cols_summary[i].metric(label, f"{int(val):,}", delta=delta_str)

else:
    # Fallback: totals only (no per-BL data yet)
    st.markdown("<div class='section-sh'>Total live modules vs pipeline. Per-BL breakdown populates when Customer Numbers sheet data is available.</div>", unsafe_allow_html=True)

    fig5 = go.Figure()
    fig5.add_bar(x=mod["period_label"], y=mod["modules_live_total"],
                 name="Live modules", marker_color="#10b981")
    if "modules_pipeline" in mod.columns and mod["modules_pipeline"].notna().any():
        fig5.add_trace(go.Scatter(x=mod["period_label"], y=mod["modules_pipeline"],
                                  mode="lines+markers", name="Pipeline",
                                  line=dict(color="#0f172a", width=2, dash="dot")))
    fig5.update_layout(height=360, yaxis_title="# modules",
                       margin=dict(l=20, r=20, t=10, b=20),
                       legend=dict(orientation="h", yanchor="bottom", y=1.02))
    st.plotly_chart(fig5, use_container_width=True)

    last_mod = mod.iloc[-1]
    prev_mod = mod.iloc[-2] if len(mod) > 1 else None
    cols_summary = st.columns(3)
    cols_summary[0].metric("Live modules (latest)", f"{int(last_mod['modules_live_total']):,}",
                           delta=(f"{int(last_mod['modules_live_total'] - prev_mod['modules_live_total']):+,} MoM"
                                  if prev_mod is not None else None))
    if "modules_pipeline" in mod.columns and pd.notna(last_mod.get("modules_pipeline")):
        cols_summary[1].metric("Pipeline", f"{int(last_mod['modules_pipeline']):,}")
    cols_summary[2].metric("Months tracked", f"{len(mod)}")

# ============================================================
# MODULE 6 — Direct Contribution Analysis
# ============================================================
st.markdown("<div class='section-h'>6. Direct Contribution Analysis</div>", unsafe_allow_html=True)
st.markdown("<div class='section-sh'>Contribution margin = (Revenue − Direct Costs) by business line. Solid bars = DC, dotted line = DC margin of total revenue.</div>", unsafe_allow_html=True)

fig6 = go.Figure()
fig6.add_bar(x=df["period_label"], y=df["contribution_ecommerce"],
             name="Ecommerce DC", marker_color="#0ea5e9")
fig6.add_bar(x=df["period_label"], y=df["contribution_ems"],
             name="EMS DC", marker_color="#8b5cf6")
fig6.add_bar(x=df["period_label"], y=df["contribution_services"],
             name="Services DC", marker_color="#f59e0b")
fig6.add_trace(go.Scatter(x=df["period_label"], y=df["contribution_total"],
                          mode="lines+markers", name="Total DC",
                          line=dict(color="#0f172a", width=2)))
# DC margin %
margin_pct = (df["contribution_total"] / df["revenue_total_actual"] * 100).round(1)
fig6.add_trace(go.Scatter(x=df["period_label"], y=margin_pct,
                          mode="lines+markers", name="DC margin %",
                          line=dict(color="#ef4444", width=2, dash="dot"),
                          yaxis="y2"))
fig6.update_layout(barmode="stack", height=420,
                   yaxis=dict(title="£k DC"),
                   yaxis2=dict(title="DC margin %", overlaying="y", side="right",
                               showgrid=False, range=[0, 100]),
                   margin=dict(l=20, r=20, t=10, b=20),
                   legend=dict(orientation="h", yanchor="bottom", y=1.02))
st.plotly_chart(fig6, use_container_width=True)

# ============================================================
# RAW DATA TABLE (collapsible)
# ============================================================
with st.expander("📋 Raw data (gold.kpi_monthly_v2)"):
    display_cols = ["period", "era", "revenue_total_actual",
                    "revenue_ecommerce_actual", "revenue_ems_actual", "revenue_services_actual",
                    "tech_mrr_actual", "tech_arr", "ecommerce_arr", "ems_arr",
                    "contribution_total", "ebitda_actual",
                    "modules_live_total", "modules_live_ecommerce", "modules_live_ems",
                    "modules_live_services", "modules_pipeline", "total_headcount"]
    show = df[[c for c in display_cols if c in df.columns]].copy()
    show["period"] = show["period"].dt.strftime("%Y-%m")
    st.dataframe(show, hide_index=True, use_container_width=True)

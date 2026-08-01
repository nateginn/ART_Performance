"""
ART Performance Dashboard
Run with: streamlit run dashboard.py
"""

import glob
import io
import os
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

BILLING_STAGES = {'Review', 'Open', 'Closed'}
OPERATIONAL_STAGES = {'No Show', 'Patient Canceled', 'Center Canceled', 'Not Started'}

st.set_page_config(
    page_title="ART Performance",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Helpers ────────────────────────────────────────────────────────────────────

def load_latest(prefix: str) -> pd.DataFrame | None:
    files = glob.glob(f"data/{prefix}*.csv")
    if not files:
        return None
    return pd.read_csv(max(files, key=os.path.getmtime))


def last_updated() -> str:
    try:
        import db_cache as _dbc
        con = _dbc.connect()
        _dbc._ensure_meta(con)
        row = con.execute(
            "SELECT loaded_at FROM _cache_meta ORDER BY loaded_at DESC LIMIT 1"
        ).fetchone()
        con.close()
        if row:
            ts = datetime.fromisoformat(row[0])
            return ts.strftime("%b %d, %Y  %I:%M %p")
    except Exception:
        pass
    return "No data loaded yet"


def clean_currency(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r'[\$,()]', '', regex=True),
                errors='coerce'
            ).fillna(0)
    return df


@st.cache_data(ttl=3600, show_spinner="Loading Prompt data…")
def load_prompt_data() -> pd.DataFrame | None:
    try:
        import db_cache as _dbc
        con = _dbc.connect()
        tables = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()}
        if "prompt_visits" not in tables:
            con.close()
            return None
        df = con.execute("SELECT * FROM prompt_visits").df()
        con.close()
        if "DOS" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["DOS"]):
            df["DOS"] = pd.to_datetime(df["DOS"], errors="coerce")
        return df
    except Exception as e:
        st.warning(f"Could not load Prompt data: {e}")
        return None


@st.cache_data(ttl=3600)
def load_qb_pl_data():
    return None


@st.cache_data(ttl=3600)
def load_amd_data():
    try:
        import db_cache as _dbc
        con = _dbc.connect()
        tables = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()}
        if "amd_billing" not in tables:
            con.close()
            return None
        df = con.execute("SELECT * FROM amd_billing").df()
        con.close()
        if "DOS_key" in df.columns:
            df["DOS_key"] = df["DOS_key"].astype(str)
        return df
    except Exception as e:
        st.warning(f"Could not load AMD data: {e}")
        return None


def compute_monthly_collections(prompt_df, amd_df, month_str: str) -> dict:
    billing = prompt_df[
        (prompt_df['DOS'].dt.to_period('M').astype(str) == month_str) &
        (prompt_df['Visit Stage'].isin(BILLING_STAGES))
    ].copy()

    amd_lookup = {}
    if amd_df is not None:
        for _, row in amd_df.iterrows():
            pid = row.get('Patient Account Number')
            dos = row.get('DOS_key')
            if pid and dos and not pd.isna(pid):
                amd_lookup[f"{pid}|{dos}"] = row

    total_charges = billing['Last Billed'].sum()
    total_ar = billing[billing['Visit Stage'] == 'Open']['Last Billed'].sum()
    visit_count = len(billing)
    open_count = (billing['Visit Stage'] == 'Open').sum()
    closed_count = (billing['Visit Stage'] == 'Closed').sum()
    review_count = (billing['Visit Stage'] == 'Review').sum()

    provider_collected = {}
    provider_charges = billing.groupby('Provider')['Last Billed'].sum().to_dict()
    total_collected = 0.0

    for _, row in billing.iterrows():
        prompt_paid = row['Total Paid']
        if prompt_paid > 0:
            collected = prompt_paid
        else:
            key = f"{row['Patient Account Number']}|{row['DOS'].strftime('%Y-%m-%d') if pd.notna(row['DOS']) else ''}"
            amd_row = amd_lookup.get(key)
            if amd_row is not None:
                collected = amd_row['Insurance Payments'] + amd_row['Patient Payments']
            else:
                collected = 0.0
        total_collected += collected
        prov = row.get('Provider', 'Unknown')
        provider_collected[prov] = provider_collected.get(prov, 0.0) + collected

    providers = set(list(provider_charges.keys()) + list(provider_collected.keys()))
    if providers:
        by_provider = pd.DataFrame([
            {'Provider': p, 'Charges': provider_charges.get(p, 0), 'Collected': provider_collected.get(p, 0)}
            for p in providers
        ]).sort_values('Charges', ascending=True)
    else:
        by_provider = pd.DataFrame(columns=['Provider', 'Charges', 'Collected'])

    allowed = billing[billing['Primary Allowed'] > 0]
    coll_rate = (
        allowed['Total Paid'].sum() / allowed['Primary Allowed'].sum() * 100
        if len(allowed) > 0 else 0
    )

    return {
        'total_charges': total_charges,
        'total_collected': total_collected,
        'total_ar': total_ar,
        'visit_count': visit_count,
        'open_count': int(open_count),
        'closed_count': int(closed_count),
        'review_count': int(review_count),
        'coll_rate': coll_rate,
        'by_provider': by_provider,
        'amd_used': amd_df is not None,
    }


def fmt_dollar(val: float) -> str:
    return f"${val:,.0f}"


def fmt_pct(val: float) -> str:
    return f"{val:.1f}%"


# ── Sidebar ─────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🏥 ART Performance")
    st.caption("Accelerated Rehab Therapy")
    st.divider()
    st.caption("**Last data load**")
    st.caption(last_updated())
    st.divider()

    try:
        import db_cache as _dbc
        con = _dbc.connect()
        _dbc._ensure_meta(con)
        meta_rows = con.execute(
            "SELECT table_name, loaded_at FROM _cache_meta ORDER BY table_name"
        ).fetchall()
        con.close()
        if meta_rows:
            STALE_DAYS = {"prompt_visits": 7, "amd_billing": 35}
            st.caption("**Loaded tables**")
            for tname, loaded_at in meta_rows:
                try:
                    dt = datetime.fromisoformat(str(loaded_at).replace("Z", "+00:00"))
                    ts = dt.strftime("%m/%d %I:%M %p")
                    age_days = (datetime.now(dt.tzinfo) - dt).days
                except Exception:
                    ts = str(loaded_at)[:10] if loaded_at else "?"
                    age_days = None
                warn = ""
                limit = STALE_DAYS.get(tname)
                if limit is not None and age_days is not None and age_days > limit:
                    warn = f" ⚠️ {age_days}d old"
                st.caption(f"• {tname}  {ts}{warn}")
            if any(t == "amd_billing" for t, _ in meta_rows):
                amd_dt = next(l for t, l in meta_rows if t == "amd_billing")
                try:
                    amd_age = (datetime.now(timezone.utc) -
                               datetime.fromisoformat(str(amd_dt).replace("Z", "+00:00"))).days
                    if amd_age > 35:
                        st.warning(f"AMD data is {amd_age} days old — commercial payment "
                                   "status is behind. Upload a fresh AMD export (Tab 3).")
                except Exception:
                    pass
        else:
            st.caption("No data loaded yet.")
    except Exception:
        st.caption("Database not initialized.")

    st.divider()
    if st.button("🔄 Refresh Display", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("Facilities")
    st.caption("• ART Denver")
    st.caption("• ART Greeley")

# ── Tabs ────────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview", "⚠️ AR & Action Items",
    "🏢 Commercial Audit", "📅 Monthly Report", "🚗 Auto/PI Audit"
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    prompt_df = load_prompt_data()

    if prompt_df is None:
        st.info("Prompt data not loaded yet. Upload your **Prompt Revenue All Data** CSV in the **Commercial Audit** tab.")
    else:
        billing_df = prompt_df[prompt_df['Visit Stage'].isin(BILLING_STAGES)].copy()
        ops_df     = prompt_df[prompt_df['Visit Stage'].isin(OPERATIONAL_STAGES)].copy()

        total_visits    = len(billing_df)
        total_charged   = billing_df['Last Billed'].sum()
        total_collected = billing_df['Total Paid'].sum()
        total_balance   = billing_df['Pt. Current Balance'].sum()

        allowed_df  = billing_df[billing_df['Primary Allowed'] > 0]
        coll_rate   = (allowed_df['Total Paid'].sum() / allowed_df['Primary Allowed'].sum() * 100
                       if len(allowed_df) > 0 else 0)

        # ── KPI Cards ──────────────────────────────────────────────────────────
        st.subheader("Financial Overview")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Visits",         f"{total_visits:,}")
        c2.metric("Total Charged",        fmt_dollar(total_charged))
        c3.metric("Total Collected",      fmt_dollar(total_collected))
        c4.metric("Collection Rate",      fmt_pct(coll_rate))
        c5.metric("Outstanding Balance",  fmt_dollar(total_balance))

        st.divider()

        # ── Facility + Stage ──────────────────────────────────────────────────
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("By Facility")
            fac = billing_df.groupby('Visit Facility').agg(
                Charged=('Last Billed', 'sum'),
                Collected=('Total Paid', 'sum'),
                Outstanding=('Pt. Current Balance', 'sum')
            ).reset_index()
            fac_melt = fac.melt(id_vars='Visit Facility',
                                value_vars=['Charged', 'Collected', 'Outstanding'],
                                var_name='Metric', value_name='Amount')
            fig_fac = px.bar(
                fac_melt, x='Visit Facility', y='Amount', color='Metric',
                barmode='group',
                color_discrete_map={'Charged': '#4A90D9', 'Collected': '#27AE60', 'Outstanding': '#E67E22'},
                labels={'Amount': 'Dollars ($)', 'Visit Facility': ''},
            )
            fig_fac.update_layout(legend_title='', margin=dict(t=10, b=10))
            st.plotly_chart(fig_fac, use_container_width=True)

        with col_right:
            st.subheader("Visit Stage Breakdown")
            stage = billing_df.groupby('Visit Stage').size().reset_index(name='Count')
            fig_stage = px.pie(
                stage, names='Visit Stage', values='Count', hole=0.45,
                color_discrete_sequence=['#27AE60', '#4A90D9', '#F39C12'],
            )
            fig_stage.update_layout(margin=dict(t=10, b=10), legend_title='')
            st.plotly_chart(fig_stage, use_container_width=True)

        st.divider()

        # ── Operational Summary ───────────────────────────────────────────────
        st.subheader("Operational (Excluded from Revenue)")
        ops_summary = load_latest('reconciliation_operational_')
        if ops_summary is not None:
            ocols = st.columns(len(ops_summary))
            for i, (_, row) in enumerate(ops_summary.iterrows()):
                ocols[i].metric(row['Visit Stage'], f"{int(row['Count']):,}",
                                f"{row['% of Operational']}% of cancels")
        else:
            st.caption("Run `--reconcile` to populate operational data.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — AR & ACTION ITEMS
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    needs_attn    = load_latest('reconciliation_needs_attention_')
    outstanding   = load_latest('reconciliation_outstanding_ar_')
    needs_posting = load_latest('reconciliation_needs_posting_')
    mismatched    = load_latest('reconciliation_mismatched_')

    missing = needs_attn is None and outstanding is None

    if missing:
        st.info("No reconciliation data found. Run `python main.py --reconcile` first.")
    else:
        # ── Alert Cards ───────────────────────────────────────────────────────
        st.subheader("Action Items")
        a1, a2, a3, a4 = st.columns(4)

        attn_count  = len(needs_attn)   if needs_attn   is not None else 0
        attn_amt    = needs_attn['Last Billed'].sum() if needs_attn is not None else 0

        ar_count    = len(outstanding)  if outstanding  is not None else 0
        ar_amt      = outstanding['Last Billed'].sum() if outstanding is not None else 0

        post_count  = len(needs_posting) if needs_posting is not None else 0
        post_amt    = (pd.to_numeric(needs_posting['AMD Insurance Payments'], errors='coerce').fillna(0).sum() +
                       pd.to_numeric(needs_posting['AMD Patient Payments'],   errors='coerce').fillna(0).sum()
                       ) if needs_posting is not None else 0

        mis_count   = len(mismatched) if mismatched is not None else 0
        mis_amt     = (pd.to_numeric(mismatched['Insurance Diff'], errors='coerce').fillna(0).abs().sum() +
                       pd.to_numeric(mismatched['Patient Diff'],   errors='coerce').fillna(0).abs().sum()
                       ) if mismatched is not None else 0

        a1.metric("🔔 Needs Attention",    f"{attn_count} visits",  fmt_dollar(attn_amt))
        a2.metric("📋 Outstanding AR",      f"{ar_count} visits",   fmt_dollar(ar_amt))
        a3.metric("📥 Needs Posting",       f"{post_count} visits", fmt_dollar(post_amt))
        a4.metric("⚡ Mismatched Payments", f"{mis_count} visits",  fmt_dollar(mis_amt))

        st.divider()

        # ── AR Aging Chart ────────────────────────────────────────────────────
        if outstanding is not None and len(outstanding) > 0:
            st.subheader("AR Aging — Outstanding Claims")

            def age_bucket(days):
                if days <= 60:   return "31–60 days"
                if days <= 90:   return "61–90 days"
                if days <= 120:  return "91–120 days"
                return "120+ days"

            ar = outstanding.copy()
            ar['Age Bucket'] = ar['Days Since DOS'].apply(age_bucket)
            aging = ar.groupby('Age Bucket').agg(
                Visits=('Last Billed', 'count'),
                Amount=('Last Billed', 'sum')
            ).reindex(["31–60 days", "61–90 days", "91–120 days", "120+ days"]).reset_index()

            fig_aging = px.bar(
                aging, x='Age Bucket', y='Amount',
                text=aging['Visits'].apply(lambda v: f"{v} visits"),
                color='Age Bucket',
                color_discrete_map={
                    "31–60 days":  "#27AE60",
                    "61–90 days":  "#F39C12",
                    "91–120 days": "#E67E22",
                    "120+ days":   "#E74C3C",
                },
                labels={'Amount': 'Total Charges ($)', 'Age Bucket': ''},
            )
            fig_aging.update_traces(textposition='outside')
            fig_aging.update_layout(showlegend=False, margin=dict(t=30, b=10))
            st.plotly_chart(fig_aging, use_container_width=True)

            st.divider()

        # ── Detail Tables ─────────────────────────────────────────────────────
        st.subheader("Detail")

        if needs_attn is not None and len(needs_attn) > 0:
            with st.expander(f"🔔 Needs Attention — {attn_count} visits unbilled > 14 days  |  {fmt_dollar(attn_amt)}"):
                st.dataframe(needs_attn, use_container_width=True, hide_index=True)

        if outstanding is not None and len(outstanding) > 0:
            with st.expander(f"📋 Outstanding AR — {ar_count} visits unpaid > 30 days  |  {fmt_dollar(ar_amt)}"):
                st.dataframe(outstanding, use_container_width=True, hide_index=True)

        if needs_posting is not None and len(needs_posting) > 0:
            with st.expander(f"📥 Needs Posting — {post_count} AMD payments not in Prompt  |  {fmt_dollar(post_amt)}"):
                st.dataframe(needs_posting, use_container_width=True, hide_index=True)

        if mismatched is not None and len(mismatched) > 0:
            with st.expander(f"⚡ Mismatched Payments — {mis_count} conflicts  |  {fmt_dollar(mis_amt)}"):
                st.dataframe(mismatched, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — COMMERCIAL AUDIT (DuckDB-powered)
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    st.subheader("Commercial Insurance Audit")
    st.caption(
        "Covers two periods: AMD billing only (May–Aug 2025) and "
        "Prompt vs AMD cross-reference (Sep 2025–present)."
    )

    # ── Data loaders ─────────────────────────────────────────────────────────
    st.caption("Upload your source files below. Each only needs to be uploaded when you have new data to load.")
    col_prompt, col_amd, col_unc = st.columns(3)

    with col_prompt.expander("📋 Load / Refresh Prompt Revenue Data", expanded=False):
        st.caption(
            "Export **Prompt Revenue All Data** from Prompt EMR (all visits, full history) "
            "as CSV and upload here. This is the primary visit and billing record for Sep 2025+."
        )
        uploaded_prompt = st.file_uploader(
            "Prompt Revenue All Data CSV", type=["csv"],
            key="prompt_upload"
        )
        force_prompt = st.checkbox("Force reload", value=False, key="force_prompt")
        if st.button("Load Prompt into Database", disabled=uploaded_prompt is None):
            try:
                import db_cache as _dbc
                raw = uploaded_prompt.read()
                with st.spinner("Parsing and loading Prompt visit data…"):
                    _dbc.load_prompt_visits_file(raw, force=force_prompt)
                st.success("Prompt data loaded.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Failed to load Prompt data: {e}")

    with col_amd.expander("📂 Load / Refresh AMD Billing Data", expanded=False):
        st.caption(
            "Export AMD commercial billing (full history, May 2025–today) "
            "as Excel or CSV and upload here. Only needed when you have new AMD data."
        )
        uploaded_amd = st.file_uploader(
            "AMD Billing Export", type=["csv", "xlsx", "xls", "tsv"],
            key="amd_upload"
        )
        force_amd = st.checkbox("Force reload", value=False, key="force_amd")
        if st.button("Load AMD into Database", disabled=uploaded_amd is None):
            try:
                import db_cache as _dbc
                raw = uploaded_amd.read()
                with st.spinner("Parsing and loading AMD billing data…"):
                    _dbc.load_amd_billing(raw, force=force_amd)
                st.success("AMD billing data loaded.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Failed to load AMD data: {e}")

    with col_unc.expander("🏫 Load / Refresh UNC Chart Notes", expanded=False):
        st.caption(
            "Export the Practice Fusion Chart Notes Report (SOAP Notes only) "
            "for all UNC therapy patients, full date range. This identifies "
            "which Campus Clinics billing records belong to UNC vs ART Greeley."
        )
        uploaded_unc = st.file_uploader(
            "Practice Fusion Chart Notes CSV", type=["csv"],
            key="unc_upload"
        )
        force_unc = st.checkbox("Force reload", value=False, key="force_unc")
        if st.button("Load UNC Notes into Database", disabled=uploaded_unc is None):
            try:
                import db_cache as _dbc
                raw = uploaded_unc.read()
                with st.spinner("Parsing UNC chart notes…"):
                    _dbc.load_unc_chart_notes(raw, force=force_unc)
                st.success("UNC chart notes loaded.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Failed to load UNC notes: {e}")

    # ── Run audit ─────────────────────────────────────────────────────────────
    @st.cache_data(ttl=600, show_spinner="Running commercial audit…")
    def _run_commercial_audit():
        try:
            from commercial_audit_v2 import run_audit
            return run_audit()
        except SystemExit:
            return None
        except Exception as e:
            return {"error": str(e)}

    result = _run_commercial_audit()

    if result is None:
        st.warning(
            "AMD billing data not loaded yet. Upload your AMD export above, "
            "then this audit will run automatically."
        )
    elif "error" in result:
        st.error(f"Audit error: {result['error']}")
    else:
        s = result["stats"]

        # ── KPI Cards ─────────────────────────────────────────────────────────
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("AMD Total Charges",   fmt_dollar(s["total_amd_charges"]))
        c2.metric("AMD Insurance Paid",  fmt_dollar(s["total_amd_ins_paid"]))
        c3.metric("AMD Patient Paid",    fmt_dollar(s["total_amd_pat_paid"]))
        c4.metric("AMD Open Balance",    fmt_dollar(s["total_amd_balance"]))
        c5.metric("Prompt Commercial\n(Sep 2025+)", f"{s['prompt_commercial']:,}")

        st.divider()

        # ── Period 2 reconciliation ───────────────────────────────────────────
        st.subheader("Sep 2025+ Reconciliation (Prompt vs AMD)")
        r1, r2, r3 = st.columns(3)
        r1.metric("✅ Matched", f"{s['matched']:,}")
        r2.metric("⚠️ Unbilled (Prompt only)", f"{s['unbilled']:,}",
                  help="Prompt shows a commercial visit but no AMD billing record found")
        r3.metric("❓ Ghost Billing (AMD only)", f"{s['ghost']:,}",
                  help="AMD has a billing record but no matching Prompt visit")

        st.divider()

        # ── Charts ────────────────────────────────────────────────────────────
        detail = result["detail"]

        col_l, col_r = st.columns([2, 1])

        with col_l:
            st.subheader("AMD Charges by Insurance")
            if "Case Primary Insurance" in detail.columns:
                ins_grp = (
                    detail.groupby("Case Primary Insurance")
                    .agg(Charges=("AMD Charges", "sum"),
                         Balance=("AMD Balance", "sum"),
                         Visits=("AMD Charges", "count"))
                    .sort_values("Charges", ascending=False)
                    .head(12)
                    .reset_index()
                )
                ins_grp["Status"] = ins_grp["Balance"].apply(
                    lambda b: "Balance Remaining" if b > 0 else "Resolved"
                )
                fig_ins = px.bar(
                    ins_grp, x="Charges", y="Case Primary Insurance",
                    orientation="h", color="Status",
                    color_discrete_map={"Balance Remaining": "#E67E22", "Resolved": "#27AE60"},
                    text=ins_grp["Visits"].apply(lambda v: f"{v}"),
                    labels={"Charges": "AMD Charges ($)", "Case Primary Insurance": ""},
                )
                fig_ins.update_traces(textposition="outside")
                fig_ins.update_layout(yaxis={"categoryorder": "total ascending"},
                                      legend_title="", margin=dict(t=10, b=10))
                st.plotly_chart(fig_ins, use_container_width=True)

        with col_r:
            st.subheader("Match Status")
            status_counts = detail["Match Status"].value_counts().reset_index()
            status_counts.columns = ["Status", "Count"]
            color_map = {
                "Both": "#27AE60",
                "Prompt Only": "#E74C3C",
                "Amd Only": "#F39C12",
                "Amd Only (Pre-Prompt)": "#4A90D9",
            }
            fig_pie = px.pie(
                status_counts, names="Status", values="Count", hole=0.4,
                color="Status", color_discrete_map=color_map,
            )
            fig_pie.update_layout(margin=dict(t=10, b=10), legend_title="")
            st.plotly_chart(fig_pie, use_container_width=True)

        st.divider()

        # ── Action tables ─────────────────────────────────────────────────────
        st.subheader("Action Items")

        ub = result["unbilled"]
        if len(ub) > 0:
            with st.expander(
                f"⚠️ Unbilled Visits — {len(ub):,} Prompt records with no AMD billing",
                expanded=True
            ):
                show_cols = [c for c in ub.columns if c not in ("Patient Name",)]
                st.dataframe(ub[show_cols], use_container_width=True, hide_index=True)
                csv = ub.to_csv(index=False).encode()
                st.download_button("Download CSV", csv, "commercial_unbilled.csv", "text/csv")
        else:
            st.success("No unbilled commercial visits found.")

        ghost = result["ghost"]
        if len(ghost) > 0:
            with st.expander(f"❓ Ghost Billing — {len(ghost):,} AMD records with no Prompt visit"):
                show_cols = [c for c in ghost.columns if c not in ("Patient Name",)]
                st.dataframe(ghost[show_cols], use_container_width=True, hide_index=True)
                csv = ghost.to_csv(index=False).encode()
                st.download_button("Download CSV", csv, "commercial_ghost.csv", "text/csv")

        bal = result["open_balance"]
        if len(bal) > 0:
            with st.expander(
                f"💰 Open Balances — {len(bal):,} records  |  "
                f"{fmt_dollar(s['open_balance_amt'])} outstanding"
            ):
                show_cols = [c for c in bal.columns if c not in ("Patient Name",)]
                st.dataframe(bal[show_cols], use_container_width=True, hide_index=True)
                csv = bal.to_csv(index=False).encode()
                st.download_button("Download CSV", csv, "commercial_open_balance.csv", "text/csv")

        st.divider()
        with st.expander(f"📄 Full Detail — {len(detail):,} rows (all periods)"):
            show_cols = [c for c in detail.columns if c not in ("Patient Name",)]
            st.dataframe(detail[show_cols], use_container_width=True, hide_index=True)
            csv = detail.to_csv(index=False).encode()
            st.download_button("Download Full Detail CSV", csv,
                               "commercial_full_detail.csv", "text/csv")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — MONTHLY REPORT
# ══════════════════════════════════════════════════════════════════════════════

with tab4:
    qb_df             = load_qb_pl_data()
    prompt_df_monthly = load_prompt_data()

    if qb_df is None and prompt_df_monthly is None:
        st.error("No data available. Check Google Drive connection.")
    else:
        # ── Build month list ──────────────────────────────────────────────────
        available_months = set()
        if qb_df is not None and 'Month' in qb_df.columns:
            available_months.update(qb_df['Month'].dropna().unique().tolist())
        if prompt_df_monthly is not None and 'DOS' in prompt_df_monthly.columns:
            prompt_months = (
                prompt_df_monthly['DOS']
                .dropna()
                .dt.to_period('M')
                .astype(str)
                .unique()
                .tolist()
            )
            available_months.update(prompt_months)

        sorted_months = sorted(available_months, reverse=True)

        if not sorted_months:
            st.error("No months found in available data.")
        else:
            selected_month = st.selectbox("Select Month", sorted_months, index=0)

            # ── SECTION 1: QuickBooks P&L ─────────────────────────────────────
            st.subheader("QuickBooks P&L")

            if qb_df is None:
                st.warning("QuickBooks data unavailable.")
            else:
                month_qb = qb_df[qb_df['Month'] == selected_month]

                qb_income   = month_qb[month_qb['Type'] == 'Income']['Amount'].sum()
                qb_expenses = month_qb[month_qb['Type'] == 'Expense']['Amount'].sum()
                qb_net      = qb_income - qb_expenses

                q1, q2, q3 = st.columns(3)
                q1.metric("Total Revenue",  fmt_dollar(qb_income))
                q2.metric("Total Expenses", fmt_dollar(qb_expenses))
                q3.metric("Net Income",     fmt_dollar(qb_net),
                          delta=fmt_dollar(qb_net),
                          delta_color="normal")

                col_rev, col_exp = st.columns(2)

                with col_rev:
                    st.caption("Revenue by Category")
                    rev_by_cat = (
                        month_qb[month_qb['Type'] == 'Income']
                        .groupby('Category')['Amount']
                        .sum()
                        .sort_values(ascending=True)
                        .reset_index()
                    )
                    if not rev_by_cat.empty:
                        fig_rev = px.bar(
                            rev_by_cat, x='Amount', y='Category', orientation='h',
                            color_discrete_sequence=['#27AE60'],
                            labels={'Amount': '$', 'Category': ''},
                        )
                        fig_rev.update_layout(margin=dict(t=10, b=10), showlegend=False)
                        st.plotly_chart(fig_rev, use_container_width=True)
                    else:
                        st.caption("No revenue data for this month.")

                with col_exp:
                    st.caption("Expenses by Category")
                    exp_by_cat = (
                        month_qb[month_qb['Type'] == 'Expense']
                        .groupby('Category')['Amount']
                        .sum()
                        .sort_values(ascending=True)
                        .reset_index()
                    )
                    if not exp_by_cat.empty:
                        fig_exp = px.bar(
                            exp_by_cat, x='Amount', y='Category', orientation='h',
                            color_discrete_sequence=['#E74C3C'],
                            labels={'Amount': '$', 'Category': ''},
                        )
                        fig_exp.update_layout(margin=dict(t=10, b=10), showlegend=False)
                        st.plotly_chart(fig_exp, use_container_width=True)
                    else:
                        st.caption("No expense data for this month.")

                fac_split = (
                    month_qb.groupby(['Facility', 'Type'])['Amount']
                    .sum()
                    .unstack(fill_value=0)
                    .reset_index()
                )
                if not fac_split.empty:
                    if 'Income' not in fac_split.columns:
                        fac_split['Income'] = 0
                    if 'Expense' not in fac_split.columns:
                        fac_split['Expense'] = 0
                    fac_split['Net'] = fac_split['Income'] - fac_split['Expense']
                    fac_split = fac_split.rename(columns={
                        'Income': 'Revenue', 'Expense': 'Expenses'
                    })
                    for col in ['Revenue', 'Expenses', 'Net']:
                        fac_split[col] = fac_split[col].apply(fmt_dollar)
                    st.dataframe(fac_split, use_container_width=True, hide_index=True)

            st.divider()

            # ── SECTION 2: Clinical Performance ──────────────────────────────
            st.subheader("Clinical Performance")

            if prompt_df_monthly is None:
                st.warning("Prompt data unavailable.")
            else:
                amd_df_monthly = load_amd_data()
                mc = compute_monthly_collections(prompt_df_monthly, amd_df_monthly, selected_month)

                if not mc['amd_used']:
                    st.caption("Showing Prompt-only collections — AMD data unavailable for full picture.")

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Visits",          f"{mc['visit_count']:,}")
                c2.metric("Total Charges",   fmt_dollar(mc['total_charges']))
                c3.metric("Collected",       fmt_dollar(mc['total_collected']))
                c4.metric("AR (Open)",       fmt_dollar(mc['total_ar']))
                c5.metric("Collection Rate", fmt_pct(mc['coll_rate']))

                st.caption(
                    f"Visit stages: {mc['closed_count']} Closed · "
                    f"{mc['open_count']} Open · {mc['review_count']} Review"
                )

                col_chg, col_pay = st.columns(2)
                bp = mc['by_provider']

                with col_chg:
                    st.caption("Charges by Provider")
                    if not bp.empty:
                        fig_chg = px.bar(
                            bp.sort_values('Charges', ascending=True),
                            x='Charges', y='Provider', orientation='h',
                            color_discrete_sequence=['#4A90D9'],
                            labels={'Charges': 'Total Charges ($)', 'Provider': ''},
                        )
                        fig_chg.update_layout(margin=dict(t=10, b=10), showlegend=False)
                        st.plotly_chart(fig_chg, use_container_width=True)

                with col_pay:
                    st.caption("Collected by Provider (Prompt + AMD)")
                    if not bp.empty:
                        fig_pay = px.bar(
                            bp.sort_values('Collected', ascending=True),
                            x='Collected', y='Provider', orientation='h',
                            color_discrete_sequence=['#27AE60'],
                            labels={'Collected': 'Total Collected ($)', 'Provider': ''},
                        )
                        fig_pay.update_layout(margin=dict(t=10, b=10), showlegend=False)
                        st.plotly_chart(fig_pay, use_container_width=True)

                if mc['visit_count'] == 0:
                    st.info(f"No billing visits found with DOS in {selected_month}.")

            st.divider()

            # ── SECTION 3: EHR vs QB Comparison ──────────────────────────────
            st.subheader("EHR Collections vs QB Revenue")

            if prompt_df_monthly is not None and qb_df is not None:
                month_qb_income = qb_df[
                    (qb_df['Month'] == selected_month) & (qb_df['Type'] == 'Income')
                ]['Amount'].sum()

                ehr_collected = mc['total_collected'] if prompt_df_monthly is not None else 0
                gap = ehr_collected - month_qb_income

                e1, e2, e3 = st.columns(3)
                e1.metric("EHR Collected",   fmt_dollar(ehr_collected))
                e2.metric("QB Revenue",      fmt_dollar(month_qb_income))
                e3.metric("Gap (EHR − QB)",  fmt_dollar(gap),
                          delta=fmt_dollar(gap),
                          delta_color="normal" if gap >= 0 else "inverse")

                st.caption(
                    "QB records cash when deposited. EHR records when posted to the visit. "
                    "A small gap is normal due to timing. A large gap warrants review."
                )
            else:
                st.info("Need both Prompt and QuickBooks data to show this comparison.")

            st.divider()

            # ── SECTION 4: AR Detail (Open visits this month) ────────────────
            st.subheader("AR Detail — Open Visits This Month")

            if prompt_df_monthly is None:
                st.warning("Prompt data unavailable.")
            else:
                month_ar = prompt_df_monthly[
                    (prompt_df_monthly['DOS'].dt.to_period('M').astype(str) == selected_month) &
                    (prompt_df_monthly['Visit Stage'] == 'Open')
                ].copy()

                ar_count   = len(month_ar)
                ar_balance = month_ar['Last Billed'].sum()

                a1, a2 = st.columns(2)
                a1.metric("Open Visits", f"{ar_count:,}")
                a2.metric("Total AR (Last Billed)", fmt_dollar(ar_balance))

                if ar_count > 0:
                    with st.expander(f"📋 AR Detail — {ar_count} visits  |  {fmt_dollar(ar_balance)}"):
                        display_cols = [c for c in month_ar.columns if c != 'Patient Name']
                        st.dataframe(month_ar[display_cols], use_container_width=True,
                                     hide_index=True)
                else:
                    st.success(f"No open visits with DOS in {selected_month}.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — AUTO / PI AUDIT
# ══════════════════════════════════════════════════════════════════════════════

with tab5:
    st.subheader("Auto / Personal Injury Case Audit")
    st.caption(
        "Analyses all Prompt EMR auto visits (Sep 2025+). "
        "Direct lien patients are separated — payment expected at case settlement, not now."
    )

    col_days, col_run = st.columns([2, 1])
    unpaid_threshold = col_days.slider(
        "Flag UNPAID if billed but no payment received after N days",
        min_value=30, max_value=180, value=60, step=15
    )

    @st.cache_data(ttl=600, show_spinner="Running auto/PI audit…")
    def _run_auto_audit(threshold: int):
        try:
            from auto_audit import run_audit as _auto_run
            return _auto_run(unpaid_days=threshold)
        except SystemExit:
            return None
        except Exception as e:
            return {"error": str(e)}

    auto_result = _run_auto_audit(unpaid_threshold)

    if auto_result is None:
        st.warning("Prompt visits not loaded. Run `python db_cache.py` first.")
    elif "error" in auto_result:
        st.error(f"Audit error: {auto_result['error']}")
    else:
        s = auto_result["stats"]

        lien_df_kpi = auto_result["lien"]
        lien_balance = 0.0
        lien_by_facility = {}
        if len(lien_df_kpi) > 0:
            lien_calc = lien_df_kpi.copy()
            lien_calc["_bal"] = (
                pd.to_numeric(lien_calc["Last Billed"], errors="coerce").fillna(0)
                - pd.to_numeric(lien_calc["Total Paid"], errors="coerce").fillna(0)
            )
            lien_balance = lien_calc["_bal"].sum()
            lien_by_facility = lien_calc.groupby("Visit Facility")["_bal"].sum().to_dict()

        # ── KPI Cards ─────────────────────────────────────────────────────────
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Total Auto Visits",      f"{s['total_auto']:,}")
        c2.metric("Direct Lien (excluded)", f"{s['lien_count']:,}")
        facility_split = " · ".join(
            f"{fac.replace('ART ', '')} {fmt_dollar(bal)}"
            for fac, bal in sorted(lien_by_facility.items())
        )
        c3.metric("⚖️ Direct Lien Balance",  fmt_dollar(lien_balance),
                  help="Total billed minus paid across all direct lien (attorney-held) "
                       "cases — outstanding until each case settles. As of the latest "
                       "prompt_visits refresh.")
        if facility_split:
            c3.caption(facility_split)
        c4.metric("🔴 Unbilled",             f"{s['unbilled_count']:,}")
        c5.metric("🟠 Unpaid",               f"{s['unpaid_count']:,}",
                  help=f"Billed but $0 received after {s['unpaid_days']} days")
        c6.metric("Revenue at Risk",        fmt_dollar(s["unpaid_exposure"]))

        st.divider()

        # ── Charts ────────────────────────────────────────────────────────────
        all_df = auto_result["all_auto"]

        col_l, col_r = st.columns([2, 1])

        with col_l:
            st.subheader("Charges & Collections by Payer")
            if "Case Primary Insurance" in all_df.columns:
                payer_grp = (
                    all_df.groupby("Case Primary Insurance")
                    .agg(Billed=("Last Billed", "sum"), Paid=("Total Paid", "sum"))
                    .sort_values("Billed", ascending=False)
                    .head(12)
                    .reset_index()
                )
                payer_melt = payer_grp.melt(
                    id_vars="Case Primary Insurance",
                    value_vars=["Billed", "Paid"],
                    var_name="Metric", value_name="Amount"
                )
                fig_payer = px.bar(
                    payer_melt, x="Case Primary Insurance", y="Amount",
                    color="Metric", barmode="group",
                    color_discrete_map={"Billed": "#4A90D9", "Paid": "#27AE60"},
                    labels={"Amount": "$", "Case Primary Insurance": ""},
                )
                fig_payer.update_layout(
                    xaxis_tickangle=-35, legend_title="", margin=dict(t=10, b=80)
                )
                st.plotly_chart(fig_payer, use_container_width=True)

        with col_r:
            st.subheader("Flag Breakdown")
            flag_counts = all_df["Flag"].value_counts().reset_index()
            flag_counts.columns = ["Flag", "Count"]
            color_map_flags = {
                "OK":       "#27AE60",
                "UNBILLED": "#E74C3C",
                "UNPAID":   "#E67E22",
                "LOW_PAY":  "#F39C12",
            }
            fig_flags = px.pie(
                flag_counts, names="Flag", values="Count", hole=0.4,
                color="Flag", color_discrete_map=color_map_flags,
            )
            fig_flags.update_layout(margin=dict(t=10, b=10), legend_title="")
            st.plotly_chart(fig_flags, use_container_width=True)

        st.divider()

        # ── Action tables ─────────────────────────────────────────────────────
        st.subheader("Action Items")

        ub = auto_result["unbilled"]
        if len(ub) > 0:
            drop = ["DOS_dt", "is_lien"]
            ub_show = ub.drop(columns=[c for c in drop if c in ub.columns])
            with st.expander(
                f"🔴 Unbilled — {len(ub):,} visits with no charge submitted",
                expanded=True
            ):
                show_cols = [c for c in ub_show.columns if c not in ("Patient",)]
                st.dataframe(ub_show[show_cols], use_container_width=True, hide_index=True)
                csv = ub_show.to_csv(index=False).encode()
                st.download_button("Download CSV", csv, "auto_unbilled.csv", "text/csv")
        else:
            st.success("No unbilled auto visits found.")

        un = auto_result["unpaid"]
        if len(un) > 0:
            drop = ["DOS_dt", "is_lien"]
            un_show = un.drop(columns=[c for c in drop if c in un.columns])
            with st.expander(
                f"🟠 Unpaid — {len(un):,} visits billed but $0 received  |  "
                f"{fmt_dollar(s['unpaid_exposure'])} at risk"
            ):
                show_cols = [c for c in un_show.columns if c not in ("Patient",)]
                st.dataframe(un_show[show_cols], use_container_width=True, hide_index=True)
                csv = un_show.to_csv(index=False).encode()
                st.download_button("Download CSV", csv, "auto_unpaid.csv", "text/csv")

        lien = auto_result["lien"]
        if len(lien) > 0:
            with st.expander(f"⚖️ Direct Lien Patients — {len(lien):,} visits (payment pending case settlement)"):
                show_cols = [c for c in lien.columns if c not in ("Patient", "is_lien")]
                st.dataframe(lien[show_cols], use_container_width=True, hide_index=True)
                csv = lien.to_csv(index=False).encode()
                st.download_button("Download CSV", csv, "auto_lien.csv", "text/csv")

        st.divider()
        with st.expander(f"📄 Full Auto/PI Detail — {len(all_df):,} non-lien visits"):
            drop = ["DOS_dt", "is_lien"]
            all_show = all_df.drop(columns=[c for c in drop if c in all_df.columns])
            show_cols = [c for c in all_show.columns if c not in ("Patient",)]
            st.dataframe(all_show[show_cols], use_container_width=True, hide_index=True)
            csv = all_show.to_csv(index=False).encode()
            st.download_button("Download Full Detail CSV", csv,
                               "auto_full_detail.csv", "text/csv")

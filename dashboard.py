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
from datetime import datetime

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
    files = glob.glob("data/reconciliation_*.csv")
    if not files:
        return "No data found"
    ts = os.path.getmtime(max(files, key=os.path.getmtime))
    return datetime.fromtimestamp(ts).strftime("%b %d, %Y  %I:%M %p")


def clean_currency(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r'[\$,()]', '', regex=True),
                errors='coerce'
            ).fillna(0)
    return df


@st.cache_data(ttl=3600, show_spinner="Loading Prompt data from Google Drive…")
def load_prompt_data() -> pd.DataFrame | None:
    try:
        from Google_Drive_Access import GoogleDriveAccessor
        drive = GoogleDriveAccessor()
        drive.authenticate()
        drive.set_folder(folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)
        files = drive.list_files()
        pf = next((f for f in files if f['name'] == 'Prompt Revenue All Data.csv'), None)
        if not pf:
            return None
        content = drive.service.files().get_media(fileId=pf['id']).execute()
        df = pd.read_csv(io.BytesIO(content))
        df = clean_currency(df, [
            'Last Billed', 'Primary Allowed', 'Primary Not Allowed',
            'Patient Paid', 'Primary Insurance Paid', 'Secondary Insurance Paid',
            'Total Paid', 'Pt. Current Balance'
        ])
        df['DOS'] = pd.to_datetime(df['DOS'], errors='coerce')
        return df
    except Exception as e:
        st.warning(f"Could not load Prompt data from Drive: {e}")
        return None


@st.cache_data(ttl=3600, show_spinner="Loading QuickBooks P&L from Google Drive…")
def load_qb_pl_data():
    try:
        from quickbooks_pl_loader import QuickBooksPLLoader
        loader = QuickBooksPLLoader()
        if loader.load_pl_reports():
            return loader.combined_df
        return None
    except Exception as e:
        st.warning(f"Could not load QuickBooks P&L data: {e}")
        return None


@st.cache_data(ttl=3600, show_spinner="Loading AMD data from Google Drive…")
def load_amd_data():
    try:
        import io, json
        from Google_Drive_Access import GoogleDriveAccessor
        drive = GoogleDriveAccessor()
        drive.authenticate()
        drive.set_folder(folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)
        files = drive.list_files()
        af = next((f for f in files if f['name'] == 'AMD_data.csv'), None)
        if not af:
            return None
        content = drive.service.files().get_media(fileId=af['id']).execute()
        df = pd.read_csv(io.BytesIO(content), encoding='utf-16', sep='\t')

        for col in ['Insurance Payments', 'Patient Payments', 'Current Balance', 'Charges', 'Adjustments']:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(r'[\$,()]', '', regex=True),
                    errors='coerce'
                ).fillna(0)

        df['Service Date'] = pd.to_datetime(df.get('Service Date', pd.Series(dtype='object')), errors='coerce')
        df['DOS_key'] = df['Service Date'].dt.strftime('%Y-%m-%d')

        master_path = 'data/master_patient_list.json'
        if not os.path.exists(master_path):
            return None
        with open(master_path, 'r') as f:
            master = json.load(f)

        def _map_name(name):
            if pd.isna(name):
                return None
            n = str(name).strip().upper()
            if n in master:
                return master[n]
            parts = n.split()
            if len(parts) >= 2:
                alt = f"{parts[0]} {parts[-1]}"
                return master.get(alt)
            return None

        df['Patient Account Number'] = df.get('Patient Name (First Last)', pd.Series(dtype='object')).apply(_map_name)
        return df
    except Exception as e:
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

    by_provider = pd.DataFrame([
        {'Provider': p, 'Charges': provider_charges.get(p, 0), 'Collected': provider_collected.get(p, 0)}
        for p in set(list(provider_charges.keys()) + list(provider_collected.keys()))
    ]).sort_values('Charges', ascending=True)

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
    st.caption(f"**Data last updated**")
    st.caption(last_updated())
    st.divider()
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("Facilities")
    st.caption("• ART Denver")
    st.caption("• ART Greeley")

# ── Tabs ────────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "⚠️ AR & Action Items", "🏢 Commercial Insurance", "📅 Monthly Report"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    prompt_df = load_prompt_data()

    if prompt_df is None:
        st.error("Prompt data unavailable. Check Google Drive connection.")
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
# TAB 3 — COMMERCIAL INSURANCE
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    comm = load_latest('commercial_audit_')

    if comm is None:
        st.info("No commercial audit data found. Run `python main.py --commercial-audit` first.")
    else:
        for col in ['Last Billed', 'AMD Insurance Payments', 'AMD Adjustments',
                    'AMD Patient Payments', 'AMD Current Balance']:
            comm[col] = pd.to_numeric(comm[col].astype(str).str.replace(r'[\$,()]', '', regex=True),
                                      errors='coerce').fillna(0)

        total_visits  = len(comm)
        total_charged = comm['Last Billed'].sum()
        total_ins     = comm['AMD Insurance Payments'].sum()
        total_pat     = comm['AMD Patient Payments'].sum()
        total_balance = comm['AMD Current Balance'].sum()
        matched_count = (comm['AMD Match'] == 'Yes').sum()

        # ── KPI Cards ──────────────────────────────────────────────────────────
        st.subheader("Commercial Insurance Overview")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Commercial Visits",    f"{total_visits:,}")
        c2.metric("AMD Matched",          f"{matched_count:,} of {total_visits:,}")
        c3.metric("Total Charges",        fmt_dollar(total_charged))
        c4.metric("AMD Insurance Paid",   fmt_dollar(total_ins))
        c5.metric("AMD Balance",          fmt_dollar(total_balance))

        st.divider()

        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.subheader("Top Insurers by Charges")
            ins_grp = comm.groupby('Case Primary Insurance').agg(
                Charges=('Last Billed', 'sum'),
                Balance=('AMD Current Balance', 'sum'),
                Visits=('Last Billed', 'count'),
            ).sort_values('Charges', ascending=False).head(10).reset_index()

            ins_grp['Has Balance'] = ins_grp['Balance'].apply(
                lambda b: 'Balance Remaining' if b > 0 else 'Fully Resolved'
            )
            fig_ins = px.bar(
                ins_grp, x='Charges', y='Case Primary Insurance',
                orientation='h',
                color='Has Balance',
                color_discrete_map={'Balance Remaining': '#E67E22', 'Fully Resolved': '#27AE60'},
                text=ins_grp['Visits'].apply(lambda v: f"{v} visits"),
                labels={'Charges': 'Total Charges ($)', 'Case Primary Insurance': ''},
            )
            fig_ins.update_traces(textposition='outside')
            fig_ins.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                legend_title='', margin=dict(t=10, b=10)
            )
            st.plotly_chart(fig_ins, use_container_width=True)

        with col_right:
            st.subheader("By Facility")
            fac_grp = comm.groupby('Visit Facility').size().reset_index(name='Visits')
            fig_pie = px.pie(
                fac_grp, names='Visit Facility', values='Visits', hole=0.4,
                color_discrete_sequence=['#4A90D9', '#27AE60'],
            )
            fig_pie.update_layout(margin=dict(t=10, b=10), legend_title='')
            st.plotly_chart(fig_pie, use_container_width=True)

        st.divider()

        # ── Visit Stage by Facility ───────────────────────────────────────────
        st.subheader("Visit Stage by Facility")
        stage_fac = comm.groupby(['Visit Facility', 'Visit Stage']).agg(
            Charges=('Last Billed', 'sum')
        ).reset_index()
        fig_stacked = px.bar(
            stage_fac, x='Visit Facility', y='Charges', color='Visit Stage',
            barmode='stack',
            color_discrete_map={'Closed': '#27AE60', 'Open': '#4A90D9', 'Review': '#F39C12'},
            labels={'Charges': 'Total Charges ($)', 'Visit Facility': ''},
        )
        fig_stacked.update_layout(legend_title='', margin=dict(t=10, b=10))
        st.plotly_chart(fig_stacked, use_container_width=True)

        st.divider()

        # ── Full Detail Table ─────────────────────────────────────────────────
        with st.expander(f"📄 Full Commercial Detail — {total_visits:,} visits"):
            display_cols = [c for c in comm.columns if c != 'Patient Name']
            st.dataframe(comm[display_cols], use_container_width=True, hide_index=True)


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

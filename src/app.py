"""
M4 – Streamlit Dashboard (app.py)
─────────────────────────────────
Modern, dark-themed dashboard with:
  1. 🚀 Pipeline Control — upload mapping sheet, run ETL, view progress
  2. 📊 Analytics — KPIs, charts, table browsers
  3. 📝 SQL Workbench — ad-hoc queries with export
  4. ⚡ Benchmark — OLTP vs OLAP comparison
  5. 📋 Data Lineage — source↔target column mapping
"""

import io
import time
import os
import sys
import tempfile
from pathlib import Path

# Ensure the root project directory is in the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/databridge.duckdb")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

# ── Page config ───────────────────────────────────────
st.set_page_config(
    page_title="DataBridge — ETL Dashboard",
    page_icon="🌉",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════
#  Custom CSS — Dark Glassmorphism Theme
# ══════════════════════════════════════════════════════

st.markdown("""
<style>
    /*
    ══════════════════════════════════════════════════
     60 - 30 - 10 COLOUR RULE
    ══════════════════════════════════════════════════
     60%  DOMINANT  #0A0A0A  — deep black
                              main background, open canvas
     30%  SECONDARY #FBBF24  — warm yellow (amber)
                              sidebar, cards, surfaces, borders
     10%  ACCENT    #FFFFFF  — pure white
                              buttons, active states, headings
    ══════════════════════════════════════════════════
    */

    /* ── Import Google Font ─────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Space+Grotesk:wght@400;500;600;700;800&display=swap');

    /* ── Global ─────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        color: #F9FAFB;
    }

    /* ── 60% DOMINANT — Main background ─────────── */
    .stApp {
        background: #0A0A0A;
    }

    /* ── 60% DOMINANT — Sidebar surface ──────────── */
    section[data-testid="stSidebar"] {
        background: #111111 !important;
        border-right: 2px solid rgba(251, 191, 36, 0.40);
    }
    section[data-testid="stSidebar"] .stMarkdown h1,
    section[data-testid="stSidebar"] .stMarkdown h2,
    section[data-testid="stSidebar"] .stMarkdown h3 {
        color: #FBBF24 !important;
        -webkit-text-fill-color: #FBBF24 !important;
    }

    /* ── 30% SECONDARY — KPI metric cards ───────── */
    div[data-testid="stMetric"] {
        background: #1C1A0A;
        border: 1.5px solid rgba(251, 191, 36, 0.30);
        border-radius: 18px;
        padding: 20px 24px;
        transition: all 0.3s ease;
        box-shadow: 0 2px 12px rgba(251, 191, 36, 0.08);
    }
    div[data-testid="stMetric"]:hover {
        border-color: #FBBF24;
        box-shadow: 0 6px 24px rgba(251, 191, 36, 0.22);
        transform: translateY(-2px);
    }
    div[data-testid="stMetric"] label {
        color: #FBBF24 !important;
        font-weight: 600;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.07em;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #FFFFFF !important;
        font-weight: 800;
        font-size: 1.85rem !important;
    }

    /* ── 30% SECONDARY — Tab list ────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        background: #1C1A0A;
        border-radius: 14px;
        padding: 5px;
        gap: 4px;
        border: 1.5px solid rgba(251, 191, 36, 0.25);
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        color: #FBBF24;
        font-weight: 500;
        padding: 10px 20px;
        transition: all 0.2s ease;
    }
    /* ── 10% ACCENT — Active tab ─────────────────── */
    .stTabs [aria-selected="true"] {
        background: #FFFFFF !important;
        color: #0A0A0A !important;
        font-weight: 700;
        box-shadow: 0 2px 12px rgba(255, 255, 255, 0.25);
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: #FFFFFF;
        background: rgba(251, 191, 36, 0.12);
    }

    /* ── 30% SECONDARY — DataFrames ─────────────── */
    .stDataFrame {
        border-radius: 14px;
        overflow: hidden;
        border: 1.5px solid rgba(251, 191, 36, 0.20);
        box-shadow: 0 2px 10px rgba(251, 191, 36, 0.06);
    }

    /* ── 10% ACCENT — Buttons ────────────────────── */
    .stButton > button {
        background: #FFFFFF;
        color: #0A0A0A;
        border: none;
        border-radius: 12px;
        font-weight: 700;
        padding: 10px 24px;
        transition: all 0.3s ease;
        box-shadow: 0 4px 14px rgba(255, 255, 255, 0.20);
    }
    .stButton > button:hover {
        background: #F3F4F6;
        box-shadow: 0 6px 24px rgba(255, 255, 255, 0.30);
        transform: translateY(-1px);
    }
    .stButton > button:active {
        transform: translateY(0px);
        background: #E5E7EB;
    }

    /* ── 30% SECONDARY — Text inputs / Text areas ── */
    .stTextArea textarea, .stTextInput input {
        background: #1C1A0A !important;
        border: 1.5px solid rgba(251, 191, 36, 0.30) !important;
        border-radius: 10px !important;
        color: #F9FAFB !important;
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
    }
    /* ── 30% SECONDARY — Focus state ────────────── */
    .stTextArea textarea:focus, .stTextInput input:focus {
        border-color: #FBBF24 !important;
        box-shadow: 0 0 0 3px rgba(251, 191, 36, 0.18) !important;
    }

    /* ── 30% SECONDARY — Select boxes ───────────── */
    .stSelectbox > div > div {
        background: #1C1A0A !important;
        border: 1.5px solid rgba(251, 191, 36, 0.30) !important;
        border-radius: 10px !important;
        color: #F9FAFB !important;
    }

    /* ── 30% SECONDARY — Sliders ────────────────── */
    .stSlider > div > div > div > div {
        background: #FBBF24 !important;
    }

    /* ── 30% SECONDARY — File uploader ──────────── */
    .stFileUploader {
        background: #1C1A0A;
        border: 2px dashed rgba(251, 191, 36, 0.40);
        border-radius: 18px;
        padding: 20px;
        transition: all 0.3s ease;
    }
    .stFileUploader:hover {
        border-color: #FBBF24;
        background: rgba(251, 191, 36, 0.06);
    }

    /* ── 10% ACCENT — Progress bar ───────────────── */
    .stProgress > div > div > div {
        background: #FFFFFF !important;
        border-radius: 10px;
    }

    /* ── 30% SECONDARY — Expanders ───────────────── */
    .streamlit-expanderHeader {
        background: #1C1A0A !important;
        border-radius: 10px;
        border: 1.5px solid rgba(251, 191, 36, 0.20);
        color: #F9FAFB !important;
    }

    /* ── 30% SECONDARY — Code blocks ────────────── */
    .stCodeBlock {
        border-radius: 12px !important;
        border: 1.5px solid rgba(251, 191, 36, 0.20) !important;
    }

    /* ── Divider ──────────────────────────────────── */
    hr {
        border-color: rgba(251, 191, 36, 0.22) !important;
    }

    /* ── 10% ACCENT — Headings (Space Grotesk) ──── */
    h1, h2, h3,
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3,
    [data-testid="stMarkdownContainer"] h1,
    [data-testid="stMarkdownContainer"] h2,
    [data-testid="stMarkdownContainer"] h3 {
        font-family: 'Space Grotesk', sans-serif !important;
        color: #FFFFFF !important;
        -webkit-text-fill-color: #FFFFFF !important;
        font-weight: 700 !important;
    }

    /* ── Alerts ──────────────────────────────────── */
    .stAlert {
        border-radius: 14px !important;
        border: none !important;
        box-shadow: 0 2px 8px rgba(251, 191, 36, 0.08);
    }

    /* ── 30% SECONDARY — Header pill ────────────── */
    .header-pill {
        display: inline-block;
        background: #FBBF24;
        border-radius: 20px;
        padding: 6px 16px;
        font-size: 0.78rem;
        color: #0A0A0A;
        font-weight: 700;
        letter-spacing: 0.06em;
    }

    /* ── 30% SECONDARY — Content cards ──────────── */
    .lineage-card {
        background: #1C1A0A;
        border: 1.5px solid rgba(251, 191, 36, 0.22);
        border-radius: 16px;
        padding: 20px;
        margin-bottom: 12px;
        box-shadow: 0 2px 10px rgba(251, 191, 36, 0.06);
    }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════════════

@st.cache_resource
def _duck_connect():
    return duckdb.connect(DUCKDB_PATH)


@st.cache_resource
def _pg_connect():
    """Attempt to connect to PostgreSQL. Returns engine or None."""
    try:
        url = (
            f"postgresql+psycopg2://"
            f"{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
            f"@{os.getenv('PG_HOST', 'localhost')}:{os.getenv('PG_PORT', '5432')}"
            f"/{os.getenv('PG_DATABASE')}"
        )
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception:
        return None


def _tables(conn) -> list[str]:
    return [r[0] for r in conn.execute("SHOW TABLES").fetchall()]


def _bench_query(executor, sql: str, iterations: int) -> list[float]:
    """Run a query multiple times and return execution times in ms."""
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        executor(sql)
        times.append((time.perf_counter() - t0) * 1000)
    return times


def _safe_query(conn, sql: str) -> pd.DataFrame:
    """Execute a query and return a DataFrame, handling errors gracefully."""
    try:
        return conn.execute(sql).fetchdf()
    except Exception:
        return pd.DataFrame()


# ══════════════════════════════════════════════════════
#  Sidebar — Navigation & Status
# ══════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("# 🌉 DataBridge")
    st.markdown('<span class="header-pill">PostgreSQL → DuckDB ETL</span>', unsafe_allow_html=True)
    st.markdown("---")

    # Navigation
    page = st.radio(
        "Navigation",
        [
            "🚀 Pipeline Control",
            "📊 Analytics",
            "📝 SQL Workbench",
            "⚡ Benchmark",
            "📋 Data Lineage",
        ],
        label_visibility="collapsed",
    )

    st.markdown("---")

    # Connection status
    st.markdown("### 🔌 Connections")
    pg_engine = _pg_connect()
    if pg_engine:
        st.success("✅ PostgreSQL")
    else:
        st.warning("⚠️ PostgreSQL offline")

    try:
        conn = _duck_connect()
        tables = _tables(conn)
        st.success(f"✅ DuckDB ({len(tables)} tables)")
    except Exception:
        tables = []
        conn = None
        st.error("❌ DuckDB unavailable")

    if tables:
        st.markdown("---")
        st.markdown("### 📋 Tables")
        for t in tables:
            icon = "🔷" if t.startswith("dim_") else "🔶" if t.startswith("fact_") else "⬜"
            st.markdown(f" {icon} `{t}`")


# ══════════════════════════════════════════════════════
#  Page 1: Pipeline Control
# ══════════════════════════════════════════════════════

if page == "🚀 Pipeline Control":
    st.markdown("# 🚀 Pipeline Control")
    st.markdown("Upload the mapping sheet and run the three-phase ETL pipeline.")
    st.markdown("---")

    col_upload, col_status = st.columns([3, 2])

    with col_upload:
        st.markdown("### 📄 Mapping Sheet")
        uploaded_file = st.file_uploader(
            "Upload DataBridge mapping sheet (.xlsx)",
            type=["xlsx"],
            help="This Excel file defines source→target column mappings and transformation rules.",
        )

        if uploaded_file is not None:
            # Parse and preview the mapping sheet
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name

            try:
                from src.transform import parse_mapping_sheet
                mapping = parse_mapping_sheet(tmp_path)
                st.session_state["parsed_mapping"] = mapping

                st.markdown("#### ✅ Mapping Parsed Successfully")

                for target_table, rules in mapping["mappings"].items():
                    with st.expander(f"📋 **{target_table}** ({len(rules)} columns)", expanded=True):
                        rules_df = pd.DataFrame(rules)
                        st.dataframe(
                            rules_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "target_col": st.column_config.TextColumn("Target Column", width="medium"),
                                "target_dtype": st.column_config.TextColumn("Type", width="small"),
                                "source_table": st.column_config.TextColumn("Source Table", width="medium"),
                                "source_col": st.column_config.TextColumn("Source Column", width="medium"),
                                "transform_rule": st.column_config.TextColumn("Transform Rule", width="large"),
                            },
                        )

                st.markdown(f"**Required source tables:** `{'`, `'.join(sorted(mapping['required_source_tables']))}`")

            except Exception as e:
                st.error(f"Failed to parse mapping sheet: {e}")
                tmp_path = None

    with col_status:
        st.markdown("### ⚙️ Pipeline Status")

        # Check if star-schema tables exist
        has_dim = "dim_account_customer" in tables if tables else False
        has_fact = "fact_transactions" in tables if tables else False

        if has_dim and has_fact:
            try:
                dim_count = conn.execute("SELECT COUNT(*) FROM dim_account_customer").fetchone()[0]
                fact_count = conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
                st.metric("Dimension Rows", f"{dim_count:,}")
                st.metric("Fact Rows", f"{fact_count:,}")
                st.info(f"🕐 Star schema is loaded")
            except Exception:
                st.warning("Star schema tables exist but may be empty")
        else:
            st.warning("Star schema not yet loaded. Run the pipeline to create dim & fact tables.")

    st.markdown("---")

    # Run Pipeline button
    if uploaded_file is not None and tmp_path:
        st.markdown("### 🏗️ Execute Pipeline")

        col_run, col_opts = st.columns([1, 2])

        run_pipeline = col_run.button("▶️ Run ETL Pipeline", type="primary", use_container_width=True)

        if run_pipeline:
            progress_bar = st.progress(0.0, text="Initializing pipeline…")
            status_text = st.empty()
            phase_container = st.container()

            def update_progress(phase: str, msg: str, pct: float):
                progress_bar.progress(min(pct, 1.0), text=f"**{phase}** — {msg}")

            try:
                from src.pipeline import run_mapping_pipeline

                with st.spinner("Running three-phase ETL pipeline…"):
                    meta = run_mapping_pipeline(
                        tmp_path,
                        progress_callback=update_progress,
                    )

                progress_bar.progress(1.0, text="✅ Pipeline complete!")
                st.balloons()

                # Show results
                col_r1, col_r2, col_r3, col_r4 = st.columns(4)
                col_r1.metric("Status", "✅ Done")
                col_r2.metric("Duration", f"{meta.duration_seconds:.1f}s")
                col_r3.metric("Dim Rows", f"{meta.dim_row_count:,}")
                col_r4.metric("Fact Rows", f"{meta.fact_row_count:,}")

                st.success(
                    f"Pipeline completed successfully! "
                    f"Extracted {len(meta.tables_extracted)} tables → "
                    f"Built star schema → Loaded into DuckDB"
                )

                # Invalidate cached connection to pick up new tables
                st.cache_resource.clear()

            except Exception as e:
                progress_bar.progress(0.0, text="❌ Pipeline failed!")
                st.error(f"Pipeline error: {e}")

    elif uploaded_file is None:
        st.info("👆 Upload a mapping sheet above to enable the pipeline.")


# ══════════════════════════════════════════════════════
#  Page 2: Analytics Dashboard
# ══════════════════════════════════════════════════════

elif page == "📊 Analytics":
    st.markdown("# 📊 Analytics Dashboard")
    st.markdown("Interactive exploration of the star-schema data.")
    st.markdown("---")

    if not tables:
        st.warning("No tables found in DuckDB. Run the ETL pipeline first.")
        st.stop()

    has_star_schema = "dim_account_customer" in tables and "fact_transactions" in tables

    if has_star_schema:
        # ── KPI Row ───────────────────────────────────
        dim_df = _safe_query(conn, "SELECT * FROM dim_account_customer")
        fact_df = _safe_query(conn, "SELECT * FROM fact_transactions")

        if not dim_df.empty and not fact_df.empty:
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("👥 Total Customers", f"{dim_df['customer_id'].nunique():,}")
            kpi2.metric("🏦 Total Accounts", f"{len(dim_df):,}")
            kpi3.metric("💳 Total Transactions", f"{len(fact_df):,}")
            kpi4.metric("💰 Total Value (₹)", f"₹{fact_df['amount'].sum():,.0f}")

            st.markdown("---")

            # ── Charts Row 1 ─────────────────────────────
            chart1, chart2 = st.columns(2)

            with chart1:
                st.markdown("### Transactions by Type")
                type_counts = fact_df["transaction_type"].value_counts().reset_index()
                type_counts.columns = ["Transaction Type", "Count"]
                fig = px.bar(
                    type_counts,
                    x="Transaction Type",
                    y="Count",
                    color="Count",
                    color_continuous_scale=["#78350f", "#b45309", "#FBBF24"],
                    template="plotly_dark",
                )
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="#111111",
                    font=dict(family="Inter", color="#F9FAFB"),
                    showlegend=False,
                    coloraxis_showscale=False,
                )
                fig.update_traces(marker_line_width=0, marker_cornerradius=6)
                st.plotly_chart(fig, use_container_width=True)

            with chart2:
                st.markdown("### Transactions Over Time")
                fact_df_copy = fact_df.copy()
                fact_df_copy["transaction_date"] = pd.to_datetime(fact_df_copy["transaction_date"])
                daily = fact_df_copy.groupby("transaction_date").agg(
                    count=("transaction_id", "count"),
                    total=("amount", "sum"),
                ).reset_index()
                fig2 = px.area(
                    daily,
                    x="transaction_date",
                    y="total",
                    template="plotly_dark",
                    labels={"total": "Amount (₹)", "transaction_date": "Date"},
                )
                fig2.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="#111111",
                    font=dict(family="Inter", color="#F9FAFB"),
                )
                fig2.update_traces(
                    fill="tozeroy",
                    line=dict(color="#FBBF24", width=2.5),
                    fillcolor="rgba(251, 191, 36, 0.15)",
                )
                st.plotly_chart(fig2, use_container_width=True)

            # ── Charts Row 2 ─────────────────────────────
            chart3, chart4 = st.columns(2)

            with chart3:
                st.markdown("### Accounts by Branch")
                branch_counts = dim_df["branch_name"].value_counts().head(10).reset_index()
                branch_counts.columns = ["Branch", "Accounts"]
                fig3 = px.bar(
                    branch_counts,
                    x="Accounts",
                    y="Branch",
                    orientation="h",
                    color="Accounts",
                    color_continuous_scale=["#78350f", "#FBBF24"],
                    template="plotly_dark",
                )
                fig3.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="#111111",
                    font=dict(family="Inter", color="#F9FAFB"),
                    showlegend=False,
                    coloraxis_showscale=False,
                    yaxis=dict(autorange="reversed"),
                )
                fig3.update_traces(marker_line_width=0, marker_cornerradius=4)
                st.plotly_chart(fig3, use_container_width=True)

            with chart4:
                st.markdown("### Top 10 Customers by Volume")
                top_customers = (
                    fact_df.merge(
                        dim_df[["account_sk", "customer_name"]],
                        on="account_sk",
                        how="left",
                    )
                    .groupby("customer_name")["amount"]
                    .sum()
                    .nlargest(10)
                    .reset_index()
                )
                top_customers.columns = ["Customer", "Total Amount"]
                fig4 = px.bar(
                    top_customers,
                    x="Total Amount",
                    y="Customer",
                    orientation="h",
                    color="Total Amount",
                    color_continuous_scale=["#78350f", "#b45309", "#FBBF24"],
                    template="plotly_dark",
                )
                fig4.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="#111111",
                    font=dict(family="Inter", color="#F9FAFB"),
                    showlegend=False,
                    coloraxis_showscale=False,
                    yaxis=dict(autorange="reversed"),
                )
                fig4.update_traces(marker_line_width=0, marker_cornerradius=4)
                st.plotly_chart(fig4, use_container_width=True)

            # ── Active Card Distribution ──────────────────
            st.markdown("---")
            st.markdown("### 💳 Active Card Distribution")
            card_dist = dim_df["has_active_card"].value_counts().reset_index()
            card_dist.columns = ["Has Active Card", "Count"]
            card_dist["Has Active Card"] = card_dist["Has Active Card"].map({True: "Active", False: "Inactive"})
            fig5 = px.pie(
                card_dist,
                values="Count",
                names="Has Active Card",
                color_discrete_sequence=["#FBBF24", "#1C1A0A"],
                template="plotly_dark",
                hole=0.55,
            )
            fig5.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", color="#F9FAFB"),
            )
            st.plotly_chart(fig5, use_container_width=True)

        st.markdown("---")

        # ── Table Browser ─────────────────────────────
        st.markdown("### 🔍 Table Browser")
        browse_table = st.selectbox("Select table", tables, key="analytics_table")
        limit = st.slider("Row limit", 10, 1000, 100, step=10, key="analytics_limit")
        preview_df = conn.execute(f"SELECT * FROM {browse_table} LIMIT {limit}").fetchdf()
        st.dataframe(preview_df, use_container_width=True, hide_index=True)

        col1, col2, col3 = st.columns(3)
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {browse_table}").fetchone()[0]
        col1.metric("Total Rows", f"{total_rows:,}")
        col2.metric("Columns", len(preview_df.columns))
        col3.metric("Preview Size (KB)", f"{preview_df.memory_usage(deep=True).sum() / 1024:.1f}")

    else:
        st.info(
            "📈 Star schema tables (`dim_account_customer`, `fact_transactions`) not found. "
            "Run the pipeline first, or browse raw tables below."
        )

        # Fallback: show raw table browser
        if tables:
            st.markdown("### 🔍 Raw Table Browser")
            selected = st.selectbox("Select table", tables, key="raw_table")
            limit = st.slider("Row limit", 10, 1000, 100, step=10, key="raw_limit")
            preview_df = conn.execute(f"SELECT * FROM {selected} LIMIT {limit}").fetchdf()
            st.dataframe(preview_df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════
#  Page 3: SQL Workbench
# ══════════════════════════════════════════════════════

elif page == "📝 SQL Workbench":
    st.markdown("# 📝 SQL Workbench")
    st.markdown("Run ad-hoc queries against the DuckDB analytical store.")
    st.markdown("---")

    if not tables or conn is None:
        st.warning("DuckDB not available. Run the ETL pipeline first.")
        st.stop()

    # Schema reference
    with st.expander("📋 Table Schema Reference", expanded=False):
        for t in tables:
            cols_df = conn.execute(
                f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{t}'"
            ).fetchdf()
            st.markdown(f"**`{t}`**")
            st.dataframe(cols_df, use_container_width=True, hide_index=True, height=150)

    # Query templates
    template_options = {
        "-- Select a template --": "",
        "Preview dimension table": "SELECT * FROM dim_account_customer LIMIT 50",
        "Preview fact table": "SELECT * FROM fact_transactions LIMIT 50",
        "Transaction volume by type": (
            "SELECT transaction_type, COUNT(*) AS txn_count, SUM(amount) AS total_amount, "
            "ROUND(AVG(amount), 2) AS avg_amount "
            "FROM fact_transactions GROUP BY transaction_type ORDER BY total_amount DESC"
        ),
        "Customer account summary": (
            "SELECT customer_name, customer_state, account_type, has_active_card "
            "FROM dim_account_customer ORDER BY customer_name LIMIT 50"
        ),
        "Top accounts by transaction value": (
            "SELECT d.customer_name, d.account_type, d.branch_name, "
            "SUM(f.amount) AS total_spent "
            "FROM fact_transactions f "
            "JOIN dim_account_customer d ON f.account_sk = d.account_sk "
            "GROUP BY d.customer_name, d.account_type, d.branch_name "
            "ORDER BY total_spent DESC LIMIT 15"
        ),
        # ── Dimensional joins ─────────────────────────
        "Transactions by Quarter (dim_date join)": (
            "SELECT d.year, d.quarter,\n"
            "       COUNT(*) AS txn_count,\n"
            "       ROUND(SUM(f.amount), 2) AS total_amount\n"
            "FROM fact_transactions f\n"
            "JOIN dim_date d ON f.date_sk = d.date_sk\n"
            "GROUP BY d.year, d.quarter\n"
            "ORDER BY d.year, d.quarter"
        ),
        "Branch performance (three-way join)": (
            "SELECT b.branch_name, b.city,\n"
            "       COUNT(*) AS txns,\n"
            "       ROUND(SUM(f.amount), 2) AS volume\n"
            "FROM fact_transactions f\n"
            "JOIN dim_account_customer a ON f.account_sk = a.account_sk\n"
            "JOIN dim_branch b ON f.branch_sk = b.branch_sk\n"
            "WHERE b.is_current = TRUE\n"
            "GROUP BY b.branch_name, b.city\n"
            "ORDER BY volume DESC"
        ),
        # ── Window functions ──────────────────────────
        "Running total per account (LAG + cumulative SUM)": (
            "SELECT transaction_id, account_sk,\n"
            "       transaction_date, amount,\n"
            "       SUM(amount) OVER (\n"
            "           PARTITION BY account_sk\n"
            "           ORDER BY transaction_date\n"
            "       ) AS running_total,\n"
            "       amount - LAG(amount, 1, 0) OVER (\n"
            "           PARTITION BY account_sk\n"
            "           ORDER BY transaction_date\n"
            "       ) AS delta_from_prev\n"
            "FROM fact_transactions\n"
            "ORDER BY account_sk, transaction_date"
        ),
        "Account rank per branch (RANK OVER)": (
            "SELECT a.branch_name, a.customer_name,\n"
            "       ROUND(SUM(f.amount), 2) AS total_spent,\n"
            "       RANK() OVER (\n"
            "           PARTITION BY a.branch_name\n"
            "           ORDER BY SUM(f.amount) DESC\n"
            "       ) AS branch_rank\n"
            "FROM fact_transactions f\n"
            "JOIN dim_account_customer a ON f.account_sk = a.account_sk\n"
            "WHERE a.is_current = TRUE\n"
            "GROUP BY a.branch_name, a.customer_name\n"
            "ORDER BY a.branch_name, branch_rank"
        ),
        # ── CTE ───────────────────────────────────────
        "Monthly cohort summary (CTE)": (
            "WITH monthly AS (\n"
            "    SELECT d.year, d.month, d.month_name,\n"
            "           COUNT(*) AS txn_count,\n"
            "           ROUND(SUM(f.amount), 2) AS total_amount,\n"
            "           ROUND(AVG(f.amount), 2) AS avg_amount\n"
            "    FROM fact_transactions f\n"
            "    JOIN dim_date d ON f.date_sk = d.date_sk\n"
            "    GROUP BY d.year, d.month, d.month_name\n"
            ")\n"
            "SELECT * FROM monthly ORDER BY year, month"
        ),
        # ── SCD Type 2 history queries ────────────────
        "SCD2: Full version history for account_id = 1": (
            "SELECT account_id, account_type, branch_name, has_active_card,\n"
            "       version, effective_from, effective_to, is_current\n"
            "FROM dim_account_customer\n"
            "WHERE account_id = 1\n"
            "ORDER BY version"
        ),
        "SCD2: Accounts that changed type this year": (
            "SELECT account_id, customer_name, account_type, effective_from\n"
            "FROM dim_account_customer\n"
            "WHERE version > 1\n"
            "  AND EXTRACT(year FROM CAST(effective_from AS DATE)) = EXTRACT(year FROM CURRENT_DATE)\n"
            "ORDER BY effective_from DESC"
        ),
        "SCD2: Point-in-time branch lookup for 2024-03-01": (
            "SELECT branch_id, branch_name, city, state,\n"
            "       effective_from, effective_to, is_current\n"
            "FROM dim_branch\n"
            "WHERE effective_from <= DATE '2024-03-01'\n"
            "  AND (effective_to > DATE '2024-03-01' OR effective_to IS NULL)\n"
            "ORDER BY branch_id"
        ),
    }

    selected_template = st.selectbox(
        "Query Templates",
        list(template_options.keys()),
        key="sql_template",
    )

    default_sql = template_options.get(selected_template, "") or f"SELECT * FROM {tables[0]} LIMIT 50"

    sql = st.text_area(
        "SQL Query",
        value=default_sql,
        height=150,
        key="sql_workbench",
    )

    col_run, col_export = st.columns([1, 4])
    run_query = col_run.button("▶ Run Query", type="primary")

    if run_query:
        t0 = time.perf_counter()
        try:
            result = conn.execute(sql).fetchdf()
            elapsed = (time.perf_counter() - t0) * 1000

            st.markdown(f"**{len(result):,} rows** returned in **{elapsed:.1f} ms**")
            st.dataframe(result, use_container_width=True, hide_index=True)

            # Export button
            csv_data = result.to_csv(index=False)
            st.download_button(
                "⬇️ Export CSV",
                data=csv_data,
                file_name="query_result.csv",
                mime="text/csv",
            )

        except Exception as exc:
            st.error(f"Query error: {exc}")


# ══════════════════════════════════════════════════════
#  Page 4: Benchmark
# ══════════════════════════════════════════════════════

elif page == "⚡ Benchmark":
    st.markdown("# ⚡ OLTP vs OLAP Query Analysis")
    st.markdown(
        "Compare the **same aggregation query** on PostgreSQL (row-store / OLTP) "
        "vs DuckDB (columnar / OLAP) to observe the performance difference."
    )
    st.markdown("---")

    if not tables or conn is None:
        st.warning("DuckDB not available.")
        st.stop()

    # All DuckDB tables (star schema + any others) exist in PostgreSQL DBANK_SRC,
    # so we show every table — true OLAP vs OLTP comparison on identical data.
    selected = st.selectbox("Table", tables, key="bench_table")

    # Pre-built query templates
    templates = {
        "Row count": f"SELECT COUNT(*) FROM {selected}",
        "Full scan (SELECT *)": f"SELECT * FROM {selected}",
        "GROUP BY aggregation": f"SELECT COUNT(*) AS cnt FROM {selected} GROUP BY 1",
        "Custom": "",
    }

    template = st.selectbox("Query template", list(templates.keys()), key="bench_template")

    if template == "Custom":
        bench_sql = st.text_area(
            "Custom query",
            value=f"SELECT COUNT(*) FROM {selected}",
            height=100,
            key="bench_custom",
        )
    elif template == "GROUP BY aggregation":
        cols = conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{selected}'"
        ).fetchdf()["column_name"].tolist()

        if cols:
            group_col = st.selectbox("Group by column", cols, key="bench_group_col")
            bench_sql = f'SELECT "{group_col}", COUNT(*) AS cnt FROM {selected} GROUP BY "{group_col}"'
        else:
            bench_sql = templates[template]
    else:
        bench_sql = templates[template]

    st.code(bench_sql, language="sql")
    iterations = st.number_input("Iterations", 1, 100, 10, key="bench_iters")

    if st.button("🚀 Run Benchmark", type="primary"):
        col_duck, col_pg = st.columns(2)

        # ── DuckDB benchmark ─────────────────────────
        with col_duck:
            st.markdown("### 🦆 DuckDB (OLAP)")
            duck_times = _bench_query(
                lambda q: conn.execute(q).fetchall(), bench_sql, iterations
            )
            duck_df = pd.DataFrame(
                {"iteration": range(1, iterations + 1), "ms": duck_times}
            )
            st.metric("Mean (ms)", f"{duck_df['ms'].mean():.2f}")
            st.metric("Median (ms)", f"{duck_df['ms'].median():.2f}")

            fig_duck = px.line(
                duck_df, x="iteration", y="ms",
                template="plotly_dark",
                labels={"ms": "Time (ms)", "iteration": "Iteration"},
            )
            fig_duck.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="#111111",
                font=dict(family="Inter", color="#F9FAFB"),
            )
            fig_duck.update_traces(line=dict(color="#FBBF24", width=2.5))
            st.plotly_chart(fig_duck, use_container_width=True)

        # ── PostgreSQL benchmark ─────────────────────
        with col_pg:
            st.markdown("### 🐘 PostgreSQL (OLTP)")
            if pg_engine is not None:
                pg_bench_sql = bench_sql.replace(
                    f"FROM {selected}", f'FROM "{PG_SCHEMA}"."{selected}"'
                ).replace(
                    f"from {selected}", f'from "{PG_SCHEMA}"."{selected}"'
                )

                def _pg_exec(q):
                    with pg_engine.connect() as c:
                        c.execute(text(q))

                pg_times = _bench_query(_pg_exec, pg_bench_sql, iterations)
                pg_df = pd.DataFrame(
                    {"iteration": range(1, iterations + 1), "ms": pg_times}
                )
                st.metric("Mean (ms)", f"{pg_df['ms'].mean():.2f}")
                st.metric("Median (ms)", f"{pg_df['ms'].median():.2f}")

                fig_pg = px.line(
                    pg_df, x="iteration", y="ms",
                    template="plotly_dark",
                    labels={"ms": "Time (ms)", "iteration": "Iteration"},
                )
                fig_pg.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="#111111",
                    font=dict(family="Inter", color="#F9FAFB"),
                )
                fig_pg.update_traces(line=dict(color="#FFFFFF", width=2.5))
                st.plotly_chart(fig_pg, use_container_width=True)
            else:
                pg_times = None
                st.warning(
                    "PostgreSQL is not connected. Configure `.env` and "
                    "restart the dashboard to enable OLTP comparison."
                )

        # ── Comparison ────────────────────────────────
        st.divider()
        st.markdown("### 📊 Side-by-Side Comparison")

        if pg_engine is not None and pg_times is not None:
            duck_mean = duck_df["ms"].mean()
            pg_mean = pg_df["ms"].mean()

            comparison = pd.DataFrame({
                "Engine": ["PostgreSQL (OLTP)", "DuckDB (OLAP)"],
                "Mean (ms)": [pg_mean, duck_mean],
                "Median (ms)": [
                    pg_df["ms"].median(), duck_df["ms"].median()
                ],
                "Std Dev (ms)": [
                    pg_df["ms"].std(), duck_df["ms"].std()
                ],
            })

            st.dataframe(comparison, use_container_width=True, hide_index=True)

            fig_compare = px.bar(
                comparison, x="Engine", y="Mean (ms)",
                color="Engine",
                color_discrete_sequence=["#FFFFFF", "#FBBF24"],
                template="plotly_dark",
            )
            fig_compare.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="#111111",
                font=dict(family="Inter", color="#F9FAFB"),
                showlegend=False,
            )
            fig_compare.update_traces(marker_line_width=0, marker_cornerradius=6)
            st.plotly_chart(fig_compare, use_container_width=True)

            # Speedup factor
            if duck_mean > 0:
                speedup = pg_mean / duck_mean
                if speedup > 1:
                    st.success(
                        f"🚀 **DuckDB was {speedup:.1f}× faster** than "
                        f"PostgreSQL on this query!"
                    )
                else:
                    factor = duck_mean / pg_mean if pg_mean > 0 else 0
                    st.info(
                        f"PostgreSQL was {factor:.1f}× faster on this query. "
                        f"This is expected for small row-lookup operations."
                    )
        else:
            st.info(
                "Connect PostgreSQL to see the full OLTP vs OLAP comparison. "
                "DuckDB-only results are shown above."
            )


# ══════════════════════════════════════════════════════
#  Page 5: Data Lineage
# ══════════════════════════════════════════════════════

elif page == "📋 Data Lineage":
    st.markdown("# 📋 Data Lineage")
    st.markdown("Trace every column from source to target with transformation rules.")
    st.markdown("---")

    # ── Live mapping from uploaded sheet ─────────────
    mapping_data = st.session_state.get("parsed_mapping", None)

    if mapping_data:
        for target_table, rules in mapping_data["mappings"].items():
            icon = "🔷" if target_table.startswith("dim_") else "🔶"
            st.markdown(f"### {icon} {target_table}")
            rules_df = pd.DataFrame(rules)
            st.dataframe(
                rules_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "target_col":     st.column_config.TextColumn("Target Column", width="medium"),
                    "target_dtype":   st.column_config.TextColumn("Type", width="small"),
                    "source_table":   st.column_config.TextColumn("Source Table", width="medium"),
                    "source_col":     st.column_config.TextColumn("Source Column", width="medium"),
                    "transform_rule": st.column_config.TextColumn("Transform Rule", width="medium"),
                    "enricher":       st.column_config.TextColumn("Enricher", width="medium"),
                    "scd_type":       st.column_config.TextColumn("SCD Type", width="small"),
                },
            )
            st.markdown("---")
    else:
        st.info(
            "Upload a mapping sheet on the **🚀 Pipeline Control** page to see live lineage. "
            "Showing fallback static view below."
        )
        dim_mappings = [
            {"Target": "account_sk",      "Type": "INTEGER", "Source": "—",                          "Transform": "Auto-generated surrogate key"},
            {"Target": "account_id",      "Type": "INTEGER", "Source": "accounts.account_id",         "Transform": "Direct mapping (Natural Key)"},
            {"Target": "customer_id",     "Type": "INTEGER", "Source": "customers.customer_id",       "Transform": "INNER JOIN accounts ↔ customers"},
            {"Target": "customer_name",   "Type": "VARCHAR", "Source": "customers.full_name",         "Transform": "Direct mapping (renamed)"},
            {"Target": "customer_state",  "Type": "VARCHAR", "Source": "customers.state",             "Transform": "Direct mapping (renamed)"},
            {"Target": "branch_name",     "Type": "VARCHAR", "Source": "branches.branch_name",        "Transform": "LEFT JOIN on branch_id"},
            {"Target": "account_type",    "Type": "VARCHAR", "Source": "accounts.account_type",       "Transform": "Direct mapping"},
            {"Target": "has_active_card", "Type": "BOOLEAN", "Source": "cards.is_active",             "Transform": "Derived: ANY(is_active) grouped by account_id"},
            {"Target": "effective_from",  "Type": "DATE",    "Source": "—",                          "Transform": "SCD2: date row became current"},
            {"Target": "effective_to",    "Type": "DATE",    "Source": "—",                          "Transform": "SCD2: date row expired (NULL = current)"},
            {"Target": "is_current",      "Type": "BOOLEAN", "Source": "—",                          "Transform": "SCD2: TRUE for current version"},
            {"Target": "version",         "Type": "INTEGER", "Source": "—",                          "Transform": "SCD2: version number (1 = original)"},
        ]
        fact_mappings = [
            {"Target": "transaction_id",   "Type": "INTEGER", "Source": "transactions.transaction_id",  "Transform": "Direct mapping"},
            {"Target": "account_sk",       "Type": "INTEGER", "Source": "dim_account_customer",         "Transform": "Lookup: account_id → current account_sk"},
            {"Target": "date_sk",          "Type": "INTEGER", "Source": "dim_date",                     "Transform": "Lookup: transaction_date → date_sk"},
            {"Target": "branch_sk",        "Type": "INTEGER", "Source": "dim_branch",                   "Transform": "Lookup: account→branch_id → branch_sk"},
            {"Target": "transaction_date", "Type": "DATE",    "Source": "transactions.txn_timestamp",   "Transform": "Cast: TIMESTAMP → DATE"},
            {"Target": "transaction_type", "Type": "VARCHAR", "Source": "transaction_types.type_name",  "Transform": "LEFT JOIN on type_id"},
            {"Target": "amount",           "Type": "DECIMAL", "Source": "transactions.amount",          "Transform": "Direct mapping"},
        ]
        st.markdown("### 🔷 dim_account_customer")
        st.dataframe(pd.DataFrame(dim_mappings), use_container_width=True, hide_index=True)
        st.markdown("---")
        st.markdown("### 🔶 fact_transactions")
        st.dataframe(pd.DataFrame(fact_mappings), use_container_width=True, hide_index=True)
        st.markdown("---")

    # Data Quality Summary
    st.markdown("### 📊 Data Quality Summary")

    if conn and "dim_account_customer" in tables and "fact_transactions" in tables:
        dq1, dq2 = st.columns(2)

        with dq1:
            st.markdown("**dim_account_customer**")
            dim_df = _safe_query(conn, "SELECT * FROM dim_account_customer")
            if not dim_df.empty:
                quality = []
                for col in dim_df.columns:
                    null_count = dim_df[col].isna().sum()
                    distinct = dim_df[col].nunique()
                    quality.append({
                        "Column": col,
                        "Nulls": null_count,
                        "Null %": f"{null_count / len(dim_df) * 100:.1f}%",
                        "Distinct": distinct,
                    })
                st.dataframe(pd.DataFrame(quality), use_container_width=True, hide_index=True)

        with dq2:
            st.markdown("**fact_transactions**")
            fact_df = _safe_query(conn, "SELECT * FROM fact_transactions")
            if not fact_df.empty:
                quality = []
                for col in fact_df.columns:
                    null_count = fact_df[col].isna().sum()
                    distinct = fact_df[col].nunique()
                    quality.append({
                        "Column": col,
                        "Nulls": null_count,
                        "Null %": f"{null_count / len(fact_df) * 100:.1f}%",
                        "Distinct": distinct,
                    })
                st.dataframe(pd.DataFrame(quality), use_container_width=True, hide_index=True)
    else:
        st.info("Run the ETL pipeline first to see data quality metrics.")

    st.markdown("---")

    # Architecture diagram
    st.markdown("### 🏗️ Pipeline Architecture")
    st.markdown("""
    ```
    ┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────┐
    │  PostgreSQL  │ ──▶ │   Parquet Files   │ ──▶ │  Star Schema     │ ──▶ │   DuckDB     │
    │  (OLTP)      │     │   (Landing Zone)  │     │  (Transform)     │     │   (OLAP)     │
    │              │     │                   │     │                  │     │              │
    │ • accounts   │     │ • accounts.pq     │     │ dim_account_     │     │ dim_account_ │
    │ • customers  │     │ • customers.pq    │     │   customer       │     │   customer   │
    │ • branches   │     │ • branches.pq     │     │                  │     │              │
    │ • cards      │     │ • cards.pq        │     │ fact_            │     │ fact_        │
    │ • txn_types  │     │ • txn_types.pq    │     │   transactions   │     │   trans.     │
    │ • txns       │     │ • txns.pq         │     │                  │     │              │
    └──────────────┘     └──────────────────┘     └──────────────────┘     └──────────────┘
         Phase 1               Phase 1                  Phase 2                Phase 3
        EXTRACT              EXTRACT                 TRANSFORM                LOAD
    ```
    """)

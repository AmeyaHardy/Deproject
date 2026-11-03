"""
Streamlit Serving & Observability Dashboard
===========================================
Real-time operational monitoring and post-hoc ground-truth evaluation for
UNSW-NB15 network anomaly detection running on Apache Spark & PostgreSQL.

RBAC Role: dashboard_reader (strictly read-only, features column denied).
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dashboard import queries

# Page configuration
st.set_page_config(
    page_title="Network Anomaly Detection | Observability Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 14px;
        border-left: 4px solid #1f77b4;
        margin-bottom: 10px;
    }
    .metric-value {
        font-size: 24px;
        font-weight: 700;
        color: #111;
    }
    .metric-label {
        font-size: 13px;
        color: #666;
        text-transform: uppercase;
    }
    .ground-truth-badge {
        background-color: #fff3cd;
        color: #856404;
        padding: 8px 12px;
        border-radius: 6px;
        font-weight: 600;
        border: 1px solid #ffeeba;
        margin-bottom: 12px;
        display: inline-block;
    }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Cached Query Wrappers (TTL = 3 seconds)
# -----------------------------------------------------------------------------

@st.cache_data(ttl=3, show_spinner=False)
def fetch_kpis():
    conn = queries.get_db_connection(use_dashboard_reader=True)
    try:
        return queries.get_headline_kpis(conn)
    finally:
        conn.close()


@st.cache_data(ttl=3, show_spinner=False)
def fetch_model_run():
    conn = queries.get_db_connection(use_dashboard_reader=True)
    try:
        return queries.get_latest_model_run(conn)
    finally:
        conn.close()


@st.cache_data(ttl=3, show_spinner=False)
def fetch_health_metrics(limit: int = 2000):
    conn = queries.get_db_connection(use_dashboard_reader=True)
    try:
        return queries.get_pipeline_health_metrics(conn, limit=limit)
    finally:
        conn.close()


@st.cache_data(ttl=3, show_spinner=False)
def fetch_windowed_traffic(window_limit: int):
    conn = queries.get_db_connection(use_dashboard_reader=True)
    try:
        return queries.get_windowed_traffic(conn, window_limit=window_limit)
    finally:
        conn.close()


@st.cache_data(ttl=3, show_spinner=False)
def fetch_top_ips(limit: int = 10):
    conn = queries.get_db_connection(use_dashboard_reader=True)
    try:
        return queries.get_top_source_ips(conn, limit=limit)
    finally:
        conn.close()


@st.cache_data(ttl=3, show_spinner=False)
def fetch_alerted_windows(limit: int = 20):
    conn = queries.get_db_connection(use_dashboard_reader=True)
    try:
        return queries.get_latest_alerted_windows(conn, limit=limit)
    finally:
        conn.close()


@st.cache_data(ttl=3, show_spinner=False)
def fetch_ground_truth(window_limit: int):
    conn = queries.get_db_connection(use_dashboard_reader=True)
    try:
        return queries.get_ground_truth_evaluation(conn, window_limit=window_limit)
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Main Application Content
# -----------------------------------------------------------------------------

def render_dashboard(window_limit_val: int):
    try:
        # Load all section data
        kpis = fetch_kpis()
        model_info = fetch_model_run()
        health = fetch_health_metrics(limit=2000)
        win_data = fetch_windowed_traffic(window_limit=window_limit_val)
        top_ips = fetch_top_ips(limit=10)
        alert_wins = fetch_alerted_windows(limit=20)
        gt_data = fetch_ground_truth(window_limit=window_limit_val)
    except Exception as e:
        st.warning(f"⚠️ Database connection unavailable or stream awaiting initialization. Details: {e}")
        return

    # Check for empty database
    if kpis["total_flows"] == 0:
        st.info("ℹ️ Database is connected but currently contains 0 records. Start the Spark streaming pipeline and replayer to begin ingestion.")
        return

    # =========================================================================
    # Section A: Headline KPIs & Model Lineage
    # =========================================================================
    st.subheader("1. Headline KPIs & Model Lineage")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Flows", f"{kpis['total_flows']:,}")
    with col2:
        st.metric("Flagged Flows", f"{kpis['flagged_flows']:,}")
    with col3:
        st.metric("Flagged Share", f"{kpis['flagged_pct']:.2f}%")
    with col4:
        st.metric("Alerted Windows", f"{kpis['alerted_windows']} / {kpis['total_windows']}")
    with col5:
        last_up = kpis["last_db_update"]
        last_str = last_up.strftime("%H:%M:%S UTC") if last_up else "N/A"
        st.metric("Last update", last_str)

    if model_info:
        with st.expander("🔍 Model Hyperparameters & Training Provenance (Gate D5)", expanded=False):
            m_col1, m_col2, m_col3 = st.columns(3)
            with m_col1:
                st.write(f"**Run ID:** `{model_info.get('run_id', 'N/A')}`")
                m_file_raw = model_info.get('model_file', 'N/A')
                m_file_name = Path(m_file_raw).name if m_file_raw != 'N/A' else 'N/A'
                st.write(f"**Model File:** `{m_file_name}`")
            with m_col2:
                thresh_val = model_info.get("threshold", 0.0)
                st.write(f"**Calibrated Threshold:** `{thresh_val:.4f}`")
                st.write(f"**Alert Filter (k):** `{model_info.get('alert_k', 3)}` flagged flows/window")
            with m_col3:
                st.write(f"**Event Watermark:** `{model_info.get('watermark', '60 seconds')}`")
                st.write(f"**Created At:** `{model_info.get('created_at', 'N/A')}`")

    st.markdown("---")

    # =========================================================================
    # Section B: Streaming Pipeline Health
    # =========================================================================
    st.subheader("2. Streaming Pipeline Health (Recent 2,000 Flows)")
    h_col1, h_col2, h_col3 = st.columns(3)

    with h_col1:
        st.metric("End-to-End Latency (p50)", f"{health['p50_latency_s']:.2f} s")
    with h_col2:
        st.metric("End-to-End Latency (p95)", f"{health['p95_latency_s']:.2f} s")
    with h_col3:
        st.metric("Max Ingestion Latency", f"{health['max_latency_s']:.2f} s")

    p_col1, p_col2 = st.columns(2)
    with p_col1:
        st.write("##### Ingestion Latency (`loaded_at - ingest_time`)")
        lat_series = health.get("latency_series", [])
        if lat_series:
            df_lat = pd.DataFrame(lat_series)
            fig_lat = px.line(
                df_lat,
                x="loaded_at",
                y="latency_s",
                labels={"loaded_at": "DB Loaded At (UTC)", "latency_s": "Latency (s)"},
                title="Flow Ingestion Latency Time Series",
            )
            fig_lat.update_layout(height=280, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_lat, use_container_width=True)
        else:
            st.write("No latency samples available.")

    with p_col2:
        st.write("##### Effective Storage Ingestion Throughput")
        tp_series = health.get("throughput_series", [])
        if tp_series:
            df_tp = pd.DataFrame(tp_series)
            fig_tp = px.bar(
                df_tp,
                x="loaded_at",
                y="rate_rec_s",
                labels={"loaded_at": "DB Loaded At (UTC)", "rate_rec_s": "Ingested rec/s"},
                title="5-Second Aggregated DB Ingestion Rate",
            )
            fig_tp.update_layout(height=280, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_tp, use_container_width=True)
        else:
            st.write("No throughput samples available.")

    st.markdown("---")

    # =========================================================================
    # Section C: Windowed Traffic & Operational Alerts (No Ground Truth)
    # =========================================================================
    st.subheader("3. Windowed Traffic & Operational Alerts")
    windows = win_data.get("windows", [])

    if windows:
        df_win = pd.DataFrame(windows)
        df_win["alert_status"] = df_win["window_alert"].apply(lambda a: "ALERT (>= k)" if a else "Normal")

        # Window Traffic Chart
        fig_win = go.Figure()
        fig_win.add_trace(go.Bar(
            x=df_win["window_start"],
            y=df_win["flow_count"],
            name="Total Flows",
            marker_color="#a0c4ff",
        ))
        fig_win.add_trace(go.Bar(
            x=df_win["window_start"],
            y=df_win["flagged_count"],
            name="Flagged Flows",
            marker_color="#ff4d4f",
        ))
        fig_win.update_layout(
            barmode="overlay",
            title=f"Traffic & Anomaly Counts per 5s Window ({len(windows)} windows shown)",
            xaxis_title="Window Start (2015 Simulated Event Time)",
            yaxis_title="Flows / Window",
            height=320,
            margin=dict(l=20, r=20, t=35, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_win, use_container_width=True)

        w_col1, w_col2 = st.columns([1, 1])

        with w_col1:
            st.write("##### Transport Protocol Mix (`window_metrics`)")
            proto_mix = win_data.get("protocol_mix", {})
            df_proto = pd.DataFrame([
                {"Protocol": k, "Flows": v} for k, v in proto_mix.items() if v > 0
            ])
            if not df_proto.empty:
                fig_proto = px.pie(
                    df_proto,
                    names="Protocol",
                    values="Flows",
                    hole=0.45,
                    title="Protocol Distribution in Selected Windows",
                )
                fig_proto.update_layout(
                    height=280,
                    margin=dict(l=10, r=10, t=30, b=10),
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
                )
                st.plotly_chart(fig_proto, use_container_width=True)
            st.caption("ℹ️ Note: Protocol mix is aggregated strictly from `window_metrics` (`tcp_count`, `udp_count`, `other_count`). Per-protocol anomaly breakdown is restricted under least-privilege RBAC.")

        with w_col2:
            st.write("##### Top Source IPs by Flagged Anomaly Count")
            if top_ips:
                df_ips = pd.DataFrame(top_ips)
                fig_ips = px.bar(
                    df_ips,
                    x="srcip",
                    y="flagged_count",
                    labels={"srcip": "Source IP", "flagged_count": "Flagged Flows"},
                    title="Top 10 Source IPs Flagged by Random Forest",
                    color="flagged_pct",
                    color_continuous_scale="Reds",
                )
                fig_ips.update_layout(
                    height=280,
                    margin=dict(l=10, r=10, t=30, b=60),
                    xaxis=dict(tickangle=-35),
                )
                st.plotly_chart(fig_ips, use_container_width=True)
            else:
                st.write("No flagged source IPs found.")

        # Latest Alerted Windows Table
        st.write("##### Latest 20 Alerted Windows (`window_alert = TRUE`)")
        if alert_wins:
            df_alr = pd.DataFrame(alert_wins)
            display_cols = ["window_start", "window_end", "flow_count", "flagged_count", "tcp_count", "udp_count", "updated_at"]
            st.dataframe(df_alr[display_cols], use_container_width=True, height=220)
        else:
            st.success("No active window alerts in current slice.")
    else:
        st.write("No window metrics available.")

    st.markdown("---")

    # =========================================================================
    # Section D: GROUND TRUTH Evaluation (Post-Hoc Verification Only)
    # =========================================================================
    st.subheader("4. Ground-Truth Operational Evaluation")
    st.markdown(
        '<div class="ground-truth-badge">🔒 GROUND TRUTH POST-HOC EVALUATION (Gate X3) — LABELS NEVER USED AS MODEL INPUT</div>',
        unsafe_allow_html=True,
    )

    gt_cm = gt_data.get("confusion_matrix", {})
    e_col1, e_col2, e_col3, e_col4, e_col5, e_col6 = st.columns(6)

    with e_col1:
        st.metric("TP", f"{gt_cm.get('tp', 0):,}")
    with e_col2:
        st.metric("FP", f"{gt_cm.get('fp', 0):,}")
    with e_col3:
        st.metric("TN", f"{gt_cm.get('tn', 0):,}")
    with e_col4:
        st.metric("FN", f"{gt_cm.get('fn', 0):,}")
    with e_col5:
        st.metric("Precision", f"{gt_data.get('precision_pct', 0.0):.2f}%")
    with e_col6:
        st.metric("Recall", f"{gt_data.get('recall_pct', 0.0):.2f}%")

    # Attack Records vs Flagged Count per Window
    gt_wins = gt_data.get("window_ground_truth", [])
    if gt_wins:
        df_gt_w = pd.DataFrame(gt_wins)
        fig_gt = go.Figure()
        fig_gt.add_trace(go.Scatter(
            x=df_gt_w["window_start"],
            y=df_gt_w["attack_records"],
            name="Ground-Truth Attacks",
            mode="lines+markers",
            line=dict(color="#d62728", width=2),
        ))
        fig_gt.add_trace(go.Scatter(
            x=df_gt_w["window_start"],
            y=df_gt_w["flagged_count"],
            name="Model Flagged Flows",
            mode="lines+markers",
            line=dict(color="#1f77b4", width=2, dash="dash"),
        ))
        fig_gt.update_layout(
            title="Ground-Truth Attack Flows vs Model Flagged Flows per 5s Window",
            xaxis_title="Window Start",
            yaxis_title="Count",
            height=300,
            margin=dict(l=20, r=20, t=50, b=20),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_gt, use_container_width=True)

    # Mandatory Disclaimers
    st.info("""
    **Evaluation Caveats & Statistical Methodology:**
    1. **5-Second Window Granularity**: 5-second tumbling windows aggregate concurrent connections, mixing normal background traffic with attack flows.
    2. **Short-Slice Sample Size**: Pure-normal window counts in live stream slices are small (< 30), meaning live false-alarm rates carry high statistical variance.
    3. **Generalization Performance**: Definitive model detection metrics derive from the full cross-session evaluation, not short live stream slices:
       - **S0 -> S1 (Train S0, Test S1)**: ROC-AUC = 0.9961, PR-AUC = 0.9411 (from `reports/anomaly_detection/anomaly_detection_results_20261031.json`, fields `S0_to_S1.roc_auc` = 0.9960620417817955, `S0_to_S1.pr_auc` = 0.9411435128411499).
       - **S1 -> S0 (Train S1, Test S0)**: ROC-AUC = 0.9996, PR-AUC = 0.9647 (from `reports/anomaly_detection/anomaly_detection_results_20261031.json`, fields `S1_to_S0.roc_auc` = 0.999567710723484, `S1_to_S0.pr_auc` = 0.9646564904200764).
       - **Out-of-Sample Live Slice (25 rec/s)**: Flow Recall = 0.9707, Precision = 0.9442, Burst Detection Rate = 0.9779 (from `reports/verification/c2b_unseen_detection_20261101.json`, field `rates.rate_25`).
    """)


# -----------------------------------------------------------------------------
# Fragment-Based Auto-Refresh & Sidebar Controls
# -----------------------------------------------------------------------------

def main():
    st.sidebar.title("🛠️ Pipeline Controls")

    # Auto-refresh toggle
    auto_refresh = st.sidebar.toggle("Auto-Refresh (every 5s)", value=True)
    if not auto_refresh:
        if st.sidebar.button("🔄 Manual Refresh"):
            st.rerun()

    # Window limit selector
    win_choice = st.sidebar.selectbox(
        "Window History Limit",
        ["Last 50 windows", "Last 200 windows", "All windows"],
        index=0,
    )
    limit_map = {
        "Last 50 windows": 50,
        "Last 200 windows": 200,
        "All windows": 0,
    }
    selected_limit = limit_map[win_choice]

    st.sidebar.markdown("---")
    st.sidebar.subheader("🚀 Live Demonstration Launch")
    st.sidebar.markdown("""
    1. **Services**:
       `docker compose up -d`
    2. **Run Demo Setup (Creates Topic & Prints Commands)**:
       `powershell -File scripts/demo_live.ps1`
    3. **Start Spark FIRST**:
       Paste command from Step 2
    4. **Start Replayer**:
       Paste command from Step 2
    """)

    # Streamlit fragment auto-refresh execution
    if hasattr(st, "fragment") and auto_refresh:
        @st.fragment(run_every=5)
        def auto_render():
            render_dashboard(selected_limit)
        auto_render()
    else:
        render_dashboard(selected_limit)
        if auto_refresh:
            time.sleep(5)
            st.rerun()


if __name__ == "__main__":
    main()


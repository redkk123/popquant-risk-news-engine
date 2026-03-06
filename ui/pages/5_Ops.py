from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.ops_workbench import build_overview_payload, run_ops_analytics_workbench


st.title("Ops")

overview = build_overview_payload(PROJECT_ROOT)
latest_ops_analytics = overview["latest_ops_analytics"]
if st.button("Run Ops Analytics"):
    result = run_ops_analytics_workbench(
        project_root=PROJECT_ROOT,
        recent_runs=5,
        output_dir=PROJECT_ROOT / "output" / "ops_analytics",
    )
    st.success(f"Ops analytics saved to {result['output_root']}")
    st.json(result["summary"])
    st.dataframe(result["runs_frame"], use_container_width=True, hide_index=True)
    st.subheader("Watchlist Runs")
    st.dataframe(result["watchlist_runs_frame"], use_container_width=True, hide_index=True)
    st.subheader("Capital Runs")
    st.dataframe(result["capital_runs_frame"], use_container_width=True, hide_index=True)
    if not result["path_leaderboard"].empty:
        st.subheader("Capital Path Leaderboard")
        st.dataframe(result["path_leaderboard"], use_container_width=True, hide_index=True)

if latest_ops_analytics:
    st.subheader("Latest Ops Analytics")
    st.json(latest_ops_analytics)

st.subheader("Latest Operator Summary")
st.json(overview["latest_operator_summary"])

st.subheader("Latest Validation Summary")
st.json(overview["latest_validation_summary"])

st.subheader("Latest Validation Governance")
st.json(overview["latest_validation_governance"])

st.subheader("Latest Trend Governance")
st.json(overview["latest_trend_governance"])

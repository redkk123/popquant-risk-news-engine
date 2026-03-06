from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.ops_workbench import build_overview_payload


st.title("Overview")

overview = build_overview_payload(PROJECT_ROOT)
operator_summary = overview["latest_operator_summary"]
validation_governance = overview["latest_validation_governance"]
trend_governance = overview["latest_trend_governance"]
capital_status = overview["latest_capital_status"]
ops_analytics = overview["latest_ops_analytics"]

metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
metric_col1.metric("Live Validation", validation_governance.get("decision", {}).get("status", "n/a"))
metric_col2.metric("Trend Governance", trend_governance.get("decision", {}).get("status", "n/a"))
metric_col3.metric("Portfolios", operator_summary.get("portfolio_count", 0))
metric_col4.metric("Capital Sandbox", capital_status.get("status", "n/a"))

if operator_summary:
    st.subheader("Latest Operator Summary")
    overview_left, overview_right = st.columns(2)
    with overview_left:
        st.json(
            {
                "watchlist_run": operator_summary.get("watchlist_run"),
                "watchlist_origin": operator_summary.get("watchlist_origin"),
                "quota_pressure": operator_summary.get("ops", {}).get("quota_pressure"),
                "zero_event_windows": operator_summary.get("ops", {}).get("zero_event_windows"),
                "reused_window_count": operator_summary.get("ops", {}).get("reused_window_count"),
                "failed_window_count": operator_summary.get("ops", {}).get("failed_window_count"),
            }
        )
    with overview_right:
        st.json(
            {
                "fresh_sync_windows": operator_summary.get("validation", {}).get("fresh_sync_windows"),
                "archive_reuse_windows": operator_summary.get("validation", {}).get("archive_reuse_windows"),
                "failed_windows": operator_summary.get("validation", {}).get("failed_windows"),
                "quota_blocked_windows": operator_summary.get("validation", {}).get("quota_blocked_windows"),
                "fresh_sync_dominant": operator_summary.get("validation", {}).get("fresh_sync_dominant"),
            }
        )

capital_sandbox = operator_summary.get("capital_sandbox", {})
if capital_sandbox:
    st.subheader("Latest Capital Sandbox")
    st.json(capital_sandbox)

capital_compare = operator_summary.get("capital_compare", {})
if capital_compare:
    st.subheader("Latest Capital Compare")
    st.json(
        {
            "run": capital_compare.get("run"),
            "overall_best_session": capital_compare.get("overall_best_session"),
            "overall_best_path": capital_compare.get("overall_best_path"),
            "overall_best_final_capital": capital_compare.get("overall_best_final_capital"),
        }
    )
    compare_frame = pd.DataFrame(capital_compare.get("best_by_session", []))
    if not compare_frame.empty:
        st.dataframe(compare_frame, use_container_width=True, hide_index=True)

if ops_analytics:
    st.subheader("Latest Ops Analytics")
    st.json(
        {
            "clean_pass_streak": ops_analytics.get("clean_pass_streak"),
            "recent_green_streak_origin": ops_analytics.get("recent_green_streak_origin"),
            "fresh_share": (ops_analytics.get("window_origin_rates") or {}).get("fresh_sync_share"),
            "archive_share": (ops_analytics.get("window_origin_rates") or {}).get("archive_reuse_share"),
            "capital_refresh_efficiency": ops_analytics.get("capital_refresh_efficiency"),
        }
    )

top_portfolios = pd.DataFrame(operator_summary.get("top_portfolios", []))
top_events = pd.DataFrame(operator_summary.get("top_events", []))

left, right = st.columns(2)
with left:
    st.subheader("Top Portfolios")
    st.dataframe(top_portfolios, use_container_width=True, hide_index=True)
with right:
    st.subheader("Top Events")
    st.dataframe(top_events, use_container_width=True, hide_index=True)

st.subheader("Latest Watchlist Summary")
st.dataframe(overview["latest_watchlist_summary"], use_container_width=True, hide_index=True)

st.subheader("Latest Watchlist Events")
st.dataframe(overview["latest_watchlist_events"].head(20), use_container_width=True, hide_index=True)

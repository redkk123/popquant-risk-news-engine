from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.capital_workbench import (
    run_capital_sandbox_compare_workbench,
    run_capital_sandbox_workbench,
)
from services.ops_workbench import build_overview_payload
from services.portfolio_manager import list_portfolio_paths


st.title("Capital Sandbox")
overview = build_overview_payload(PROJECT_ROOT)

portfolio_options = [str(path) for path in list_portfolio_paths(PROJECT_ROOT)]
if not portfolio_options:
    st.warning("No portfolio JSON files found under config/portfolios.")
    st.stop()

portfolio_path = st.selectbox("Portfolio", options=portfolio_options, index=0)
mode = st.selectbox("Mode", options=["live_session_real_time", "replay_intraday", "historical_daily"], index=0)

left, middle, right = st.columns(3)
initial_capital = left.number_input("Initial capital", min_value=10.0, value=100.0, step=10.0)
interval_options = [60, 120, 300] if mode == "live_session_real_time" else [10, 20, 30, 60]
decision_interval_seconds = middle.selectbox("Decision interval", options=interval_options, index=0)
session_minutes = right.selectbox("Session preset", options=[5, 15, 30], index=0)
news_refresh_minutes = st.number_input(
    "News refresh cadence (minutes)",
    min_value=1,
    value=2,
    step=1,
    disabled=(mode != "live_session_real_time"),
)
compare_sessions = st.multiselect(
    "Compare sessions",
    options=[5, 15, 30],
    default=[],
    help="If you select multiple values, the sandbox runs all of them in one report.",
    disabled=(mode == "live_session_real_time"),
)

fixture_mode = st.checkbox(
    "Use fixture instead of live providers",
    value=(mode == "historical_daily"),
    disabled=(mode == "live_session_real_time"),
)
fixture_path = st.text_input(
    "Fixture path",
    value=str(PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news_history.json"),
    disabled=not fixture_mode,
)
fixture_provider = st.selectbox(
    "Fixture provider",
    options=["marketaux", "thenewsapi", "newsapi", "alphavantage"],
    index=0,
    disabled=not fixture_mode,
)

providers = st.multiselect(
    "Live providers",
    options=["marketaux", "thenewsapi", "newsapi", "alphavantage"],
    default=["marketaux", "thenewsapi", "newsapi", "alphavantage"],
    disabled=fixture_mode,
)

latest_capital = (overview.get("latest_operator_summary") or {}).get("capital_sandbox", {})
latest_compare = (overview.get("latest_operator_summary") or {}).get("capital_compare", {})
if latest_capital:
    st.subheader("Latest Live Session")
    st.json(latest_capital)
if latest_compare:
    st.subheader("Latest Session Compare")
    st.json(
        {
            "run": latest_compare.get("run"),
            "overall_best_session": latest_compare.get("overall_best_session"),
            "overall_best_path": latest_compare.get("overall_best_path"),
            "overall_best_final_capital": latest_compare.get("overall_best_final_capital"),
        }
    )

start_end_cols = st.columns(2)
start = start_end_cols[0].text_input("Historical start", value="2024-01-01")
end = start_end_cols[1].text_input("Historical end", value="2026-03-06")

if st.button("Run Capital Sandbox", disabled=not bool(portfolio_options)):
    if compare_sessions:
        result = run_capital_sandbox_compare_workbench(
            portfolio_config=portfolio_path,
            mode=mode,
            initial_capital=float(initial_capital),
            decision_interval_seconds=int(decision_interval_seconds),
            session_minutes_list=compare_sessions,
            start=start,
            end=end,
            news_fixture=fixture_path if fixture_mode else None,
            fixture_provider=fixture_provider,
            providers=providers,
            output_dir=PROJECT_ROOT / "output" / "capital_sandbox",
        )
    else:
        result = run_capital_sandbox_workbench(
            portfolio_config=portfolio_path,
            mode=mode,
            initial_capital=float(initial_capital),
            decision_interval_seconds=int(decision_interval_seconds),
            session_minutes=int(session_minutes),
            news_refresh_minutes=int(news_refresh_minutes),
            start=start,
            end=end,
            news_fixture=fixture_path if fixture_mode else None,
            fixture_provider=fixture_provider,
            providers=providers,
            output_dir=PROJECT_ROOT / "output" / "capital_sandbox",
        )
    st.success(f"Sandbox saved to {result['output_root']}")
    st.subheader("Path Summary")
    st.dataframe(result["summary_frame"], use_container_width=True, hide_index=True)
    if result.get("session_meta"):
        st.subheader("Session Meta")
        st.json(result["session_meta"])

    snapshot_frame = result["snapshot_frame"].copy()
    if not snapshot_frame.empty:
        st.subheader("Snapshots")
        session_options = snapshot_frame["session_label"].drop_duplicates().tolist() if "session_label" in snapshot_frame.columns else ["single"]
        selected_session = st.selectbox("Snapshot session", options=session_options, index=0)
        selected_snapshots = (
            snapshot_frame.loc[snapshot_frame["session_label"] == selected_session].copy()
            if "session_label" in snapshot_frame.columns
            else snapshot_frame.copy()
        )
        selected_snapshots["snapshot_time"] = selected_snapshots["snapshot_time"].astype(str)
        curve_frame = selected_snapshots.pivot(index="snapshot_time", columns="path_name", values="capital")
        st.line_chart(curve_frame)
        st.dataframe(selected_snapshots, use_container_width=True, hide_index=True)

    journal_frame = result["journal_frame"]
    if not journal_frame.empty:
        st.subheader("Decision Journal")
        st.dataframe(journal_frame.tail(30), use_container_width=True, hide_index=True)

    st.subheader("Report")
    st.markdown(result["report_markdown"])


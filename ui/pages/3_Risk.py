from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.portfolio_manager import list_portfolio_paths
from services.risk_workbench import run_risk_snapshot_workbench


st.title("Risk")

portfolio_options = [str(path) for path in list_portfolio_paths(PROJECT_ROOT)]
if not portfolio_options:
    st.warning("No portfolio JSON files found under config/portfolios.")
    st.stop()

portfolio_path = st.selectbox("Portfolio", options=portfolio_options, index=0)

left, right, third = st.columns(3)
start = left.text_input("Start", value="2023-01-01")
end = right.text_input("End", value="2026-03-06")
alpha = third.number_input("Alpha", min_value=0.001, max_value=0.10, value=0.01, step=0.001, format="%.3f")
lam = st.number_input("EWMA lambda", min_value=0.50, max_value=0.999, value=0.94, step=0.01, format="%.3f")

if st.button("Run Risk Snapshot", disabled=not bool(portfolio_options)):
    result = run_risk_snapshot_workbench(
        portfolio_config=portfolio_path,
        start=start,
        end=end,
        alpha=float(alpha),
        lam=float(lam),
        output_dir=PROJECT_ROOT / "output" / "risk_snapshots",
    )
    st.success(f"Saved run to {result['output_root']}")
    st.subheader("Snapshot")
    st.json(
        {
            "portfolio_id": result["snapshot"]["metadata"]["portfolio_id"],
            "daily_volatility": result["snapshot"]["portfolio_stats"]["daily_volatility"],
            "annualized_volatility": result["snapshot"]["portfolio_stats"]["annualized_volatility"],
            "max_drawdown": result["snapshot"]["portfolio_stats"]["max_drawdown"],
            "regime": result["snapshot"]["risk_v2"]["regime"]["regime"],
        }
    )
    st.subheader("Model Metrics")
    st.dataframe(result["model_table"], use_container_width=True, hide_index=True)
    st.subheader("Risk Contributions")
    st.dataframe(result["contribution_table"], use_container_width=True, hide_index=True)
    st.subheader("Sector Risk Contributions")
    st.dataframe(result["sector_contributions"], use_container_width=True, hide_index=True)
    st.subheader("Covariance Model Compare")
    st.dataframe(result["covariance_model_compare"], use_container_width=True, hide_index=True)
    st.subheader("Correlation")
    st.dataframe(result["correlation"], use_container_width=True)

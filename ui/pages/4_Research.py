from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.portfolio_manager import list_portfolio_paths
from services.research_workbench import (
    compare_calibration_snapshots_workbench,
    list_calibration_snapshots,
    run_event_calibration_workbench,
    run_grouped_integration_backtest_workbench,
)


st.title("Research")

portfolio_options = [str(path) for path in list_portfolio_paths(PROJECT_ROOT)]
if not portfolio_options:
    st.warning("No portfolio JSON files found under config/portfolios.")
    st.stop()

st.subheader("Calibration")
calib_portfolio = st.selectbox("Calibration portfolio", options=portfolio_options, key="calib_portfolio")
snapshot_label = st.text_input("Snapshot label", value="streamlit")
if st.button("Run Calibration", disabled=not bool(portfolio_options)):
    result = run_event_calibration_workbench(
        portfolio_config=calib_portfolio,
        snapshot_label=snapshot_label,
        news_fixture=PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news_history.json",
        output_dir=PROJECT_ROOT / "output" / "event_calibration",
        registry_root=PROJECT_ROOT / "output" / "event_calibration_registry",
    )
    st.success(f"Calibration saved to {result['output_root']}")
    st.json(result["snapshot_metadata"])
    st.dataframe(result["summary"], use_container_width=True, hide_index=True)

st.subheader("Calibration Registry")
registry_frame = list_calibration_snapshots(PROJECT_ROOT / "output" / "event_calibration_registry")
st.dataframe(registry_frame, use_container_width=True, hide_index=True)

if not registry_frame.empty and len(registry_frame) >= 2:
    left_snapshot = st.selectbox("Left snapshot", options=registry_frame["snapshot_id"].tolist(), key="left_snapshot")
    right_snapshot = st.selectbox("Right snapshot", options=registry_frame["snapshot_id"].tolist(), index=1, key="right_snapshot")
    if st.button("Compare Snapshots"):
        comparison = compare_calibration_snapshots_workbench(
            left_snapshot_id=left_snapshot,
            right_snapshot_id=right_snapshot,
            registry_root=PROJECT_ROOT / "output" / "event_calibration_registry",
        )
        st.json(comparison)

st.subheader("Grouped Integration Backtest")
bt_watchlist = st.text_input(
    "Watchlist config",
    value=str(PROJECT_ROOT / "config" / "watchlists" / "validation_watchlist.yaml"),
)
group_columns = st.multiselect(
    "Group by",
    options=["event_type", "event_subtype", "story_bucket", "source_tier"],
    default=["event_type", "event_subtype", "story_bucket", "source_tier"],
)
mapping_variants = st.multiselect(
    "Mapping variants",
    options=["configured", "manual", "calibrated", "source_aware"],
    default=["manual", "calibrated", "source_aware"],
)
if st.button("Run Grouped Backtest"):
    result = run_grouped_integration_backtest_workbench(
        watchlist_config=bt_watchlist,
        mapping_variants=mapping_variants,
        group_by=group_columns,
        news_fixture=PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news_history.json",
        output_dir=PROJECT_ROOT / "output" / "integration_backtest",
    )
    st.success(f"Backtest saved to {result['output_root']}")
    st.json(result["summary"])
    st.dataframe(result["variant_compare"], use_container_width=True, hide_index=True)
    for column, frame in result["best_variant_by_group"].items():
        st.markdown(f"**Best variant by {column}**")
        st.dataframe(frame, use_container_width=True, hide_index=True)
    for column, frame in result["grouped"].items():
        st.markdown(f"**Grouped by {column}**")
        st.dataframe(frame, use_container_width=True, hide_index=True)

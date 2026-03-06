from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


st.set_page_config(
    page_title="PopQuant Workbench",
    page_icon="PQ",
    layout="wide",
)

st.title("PopQuant Workbench")
st.caption("Local research and operations workbench for quant risk + news integration.")

st.markdown(
    """
Use the pages on the left to:

- inspect the latest operator and validation state
- create or edit portfolio JSON files
- run baseline risk and `risk_v2`
- run capital sandbox sessions with 5m/15m/30m presets
- browse calibration snapshots and grouped integration backtests
- inspect historical operational analytics
"""
)

st.info("This UI is local-only. It writes artifacts to the same project folders used by the CLI.")

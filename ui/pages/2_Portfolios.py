from __future__ import annotations

import json
import sys
from pathlib import Path

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.portfolio_manager import (
    list_portfolio_paths,
    load_portfolio_payload,
    save_portfolio_payload,
)


st.title("Portfolios")

portfolio_paths = list_portfolio_paths(PROJECT_ROOT)
selected_path = st.selectbox(
    "Existing portfolio",
    options=[""] + [str(path) for path in portfolio_paths],
    index=0,
)

existing_payload = None
if selected_path:
    existing_payload = load_portfolio_payload(selected_path)
    st.subheader("Loaded JSON")
    st.json(existing_payload)

uploaded_file = st.file_uploader("Upload portfolio JSON", type=["json"])
if uploaded_file is not None:
    uploaded_payload = json.load(uploaded_file)
    st.subheader("Uploaded Preview")
    st.json(uploaded_payload)
    if st.button("Save Uploaded Portfolio"):
        output_path = save_portfolio_payload(uploaded_payload, project_root=PROJECT_ROOT)
        st.success(f"Saved to {output_path}")

st.subheader("Create or Edit Portfolio")
with st.form("portfolio_form"):
    portfolio_id = st.text_input("portfolio_id", value=(existing_payload or {}).get("portfolio_id", ""))
    description = st.text_input("description", value=(existing_payload or {}).get("description", ""))
    base_currency = st.text_input("base_currency", value=(existing_payload or {}).get("base_currency", "USD"))
    benchmark = st.text_input("benchmark", value=(existing_payload or {}).get("benchmark", "SPY") or "")

    default_positions = (existing_payload or {}).get(
        "positions",
        [
            {"ticker": "AAPL", "weight": 0.4},
            {"ticker": "MSFT", "weight": 0.35},
            {"ticker": "SPY", "weight": 0.25},
        ],
    )
    positions_text = st.text_area(
        "positions (JSON list)",
        value=json.dumps(default_positions, indent=2),
        height=220,
    )
    submitted = st.form_submit_button("Validate and Save")

if submitted:
    try:
        payload = {
            "portfolio_id": portfolio_id,
            "description": description,
            "base_currency": base_currency,
            "benchmark": benchmark,
            "positions": json.loads(positions_text),
        }
        output_path = save_portfolio_payload(payload, project_root=PROJECT_ROOT)
        st.success(f"Saved to {output_path}")
        st.json(load_portfolio_payload(output_path))
    except Exception as exc:  # noqa: BLE001
        st.error(str(exc))

if positions_text:
    try:
        preview_positions = json.loads(positions_text)
        preview_frame = st.dataframe(preview_positions, use_container_width=True, hide_index=True)
        total_weight = sum(float(row.get("weight", 0.0) or 0.0) for row in preview_positions)
        st.caption(f"Preview weight sum: {total_weight:.4f}")
    except Exception:
        st.caption("Preview unavailable until positions JSON parses cleanly.")

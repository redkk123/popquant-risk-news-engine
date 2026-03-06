from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from data.schemas import PORTFOLIO_REQUIRED_FIELDS, POSITION_REQUIRED_FIELDS


def validate_price_frame(prices: pd.DataFrame) -> pd.DataFrame:
    """Validate market prices before return conversion."""
    if prices.empty:
        raise ValueError("Price frame is empty.")
    if not prices.index.is_monotonic_increasing:
        raise ValueError("Price index must be sorted in ascending order.")
    if prices.columns.duplicated().any():
        duplicates = prices.columns[prices.columns.duplicated()].tolist()
        raise ValueError(f"Duplicate price columns found: {duplicates}")
    if prices.isna().all(axis=0).any():
        empty_cols = prices.columns[prices.isna().all(axis=0)].tolist()
        raise ValueError(f"Price columns with no data: {empty_cols}")
    return prices


def validate_portfolio_payload(payload: dict[str, Any]) -> None:
    """Validate raw portfolio config payload."""
    if not isinstance(payload, dict):
        raise TypeError("Portfolio config must be a JSON object.")

    missing = [field for field in PORTFOLIO_REQUIRED_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"Portfolio config missing fields: {missing}")

    positions = payload["positions"]
    if not isinstance(positions, list) or not positions:
        raise ValueError("Portfolio config must contain a non-empty positions list.")


def validate_positions_frame(
    positions: pd.DataFrame,
    *,
    allow_short: bool = False,
    weight_tolerance: float = 1e-6,
) -> pd.DataFrame:
    """Validate canonical positions frame."""
    missing_cols = [column for column in POSITION_REQUIRED_FIELDS if column not in positions]
    if missing_cols:
        raise ValueError(f"Positions missing required columns: {missing_cols}")

    frame = positions.copy()
    frame["ticker"] = frame["ticker"].astype(str).str.upper().str.strip()
    if (frame["ticker"] == "").any():
        raise ValueError("Ticker values must be non-empty.")
    if frame["ticker"].duplicated().any():
        duplicates = frame.loc[frame["ticker"].duplicated(), "ticker"].tolist()
        raise ValueError(f"Duplicate tickers in positions: {duplicates}")

    frame["weight"] = pd.to_numeric(frame["weight"], errors="raise")
    if not np.isfinite(frame["weight"]).all():
        raise ValueError("All weights must be finite numbers.")

    if not allow_short and (frame["weight"] < 0).any():
        raise ValueError("Negative weights are not allowed unless allow_short=true.")

    total_weight = float(frame["weight"].sum())
    if abs(total_weight) <= weight_tolerance:
        raise ValueError("Total portfolio weight cannot be zero.")

    if allow_short:
        gross_weight = float(frame["weight"].abs().sum())
        if gross_weight <= weight_tolerance:
            raise ValueError("Gross portfolio exposure cannot be zero.")
    elif not np.isclose(total_weight, 1.0, atol=weight_tolerance):
        raise ValueError(
            f"Long-only portfolio weights must sum to 1.0. Current sum={total_weight:.6f}"
        )

    return frame


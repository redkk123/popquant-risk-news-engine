from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data.validation import validate_portfolio_payload, validate_positions_frame


def _standardize_weights(
    positions: pd.DataFrame, *, allow_short: bool, normalize: bool
) -> pd.DataFrame:
    frame = positions.copy()
    total_weight = float(frame["weight"].sum())

    if np.isclose(total_weight, 100.0, atol=1e-6):
        frame["weight"] = frame["weight"] / 100.0
    elif normalize and not np.isclose(total_weight, 1.0, atol=1e-6):
        frame["weight"] = frame["weight"] / total_weight

    return validate_positions_frame(frame, allow_short=allow_short)


def load_portfolio_config(
    path: str | Path, *, normalize: bool = True
) -> tuple[dict[str, Any], pd.DataFrame]:
    """Load a portfolio config JSON file and return metadata plus positions."""
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    validate_portfolio_payload(payload)

    allow_short = bool(payload.get("allow_short", False))
    positions = pd.DataFrame(payload["positions"])
    positions = _standardize_weights(
        positions, allow_short=allow_short, normalize=normalize
    )

    metadata = {
        "portfolio_id": payload["portfolio_id"],
        "benchmark": payload.get("benchmark"),
        "base_currency": payload.get("base_currency", "USD"),
        "allow_short": allow_short,
        "description": payload.get("description", ""),
        "source_path": str(config_path),
    }
    return metadata, positions


def canonicalize_portfolio_payload(
    payload: dict[str, Any], *, normalize: bool = True
) -> dict[str, Any]:
    """Validate and normalize a raw portfolio payload into canonical JSON form."""
    validate_portfolio_payload(payload)

    allow_short = bool(payload.get("allow_short", False))
    positions = pd.DataFrame(payload["positions"])
    positions = _standardize_weights(
        positions,
        allow_short=allow_short,
        normalize=normalize,
    )

    canonical_payload: dict[str, Any] = {
        "portfolio_id": str(payload["portfolio_id"]).strip(),
        "description": str(payload.get("description", "")).strip(),
        "base_currency": str(payload.get("base_currency", "USD")).strip() or "USD",
        "benchmark": str(payload.get("benchmark", "")).strip().upper() or None,
        "positions": positions.assign(ticker=positions["ticker"].astype(str).str.upper())
        .to_dict(orient="records"),
    }
    if allow_short:
        canonical_payload["allow_short"] = True
    return canonical_payload


def write_portfolio_config(
    payload: dict[str, Any],
    path: str | Path,
    *,
    normalize: bool = True,
) -> Path:
    """Write a canonical portfolio config JSON file."""
    canonical_payload = canonicalize_portfolio_payload(payload, normalize=normalize)
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(canonical_payload, handle, indent=2)
    return output_path


def weights_series(positions: pd.DataFrame) -> pd.Series:
    """Return portfolio weights indexed by ticker."""
    validated = validate_positions_frame(
        positions, allow_short=bool((positions["weight"] < 0).any())
    )
    return validated.set_index("ticker")["weight"].rename("weight")

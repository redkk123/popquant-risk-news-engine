from __future__ import annotations

import warnings

import pandas as pd

from models.ewma import ewma_next_volatility, ewma_volatility


def test_ewma_volatility_single_observation_has_no_runtime_warning() -> None:
    returns = pd.Series([0.012], index=pd.to_datetime(["2026-03-06T13:00:00Z"]))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        sigma = ewma_volatility(returns)

    assert len(sigma) == 1
    assert float(sigma.iloc[0]) > 0.0
    assert not any("Degrees of freedom <= 0" in str(item.message) for item in caught)


def test_ewma_next_volatility_single_observation_is_finite() -> None:
    returns = pd.Series([0.012], index=pd.to_datetime(["2026-03-06T13:00:00Z"]))

    next_sigma = ewma_next_volatility(returns)

    assert next_sigma > 0.0


def test_ewma_volatility_all_nan_raises_value_error() -> None:
    returns = pd.Series([float("nan")], index=pd.to_datetime(["2026-03-06T13:00:00Z"]))

    try:
        ewma_volatility(returns)
    except ValueError as exc:
        assert "finite observations" in str(exc)
    else:
        raise AssertionError("Expected ValueError for all-NaN returns.")

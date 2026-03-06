from __future__ import annotations

import numpy as np
import pandas as pd

from models.covariance import constant_correlation_shrinkage_covariance
from risk.factors import sector_risk_contributions
from risk.portfolio import build_risk_snapshot_bundle
from risk.regime import classify_risk_regime


def test_constant_correlation_shrinkage_covariance_returns_valid_matrix() -> None:
    returns = pd.DataFrame(
        {
            "AAPL": [0.01, -0.02, 0.015, 0.005, -0.01, 0.008],
            "MSFT": [0.008, -0.01, 0.012, 0.004, -0.009, 0.007],
            "JPM": [0.004, -0.006, 0.005, 0.002, -0.003, 0.004],
        }
    )

    covariance, meta = constant_correlation_shrinkage_covariance(returns)

    assert covariance.shape == (3, 3)
    assert list(covariance.index) == ["AAPL", "MSFT", "JPM"]
    assert 0.0 <= meta["shrinkage"] <= 1.0
    assert np.allclose(covariance.to_numpy(), covariance.to_numpy().T)


def test_sector_risk_contributions_aggregate_assets() -> None:
    covariance = pd.DataFrame(
        [[0.04, 0.01, 0.00], [0.01, 0.03, 0.00], [0.00, 0.00, 0.02]],
        index=["AAPL", "MSFT", "JPM"],
        columns=["AAPL", "MSFT", "JPM"],
    )
    weights = pd.Series({"AAPL": 0.4, "MSFT": 0.3, "JPM": 0.3})

    sector_frame = sector_risk_contributions(
        weights=weights,
        covariance=covariance,
        ticker_sector_map={"AAPL": "technology", "MSFT": "technology", "JPM": "financials"},
    )

    assert set(sector_frame["sector"]) == {"technology", "financials"}
    tech_row = sector_frame.loc[sector_frame["sector"] == "technology"].iloc[0]
    assert tech_row["asset_count"] == 2


def test_classify_risk_regime_identifies_stress() -> None:
    stressed_returns = pd.Series(
        [0.005] * 40 + [-0.06, 0.04, -0.05, 0.03, -0.04, 0.035, -0.03, 0.025],
        index=pd.date_range("2025-01-01", periods=48, freq="B"),
    )
    benchmark_returns = pd.Series(
        [0.004] * 40 + [-0.05, -0.04, -0.03, 0.01, -0.02, 0.01, -0.01, 0.005],
        index=stressed_returns.index,
    )

    regime = classify_risk_regime(portfolio_returns=stressed_returns, benchmark_returns=benchmark_returns)

    assert regime["regime"] == "stress"
    assert regime["stress_signals"] >= 2


def test_build_risk_snapshot_bundle_includes_risk_v2_payload() -> None:
    index = pd.date_range("2025-01-01", periods=40, freq="B")
    asset_returns = pd.DataFrame(
        {
            "AAPL": np.linspace(-0.02, 0.02, 40),
            "MSFT": np.linspace(-0.015, 0.018, 40),
            "JPM": np.linspace(-0.01, 0.012, 40),
        },
        index=index,
    )
    benchmark_returns = pd.Series(np.linspace(-0.01, 0.01, 40), index=index, name="SPY")
    weights = pd.Series({"AAPL": 0.4, "MSFT": 0.35, "JPM": 0.25})

    snapshot, _, _, _, extras = build_risk_snapshot_bundle(
        asset_returns=asset_returns,
        weights=weights,
        benchmark_returns=benchmark_returns,
        portfolio_id="demo_book",
        benchmark_name="SPY",
        ticker_sector_map={"AAPL": "technology", "MSFT": "technology", "JPM": "financials"},
    )

    assert "risk_v2" in snapshot
    assert "factor_summary" in snapshot["risk_v2"]
    assert "regime" in snapshot["risk_v2"]
    assert "covariance_models" in snapshot["risk_v2"]
    assert not extras["sector_contributions"].empty
    assert not extras["covariance_model_compare"].empty

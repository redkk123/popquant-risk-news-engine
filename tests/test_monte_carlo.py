from __future__ import annotations

import numpy as np
import pandas as pd

from simulation.monte_carlo import simulate_portfolio_losses


def test_monte_carlo_outputs_expected_columns() -> None:
    rng = np.random.default_rng(10)
    asset_returns = pd.DataFrame(
        rng.normal(0.0005, 0.01, size=(400, 3)),
        columns=["AAPL", "MSFT", "SPY"],
    )
    weights = pd.Series({"AAPL": 0.4, "MSFT": 0.4, "SPY": 0.2})

    paths, summary = simulate_portfolio_losses(
        asset_returns, weights, horizon_days=5, n_sims=500, alpha=0.05
    )

    assert set(paths.columns) == {
        "gaussian_portfolio_return",
        "gaussian_portfolio_loss",
        "student_t_portfolio_return",
        "student_t_portfolio_loss",
    }
    assert len(paths) == 500
    assert summary["gaussian"]["es_loss"] >= summary["gaussian"]["var_loss"]
    assert summary["student_t"]["es_loss"] >= summary["student_t"]["var_loss"]


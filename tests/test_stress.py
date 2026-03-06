from __future__ import annotations

import numpy as np
import pandas as pd

from risk.stress import run_stress_scenarios


def test_stress_scenarios_increase_reported_var() -> None:
    rng = np.random.default_rng(4)
    asset_returns = pd.DataFrame(
        rng.normal(0.0004, 0.012, size=(300, 3)),
        columns=["AAPL", "MSFT", "SPY"],
    )
    weights = pd.Series({"AAPL": 0.5, "MSFT": 0.3, "SPY": 0.2})
    scenarios = [
        {
            "name": "selloff",
            "default_return_shock": -0.08,
            "vol_multiplier": 1.4,
            "correlation_multiplier": 1.1,
        }
    ]

    summary, detail = run_stress_scenarios(
        asset_returns=asset_returns,
        weights=weights,
        scenarios=scenarios,
        alpha=0.01,
    )

    assert summary.loc[0, "stressed_normal_var_loss_1d_99"] > summary.loc[0, "base_normal_var_loss_1d_99"]
    assert detail["scenario"].nunique() == 1


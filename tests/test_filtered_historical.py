from __future__ import annotations

import numpy as np
import pandas as pd

from models.filtered_historical import (
    filtered_historical_es_loss,
    filtered_historical_horizon_loss,
    filtered_historical_var_loss,
)


def test_filtered_historical_losses_are_positive() -> None:
    returns = pd.Series(
        np.array(
            [
                -0.03,
                0.01,
                -0.02,
                0.015,
                -0.01,
                0.012,
                -0.018,
                0.009,
                -0.022,
                0.011,
            ]
            * 30
        )
    )
    var_loss = filtered_historical_var_loss(returns, alpha=0.05, lam=0.94)
    es_loss = filtered_historical_es_loss(returns, alpha=0.05, lam=0.94)

    assert var_loss > 0.0
    assert es_loss >= var_loss


def test_filtered_historical_horizon_loss_ordering() -> None:
    returns = pd.Series(np.linspace(-0.03, 0.03, 400))
    var_loss, es_loss = filtered_historical_horizon_loss(
        returns, alpha=0.05, lam=0.94, horizon_days=5, n_bootstrap=500
    )

    assert var_loss > 0.0
    assert es_loss >= var_loss


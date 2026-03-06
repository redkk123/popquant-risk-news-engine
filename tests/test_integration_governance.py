from __future__ import annotations

import pandas as pd

from fusion.integration_governance import compare_integration_variants


def test_compare_integration_variants_selects_lower_mae_variant() -> None:
    index = pd.date_range("2024-01-01", periods=40, freq="B")
    prices = pd.DataFrame(
        {
            "AAPL": [100 - 0.7 * i for i in range(40)],
            "MSFT": [100 - 0.35 * i for i in range(40)],
            "SPY": [100 - 0.2 * i for i in range(40)],
        },
        index=index,
    )
    weights = pd.Series({"AAPL": 0.4, "MSFT": 0.4, "SPY": 0.2})
    events = [
        {
            "event_id": "evt_1",
            "event_type": "guidance",
            "polarity": -0.8,
            "severity": 1.0,
                "published_at": "2024-02-20T13:30:00Z",
                "tickers": ["AAPL"],
                "headline": "AAPL cuts guidance",
            }
        ]
    manual_mapping = {
        "settings": {"max_age_days": 7.0},
        "event_mappings": {
            "guidance": {
                "market_wide": False,
                "negative": {
                    "return_shock": -0.07,
                    "vol_multiplier": 1.20,
                    "correlation_multiplier": 1.05,
                }
            },
            "other": {"market_wide": False},
        },
    }
    calibrated_mapping = {
        "settings": {"max_age_days": 7.0},
        "event_mappings": {
            "guidance": {
                "market_wide": False,
                "negative": {
                    "return_shock": -0.015,
                    "vol_multiplier": 1.05,
                    "correlation_multiplier": 1.02,
                }
            },
            "other": {"market_wide": False},
        },
    }

    comparison = compare_integration_variants(
        prices=prices,
        weights=weights,
        events=events,
        manual_mapping_config=manual_mapping,
        calibrated_mapping_config=calibrated_mapping,
            window=30,
            portfolio_id="test_portfolio",
        )

    assert comparison["decision"]["selected_variant"] == "calibrated"

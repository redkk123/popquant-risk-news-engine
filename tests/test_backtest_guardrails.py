from __future__ import annotations

import pandas as pd

from fusion.backtest_guardrails import build_backtest_guarded_mapping


def test_build_backtest_guarded_mapping_dampens_losing_families_only() -> None:
    mapping = {
        "settings": {},
        "event_mappings": {
            "macro": {
                "negative": {
                    "return_shock": -0.04,
                    "vol_multiplier": 1.3,
                    "correlation_multiplier": 1.12,
                }
            },
            "earnings": {
                "positive": {
                    "return_shock": 0.03,
                    "vol_multiplier": 1.08,
                    "correlation_multiplier": 1.0,
                }
            },
        },
    }
    summary = pd.DataFrame(
        [
            {"event_type": "macro", "horizon_days": 1, "mae_improvement": -0.01},
            {"event_type": "macro", "horizon_days": 3, "mae_improvement": -0.02},
            {"event_type": "macro", "horizon_days": 5, "mae_improvement": -0.03},
            {"event_type": "earnings", "horizon_days": 1, "mae_improvement": 0.01},
            {"event_type": "earnings", "horizon_days": 3, "mae_improvement": 0.02},
            {"event_type": "earnings", "horizon_days": 5, "mae_improvement": 0.03},
        ]
    )

    guarded, decisions = build_backtest_guarded_mapping(
        mapping_config=mapping,
        event_type_summary=summary,
        min_negative_horizons=2,
        dampening_factor=0.25,
    )

    macro_negative = guarded["event_mappings"]["macro"]["negative"]
    earnings_positive = guarded["event_mappings"]["earnings"]["positive"]

    assert macro_negative["return_shock"] == -0.01
    assert macro_negative["vol_multiplier"] == 1.075
    assert macro_negative["correlation_multiplier"] == 1.03
    assert earnings_positive["return_shock"] == 0.03
    assert any(row["event_type"] == "macro" and row["guardrail_applied"] for row in decisions)
    assert any(row["event_type"] == "earnings" and not row["guardrail_applied"] for row in decisions)

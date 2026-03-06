from __future__ import annotations

import pandas as pd

from fusion.calibration import (
    build_calibrated_event_mapping,
    resolve_event_trade_date,
    summarize_sector_peer_impacts,
)


def test_resolve_event_trade_date_rolls_after_close_to_next_session() -> None:
    price_index = pd.to_datetime(["2026-03-05", "2026-03-06", "2026-03-09"])
    trade_date = resolve_event_trade_date(price_index, "2026-03-05T21:00:00Z")
    assert trade_date == pd.Timestamp("2026-03-06")


def test_build_calibrated_event_mapping_preserves_rule_direction() -> None:
    summary = pd.DataFrame(
        [
            {
                "event_type": "guidance",
                "direction": "negative",
                "observation_count": 3,
                "median_forward_return_1d": 0.028,
                "median_forward_vol_ratio_10d": 1.18,
            }
        ]
    )
    base_mapping = {
        "settings": {},
        "event_mappings": {
            "guidance": {
                "market_wide": False,
                "negative": {
                    "return_shock": -0.07,
                    "vol_multiplier": 1.25,
                    "correlation_multiplier": 1.07,
                }
            }
        },
    }

    calibrated = build_calibrated_event_mapping(
        summary=summary,
        base_mapping_config=base_mapping,
        min_observations=2,
        return_horizon=1,
        vol_window=10,
    )

    negative_rule = calibrated["event_mappings"]["guidance"]["negative"]
    assert negative_rule["return_shock"] == -0.0448
    assert negative_rule["vol_multiplier"] == 1.208


def test_negative_event_vol_guardrail_does_not_drop_below_one() -> None:
    summary = pd.DataFrame(
        [
            {
                "event_type": "macro",
                "direction": "negative",
                "observation_count": 5,
                "median_forward_return_1d": 0.001,
                "median_forward_vol_ratio_10d": 0.4,
            }
        ]
    )
    base_mapping = {
        "settings": {},
        "event_mappings": {
            "macro": {
                "market_wide": True,
                "negative": {
                    "return_shock": -0.03,
                    "vol_multiplier": 1.30,
                    "correlation_multiplier": 1.12,
                }
            }
        },
    }

    calibrated = build_calibrated_event_mapping(
        summary=summary,
        base_mapping_config=base_mapping,
        min_observations=2,
        return_horizon=1,
        vol_window=10,
    )

    negative_rule = calibrated["event_mappings"]["macro"]["negative"]
    assert negative_rule["vol_multiplier"] == 1.0


def test_sector_summary_updates_peer_spillover_rule() -> None:
    summary = pd.DataFrame(
        [
            {
                "event_type": "guidance",
                "direction": "negative",
                "observation_count": 4,
                "median_forward_return_1d": 0.03,
                "median_forward_vol_ratio_10d": 1.15,
            }
        ]
    )
    sector_summary = pd.DataFrame(
        [
            {
                "event_type": "guidance",
                "direction": "negative",
                "event_sector": "technology",
                "observation_count": 4,
                "median_abs_peer_forward_return_1d": 0.012,
                "median_forward_vol_ratio_10d": 1.08,
            }
        ]
    )
    base_mapping = {
        "settings": {},
        "event_mappings": {
            "guidance": {
                "market_wide": False,
                "negative": {
                    "return_shock": -0.07,
                    "vol_multiplier": 1.25,
                    "correlation_multiplier": 1.07,
                }
            }
        },
    }

    calibrated = build_calibrated_event_mapping(
        summary=summary,
        sector_summary=sector_summary,
        base_mapping_config=base_mapping,
        min_observations=2,
        return_horizon=1,
        vol_window=10,
    )

    sector_rule = calibrated["event_mappings"]["guidance"]["sector_overrides"]["technology"]["negative"]
    assert sector_rule["peer_return_multiplier"] > 0.1
    assert sector_rule["peer_vol_multiplier"] >= 1.0


def test_summarize_sector_peer_impacts_filters_to_peer_rows() -> None:
    observations = pd.DataFrame(
        [
            {
                "event_id": "evt_1",
                "event_type": "guidance",
                "direction": "negative",
                "event_sector": "technology",
                "impact_scope": "direct",
                "ticker": "AAPL",
                "forward_return_1d": -0.03,
                "forward_vol_ratio_10d": 1.2,
            },
            {
                "event_id": "evt_1",
                "event_type": "guidance",
                "direction": "negative",
                "event_sector": "technology",
                "impact_scope": "peer_sector",
                "ticker": "MSFT",
                "forward_return_1d": -0.01,
                "forward_vol_ratio_10d": 1.05,
            },
        ]
    )

    summary = summarize_sector_peer_impacts(observations, horizons=[1], vol_window=10)

    assert len(summary) == 1
    assert summary.loc[0, "event_sector"] == "technology"
    assert summary.loc[0, "observation_count"] == 1

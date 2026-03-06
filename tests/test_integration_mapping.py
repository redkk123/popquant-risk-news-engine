from __future__ import annotations

import math

from fusion.scenario_mapper import map_event_to_scenario


MAPPING_CONFIG = {
    "guidance": {
        "market_wide": False,
        "negative": {
            "return_shock": -0.07,
            "vol_multiplier": 1.25,
            "correlation_multiplier": 1.07,
        },
        "positive": {
            "return_shock": 0.04,
            "vol_multiplier": 1.10,
            "correlation_multiplier": 1.00,
        },
    },
    "macro": {
        "market_wide": True,
        "negative": {
            "return_shock": -0.03,
            "vol_multiplier": 1.30,
            "correlation_multiplier": 1.12,
        },
    },
    "other": {
        "market_wide": False,
        "negative": {
            "return_shock": -0.01,
            "vol_multiplier": 1.05,
            "correlation_multiplier": 1.01,
        },
    },
}


def test_guidance_event_maps_only_to_portfolio_intersection() -> None:
    event = {
        "event_id": "evt_1",
        "event_type": "guidance",
        "polarity": -0.8,
        "severity": 0.9,
        "tickers": ["AAPL", "NVDA"],
        "headline": "Apple cuts guidance",
    }

    scenario = map_event_to_scenario(
        event,
        portfolio_tickers=["AAPL", "MSFT", "SPY"],
        mapping_config=MAPPING_CONFIG,
    )

    assert scenario is not None
    assert scenario["affected_tickers"] == ["AAPL"]
    assert scenario["return_shocks"]["AAPL"] < 0.0


def test_macro_event_maps_to_whole_portfolio() -> None:
    event = {
        "event_id": "evt_2",
        "event_type": "macro",
        "polarity": -0.2,
        "severity": 0.7,
        "tickers": [],
        "headline": "Fed inflation warning",
    }

    scenario = map_event_to_scenario(
        event,
        portfolio_tickers=["AAPL", "MSFT", "SPY"],
        mapping_config=MAPPING_CONFIG,
    )

    assert scenario is not None
    assert scenario["affected_tickers"] == ["AAPL", "MSFT", "SPY"]


def test_recency_decay_reduces_stale_event_shock() -> None:
    fresh_event = {
        "event_id": "evt_3",
        "event_type": "guidance",
        "polarity": -0.8,
        "severity": 0.9,
        "published_at": "2026-03-05T12:00:00Z",
        "tickers": ["AAPL"],
        "headline": "Fresh guidance cut",
    }
    stale_event = {
        **fresh_event,
        "event_id": "evt_4",
        "published_at": "2026-03-01T12:00:00Z",
    }

    fresh = map_event_to_scenario(
        fresh_event,
        portfolio_tickers=["AAPL", "MSFT", "SPY"],
        mapping_config=MAPPING_CONFIG,
        as_of="2026-03-05T18:00:00Z",
    )
    stale = map_event_to_scenario(
        stale_event,
        portfolio_tickers=["AAPL", "MSFT", "SPY"],
        mapping_config=MAPPING_CONFIG,
        as_of="2026-03-05T18:00:00Z",
    )

    assert fresh is not None
    assert stale is not None
    assert abs(fresh["return_shocks"]["AAPL"]) > abs(stale["return_shocks"]["AAPL"])
    assert math.isclose(fresh["recency_decay"], 1.0, rel_tol=0.0, abs_tol=0.1)
    assert stale["recency_decay"] < 1.0


def test_sector_spillover_applies_to_same_sector_peers() -> None:
    mapping_config = {
        "settings": {"max_age_days": 7.0},
        "event_mappings": {
            "guidance": {
                "market_wide": False,
                "negative": {
                    "return_shock": -0.07,
                    "vol_multiplier": 1.20,
                    "correlation_multiplier": 1.05,
                },
                "sector_overrides": {
                    "technology": {
                        "negative": {
                            "peer_return_multiplier": 0.4,
                            "peer_vol_multiplier": 1.06,
                        }
                    }
                },
            },
            "other": {"market_wide": False},
        },
    }
    event = {
        "event_id": "evt_sector_1",
        "event_type": "guidance",
        "polarity": -0.9,
        "severity": 0.8,
        "tickers": ["AAPL"],
        "headline": "Apple cuts guidance",
    }

    scenario = map_event_to_scenario(
        event,
        portfolio_tickers=["AAPL", "MSFT", "SPY"],
        mapping_config=mapping_config,
        ticker_sector_map={"AAPL": "technology", "MSFT": "technology", "SPY": "broad_market"},
    )

    assert scenario is not None
    assert "MSFT" in scenario["return_shocks"]
    assert abs(scenario["return_shocks"]["MSFT"]) < abs(scenario["return_shocks"]["AAPL"])
    assert scenario["sector_peer_tickers"]["technology"] == ["MSFT"]


def test_sector_spillover_can_map_nonportfolio_event_into_portfolio_peer() -> None:
    mapping_config = {
        "settings": {"max_age_days": 7.0},
        "event_mappings": {
            "m_and_a": {
                "market_wide": False,
                "positive": {
                    "return_shock": 0.03,
                    "vol_multiplier": 1.10,
                    "correlation_multiplier": 1.02,
                },
                "sector_overrides": {
                    "technology": {
                        "positive": {
                            "peer_return_multiplier": 0.5,
                            "peer_vol_multiplier": 1.03,
                        }
                    }
                },
            },
            "other": {"market_wide": False},
        },
    }
    event = {
        "event_id": "evt_sector_2",
        "event_type": "m_and_a",
        "polarity": 0.7,
        "severity": 0.6,
        "tickers": ["NVDA"],
        "headline": "Nvidia announces AI acquisition",
    }

    scenario = map_event_to_scenario(
        event,
        portfolio_tickers=["AAPL", "MSFT", "SPY"],
        mapping_config=mapping_config,
        ticker_sector_map={
            "NVDA": "technology",
            "AAPL": "technology",
            "MSFT": "technology",
            "SPY": "broad_market",
        },
    )

    assert scenario is not None
    assert sorted(scenario["affected_tickers"]) == ["AAPL", "MSFT"]
    assert scenario["direct_tickers"] == []


def test_macro_subtype_override_changes_shock_size() -> None:
    mapping_config = {
        "settings": {"source_scaling": {"tiers": {"tier1": 1.05}, "buckets": {"primary_reporting": 1.0}}},
        "event_mappings": {
            "macro": {
                "market_wide": True,
                "negative": {
                    "return_shock": -0.03,
                    "vol_multiplier": 1.20,
                    "correlation_multiplier": 1.10,
                },
                "subtypes": {
                    "oil_geopolitical": {
                        "market_wide": True,
                        "negative": {
                            "return_shock": -0.05,
                            "vol_multiplier": 1.30,
                            "correlation_multiplier": 1.15,
                        }
                    }
                },
            },
            "other": {"market_wide": False},
        },
    }
    event = {
        "event_id": "evt_macro_1",
        "event_type": "macro",
        "event_subtype": "oil_geopolitical",
        "polarity": -0.4,
        "severity": 0.8,
        "event_confidence": 0.9,
        "link_confidence": 0.0,
        "source_tier": "tier1",
        "source_bucket": "primary_reporting",
        "tickers": [],
        "headline": "Oil spikes on geopolitical tensions",
    }

    scenario = map_event_to_scenario(
        event,
        portfolio_tickers=["AAPL", "MSFT", "SPY"],
        mapping_config=mapping_config,
    )

    assert scenario is not None
    assert scenario["event_subtype"] == "oil_geopolitical"
    assert scenario["source_scale"] > 1.0
    assert abs(scenario["return_shocks"]["AAPL"]) > 0.03


def test_confidence_aware_spillover_reduces_peer_shock_for_weak_link() -> None:
    mapping_config = {
        "settings": {},
        "event_mappings": {
            "guidance": {
                "market_wide": False,
                "negative": {
                    "return_shock": -0.08,
                    "vol_multiplier": 1.20,
                    "correlation_multiplier": 1.05,
                },
                "sector_overrides": {
                    "technology": {
                        "negative": {
                            "peer_return_multiplier": 0.5,
                            "peer_vol_multiplier": 1.08,
                        }
                    }
                },
            },
            "other": {"market_wide": False},
        },
    }
    weak_event = {
        "event_id": "evt_guidance_weak",
        "event_type": "guidance",
        "polarity": -0.8,
        "severity": 0.8,
        "event_confidence": 0.6,
        "link_confidence": 0.2,
        "tickers": ["AAPL"],
        "headline": "Apple cuts guidance",
    }
    strong_event = {
        **weak_event,
        "event_id": "evt_guidance_strong",
        "event_confidence": 0.95,
        "link_confidence": 0.95,
    }

    weak = map_event_to_scenario(
        weak_event,
        portfolio_tickers=["AAPL", "MSFT"],
        mapping_config=mapping_config,
        ticker_sector_map={"AAPL": "technology", "MSFT": "technology"},
    )
    strong = map_event_to_scenario(
        strong_event,
        portfolio_tickers=["AAPL", "MSFT"],
        mapping_config=mapping_config,
        ticker_sector_map={"AAPL": "technology", "MSFT": "technology"},
    )

    assert weak is not None
    assert strong is not None
    assert abs(strong["return_shocks"]["MSFT"]) > abs(weak["return_shocks"]["MSFT"])

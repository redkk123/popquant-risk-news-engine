from __future__ import annotations

from event_engine.quality import assess_event_quality
from event_engine.source_policy import infer_source_domain, resolve_source_policy


def test_infer_source_domain_prefers_clean_source_domain() -> None:
    domain = infer_source_domain("www.reuters.com", "https://www.reuters.com/world/test-story")
    assert domain == "reuters.com"


def test_resolve_source_policy_maps_primary_reporting_source() -> None:
    profile = resolve_source_policy(
        {
            "source": "reuters.com",
            "url": "https://www.reuters.com/world/test-story",
        }
    )
    assert profile["source_tier"] == "tier1"
    assert profile["source_bucket"] == "primary_reporting"
    assert profile["source_adjustment"] > 0.0


def test_resolve_source_policy_maps_recap_source_to_strict_gate() -> None:
    profile = resolve_source_policy(
        {
            "source": "thestockmarketwatch.com",
            "url": "https://thestockmarketwatch.com/markets/review",
        }
    )
    assert profile["source_tier"] == "tier3"
    assert profile["source_low_signal"] is True
    assert profile["source_strict_watchlist"] is True


def test_quality_gate_exposes_source_metadata() -> None:
    quality = assess_event_quality(
        {
            "title": "Apple beats earnings estimates after iPhone strength",
            "description": "Reuters reports Apple posted results above consensus and guided cautiously higher.",
            "source": "reuters.com",
            "url": "https://www.reuters.com/technology/apple-earnings",
        },
        event_type="earnings",
        event_confidence=0.92,
        link_confidence=0.95,
    )
    assert quality["source_tier"] == "tier1"
    assert quality["source_bucket"] == "primary_reporting"
    assert "source_tier:tier1" in quality["quality_reasons"]
    assert quality["watchlist_eligible"] is True


def test_quality_gate_filters_low_signal_recap_source_under_strict_gate() -> None:
    quality = assess_event_quality(
        {
            "title": "Wall Street Slumps as Geopolitical Tensions Drive Oil Higher",
            "description": "A recap of the market session as stocks moved lower and energy rose.",
            "source": "thestockmarketwatch.com",
            "url": "https://thestockmarketwatch.com/markets/wall-street-slumps",
        },
        event_type="macro",
        event_confidence=0.70,
        link_confidence=0.0,
    )
    assert quality["source_tier"] == "tier3"
    assert quality["watchlist_eligible"] is False
    assert "low_signal_hard_filtered" in quality["quality_reasons"]


def test_resolve_source_policy_maps_marketbeat_to_recap_override() -> None:
    profile = resolve_source_policy(
        {
            "source": "marketbeat.com",
            "url": "https://www.marketbeat.com/stocks/example",
        }
    )
    assert profile["source_bucket"] == "recap_aggregator"
    assert profile["source_strict_watchlist"] is True


def test_resolve_source_policy_blocks_general_interest_source_from_event_engine() -> None:
    profile = resolve_source_policy(
        {
            "source": "eonline.com",
            "url": "https://www.eonline.com/news/example",
        }
    )
    assert profile["source_block_watchlist"] is True
    assert profile["source_block_event_engine"] is True


def test_resolve_source_policy_blocks_celebrity_blog_source_from_event_engine() -> None:
    profile = resolve_source_policy(
        {
            "source": "justjared.com",
            "url": "https://www.justjared.com/2026/03/05/example",
        }
    )
    assert profile["source_block_watchlist"] is True
    assert profile["source_block_event_engine"] is True

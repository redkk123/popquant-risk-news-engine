from __future__ import annotations

from datetime import datetime

from services.sandbox_time import build_replay_timestamp_defaults


def test_build_replay_timestamp_defaults_uses_24h_delay_for_newsapi() -> None:
    now = datetime.fromisoformat("2026-03-06T22:52:00-03:00")
    result = build_replay_timestamp_defaults(
        mode="replay_as_of_timestamp",
        fixture_mode=False,
        primary_provider="newsapi",
        now=now,
    )

    assert result["current_timestamp"] == "2026-03-06T22:52:00-03:00"
    assert result["suggested_timestamp"] == "2026-03-05T22:52:00-03:00"
    assert result["is_newsapi_delayed"] is True
    assert result["auto_mode"] == "newsapi_delayed_24h"


def test_build_replay_timestamp_defaults_uses_live_now_for_non_newsapi() -> None:
    now = datetime.fromisoformat("2026-03-06T22:52:00-03:00")
    result = build_replay_timestamp_defaults(
        mode="replay_as_of_timestamp",
        fixture_mode=False,
        primary_provider="alphavantage",
        now=now,
    )

    assert result["current_timestamp"] == "2026-03-06T22:52:00-03:00"
    assert result["suggested_timestamp"] == "2026-03-06T22:52:00-03:00"
    assert result["is_newsapi_delayed"] is False
    assert result["auto_mode"] == "live_now"

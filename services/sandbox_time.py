from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any


def build_replay_timestamp_defaults(
    *,
    mode: str,
    fixture_mode: bool,
    primary_provider: str | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = (now or datetime.now().astimezone()).replace(microsecond=0)
    delayed_newsapi = mode == "replay_as_of_timestamp" and not fixture_mode and primary_provider == "newsapi"
    live_now = mode == "replay_as_of_timestamp" and not fixture_mode and primary_provider not in {None, "newsapi"}

    if delayed_newsapi:
        suggested = current - timedelta(hours=24)
        mode_label = "newsapi_delayed_24h"
    else:
        suggested = current
        mode_label = "live_now"

    return {
        "current_timestamp": current.isoformat(),
        "suggested_timestamp": suggested.isoformat(),
        "primary_provider": primary_provider or "none",
        "auto_mode": mode_label,
        "auto_active": mode == "replay_as_of_timestamp" and not fixture_mode,
        "is_newsapi_delayed": delayed_newsapi,
        "is_live_now": live_now or (mode == "replay_as_of_timestamp" and not fixture_mode and primary_provider is None),
    }

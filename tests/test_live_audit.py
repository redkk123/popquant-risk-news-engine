from __future__ import annotations

import pandas as pd

from event_engine.live_audit import build_live_event_audit


def test_build_live_event_audit_counts_filtered_and_zero_link_events() -> None:
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_1",
                "event_type": "macro",
                "source": "example.com",
                "tickers": [],
                "watchlist_eligible": True,
                "quality_label": "high",
                "link_confidence": 0.0,
                "event_confidence": 0.95,
            },
            {
                "event_id": "evt_2",
                "event_type": "earnings",
                "source": "globenewswire.com",
                "tickers": ["AAPL"],
                "watchlist_eligible": False,
                "quality_label": "low",
                "link_confidence": 0.0,
                "event_confidence": 0.63,
            },
        ]
    )

    audit = build_live_event_audit(events)

    assert audit["summary"]["total_events"] == 2
    assert audit["summary"]["watchlist_eligible_events"] == 1
    assert audit["summary"]["filtered_events"] == 1
    assert audit["summary"]["zero_link_events"] == 1
    assert audit["summary"]["suspicious_link_events"] == 1
    assert audit["summary"]["eligible_suspicious_link_events"] == 0


def test_live_audit_does_not_flag_macro_benchmark_link_as_suspicious() -> None:
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_macro",
                "event_type": "macro",
                "source": "example.com",
                "tickers": ["SPY"],
                "watchlist_eligible": True,
                "quality_label": "medium",
                "link_confidence": 0.65,
                "event_confidence": 0.63,
            }
        ]
    )

    audit = build_live_event_audit(events)

    assert audit["summary"]["suspicious_link_events"] == 0
    assert audit["summary"]["eligible_suspicious_link_events"] == 0


def test_live_audit_ignores_filtered_commentary_for_suspicious_link_metric() -> None:
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_commentary",
                "event_type": "commentary",
                "source": "zacks.com",
                "tickers": ["JNJ"],
                "watchlist_eligible": False,
                "quality_label": "low",
                "link_confidence": 0.95,
                "event_confidence": 0.71,
            }
        ]
    )

    audit = build_live_event_audit(events)

    assert audit["summary"]["suspicious_link_events"] == 0
    assert audit["summary"]["eligible_suspicious_link_events"] == 0

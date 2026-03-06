from __future__ import annotations

import pandas as pd

from event_engine.live_validation import (
    build_validation_windows,
    choose_validation_providers,
    collect_gap_samples,
    load_symbol_universe,
    summarize_validation_runs,
)


def test_build_validation_windows_returns_descending_windows() -> None:
    windows = build_validation_windows(as_of="2026-03-06", windows=3, window_days=1, step_days=1)

    assert len(windows) == 3
    assert windows[0]["published_before"] == "2026-03-06"
    assert windows[1]["published_before"] == "2026-03-05"


def test_choose_validation_providers_prefers_newsapi_for_delayed_windows() -> None:
    decision = choose_validation_providers(
        ["marketaux", "thenewsapi", "newsapi", "alphavantage"],
        published_before="2026-03-05",
        now="2026-03-06T12:00:00Z",
    )

    assert decision["strategy"] == "delayed"
    assert decision["providers"] == ["newsapi", "thenewsapi", "marketaux", "alphavantage"]


def test_choose_validation_providers_keeps_alpha_ahead_of_newsapi_for_fresh_windows() -> None:
    decision = choose_validation_providers(
        ["marketaux", "thenewsapi", "newsapi", "alphavantage"],
        published_before="2026-03-06",
        now="2026-03-06T12:00:00Z",
    )

    assert decision["strategy"] == "fresh"
    assert decision["providers"] == ["marketaux", "thenewsapi", "alphavantage", "newsapi"]


def test_collect_gap_samples_flags_other_and_zero_link_non_macro() -> None:
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_1",
                "headline": "Unclear headline",
                "source": "example.com",
                "event_type": "other",
                "quality_label": "medium",
                "watchlist_eligible": True,
                "tickers": [],
                "anchored_provider_symbols": ["AAPL"],
                "link_confidence": 0.0,
                "event_confidence": 0.4,
                "quality_reasons": ["ok"],
                "event_reasons": ["fallback"],
            }
        ]
    )

    gaps = collect_gap_samples(events_frame=events, window_label="window_01", run_dir="D:/tmp/run")

    assert len(gaps) == 1
    assert "taxonomy_other" in gaps.loc[0, "gap_reason"]
    assert "zero_link_non_macro" in gaps.loc[0, "gap_reason"]


def test_collect_gap_samples_ignores_commentary_and_macro_benchmark_noise() -> None:
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_commentary",
                "headline": "Company Presents at Conference Transcript",
                "source": "example.com",
                "event_type": "commentary",
                "quality_label": "low",
                "watchlist_eligible": False,
                "tickers": [],
                "anchored_provider_symbols": [],
                "link_confidence": 0.0,
                "event_confidence": 0.8,
                "quality_reasons": ["generic_content_penalty"],
                "event_reasons": ["transcript"],
            },
            {
                "event_id": "evt_macro",
                "headline": "Oil spikes as geopolitical tensions rise",
                "source": "example.com",
                "event_type": "macro",
                "quality_label": "medium",
                "watchlist_eligible": True,
                "tickers": ["SPY"],
                "anchored_provider_symbols": [],
                "link_confidence": 0.65,
                "event_confidence": 0.7,
                "quality_reasons": ["watchlist_eligible"],
                "event_reasons": ["oil"],
            },
        ]
    )

    gaps = collect_gap_samples(events_frame=events, window_label="window_01", run_dir="D:/tmp/run")

    assert gaps.empty


def test_collect_gap_samples_ignores_filtered_unanchored_press_release_noise() -> None:
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_press",
                "headline": "Rumble Reports Fourth Quarter and Full Year 2025 Results",
                "source": "globenewswire.com",
                "event_type": "earnings",
                "quality_label": "low",
                "watchlist_eligible": False,
                "tickers": [],
                "anchored_provider_symbols": [],
                "link_confidence": 0.0,
                "event_confidence": 0.63,
                "quality_reasons": ["press_release_penalty"],
                "event_reasons": ["q4"],
            }
        ]
    )

    gaps = collect_gap_samples(events_frame=events, window_label="window_01", run_dir="D:/tmp/run")

    assert gaps.empty


def test_summarize_validation_runs_aggregates_rates() -> None:
    frame = pd.DataFrame(
        [
            {
                "status": "success",
                "window_origin": "fresh_sync",
                "fresh_sync_requested": True,
                "quota_blocked": False,
                "total_events": 4,
                "event_rows": 3,
                "watchlist_eligible_rate": 0.5,
                "filtered_rate": 0.5,
                "other_rate": 0.25,
                "suspicious_link_rate": 0.0,
                "active_other_rate": 0.0,
                "active_suspicious_link_rate": 0.0,
                "event_type_distribution": {"macro": 2, "other": 1},
                "quality_distribution": {"high": 2, "low": 1},
            },
            {
                "status": "success",
                "window_origin": "archive_reuse",
                "fresh_sync_requested": True,
                "quota_blocked": True,
                "total_events": 6,
                "event_rows": 4,
                "watchlist_eligible_rate": 0.6,
                "filtered_rate": 0.4,
                "other_rate": 0.1,
                "suspicious_link_rate": 0.05,
                "active_other_rate": 0.02,
                "active_suspicious_link_rate": 0.0,
                "event_type_distribution": {"macro": 3, "earnings": 2},
                "quality_distribution": {"medium": 3},
            },
        ]
    )

    summary = summarize_validation_runs(frame)

    assert summary["successful_windows"] == 2
    assert summary["total_events"] == 10
    assert summary["fresh_sync_windows"] == 1
    assert summary["archive_reuse_windows"] == 1
    assert summary["failed_windows"] == 0
    assert summary["quota_blocked_windows"] == 1
    assert summary["event_type_totals"]["macro"] == 5
    assert summary["quality_totals"]["medium"] == 3
    assert summary["avg_active_other_rate"] == 0.01
    assert summary["fresh_sync_metrics"]["avg_active_other_rate"] == 0.0
    assert summary["archive_reuse_metrics"]["avg_active_other_rate"] == 0.02


def test_load_symbol_universe_supports_thematic_pack(tmp_path) -> None:
    config_path = tmp_path / "symbols.yaml"
    config_path.write_text(
        "\n".join(
            [
                "symbols:",
                "  - AAPL",
                "packs:",
                "  semis_pack:",
                "    symbols:",
                "      - NVDA",
                "      - AMD",
            ]
        ),
        encoding="utf-8",
    )

    symbols = load_symbol_universe(config_path, pack="semis_pack")

    assert symbols == ["NVDA", "AMD"]

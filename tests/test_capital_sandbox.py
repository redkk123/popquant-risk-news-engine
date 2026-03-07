from __future__ import annotations

import pandas as pd

from capital.sandbox import build_snapshot_frame, run_capital_sandbox, run_capital_sandbox_live_session


def test_run_capital_sandbox_generates_summary_and_journal() -> None:
    index = pd.date_range("2026-03-06 13:00:00+00:00", periods=8, freq="1min")
    prices = pd.DataFrame(
        {
            "AAPL": [100.0, 100.8, 101.5, 102.1, 102.8, 103.4, 104.0, 104.7],
            "MSFT": [100.0, 100.4, 100.9, 101.3, 101.8, 102.1, 102.6, 103.0],
        },
        index=index,
    )
    benchmark = pd.Series([100.0, 100.2, 100.4, 100.5, 100.7, 100.9, 101.0, 101.2], index=index, name="SPY")
    weights = pd.Series({"AAPL": 0.6, "MSFT": 0.4})
    mapping_config = {
        "settings": {
            "source_scaling": {
                "tiers": {"top_tier": 1.0},
                "buckets": {"editorial": 1.0},
            }
        },
        "event_mappings": {
            "guidance": {
                "positive": {
                    "return_shock": 0.01,
                    "vol_multiplier": 1.05,
                    "correlation_multiplier": 1.0,
                },
                "negative": {
                    "return_shock": -0.01,
                    "vol_multiplier": 1.10,
                    "correlation_multiplier": 1.05,
                },
            },
            "other": {
                "neutral": {
                    "return_shock": 0.0,
                    "vol_multiplier": 1.0,
                    "correlation_multiplier": 1.0,
                }
            },
        },
    }
    events = [
        {
            "event_id": "evt_positive",
            "published_at": index[0].isoformat(),
            "headline": "Apple lifts guidance",
            "event_type": "guidance",
            "event_subtype": None,
            "polarity": 0.8,
            "severity": 0.9,
            "tickers": ["AAPL"],
            "event_confidence": 0.9,
            "link_confidence": 0.9,
            "source_tier": "top_tier",
            "source_bucket": "editorial",
            "watchlist_eligible": True,
        }
    ]

    result = run_capital_sandbox(
        price_frame=prices,
        benchmark_prices=benchmark,
        weights=weights,
        events=events,
        mapping_config=mapping_config,
        ticker_sector_map={"AAPL": "Technology", "MSFT": "Technology"},
        initial_capital=100.0,
        decision_interval_seconds=10,
        session_minutes=5,
    )

    summary = result["summary_frame"]
    journal = result["journal_frame"]
    assert set(summary["path_name"]) == {
        "cash_only",
        "benchmark_hold",
        "portfolio_hold",
        "event_quant_pathing",
        "benchmark_timing",
        "capped_risk_long",
        "sector_basket",
    }
    assert not journal.empty
    dynamic_row = summary.loc[summary["path_name"] == "event_quant_pathing"].iloc[0]
    assert dynamic_row["trade_count"] >= 1
    assert dynamic_row["confirmed_risk_steps"] >= 1
    sector_row = summary.loc[summary["path_name"] == "sector_basket"].iloc[0]
    assert sector_row["trade_count"] >= 1
    assert journal["sector_basket_target_exposure"].max() > 0.0
    assert journal["sector_basket_tickers"].str.contains("AAPL").any()
    assert {"path_confirmation", "path_confirmation_score"}.issubset(journal.columns)


def test_run_capital_sandbox_live_session_uses_minute_steps_without_sleeping() -> None:
    frames = [
        pd.DataFrame(
            {
                "AAPL": [100.0, 100.5],
                "MSFT": [100.0, 100.2],
                "SPY": [100.0, 100.1],
            },
            index=pd.to_datetime(["2026-03-06T13:00:00Z", "2026-03-06T13:01:00Z"]),
        ),
        pd.DataFrame(
            {
                "AAPL": [100.0, 100.5, 101.0],
                "MSFT": [100.0, 100.2, 100.6],
                "SPY": [100.0, 100.1, 100.3],
            },
            index=pd.to_datetime(["2026-03-06T13:00:00Z", "2026-03-06T13:01:00Z", "2026-03-06T13:02:00Z"]),
        ),
        pd.DataFrame(
            {
                "AAPL": [100.0, 100.5, 101.0, 101.4],
                "MSFT": [100.0, 100.2, 100.6, 100.9],
                "SPY": [100.0, 100.1, 100.3, 100.4],
            },
            index=pd.to_datetime(
                [
                    "2026-03-06T13:00:00Z",
                    "2026-03-06T13:01:00Z",
                    "2026-03-06T13:02:00Z",
                    "2026-03-06T13:03:00Z",
                ]
            ),
        ),
    ]

    fetch_count = {"value": 0}
    sleep_calls: list[int] = []

    def _fetcher():
        index = min(fetch_count["value"], len(frames) - 1)
        fetch_count["value"] += 1
        return frames[index]

    def _sleep(seconds: int) -> None:
        sleep_calls.append(seconds)

    result = run_capital_sandbox_live_session(
        price_fetcher=_fetcher,
        weights=pd.Series({"AAPL": 0.6, "MSFT": 0.4}),
        benchmark_name="SPY",
        events=[],
        mapping_config={
            "settings": {"source_scaling": {"tiers": {}, "buckets": {}}},
            "event_mappings": {
                "other": {
                    "neutral": {
                        "return_shock": 0.0,
                        "vol_multiplier": 1.0,
                        "correlation_multiplier": 1.0,
                    }
                }
            },
        },
        ticker_sector_map={"AAPL": "Technology", "MSFT": "Technology"},
        initial_capital=100.0,
        poll_interval_seconds=60,
        session_minutes=3,
        sleep_fn=_sleep,
    )

    summary = result["summary_frame"]
    journal = result["journal_frame"]
    assert len(journal) == 3
    assert sleep_calls == [60, 60, 60]
    assert set(summary["path_name"]) == {
        "cash_only",
        "benchmark_hold",
        "portfolio_hold",
        "event_quant_pathing",
        "benchmark_timing",
        "capped_risk_long",
        "sector_basket",
    }
    pathing_row = summary.loc[summary["path_name"] == "event_quant_pathing"].iloc[0]
    assert pathing_row["trade_count"] == 0
    assert "path_blocked_count" in summary.columns


def test_run_capital_sandbox_live_session_refreshes_events_and_records_quant_fields() -> None:
    frames = [
        pd.DataFrame(
            {
                "AAPL": [100.0, 100.4],
                "MSFT": [100.0, 100.1],
                "SPY": [100.0, 100.1],
            },
            index=pd.to_datetime(["2026-03-06T13:00:00Z", "2026-03-06T13:01:00Z"]),
        ),
        pd.DataFrame(
            {
                "AAPL": [100.0, 100.4, 101.2],
                "MSFT": [100.0, 100.1, 100.7],
                "SPY": [100.0, 100.1, 100.4],
            },
            index=pd.to_datetime(["2026-03-06T13:00:00Z", "2026-03-06T13:01:00Z", "2026-03-06T13:02:00Z"]),
        ),
    ]
    fetch_count = {"value": 0}

    def _fetcher():
        index = min(fetch_count["value"], len(frames) - 1)
        fetch_count["value"] += 1
        return frames[index]

    refresh_calls: list[int] = []

    def _refresh(*, as_of, step: int, current_events):
        del as_of, current_events
        refresh_calls.append(step)
        return {
            "status": "success",
            "events": [
                {
                    "event_id": "evt_refresh",
                    "published_at": "2026-03-06T13:01:30Z",
                    "headline": "Apple raises outlook",
                    "event_type": "guidance",
                    "event_subtype": "earnings_guidance",
                    "polarity": 0.8,
                    "severity": 0.9,
                    "tickers": ["AAPL"],
                    "event_confidence": 0.9,
                    "link_confidence": 0.9,
                    "source_tier": "top_tier",
                    "source_bucket": "editorial",
                    "watchlist_eligible": True,
                }
            ],
            "sync_stats": {"provider": "thenewsapi", "articles_seen": 1, "inserted": 1},
        }

    result = run_capital_sandbox_live_session(
        price_fetcher=_fetcher,
        weights=pd.Series({"AAPL": 0.6, "MSFT": 0.4}),
        benchmark_name="SPY",
        events=[],
        mapping_config={
            "settings": {"source_scaling": {"tiers": {"top_tier": 1.0}, "buckets": {"editorial": 1.0}}},
            "event_mappings": {
                "guidance": {
                    "positive": {
                        "return_shock": 0.01,
                        "vol_multiplier": 1.05,
                        "correlation_multiplier": 1.0,
                    }
                },
                "other": {
                    "neutral": {
                        "return_shock": 0.0,
                        "vol_multiplier": 1.0,
                        "correlation_multiplier": 1.0,
                    }
                },
            },
        },
        ticker_sector_map={"AAPL": "Technology", "MSFT": "Technology"},
        initial_capital=100.0,
        poll_interval_seconds=60,
        session_minutes=2,
        event_refresh_interval_steps=1,
        event_refresh_callback=_refresh,
        sleep_fn=lambda _: None,
    )

    journal = result["journal_frame"]
    session_meta = result["session_meta"]
    assert refresh_calls == [1, 2]
    assert session_meta["news_refresh_attempts"] == 2
    assert session_meta["news_refresh_successes"] == 2
    assert {"quant_confirmation", "risk_on_allowed", "refresh_status"}.issubset(journal.columns)
    assert set(journal["refresh_status"]) == {"success"}


def test_run_capital_sandbox_live_session_emits_absolute_countdown_timestamps() -> None:
    frames = [
        pd.DataFrame(
            {
                "BTC-USD": [90000.0, 90020.0],
            },
            index=pd.to_datetime(["2026-03-06T13:00:00Z", "2026-03-06T13:01:00Z"]),
        ),
        pd.DataFrame(
            {
                "BTC-USD": [90000.0, 90020.0, 90010.0],
            },
            index=pd.to_datetime(["2026-03-06T13:00:00Z", "2026-03-06T13:01:00Z", "2026-03-06T13:02:00Z"]),
        ),
    ]
    fetch_count = {"value": 0}
    progress_events: list[dict[str, object]] = []

    def _fetcher():
        index = min(fetch_count["value"], len(frames) - 1)
        fetch_count["value"] += 1
        return frames[index]

    def _progress(payload: dict[str, object]) -> None:
        progress_events.append(payload)

    run_capital_sandbox_live_session(
        price_fetcher=_fetcher,
        weights=pd.Series({"BTC-USD": 1.0}),
        benchmark_name="BTC-USD",
        events=[],
        mapping_config={
            "settings": {"source_scaling": {"tiers": {}, "buckets": {}}},
            "event_mappings": {
                "other": {
                    "neutral": {
                        "return_shock": 0.0,
                        "vol_multiplier": 1.0,
                        "correlation_multiplier": 1.0,
                    }
                }
            },
        },
        ticker_sector_map={"BTC-USD": "Crypto"},
        initial_capital=100.0,
        poll_interval_seconds=60,
        session_minutes=2,
        sleep_fn=lambda _seconds: None,
        progress_callback=_progress,
    )

    assert progress_events
    first = progress_events[0]
    last = progress_events[-1]
    assert "session_started_at" in first
    assert "expected_end_at" in first
    assert pd.Timestamp(last["expected_end_at"]) >= pd.Timestamp(last["session_started_at"])


def test_run_capital_sandbox_live_session_skips_refresh_after_quota_error() -> None:
    frames = [
        pd.DataFrame(
            {
                "AAPL": [100.0, 100.2],
                "MSFT": [100.0, 100.1],
                "SPY": [100.0, 100.1],
            },
            index=pd.to_datetime(["2026-03-06T13:00:00Z", "2026-03-06T13:01:00Z"]),
        ),
        pd.DataFrame(
            {
                "AAPL": [100.0, 100.2, 100.4],
                "MSFT": [100.0, 100.1, 100.2],
                "SPY": [100.0, 100.1, 100.2],
            },
            index=pd.to_datetime(["2026-03-06T13:00:00Z", "2026-03-06T13:01:00Z", "2026-03-06T13:02:00Z"]),
        ),
    ]
    fetch_count = {"value": 0}

    def _fetcher():
        index = min(fetch_count["value"], len(frames) - 1)
        fetch_count["value"] += 1
        return frames[index]

    refresh_calls: list[int] = []

    def _refresh(*, as_of, step: int, current_events):
        del as_of, current_events
        refresh_calls.append(step)
        return {
            "status": "error",
            "error": "daily limit reached",
            "events": [],
            "sync_stats": {"provider": "thenewsapi", "articles_seen": 0, "inserted": 0},
        }

    result = run_capital_sandbox_live_session(
        price_fetcher=_fetcher,
        weights=pd.Series({"AAPL": 0.6, "MSFT": 0.4}),
        benchmark_name="SPY",
        events=[],
        mapping_config={
            "settings": {"source_scaling": {"tiers": {}, "buckets": {}}},
            "event_mappings": {
                "other": {
                    "neutral": {
                        "return_shock": 0.0,
                        "vol_multiplier": 1.0,
                        "correlation_multiplier": 1.0,
                    }
                }
            },
        },
        ticker_sector_map={"AAPL": "Technology", "MSFT": "Technology"},
        initial_capital=100.0,
        poll_interval_seconds=60,
        session_minutes=2,
        event_refresh_interval_steps=1,
        event_refresh_callback=_refresh,
        sleep_fn=lambda _: None,
    )

    journal = result["journal_frame"]
    session_meta = result["session_meta"]
    assert refresh_calls == [1]
    assert session_meta["news_refresh_attempts"] == 1
    assert session_meta["news_refresh_errors"] == 1
    assert session_meta["news_refresh_skipped_quota_cooldown"] == 1
    assert "quota_cooldown_skip" in set(journal["refresh_status"])


def test_build_snapshot_frame_preserves_live_progress_when_market_timestamp_repeats() -> None:
    equity = pd.DataFrame(
        [
            {
                "timestamp": "2026-03-06T20:59:00Z",
                "capture_timestamp": "2026-03-06T23:10:00Z",
                "session_step": 1,
                "path_name": "cash_only",
                "capital": 100.0,
            },
            {
                "timestamp": "2026-03-06T20:59:00Z",
                "capture_timestamp": "2026-03-06T23:11:00Z",
                "session_step": 2,
                "path_name": "cash_only",
                "capital": 100.0,
            },
        ]
    )

    snapshot = build_snapshot_frame(equity, frequency="1min")

    assert len(snapshot) == 2
    assert snapshot["session_step"].tolist() == [1, 2]
    assert snapshot["tracking_time"].astype(str).tolist() == [
        "2026-03-06 23:10:00+00:00",
        "2026-03-06 23:11:00+00:00",
    ]

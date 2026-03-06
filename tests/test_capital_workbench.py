from __future__ import annotations

import json

import pandas as pd

from services import capital_workbench as capital_workbench_module


def test_run_capital_sandbox_compare_workbench_combines_sessions(monkeypatch, tmp_path) -> None:
    prepared = {
        "metadata": {"portfolio_id": "demo_book"},
        "positions": pd.DataFrame([{"ticker": "AAPL", "weight": 1.0}]),
        "sync_stats": {"provider": "fixture"},
        "pipeline_stats": {"events": 1},
        "output_root": tmp_path / "output",
        "mode": "replay_intraday",
    }

    def _prepare(**kwargs):
        return prepared

    def _run_single(*, prepared, initial_capital, decision_interval_seconds, session_minutes, news_refresh_minutes, fee_rate, slippage_rate):
        del prepared, initial_capital, decision_interval_seconds, news_refresh_minutes, fee_rate, slippage_rate
        label = f"{session_minutes}m"
        summary = pd.DataFrame(
            [
                {
                    "path_name": "cash_only",
                    "final_capital": 100.0,
                    "total_return": 0.0,
                    "max_drawdown": 0.0,
                    "trade_count": 0,
                    "total_costs": 0.0,
                    "avg_capital": 100.0,
                    "session_minutes": session_minutes,
                    "session_label": label,
                }
            ]
        )
        journal = pd.DataFrame(
            [
                {
                    "timestamp": "2026-03-06T00:00:00Z",
                    "path_name": "event_quant_pathing",
                    "capital_after_costs": 100.0,
                    "target_exposure": 0.0,
                    "action": "hold_existing",
                    "decision_reason": "no_eligible_event",
                    "session_minutes": session_minutes,
                    "session_label": label,
                }
            ]
        )
        equity = pd.DataFrame(
            [
                {
                    "timestamp": "2026-03-06T00:00:00Z",
                    "path_name": "cash_only",
                    "capital": 100.0,
                    "session_minutes": session_minutes,
                    "session_label": label,
                }
            ]
        )
        snapshots = pd.DataFrame(
            [
                {
                    "snapshot_time": "2026-03-06T00:00:00Z",
                    "path_name": "cash_only",
                    "capital": 100.0,
                    "session_minutes": session_minutes,
                    "session_label": label,
                }
            ]
        )
        return {
            "summary_frame": summary,
            "journal_frame": journal,
            "equity_frame": equity,
            "snapshot_frame": snapshots,
            "effective_interval_seconds": 10,
            "effective_session_minutes": session_minutes,
            "session_meta": {},
        }

    monkeypatch.setattr(capital_workbench_module, "_prepare_capital_sandbox_inputs", _prepare)
    monkeypatch.setattr(capital_workbench_module, "_run_single_capital_session", _run_single)

    result = capital_workbench_module.run_capital_sandbox_compare_workbench(
        portfolio_config="ignored.json",
        session_minutes_list=[5, 15, 30],
        output_dir=tmp_path / "sandbox_outputs",
    )

    assert set(result["summary_frame"]["session_label"]) == {"5m", "15m", "30m"}
    assert result["outputs"]["report_md"].endswith("capital_compare_report.md")


def test_run_capital_sandbox_compare_workbench_rejects_live_mode() -> None:
    try:
        capital_workbench_module.run_capital_sandbox_compare_workbench(
            portfolio_config="ignored.json",
            mode="live_session_real_time",
        )
    except ValueError as exc:
        assert "only supported" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected ValueError for live compare mode")


def test_prepare_capital_sandbox_inputs_prefers_newsapi_for_delayed_windows(monkeypatch, tmp_path) -> None:
    portfolio_path = tmp_path / "portfolio.json"
    portfolio_path.write_text(
        json.dumps(
            {
                "portfolio_id": "demo_book",
                "description": "demo",
                "base_currency": "USD",
                "benchmark": "SPY",
                "positions": [
                    {"ticker": "AAPL", "weight": 0.6},
                    {"ticker": "MSFT", "weight": 0.4},
                ],
            }
        ),
        encoding="utf-8",
    )

    call_details: dict[str, object] = {}

    def _sync_news(repository, **kwargs):
        del repository
        call_details["providers"] = kwargs["providers"]
        return {
            "provider": "newsapi",
            "providers_requested": list(kwargs["providers"]),
            "providers_used": ["newsapi"],
            "articles_seen": 0,
            "inserted": 0,
            "skipped": 0,
            "pages_fetched": 0,
            "partial_success": False,
        }

    class _FakeRepository:
        def __init__(self, root):
            self.root = root

        def load_events_frame(self):
            return pd.DataFrame()

    monkeypatch.setattr(capital_workbench_module, "sync_news", _sync_news)
    monkeypatch.setattr(capital_workbench_module, "process_raw_documents", lambda repository, alias_path: {"events": 0})
    monkeypatch.setattr(capital_workbench_module, "NewsRepository", _FakeRepository)
    monkeypatch.setattr(
        capital_workbench_module,
        "load_prices",
        lambda **kwargs: pd.DataFrame(
            {
                "AAPL": [100.0, 101.0],
                "MSFT": [100.0, 100.5],
                "SPY": [100.0, 100.3],
            },
            index=pd.to_datetime(["2026-03-03T00:00:00Z", "2026-03-04T00:00:00Z"]),
        ),
    )

    prepared = capital_workbench_module._prepare_capital_sandbox_inputs(
        portfolio_config=portfolio_path,
        mode="historical_daily",
        session_minutes=5,
        start="2026-03-01",
        end="2026-03-04",
        news_fixture=None,
        fixture_provider="marketaux",
        providers=["marketaux", "thenewsapi", "newsapi", "alphavantage"],
        alias_table=None,
        event_map_config=None,
        ticker_sector_map_path=None,
        symbol_batch_size=5,
        limit=5,
        max_pages=1,
        published_after="2026-03-02",
        published_before=None,
        as_of_timestamp=None,
        intraday_period="5d",
        cache_dir=None,
        output_dir=tmp_path / "out",
    )

    assert prepared["provider_strategy"] == "delayed"
    assert call_details["providers"] == ["newsapi", "thenewsapi", "marketaux", "alphavantage"]


def test_live_refresh_callback_keeps_alphavantage_ahead_of_newsapi_for_fresh_windows(monkeypatch, tmp_path) -> None:
    call_details: dict[str, object] = {}

    class _FakeRepository:
        def load_events_frame(self):
            return pd.DataFrame()

    def _sync_news(repository, **kwargs):
        del repository
        call_details["providers"] = kwargs["providers"]
        return {
            "provider": "alphavantage",
            "providers_requested": list(kwargs["providers"]),
            "providers_used": ["alphavantage"],
            "articles_seen": 0,
            "inserted": 0,
            "skipped": 0,
            "pages_fetched": 0,
            "partial_success": False,
        }

    monkeypatch.setattr(capital_workbench_module, "sync_news", _sync_news)
    monkeypatch.setattr(capital_workbench_module, "process_raw_documents", lambda repository, alias_path: {"events": 0})

    callback = capital_workbench_module._build_live_event_refresh_callback(
        {
            "news_fixture": None,
            "repository": _FakeRepository(),
            "alias_table_path": tmp_path / "aliases.csv",
            "providers": ["marketaux", "thenewsapi", "newsapi", "alphavantage"],
            "provider_symbols": ["AAPL", "MSFT", "SPY"],
            "limit": 3,
            "max_pages": 1,
            "symbol_batch_size": 5,
            "published_after": "2026-03-05",
        }
    )

    result = callback(
        as_of=pd.Timestamp("2026-03-06T12:00:00Z"),
        step=1,
        current_events=[],
    )

    assert result["status"] == "success"
    assert result["sync_stats"]["provider_strategy"] == "fresh"
    assert call_details["providers"] == ["marketaux", "thenewsapi", "alphavantage", "newsapi"]


def test_prepare_capital_sandbox_inputs_supports_replay_as_of_timestamp(monkeypatch, tmp_path) -> None:
    portfolio_path = tmp_path / "portfolio.json"
    portfolio_path.write_text(
        json.dumps(
            {
                "portfolio_id": "demo_book",
                "description": "demo",
                "base_currency": "USD",
                "benchmark": "SPY",
                "positions": [
                    {"ticker": "AAPL", "weight": 0.6},
                    {"ticker": "MSFT", "weight": 0.4},
                ],
            }
        ),
        encoding="utf-8",
    )

    class _FakeRepository:
        def __init__(self, root):
            self.root = root

        def load_events_frame(self):
            return pd.DataFrame()

    call_details: dict[str, object] = {}

    def _sync_news(repository, **kwargs):
        del repository
        call_details["providers"] = kwargs["providers"]
        call_details["published_before"] = kwargs["published_before"]
        return {
            "provider": "newsapi",
            "providers_requested": list(kwargs["providers"]),
            "providers_used": ["newsapi"],
            "articles_seen": 0,
            "inserted": 0,
            "skipped": 0,
            "pages_fetched": 0,
            "partial_success": False,
        }

    monkeypatch.setattr(capital_workbench_module, "sync_news", _sync_news)
    monkeypatch.setattr(capital_workbench_module, "process_raw_documents", lambda repository, alias_path: {"events": 0})
    monkeypatch.setattr(capital_workbench_module, "NewsRepository", _FakeRepository)
    monkeypatch.setattr(
        capital_workbench_module,
        "load_intraday_prices",
        lambda **kwargs: pd.DataFrame(
            {
                "AAPL": [100.0, 100.2, 100.4, 100.5],
                "MSFT": [100.0, 100.1, 100.3, 100.4],
                "SPY": [100.0, 100.1, 100.2, 100.3],
            },
            index=pd.to_datetime(
                [
                    "2026-03-05T21:59:00Z",
                    "2026-03-05T22:00:00Z",
                    "2026-03-05T22:01:00Z",
                    "2026-03-05T22:02:00Z",
                ]
            ),
        ),
    )

    prepared = capital_workbench_module._prepare_capital_sandbox_inputs(
        portfolio_config=portfolio_path,
        mode="replay_as_of_timestamp",
        session_minutes=2,
        start="2026-03-01",
        end=None,
        news_fixture=None,
        fixture_provider="marketaux",
        providers=["marketaux", "thenewsapi", "newsapi", "alphavantage"],
        alias_table=None,
        event_map_config=None,
        ticker_sector_map_path=None,
        symbol_batch_size=5,
        limit=5,
        max_pages=1,
        published_after=None,
        published_before=None,
        as_of_timestamp="2026-03-05T19:01:00-03:00",
        intraday_period="5d",
        cache_dir=None,
        output_dir=tmp_path / "out",
    )

    assert prepared["provider_strategy"] == "delayed"
    assert call_details["providers"] == ["newsapi", "thenewsapi", "marketaux", "alphavantage"]
    assert call_details["published_before"] == "2026-03-05T22:01:00+00:00"
    assert prepared["asset_prices"].index.max() == pd.Timestamp("2026-03-05T22:01:00Z")

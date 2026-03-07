from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from capital.reporting import (
    LIVE_EQUITY_COLUMNS,
    LIVE_JOURNAL_COLUMNS,
    LIVE_SNAPSHOT_COLUMNS,
    build_capital_compare_report,
    build_capital_sandbox_report,
    write_capital_live_progress,
    write_capital_compare_outputs,
    write_capital_sandbox_outputs,
)
from capital.sandbox import (
    build_snapshot_frame,
    run_capital_sandbox,
    run_capital_sandbox_live_session,
)
from data.loaders import load_intraday_prices, load_prices
from data.positions import load_portfolio_config, weights_series
from data.validation import validate_price_frame
from event_engine.ingestion.sync_news import ingest_fixture, sync_news
from event_engine.live_validation import choose_validation_providers
from event_engine.pipeline import process_raw_documents
from event_engine.storage.repository import NewsRepository
from fusion.scenario_mapper import load_event_mapping_config
from fusion.sector_mapping import load_ticker_sector_map
from services.pathing import PROJECT_ROOT
from services.portfolio_manager import load_portfolio_payload


def initialize_capital_live_run(
    *,
    portfolio_config: str | Path,
    session_minutes: int,
    decision_interval_seconds: int,
    output_dir: str | Path | None = None,
    run_id_override: str | None = None,
    providers: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    payload = load_portfolio_payload(portfolio_config)
    portfolio_id = str(payload["portfolio_id"])
    run_id = run_id_override or f"{pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%S%fZ')}_{portfolio_id}"
    output_root = Path(output_dir or (PROJECT_ROOT / "output" / "capital_sandbox")) / run_id
    session_started_at = pd.Timestamp.now(tz="UTC")
    effective_interval_seconds = max(60, int(decision_interval_seconds))
    total_steps = max(1, int((int(session_minutes) * 60 + effective_interval_seconds - 1) / effective_interval_seconds))
    expected_end_at = session_started_at + pd.Timedelta(seconds=total_steps * effective_interval_seconds)

    write_capital_live_progress(
        output_root=output_root,
        status_payload={
            "status": "starting",
            "mode": "live_session_real_time",
            "step": 0,
            "total_steps": total_steps,
            "current_timestamp": None,
            "session_started_at": session_started_at,
            "expected_end_at": expected_end_at,
            "session_minutes": int(session_minutes),
            "decision_interval_seconds": effective_interval_seconds,
            "portfolio_id": portfolio_id,
            "providers_used": list(providers),
            "degraded_to_empty_news": False,
            "sync_error": None,
            "best_path": None,
            "session_meta": {
                "mode": "live_session_real_time",
                "session_started_at": session_started_at,
                "expected_end_at": expected_end_at,
                "news_refresh_attempts": 0,
                "news_refresh_successes": 0,
                "news_refresh_errors": 0,
                "news_refresh_skipped": 0,
                "news_refresh_skipped_quota_cooldown": 0,
                "stale_price_steps": 0,
                "last_refresh_status": "not_requested",
                "last_refresh_provider": None,
                "last_refresh_events": 0,
                "last_refresh_inserted": 0,
                "last_refresh_articles_seen": 0,
                "last_refresh_error": None,
                "last_refresh_step": None,
                "quota_cooldown_until_step": 0,
            },
        },
        journal_frame=pd.DataFrame(columns=LIVE_JOURNAL_COLUMNS),
        equity_frame=pd.DataFrame(columns=LIVE_EQUITY_COLUMNS),
        snapshot_frame=pd.DataFrame(columns=LIVE_SNAPSHOT_COLUMNS),
    )

    return {
        "run_id": run_id,
        "output_root": output_root,
        "session_started_at": session_started_at,
        "expected_end_at": expected_end_at,
        "portfolio_id": portfolio_id,
        "total_steps": total_steps,
    }


def _prepare_capital_sandbox_inputs(
    *,
    portfolio_config: str | Path,
    mode: str,
    session_minutes: int,
    start: str,
    end: str | None,
    news_fixture: str | Path | None,
    fixture_provider: str,
    providers: list[str] | tuple[str, ...],
    alias_table: str | Path | None,
    event_map_config: str | Path | None,
    ticker_sector_map_path: str | Path | None,
    symbol_batch_size: int,
    limit: int,
    max_pages: int,
    published_after: str | None,
    published_before: str | None,
    as_of_timestamp: str | None,
    intraday_period: str,
    cache_dir: str | Path | None,
    output_dir: str | Path | None,
    run_id_override: str | None = None,
) -> dict[str, Any]:
    metadata, positions = load_portfolio_config(portfolio_config)
    weights = weights_series(positions)
    benchmark = metadata.get("benchmark")

    run_id = run_id_override or f"{pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%S%fZ')}_{metadata['portfolio_id']}"
    output_root = Path(output_dir or (PROJECT_ROOT / "output" / "capital_sandbox")) / run_id
    repository = NewsRepository(output_root / "repository")
    provider_symbols = sorted(set(weights.index.tolist() + ([benchmark] if benchmark else [])))

    effective_published_after = published_after
    effective_published_before = published_before
    if mode == "replay_as_of_timestamp":
        if not as_of_timestamp:
            raise ValueError("replay_as_of_timestamp mode requires as_of_timestamp.")
        as_of = pd.Timestamp(as_of_timestamp)
        if as_of.tzinfo is None:
            as_of = as_of.tz_localize("UTC")
        else:
            as_of = as_of.tz_convert("UTC")
        if not effective_published_before:
            effective_published_before = as_of.isoformat()
        if not effective_published_after:
            effective_published_after = (as_of - pd.Timedelta(days=2)).isoformat()

    provider_plan_before = (
        effective_published_before
        or as_of_timestamp
        or end
        or (pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=1)).date().isoformat()
    )
    provider_plan = choose_validation_providers(
        list(providers),
        published_before=provider_plan_before,
    )
    selected_providers = provider_plan["providers"]

    sync_error: str | None = None
    if news_fixture:
        sync_stats = ingest_fixture(repository, news_fixture, provider=fixture_provider)
    else:
        try:
            sync_stats = sync_news(
                repository,
                providers=selected_providers,
                symbols=provider_symbols,
                published_after=effective_published_after,
                published_before=effective_published_before,
                limit=limit,
                max_pages=max_pages,
                symbol_batch_size=symbol_batch_size,
            )
        except RuntimeError as exc:
            sync_error = str(exc)
            sync_stats = {
                "provider": "none",
                "providers_requested": selected_providers,
                "providers_used": [],
                "articles_seen": 0,
                "inserted": 0,
                "skipped": 0,
                "pages_fetched": 0,
                "partial_success": False,
                "degraded_to_empty_news": True,
                "sync_error": sync_error,
            }
    sync_stats["provider_strategy"] = provider_plan["strategy"]
    sync_stats["providers_requested"] = list(sync_stats.get("providers_requested", selected_providers))
    pipeline_stats = process_raw_documents(
        repository,
        alias_path=alias_table or (PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
    )
    if sync_error:
        pipeline_stats["degraded_to_empty_news"] = True
        pipeline_stats["sync_error"] = sync_error
    events_frame = repository.load_events_frame()
    events = events_frame.to_dict(orient="records")

    if mode == "replay_intraday":
        prices = load_intraday_prices(
            tickers=weights.index.tolist() + ([benchmark] if benchmark else []),
            period="1d",
            interval="1m",
        )
        effective_interval_seconds = None
        snapshot_frequency = "1min"
        asset_prices = validate_price_frame(prices.loc[:, weights.index.tolist()])
        benchmark_prices = prices[benchmark] if benchmark and benchmark in prices.columns else None
        price_fetcher = None
    elif mode == "live_session_real_time":
        requested_symbols = weights.index.tolist() + ([benchmark] if benchmark else [])

        def _price_fetcher() -> pd.DataFrame:
            return validate_price_frame(
                load_intraday_prices(
                    tickers=requested_symbols,
                    period="1d",
                    interval="1m",
                )
            )

        prices = _price_fetcher()
        effective_interval_seconds = None
        snapshot_frequency = "1min"
        asset_prices = prices.loc[:, weights.index.tolist()]
        benchmark_prices = prices[benchmark] if benchmark and benchmark in prices.columns else None
        price_fetcher = _price_fetcher
    elif mode == "replay_as_of_timestamp":
        if not as_of_timestamp:
            raise ValueError("replay_as_of_timestamp mode requires as_of_timestamp.")
        as_of = pd.Timestamp(as_of_timestamp)
        if as_of.tzinfo is None:
            as_of = as_of.tz_localize("UTC")
        else:
            as_of = as_of.tz_convert("UTC")
        prices = load_intraday_prices(
            tickers=weights.index.tolist() + ([benchmark] if benchmark else []),
            period=intraday_period,
            interval="1m",
        )
        prices = validate_price_frame(prices)
        aligned_index = pd.DatetimeIndex(prices.index)
        if aligned_index.tz is None:
            aligned_index = aligned_index.tz_localize("UTC")
        else:
            aligned_index = aligned_index.tz_convert("UTC")
        prices.index = aligned_index
        anchor_candidates = prices.index[prices.index <= as_of]
        if len(anchor_candidates) == 0:
            raise RuntimeError(
                f"No intraday prices available up to {as_of.isoformat()} for replay_as_of_timestamp."
            )
        anchor = anchor_candidates.max()
        session_start = anchor - pd.Timedelta(minutes=int(max(1, session_minutes)))
        trimmed = prices.loc[(prices.index >= session_start) & (prices.index <= anchor)].copy()
        if trimmed.empty:
            raise RuntimeError(
                f"No intraday prices available up to {as_of.isoformat()} for replay_as_of_timestamp."
            )
        effective_interval_seconds = None
        snapshot_frequency = "1min"
        asset_prices = trimmed.loc[:, weights.index.tolist()]
        benchmark_prices = trimmed[benchmark] if benchmark and benchmark in trimmed.columns else None
        price_fetcher = None
    elif mode == "historical_daily":
        prices = load_prices(
            tickers=weights.index.tolist() + ([benchmark] if benchmark else []),
            start=start,
            end=end or pd.Timestamp.today().date().isoformat(),
            cache_dir=cache_dir or (PROJECT_ROOT / "data" / "cache"),
        )
        effective_interval_seconds = 86400
        snapshot_frequency = "1D"
        prices = validate_price_frame(prices)
        asset_prices = prices.loc[:, weights.index.tolist()]
        benchmark_prices = prices[benchmark] if benchmark and benchmark in prices.columns else None
        price_fetcher = None
    else:
        raise ValueError(
            "Unsupported capital sandbox mode. Use live_session_real_time, replay_intraday, replay_as_of_timestamp, or historical_daily."
        )

    return {
        "metadata": metadata,
        "positions": positions,
        "weights": weights,
        "benchmark": benchmark,
        "events": events,
        "sync_stats": sync_stats,
        "pipeline_stats": pipeline_stats,
        "asset_prices": asset_prices,
        "benchmark_prices": benchmark_prices,
        "mapping_config": load_event_mapping_config(
            event_map_config or (PROJECT_ROOT / "config" / "event_scenario_map.yaml")
        ),
        "ticker_sector_map": load_ticker_sector_map(
            ticker_sector_map_path or (PROJECT_ROOT / "config" / "ticker_sector_map.csv")
        ),
        "repository": repository,
        "alias_table_path": Path(alias_table or (PROJECT_ROOT / "config" / "news_entity_aliases.csv")),
        "providers": selected_providers,
        "provider_strategy": provider_plan["strategy"],
        "provider_symbols": provider_symbols,
        "symbol_batch_size": int(symbol_batch_size),
        "limit": int(limit),
        "max_pages": int(max_pages),
        "published_after": effective_published_after,
        "published_before": effective_published_before,
        "as_of_timestamp": as_of_timestamp,
        "replay_anchor_timestamp": anchor.isoformat() if mode == "replay_as_of_timestamp" else None,
        "intraday_period": intraday_period,
        "news_fixture": str(news_fixture) if news_fixture else None,
        "output_root": output_root,
        "mode": mode,
        "effective_interval_seconds": effective_interval_seconds,
        "snapshot_frequency": snapshot_frequency,
        "price_fetcher": price_fetcher,
    }


def _build_live_event_refresh_callback(prepared: dict[str, Any]):
    if prepared.get("news_fixture"):
        return None

    repository = prepared["repository"]
    alias_table_path = prepared["alias_table_path"]
    providers = prepared["providers"]
    symbols = prepared["provider_symbols"]
    limit = int(prepared["limit"])
    max_pages = int(prepared["max_pages"])
    symbol_batch_size = int(prepared["symbol_batch_size"])
    default_published_after = prepared.get("published_after")

    def _refresh_events(*, as_of, step: int, current_events: list[dict[str, Any]]) -> dict[str, Any]:
        del step
        published_after = default_published_after
        if not published_after:
            published_after = (pd.Timestamp(as_of).tz_convert("UTC") - pd.Timedelta(days=2)).date().isoformat()
        published_before = (pd.Timestamp(as_of).tz_convert("UTC") + pd.Timedelta(days=1)).date().isoformat()
        provider_plan = choose_validation_providers(
            providers,
            published_before=published_before,
            now=as_of,
        )
        refresh_providers = provider_plan["providers"]
        try:
            sync_stats = sync_news(
                repository,
                providers=refresh_providers,
                symbols=symbols,
                published_after=published_after,
                published_before=published_before,
                limit=limit,
                max_pages=max_pages,
                symbol_batch_size=symbol_batch_size,
            )
            sync_stats["provider_strategy"] = provider_plan["strategy"]
            pipeline_stats = process_raw_documents(repository, alias_path=alias_table_path)
            events_frame = repository.load_events_frame()
            return {
                "status": "success",
                "events": events_frame.to_dict(orient="records"),
                "sync_stats": sync_stats,
                "pipeline_stats": pipeline_stats,
            }
        except RuntimeError as exc:
            return {
                "status": "error",
                "events": current_events,
                "error": str(exc),
                "sync_stats": {
                    "provider": "none",
                    "providers_requested": list(refresh_providers),
                    "providers_used": [],
                    "provider_strategy": provider_plan["strategy"],
                    "articles_seen": 0,
                    "inserted": 0,
                    "skipped": 0,
                    "pages_fetched": 0,
                    "failed_batch_count": 0,
                },
                "pipeline_stats": {},
            }

    return _refresh_events


def _run_single_capital_session(
    *,
    prepared: dict[str, Any],
    initial_capital: float,
    decision_interval_seconds: int,
    session_minutes: int,
    news_refresh_minutes: int | None,
    fee_rate: float,
    slippage_rate: float,
    session_started_at_override: Any | None = None,
) -> dict[str, Any]:
    mode = prepared["mode"]
    if mode in {"replay_intraday", "replay_as_of_timestamp"}:
        effective_interval_seconds = int(decision_interval_seconds)
        effective_session_minutes = int(session_minutes)
    elif mode == "live_session_real_time":
        effective_interval_seconds = max(60, int(decision_interval_seconds))
        effective_session_minutes = int(session_minutes)
    else:
        effective_interval_seconds = int(prepared["effective_interval_seconds"])
        effective_session_minutes = max(
            1440,
            max(int(len(prepared["asset_prices"].index) - 1), 1) * 1440,
        )

    if mode == "live_session_real_time":
        refresh_interval_steps = None
        if news_refresh_minutes is not None and int(news_refresh_minutes) > 0:
            refresh_interval_steps = max(
                1,
                int((int(news_refresh_minutes) * 60 + effective_interval_seconds - 1) / effective_interval_seconds),
            )
        event_refresh_callback = _build_live_event_refresh_callback(prepared)

        def _progress_callback(progress: dict[str, Any]) -> None:
            snapshot_frame = build_snapshot_frame(progress["equity_frame"], frequency=prepared["snapshot_frequency"])
            summary_frame = progress["summary_frame"]
            best_path = None
            if not summary_frame.empty and "final_capital" in summary_frame.columns:
                best_path = (
                    summary_frame.sort_values("final_capital", ascending=False)
                    .iloc[0]
                    .to_dict()
                )
            write_capital_live_progress(
                output_root=prepared["output_root"],
                status_payload={
                    "status": progress["status"],
                    "mode": mode,
                    "step": int(progress["step"]),
                    "total_steps": int(progress["total_steps"]),
                    "current_timestamp": progress["current_timestamp"],
                    "session_started_at": progress.get("session_started_at"),
                    "expected_end_at": progress.get("expected_end_at"),
                    "session_minutes": int(session_minutes),
                    "decision_interval_seconds": max(60, int(decision_interval_seconds)),
                    "portfolio_id": prepared["metadata"]["portfolio_id"],
                    "providers_used": prepared["sync_stats"].get(
                        "providers_used",
                        [prepared["sync_stats"].get("provider")],
                    ),
                    "degraded_to_empty_news": bool(prepared["sync_stats"].get("degraded_to_empty_news", False)),
                    "sync_error": prepared["sync_stats"].get("sync_error"),
                    "best_path": best_path,
                    "session_meta": progress.get("session_meta", {}),
                },
                journal_frame=progress["journal_frame"],
                equity_frame=progress["equity_frame"],
                snapshot_frame=snapshot_frame,
            )

        sandbox = run_capital_sandbox_live_session(
            price_fetcher=prepared["price_fetcher"],
            weights=prepared["weights"],
            benchmark_name=prepared["benchmark"],
            events=prepared["events"],
            mapping_config=prepared["mapping_config"],
            ticker_sector_map=prepared["ticker_sector_map"],
            initial_capital=initial_capital,
            poll_interval_seconds=max(60, int(decision_interval_seconds)),
            session_minutes=int(session_minutes),
            event_refresh_interval_steps=refresh_interval_steps,
            event_refresh_callback=event_refresh_callback,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
            progress_callback=_progress_callback,
            session_started_at_override=session_started_at_override,
        )
        effective_interval_seconds = max(60, int(decision_interval_seconds))
        effective_session_minutes = int(session_minutes)
    else:
        sandbox = run_capital_sandbox(
            price_frame=prepared["asset_prices"],
            benchmark_prices=prepared["benchmark_prices"],
            weights=prepared["weights"],
            events=prepared["events"],
            mapping_config=prepared["mapping_config"],
            ticker_sector_map=prepared["ticker_sector_map"],
            initial_capital=initial_capital,
            decision_interval_seconds=effective_interval_seconds,
            session_minutes=effective_session_minutes,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
        )
    snapshot_frame = build_snapshot_frame(
        sandbox["equity_frame"],
        frequency=prepared["snapshot_frequency"],
    )

    if mode in {"replay_intraday", "replay_as_of_timestamp", "live_session_real_time"}:
        session_label = f"{int(session_minutes)}m"
    else:
        session_label = "daily"
    summary_frame = sandbox["summary_frame"].copy()
    journal_frame = sandbox["journal_frame"].copy()
    equity_frame = sandbox["equity_frame"].copy()
    snapshot_frame = snapshot_frame.copy()

    for frame in (summary_frame, journal_frame, equity_frame, snapshot_frame):
        if not frame.empty:
            frame["session_minutes"] = int(session_minutes)
            frame["session_label"] = session_label

    return {
        "summary_frame": summary_frame,
        "journal_frame": journal_frame,
        "equity_frame": equity_frame,
        "snapshot_frame": snapshot_frame,
        "effective_interval_seconds": effective_interval_seconds,
        "effective_session_minutes": effective_session_minutes,
        "session_meta": sandbox.get("session_meta", {}),
    }


def run_capital_sandbox_workbench(
    *,
    portfolio_config: str | Path,
    mode: str = "live_session_real_time",
    initial_capital: float = 100.0,
    decision_interval_seconds: int = 60,
    session_minutes: int = 5,
    news_refresh_minutes: int | None = 2,
    start: str = "2024-01-01",
    end: str | None = None,
    news_fixture: str | Path | None = None,
    fixture_provider: str = "marketaux",
    providers: list[str] | tuple[str, ...] = ("marketaux", "thenewsapi", "newsapi", "alphavantage"),
    alias_table: str | Path | None = None,
    event_map_config: str | Path | None = None,
    ticker_sector_map_path: str | Path | None = None,
    fee_rate: float = 0.001,
    slippage_rate: float = 0.0005,
    symbol_batch_size: int = 5,
    limit: int = 3,
    max_pages: int = 1,
    published_after: str | None = None,
    published_before: str | None = None,
    as_of_timestamp: str | None = None,
    intraday_period: str = "5d",
    cache_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    run_id_override: str | None = None,
    session_started_at_override: Any | None = None,
) -> dict[str, Any]:
    prepared = _prepare_capital_sandbox_inputs(
        portfolio_config=portfolio_config,
        mode=mode,
        session_minutes=session_minutes,
        start=start,
        end=end,
        news_fixture=news_fixture,
        fixture_provider=fixture_provider,
        providers=providers,
        alias_table=alias_table,
        event_map_config=event_map_config,
        ticker_sector_map_path=ticker_sector_map_path,
        symbol_batch_size=symbol_batch_size,
        limit=limit,
        max_pages=max_pages,
        published_after=published_after,
        published_before=published_before,
        as_of_timestamp=as_of_timestamp,
        intraday_period=intraday_period,
        cache_dir=cache_dir,
        output_dir=output_dir,
        run_id_override=run_id_override,
    )
    session_result = _run_single_capital_session(
        prepared=prepared,
        initial_capital=initial_capital,
        decision_interval_seconds=decision_interval_seconds,
        session_minutes=session_minutes,
        news_refresh_minutes=news_refresh_minutes,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        session_started_at_override=session_started_at_override,
    )

    report_markdown = build_capital_sandbox_report(
        summary_frame=session_result["summary_frame"],
        journal_frame=session_result["journal_frame"],
        snapshot_frame=session_result["snapshot_frame"],
        metadata={
            "portfolio_id": prepared["metadata"]["portfolio_id"],
            "mode": mode,
            "initial_capital": initial_capital,
            "decision_interval_seconds": session_result["effective_interval_seconds"],
            "providers_used": prepared["sync_stats"].get(
                "providers_used",
                [prepared["sync_stats"].get("provider")],
            ),
            "provider_strategy": prepared.get("provider_strategy"),
            "as_of_timestamp": prepared.get("as_of_timestamp"),
            "replay_anchor_timestamp": prepared.get("replay_anchor_timestamp"),
            "intraday_period": prepared.get("intraday_period"),
            "session_meta": session_result.get("session_meta", {}),
        },
    )
    outputs = write_capital_sandbox_outputs(
        output_root=prepared["output_root"],
        summary_frame=session_result["summary_frame"],
        journal_frame=session_result["journal_frame"],
        equity_frame=session_result["equity_frame"],
        snapshot_frame=session_result["snapshot_frame"],
        report_markdown=report_markdown,
    )
    if mode == "live_session_real_time":
        best_path = None
        if not session_result["summary_frame"].empty:
            best_path = (
                session_result["summary_frame"]
                .sort_values("final_capital", ascending=False)
                .iloc[0]
                .to_dict()
            )
        write_capital_live_progress(
            output_root=prepared["output_root"],
            status_payload={
                "status": "completed",
                "mode": mode,
                "step": int(session_result["effective_session_minutes"]),
                "total_steps": int(session_result["effective_session_minutes"]),
                "current_timestamp": (
                    session_result["journal_frame"]["timestamp"].iloc[-1]
                    if not session_result["journal_frame"].empty
                    else None
                ),
                "session_started_at": session_result["session_meta"].get("session_started_at"),
                "expected_end_at": session_result["session_meta"].get("expected_end_at"),
                "session_minutes": int(session_minutes),
                "decision_interval_seconds": int(session_result["effective_interval_seconds"]),
                "portfolio_id": prepared["metadata"]["portfolio_id"],
                "providers_used": prepared["sync_stats"].get(
                    "providers_used",
                    [prepared["sync_stats"].get("provider")],
                ),
                "degraded_to_empty_news": bool(prepared["sync_stats"].get("degraded_to_empty_news", False)),
                "sync_error": prepared["sync_stats"].get("sync_error"),
                "best_path": best_path,
                "session_meta": session_result.get("session_meta", {}),
                "final_outputs": {key: str(path) for key, path in outputs.items()},
            },
            journal_frame=session_result["journal_frame"],
            equity_frame=session_result["equity_frame"],
            snapshot_frame=session_result["snapshot_frame"],
        )

    return {
        "metadata": prepared["metadata"],
        "positions": prepared["positions"],
        "sync_stats": prepared["sync_stats"],
        "pipeline_stats": prepared["pipeline_stats"],
        "provider_strategy": prepared.get("provider_strategy"),
        "as_of_timestamp": prepared.get("as_of_timestamp"),
        "replay_anchor_timestamp": prepared.get("replay_anchor_timestamp"),
        "intraday_period": prepared.get("intraday_period"),
        "summary_frame": session_result["summary_frame"],
        "journal_frame": session_result["journal_frame"],
        "equity_frame": session_result["equity_frame"],
        "snapshot_frame": session_result["snapshot_frame"],
        "session_meta": session_result.get("session_meta", {}),
        "report_markdown": report_markdown,
        "output_root": str(prepared["output_root"]),
        "outputs": {key: str(path) for key, path in outputs.items()},
    }


def run_capital_sandbox_compare_workbench(
    *,
    portfolio_config: str | Path,
    mode: str = "replay_intraday",
    initial_capital: float = 100.0,
    decision_interval_seconds: int = 10,
    session_minutes_list: list[int] | tuple[int, ...] = (5, 15, 30),
    start: str = "2024-01-01",
    end: str | None = None,
    news_fixture: str | Path | None = None,
    fixture_provider: str = "marketaux",
    providers: list[str] | tuple[str, ...] = ("marketaux", "thenewsapi", "newsapi", "alphavantage"),
    alias_table: str | Path | None = None,
    event_map_config: str | Path | None = None,
    ticker_sector_map_path: str | Path | None = None,
    fee_rate: float = 0.001,
    slippage_rate: float = 0.0005,
    symbol_batch_size: int = 5,
    limit: int = 3,
    max_pages: int = 1,
    published_after: str | None = None,
    published_before: str | None = None,
    as_of_timestamp: str | None = None,
    intraday_period: str = "5d",
    cache_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    run_id_override: str | None = None,
) -> dict[str, Any]:
    if mode == "live_session_real_time":
        raise ValueError(
            "Session compare is only supported for replay_intraday, replay_as_of_timestamp, or historical_daily."
        )
    prepared = _prepare_capital_sandbox_inputs(
        portfolio_config=portfolio_config,
        mode=mode,
        session_minutes=max(int(value) for value in session_minutes_list) if session_minutes_list else 5,
        start=start,
        end=end,
        news_fixture=news_fixture,
        fixture_provider=fixture_provider,
        providers=providers,
        alias_table=alias_table,
        event_map_config=event_map_config,
        ticker_sector_map_path=ticker_sector_map_path,
        symbol_batch_size=symbol_batch_size,
        limit=limit,
        max_pages=max_pages,
        published_after=published_after,
        published_before=published_before,
        as_of_timestamp=as_of_timestamp,
        intraday_period=intraday_period,
        cache_dir=cache_dir,
        output_dir=output_dir,
        run_id_override=run_id_override,
    )
    sessions = sorted({int(value) for value in session_minutes_list if int(value) > 0})
    if not sessions:
        raise ValueError("session_minutes_list must contain at least one positive value.")

    summary_frames: list[pd.DataFrame] = []
    journal_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    snapshot_frames: list[pd.DataFrame] = []
    for session_minutes in sessions:
        session_result = _run_single_capital_session(
            prepared=prepared,
            initial_capital=initial_capital,
            decision_interval_seconds=decision_interval_seconds,
            session_minutes=session_minutes,
            news_refresh_minutes=None,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
        )
        summary_frames.append(session_result["summary_frame"])
        journal_frames.append(session_result["journal_frame"])
        equity_frames.append(session_result["equity_frame"])
        snapshot_frames.append(session_result["snapshot_frame"])

    summary_frame = pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame()
    journal_frame = pd.concat(journal_frames, ignore_index=True) if journal_frames else pd.DataFrame()
    equity_frame = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    snapshot_frame = pd.concat(snapshot_frames, ignore_index=True) if snapshot_frames else pd.DataFrame()

    report_markdown = build_capital_compare_report(
        summary_frame=summary_frame,
        snapshot_frame=snapshot_frame,
        metadata={
            "portfolio_id": prepared["metadata"]["portfolio_id"],
            "mode": mode,
            "initial_capital": initial_capital,
            "decision_interval_seconds": (
                decision_interval_seconds
                if mode in {"replay_intraday", "replay_as_of_timestamp"}
                else 86400
            ),
            "session_labels": [
                f"{value}m" if mode in {"replay_intraday", "replay_as_of_timestamp"} else "daily"
                for value in sessions
            ],
            "provider_strategy": prepared.get("provider_strategy"),
            "as_of_timestamp": prepared.get("as_of_timestamp"),
            "replay_anchor_timestamp": prepared.get("replay_anchor_timestamp"),
            "intraday_period": prepared.get("intraday_period"),
        },
    )
    outputs = write_capital_compare_outputs(
        output_root=prepared["output_root"],
        summary_frame=summary_frame,
        journal_frame=journal_frame,
        equity_frame=equity_frame,
        snapshot_frame=snapshot_frame,
        report_markdown=report_markdown,
    )

    return {
        "metadata": prepared["metadata"],
        "positions": prepared["positions"],
        "sync_stats": prepared["sync_stats"],
        "pipeline_stats": prepared["pipeline_stats"],
        "provider_strategy": prepared.get("provider_strategy"),
        "as_of_timestamp": prepared.get("as_of_timestamp"),
        "replay_anchor_timestamp": prepared.get("replay_anchor_timestamp"),
        "intraday_period": prepared.get("intraday_period"),
        "summary_frame": summary_frame,
        "journal_frame": journal_frame,
        "equity_frame": equity_frame,
        "snapshot_frame": snapshot_frame,
        "report_markdown": report_markdown,
        "output_root": str(prepared["output_root"]),
        "outputs": {key: str(path) for key, path in outputs.items()},
    }

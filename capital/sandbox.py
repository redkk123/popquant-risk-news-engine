from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Any

import numpy as np
import pandas as pd

from capital.policy import decide_target_exposure
from data.returns import compute_log_returns, weighted_portfolio_returns
from fusion.scenario_mapper import map_event_to_scenario
from models.ewma import ewma_volatility
from risk.regime import classify_risk_regime


def _build_portfolio_price_proxy(prices: pd.DataFrame, weights: pd.Series) -> pd.Series:
    normalized = prices.loc[:, weights.index.tolist()].ffill().dropna(how="any")
    rebased = normalized / normalized.iloc[0]
    proxy = rebased.mul(weights, axis=1).sum(axis=1)
    proxy.name = "portfolio_proxy"
    return proxy


def _expand_prices_to_grid(prices: pd.DataFrame, *, freq_seconds: int, session_minutes: int) -> pd.DataFrame:
    if prices.empty:
        raise ValueError("Price frame is empty.")
    latest = prices.sort_index().ffill().dropna(how="all")
    if latest.empty:
        raise ValueError("Price frame has no usable observations.")
    window_start = latest.index.max() - pd.Timedelta(minutes=session_minutes)
    trimmed = latest.loc[latest.index >= window_start].copy()
    if trimmed.shape[0] < 2:
        trimmed = latest.tail(max(2, session_minutes + 1)).copy()

    grid = pd.date_range(
        start=trimmed.index.min(),
        end=trimmed.index.max(),
        freq=f"{int(freq_seconds)}s",
        tz=trimmed.index.tz,
    )
    expanded = trimmed.reindex(trimmed.index.union(grid)).sort_index().ffill().loc[grid]
    return expanded.dropna(how="all")


def _coerce_event_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _align_timestamp_to_reference(timestamp: pd.Timestamp, reference: pd.Timestamp) -> pd.Timestamp:
    if reference.tzinfo is None:
        return timestamp.tz_localize(None) if timestamp.tzinfo is not None else timestamp
    if timestamp.tzinfo is None:
        return timestamp.tz_localize(reference.tzinfo)
    return timestamp.tz_convert(reference.tzinfo)


def _select_active_events(
    events: list[dict[str, Any]],
    *,
    as_of: pd.Timestamp,
    lookback_hours: int,
    eligible_only: bool = True,
) -> list[dict[str, Any]]:
    lower_bound = as_of - pd.Timedelta(hours=int(lookback_hours))
    active: list[dict[str, Any]] = []
    for event in events:
        if eligible_only and not bool(event.get("watchlist_eligible", False)):
            continue
        published_at = _coerce_event_timestamp(event.get("published_at"))
        if published_at is None:
            continue
        published_at = _align_timestamp_to_reference(published_at, as_of)
        if lower_bound <= published_at <= as_of:
            active.append(event)
    return active


def _current_regime(
    *,
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series | None,
    as_of: pd.Timestamp,
    min_history: int = 20,
) -> dict[str, Any]:
    if isinstance(portfolio_returns.index, pd.DatetimeIndex) and len(portfolio_returns.index) > 0:
        as_of = _align_timestamp_to_reference(as_of, portfolio_returns.index[-1])
    aligned_portfolio = portfolio_returns.loc[portfolio_returns.index <= as_of]
    aligned_benchmark = (
        benchmark_returns.loc[benchmark_returns.index <= as_of] if benchmark_returns is not None else None
    )
    if len(aligned_portfolio) < min_history:
        return {
            "regime": "normal",
            "stress_signals": 0,
            "calm_signals": 0,
            "ewma_volatility": float(aligned_portfolio.std(ddof=1)) if len(aligned_portfolio) > 1 else 0.0,
            "ewma_volatility_percentile": 0.5,
            "realized_volatility": float(aligned_portfolio.std(ddof=1)) if len(aligned_portfolio) > 1 else 0.0,
            "realized_volatility_percentile": 0.5,
            "benchmark_drawdown": None,
            "realized_window": 20,
            "benchmark_window": 60,
        }
    return classify_risk_regime(
        portfolio_returns=aligned_portfolio,
        benchmark_returns=aligned_benchmark,
    )


def _event_signal_for_time(
    *,
    active_events: list[dict[str, Any]],
    weights: pd.Series,
    mapping_config: dict[str, Any],
    ticker_sector_map: dict[str, str],
    as_of: pd.Timestamp,
) -> dict[str, Any]:
    scenarios = []
    top_event = None
    top_score = 0.0
    positive_events = 0
    negative_events = 0
    signal_score = 0.0

    for event in active_events:
        scenario = map_event_to_scenario(
            event,
            portfolio_tickers=weights.index.tolist(),
            mapping_config=mapping_config,
            ticker_sector_map=ticker_sector_map,
            as_of=as_of,
        )
        if scenario is None:
            continue
        scenarios.append(scenario)
        scenario_score = 0.0
        for ticker, shock in scenario["return_shocks"].items():
            scenario_score += float(weights.get(ticker, 0.0)) * float(shock)
        signal_score += scenario_score
        if scenario_score >= 0.0:
            positive_events += 1
        else:
            negative_events += 1
        if abs(scenario_score) >= abs(top_score):
            top_score = scenario_score
            top_event = event

    return {
        "signal_score": float(signal_score),
        "eligible_event_count": int(len(scenarios)),
        "positive_events": int(positive_events),
        "negative_events": int(negative_events),
        "scenario_count": int(len(scenarios)),
        "top_event": top_event or {},
    }


def _quant_confirmation_for_time(
    *,
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series | None,
    regime: dict[str, Any],
) -> dict[str, Any]:
    if portfolio_returns.empty:
        return {
            "quant_confirmation": "neutral",
            "confirmation_score": 0.0,
            "momentum_signal": 0.0,
            "benchmark_momentum": 0.0,
            "positive_hit_rate": 0.0,
            "ewma_ratio": 1.0,
        }

    recent_window = max(1, min(5, len(portfolio_returns)))
    recent_portfolio = portfolio_returns.tail(recent_window)
    recent_portfolio_effective = recent_portfolio.loc[recent_portfolio.abs() > 1e-10]
    if recent_portfolio_effective.empty:
        recent_portfolio_effective = recent_portfolio.tail(1)

    recent_benchmark = benchmark_returns.tail(recent_window) if benchmark_returns is not None else None
    recent_benchmark_effective = None
    if recent_benchmark is not None and not recent_benchmark.empty:
        recent_benchmark_effective = recent_benchmark.loc[recent_benchmark.abs() > 1e-10]
        if recent_benchmark_effective.empty:
            recent_benchmark_effective = recent_benchmark.tail(1)

    momentum_signal = float(recent_portfolio_effective.mean())
    positive_hit_rate = float((recent_portfolio_effective > 0.0).mean())
    benchmark_momentum = (
        float(recent_benchmark_effective.mean())
        if recent_benchmark_effective is not None and not recent_benchmark_effective.empty
        else 0.0
    )

    ewma_series = ewma_volatility(portfolio_returns)
    current_ewma = float(ewma_series.iloc[-1])
    median_ewma = float(ewma_series.dropna().median()) if not ewma_series.dropna().empty else current_ewma
    ewma_ratio = current_ewma / median_ewma if median_ewma > 0.0 else 1.0

    confirmation_score = float(
        momentum_signal
        + 0.35 * benchmark_momentum
        + max(positive_hit_rate - 0.5, 0.0) * 0.002
        - max(ewma_ratio - 1.0, 0.0) * 0.0015
    )

    vol_percentile = float(regime.get("ewma_volatility_percentile", 0.5) or 0.5)
    realized_percentile = float(regime.get("realized_volatility_percentile", 0.5) or 0.5)
    regime_name = str(regime.get("regime", "normal"))

    quant_confirmation = "neutral"
    if regime_name == "stress" and (momentum_signal <= 0.0 or ewma_ratio >= 1.50):
        quant_confirmation = "risk_off"
    elif momentum_signal <= -0.0005 or benchmark_momentum <= -0.0007:
        quant_confirmation = "risk_off"
    elif positive_hit_rate < 0.40 and momentum_signal <= 0.0002:
        quant_confirmation = "risk_off"
    elif vol_percentile >= 0.90 and realized_percentile >= 0.85 and momentum_signal <= 0.0003:
        quant_confirmation = "risk_off"
    elif regime_name != "stress" and (
        (
            momentum_signal >= 0.0005
            and positive_hit_rate >= 0.60
            and benchmark_momentum > -0.0005
            and ewma_ratio <= 1.35
        )
        or (
            confirmation_score >= 0.0012
            and momentum_signal > 0.0
            and positive_hit_rate >= 0.55
            and ewma_ratio <= 2.50
        )
    ):
        quant_confirmation = "confirmed_long"

    return {
        "quant_confirmation": quant_confirmation,
        "confirmation_score": confirmation_score,
        "momentum_signal": momentum_signal,
        "benchmark_momentum": benchmark_momentum,
        "positive_hit_rate": positive_hit_rate,
        "ewma_ratio": ewma_ratio,
    }


def _path_performance_confirmation(
    *,
    equity_rows: list[dict[str, Any]],
    path_states: dict[str, PathState],
    lookback_points: int = 4,
) -> dict[str, Any]:
    if not equity_rows:
        return {
            "path_confirmation": "neutral",
            "path_confirmation_score": 0.0,
            "path_return": 0.0,
            "relative_vs_portfolio_hold": 0.0,
            "relative_vs_benchmark_hold": 0.0,
        }

    history = pd.DataFrame(equity_rows)
    if history.empty or "path_name" not in history.columns or "capital" not in history.columns:
        return {
            "path_confirmation": "neutral",
            "path_confirmation_score": 0.0,
            "path_return": 0.0,
            "relative_vs_portfolio_hold": 0.0,
            "relative_vs_benchmark_hold": 0.0,
        }

    def _series_for(path_name: str) -> list[float]:
        values = (
            history.loc[history["path_name"] == path_name, "capital"]
            .astype(float)
            .tail(max(int(lookback_points) - 1, 1))
            .tolist()
        )
        values.append(float(path_states[path_name].capital))
        return values

    dynamic_values = _series_for("event_quant_pathing")
    portfolio_hold_values = _series_for("portfolio_hold")
    benchmark_hold_values = _series_for("benchmark_hold")
    if len(dynamic_values) < 2:
        return {
            "path_confirmation": "neutral",
            "path_confirmation_score": 0.0,
            "path_return": 0.0,
            "relative_vs_portfolio_hold": 0.0,
            "relative_vs_benchmark_hold": 0.0,
        }

    def _path_return(values: list[float]) -> float:
        start_value = float(values[0])
        end_value = float(values[-1])
        if start_value <= 0.0:
            return 0.0
        return end_value / start_value - 1.0

    dynamic_return = _path_return(dynamic_values)
    portfolio_hold_return = _path_return(portfolio_hold_values)
    benchmark_hold_return = _path_return(benchmark_hold_values)
    relative_vs_portfolio_hold = dynamic_return - portfolio_hold_return
    relative_vs_benchmark_hold = dynamic_return - benchmark_hold_return
    path_confirmation_score = float(
        0.7 * relative_vs_portfolio_hold + 0.3 * relative_vs_benchmark_hold
    )

    path_confirmation = "neutral"
    if relative_vs_portfolio_hold <= -0.0020 and relative_vs_benchmark_hold <= -0.0015:
        path_confirmation = "underperforming"
    elif (
        dynamic_return >= -0.0005
        and relative_vs_portfolio_hold >= -0.0005
        and relative_vs_benchmark_hold >= -0.0005
    ):
        path_confirmation = "confirmed"

    return {
        "path_confirmation": path_confirmation,
        "path_confirmation_score": path_confirmation_score,
        "path_return": dynamic_return,
        "relative_vs_portfolio_hold": relative_vs_portfolio_hold,
        "relative_vs_benchmark_hold": relative_vs_benchmark_hold,
    }


def _looks_like_quota_error(message: Any) -> bool:
    text = str(message or "").lower()
    return any(
        keyword in text
        for keyword in ("payment required", "quota", "402", "daily limit", "limit reached")
    )


def _max_drawdown_from_capital(capital_series: pd.Series) -> float:
    if capital_series.empty:
        return 0.0
    running_peak = capital_series.cummax()
    drawdown = capital_series / running_peak - 1.0
    return float(drawdown.min())


def build_snapshot_frame(equity_frame: pd.DataFrame, *, frequency: str = "1min") -> pd.DataFrame:
    if equity_frame.empty:
        return pd.DataFrame(columns=["snapshot_time", "path_name", "capital"])
    working = equity_frame.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], utc=True, errors="coerce")
    if "capture_timestamp" in working.columns:
        working["capture_timestamp"] = pd.to_datetime(working["capture_timestamp"], utc=True, errors="coerce")
    working = working.dropna(subset=["timestamp"])
    if working.empty:
        return pd.DataFrame(columns=["snapshot_time", "path_name", "capital"])

    if "session_step" in working.columns:
        rows: list[dict[str, Any]] = []
        for path_name, path_frame in working.groupby("path_name"):
            ordered = path_frame.sort_values(["session_step", "timestamp"]).copy()
            latest_per_step = ordered.groupby("session_step", as_index=False).last()
            rows.extend(
                {
                    "snapshot_time": row["timestamp"],
                    "tracking_time": row["capture_timestamp"] if "capture_timestamp" in latest_per_step.columns else row["timestamp"],
                    "session_step": int(row["session_step"]),
                    "path_name": path_name,
                    "capital": float(row["capital"]),
                }
                for _, row in latest_per_step.iterrows()
            )
        return pd.DataFrame(rows).sort_values(["session_step", "path_name"]).reset_index(drop=True)

    rows: list[dict[str, Any]] = []
    for path_name, path_frame in working.groupby("path_name"):
        indexed = path_frame.sort_values("timestamp").set_index("timestamp")
        snap = indexed["capital"].resample(frequency).last().dropna()
        rows.extend(
            {
                "snapshot_time": timestamp,
                "tracking_time": timestamp,
                "path_name": path_name,
                "capital": float(value),
            }
            for timestamp, value in snap.items()
        )
    return pd.DataFrame(rows).sort_values(["snapshot_time", "path_name"]).reset_index(drop=True)


@dataclass
class PathState:
    name: str
    capital: float
    exposure: float
    trade_count: int = 0
    total_costs: float = 0.0
    basket_weights: pd.Series | None = None


def _build_summary_frame(
    *,
    path_states: dict[str, PathState],
    equity_frame: pd.DataFrame,
    journal_frame: pd.DataFrame,
    initial_capital: float,
    session_meta: dict[str, Any] | None = None,
) -> pd.DataFrame:
    session_meta = session_meta or {}
    quant_blocked_count = 0
    confirmed_risk_steps = 0
    path_blocked_count = 0
    path_confirmed_steps = 0
    if not journal_frame.empty and {"eligible_event_count", "risk_on_allowed"}.issubset(journal_frame.columns):
        quant_blocked_count = int(
            ((journal_frame["eligible_event_count"].fillna(0) > 0) & (~journal_frame["risk_on_allowed"].fillna(False))).sum()
        )
        confirmed_risk_steps = int(
            ((journal_frame["risk_on_allowed"].fillna(False)) & (journal_frame["target_exposure"].fillna(0) > 0)).sum()
        )
    if not journal_frame.empty and {"eligible_event_count", "path_confirmation"}.issubset(journal_frame.columns):
        path_blocked_count = int(
            (
                (journal_frame["eligible_event_count"].fillna(0) > 0)
                & (journal_frame["path_confirmation"].fillna("neutral") == "underperforming")
            ).sum()
        )
        path_confirmed_steps = int(
            (journal_frame["path_confirmation"].fillna("neutral") == "confirmed").sum()
        )

    summary_rows = []
    for name, state in path_states.items():
        capital_series = equity_frame.loc[equity_frame["path_name"] == name, "capital"]
        row = {
            "path_name": name,
            "final_capital": float(state.capital),
            "total_return": float(state.capital / initial_capital - 1.0),
            "max_drawdown": _max_drawdown_from_capital(capital_series),
            "trade_count": int(state.trade_count),
            "total_costs": float(state.total_costs),
            "avg_capital": float(capital_series.mean()) if not capital_series.empty else float(initial_capital),
        }
        if name == "event_quant_pathing":
            row.update(
                {
                    "news_refresh_attempts": int(session_meta.get("news_refresh_attempts", 0) or 0),
                    "news_refresh_successes": int(session_meta.get("news_refresh_successes", 0) or 0),
                    "news_refresh_errors": int(session_meta.get("news_refresh_errors", 0) or 0),
                    "news_refresh_skipped": int(session_meta.get("news_refresh_skipped", 0) or 0),
                    "news_refresh_skipped_quota_cooldown": int(
                        session_meta.get("news_refresh_skipped_quota_cooldown", 0) or 0
                    ),
                    "stale_price_steps": int(session_meta.get("stale_price_steps", 0) or 0),
                    "quant_blocked_count": int(quant_blocked_count),
                    "confirmed_risk_steps": int(confirmed_risk_steps),
                    "path_blocked_count": int(path_blocked_count),
                    "path_confirmed_steps": int(path_confirmed_steps),
                }
            )
        else:
            row.update(
                {
                    "news_refresh_attempts": 0,
                    "news_refresh_successes": 0,
                    "news_refresh_errors": 0,
                    "news_refresh_skipped": 0,
                    "news_refresh_skipped_quota_cooldown": 0,
                    "stale_price_steps": 0,
                    "quant_blocked_count": 0,
                    "confirmed_risk_steps": 0,
                    "path_blocked_count": 0,
                    "path_confirmed_steps": 0,
                }
            )
        summary_rows.append(row)
    return pd.DataFrame(summary_rows).sort_values("final_capital", ascending=False).reset_index(drop=True)


def _coerce_event_tickers(event: dict[str, Any]) -> list[str]:
    raw = event.get("tickers") or event.get("direct_tickers") or []
    if isinstance(raw, str):
        values = [part.strip().upper() for part in raw.split(",")]
    else:
        values = [str(part).strip().upper() for part in raw]
    return [value for value in values if value]


def _coerce_event_sectors(event: dict[str, Any]) -> list[str]:
    raw = event.get("event_sectors") or []
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",")]
    else:
        values = [str(part).strip() for part in raw]
    return [value for value in values if value and value.lower() != "nan"]


def _select_sector_basket_weights(
    *,
    top_event: dict[str, Any],
    weights: pd.Series,
    ticker_sector_map: dict[str, str],
) -> pd.Series | None:
    direct_tickers = [ticker for ticker in _coerce_event_tickers(top_event) if ticker in weights.index]
    if direct_tickers:
        basket = weights.reindex(direct_tickers).dropna()
        basket = basket / basket.sum()
        return basket

    event_sectors = set(_coerce_event_sectors(top_event))
    if not event_sectors:
        for ticker in _coerce_event_tickers(top_event):
            sector = ticker_sector_map.get(ticker)
            if sector:
                event_sectors.add(sector)

    if not event_sectors:
        return None

    candidate_tickers = [ticker for ticker in weights.index if ticker_sector_map.get(ticker) in event_sectors]
    if not candidate_tickers:
        return None
    basket = weights.reindex(candidate_tickers).dropna()
    basket = basket / basket.sum()
    return basket


def _long_only_target_from_regime(regime_name: str) -> float:
    if regime_name == "stress":
        return 0.25
    if regime_name == "calm":
        return 0.85
    return 0.60


def _benchmark_timing_target(
    *,
    quant_confirmation: str,
    benchmark_momentum: float,
    regime_name: str,
) -> float:
    if quant_confirmation == "risk_off":
        return 0.0
    if benchmark_momentum >= 0.0003:
        return 1.0 if regime_name != "stress" else 0.35
    if benchmark_momentum >= -0.0001:
        return 0.35 if regime_name == "calm" else 0.20
    return 0.0


def _capped_risk_long_target(
    *,
    quant_confirmation: str,
    regime_name: str,
    eligible_event_count: int,
) -> float:
    if quant_confirmation == "risk_off":
        return 0.0
    base_target = _long_only_target_from_regime(regime_name)
    if quant_confirmation == "confirmed_long":
        return base_target
    if int(eligible_event_count) > 0 and regime_name != "stress":
        return min(base_target, 0.35)
    return 0.15 if regime_name == "calm" else 0.0


def _sector_basket_target(
    *,
    quant_confirmation: str,
    regime_name: str,
    eligible_event_count: int,
    basket_weights: pd.Series | None,
) -> float:
    if basket_weights is None or basket_weights.empty or int(eligible_event_count) <= 0:
        return 0.0
    if quant_confirmation == "risk_off":
        return 0.0
    if quant_confirmation == "confirmed_long":
        return 1.0 if regime_name != "stress" else 0.35
    return 0.25 if regime_name == "calm" else 0.0


def _basket_turnover(current_weights: pd.Series | None, target_weights: pd.Series | None) -> float:
    current = current_weights if current_weights is not None else pd.Series(dtype=float)
    target = target_weights if target_weights is not None else pd.Series(dtype=float)
    union = current.index.union(target.index)
    if len(union) == 0:
        return 0.0
    current_aligned = current.reindex(union).fillna(0.0)
    target_aligned = target.reindex(union).fillna(0.0)
    return float(0.5 * (current_aligned - target_aligned).abs().sum())


def run_capital_sandbox(
    *,
    price_frame: pd.DataFrame,
    benchmark_prices: pd.Series | None,
    weights: pd.Series,
    events: list[dict[str, Any]],
    mapping_config: dict[str, Any],
    ticker_sector_map: dict[str, str],
    initial_capital: float = 100.0,
    decision_interval_seconds: int = 10,
    session_minutes: int = 5,
    event_lookback_hours: int = 48,
    fee_rate: float = 0.001,
    slippage_rate: float = 0.0005,
) -> dict[str, Any]:
    expanded_prices = _expand_prices_to_grid(
        price_frame,
        freq_seconds=decision_interval_seconds,
        session_minutes=session_minutes,
    )
    portfolio_proxy = _build_portfolio_price_proxy(expanded_prices, weights)
    portfolio_returns = compute_log_returns(expanded_prices.loc[:, weights.index.tolist()])
    portfolio_grid_returns = np.log(portfolio_proxy / portfolio_proxy.shift(1)).dropna()

    benchmark_proxy = None
    benchmark_grid_returns = None
    base_benchmark_returns = None
    if benchmark_prices is not None and not benchmark_prices.empty:
        benchmark_grid = _expand_prices_to_grid(
            benchmark_prices.to_frame(name=benchmark_prices.name or "benchmark"),
            freq_seconds=decision_interval_seconds,
            session_minutes=session_minutes,
        )
        benchmark_proxy = benchmark_grid.iloc[:, 0]
        benchmark_grid_returns = np.log(benchmark_proxy / benchmark_proxy.shift(1)).dropna()
        base_benchmark_returns = np.log(benchmark_prices / benchmark_prices.shift(1)).dropna()

    base_portfolio_returns = weighted_portfolio_returns(portfolio_returns, weights)
    session_meta = {
        "news_refresh_attempts": 0,
        "news_refresh_successes": 0,
        "news_refresh_errors": 0,
        "news_refresh_skipped": 0,
        "news_refresh_skipped_quota_cooldown": 0,
        "stale_price_steps": 0,
        "event_lookback_hours": int(event_lookback_hours),
        "mode": "replay_intraday",
    }

    path_states = {
        "cash_only": PathState(name="cash_only", capital=float(initial_capital), exposure=0.0),
        "benchmark_hold": PathState(name="benchmark_hold", capital=float(initial_capital), exposure=1.0),
        "portfolio_hold": PathState(name="portfolio_hold", capital=float(initial_capital), exposure=1.0),
        "event_quant_pathing": PathState(name="event_quant_pathing", capital=float(initial_capital), exposure=0.0),
        "benchmark_timing": PathState(name="benchmark_timing", capital=float(initial_capital), exposure=0.0),
        "capped_risk_long": PathState(name="capped_risk_long", capital=float(initial_capital), exposure=0.0),
        "sector_basket": PathState(name="sector_basket", capital=float(initial_capital), exposure=0.0),
    }
    equity_rows: list[dict[str, Any]] = []
    journal_rows: list[dict[str, Any]] = []

    if portfolio_grid_returns.empty:
        raise RuntimeError("Not enough intraday observations to run the capital sandbox.")

    initial_timestamp = portfolio_proxy.index[0]
    initial_active_events = _select_active_events(events, as_of=initial_timestamp, lookback_hours=event_lookback_hours)
    initial_regime = _current_regime(
        portfolio_returns=base_portfolio_returns,
        benchmark_returns=base_benchmark_returns,
        as_of=initial_timestamp,
    )
    initial_signal = _event_signal_for_time(
        active_events=initial_active_events,
        weights=weights,
        mapping_config=mapping_config,
        ticker_sector_map=ticker_sector_map,
        as_of=initial_timestamp,
    )
    initial_quant = _quant_confirmation_for_time(
        portfolio_returns=base_portfolio_returns.loc[base_portfolio_returns.index <= initial_timestamp],
        benchmark_returns=(
            base_benchmark_returns.loc[base_benchmark_returns.index <= initial_timestamp]
            if base_benchmark_returns is not None
            else None
        ),
        regime=initial_regime,
    )
    initial_decision = decide_target_exposure(
        signal_score=initial_signal["signal_score"],
        regime=initial_regime["regime"],
        positive_events=initial_signal["positive_events"],
        negative_events=initial_signal["negative_events"],
        eligible_event_count=initial_signal["eligible_event_count"],
        current_exposure=0.0,
        quant_confirmation=initial_quant["quant_confirmation"],
        confirmation_score=initial_quant["confirmation_score"],
        path_confirmation="neutral",
        path_confirmation_score=0.0,
    )
    initial_state = path_states["event_quant_pathing"]
    initial_state.exposure = float(initial_decision["target_exposure"])
    initial_cost = initial_state.capital * initial_state.exposure * (fee_rate + slippage_rate)
    initial_state.capital -= initial_cost
    initial_state.total_costs += initial_cost
    if initial_state.exposure > 0.0:
        initial_state.trade_count += 1

    benchmark_timing_state = path_states["benchmark_timing"]
    benchmark_timing_state.exposure = _benchmark_timing_target(
        quant_confirmation=initial_quant["quant_confirmation"],
        benchmark_momentum=initial_quant["benchmark_momentum"],
        regime_name=initial_regime["regime"],
    )
    benchmark_timing_cost = benchmark_timing_state.capital * benchmark_timing_state.exposure * (fee_rate + slippage_rate)
    benchmark_timing_state.capital -= benchmark_timing_cost
    benchmark_timing_state.total_costs += benchmark_timing_cost
    if benchmark_timing_state.exposure > 0.0:
        benchmark_timing_state.trade_count += 1

    capped_state = path_states["capped_risk_long"]
    capped_state.exposure = _capped_risk_long_target(
        quant_confirmation=initial_quant["quant_confirmation"],
        regime_name=initial_regime["regime"],
        eligible_event_count=initial_signal["eligible_event_count"],
    )
    capped_cost = capped_state.capital * capped_state.exposure * (fee_rate + slippage_rate)
    capped_state.capital -= capped_cost
    capped_state.total_costs += capped_cost
    if capped_state.exposure > 0.0:
        capped_state.trade_count += 1

    sector_state = path_states["sector_basket"]
    initial_sector_weights = _select_sector_basket_weights(
        top_event=initial_signal["top_event"] or {},
        weights=weights,
        ticker_sector_map=ticker_sector_map,
    )
    sector_state.basket_weights = initial_sector_weights
    sector_state.exposure = _sector_basket_target(
        quant_confirmation=initial_quant["quant_confirmation"],
        regime_name=initial_regime["regime"],
        eligible_event_count=initial_signal["eligible_event_count"],
        basket_weights=initial_sector_weights,
    )
    sector_cost = sector_state.capital * sector_state.exposure * (fee_rate + slippage_rate)
    sector_state.capital -= sector_cost
    sector_state.total_costs += sector_cost
    if sector_state.exposure > 0.0:
        sector_state.trade_count += 1

    previous_price_row = expanded_prices.loc[initial_timestamp]

    for timestamp, portfolio_return in portfolio_grid_returns.items():
        benchmark_return = 0.0
        if benchmark_grid_returns is not None and timestamp in benchmark_grid_returns.index:
            benchmark_return = float(benchmark_grid_returns.loc[timestamp])

        benchmark_timing_state = path_states["benchmark_timing"]
        capped_state = path_states["capped_risk_long"]
        sector_state = path_states["sector_basket"]
        sector_return = 0.0
        if sector_state.basket_weights is not None and not sector_state.basket_weights.empty:
            sector_return = _portfolio_simple_return(previous_price_row, expanded_prices.loc[timestamp], sector_state.basket_weights)

        path_states["benchmark_hold"].capital *= (1.0 + benchmark_return)
        path_states["portfolio_hold"].capital *= (1.0 + float(portfolio_return))
        benchmark_timing_state.capital *= (1.0 + benchmark_timing_state.exposure * benchmark_return)
        capped_state.capital *= (1.0 + capped_state.exposure * float(portfolio_return))
        sector_state.capital *= (1.0 + sector_state.exposure * float(sector_return))
        dynamic_state = path_states["event_quant_pathing"]
        dynamic_state.capital *= (1.0 + dynamic_state.exposure * float(portfolio_return))

        active_events = _select_active_events(events, as_of=timestamp, lookback_hours=event_lookback_hours)
        regime = _current_regime(
            portfolio_returns=base_portfolio_returns,
            benchmark_returns=base_benchmark_returns,
            as_of=timestamp,
        )
        quant = _quant_confirmation_for_time(
            portfolio_returns=base_portfolio_returns.loc[base_portfolio_returns.index <= timestamp],
            benchmark_returns=(
                base_benchmark_returns.loc[base_benchmark_returns.index <= timestamp]
                if base_benchmark_returns is not None
                else None
            ),
            regime=regime,
        )
        signal = _event_signal_for_time(
            active_events=active_events,
            weights=weights,
            mapping_config=mapping_config,
            ticker_sector_map=ticker_sector_map,
            as_of=timestamp,
        )
        path_confirmation = _path_performance_confirmation(
            equity_rows=equity_rows,
            path_states=path_states,
        )
        decision = decide_target_exposure(
            signal_score=signal["signal_score"],
            regime=regime["regime"],
            positive_events=signal["positive_events"],
            negative_events=signal["negative_events"],
            eligible_event_count=signal["eligible_event_count"],
            current_exposure=dynamic_state.exposure,
            quant_confirmation=quant["quant_confirmation"],
            confirmation_score=quant["confirmation_score"],
            path_confirmation=path_confirmation["path_confirmation"],
            path_confirmation_score=path_confirmation["path_confirmation_score"],
        )
        benchmark_timing_target = _benchmark_timing_target(
            quant_confirmation=quant["quant_confirmation"],
            benchmark_momentum=quant["benchmark_momentum"],
            regime_name=regime["regime"],
        )
        capped_target = _capped_risk_long_target(
            quant_confirmation=quant["quant_confirmation"],
            regime_name=regime["regime"],
            eligible_event_count=signal["eligible_event_count"],
        )
        sector_target_weights = _select_sector_basket_weights(
            top_event=signal["top_event"] or {},
            weights=weights,
            ticker_sector_map=ticker_sector_map,
        )
        sector_target = _sector_basket_target(
            quant_confirmation=quant["quant_confirmation"],
            regime_name=regime["regime"],
            eligible_event_count=signal["eligible_event_count"],
            basket_weights=sector_target_weights,
        )

        target_exposure = float(decision["target_exposure"])
        turnover = abs(target_exposure - dynamic_state.exposure)
        costs = dynamic_state.capital * turnover * (fee_rate + slippage_rate)
        if turnover > 0.0:
            dynamic_state.capital -= costs
            dynamic_state.total_costs += costs
            dynamic_state.trade_count += 1
            dynamic_state.exposure = target_exposure

        benchmark_timing_turnover = abs(float(benchmark_timing_target) - benchmark_timing_state.exposure)
        benchmark_timing_costs = benchmark_timing_state.capital * benchmark_timing_turnover * (fee_rate + slippage_rate)
        if benchmark_timing_turnover > 0.0:
            benchmark_timing_state.capital -= benchmark_timing_costs
            benchmark_timing_state.total_costs += benchmark_timing_costs
            benchmark_timing_state.trade_count += 1
            benchmark_timing_state.exposure = float(benchmark_timing_target)

        capped_turnover = abs(float(capped_target) - capped_state.exposure)
        capped_costs = capped_state.capital * capped_turnover * (fee_rate + slippage_rate)
        if capped_turnover > 0.0:
            capped_state.capital -= capped_costs
            capped_state.total_costs += capped_costs
            capped_state.trade_count += 1
            capped_state.exposure = float(capped_target)

        sector_turnover = abs(float(sector_target) - sector_state.exposure)
        basket_turnover = _basket_turnover(sector_state.basket_weights, sector_target_weights)
        total_sector_turnover = sector_turnover + min(sector_state.exposure, float(sector_target)) * basket_turnover
        sector_costs = sector_state.capital * total_sector_turnover * (fee_rate + slippage_rate)
        if total_sector_turnover > 0.0:
            sector_state.capital -= sector_costs
            sector_state.total_costs += sector_costs
            sector_state.trade_count += 1
        sector_state.exposure = float(sector_target)
        sector_state.basket_weights = sector_target_weights

        session_step = int(len(journal_rows) + 1)
        capture_timestamp = pd.Timestamp.now(tz="UTC")
        equity_rows.extend(
            [
                {
                    "timestamp": timestamp,
                    "capture_timestamp": capture_timestamp,
                    "session_step": session_step,
                    "path_name": name,
                    "capital": state.capital,
                }
                for name, state in path_states.items()
            ]
        )
        top_event = signal["top_event"] or {}
        journal_rows.append(
            {
                "timestamp": timestamp,
                "capture_timestamp": capture_timestamp,
                "session_step": session_step,
                "path_name": "event_quant_pathing",
                "portfolio_return": float(portfolio_return),
                "benchmark_return": float(benchmark_return),
                "capital_after_costs": float(dynamic_state.capital),
                "target_exposure": target_exposure,
                "turnover": float(turnover),
                "costs": float(costs),
                "action": decision["action"],
                "decision_reason": decision["reason"],
                "regime": regime["regime"],
                "signal_score": float(signal["signal_score"]),
                "quant_confirmation": decision["quant_confirmation"],
                "confirmation_score": float(decision["confirmation_score"]),
                "path_confirmation": decision["path_confirmation"],
                "path_confirmation_score": float(decision["path_confirmation_score"]),
                "path_return": float(path_confirmation["path_return"]),
                "relative_vs_portfolio_hold": float(path_confirmation["relative_vs_portfolio_hold"]),
                "relative_vs_benchmark_hold": float(path_confirmation["relative_vs_benchmark_hold"]),
                "momentum_signal": float(quant["momentum_signal"]),
                "benchmark_momentum": float(quant["benchmark_momentum"]),
                "positive_hit_rate": float(quant["positive_hit_rate"]),
                "ewma_ratio": float(quant["ewma_ratio"]),
                "risk_on_allowed": bool(decision["risk_on_allowed"]),
                "benchmark_timing_target_exposure": float(benchmark_timing_target),
                "capped_risk_long_target_exposure": float(capped_target),
                "sector_basket_target_exposure": float(sector_target),
                "sector_basket_tickers": ",".join(sector_target_weights.index.tolist()) if sector_target_weights is not None else "",
                "eligible_event_count": int(signal["eligible_event_count"]),
                "positive_events": int(signal["positive_events"]),
                "negative_events": int(signal["negative_events"]),
                "scenario_count": int(signal["scenario_count"]),
                "refresh_attempted_this_step": False,
                "refresh_status": "not_requested",
                "news_refresh_attempts": 0,
                "news_refresh_successes": 0,
                "news_refresh_errors": 0,
                "price_timestamp_advanced": True,
                "top_event_id": top_event.get("event_id"),
                "top_event_type": top_event.get("event_type"),
                "top_event_headline": top_event.get("headline"),
            }
        )
        previous_price_row = expanded_prices.loc[timestamp]

    equity_frame = pd.DataFrame(equity_rows)
    journal_frame = pd.DataFrame(journal_rows)
    summary_frame = _build_summary_frame(
        path_states=path_states,
        equity_frame=equity_frame,
        journal_frame=journal_frame,
        initial_capital=initial_capital,
        session_meta=session_meta,
    )

    return {
        "summary_frame": summary_frame,
        "journal_frame": journal_frame,
        "equity_frame": equity_frame,
        "session_meta": session_meta,
    }


def _coerce_price_timestamp(timestamp: pd.Timestamp) -> pd.Timestamp:
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _latest_valid_row(prices: pd.DataFrame) -> tuple[pd.Timestamp, pd.Series]:
    latest = prices.sort_index().ffill().dropna(how="all")
    if latest.empty:
        raise RuntimeError("No usable prices returned by the intraday fetcher.")
    timestamp = _coerce_price_timestamp(pd.Timestamp(latest.index[-1]))
    row = latest.iloc[-1].astype(float)
    return timestamp, row


def _portfolio_simple_return(previous_row: pd.Series, current_row: pd.Series, weights: pd.Series) -> float:
    aligned_previous = previous_row.reindex(weights.index).astype(float)
    aligned_current = current_row.reindex(weights.index).astype(float)
    relative = aligned_current / aligned_previous
    proxy = float((relative * weights).sum())
    return proxy - 1.0


def _benchmark_simple_return(
    previous_row: pd.Series | None,
    current_row: pd.Series | None,
    benchmark_name: str | None,
) -> float:
    if previous_row is None or current_row is None or not benchmark_name:
        return 0.0
    if benchmark_name not in previous_row.index or benchmark_name not in current_row.index:
        return 0.0
    prev_value = float(previous_row[benchmark_name])
    curr_value = float(current_row[benchmark_name])
    if prev_value == 0.0:
        return 0.0
    return curr_value / prev_value - 1.0


def run_capital_sandbox_live_session(
    *,
    price_fetcher,
    weights: pd.Series,
    benchmark_name: str | None,
    events: list[dict[str, Any]],
    mapping_config: dict[str, Any],
    ticker_sector_map: dict[str, str],
    initial_capital: float = 100.0,
    poll_interval_seconds: int = 60,
    session_minutes: int = 5,
    event_lookback_hours: int = 48,
    event_refresh_interval_steps: int | None = None,
    event_refresh_callback=None,
    fee_rate: float = 0.001,
    slippage_rate: float = 0.0005,
    sleep_fn=time.sleep,
    progress_callback=None,
) -> dict[str, Any]:
    effective_interval_seconds = max(60, int(poll_interval_seconds))
    session_steps = max(1, int(math.ceil((int(session_minutes) * 60) / effective_interval_seconds)))
    session_started_at = pd.Timestamp.now(tz="UTC")
    expected_end_at = session_started_at + pd.Timedelta(seconds=session_steps * effective_interval_seconds)

    initial_prices = price_fetcher()
    initial_prices = initial_prices.sort_index().ffill().dropna(how="all")
    if initial_prices.empty:
        raise RuntimeError("Live session could not start because no intraday prices were returned.")

    observed_prices = initial_prices.copy()
    previous_timestamp, previous_row = _latest_valid_row(initial_prices)

    path_states = {
        "cash_only": PathState(name="cash_only", capital=float(initial_capital), exposure=0.0),
        "benchmark_hold": PathState(name="benchmark_hold", capital=float(initial_capital), exposure=1.0),
        "portfolio_hold": PathState(name="portfolio_hold", capital=float(initial_capital), exposure=1.0),
        "event_quant_pathing": PathState(name="event_quant_pathing", capital=float(initial_capital), exposure=0.0),
        "benchmark_timing": PathState(name="benchmark_timing", capital=float(initial_capital), exposure=0.0),
        "capped_risk_long": PathState(name="capped_risk_long", capital=float(initial_capital), exposure=0.0),
        "sector_basket": PathState(name="sector_basket", capital=float(initial_capital), exposure=0.0),
    }
    equity_rows: list[dict[str, Any]] = []
    journal_rows: list[dict[str, Any]] = []
    session_meta = {
        "news_refresh_attempts": 0,
        "news_refresh_successes": 0,
        "news_refresh_errors": 0,
        "news_refresh_skipped": 0,
        "news_refresh_skipped_quota_cooldown": 0,
        "stale_price_steps": 0,
        "event_lookback_hours": int(event_lookback_hours),
        "event_refresh_interval_steps": int(event_refresh_interval_steps or 0),
        "last_refresh_status": "not_requested",
        "last_refresh_provider": None,
        "last_refresh_events": int(len(events)),
        "last_refresh_inserted": 0,
        "last_refresh_articles_seen": 0,
        "last_refresh_error": None,
        "last_refresh_step": None,
        "quota_cooldown_until_step": 0,
        "mode": "live_session_real_time",
    }

    asset_initial = initial_prices.loc[:, weights.index.tolist()]
    asset_initial_proxy = _build_portfolio_price_proxy(asset_initial, weights)
    initial_portfolio_returns = np.log(asset_initial_proxy / asset_initial_proxy.shift(1)).dropna()
    benchmark_initial_returns = None
    if benchmark_name and benchmark_name in initial_prices.columns:
        benchmark_initial_returns = np.log(
            initial_prices[benchmark_name] / initial_prices[benchmark_name].shift(1)
        ).dropna()

    initial_regime = _current_regime(
        portfolio_returns=initial_portfolio_returns,
        benchmark_returns=benchmark_initial_returns,
        as_of=previous_timestamp,
    )
    initial_active_events = _select_active_events(events, as_of=previous_timestamp, lookback_hours=event_lookback_hours)
    initial_signal = _event_signal_for_time(
        active_events=initial_active_events,
        weights=weights,
        mapping_config=mapping_config,
        ticker_sector_map=ticker_sector_map,
        as_of=previous_timestamp,
    )
    initial_quant = _quant_confirmation_for_time(
        portfolio_returns=initial_portfolio_returns,
        benchmark_returns=benchmark_initial_returns,
        regime=initial_regime,
    )
    initial_decision = decide_target_exposure(
        signal_score=initial_signal["signal_score"],
        regime=initial_regime["regime"],
        positive_events=initial_signal["positive_events"],
        negative_events=initial_signal["negative_events"],
        eligible_event_count=initial_signal["eligible_event_count"],
        current_exposure=0.0,
        quant_confirmation=initial_quant["quant_confirmation"],
        confirmation_score=initial_quant["confirmation_score"],
        path_confirmation="neutral",
        path_confirmation_score=0.0,
    )
    dynamic_state = path_states["event_quant_pathing"]
    dynamic_state.exposure = float(initial_decision["target_exposure"])
    initial_cost = dynamic_state.capital * dynamic_state.exposure * (fee_rate + slippage_rate)
    dynamic_state.capital -= initial_cost
    dynamic_state.total_costs += initial_cost
    if dynamic_state.exposure > 0.0:
        dynamic_state.trade_count += 1

    benchmark_timing_state = path_states["benchmark_timing"]
    benchmark_timing_state.exposure = _benchmark_timing_target(
        quant_confirmation=initial_quant["quant_confirmation"],
        benchmark_momentum=initial_quant["benchmark_momentum"],
        regime_name=initial_regime["regime"],
    )
    benchmark_timing_cost = benchmark_timing_state.capital * benchmark_timing_state.exposure * (fee_rate + slippage_rate)
    benchmark_timing_state.capital -= benchmark_timing_cost
    benchmark_timing_state.total_costs += benchmark_timing_cost
    if benchmark_timing_state.exposure > 0.0:
        benchmark_timing_state.trade_count += 1

    capped_state = path_states["capped_risk_long"]
    capped_state.exposure = _capped_risk_long_target(
        quant_confirmation=initial_quant["quant_confirmation"],
        regime_name=initial_regime["regime"],
        eligible_event_count=initial_signal["eligible_event_count"],
    )
    capped_cost = capped_state.capital * capped_state.exposure * (fee_rate + slippage_rate)
    capped_state.capital -= capped_cost
    capped_state.total_costs += capped_cost
    if capped_state.exposure > 0.0:
        capped_state.trade_count += 1

    sector_state = path_states["sector_basket"]
    initial_sector_weights = _select_sector_basket_weights(
        top_event=initial_signal["top_event"] or {},
        weights=weights,
        ticker_sector_map=ticker_sector_map,
    )
    sector_state.basket_weights = initial_sector_weights
    sector_state.exposure = _sector_basket_target(
        quant_confirmation=initial_quant["quant_confirmation"],
        regime_name=initial_regime["regime"],
        eligible_event_count=initial_signal["eligible_event_count"],
        basket_weights=initial_sector_weights,
    )
    sector_cost = sector_state.capital * sector_state.exposure * (fee_rate + slippage_rate)
    sector_state.capital -= sector_cost
    sector_state.total_costs += sector_cost
    if sector_state.exposure > 0.0:
        sector_state.trade_count += 1

    if progress_callback is not None:
        progress_callback(
            {
                "step": 0,
                "total_steps": session_steps,
                "status": "running",
                "journal_frame": pd.DataFrame(journal_rows),
                "equity_frame": pd.DataFrame(equity_rows),
                "summary_frame": pd.DataFrame(),
                "current_timestamp": previous_timestamp,
                "session_started_at": session_started_at,
                "expected_end_at": expected_end_at,
                "session_meta": dict(session_meta),
            }
        )

    for step in range(1, session_steps + 1):
        sleep_fn(effective_interval_seconds)

        current_prices = price_fetcher()
        current_prices = current_prices.sort_index().ffill().dropna(how="all")
        observed_prices = pd.concat([observed_prices, current_prices]).sort_index()
        observed_prices = observed_prices[~observed_prices.index.duplicated(keep="last")]

        current_timestamp, current_row = _latest_valid_row(current_prices)
        price_timestamp_advanced = bool(current_timestamp > previous_timestamp)
        if not price_timestamp_advanced:
            session_meta["stale_price_steps"] += 1
        portfolio_return = _portfolio_simple_return(previous_row, current_row, weights)
        benchmark_return = _benchmark_simple_return(previous_row, current_row, benchmark_name)
        sector_return = 0.0
        sector_state = path_states["sector_basket"]
        if sector_state.basket_weights is not None and not sector_state.basket_weights.empty:
            sector_return = _portfolio_simple_return(previous_row, current_row, sector_state.basket_weights)

        path_states["benchmark_hold"].capital *= (1.0 + benchmark_return)
        path_states["portfolio_hold"].capital *= (1.0 + portfolio_return)
        path_states["benchmark_timing"].capital *= (1.0 + path_states["benchmark_timing"].exposure * benchmark_return)
        path_states["capped_risk_long"].capital *= (1.0 + path_states["capped_risk_long"].exposure * portfolio_return)
        path_states["sector_basket"].capital *= (1.0 + path_states["sector_basket"].exposure * sector_return)
        dynamic_state.capital *= (1.0 + dynamic_state.exposure * portfolio_return)

        observed_asset_prices = observed_prices.loc[:, weights.index.tolist()]
        observed_asset_proxy = _build_portfolio_price_proxy(observed_asset_prices, weights)
        observed_portfolio_returns = np.log(observed_asset_proxy / observed_asset_proxy.shift(1)).dropna()
        observed_benchmark_returns = None
        if benchmark_name and benchmark_name in observed_prices.columns:
            observed_benchmark_returns = np.log(
                observed_prices[benchmark_name] / observed_prices[benchmark_name].shift(1)
            ).dropna()

        regime = _current_regime(
            portfolio_returns=observed_portfolio_returns,
            benchmark_returns=observed_benchmark_returns,
            as_of=current_timestamp,
        )
        active_events = _select_active_events(events, as_of=current_timestamp, lookback_hours=event_lookback_hours)
        refresh_attempted_this_step = False
        refresh_status = "not_requested"
        refresh_error = None
        refresh_provider = None
        refresh_inserted = 0
        refresh_articles_seen = 0

        if (
            event_refresh_callback is not None
            and event_refresh_interval_steps is not None
            and int(event_refresh_interval_steps) > 0
            and step % int(event_refresh_interval_steps) == 0
        ):
            quota_cooldown_until_step = int(session_meta.get("quota_cooldown_until_step", 0) or 0)
            if step <= quota_cooldown_until_step:
                refresh_status = "quota_cooldown_skip"
                session_meta["news_refresh_skipped"] += 1
                session_meta["news_refresh_skipped_quota_cooldown"] += 1
            else:
                refresh_attempted_this_step = True
                session_meta["news_refresh_attempts"] += 1
                try:
                    refresh_result = event_refresh_callback(
                        as_of=current_timestamp,
                        step=step,
                        current_events=list(events),
                    ) or {}
                    refresh_status = str(refresh_result.get("status", "success"))
                    if refresh_result.get("events") is not None:
                        events = list(refresh_result["events"])
                    sync_stats = refresh_result.get("sync_stats", {}) or {}
                    refresh_provider = sync_stats.get("provider")
                    refresh_inserted = int(sync_stats.get("inserted", 0) or 0)
                    refresh_articles_seen = int(sync_stats.get("articles_seen", 0) or 0)
                    if refresh_status == "error":
                        session_meta["news_refresh_errors"] += 1
                        refresh_error = str(refresh_result.get("error", "event refresh failed"))
                        if _looks_like_quota_error(refresh_error):
                            session_meta["quota_cooldown_until_step"] = int(step) + max(
                                1, int(event_refresh_interval_steps or 1)
                            )
                    else:
                        session_meta["news_refresh_successes"] += 1
                    session_meta["last_refresh_status"] = refresh_status
                    session_meta["last_refresh_provider"] = refresh_provider
                    session_meta["last_refresh_events"] = int(len(events))
                    session_meta["last_refresh_inserted"] = refresh_inserted
                    session_meta["last_refresh_articles_seen"] = refresh_articles_seen
                    session_meta["last_refresh_error"] = refresh_error
                    session_meta["last_refresh_step"] = int(step)
                except Exception as exc:  # pragma: no cover
                    refresh_status = "error"
                    refresh_error = str(exc)
                    session_meta["news_refresh_errors"] += 1
                    if _looks_like_quota_error(refresh_error):
                        session_meta["quota_cooldown_until_step"] = int(step) + max(
                            1, int(event_refresh_interval_steps or 1)
                        )
                    session_meta["last_refresh_status"] = refresh_status
                    session_meta["last_refresh_provider"] = refresh_provider
                    session_meta["last_refresh_events"] = int(len(events))
                    session_meta["last_refresh_inserted"] = 0
                    session_meta["last_refresh_articles_seen"] = 0
                    session_meta["last_refresh_error"] = refresh_error
                    session_meta["last_refresh_step"] = int(step)

            active_events = _select_active_events(events, as_of=current_timestamp, lookback_hours=event_lookback_hours)

        quant = _quant_confirmation_for_time(
            portfolio_returns=observed_portfolio_returns,
            benchmark_returns=observed_benchmark_returns,
            regime=regime,
        )
        signal = _event_signal_for_time(
            active_events=active_events,
            weights=weights,
            mapping_config=mapping_config,
            ticker_sector_map=ticker_sector_map,
            as_of=current_timestamp,
        )
        path_confirmation = _path_performance_confirmation(
            equity_rows=equity_rows,
            path_states=path_states,
        )
        decision = decide_target_exposure(
            signal_score=signal["signal_score"],
            regime=regime["regime"],
            positive_events=signal["positive_events"],
            negative_events=signal["negative_events"],
            eligible_event_count=signal["eligible_event_count"],
            current_exposure=dynamic_state.exposure,
            quant_confirmation=quant["quant_confirmation"],
            confirmation_score=quant["confirmation_score"],
            path_confirmation=path_confirmation["path_confirmation"],
            path_confirmation_score=path_confirmation["path_confirmation_score"],
        )
        benchmark_timing_target = _benchmark_timing_target(
            quant_confirmation=quant["quant_confirmation"],
            benchmark_momentum=quant["benchmark_momentum"],
            regime_name=regime["regime"],
        )
        capped_target = _capped_risk_long_target(
            quant_confirmation=quant["quant_confirmation"],
            regime_name=regime["regime"],
            eligible_event_count=signal["eligible_event_count"],
        )
        sector_target_weights = _select_sector_basket_weights(
            top_event=signal["top_event"] or {},
            weights=weights,
            ticker_sector_map=ticker_sector_map,
        )
        sector_target = _sector_basket_target(
            quant_confirmation=quant["quant_confirmation"],
            regime_name=regime["regime"],
            eligible_event_count=signal["eligible_event_count"],
            basket_weights=sector_target_weights,
        )

        target_exposure = float(decision["target_exposure"])
        turnover = abs(target_exposure - dynamic_state.exposure)
        costs = dynamic_state.capital * turnover * (fee_rate + slippage_rate)
        if turnover > 0.0:
            dynamic_state.capital -= costs
            dynamic_state.total_costs += costs
            dynamic_state.trade_count += 1
            dynamic_state.exposure = target_exposure

        benchmark_timing_state = path_states["benchmark_timing"]
        benchmark_timing_turnover = abs(float(benchmark_timing_target) - benchmark_timing_state.exposure)
        benchmark_timing_costs = benchmark_timing_state.capital * benchmark_timing_turnover * (fee_rate + slippage_rate)
        if benchmark_timing_turnover > 0.0:
            benchmark_timing_state.capital -= benchmark_timing_costs
            benchmark_timing_state.total_costs += benchmark_timing_costs
            benchmark_timing_state.trade_count += 1
            benchmark_timing_state.exposure = float(benchmark_timing_target)

        capped_state = path_states["capped_risk_long"]
        capped_turnover = abs(float(capped_target) - capped_state.exposure)
        capped_costs = capped_state.capital * capped_turnover * (fee_rate + slippage_rate)
        if capped_turnover > 0.0:
            capped_state.capital -= capped_costs
            capped_state.total_costs += capped_costs
            capped_state.trade_count += 1
            capped_state.exposure = float(capped_target)

        sector_state = path_states["sector_basket"]
        sector_turnover = abs(float(sector_target) - sector_state.exposure)
        basket_turnover = _basket_turnover(sector_state.basket_weights, sector_target_weights)
        total_sector_turnover = sector_turnover + min(sector_state.exposure, float(sector_target)) * basket_turnover
        sector_costs = sector_state.capital * total_sector_turnover * (fee_rate + slippage_rate)
        if total_sector_turnover > 0.0:
            sector_state.capital -= sector_costs
            sector_state.total_costs += sector_costs
            sector_state.trade_count += 1
        sector_state.exposure = float(sector_target)
        sector_state.basket_weights = sector_target_weights

        capture_timestamp = pd.Timestamp.now(tz="UTC")
        equity_rows.extend(
            [
                {
                    "timestamp": current_timestamp,
                    "capture_timestamp": capture_timestamp,
                    "session_step": int(step),
                    "path_name": name,
                    "capital": state.capital,
                }
                for name, state in path_states.items()
            ]
        )
        top_event = signal["top_event"] or {}
        journal_rows.append(
            {
                "timestamp": current_timestamp,
                "capture_timestamp": capture_timestamp,
                "session_step": int(step),
                "path_name": "event_quant_pathing",
                "portfolio_return": float(portfolio_return),
                "benchmark_return": float(benchmark_return),
                "capital_after_costs": float(dynamic_state.capital),
                "target_exposure": target_exposure,
                "turnover": float(turnover),
                "costs": float(costs),
                "action": decision["action"],
                "decision_reason": decision["reason"],
                "regime": regime["regime"],
                "signal_score": float(signal["signal_score"]),
                "quant_confirmation": decision["quant_confirmation"],
                "confirmation_score": float(decision["confirmation_score"]),
                "path_confirmation": decision["path_confirmation"],
                "path_confirmation_score": float(decision["path_confirmation_score"]),
                "path_return": float(path_confirmation["path_return"]),
                "relative_vs_portfolio_hold": float(path_confirmation["relative_vs_portfolio_hold"]),
                "relative_vs_benchmark_hold": float(path_confirmation["relative_vs_benchmark_hold"]),
                "momentum_signal": float(quant["momentum_signal"]),
                "benchmark_momentum": float(quant["benchmark_momentum"]),
                "positive_hit_rate": float(quant["positive_hit_rate"]),
                "ewma_ratio": float(quant["ewma_ratio"]),
                "risk_on_allowed": bool(decision["risk_on_allowed"]),
                "benchmark_timing_target_exposure": float(benchmark_timing_target),
                "capped_risk_long_target_exposure": float(capped_target),
                "sector_basket_target_exposure": float(sector_target),
                "sector_basket_tickers": ",".join(sector_target_weights.index.tolist()) if sector_target_weights is not None else "",
                "eligible_event_count": int(signal["eligible_event_count"]),
                "positive_events": int(signal["positive_events"]),
                "negative_events": int(signal["negative_events"]),
                "scenario_count": int(signal["scenario_count"]),
                "refresh_attempted_this_step": bool(refresh_attempted_this_step),
                "refresh_status": refresh_status,
                "refresh_provider": refresh_provider,
                "refresh_inserted": int(refresh_inserted),
                "refresh_articles_seen": int(refresh_articles_seen),
                "refresh_error": refresh_error,
                "news_refresh_attempts": int(session_meta["news_refresh_attempts"]),
                "news_refresh_successes": int(session_meta["news_refresh_successes"]),
                "news_refresh_errors": int(session_meta["news_refresh_errors"]),
                "news_refresh_skipped": int(session_meta["news_refresh_skipped"]),
                "price_timestamp_advanced": bool(price_timestamp_advanced),
                "top_event_id": top_event.get("event_id"),
                "top_event_type": top_event.get("event_type"),
                "top_event_headline": top_event.get("headline"),
            }
        )
        if progress_callback is not None:
            partial_equity = pd.DataFrame(equity_rows)
            partial_journal = pd.DataFrame(journal_rows)
            partial_summary = _build_summary_frame(
                path_states=path_states,
                equity_frame=partial_equity,
                journal_frame=partial_journal,
                initial_capital=initial_capital,
                session_meta=session_meta,
            )
            progress_callback(
                {
                    "step": step,
                    "total_steps": session_steps,
                    "status": "running" if step < session_steps else "completing",
                    "journal_frame": partial_journal,
                    "equity_frame": partial_equity,
                    "summary_frame": partial_summary,
                    "current_timestamp": current_timestamp,
                    "session_started_at": session_started_at,
                    "expected_end_at": expected_end_at,
                    "session_meta": dict(session_meta),
                }
            )
        previous_timestamp = current_timestamp
        previous_row = current_row

    equity_frame = pd.DataFrame(equity_rows)
    journal_frame = pd.DataFrame(journal_rows)
    summary_frame = _build_summary_frame(
        path_states=path_states,
        equity_frame=equity_frame,
        journal_frame=journal_frame,
        initial_capital=initial_capital,
        session_meta=session_meta,
    )

    return {
        "summary_frame": summary_frame,
        "journal_frame": journal_frame,
        "equity_frame": equity_frame,
        "session_meta": session_meta,
    }

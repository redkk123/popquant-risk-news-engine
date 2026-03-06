from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from data.returns import compute_log_returns, weighted_portfolio_returns
from fusion.calibration import resolve_event_trade_date
from fusion.scenario_mapper import map_event_to_scenario
from risk.stress import evaluate_stress_scenario


def _normalize_horizons(horizons: Iterable[int]) -> tuple[int, ...]:
    normalized = tuple(sorted({int(horizon) for horizon in horizons if int(horizon) >= 1}))
    if not normalized:
        raise ValueError("At least one horizon is required.")
    return normalized


def _empty_summary() -> dict[str, Any]:
    return {
        "n_events": 0,
        "n_event_rows": 0,
        "n_event_days": 0,
        "baseline_violation_rate": None,
        "stressed_violation_rate": None,
        "baseline_mae": None,
        "stressed_mae": None,
        "improved_days": 0,
        "worse_days": 0,
        "unchanged_days": 0,
        "avg_var_uplift": None,
        "per_horizon": {},
    }


def _event_day_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    keys = ["event_date"]
    if "portfolio_id" in frame.columns:
        keys = ["portfolio_id", "event_date"]
    return int(frame.loc[:, keys].drop_duplicates().shape[0])


def _summarize_valid_rows(valid: pd.DataFrame, *, horizon: int) -> dict[str, Any]:
    base_error_col = f"baseline_abs_error_{horizon}d"
    stressed_error_col = f"stressed_abs_error_{horizon}d"
    base_violation_col = f"baseline_violation_{horizon}d"
    stressed_violation_col = f"stressed_violation_{horizon}d"
    var_uplift_col = f"var_uplift_{horizon}d"

    baseline_errors = valid[base_error_col]
    stressed_errors = valid[stressed_error_col]
    improved = stressed_errors < baseline_errors
    worse = stressed_errors > baseline_errors
    unchanged = ~(improved | worse)
    return {
        "n_events": int(valid["event_id"].nunique()) if "event_id" in valid.columns else int(len(valid)),
        "n_event_rows": int(len(valid)),
        "n_event_days": _event_day_count(valid),
        "baseline_violation_rate": float(valid[base_violation_col].mean()),
        "stressed_violation_rate": float(valid[stressed_violation_col].mean()),
        "baseline_mae": float(baseline_errors.mean()),
        "stressed_mae": float(stressed_errors.mean()),
        "improved_days": int(improved.sum()),
        "worse_days": int(worse.sum()),
        "unchanged_days": int(unchanged.sum()),
        "avg_var_uplift": float(valid[var_uplift_col].mean()),
    }


def run_event_conditioned_backtest(
    *,
    prices: pd.DataFrame,
    weights: pd.Series,
    events: list[dict[str, Any]],
    mapping_config: dict[str, Any],
    ticker_sector_map: dict[str, str] | None = None,
    alpha: float = 0.01,
    lam: float = 0.94,
    window: int = 252,
    portfolio_id: str = "unknown",
    benchmark_name: str | None = None,
    horizons: Iterable[int] = (1,),
    mapping_variant: str | None = None,
) -> pd.DataFrame:
    """Backtest baseline normal VaR against event-conditioned stressed VaR per event."""
    del lam, benchmark_name
    horizons = _normalize_horizons(horizons)

    asset_prices = prices.loc[:, weights.index.tolist()].sort_index()
    asset_returns = compute_log_returns(asset_prices)
    portfolio_returns = weighted_portfolio_returns(asset_returns, weights)

    grouped_events: dict[pd.Timestamp, list[dict[str, Any]]] = {}
    for event in events:
        anchor_date = resolve_event_trade_date(asset_prices.index, event.get("published_at"))
        if anchor_date is None:
            continue
        grouped_events.setdefault(anchor_date, []).append(event)

    rows: list[dict[str, Any]] = []
    for anchor_date, daily_events in sorted(grouped_events.items()):
        history_prices = asset_prices.loc[:anchor_date]
        if history_prices.shape[0] < window + 1:
            continue
        price_window = history_prices.tail(window + 1)
        window_returns = compute_log_returns(price_window)

        future_losses = portfolio_returns.loc[portfolio_returns.index > anchor_date]
        if future_losses.empty:
            continue

        as_of = pd.Timestamp(anchor_date).tz_localize("UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        scenario_pairs: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for event in daily_events:
            scenario = map_event_to_scenario(
                event,
                portfolio_tickers=weights.index.tolist(),
                mapping_config=mapping_config,
                ticker_sector_map=ticker_sector_map,
                as_of=as_of,
            )
            if scenario is None:
                continue
            stress_metrics, _ = evaluate_stress_scenario(
                asset_returns=window_returns,
                weights=weights,
                scenario=scenario,
                alpha=alpha,
                horizons=horizons,
            )
            scenario_pairs.append((event, scenario, stress_metrics))

        if not scenario_pairs:
            continue

        next_date = future_losses.index[0]
        for event, scenario, stress_metrics in scenario_pairs:
            row = {
                "portfolio_id": portfolio_id,
                "mapping_variant": mapping_variant or "configured",
                "event_id": event.get("event_id"),
                "event_date": anchor_date.date().isoformat(),
                "next_trading_date": next_date.date().isoformat(),
                "event_type": event.get("event_type"),
                "event_subtype": event.get("event_subtype") or "unknown",
                "story_bucket": event.get("story_bucket") or "unknown",
                "headline": event.get("headline"),
                "published_at": event.get("published_at"),
                "source": event.get("source"),
                "source_tier": event.get("source_tier") or "unknown",
                "source_bucket": event.get("source_bucket") or "unknown",
                "quality_label": event.get("quality_label"),
                "quality_score": event.get("quality_score"),
                "event_confidence": event.get("event_confidence"),
                "link_confidence": event.get("link_confidence"),
                "scenario_name": scenario["name"],
                "direct_tickers": scenario.get("direct_tickers"),
                "affected_tickers": scenario.get("affected_tickers"),
                "event_sectors": scenario.get("event_sectors"),
                "sector_peer_tickers": scenario.get("sector_peer_tickers"),
                "polarity": scenario["polarity"],
                "severity": scenario["severity"],
                "severity_scale": scenario.get("severity_scale"),
                "recency_decay": scenario.get("recency_decay"),
                "shock_scale": scenario.get("shock_scale"),
                "source_scale": scenario.get("source_scale"),
                "spillover_confidence_scale": scenario.get("spillover_confidence_scale"),
                "event_age_days": scenario.get("event_age_days"),
            }

            alpha_pct = int(round((1.0 - alpha) * 100))
            for horizon in horizons:
                suffix = f"{horizon}d_{alpha_pct}"
                baseline_var = float(stress_metrics[f"base_normal_var_loss_{suffix}"])
                baseline_es = float(stress_metrics[f"base_normal_es_loss_{suffix}"])
                stressed_var = float(stress_metrics[f"stressed_normal_var_loss_{suffix}"])
                stressed_es = float(stress_metrics[f"stressed_normal_es_loss_{suffix}"])

                realized_return = None
                realized_loss = None
                if len(future_losses) >= horizon:
                    realized_return = float(future_losses.head(horizon).sum())
                    realized_loss = float(max(-realized_return, 0.0))

                row[f"baseline_normal_var_loss_{suffix}"] = baseline_var
                row[f"stressed_normal_var_loss_{suffix}"] = stressed_var
                row[f"baseline_normal_es_loss_{suffix}"] = baseline_es
                row[f"stressed_normal_es_loss_{suffix}"] = stressed_es
                row[f"delta_normal_var_loss_{suffix}"] = float(stress_metrics[f"delta_normal_var_loss_{suffix}"])
                row[f"delta_normal_es_loss_{suffix}"] = float(stress_metrics[f"delta_normal_es_loss_{suffix}"])
                row[f"realized_return_{horizon}d"] = realized_return
                row[f"realized_loss_{horizon}d"] = realized_loss
                row[f"baseline_violation_{horizon}d"] = (
                    bool(realized_loss > baseline_var) if realized_loss is not None else None
                )
                row[f"stressed_violation_{horizon}d"] = (
                    bool(realized_loss > stressed_var) if realized_loss is not None else None
                )
                row[f"baseline_abs_error_{horizon}d"] = (
                    float(abs(baseline_var - realized_loss)) if realized_loss is not None else None
                )
                row[f"stressed_abs_error_{horizon}d"] = (
                    float(abs(stressed_var - realized_loss)) if realized_loss is not None else None
                )
                row[f"var_uplift_{horizon}d"] = float(stressed_var - baseline_var)

            if 1 in horizons:
                row["baseline_violation"] = row["baseline_violation_1d"]
                row["stressed_violation"] = row["stressed_violation_1d"]
                row["baseline_abs_error"] = row["baseline_abs_error_1d"]
                row["stressed_abs_error"] = row["stressed_abs_error_1d"]
                row["var_uplift"] = row["var_uplift_1d"]
            rows.append(row)

    return pd.DataFrame(rows)


def summarize_event_conditioned_backtest(
    backtest_frame: pd.DataFrame,
    *,
    horizons: Iterable[int] | None = None,
) -> dict[str, Any]:
    """Summarize how the event-conditioned layer behaved on event rows."""
    if backtest_frame.empty:
        return _empty_summary()

    if horizons is None:
        horizons = sorted(
            {
                int(column.replace("baseline_abs_error_", "").replace("d", ""))
                for column in backtest_frame.columns
                if column.startswith("baseline_abs_error_") and column.endswith("d")
            }
        )
        if not horizons and "baseline_abs_error" in backtest_frame.columns:
            horizons = [1]
    else:
        horizons = sorted({int(horizon) for horizon in horizons if int(horizon) >= 1})

    per_horizon: dict[str, Any] = {}
    for horizon in horizons:
        base_error_col = f"baseline_abs_error_{horizon}d"
        if base_error_col not in backtest_frame.columns:
            continue
        valid = backtest_frame.loc[backtest_frame[base_error_col].notna()].copy()
        if valid.empty:
            continue
        per_horizon[f"{horizon}d"] = _summarize_valid_rows(valid, horizon=horizon)

    if not per_horizon:
        return _empty_summary()

    primary = per_horizon.get("1d") or next(iter(per_horizon.values()))
    return {
        **primary,
        "portfolio_count": int(backtest_frame["portfolio_id"].nunique()) if "portfolio_id" in backtest_frame.columns else 1,
        "mapping_variants": sorted(backtest_frame["mapping_variant"].dropna().astype(str).unique().tolist()) if "mapping_variant" in backtest_frame.columns else [],
        "per_horizon": per_horizon,
    }


def summarize_event_conditioned_backtest_groups(
    backtest_frame: pd.DataFrame,
    *,
    group_by: Iterable[str],
    horizons: Iterable[int] | None = None,
    min_events: int = 1,
) -> pd.DataFrame:
    """Summarize backtest performance by arbitrary event dimensions."""
    if backtest_frame.empty:
        return pd.DataFrame()

    group_columns = [str(column).strip() for column in group_by if str(column).strip()]
    if not group_columns:
        raise ValueError("group_by must contain at least one column.")

    working = backtest_frame.copy()
    for column in group_columns:
        if column not in working.columns:
            working[column] = "unknown"
        working[column] = working[column].fillna("unknown").astype(str)
        if column == "event_subtype":
            working[column] = working[column].replace({"": "unknown"})

    if horizons is None:
        horizons = sorted(
            {
                int(column.replace("baseline_abs_error_", "").replace("d", ""))
                for column in working.columns
                if column.startswith("baseline_abs_error_") and column.endswith("d")
            }
        )
    horizons = [int(horizon) for horizon in horizons if int(horizon) >= 1]

    rows: list[dict[str, Any]] = []
    grouped = working.groupby(group_columns, dropna=False)
    for group_key, group in grouped:
        group_values = group_key if isinstance(group_key, tuple) else (group_key,)
        base_row = dict(zip(group_columns, group_values))
        base_row["n_events_total"] = int(group["event_id"].nunique()) if "event_id" in group.columns else int(len(group))
        if base_row["n_events_total"] < int(min_events):
            continue

        for horizon in horizons:
            base_error_col = f"baseline_abs_error_{horizon}d"
            if base_error_col not in group.columns:
                continue
            valid = group.loc[group[base_error_col].notna()].copy()
            if valid.empty:
                continue
            metrics = _summarize_valid_rows(valid, horizon=horizon)
            rows.append(
                {
                    **base_row,
                    "horizon_days": int(horizon),
                    "portfolio_count": int(valid["portfolio_id"].nunique()) if "portfolio_id" in valid.columns else 1,
                    **metrics,
                }
            )

    if not rows:
        return pd.DataFrame()

    sort_columns = group_columns + ["horizon_days"]
    return pd.DataFrame(rows).sort_values(sort_columns).reset_index(drop=True)

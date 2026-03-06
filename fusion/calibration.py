from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable

import numpy as np
import pandas as pd

from data.returns import compute_log_returns
from data.validation import validate_price_frame
from fusion.scenario_mapper import _resolve_mapping_sections


def event_direction(polarity: float) -> str:
    return "positive" if float(polarity) >= 0.0 else "negative"


def resolve_event_trade_date(
    price_index: pd.DatetimeIndex,
    published_at: Any,
    *,
    market_close_hour_utc: int = 20,
) -> pd.Timestamp | None:
    """Map an event timestamp to the next relevant trading day in the price index."""
    if published_at in (None, ""):
        return None

    timestamp = pd.Timestamp(published_at)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")

    trading_day = timestamp.normalize()
    if timestamp.hour >= market_close_hour_utc:
        trading_day = trading_day + pd.Timedelta(days=1)

    target = trading_day.tz_localize(None)
    location = price_index.searchsorted(target)
    if location >= len(price_index):
        return None
    return pd.Timestamp(price_index[location])


def build_event_impact_observations(
    *,
    prices: pd.DataFrame,
    events: list[dict[str, Any]],
    benchmark_ticker: str | None = None,
    ticker_sector_map: dict[str, str] | None = None,
    horizons: Iterable[int] = (1, 3, 5),
    vol_window: int = 10,
) -> pd.DataFrame:
    """Build realized forward-return observations for processed news events."""
    clean_prices = validate_price_frame(prices.copy()).sort_index()
    ticker_sector_map = ticker_sector_map or {}
    horizons = sorted({int(horizon) for horizon in horizons if int(horizon) >= 1})
    if not horizons:
        raise ValueError("At least one valid horizon is required.")

    def _build_row(
        *,
        event: dict[str, Any],
        ticker: str,
        event_ticker: str,
        anchor_date: pd.Timestamp,
        impact_scope: str,
        event_sector: str | None,
    ) -> dict[str, Any] | None:
        ticker_prices = clean_prices[[ticker]].dropna()
        if ticker_prices.empty or anchor_date not in ticker_prices.index:
            return None

        anchor_location = ticker_prices.index.get_loc(anchor_date)
        if isinstance(anchor_location, slice):
            anchor_location = anchor_location.start

        returns = compute_log_returns(ticker_prices)[ticker]
        pre_window = returns.loc[:anchor_date].tail(vol_window)
        post_window = returns.loc[returns.index > anchor_date].head(vol_window)
        pre_vol = float(pre_window.std(ddof=1)) if len(pre_window) >= min(vol_window, 5) else np.nan
        post_vol = float(post_window.std(ddof=1)) if len(post_window) >= min(vol_window, 5) else np.nan
        vol_ratio = np.nan
        if np.isfinite(pre_vol) and np.isfinite(post_vol) and pre_vol > 0.0:
            vol_ratio = float(post_vol / pre_vol)

        row = {
            "event_id": event.get("event_id"),
            "event_type": event.get("event_type"),
            "direction": event_direction(float(event.get("polarity", 0.0))),
            "ticker": ticker,
            "event_ticker": event_ticker,
            "event_sector": event_sector,
            "impact_scope": impact_scope,
            "published_at": event.get("published_at"),
            "anchor_date": anchor_date.date().isoformat(),
            "severity": float(event.get("severity", 0.0)),
            f"forward_vol_ratio_{vol_window}d": vol_ratio,
        }

        anchor_price = float(ticker_prices.loc[anchor_date, ticker])
        if isinstance(anchor_price, pd.Series):
            anchor_price = float(anchor_price.iloc[0])
        for horizon in horizons:
            column = f"forward_return_{horizon}d"
            forward_location = anchor_location + horizon
            if forward_location >= len(ticker_prices.index):
                row[column] = np.nan
                continue
            forward_price = float(ticker_prices.iloc[forward_location, 0])
            row[column] = float(forward_price / anchor_price - 1.0)
        return row

    rows: list[dict[str, Any]] = []
    for event in events:
        raw_tickers = [str(ticker).upper() for ticker in (event.get("tickers") or [])]
        if raw_tickers:
            tickers = [ticker for ticker in raw_tickers if ticker in clean_prices.columns]
        elif str(event.get("event_type", "")) == "macro" and benchmark_ticker in clean_prices.columns:
            tickers = [str(benchmark_ticker)]
        else:
            tickers = []
        if not tickers:
            continue

        for event_ticker in tickers:
            ticker_prices = clean_prices[[event_ticker]].dropna()
            if ticker_prices.empty:
                continue

            anchor_date = resolve_event_trade_date(ticker_prices.index, event.get("published_at"))
            if anchor_date is None or anchor_date not in ticker_prices.index:
                continue

            event_sector = ticker_sector_map.get(event_ticker)
            direct_row = _build_row(
                event=event,
                ticker=event_ticker,
                event_ticker=event_ticker,
                anchor_date=anchor_date,
                impact_scope="direct",
                event_sector=event_sector,
            )
            if direct_row is not None:
                rows.append(direct_row)

            if not event_sector:
                continue

            peer_tickers = sorted(
                ticker
                for ticker, sector in ticker_sector_map.items()
                if sector == event_sector and ticker != event_ticker and ticker in clean_prices.columns
            )
            for peer_ticker in peer_tickers:
                peer_row = _build_row(
                    event=event,
                    ticker=peer_ticker,
                    event_ticker=event_ticker,
                    anchor_date=anchor_date,
                    impact_scope="peer_sector",
                    event_sector=event_sector,
                )
                if peer_row is not None:
                    rows.append(peer_row)

    return pd.DataFrame(rows)


def summarize_event_impacts(
    observations: pd.DataFrame,
    *,
    horizons: Iterable[int] = (1, 3, 5),
    vol_window: int = 10,
    impact_scope: str = "direct",
) -> pd.DataFrame:
    """Aggregate event impact observations by event type and direction."""
    if observations.empty:
        return pd.DataFrame()

    frame = observations.copy()
    if "impact_scope" in frame.columns:
        frame = frame.loc[frame["impact_scope"] == impact_scope].copy()
    if frame.empty:
        return pd.DataFrame()

    horizons = sorted({int(horizon) for horizon in horizons if int(horizon) >= 1})
    vol_column = f"forward_vol_ratio_{vol_window}d"
    rows: list[dict[str, Any]] = []

    grouped = frame.groupby(["event_type", "direction"], dropna=False)
    for (event_type, direction), group in grouped:
        row: dict[str, Any] = {
            "event_type": event_type,
            "direction": direction,
            "event_count": int(group["event_id"].nunique()),
            "observation_count": int(len(group)),
            "mean_severity": float(group["severity"].mean()),
        }

        for horizon in horizons:
            values = group[f"forward_return_{horizon}d"].dropna()
            row[f"median_forward_return_{horizon}d"] = float(values.median()) if not values.empty else np.nan
            row[f"mean_forward_return_{horizon}d"] = float(values.mean()) if not values.empty else np.nan
            row[f"p25_forward_return_{horizon}d"] = float(values.quantile(0.25)) if not values.empty else np.nan
            row[f"p75_forward_return_{horizon}d"] = float(values.quantile(0.75)) if not values.empty else np.nan

        vol_values = group[vol_column].dropna()
        row[f"median_forward_vol_ratio_{vol_window}d"] = (
            float(vol_values.median()) if not vol_values.empty else np.nan
        )
        row[f"mean_forward_vol_ratio_{vol_window}d"] = (
            float(vol_values.mean()) if not vol_values.empty else np.nan
        )
        rows.append(row)

    return pd.DataFrame(rows).sort_values(["event_type", "direction"]).reset_index(drop=True)


def summarize_sector_peer_impacts(
    observations: pd.DataFrame,
    *,
    horizons: Iterable[int] = (1, 3, 5),
    vol_window: int = 10,
) -> pd.DataFrame:
    """Aggregate sector spillover observations by event type, direction, and sector."""
    if observations.empty or "impact_scope" not in observations.columns:
        return pd.DataFrame()

    frame = observations.loc[observations["impact_scope"] == "peer_sector"].copy()
    if frame.empty or "event_sector" not in frame.columns:
        return pd.DataFrame()

    horizons = sorted({int(horizon) for horizon in horizons if int(horizon) >= 1})
    vol_column = f"forward_vol_ratio_{vol_window}d"
    rows: list[dict[str, Any]] = []
    grouped = frame.groupby(["event_type", "direction", "event_sector"], dropna=False)
    for (event_type, direction, event_sector), group in grouped:
        row: dict[str, Any] = {
            "event_type": event_type,
            "direction": direction,
            "event_sector": event_sector,
            "event_count": int(group["event_id"].nunique()),
            "observation_count": int(len(group)),
            "impacted_ticker_count": int(group["ticker"].nunique()),
        }

        for horizon in horizons:
            values = group[f"forward_return_{horizon}d"].dropna()
            row[f"median_peer_forward_return_{horizon}d"] = float(values.median()) if not values.empty else np.nan
            row[f"mean_peer_forward_return_{horizon}d"] = float(values.mean()) if not values.empty else np.nan
            row[f"median_abs_peer_forward_return_{horizon}d"] = (
                float(values.abs().median()) if not values.empty else np.nan
            )

        vol_values = group[vol_column].dropna()
        row[f"median_forward_vol_ratio_{vol_window}d"] = (
            float(vol_values.median()) if not vol_values.empty else np.nan
        )
        row[f"mean_forward_vol_ratio_{vol_window}d"] = (
            float(vol_values.mean()) if not vol_values.empty else np.nan
        )
        rows.append(row)

    return pd.DataFrame(rows).sort_values(["event_type", "direction", "event_sector"]).reset_index(drop=True)


def build_calibrated_event_mapping(
    *,
    summary: pd.DataFrame,
    base_mapping_config: dict[str, Any],
    sector_summary: pd.DataFrame | None = None,
    min_observations: int = 2,
    sector_min_observations: int | None = None,
    return_horizon: int = 1,
    vol_window: int = 10,
    shrinkage_target_observations: int = 5,
    default_peer_return_multiplier: float = 0.35,
    default_peer_vol_multiplier: float = 1.05,
) -> dict[str, Any]:
    """Create a calibrated mapping payload by updating the hand-authored base config."""
    if "event_mappings" in base_mapping_config:
        payload = deepcopy(base_mapping_config)
    else:
        payload = {"settings": {}, "event_mappings": deepcopy(base_mapping_config)}

    _, mappings = _resolve_mapping_sections(payload)
    return_column = f"median_forward_return_{return_horizon}d"
    vol_column = f"median_forward_vol_ratio_{vol_window}d"

    updates = 0
    sector_updates = 0
    for record in summary.to_dict(orient="records"):
        observation_count = int(record.get("observation_count", 0))
        if observation_count < min_observations:
            continue

        event_type = str(record["event_type"])
        direction = str(record["direction"])
        if event_type not in mappings:
            continue
        direction_cfg = mappings[event_type].get(direction)
        if direction_cfg is None:
            continue
        base_return = float(direction_cfg.get("return_shock", 0.0))
        base_vol = float(direction_cfg.get("vol_multiplier", 1.0))
        blend = min(1.0, observation_count / max(int(shrinkage_target_observations), 1))

        calibrated_return = record.get(return_column)
        if pd.notna(calibrated_return):
            signed_return = abs(float(calibrated_return))
            empirical_return = -signed_return if direction == "negative" else signed_return
            blended_return = (blend * empirical_return) + ((1.0 - blend) * base_return)
            direction_cfg["return_shock"] = round(float(blended_return), 4)
            updates += 1

        calibrated_vol = record.get(vol_column)
        if pd.notna(calibrated_vol):
            blended_vol = (blend * float(calibrated_vol)) + ((1.0 - blend) * base_vol)
            if direction == "negative":
                blended_vol = float(np.clip(blended_vol, 1.0, 1.75))
            else:
                blended_vol = float(np.clip(blended_vol, 0.85, 1.75))
            direction_cfg["vol_multiplier"] = round(blended_vol, 4)

    sector_min_observations = (
        int(min_observations) if sector_min_observations is None else int(sector_min_observations)
    )
    if sector_summary is not None and not sector_summary.empty:
        for record in sector_summary.to_dict(orient="records"):
            observation_count = int(record.get("observation_count", 0))
            if observation_count < sector_min_observations:
                continue

            event_type = str(record["event_type"])
            direction = str(record["direction"])
            event_sector = str(record.get("event_sector", "")).strip().lower()
            if not event_sector or event_type not in mappings:
                continue

            event_cfg = mappings[event_type]
            direction_cfg = event_cfg.get(direction)
            if direction_cfg is None:
                continue

            blend = min(1.0, observation_count / max(int(shrinkage_target_observations), 1))
            base_abs_return = abs(float(direction_cfg.get("return_shock", 0.0)))
            sector_cfg = event_cfg.setdefault("sector_overrides", {})
            direction_sector_cfg = sector_cfg.setdefault(event_sector, {}).setdefault(direction, {})

            peer_return = record.get(f"median_abs_peer_forward_return_{return_horizon}d")
            if pd.notna(peer_return) and base_abs_return > 0.0:
                empirical_ratio = float(peer_return) / base_abs_return
                blended_ratio = (blend * empirical_ratio) + ((1.0 - blend) * default_peer_return_multiplier)
                direction_sector_cfg["peer_return_multiplier"] = round(
                    float(np.clip(blended_ratio, 0.1, 0.85)),
                    4,
                )
                sector_updates += 1

            peer_vol = record.get(vol_column)
            if pd.notna(peer_vol):
                blended_peer_vol = (blend * float(peer_vol)) + ((1.0 - blend) * default_peer_vol_multiplier)
                direction_sector_cfg["peer_vol_multiplier"] = round(
                    float(np.clip(blended_peer_vol, 1.0, 1.5)),
                    4,
                )

    payload["calibration_metadata"] = {
        "min_observations": int(min_observations),
        "sector_min_observations": int(sector_min_observations),
        "return_horizon_days": int(return_horizon),
        "vol_window_days": int(vol_window),
        "shrinkage_target_observations": int(shrinkage_target_observations),
        "updated_direction_rules": int(updates),
        "updated_sector_rules": int(sector_updates),
    }
    return payload

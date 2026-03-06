from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def load_event_mapping_config(path: str | Path) -> dict[str, Any]:
    """Load event-to-scenario mapping configuration from YAML."""
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    mappings = payload.get("event_mappings", {})
    if not mappings:
        raise ValueError("Event mapping config must contain event_mappings.")
    return payload


def _resolve_mapping_sections(mapping_config: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if "event_mappings" in mapping_config:
        return mapping_config.get("settings", {}), mapping_config["event_mappings"]
    return {}, mapping_config


def _coerce_utc_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def map_event_to_scenario(
    event: dict[str, Any],
    *,
    portfolio_tickers: list[str],
    mapping_config: dict[str, Any],
    ticker_sector_map: dict[str, str] | None = None,
    as_of: Any | None = None,
) -> dict[str, Any] | None:
    """Translate a normalized event into a concrete stress scenario."""
    settings, mappings = _resolve_mapping_sections(mapping_config)
    event_type = str(event.get("event_type", "other"))
    event_subtype = str(event.get("event_subtype", "")).strip() or None
    polarity = float(event.get("polarity", 0.0))
    severity = float(event.get("severity", 0.0))
    event_confidence = float(event.get("event_confidence", 0.0) or 0.0)
    link_confidence = float(event.get("link_confidence", 0.0) or 0.0)
    linked_tickers = [str(ticker).upper() for ticker in (event.get("tickers") or [])]
    ticker_sector_map = ticker_sector_map or {}

    direct_tickers = sorted(set(linked_tickers).intersection(portfolio_tickers))
    event_cfg = mappings.get(event_type) or mappings.get("other")
    if event_cfg is None:
        return None
    subtype_cfg = {}
    if event_subtype:
        subtype_cfg = (event_cfg.get("subtypes") or {}).get(event_subtype) or {}

    direction = "positive" if polarity >= 0.0 else "negative"
    direction_cfg = subtype_cfg.get(direction) or event_cfg.get(direction) or subtype_cfg.get("neutral") or event_cfg.get("neutral") or {}
    market_wide = bool(subtype_cfg.get("market_wide", event_cfg.get("market_wide", False)))
    if market_wide:
        direct_tickers = sorted(set(portfolio_tickers))

    as_of_timestamp = _coerce_utc_timestamp(as_of)
    published_at = _coerce_utc_timestamp(event.get("published_at"))
    event_age_days = None
    recency_decay = 1.0
    if as_of_timestamp is not None and published_at is not None:
        event_age_days = max(0.0, (as_of_timestamp - published_at).total_seconds() / 86400.0)
        max_age_days = settings.get("max_age_days")
        if max_age_days is not None and event_age_days > float(max_age_days):
            return None
        half_life_days = float(settings.get("recency_half_life_days", 3.0))
        min_decay = float(settings.get("min_decay", 0.25))
        if half_life_days > 0.0:
            recency_decay = max(min_decay, 0.5 ** (event_age_days / half_life_days))

    severity_scale = 0.5 + 0.5 * max(0.0, min(severity, 1.0))
    tier_scaling = settings.get("source_scaling", {}).get("tiers", {})
    bucket_scaling = settings.get("source_scaling", {}).get("buckets", {})
    source_scale = float(tier_scaling.get(str(event.get("source_tier", "")).strip(), 1.0) or 1.0)
    source_scale *= float(bucket_scaling.get(str(event.get("source_bucket", "")).strip(), 1.0) or 1.0)
    shock_scale = severity_scale * recency_decay * source_scale
    base_return_shock = float(direction_cfg.get("return_shock", 0.0))
    base_vol_multiplier = float(direction_cfg.get("vol_multiplier", 1.0))
    base_corr_multiplier = float(direction_cfg.get("correlation_multiplier", 1.0))
    spillover_confidence_scale = 0.5 + 0.25 * max(0.0, min(event_confidence, 1.0)) + 0.25 * max(0.0, min(link_confidence, 1.0))

    return_shock = base_return_shock * shock_scale
    vol_multiplier = 1.0 + (base_vol_multiplier - 1.0) * shock_scale
    correlation_multiplier = 1.0 + (base_corr_multiplier - 1.0) * shock_scale
    return_shocks = {ticker: return_shock for ticker in direct_tickers}

    event_sectors = sorted(
        {
            ticker_sector_map[ticker]
            for ticker in linked_tickers
            if ticker in ticker_sector_map
        }
    )
    sector_peer_tickers: dict[str, list[str]] = {}
    if not market_wide and event_sectors:
        sector_overrides = event_cfg.get("sector_overrides", {})
        subtype_sector_overrides = subtype_cfg.get("sector_overrides", {})
        for sector in event_sectors:
            direction_override = (
                (subtype_sector_overrides.get(sector) or {}).get(direction)
                or (sector_overrides.get(sector) or {}).get(direction)
            )
            if not direction_override:
                continue

            peers = sorted(
                ticker
                for ticker in portfolio_tickers
                if ticker not in direct_tickers and ticker_sector_map.get(ticker) == sector
            )
            if not peers:
                continue

            sector_peer_tickers[sector] = peers
            peer_multiplier = float(direction_override.get("peer_return_multiplier", 0.0))
            if peer_multiplier > 0.0:
                peer_return_shock = base_return_shock * peer_multiplier * shock_scale * spillover_confidence_scale
                for ticker in peers:
                    existing = return_shocks.get(ticker)
                    if existing is None or abs(peer_return_shock) > abs(existing):
                        return_shocks[ticker] = peer_return_shock

            peer_vol_multiplier = float(direction_override.get("peer_vol_multiplier", 1.0))
            if peer_vol_multiplier > 1.0:
                sector_vol_multiplier = 1.0 + (peer_vol_multiplier - 1.0) * shock_scale * spillover_confidence_scale
                vol_multiplier = max(vol_multiplier, sector_vol_multiplier)

    affected_tickers = sorted(return_shocks)
    if market_wide and not affected_tickers:
        affected_tickers = sorted(set(portfolio_tickers))
        return_shocks = {ticker: return_shock for ticker in affected_tickers}
    if not affected_tickers:
        return None

    return {
        "name": f"{event.get('event_id', 'evt')}_{event_type}",
        "description": event.get("headline", event_type),
        "return_shocks": return_shocks,
        "vol_multiplier": vol_multiplier,
        "correlation_multiplier": correlation_multiplier,
        "event_id": event.get("event_id"),
        "event_type": event_type,
        "event_subtype": event_subtype,
        "polarity": polarity,
        "severity": severity,
        "severity_scale": severity_scale,
        "recency_decay": recency_decay,
        "shock_scale": shock_scale,
        "source_scale": source_scale,
        "spillover_confidence_scale": spillover_confidence_scale,
        "event_age_days": event_age_days,
        "direct_tickers": direct_tickers,
        "event_sectors": event_sectors,
        "sector_peer_tickers": sector_peer_tickers,
        "affected_tickers": affected_tickers,
    }

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import yaml

FRESH_PROVIDER_PRIORITY = ["marketaux", "thenewsapi", "alphavantage", "newsapi"]
DELAYED_PROVIDER_PRIORITY = ["newsapi", "thenewsapi", "marketaux", "alphavantage"]


def _coerce_utc_timestamp(value: str | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def build_validation_windows(
    *,
    as_of: str | pd.Timestamp,
    windows: int,
    window_days: int = 1,
    step_days: int | None = None,
) -> list[dict[str, str]]:
    """Build descending live-validation windows ending at `as_of`."""
    if windows < 1:
        raise ValueError("windows must be at least 1")
    if window_days < 1:
        raise ValueError("window_days must be at least 1")

    end = pd.Timestamp(as_of)
    if end.tzinfo is None:
        end = end.tz_localize("UTC")
    else:
        end = end.tz_convert("UTC")

    step = int(step_days or window_days)
    rows: list[dict[str, str]] = []
    for offset in range(windows):
        window_end = end - pd.Timedelta(days=offset * step)
        window_start = window_end - pd.Timedelta(days=window_days)
        rows.append(
            {
                "window_label": f"window_{offset + 1:02d}",
                "published_after": window_start.date().isoformat(),
                "published_before": window_end.date().isoformat(),
            }
        )
    return rows


def choose_validation_providers(
    providers: list[str] | tuple[str, ...],
    *,
    published_before: str | pd.Timestamp,
    now: str | pd.Timestamp | None = None,
) -> dict[str, Any]:
    normalized: list[str] = []
    for provider in providers:
        name = str(provider).strip().lower()
        if name and name not in normalized:
            normalized.append(name)
    if not normalized:
        raise ValueError("At least one provider must be supplied.")

    window_end = _coerce_utc_timestamp(published_before)
    now_utc = _coerce_utc_timestamp(now or pd.Timestamp.now(tz="UTC"))
    delayed_cutoff = (now_utc - pd.Timedelta(days=1)).normalize()
    strategy = "delayed" if window_end.normalize() <= delayed_cutoff else "fresh"
    priority = DELAYED_PROVIDER_PRIORITY if strategy == "delayed" else FRESH_PROVIDER_PRIORITY
    ordered = [provider for provider in priority if provider in normalized]
    ordered.extend(provider for provider in normalized if provider not in ordered)
    return {
        "strategy": strategy,
        "providers": ordered,
        "delayed_cutoff": delayed_cutoff.date().isoformat(),
    }


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_events_frame(path: str | Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return pd.DataFrame(rows)


def collect_gap_samples(
    *,
    events_frame: pd.DataFrame,
    window_label: str,
    run_dir: str | Path,
) -> pd.DataFrame:
    """Collect candidate taxonomy or quality gaps from a processed run."""
    if events_frame.empty:
        return pd.DataFrame()

    frame = events_frame.copy()
    if "tickers" not in frame.columns:
        frame["tickers"] = [[] for _ in range(len(frame))]
    if "anchored_provider_symbols" not in frame.columns:
        frame["anchored_provider_symbols"] = [[] for _ in range(len(frame))]
    frame["tickers"] = frame["tickers"].apply(lambda value: value if isinstance(value, list) else [])
    frame["anchored_provider_symbols"] = frame["anchored_provider_symbols"].apply(
        lambda value: value if isinstance(value, list) else []
    )
    frame["ticker_count"] = frame["tickers"].apply(len)
    frame["anchored_provider_symbol_count"] = frame["anchored_provider_symbols"].apply(len)
    frame["quality_label"] = frame.get("quality_label", "unknown").fillna("unknown").astype(str)
    frame["watchlist_eligible"] = frame.get("watchlist_eligible", False).fillna(False).astype(bool)
    frame["event_type"] = frame.get("event_type", "other").fillna("other").astype(str)
    frame["event_confidence"] = pd.to_numeric(frame.get("event_confidence", 0.0), errors="coerce").fillna(0.0)
    frame["link_confidence"] = pd.to_numeric(frame.get("link_confidence", 0.0), errors="coerce").fillna(0.0)

    gap_rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        if record["event_type"] == "commentary":
            continue

        reasons: list[str] = []
        if record["event_type"] == "other":
            reasons.append("taxonomy_other")
        if (
            record["ticker_count"] == 0
            and record["event_type"] != "macro"
            and record["anchored_provider_symbol_count"] > 0
        ):
            reasons.append("zero_link_non_macro")
        if (
            record["event_type"] != "macro"
            and record["watchlist_eligible"]
            and record["ticker_count"] > 0
            and record["link_confidence"] < 0.8
        ):
            reasons.append("weak_link")
        if not reasons:
            continue

        gap_rows.append(
            {
                "window_label": window_label,
                "run_dir": str(run_dir),
                "event_id": record.get("event_id"),
                "headline": record.get("headline"),
                "source": record.get("source"),
                "source_tier": record.get("source_tier"),
                "source_bucket": record.get("source_bucket"),
                "event_type": record.get("event_type"),
                "quality_label": record.get("quality_label"),
                "watchlist_eligible": bool(record.get("watchlist_eligible")),
                "tickers": ",".join(record.get("tickers") or []),
                "anchored_provider_symbols": ",".join(record.get("anchored_provider_symbols") or []),
                "link_confidence": float(record.get("link_confidence", 0.0)),
                "event_confidence": float(record.get("event_confidence", 0.0)),
                "gap_reason": ",".join(reasons),
                "quality_reasons": " | ".join(record.get("quality_reasons") or []),
                "event_reasons": " | ".join(record.get("event_reasons") or []),
            }
        )
    return pd.DataFrame(gap_rows)


def summarize_validation_runs(run_rows: pd.DataFrame) -> dict[str, Any]:
    """Aggregate live-validation windows into a compact scorecard."""
    def _safe_mean(frame: pd.DataFrame, column: str) -> float | None:
        if frame.empty or column not in frame.columns:
            return None
        series = pd.to_numeric(frame[column], errors="coerce").dropna()
        if series.empty:
            return None
        return float(series.mean())

    def _origin_metrics(frame: pd.DataFrame, origin: str) -> dict[str, Any]:
        origin_rows = frame.loc[frame["window_origin"] == origin].copy()
        successful_rows = origin_rows.loc[origin_rows["status"] == "success"].copy()
        return {
            "window_count": int(len(origin_rows)),
            "successful_window_count": int(len(successful_rows)),
            "avg_watchlist_eligible_rate": _safe_mean(successful_rows, "watchlist_eligible_rate"),
            "avg_filtered_rate": _safe_mean(successful_rows, "filtered_rate"),
            "avg_other_rate": _safe_mean(successful_rows, "other_rate"),
            "avg_suspicious_link_rate": _safe_mean(successful_rows, "suspicious_link_rate"),
            "avg_active_other_rate": _safe_mean(successful_rows, "active_other_rate"),
            "avg_active_suspicious_link_rate": _safe_mean(successful_rows, "active_suspicious_link_rate"),
        }

    if run_rows.empty:
        return {
            "n_windows": 0,
            "successful_windows": 0,
            "fresh_sync_windows": 0,
            "archive_reuse_windows": 0,
            "failed_windows": 0,
            "fresh_sync_requested_windows": 0,
            "quota_blocked_windows": 0,
            "total_events": 0,
            "total_event_rows": 0,
            "avg_watchlist_eligible_rate": None,
            "avg_filtered_rate": None,
            "avg_other_rate": None,
            "avg_suspicious_link_rate": None,
            "avg_active_other_rate": None,
            "avg_active_suspicious_link_rate": None,
            "event_type_totals": {},
            "quality_totals": {},
            "fresh_sync_metrics": _origin_metrics(pd.DataFrame(columns=["window_origin", "status"]), "fresh_sync"),
            "archive_reuse_metrics": _origin_metrics(
                pd.DataFrame(columns=["window_origin", "status"]), "archive_reuse"
            ),
        }

    working = run_rows.copy()
    if "window_origin" not in working.columns:
        working["window_origin"] = working.apply(
            lambda row: "failed"
            if row.get("status") == "failed"
            else ("archive_reuse" if row.get("reused_from_archive") else "fresh_sync"),
            axis=1,
        )
    if "fresh_sync_requested" not in working.columns:
        working["fresh_sync_requested"] = True
    if "quota_blocked" not in working.columns:
        working["quota_blocked"] = False
    successful = working.loc[working["status"] == "success"].copy()
    if successful.empty:
        return {
            "n_windows": int(len(working)),
            "successful_windows": 0,
            "fresh_sync_windows": int((working["window_origin"] == "fresh_sync").sum()),
            "archive_reuse_windows": int((working["window_origin"] == "archive_reuse").sum()),
            "failed_windows": int((working["window_origin"] == "failed").sum()),
            "fresh_sync_requested_windows": int(working["fresh_sync_requested"].fillna(False).astype(bool).sum()),
            "quota_blocked_windows": int(working["quota_blocked"].fillna(False).astype(bool).sum()),
            "total_events": 0,
            "total_event_rows": 0,
            "avg_watchlist_eligible_rate": None,
            "avg_filtered_rate": None,
            "avg_other_rate": None,
            "avg_suspicious_link_rate": None,
            "avg_active_other_rate": None,
            "avg_active_suspicious_link_rate": None,
            "event_type_totals": {},
            "quality_totals": {},
            "fresh_sync_metrics": _origin_metrics(working, "fresh_sync"),
            "archive_reuse_metrics": _origin_metrics(working, "archive_reuse"),
        }

    def _weighted_total(records: Iterable[dict[str, Any]], column: str) -> dict[str, int]:
        totals: dict[str, int] = {}
        for record in records:
            payload = record.get(column) or {}
            for key, value in payload.items():
                totals[str(key)] = totals.get(str(key), 0) + int(value)
        return dict(sorted(totals.items(), key=lambda item: (-item[1], item[0])))

    return {
        "n_windows": int(len(working)),
        "successful_windows": int(len(successful)),
        "fresh_sync_windows": int((working["window_origin"] == "fresh_sync").sum()),
        "archive_reuse_windows": int((working["window_origin"] == "archive_reuse").sum()),
        "failed_windows": int((working["window_origin"] == "failed").sum()),
        "fresh_sync_requested_windows": int(working["fresh_sync_requested"].fillna(False).astype(bool).sum()),
        "quota_blocked_windows": int(working["quota_blocked"].fillna(False).astype(bool).sum()),
        "total_events": int(successful["total_events"].sum()),
        "total_event_rows": int(successful["event_rows"].sum()),
        "avg_watchlist_eligible_rate": float(successful["watchlist_eligible_rate"].mean()),
        "avg_filtered_rate": float(successful["filtered_rate"].mean()),
        "avg_other_rate": float(successful["other_rate"].mean()),
        "avg_suspicious_link_rate": float(successful["suspicious_link_rate"].mean()),
        "avg_active_other_rate": float(successful["active_other_rate"].mean()),
        "avg_active_suspicious_link_rate": float(successful["active_suspicious_link_rate"].mean()),
        "event_type_totals": _weighted_total(successful.to_dict(orient="records"), "event_type_distribution"),
        "quality_totals": _weighted_total(successful.to_dict(orient="records"), "quality_distribution"),
        "fresh_sync_metrics": _origin_metrics(working, "fresh_sync"),
        "archive_reuse_metrics": _origin_metrics(working, "archive_reuse"),
    }


def load_symbol_universe(path: str | Path, *, pack: str | None = None) -> list[str]:
    """Load a symbol universe or thematic pack from YAML."""
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    def _normalize(raw_symbols: Any) -> list[str]:
        return [str(symbol).upper() for symbol in (raw_symbols or []) if str(symbol).strip()]

    packs = payload.get("packs", {}) or {}
    symbols: list[str] = []
    if pack:
        pack_payload = packs.get(pack)
        if pack_payload is None:
            raise ValueError(f"Validation symbol config does not define pack '{pack}'.")
        if isinstance(pack_payload, dict):
            symbols = _normalize(pack_payload.get("symbols", []))
        else:
            symbols = _normalize(pack_payload)
    else:
        symbols = _normalize(payload.get("symbols", []))
        if not symbols:
            default_packs = payload.get("default_packs", [])
            for pack_name in default_packs:
                pack_payload = packs.get(pack_name, [])
                if isinstance(pack_payload, dict):
                    symbols.extend(_normalize(pack_payload.get("symbols", [])))
                else:
                    symbols.extend(_normalize(pack_payload))

    normalized = list(dict.fromkeys(symbols))
    if not normalized:
        raise ValueError("Validation symbol config must contain a non-empty symbols list.")
    return normalized

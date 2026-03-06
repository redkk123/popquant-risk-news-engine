from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def _coerce_utc_date(value: str | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.normalize()


def build_backfill_as_of_dates(
    *,
    start_as_of: str | pd.Timestamp,
    end_as_of: str | pd.Timestamp,
    cadence_days: int = 1,
) -> list[str]:
    """Build descending as-of dates for repeated live-validation suite runs."""
    if cadence_days < 1:
        raise ValueError("cadence_days must be at least 1")

    start = _coerce_utc_date(start_as_of)
    end = _coerce_utc_date(end_as_of)
    if end > start:
        raise ValueError("end_as_of must be on or before start_as_of")

    dates: list[str] = []
    current = start
    while current >= end:
        dates.append(current.date().isoformat())
        current = current - pd.Timedelta(days=cadence_days)
    return dates


def _load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_suite_result(suite_run: str | Path) -> dict[str, Any]:
    """Load the key status fields from a live-validation suite run."""
    suite_root = Path(suite_run)
    manifest = _load_json(suite_root / "live_validation_suite_manifest.json")
    validation_summary = _load_json(Path(manifest["validation_run"]) / "validation_summary.json")
    validation_governance = _load_json(Path(manifest["validation_governance_run"]) / "live_validation_governance.json")
    trend_governance = _load_json(Path(manifest["trend_governance_run"]) / "validation_trend_governance.json")

    aggregate = validation_summary.get("aggregate", {})
    validation_decision = validation_governance.get("decision", {})
    trend_decision = trend_governance.get("decision", {})

    return {
        "suite_run": str(suite_root),
        "validation_run": manifest["validation_run"],
        "validation_governance_run": manifest["validation_governance_run"],
        "trend_run": manifest["trend_run"],
        "trend_governance_run": manifest["trend_governance_run"],
        "validation_status": validation_decision.get("status"),
        "trend_status": trend_decision.get("status"),
        "n_windows": int(aggregate.get("n_windows", 0) or 0),
        "successful_windows": int(aggregate.get("successful_windows", 0) or 0),
        "total_events": int(aggregate.get("total_events", 0) or 0),
        "total_event_rows": int(aggregate.get("total_event_rows", 0) or 0),
        "avg_watchlist_eligible_rate": aggregate.get("avg_watchlist_eligible_rate"),
        "avg_filtered_rate": aggregate.get("avg_filtered_rate"),
        "avg_other_rate": aggregate.get("avg_other_rate"),
        "avg_suspicious_link_rate": aggregate.get("avg_suspicious_link_rate"),
        "avg_active_other_rate": aggregate.get("avg_active_other_rate"),
        "avg_active_suspicious_link_rate": aggregate.get("avg_active_suspicious_link_rate"),
        "gap_sample_count": int(validation_summary.get("gap_sample_count", 0) or 0),
        "trend_clean_pass_streak": trend_decision.get("metrics", {}).get("clean_pass_streak"),
        "trend_governed_run_count": trend_decision.get("metrics", {}).get("governed_run_count"),
    }


def summarize_backfill_runs(run_rows: pd.DataFrame) -> dict[str, Any]:
    """Summarize a batch of suite runs into a compact execution scorecard."""
    if run_rows.empty:
        return {
            "requested_runs": 0,
            "successful_runs": 0,
            "validation_pass_count": 0,
            "trend_pass_count": 0,
            "latest_suite_run": None,
            "latest_validation_status": None,
            "latest_trend_status": None,
            "avg_watchlist_eligible_rate": None,
            "avg_filtered_rate": None,
            "avg_other_rate": None,
            "avg_suspicious_link_rate": None,
            "avg_active_other_rate": None,
            "avg_active_suspicious_link_rate": None,
        }

    working = run_rows.copy()
    if "as_of" in working.columns:
        working = working.sort_values("as_of").reset_index(drop=True)
    latest = working.iloc[-1]

    def _safe_mean(column: str) -> float | None:
        if column not in working.columns:
            return None
        series = pd.to_numeric(working[column], errors="coerce").dropna()
        if series.empty:
            return None
        return float(series.mean())

    validation_statuses = working.get("validation_status", pd.Series(dtype=object)).fillna("")
    trend_statuses = working.get("trend_status", pd.Series(dtype=object)).fillna("")
    suite_statuses = working.get("suite_status", pd.Series(dtype=object)).fillna("")

    return {
        "requested_runs": int(len(working)),
        "successful_runs": int((suite_statuses == "success").sum()) if not suite_statuses.empty else int(len(working)),
        "validation_pass_count": int((validation_statuses == "PASS").sum()),
        "trend_pass_count": int((trend_statuses == "PASS").sum()),
        "latest_suite_run": latest.get("suite_run"),
        "latest_validation_status": latest.get("validation_status"),
        "latest_trend_status": latest.get("trend_status"),
        "avg_watchlist_eligible_rate": _safe_mean("avg_watchlist_eligible_rate"),
        "avg_filtered_rate": _safe_mean("avg_filtered_rate"),
        "avg_other_rate": _safe_mean("avg_other_rate"),
        "avg_suspicious_link_rate": _safe_mean("avg_suspicious_link_rate"),
        "avg_active_other_rate": _safe_mean("avg_active_other_rate"),
        "avg_active_suspicious_link_rate": _safe_mean("avg_active_suspicious_link_rate"),
    }

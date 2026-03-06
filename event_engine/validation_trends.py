from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _extract_as_of_from_run_log(path: str | Path | None) -> str | None:
    if not path:
        return None
    log_path = Path(path)
    if not log_path.exists():
        return None
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            details = record.get("details", {}) or {}
            as_of = details.get("as_of")
            if as_of:
                return str(as_of)
    return None


def _infer_promotion_scope(run_id: str, payload: dict[str, Any]) -> tuple[str, str | None]:
    explicit_scope = str(payload.get("promotion_scope", "")).strip().lower()
    as_of = payload.get("as_of") or _extract_as_of_from_run_log(payload.get("run_log"))
    if as_of:
        run_timestamp = pd.to_datetime(run_id, format="%Y%m%dT%H%M%SZ", utc=True)
        as_of_timestamp = pd.Timestamp(as_of)
        if as_of_timestamp.tzinfo is None:
            as_of_timestamp = as_of_timestamp.tz_localize("UTC")
        else:
            as_of_timestamp = as_of_timestamp.tz_convert("UTC")
        if abs((run_timestamp.normalize() - as_of_timestamp.normalize()).days) > 1:
            return "backfill", str(as_of_timestamp.date())
        if explicit_scope:
            return explicit_scope, str(as_of_timestamp.date())
        return "live", str(as_of_timestamp.date())

    if explicit_scope:
        return explicit_scope, str(as_of) if as_of else None

    return "live", None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any) -> float | None:
    coerced = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(coerced):
        return None
    return float(coerced)


def _window_origin(row: dict[str, Any]) -> str:
    explicit = str(row.get("window_origin", "")).strip().lower()
    if explicit in {"fresh_sync", "archive_reuse", "failed"}:
        return explicit
    if str(row.get("status", "")).lower() == "failed":
        return "failed"
    if bool(row.get("reused_from_archive")):
        return "archive_reuse"
    return "fresh_sync"


def _window_origin_stats(windows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        "fresh_sync_windows": 0,
        "archive_reuse_windows": 0,
        "failed_windows": 0,
        "fresh_sync_requested_windows": 0,
        "quota_blocked_windows": 0,
    }
    success_rows = {"fresh_sync": [], "archive_reuse": []}
    for row in windows:
        origin = _window_origin(row)
        counts[f"{origin}_windows"] += 1
        if bool(row.get("fresh_sync_requested", True)):
            counts["fresh_sync_requested_windows"] += 1
        if bool(row.get("quota_blocked", False)):
            counts["quota_blocked_windows"] += 1
        if str(row.get("status", "")).lower() == "success" and origin in success_rows:
            success_rows[origin].append(row)

    def _origin_metric(origin: str, metric: str) -> float | None:
        rows = success_rows[origin]
        if not rows:
            return None
        values = pd.to_numeric(pd.Series([row.get(metric) for row in rows]), errors="coerce").dropna()
        if values.empty:
            return None
        return float(values.mean())

    metrics: dict[str, Any] = {}
    for prefix, origin in (("fresh", "fresh_sync"), ("reuse", "archive_reuse")):
        metrics[f"{prefix}_avg_watchlist_eligible_rate"] = _origin_metric(origin, "watchlist_eligible_rate")
        metrics[f"{prefix}_avg_filtered_rate"] = _origin_metric(origin, "filtered_rate")
        metrics[f"{prefix}_avg_other_rate"] = _origin_metric(origin, "other_rate")
        metrics[f"{prefix}_avg_suspicious_link_rate"] = _origin_metric(origin, "suspicious_link_rate")
        metrics[f"{prefix}_avg_active_other_rate"] = _origin_metric(origin, "active_other_rate")
        metrics[f"{prefix}_avg_active_suspicious_link_rate"] = _origin_metric(origin, "active_suspicious_link_rate")
    return {**counts, **metrics}


def collect_validation_runs(root: str | Path, *, promotion_scope: str | None = "live") -> pd.DataFrame:
    """Collect all live-validation summaries under the output root."""
    validation_root = Path(root)
    rows: list[dict[str, Any]] = []
    for run_dir in sorted(path for path in validation_root.glob("*") if path.is_dir()):
        summary_path = run_dir / "validation_summary.json"
        if not summary_path.exists():
            continue
        payload = _load_json(summary_path)
        aggregate = payload.get("aggregate", {})
        inferred_scope, as_of = _infer_promotion_scope(run_dir.name, payload)
        window_stats = _window_origin_stats(payload.get("windows", []))
        window_summary_path = run_dir / "validation_window_summary.csv"
        zero_event_windows = 0
        if window_summary_path.exists():
            window_frame = pd.read_csv(window_summary_path)
            if "total_events" in window_frame.columns:
                zero_event_windows = int((pd.to_numeric(window_frame["total_events"], errors="coerce").fillna(0) == 0).sum())
        rows.append(
            {
                "run_id": run_dir.name,
                "run_dir": str(run_dir),
                "as_of": as_of,
                "promotion_scope": inferred_scope,
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
                "event_type_totals": aggregate.get("event_type_totals", {}),
                "quality_totals": aggregate.get("quality_totals", {}),
                "gap_sample_count": int(payload.get("gap_sample_count", 0) or 0),
                "zero_event_windows": int(zero_event_windows),
                "fresh_sync_windows": _safe_int(
                    aggregate.get("fresh_sync_windows", window_stats["fresh_sync_windows"])
                ),
                "archive_reuse_windows": _safe_int(
                    aggregate.get("archive_reuse_windows", window_stats["archive_reuse_windows"])
                ),
                "failed_windows": _safe_int(aggregate.get("failed_windows", window_stats["failed_windows"])),
                "fresh_sync_requested_windows": _safe_int(
                    aggregate.get("fresh_sync_requested_windows", window_stats["fresh_sync_requested_windows"])
                ),
                "quota_blocked_windows": _safe_int(
                    aggregate.get("quota_blocked_windows", window_stats["quota_blocked_windows"])
                ),
                "fresh_avg_watchlist_eligible_rate": aggregate.get(
                    "fresh_sync_metrics", {}
                ).get("avg_watchlist_eligible_rate", window_stats["fresh_avg_watchlist_eligible_rate"]),
                "fresh_avg_filtered_rate": aggregate.get("fresh_sync_metrics", {}).get(
                    "avg_filtered_rate", window_stats["fresh_avg_filtered_rate"]
                ),
                "fresh_avg_other_rate": aggregate.get("fresh_sync_metrics", {}).get(
                    "avg_other_rate", window_stats["fresh_avg_other_rate"]
                ),
                "fresh_avg_suspicious_link_rate": aggregate.get("fresh_sync_metrics", {}).get(
                    "avg_suspicious_link_rate", window_stats["fresh_avg_suspicious_link_rate"]
                ),
                "fresh_avg_active_other_rate": aggregate.get("fresh_sync_metrics", {}).get(
                    "avg_active_other_rate", window_stats["fresh_avg_active_other_rate"]
                ),
                "fresh_avg_active_suspicious_link_rate": aggregate.get("fresh_sync_metrics", {}).get(
                    "avg_active_suspicious_link_rate", window_stats["fresh_avg_active_suspicious_link_rate"]
                ),
                "reuse_avg_watchlist_eligible_rate": aggregate.get("archive_reuse_metrics", {}).get(
                    "avg_watchlist_eligible_rate", window_stats["reuse_avg_watchlist_eligible_rate"]
                ),
                "reuse_avg_filtered_rate": aggregate.get("archive_reuse_metrics", {}).get(
                    "avg_filtered_rate", window_stats["reuse_avg_filtered_rate"]
                ),
                "reuse_avg_other_rate": aggregate.get("archive_reuse_metrics", {}).get(
                    "avg_other_rate", window_stats["reuse_avg_other_rate"]
                ),
                "reuse_avg_suspicious_link_rate": aggregate.get("archive_reuse_metrics", {}).get(
                    "avg_suspicious_link_rate", window_stats["reuse_avg_suspicious_link_rate"]
                ),
                "reuse_avg_active_other_rate": aggregate.get("archive_reuse_metrics", {}).get(
                    "avg_active_other_rate", window_stats["reuse_avg_active_other_rate"]
                ),
                "reuse_avg_active_suspicious_link_rate": aggregate.get("archive_reuse_metrics", {}).get(
                    "avg_active_suspicious_link_rate", window_stats["reuse_avg_active_suspicious_link_rate"]
                ),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty or promotion_scope is None:
        return frame
    return frame.loc[frame["promotion_scope"] == promotion_scope].reset_index(drop=True)


def collect_validation_governance(root: str | Path) -> pd.DataFrame:
    """Collect governance decisions keyed by validation run path."""
    governance_root = Path(root)
    rows: list[dict[str, Any]] = []
    for run_dir in sorted(path for path in governance_root.glob("*") if path.is_dir()):
        decision_path = run_dir / "live_validation_governance.json"
        if not decision_path.exists():
            continue
        payload = _load_json(decision_path)
        decision = payload.get("decision", {})
        rows.append(
            {
                "governance_run_id": run_dir.name,
                "validation_run": str(payload.get("validation_run", "")),
                "governance_status": decision.get("status"),
                "governance_rationale": decision.get("rationale"),
                "findings_count": int(len(decision.get("findings", []))),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values("governance_run_id").drop_duplicates(subset=["validation_run"], keep="last")


def summarize_validation_trends(
    validation_runs: pd.DataFrame,
    governance_runs: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Summarize drift and current state across validation runs."""
    def _mean(frame: pd.DataFrame, column: str) -> float | None:
        if frame.empty or column not in frame.columns:
            return None
        values = pd.to_numeric(frame[column], errors="coerce").dropna()
        if values.empty:
            return None
        return float(values.mean())

    def _sum(frame: pd.DataFrame, column: str) -> int:
        if frame.empty or column not in frame.columns:
            return 0
        return int(pd.to_numeric(frame[column], errors="coerce").fillna(0).sum())

    if validation_runs.empty:
        return {
            "n_runs": 0,
            "latest_run_id": None,
            "latest_governance_status": None,
            "avg_other_rate": None,
            "avg_filtered_rate": None,
            "avg_watchlist_eligible_rate": None,
            "avg_active_other_rate": None,
            "avg_active_suspicious_link_rate": None,
            "latest_active_other_rate": None,
            "latest_active_suspicious_link_rate": None,
            "recent_avg_active_other_rate": None,
            "recent_avg_active_suspicious_link_rate": None,
            "recent_avg_filtered_rate": None,
            "recent_avg_watchlist_eligible_rate": None,
            "total_events": 0,
            "total_gap_samples": 0,
            "window_origin_totals": {
                "fresh_sync_windows": 0,
                "archive_reuse_windows": 0,
                "failed_windows": 0,
                "fresh_sync_requested_windows": 0,
                "quota_blocked_windows": 0,
            },
            "recent_window_origin_totals": {
                "fresh_sync_windows": 0,
                "archive_reuse_windows": 0,
                "failed_windows": 0,
                "fresh_sync_requested_windows": 0,
                "quota_blocked_windows": 0,
            },
            "fresh_sync_metrics": {},
            "archive_reuse_metrics": {},
            "drift_active_other_rate": None,
            "drift_active_suspicious_link_rate": None,
        }

    working = validation_runs.copy().sort_values("run_id").reset_index(drop=True)
    latest = working.iloc[-1]
    recent = working.tail(min(3, len(working)))

    latest_governance_status = None
    if governance_runs is not None and not governance_runs.empty:
        match = governance_runs.loc[governance_runs["validation_run"] == latest["run_dir"]]
        if not match.empty:
            latest_governance_status = match.iloc[-1]["governance_status"]

    def _drift(metric: str) -> float | None:
        values = pd.to_numeric(working[metric], errors="coerce").dropna()
        if len(values) < 2:
            return None
        return float(values.iloc[-1] - values.iloc[0])

    def _origin_metric_payload(prefix: str, frame: pd.DataFrame) -> dict[str, Any]:
        return {
            "avg_watchlist_eligible_rate": _mean(frame, f"{prefix}_avg_watchlist_eligible_rate"),
            "avg_filtered_rate": _mean(frame, f"{prefix}_avg_filtered_rate"),
            "avg_other_rate": _mean(frame, f"{prefix}_avg_other_rate"),
            "avg_suspicious_link_rate": _mean(frame, f"{prefix}_avg_suspicious_link_rate"),
            "avg_active_other_rate": _mean(frame, f"{prefix}_avg_active_other_rate"),
            "avg_active_suspicious_link_rate": _mean(frame, f"{prefix}_avg_active_suspicious_link_rate"),
        }

    return {
        "n_runs": int(len(working)),
        "latest_run_id": latest["run_id"],
        "latest_governance_status": latest_governance_status,
        "avg_other_rate": float(pd.to_numeric(working["avg_other_rate"], errors="coerce").mean()),
        "avg_filtered_rate": float(pd.to_numeric(working["avg_filtered_rate"], errors="coerce").mean()),
        "avg_watchlist_eligible_rate": float(
            pd.to_numeric(working["avg_watchlist_eligible_rate"], errors="coerce").mean()
        ),
        "avg_active_other_rate": float(pd.to_numeric(working["avg_active_other_rate"], errors="coerce").mean()),
        "avg_active_suspicious_link_rate": float(
            pd.to_numeric(working["avg_active_suspicious_link_rate"], errors="coerce").mean()
        ),
        "latest_active_other_rate": float(pd.to_numeric(pd.Series([latest["avg_active_other_rate"]]), errors="coerce").iloc[0]),
        "latest_active_suspicious_link_rate": float(
            pd.to_numeric(pd.Series([latest["avg_active_suspicious_link_rate"]]), errors="coerce").iloc[0]
        ),
        "recent_avg_active_other_rate": float(
            pd.to_numeric(recent["avg_active_other_rate"], errors="coerce").mean()
        ),
        "recent_avg_active_suspicious_link_rate": float(
            pd.to_numeric(recent["avg_active_suspicious_link_rate"], errors="coerce").mean()
        ),
        "recent_avg_filtered_rate": float(pd.to_numeric(recent["avg_filtered_rate"], errors="coerce").mean()),
        "recent_avg_watchlist_eligible_rate": float(
            pd.to_numeric(recent["avg_watchlist_eligible_rate"], errors="coerce").mean()
        ),
        "total_events": int(pd.to_numeric(working["total_events"], errors="coerce").sum()),
        "total_gap_samples": int(pd.to_numeric(working["gap_sample_count"], errors="coerce").sum()),
        "window_origin_totals": {
            "fresh_sync_windows": _sum(working, "fresh_sync_windows"),
            "archive_reuse_windows": _sum(working, "archive_reuse_windows"),
            "failed_windows": _sum(working, "failed_windows"),
            "fresh_sync_requested_windows": _sum(working, "fresh_sync_requested_windows"),
            "quota_blocked_windows": _sum(working, "quota_blocked_windows"),
        },
        "recent_window_origin_totals": {
            "fresh_sync_windows": _sum(recent, "fresh_sync_windows"),
            "archive_reuse_windows": _sum(recent, "archive_reuse_windows"),
            "failed_windows": _sum(recent, "failed_windows"),
            "fresh_sync_requested_windows": _sum(recent, "fresh_sync_requested_windows"),
            "quota_blocked_windows": _sum(recent, "quota_blocked_windows"),
        },
        "fresh_sync_metrics": _origin_metric_payload("fresh", working),
        "archive_reuse_metrics": _origin_metric_payload("reuse", working),
        "recent_fresh_sync_metrics": _origin_metric_payload("fresh", recent),
        "recent_archive_reuse_metrics": _origin_metric_payload("reuse", recent),
        "drift_other_rate": _drift("avg_other_rate"),
        "drift_filtered_rate": _drift("avg_filtered_rate"),
        "drift_watchlist_eligible_rate": _drift("avg_watchlist_eligible_rate"),
        "drift_active_other_rate": _drift("avg_active_other_rate"),
        "drift_active_suspicious_link_rate": _drift("avg_active_suspicious_link_rate"),
    }

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ValidationTrendThresholds:
    min_governed_runs: int = 10
    recent_runs: int = 5
    require_latest_governance_pass: bool = True
    min_clean_pass_streak: int = 5
    max_recent_avg_active_other_rate: float = 0.05
    max_recent_avg_active_suspicious_link_rate: float = 0.05
    max_recent_avg_filtered_rate: float = 0.60
    min_recent_avg_watchlist_eligible_rate: float = 0.50
    note_history_avg_active_suspicious_link_rate: float = 0.05
    min_total_events_for_coverage_metrics: int = 3
    min_recent_supported_runs_for_coverage_metrics: int = 3


def _safe_float(value: Any) -> float | None:
    coerced = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(coerced):
        return None
    return float(coerced)


def _mean(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns or frame.empty:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.mean())


def assess_validation_trend(
    *,
    trend_summary: dict[str, Any],
    trend_runs: pd.DataFrame,
    thresholds: ValidationTrendThresholds | None = None,
) -> dict[str, Any]:
    """Assess whether recent live-validation history is healthy enough for promotion."""
    thresholds = thresholds or ValidationTrendThresholds()
    findings: list[dict[str, Any]] = []
    observations: list[str] = []

    if trend_runs.empty:
        return {
            "status": "FAIL",
            "rationale": "No validation trend runs were available.",
            "thresholds": asdict(thresholds),
            "metrics": {
                "n_runs": 0,
                "governed_run_count": 0,
                "recent_run_count": 0,
                "latest_governance_status": None,
            },
            "findings": [
                {
                    "metric": "governed_run_count",
                    "actual": 0,
                    "limit": thresholds.min_governed_runs,
                    "comparator": ">=",
                    "severity": "high",
                }
            ],
            "observations": observations,
        }

    working = trend_runs.copy().sort_values("run_id").reset_index(drop=True)
    governed = (
        working.loc[working["governance_status"].notna()].copy()
        if "governance_status" in working.columns
        else pd.DataFrame()
    )
    if governed.empty:
        return {
            "status": "FAIL",
            "rationale": "No governed validation runs were available.",
            "thresholds": asdict(thresholds),
            "metrics": {
                "n_runs": int(trend_summary.get("n_runs", len(working)) or len(working)),
                "governed_run_count": 0,
                "recent_run_count": 0,
                "latest_governance_status": None,
            },
            "findings": [
                {
                    "metric": "governed_run_count",
                    "actual": 0,
                    "limit": thresholds.min_governed_runs,
                    "comparator": ">=",
                    "severity": "high",
                }
            ],
            "observations": [
                f"{int(len(working))} validation runs exist, but none have a paired governance decision."
            ],
        }

    recent = governed.tail(min(thresholds.recent_runs, len(governed))).copy()
    latest = governed.iloc[-1]
    latest_governance_status = latest.get("governance_status", trend_summary.get("latest_governance_status"))
    recent_supported = (
        recent.loc[pd.to_numeric(recent.get("total_events", pd.Series(dtype=float)), errors="coerce").fillna(0) >= thresholds.min_total_events_for_coverage_metrics].copy()
        if "total_events" in recent.columns
        else recent
    )
    coverage_metrics_supported = len(recent_supported) >= thresholds.min_recent_supported_runs_for_coverage_metrics

    recent_statuses = [str(value) if pd.notna(value) else "UNKNOWN" for value in recent["governance_status"]]
    clean_pass_streak = 0
    for value in reversed([str(status) for status in governed["governance_status"]]):
        if value == "PASS":
            clean_pass_streak += 1
            continue
        break

    metrics = {
        "n_runs": int(trend_summary.get("n_runs", len(working)) or len(working)),
        "governed_run_count": int(len(governed)),
        "recent_run_count": int(len(recent)),
        "latest_governance_status": latest_governance_status,
        "clean_pass_streak": clean_pass_streak,
        "ungoverned_run_count": int(len(working) - len(governed)),
        "history_avg_active_other_rate": _safe_float(
            trend_summary.get("avg_active_other_rate", trend_summary.get("avg_other_rate"))
        ),
        "history_avg_active_suspicious_link_rate": _safe_float(
            trend_summary.get("avg_active_suspicious_link_rate", trend_summary.get("avg_suspicious_link_rate"))
        ),
        "recent_avg_active_other_rate": _mean(recent, "avg_active_other_rate"),
        "recent_avg_active_suspicious_link_rate": _mean(recent, "avg_active_suspicious_link_rate"),
        "recent_avg_filtered_rate": _mean(recent_supported, "avg_filtered_rate"),
        "recent_avg_watchlist_eligible_rate": _mean(recent_supported, "avg_watchlist_eligible_rate"),
        "recent_supported_run_count": int(len(recent_supported)),
        "coverage_metrics_supported": bool(coverage_metrics_supported),
        "drift_active_other_rate": _safe_float(trend_summary.get("drift_active_other_rate")),
        "drift_active_suspicious_link_rate": _safe_float(trend_summary.get("drift_active_suspicious_link_rate")),
        "recent_window_origin_totals": trend_summary.get("recent_window_origin_totals", {}),
        "window_origin_totals": trend_summary.get("window_origin_totals", {}),
    }

    def _check(
        condition: bool,
        metric: str,
        actual: float | str | None,
        limit: float | int | str,
        comparator: str,
        severity: str,
    ) -> None:
        if condition:
            return
        findings.append(
            {
                "metric": metric,
                "actual": actual,
                "limit": limit,
                "comparator": comparator,
                "severity": severity,
            }
        )

    _check(
        metrics["governed_run_count"] >= thresholds.min_governed_runs,
        "governed_run_count",
        metrics["governed_run_count"],
        thresholds.min_governed_runs,
        ">=",
        "high",
    )
    if thresholds.require_latest_governance_pass:
        _check(
            latest_governance_status == "PASS",
            "latest_governance_status",
            latest_governance_status,
            "PASS",
            "==",
            "high",
        )
    _check(
        clean_pass_streak >= thresholds.min_clean_pass_streak,
        "clean_pass_streak",
        clean_pass_streak,
        thresholds.min_clean_pass_streak,
        ">=",
        "high",
    )

    recent_avg_active_other_rate = metrics["recent_avg_active_other_rate"]
    if recent_avg_active_other_rate is not None:
        _check(
            recent_avg_active_other_rate <= thresholds.max_recent_avg_active_other_rate,
            "recent_avg_active_other_rate",
            recent_avg_active_other_rate,
            thresholds.max_recent_avg_active_other_rate,
            "<=",
            "medium",
        )

    recent_avg_active_suspicious_link_rate = metrics["recent_avg_active_suspicious_link_rate"]
    if recent_avg_active_suspicious_link_rate is not None:
        _check(
            recent_avg_active_suspicious_link_rate <= thresholds.max_recent_avg_active_suspicious_link_rate,
            "recent_avg_active_suspicious_link_rate",
            recent_avg_active_suspicious_link_rate,
            thresholds.max_recent_avg_active_suspicious_link_rate,
            "<=",
            "high",
        )

    recent_avg_filtered_rate = metrics["recent_avg_filtered_rate"]
    if coverage_metrics_supported and recent_avg_filtered_rate is not None:
        _check(
            recent_avg_filtered_rate <= thresholds.max_recent_avg_filtered_rate,
            "recent_avg_filtered_rate",
            recent_avg_filtered_rate,
            thresholds.max_recent_avg_filtered_rate,
            "<=",
            "medium",
        )

    recent_avg_watchlist_eligible_rate = metrics["recent_avg_watchlist_eligible_rate"]
    if coverage_metrics_supported and recent_avg_watchlist_eligible_rate is not None:
        _check(
            recent_avg_watchlist_eligible_rate >= thresholds.min_recent_avg_watchlist_eligible_rate,
            "recent_avg_watchlist_eligible_rate",
            recent_avg_watchlist_eligible_rate,
            thresholds.min_recent_avg_watchlist_eligible_rate,
            ">=",
            "medium",
        )
    if not coverage_metrics_supported:
        observations.append(
            "Recent governed runs did not provide enough supported samples for coverage metrics "
            f"(need >= {thresholds.min_recent_supported_runs_for_coverage_metrics} runs with "
            f"total_events >= {thresholds.min_total_events_for_coverage_metrics})."
        )

    history_avg_active_suspicious_link_rate = metrics["history_avg_active_suspicious_link_rate"]
    if (
        history_avg_active_suspicious_link_rate is not None
        and history_avg_active_suspicious_link_rate > thresholds.note_history_avg_active_suspicious_link_rate
    ):
        observations.append(
            "Historical active suspicious-link average is still elevated, which indicates earlier runs still contain debt."
        )
    if metrics["ungoverned_run_count"] > 0:
        observations.append(
            f"{metrics['ungoverned_run_count']} validation runs are ungoverned and were excluded from promotion gating."
        )
    recent_origin_totals = metrics["recent_window_origin_totals"] or {}
    recent_reuse_windows = int(recent_origin_totals.get("archive_reuse_windows", 0) or 0)
    recent_failed_windows = int(recent_origin_totals.get("failed_windows", 0) or 0)
    if recent_reuse_windows > 0:
        observations.append(
            f"Recent governed history includes {recent_reuse_windows} archive-reuse windows; validation quality is clean but not fully fresh."
        )
    if recent_failed_windows > 0:
        observations.append(
            f"Recent governed history still includes {recent_failed_windows} failed validation windows."
        )

    drift_active_suspicious_link_rate = metrics["drift_active_suspicious_link_rate"]
    if drift_active_suspicious_link_rate is not None:
        if drift_active_suspicious_link_rate < 0.0:
            observations.append("Active suspicious-link drift is improving across the collected run history.")
        elif drift_active_suspicious_link_rate > 0.0:
            observations.append("Active suspicious-link drift is worsening across the collected run history.")

    if not findings:
        status = "PASS"
        rationale = "Recent live-validation history is healthy enough for promotion."
    elif any(finding["severity"] == "high" for finding in findings):
        status = "FAIL"
        rationale = "At least one high-severity trend threshold failed."
    else:
        status = "WARN"
        rationale = "Recent live-validation history is usable, but medium-severity thresholds need attention."

    return {
        "status": status,
        "rationale": rationale,
        "thresholds": asdict(thresholds),
        "metrics": metrics,
        "findings": findings,
        "observations": observations,
        "recent_runs": recent[["run_id", "governance_status"]].to_dict(orient="records")
        if {"run_id", "governance_status"}.issubset(recent.columns)
        else recent[["run_id"]].to_dict(orient="records"),
    }

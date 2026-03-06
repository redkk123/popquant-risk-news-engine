from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class LiveValidationThresholds:
    min_successful_windows_ratio: float = 0.8
    max_avg_active_other_rate: float = 0.10
    max_avg_active_suspicious_link_rate: float = 0.05
    max_avg_filtered_rate: float = 0.60
    min_avg_watchlist_eligible_rate: float = 0.50
    min_total_events_for_coverage_metrics: int = 3


def assess_live_validation(
    *,
    summary: dict[str, Any],
    window_frame: pd.DataFrame,
    thresholds: LiveValidationThresholds | None = None,
) -> dict[str, Any]:
    """Turn a live-validation summary into a PASS/WARN/FAIL decision."""
    thresholds = thresholds or LiveValidationThresholds()
    findings: list[dict[str, Any]] = []
    observations: list[str] = []

    n_windows = int(summary.get("n_windows", 0) or 0)
    successful_windows = int(summary.get("successful_windows", 0) or 0)
    success_ratio = float(successful_windows / n_windows) if n_windows else 0.0

    def _check(condition: bool, metric: str, actual: float | None, limit: float, comparator: str, severity: str) -> None:
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

    avg_active_other_rate = summary.get("avg_active_other_rate", summary.get("avg_other_rate"))
    avg_active_suspicious_link_rate = summary.get(
        "avg_active_suspicious_link_rate",
        summary.get("avg_suspicious_link_rate"),
    )
    avg_filtered_rate = summary.get("avg_filtered_rate")
    avg_watchlist_eligible_rate = summary.get("avg_watchlist_eligible_rate")
    total_events = int(summary.get("total_events", 0) or 0)
    coverage_metrics_supported = total_events >= thresholds.min_total_events_for_coverage_metrics

    _check(success_ratio >= thresholds.min_successful_windows_ratio, "successful_windows_ratio", success_ratio, thresholds.min_successful_windows_ratio, ">=", "high")
    if avg_active_other_rate is not None:
        _check(
            float(avg_active_other_rate) <= thresholds.max_avg_active_other_rate,
            "avg_active_other_rate",
            float(avg_active_other_rate),
            thresholds.max_avg_active_other_rate,
            "<=",
            "medium",
        )
    if avg_active_suspicious_link_rate is not None:
        _check(
            float(avg_active_suspicious_link_rate) <= thresholds.max_avg_active_suspicious_link_rate,
            "avg_active_suspicious_link_rate",
            float(avg_active_suspicious_link_rate),
            thresholds.max_avg_active_suspicious_link_rate,
            "<=",
            "high",
        )
    if coverage_metrics_supported and avg_filtered_rate is not None:
        _check(
            float(avg_filtered_rate) <= thresholds.max_avg_filtered_rate,
            "avg_filtered_rate",
            float(avg_filtered_rate),
            thresholds.max_avg_filtered_rate,
            "<=",
            "medium",
        )
    if coverage_metrics_supported and avg_watchlist_eligible_rate is not None:
        _check(
            float(avg_watchlist_eligible_rate) >= thresholds.min_avg_watchlist_eligible_rate,
            "avg_watchlist_eligible_rate",
            float(avg_watchlist_eligible_rate),
            thresholds.min_avg_watchlist_eligible_rate,
            ">=",
            "medium",
        )
    if not coverage_metrics_supported:
        observations.append(
            f"Coverage metrics were not enforced because total_events={total_events} is below the support threshold of {thresholds.min_total_events_for_coverage_metrics}."
        )

    if not findings:
        status = "PASS"
        rationale = "Live validation passed all configured thresholds."
    elif any(finding["severity"] == "high" for finding in findings):
        status = "FAIL"
        rationale = "At least one high-severity validation threshold failed."
    else:
        status = "WARN"
        rationale = "Validation stayed operational, but one or more medium-severity thresholds failed."

    failing_windows = []
    if not window_frame.empty:
        failing_windows = window_frame.loc[window_frame["status"] != "success", ["window_label", "status"]].to_dict(orient="records")

    return {
        "status": status,
        "rationale": rationale,
        "thresholds": asdict(thresholds),
        "metrics": {
            "n_windows": n_windows,
            "successful_windows": successful_windows,
            "successful_windows_ratio": success_ratio,
            "total_events": total_events,
            "coverage_metrics_supported": coverage_metrics_supported,
            "avg_active_other_rate": avg_active_other_rate,
            "avg_active_suspicious_link_rate": avg_active_suspicious_link_rate,
            "avg_filtered_rate": avg_filtered_rate,
            "avg_watchlist_eligible_rate": avg_watchlist_eligible_rate,
        },
        "findings": findings,
        "failing_windows": failing_windows,
        "observations": observations,
    }

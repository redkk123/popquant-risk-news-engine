from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    file_path = Path(path)
    if not file_path.exists():
        return records
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


def analyze_run_log(path: str | Path | None) -> dict[str, Any]:
    records = load_jsonl(path) if path else []
    error_records = [record for record in records if str(record.get("status")) == "error"]
    reused_records = [record for record in records if str(record.get("status")) == "reused"]
    quota_pressure = any(
        keyword in " ".join(
            [
                str(record.get("message", "")),
                json.dumps(record.get("details", {})),
            ]
        ).lower()
        for record in records
        for keyword in ("payment required", "quota", "402", "daily limit", "limit reached")
    )
    return {
        "event_count": int(len(records)),
        "error_count": int(len(error_records)),
        "reused_count": int(len(reused_records)),
        "quota_pressure": bool(quota_pressure),
    }


def _safe_float(value: Any) -> float | None:
    coerced = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(coerced):
        return None
    return float(coerced)


def rollup_event_frame(event_frame: pd.DataFrame, column: str) -> list[dict[str, Any]]:
    if event_frame.empty or column not in event_frame.columns:
        return []
    working = event_frame.copy()
    working[column] = working[column].fillna("unknown").astype(str)
    grouped = (
        working.groupby(column, dropna=False)
        .agg(
            event_rows=("event_id", "count"),
            unique_events=("event_id", "nunique"),
            unique_portfolios=("portfolio_id", "nunique"),
            max_delta_var=("delta_normal_var_loss_1d_99", "max"),
        )
        .reset_index()
        .sort_values(["event_rows", "max_delta_var"], ascending=[False, False])
    )
    return grouped.to_dict(orient="records")


def _derive_validation_origin_totals(validation_summary: dict[str, Any] | None) -> dict[str, int]:
    aggregate = ((validation_summary or {}).get("aggregate", {}) or {})
    origin_totals = dict(aggregate.get("window_origin_totals", {}) or {})
    if origin_totals:
        return {
            "fresh_sync_windows": int(origin_totals.get("fresh_sync_windows", 0) or 0),
            "archive_reuse_windows": int(origin_totals.get("archive_reuse_windows", 0) or 0),
            "failed_windows": int(origin_totals.get("failed_windows", 0) or 0),
            "fresh_sync_requested_windows": int(origin_totals.get("fresh_sync_requested_windows", 0) or 0),
            "quota_blocked_windows": int(origin_totals.get("quota_blocked_windows", 0) or 0),
        }

    counts = {
        "fresh_sync_windows": 0,
        "archive_reuse_windows": 0,
        "failed_windows": 0,
        "fresh_sync_requested_windows": 0,
        "quota_blocked_windows": 0,
    }
    for row in (validation_summary or {}).get("windows", []) or []:
        origin = str(row.get("window_origin", "")).strip().lower()
        if origin not in {"fresh_sync", "archive_reuse", "failed"}:
            origin = "archive_reuse" if bool(row.get("reused_from_archive")) else "failed" if str(row.get("status", "")).lower() == "failed" else "fresh_sync"
        counts[f"{origin}_windows"] += 1
        if bool(row.get("fresh_sync_requested", True)):
            counts["fresh_sync_requested_windows"] += 1
        if bool(row.get("quota_blocked", False)):
            counts["quota_blocked_windows"] += 1
    return counts


def _find_latest_capital_session_run(capital_sandbox_run: str | Path | None) -> Path | None:
    if not capital_sandbox_run:
        return None
    root = Path(capital_sandbox_run)
    candidates = [root]
    parent = root.parent
    if parent.exists():
        candidates.extend(sorted(path for path in parent.glob("*") if path.is_dir()))
    session_candidates = [
        candidate
        for candidate in candidates
        if (candidate / "capital_sandbox_summary.csv").exists() or (candidate / "live_session_status.json").exists()
    ]
    return session_candidates[-1] if session_candidates else None


def _load_capital_sandbox_block(capital_sandbox_run: str | Path | None) -> dict[str, Any]:
    root = _find_latest_capital_session_run(capital_sandbox_run)
    if root is None:
        return {}
    summary_path = root / "capital_sandbox_summary.csv"
    journal_path = root / "decision_journal.csv"
    live_journal_path = root / "decision_journal.live.csv"
    status_path = root / "live_session_status.json"

    summary_frame = pd.read_csv(summary_path) if summary_path.exists() else pd.DataFrame()
    journal_frame = (
        pd.read_csv(journal_path)
        if journal_path.exists()
        else pd.read_csv(live_journal_path)
        if live_journal_path.exists()
        else pd.DataFrame()
    )
    status_payload = load_json(status_path) if status_path.exists() else {}

    best_path = {}
    if not summary_frame.empty:
        best_path = summary_frame.sort_values("final_capital", ascending=False).iloc[0].to_dict()
    elif status_payload.get("best_path"):
        best_path = dict(status_payload["best_path"])

    quant_blocked_steps = 0
    confirmed_risk_steps = 0
    if not journal_frame.empty and {"eligible_event_count", "risk_on_allowed"}.issubset(journal_frame.columns):
        quant_blocked_steps = int(
            ((journal_frame["eligible_event_count"].fillna(0) > 0) & (~journal_frame["risk_on_allowed"].fillna(False))).sum()
        )
        confirmed_risk_steps = int(
            ((journal_frame["risk_on_allowed"].fillna(False)) & (journal_frame.get("target_exposure", pd.Series(dtype=float)).fillna(0) > 0)).sum()
        )

    session_meta = status_payload.get("session_meta", {}) or {}
    return {
        "run": str(root),
        "status": status_payload.get("status", "completed" if summary_path.exists() else "unknown"),
        "mode": status_payload.get("mode"),
        "best_path": best_path.get("path_name"),
        "final_capital": _safe_float(best_path.get("final_capital")),
        "total_return": _safe_float(best_path.get("total_return")),
        "trade_count": int(best_path.get("trade_count", 0) or 0),
        "providers_used": status_payload.get("providers_used", []),
        "degraded_to_empty_news": bool(status_payload.get("degraded_to_empty_news", False)),
        "news_refresh_attempts": int(session_meta.get("news_refresh_attempts", 0) or 0),
        "news_refresh_successes": int(session_meta.get("news_refresh_successes", 0) or 0),
        "news_refresh_errors": int(session_meta.get("news_refresh_errors", 0) or 0),
        "news_refresh_skipped": int(session_meta.get("news_refresh_skipped", 0) or 0),
        "news_refresh_skipped_quota_cooldown": int(
            session_meta.get("news_refresh_skipped_quota_cooldown", 0) or 0
        ),
        "stale_price_steps": int(session_meta.get("stale_price_steps", 0) or 0),
        "quant_blocked_steps": int(quant_blocked_steps),
        "confirmed_risk_steps": int(confirmed_risk_steps),
    }


def _find_latest_capital_compare_run(capital_sandbox_run: str | Path | None) -> Path | None:
    if not capital_sandbox_run:
        return None
    root = Path(capital_sandbox_run)
    candidates = [root]
    parent = root.parent
    if parent.exists():
        candidates.extend(sorted(path for path in parent.glob("*") if path.is_dir()))
    compare_candidates = [
        candidate for candidate in candidates if (candidate / "capital_compare_summary.csv").exists()
    ]
    return compare_candidates[-1] if compare_candidates else None


def _load_capital_compare_block(capital_sandbox_run: str | Path | None) -> dict[str, Any]:
    compare_root = _find_latest_capital_compare_run(capital_sandbox_run)
    if compare_root is None:
        return {}

    summary_path = compare_root / "capital_compare_summary.csv"
    if not summary_path.exists():
        return {}

    summary_frame = pd.read_csv(summary_path)
    if summary_frame.empty:
        return {"run": str(compare_root), "session_count": 0, "path_count": 0, "best_by_session": []}

    best_overall = summary_frame.sort_values("final_capital", ascending=False).iloc[0]
    best_by_session = (
        summary_frame.sort_values(["session_minutes", "final_capital"], ascending=[True, False])
        .groupby("session_label", as_index=False)
        .first()
        .sort_values("session_minutes")
    )
    return {
        "run": str(compare_root),
        "session_count": int(summary_frame.get("session_label", pd.Series(dtype=str)).nunique()),
        "path_count": int(summary_frame.get("path_name", pd.Series(dtype=str)).nunique()),
        "overall_best_session": best_overall.get("session_label"),
        "overall_best_path": best_overall.get("path_name"),
        "overall_best_final_capital": _safe_float(best_overall.get("final_capital")),
        "best_by_session": best_by_session.to_dict(orient="records"),
        "report_md": str(compare_root / "capital_compare_report.md")
        if (compare_root / "capital_compare_report.md").exists()
        else None,
        "equity_curve_png": str(compare_root / "capital_compare_equity_curve.png")
        if (compare_root / "capital_compare_equity_curve.png").exists()
        else None,
        "final_capital_png": str(compare_root / "capital_compare_final_capital.png")
        if (compare_root / "capital_compare_final_capital.png").exists()
        else None,
    }


def build_operator_summary(
    *,
    watchlist_run: str | Path,
    validation_run: str | Path | None = None,
    validation_governance_run: str | Path | None = None,
    trend_governance_run: str | Path | None = None,
    capital_sandbox_run: str | Path | None = None,
) -> dict[str, Any]:
    watchlist_root = Path(watchlist_run)
    live_manifest_exists = (watchlist_root / "live_marketaux_manifest.json").exists()
    manifest_name = "live_marketaux_manifest.json" if live_manifest_exists else "watchlist_manifest.json"
    manifest = load_json(watchlist_root / manifest_name)
    summary_frame = pd.read_csv(manifest["outputs"]["summary_csv"])
    events_frame = pd.read_csv(manifest["outputs"]["events_csv"])

    validation_summary = None
    validation_window_frame = pd.DataFrame()
    if validation_run:
        validation_root = Path(validation_run)
        validation_summary = load_json(validation_root / "validation_summary.json")
        validation_csv = validation_root / "validation_window_summary.csv"
        if validation_csv.exists():
            validation_window_frame = pd.read_csv(validation_csv)

    validation_governance = None
    if validation_governance_run:
        validation_governance = load_json(Path(validation_governance_run) / "live_validation_governance.json")

    trend_governance = None
    if trend_governance_run:
        trend_governance = load_json(Path(trend_governance_run) / "validation_trend_governance.json")

    top_portfolios = summary_frame.sort_values(
        ["max_delta_normal_var_loss_1d_99", "stressed_normal_var_loss_1d_99"],
        ascending=[False, False],
    ).head(5)
    top_events = events_frame.sort_values(
        ["delta_normal_var_loss_1d_99", "shock_scale"],
        ascending=[False, False],
    ).head(10)

    zero_event_windows = 0
    reused_window_count = 0
    failed_window_count = 0
    validation_origin_totals: dict[str, Any] = {}
    if validation_summary:
        aggregate = validation_summary.get("aggregate", {})
        validation_origin_totals = _derive_validation_origin_totals(validation_summary)
        zero_event_windows = int((validation_window_frame.get("total_events", pd.Series(dtype=float)).fillna(0) == 0).sum()) if not validation_window_frame.empty else 0
        reused_window_count = int(aggregate.get("archive_reuse_windows", 0) or validation_origin_totals.get("archive_reuse_windows", 0) or 0)
        failed_window_count = int(aggregate.get("failed_windows", 0) or validation_origin_totals.get("failed_windows", 0) or 0)

    run_log_stats = analyze_run_log(manifest.get("run_log"))
    sync_stats = manifest.get("sync_stats", {})
    audit_summary = manifest.get("live_audit_summary", {})
    capital_sandbox = _load_capital_sandbox_block(capital_sandbox_run)
    capital_compare = _load_capital_compare_block(capital_sandbox_run)

    validation_block = {
        "fresh_sync_windows": int(validation_origin_totals.get("fresh_sync_windows", 0) or 0),
        "archive_reuse_windows": int(validation_origin_totals.get("archive_reuse_windows", 0) or 0),
        "failed_windows": int(validation_origin_totals.get("failed_windows", 0) or 0),
        "fresh_sync_requested_windows": int(validation_origin_totals.get("fresh_sync_requested_windows", 0) or 0),
        "quota_blocked_windows": int(validation_origin_totals.get("quota_blocked_windows", 0) or 0),
        "fresh_sync_dominant": bool(
            int(validation_origin_totals.get("fresh_sync_windows", 0) or 0)
            >= int(validation_origin_totals.get("archive_reuse_windows", 0) or 0)
        ),
        "fresh_avg_active_other_rate": _safe_float(
            ((validation_summary or {}).get("aggregate", {}) or {}).get("fresh_sync_metrics", {}).get("avg_active_other_rate")
        ),
        "fresh_avg_active_suspicious_link_rate": _safe_float(
            ((validation_summary or {}).get("aggregate", {}) or {}).get("fresh_sync_metrics", {}).get(
                "avg_active_suspicious_link_rate"
            )
        ),
        "archive_avg_active_other_rate": _safe_float(
            ((validation_summary or {}).get("aggregate", {}) or {}).get("archive_reuse_metrics", {}).get("avg_active_other_rate")
        ),
        "archive_avg_active_suspicious_link_rate": _safe_float(
            ((validation_summary or {}).get("aggregate", {}) or {}).get("archive_reuse_metrics", {}).get(
                "avg_active_suspicious_link_rate"
            )
        ),
    }

    if not top_portfolios.empty:
        top_portfolios = top_portfolios.copy()
        top_portfolios["why_ranked"] = top_portfolios.apply(
            lambda row: f"{row.get('top_event_type', 'unknown')} moved VaR by {row.get('max_delta_normal_var_loss_1d_99')}",
            axis=1,
        )

    if not top_events.empty:
        top_events = top_events.copy()
        top_events["why_ranked"] = top_events.apply(
            lambda row: f"{row.get('event_type', 'unknown')} / {row.get('source_tier', 'unknown')} / delta {row.get('delta_normal_var_loss_1d_99')}",
            axis=1,
        )

    return {
        "watchlist_run": str(watchlist_root),
        "watchlist_origin": "fresh_sync" if live_manifest_exists else "fixture_refresh",
        "portfolio_count": int(len(summary_frame)),
        "event_row_count": int(len(events_frame)),
        "governance": {
            "live_validation_status": (validation_governance or {}).get("decision", {}).get("status"),
            "trend_status": (trend_governance or {}).get("decision", {}).get("status"),
        },
        "sync": {
            "provider": sync_stats.get("provider"),
            "articles_seen": int(sync_stats.get("articles_seen", 0) or 0),
            "inserted": int(sync_stats.get("inserted", 0) or 0),
            "pages_fetched": int(sync_stats.get("pages_fetched", 0) or 0),
            "symbols": sync_stats.get("request", {}).get("symbols", []),
        },
        "ops": {
            "quota_pressure": bool(run_log_stats["quota_pressure"]),
            "run_log_errors": int(run_log_stats["error_count"]),
            "run_log_reused": int(run_log_stats["reused_count"]),
            "zero_event_windows": int(zero_event_windows),
            "reused_window_count": int(reused_window_count),
            "failed_window_count": int(failed_window_count),
        },
        "validation": validation_block,
        "quality": {
            "total_events": int(audit_summary.get("total_events", 0) or 0),
            "watchlist_eligible_events": int(audit_summary.get("watchlist_eligible_events", 0) or 0),
            "filtered_events": int(audit_summary.get("filtered_events", 0) or 0),
            "suspicious_link_events": int(audit_summary.get("suspicious_link_events", 0) or 0),
            "eligible_suspicious_link_events": int(audit_summary.get("eligible_suspicious_link_events", 0) or 0),
        },
        "capital_sandbox": capital_sandbox,
        "capital_compare": capital_compare,
        "validation_origin_totals": validation_origin_totals,
        "top_portfolios": top_portfolios.to_dict(orient="records"),
        "top_events": top_events.to_dict(orient="records"),
        "rollups": {
            "event_type": rollup_event_frame(events_frame, "event_type"),
            "event_subtype": rollup_event_frame(events_frame, "event_subtype"),
            "story_bucket": rollup_event_frame(events_frame, "story_bucket"),
            "source_tier": rollup_event_frame(events_frame, "source_tier"),
        },
        "headline_metrics": {
            "max_portfolio_delta_var": _safe_float(summary_frame.get("max_delta_normal_var_loss_1d_99", pd.Series(dtype=float)).max()),
            "max_event_delta_var": _safe_float(events_frame.get("delta_normal_var_loss_1d_99", pd.Series(dtype=float)).max()),
        },
    }

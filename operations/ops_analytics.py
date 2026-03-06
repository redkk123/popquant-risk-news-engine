from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from event_engine.validation_trends import (
    collect_validation_governance,
    collect_validation_runs,
    summarize_validation_trends,
)
from operations.operator_summary import analyze_run_log, load_json


def _safe_float(value: Any) -> float | None:
    coerced = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(coerced):
        return None
    return float(coerced)


def _latest_json_payload(root: Path, filename: str) -> dict[str, Any] | None:
    candidates = sorted(path for path in root.glob("*") if path.is_dir())
    for run_dir in reversed(candidates):
        payload_path = run_dir / filename
        if payload_path.exists():
            with payload_path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
    return None


def _rollup_named_counts(named_counts: list[pd.Series], column_name: str) -> list[dict[str, Any]]:
    if not named_counts:
        return []
    combined = pd.concat(named_counts, axis=1).fillna(0.0).sum(axis=1).sort_values(ascending=False)
    rows = pd.DataFrame({column_name: combined.index.astype(str), "event_rows": combined.to_numpy()})
    return rows.to_dict(orient="records")


def _path_leaderboard(frame: pd.DataFrame, *, column_name: str = "best_path") -> list[dict[str, Any]]:
    if frame.empty or column_name not in frame.columns:
        return []
    working = frame.loc[frame[column_name].notna()].copy()
    if working.empty:
        return []
    grouped = (
        working.groupby(column_name, dropna=False)
        .agg(
            run_count=("run_id", "count"),
            avg_final_capital=("final_capital", "mean"),
            avg_total_return=("total_return", "mean"),
        )
        .reset_index()
        .sort_values(["run_count", "avg_final_capital"], ascending=[False, False])
    )
    grouped[column_name] = grouped[column_name].fillna("unknown").astype(str)
    return grouped.rename(columns={column_name: "path_name"}).to_dict(orient="records")


def collect_watchlist_runs(root: str | Path) -> pd.DataFrame:
    watchlist_root = Path(root)
    rows: list[dict[str, Any]] = []
    for run_dir in sorted(path for path in watchlist_root.glob("*") if path.is_dir()):
        live_manifest = run_dir / "live_marketaux_manifest.json"
        fixture_manifest = run_dir / "watchlist_manifest.json"
        manifest_path = live_manifest if live_manifest.exists() else fixture_manifest if fixture_manifest.exists() else None
        if manifest_path is None:
            continue

        manifest = load_json(manifest_path)
        outputs = manifest.get("outputs", {})
        summary_csv = outputs.get("summary_csv")
        events_csv = outputs.get("events_csv")
        summary_frame = pd.read_csv(summary_csv) if summary_csv and Path(summary_csv).exists() else pd.DataFrame()
        events_frame = pd.read_csv(events_csv) if events_csv and Path(events_csv).exists() else pd.DataFrame()
        run_log_stats = analyze_run_log(manifest.get("run_log"))
        sync_stats = manifest.get("sync_stats", {})
        audit = manifest.get("live_audit_summary", {})

        rows.append(
            {
                "run_id": run_dir.name,
                "run_dir": str(run_dir),
                "run_origin": "fresh_sync" if live_manifest.exists() else "fixture_refresh",
                "portfolio_count": int(len(summary_frame)),
                "event_row_count": int(len(events_frame)),
                "quota_pressure": bool(run_log_stats["quota_pressure"]),
                "run_log_errors": int(run_log_stats["error_count"]),
                "run_log_reused": int(run_log_stats["reused_count"]),
                "articles_seen": int(sync_stats.get("articles_seen", 0) or 0),
                "pages_fetched": int(sync_stats.get("pages_fetched", 0) or 0),
                "total_events": int(audit.get("total_events", 0) or 0),
                "watchlist_eligible_events": int(audit.get("watchlist_eligible_events", 0) or 0),
                "filtered_events": int(audit.get("filtered_events", 0) or 0),
                "suspicious_link_events": int(audit.get("suspicious_link_events", 0) or 0),
                "eligible_suspicious_link_events": int(audit.get("eligible_suspicious_link_events", 0) or 0),
                "event_type_totals": events_frame.get("event_type", pd.Series(dtype=str)).fillna("unknown").value_counts().to_dict(),
                "event_subtype_totals": events_frame.get("event_subtype", pd.Series(dtype=str)).fillna("unknown").replace({"": "unknown"}).value_counts().to_dict(),
                "source_tier_totals": events_frame.get("source_tier", pd.Series(dtype=str)).fillna("unknown").value_counts().to_dict(),
            }
        )
    return pd.DataFrame(rows).sort_values("run_id").reset_index(drop=True) if rows else pd.DataFrame()


def collect_capital_sandbox_runs(root: str | Path) -> pd.DataFrame:
    sandbox_root = Path(root)
    rows: list[dict[str, Any]] = []
    for run_dir in sorted(path for path in sandbox_root.glob("*") if path.is_dir()):
        summary_path = run_dir / "capital_sandbox_summary.csv"
        compare_summary_path = run_dir / "capital_compare_summary.csv"
        journal_path = run_dir / "decision_journal.csv"
        live_journal_path = run_dir / "decision_journal.live.csv"
        status_path = run_dir / "live_session_status.json"
        if not summary_path.exists() and not status_path.exists() and not compare_summary_path.exists():
            continue

        if summary_path.exists() or status_path.exists():
            summary_frame = pd.read_csv(summary_path) if summary_path.exists() else pd.DataFrame()
            journal_frame = (
                pd.read_csv(journal_path)
                if journal_path.exists()
                else pd.read_csv(live_journal_path)
                if live_journal_path.exists()
                else pd.DataFrame()
            )
            status_payload = load_json(status_path) if status_path.exists() else {}
            best_row = (
                summary_frame.sort_values("final_capital", ascending=False).iloc[0].to_dict()
                if not summary_frame.empty
                else dict(status_payload.get("best_path", {}))
            )

            quant_blocked_steps = 0
            confirmed_risk_steps = 0
            path_blocked_steps = 0
            if not journal_frame.empty and {"eligible_event_count", "risk_on_allowed"}.issubset(journal_frame.columns):
                quant_blocked_steps = int(
                    ((journal_frame["eligible_event_count"].fillna(0) > 0) & (~journal_frame["risk_on_allowed"].fillna(False))).sum()
                )
                confirmed_risk_steps = int(
                    ((journal_frame["risk_on_allowed"].fillna(False)) & (journal_frame.get("target_exposure", pd.Series(dtype=float)).fillna(0) > 0)).sum()
                )
            if not journal_frame.empty and {"eligible_event_count", "path_confirmation"}.issubset(journal_frame.columns):
                path_blocked_steps = int(
                    (
                        (journal_frame["eligible_event_count"].fillna(0) > 0)
                        & (journal_frame["path_confirmation"].fillna("neutral") == "underperforming")
                    ).sum()
                )

            session_meta = status_payload.get("session_meta", {}) or {}
            rows.append(
                {
                    "run_id": run_dir.name,
                    "run_record_id": f"{run_dir.name}:single_session",
                    "run_dir": str(run_dir),
                    "run_kind": "single_session",
                    "status": status_payload.get("status", "completed" if summary_path.exists() else "unknown"),
                    "best_path": best_row.get("path_name"),
                    "final_capital": _safe_float(best_row.get("final_capital")),
                    "total_return": _safe_float(best_row.get("total_return")),
                    "news_refresh_attempts": int(session_meta.get("news_refresh_attempts", 0) or 0),
                    "news_refresh_successes": int(session_meta.get("news_refresh_successes", 0) or 0),
                    "news_refresh_errors": int(session_meta.get("news_refresh_errors", 0) or 0),
                    "news_refresh_skipped": int(session_meta.get("news_refresh_skipped", 0) or 0),
                    "stale_price_steps": int(session_meta.get("stale_price_steps", 0) or 0),
                    "quant_blocked_steps": int(quant_blocked_steps),
                    "confirmed_risk_steps": int(confirmed_risk_steps),
                    "path_blocked_steps": int(path_blocked_steps),
                    "degraded_to_empty_news": bool(status_payload.get("degraded_to_empty_news", False)),
                }
            )

        if compare_summary_path.exists():
            compare_frame = pd.read_csv(compare_summary_path)
            if not compare_frame.empty:
                best_row = compare_frame.sort_values("final_capital", ascending=False).iloc[0].to_dict()
                rows.append(
                    {
                        "run_id": run_dir.name,
                        "run_record_id": f"{run_dir.name}:session_compare",
                        "run_dir": str(run_dir),
                        "run_kind": "session_compare",
                        "status": "completed",
                        "best_path": best_row.get("path_name"),
                        "best_session": best_row.get("session_label"),
                        "final_capital": _safe_float(best_row.get("final_capital")),
                        "total_return": _safe_float(best_row.get("total_return")),
                        "news_refresh_attempts": 0,
                        "news_refresh_successes": 0,
                        "news_refresh_errors": 0,
                        "news_refresh_skipped": 0,
                        "stale_price_steps": 0,
                        "quant_blocked_steps": 0,
                        "confirmed_risk_steps": 0,
                        "path_blocked_steps": 0,
                        "degraded_to_empty_news": False,
                        "session_count": int(compare_frame.get("session_label", pd.Series(dtype=str)).nunique()),
                        "path_count": int(compare_frame.get("path_name", pd.Series(dtype=str)).nunique()),
                    }
                )
    return pd.DataFrame(rows).sort_values("run_id").reset_index(drop=True) if rows else pd.DataFrame()


def build_ops_analytics(
    *,
    project_root: str | Path,
    recent_runs: int = 5,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    root = Path(project_root)
    validation_runs = collect_validation_runs(root / "output" / "live_validation", promotion_scope=None)
    governance_runs = collect_validation_governance(root / "output" / "live_validation_governance")
    if not validation_runs.empty and not governance_runs.empty:
        validation_runs = validation_runs.merge(
            governance_runs[["validation_run", "governance_status", "governance_rationale"]],
            left_on="run_dir",
            right_on="validation_run",
            how="left",
        )

    watchlist_runs = collect_watchlist_runs(root / "output" / "live_marketaux_watchlist")
    capital_sandbox_runs = collect_capital_sandbox_runs(root / "output" / "capital_sandbox")
    trend_summary = summarize_validation_trends(validation_runs, governance_runs)
    latest_trend_governance = _latest_json_payload(root / "output" / "validation_trend_governance", "validation_trend_governance.json")
    latest_validation_governance = _latest_json_payload(root / "output" / "live_validation_governance", "live_validation_governance.json")
    latest_operator_summary = _latest_json_payload(root / "output" / "operator_summary", "operator_summary.json")

    recent_validation = validation_runs.tail(min(int(recent_runs), len(validation_runs))).copy() if not validation_runs.empty else pd.DataFrame()
    recent_watchlist = watchlist_runs.tail(min(int(recent_runs), len(watchlist_runs))).copy() if not watchlist_runs.empty else pd.DataFrame()
    recent_capital = capital_sandbox_runs.tail(min(int(recent_runs), len(capital_sandbox_runs))).copy() if not capital_sandbox_runs.empty else pd.DataFrame()
    failed_validation_windows = int(pd.to_numeric(validation_runs.get("failed_windows", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not validation_runs.empty else 0
    total_validation_windows = int(pd.to_numeric(validation_runs.get("n_windows", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not validation_runs.empty else 0
    zero_event_windows = int(pd.to_numeric(validation_runs.get("zero_event_windows", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not validation_runs.empty else 0
    quota_pressure_count = int(pd.to_numeric(validation_runs.get("quota_blocked_windows", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not validation_runs.empty else 0

    recent_pass_runs = recent_validation.loc[recent_validation.get("governance_status", pd.Series(dtype=str)) == "PASS"].copy() if not recent_validation.empty else pd.DataFrame()
    if recent_pass_runs.empty:
        green_streak_origin = "none"
    else:
        all_fresh = bool((recent_pass_runs["fresh_sync_windows"].fillna(0) > 0).all() and (recent_pass_runs["archive_reuse_windows"].fillna(0) == 0).all())
        all_reuse = bool((recent_pass_runs["archive_reuse_windows"].fillna(0) > 0).all() and (recent_pass_runs["fresh_sync_windows"].fillna(0) == 0).all())
        green_streak_origin = "fresh_only" if all_fresh else "archive_only" if all_reuse else "mixed"

    source_rollups = _rollup_named_counts(
        [pd.Series(row["source_tier_totals"]) for _, row in recent_watchlist.iterrows() if row["source_tier_totals"]],
        "source_tier",
    )
    event_type_rollups = _rollup_named_counts(
        [pd.Series(row["event_type_totals"]) for _, row in recent_watchlist.iterrows() if row["event_type_totals"]],
        "event_type",
    )
    event_subtype_rollups = _rollup_named_counts(
        [pd.Series(row["event_subtype_totals"]) for _, row in recent_watchlist.iterrows() if row["event_subtype_totals"]],
        "event_subtype",
    )
    single_session_capital = capital_sandbox_runs.loc[capital_sandbox_runs.get("run_kind", pd.Series(dtype=str)) == "single_session"].copy() if not capital_sandbox_runs.empty else pd.DataFrame()
    compare_capital = capital_sandbox_runs.loc[capital_sandbox_runs.get("run_kind", pd.Series(dtype=str)) == "session_compare"].copy() if not capital_sandbox_runs.empty else pd.DataFrame()
    capital_path_leaderboard = _path_leaderboard(single_session_capital)
    capital_compare_leaderboard = _path_leaderboard(compare_capital)
    refresh_attempts = int(pd.to_numeric(single_session_capital.get("news_refresh_attempts", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not single_session_capital.empty else 0
    refresh_successes = int(pd.to_numeric(single_session_capital.get("news_refresh_successes", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not single_session_capital.empty else 0
    refresh_errors = int(pd.to_numeric(single_session_capital.get("news_refresh_errors", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not single_session_capital.empty else 0
    refresh_skips = int(pd.to_numeric(single_session_capital.get("news_refresh_skipped", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not single_session_capital.empty else 0

    summary = {
        "latest_watchlist_run_id": watchlist_runs.iloc[-1]["run_id"] if not watchlist_runs.empty else None,
        "latest_validation_run_id": validation_runs.iloc[-1]["run_id"] if not validation_runs.empty else None,
        "latest_capital_sandbox_run_id": capital_sandbox_runs.iloc[-1]["run_id"] if not capital_sandbox_runs.empty else None,
        "latest_governance_state": {
            "live_validation": (latest_validation_governance or {}).get("decision", {}).get("status"),
            "trend_governance": (latest_trend_governance or {}).get("decision", {}).get("status"),
        },
        "clean_pass_streak": ((latest_trend_governance or {}).get("decision", {}).get("metrics", {}) or {}).get("clean_pass_streak"),
        "recent_avg_filtered_rate": trend_summary.get("recent_avg_filtered_rate"),
        "recent_avg_active_suspicious_link_rate": trend_summary.get("recent_avg_active_suspicious_link_rate"),
        "recent_avg_active_other_rate": trend_summary.get("recent_avg_active_other_rate"),
        "window_origins": trend_summary.get("window_origin_totals", {}),
        "recent_window_origins": trend_summary.get("recent_window_origin_totals", {}),
        "window_origin_rates": {
            "fresh_sync_share": (
                trend_summary.get("window_origin_totals", {}).get("fresh_sync_windows", 0) / total_validation_windows
                if total_validation_windows
                else None
            ),
            "archive_reuse_share": (
                trend_summary.get("window_origin_totals", {}).get("archive_reuse_windows", 0) / total_validation_windows
                if total_validation_windows
                else None
            ),
            "failed_share": (
                trend_summary.get("window_origin_totals", {}).get("failed_windows", 0) / total_validation_windows
                if total_validation_windows
                else None
            ),
        },
        "failed_validation_windows": failed_validation_windows,
        "total_validation_windows": total_validation_windows,
        "validation_run_failure_rate": (failed_validation_windows / total_validation_windows) if total_validation_windows else None,
        "zero_event_windows": zero_event_windows,
        "quota_pressure_count": quota_pressure_count,
        "recent_green_streak_origin": green_streak_origin,
        "source_tier_distribution_trend": source_rollups,
        "event_type_trend": event_type_rollups,
        "event_subtype_trend": event_subtype_rollups,
        "capital_sandbox_recent": recent_capital.to_dict(orient="records") if not recent_capital.empty else [],
        "capital_path_leaderboard": capital_path_leaderboard,
        "capital_compare_leaderboard": capital_compare_leaderboard,
        "capital_refresh_efficiency": {
            "attempts": refresh_attempts,
            "successes": refresh_successes,
            "errors": refresh_errors,
            "skips": refresh_skips,
            "success_rate": (refresh_successes / refresh_attempts) if refresh_attempts else None,
        },
        "latest_operator_summary": latest_operator_summary or {},
        "validation_trend_summary": trend_summary,
    }
    return validation_runs, summary

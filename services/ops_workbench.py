from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from operations.ops_analytics import build_ops_analytics, collect_capital_sandbox_runs, collect_watchlist_runs
from operations.operator_summary import load_json
from services.pathing import PROJECT_ROOT


def _latest_run_dir(root: Path) -> Path | None:
    candidates = sorted(path for path in root.glob("*") if path.is_dir())
    return candidates[-1] if candidates else None


def _load_latest_json(root: Path, filename: str) -> dict[str, Any] | None:
    run_dir = _latest_run_dir(root)
    if run_dir is None:
        return None
    path = run_dir / filename
    if not path.exists():
        return None
    return load_json(path)


def build_overview_payload(project_root: str | Path | None = None) -> dict[str, Any]:
    root = Path(project_root) if project_root else PROJECT_ROOT
    latest_operator_summary = _load_latest_json(root / "output" / "operator_summary", "operator_summary.json")
    latest_validation_summary = _load_latest_json(root / "output" / "live_validation", "validation_summary.json")
    latest_validation_governance = _load_latest_json(root / "output" / "live_validation_governance", "live_validation_governance.json")
    latest_trend_governance = _load_latest_json(root / "output" / "validation_trend_governance", "validation_trend_governance.json")
    latest_capital_status = _load_latest_json(root / "output" / "capital_sandbox", "live_session_status.json")
    latest_ops_analytics = _load_latest_json(root / "output" / "ops_analytics", "ops_analytics_summary.json")
    latest_watchlist_dir = _latest_run_dir(root / "output" / "live_marketaux_watchlist")

    watchlist_summary = pd.DataFrame()
    watchlist_events = pd.DataFrame()
    if latest_watchlist_dir is not None:
        manifest_path = latest_watchlist_dir / "live_marketaux_manifest.json"
        if not manifest_path.exists():
            manifest_path = latest_watchlist_dir / "watchlist_manifest.json"
        if manifest_path.exists():
            manifest = load_json(manifest_path)
            outputs = manifest.get("outputs", {})
            if outputs.get("summary_csv") and Path(outputs["summary_csv"]).exists():
                watchlist_summary = pd.read_csv(outputs["summary_csv"])
            if outputs.get("events_csv") and Path(outputs["events_csv"]).exists():
                watchlist_events = pd.read_csv(outputs["events_csv"])

    return {
        "latest_operator_summary": latest_operator_summary or {},
        "latest_validation_summary": latest_validation_summary or {},
        "latest_validation_governance": latest_validation_governance or {},
        "latest_trend_governance": latest_trend_governance or {},
        "latest_capital_status": latest_capital_status or {},
        "latest_ops_analytics": latest_ops_analytics or {},
        "latest_watchlist_summary": watchlist_summary,
        "latest_watchlist_events": watchlist_events,
    }


def run_ops_analytics_workbench(
    *,
    project_root: str | Path | None = None,
    recent_runs: int = 5,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(project_root) if project_root else PROJECT_ROOT
    validation_runs, summary = build_ops_analytics(project_root=root, recent_runs=recent_runs)
    watchlist_runs = collect_watchlist_runs(root / "output" / "live_marketaux_watchlist")
    capital_runs = collect_capital_sandbox_runs(root / "output" / "capital_sandbox")
    path_leaderboard = pd.DataFrame(summary.get("capital_path_leaderboard", []))
    result = {
        "runs_frame": validation_runs,
        "watchlist_runs_frame": watchlist_runs,
        "capital_runs_frame": capital_runs,
        "path_leaderboard": path_leaderboard,
        "summary": summary,
        "output_root": None,
        "report_markdown": None,
        "outputs": {},
    }

    lines = [
        "# Ops Analytics",
        "",
        f"Latest watchlist run: `{summary['latest_watchlist_run_id']}`",
        f"Latest validation run: `{summary['latest_validation_run_id']}`",
        f"Latest capital sandbox run: `{summary['latest_capital_sandbox_run_id']}`",
        f"Live validation governance: `{summary['latest_governance_state']['live_validation']}`",
        f"Trend governance: `{summary['latest_governance_state']['trend_governance']}`",
        f"Clean pass streak: `{summary['clean_pass_streak']}`",
        "",
        "## Validation Windows",
        "",
        f"- fresh windows: `{summary['window_origins'].get('fresh_sync_windows', 0)}`",
        f"- archive reuse windows: `{summary['window_origins'].get('archive_reuse_windows', 0)}`",
        f"- failed windows: `{summary['window_origins'].get('failed_windows', 0)}`",
        f"- fresh share: `{summary['window_origin_rates'].get('fresh_sync_share')}`",
        f"- archive share: `{summary['window_origin_rates'].get('archive_reuse_share')}`",
        f"- zero-event windows: `{summary['zero_event_windows']}`",
        f"- quota pressure count: `{summary['quota_pressure_count']}`",
        f"- validation run failure rate: `{summary['validation_run_failure_rate']}`",
        f"- recent green streak origin: `{summary['recent_green_streak_origin']}`",
        "",
        "## Recent Rates",
        "",
        f"- recent avg filtered rate: `{summary['recent_avg_filtered_rate']}`",
        f"- recent avg active suspicious-link rate: `{summary['recent_avg_active_suspicious_link_rate']}`",
        f"- recent avg active other rate: `{summary['recent_avg_active_other_rate']}`",
        "",
        "## Capital Sandbox",
        "",
    ]
    recent_capital_runs = summary.get("capital_sandbox_recent", [])
    if recent_capital_runs:
        latest_capital = recent_capital_runs[-1]
        lines.extend(
            [
                f"- latest status: `{latest_capital.get('status')}`",
                f"- run kind: `{latest_capital.get('run_kind')}`",
                f"- best path: `{latest_capital.get('best_path')}`",
                f"- final capital: `{latest_capital.get('final_capital')}`",
                f"- refreshes: `{latest_capital.get('news_refresh_successes')}` success / `{latest_capital.get('news_refresh_errors')}` error",
                f"- refresh skips: `{latest_capital.get('news_refresh_skipped')}`",
                f"- quant blocked steps: `{latest_capital.get('quant_blocked_steps')}`",
                f"- path blocked steps: `{latest_capital.get('path_blocked_steps')}`",
                "",
            ]
        )
    else:
        lines.extend(["- No capital sandbox history found.", "",])

    lines.extend(
        [
            "## Capital Refresh Efficiency",
            "",
            f"- attempts: `{summary['capital_refresh_efficiency'].get('attempts')}`",
            f"- successes: `{summary['capital_refresh_efficiency'].get('successes')}`",
            f"- errors: `{summary['capital_refresh_efficiency'].get('errors')}`",
            f"- skips: `{summary['capital_refresh_efficiency'].get('skips')}`",
            f"- success rate: `{summary['capital_refresh_efficiency'].get('success_rate')}`",
            "",
            "## Capital Path Leaderboard",
            "",
        ]
    )
    for row in summary.get("capital_path_leaderboard", [])[:10]:
        lines.append(
            f"- `{row['path_name']}` runs `{row['run_count']}` avg final capital `{row['avg_final_capital']}`"
        )
    lines.extend(["", "## Capital Compare Leaderboard", ""])
    for row in summary.get("capital_compare_leaderboard", [])[:10]:
        lines.append(
            f"- `{row['path_name']}` runs `{row['run_count']}` avg final capital `{row['avg_final_capital']}`"
        )

    lines.extend([
        "## Source Tier Trend",
        "",
    ])
    for row in summary["source_tier_distribution_trend"][:10]:
        lines.append(f"- `{row['source_tier']}` rows `{row['event_rows']}`")
    lines.extend(["", "## Event Type Trend", ""])
    for row in summary["event_type_trend"][:10]:
        lines.append(f"- `{row['event_type']}` rows `{row['event_rows']}`")
    lines.extend(["", "## Event Subtype Trend", ""])
    for row in summary["event_subtype_trend"][:10]:
        lines.append(f"- `{row['event_subtype']}` rows `{row['event_rows']}`")
    result["report_markdown"] = "\n".join(lines) + "\n"

    if output_dir is not None:
        run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
        out_root = Path(output_dir) / run_id
        out_root.mkdir(parents=True, exist_ok=True)
        outputs = {
            "runs_csv": out_root / "ops_analytics_runs.csv",
            "watchlist_runs_csv": out_root / "ops_analytics_watchlist_runs.csv",
            "capital_runs_csv": out_root / "ops_analytics_capital_runs.csv",
            "path_leaderboard_csv": out_root / "ops_analytics_path_leaderboard.csv",
            "summary_json": out_root / "ops_analytics_summary.json",
            "report_md": out_root / "ops_analytics_report.md",
        }
        validation_runs.to_csv(outputs["runs_csv"], index=False)
        watchlist_runs.to_csv(outputs["watchlist_runs_csv"], index=False)
        capital_runs.to_csv(outputs["capital_runs_csv"], index=False)
        path_leaderboard.to_csv(outputs["path_leaderboard_csv"], index=False)
        with outputs["summary_json"].open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)
        with outputs["report_md"].open("w", encoding="utf-8") as handle:
            handle.write(result["report_markdown"])
        result["output_root"] = str(out_root)
        result["outputs"] = {key: str(path) for key, path in outputs.items()}
    return result

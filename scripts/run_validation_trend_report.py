from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.validation_trends import (
    collect_validation_governance,
    collect_validation_runs,
    summarize_validation_trends,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aggregate multiple live-validation runs into a trend report.")
    parser.add_argument(
        "--validation-root",
        default=str(PROJECT_ROOT / "output" / "live_validation"),
        help="Directory containing live_validation runs.",
    )
    parser.add_argument(
        "--governance-root",
        default=str(PROJECT_ROOT / "output" / "live_validation_governance"),
        help="Directory containing live_validation_governance runs.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "validation_trends"),
        help="Directory for trend artifacts.",
    )
    parser.add_argument(
        "--promotion-scope",
        default="live",
        help="Validation scope to aggregate (`live` or `backfill`).",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    validation_runs = collect_validation_runs(args.validation_root, promotion_scope=args.promotion_scope)
    governance_runs = collect_validation_governance(args.governance_root)
    trend_summary = summarize_validation_trends(validation_runs, governance_runs)

    if not validation_runs.empty and not governance_runs.empty:
        validation_runs = validation_runs.merge(
            governance_runs,
            left_on="run_dir",
            right_on="validation_run",
            how="left",
        )

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    runs_csv = output_root / "validation_trend_runs.csv"
    summary_json = output_root / "validation_trend_summary.json"
    report_md = output_root / "validation_trend_report.md"

    validation_runs.to_csv(runs_csv, index=False)
    with summary_json.open("w", encoding="utf-8") as handle:
        json.dump(trend_summary, handle, indent=2)

    lines = [
        "# Validation Trend Report",
        "",
        f"Runs: `{trend_summary['n_runs']}`",
        f"Latest run: `{trend_summary['latest_run_id']}`",
        f"Latest governance: `{trend_summary['latest_governance_status']}`",
        f"Total events across runs: `{trend_summary['total_events']}`",
        f"Total gap samples: `{trend_summary['total_gap_samples']}`",
        "",
        "## Health",
        "",
        f"- avg active other rate: `{trend_summary['avg_active_other_rate']}`",
        f"- avg active suspicious-link rate: `{trend_summary['avg_active_suspicious_link_rate']}`",
        f"- recent avg active other rate: `{trend_summary['recent_avg_active_other_rate']}`",
        f"- recent avg active suspicious-link rate: `{trend_summary['recent_avg_active_suspicious_link_rate']}`",
        f"- recent avg filtered rate: `{trend_summary['recent_avg_filtered_rate']}`",
        f"- recent avg watchlist eligible rate: `{trend_summary['recent_avg_watchlist_eligible_rate']}`",
        "",
        "## Window Origins",
        "",
        f"- total fresh sync windows: `{trend_summary['window_origin_totals']['fresh_sync_windows']}`",
        f"- total archive reuse windows: `{trend_summary['window_origin_totals']['archive_reuse_windows']}`",
        f"- total failed windows: `{trend_summary['window_origin_totals']['failed_windows']}`",
        f"- total quota blocked windows: `{trend_summary['window_origin_totals']['quota_blocked_windows']}`",
        f"- recent fresh sync windows: `{trend_summary['recent_window_origin_totals']['fresh_sync_windows']}`",
        f"- recent archive reuse windows: `{trend_summary['recent_window_origin_totals']['archive_reuse_windows']}`",
        f"- recent failed windows: `{trend_summary['recent_window_origin_totals']['failed_windows']}`",
        "",
        "## Origin-Separated Rates",
        "",
        f"- fresh avg active other rate: `{trend_summary['fresh_sync_metrics'].get('avg_active_other_rate')}`",
        f"- fresh avg active suspicious-link rate: `{trend_summary['fresh_sync_metrics'].get('avg_active_suspicious_link_rate')}`",
        f"- reuse avg active other rate: `{trend_summary['archive_reuse_metrics'].get('avg_active_other_rate')}`",
        f"- reuse avg active suspicious-link rate: `{trend_summary['archive_reuse_metrics'].get('avg_active_suspicious_link_rate')}`",
        "",
        "## Drift",
        "",
        f"- other rate drift: `{trend_summary['drift_other_rate']}`",
        f"- filtered rate drift: `{trend_summary['drift_filtered_rate']}`",
        f"- watchlist eligible drift: `{trend_summary['drift_watchlist_eligible_rate']}`",
        f"- active other rate drift: `{trend_summary['drift_active_other_rate']}`",
        f"- active suspicious-link rate drift: `{trend_summary['drift_active_suspicious_link_rate']}`",
    ]
    if not validation_runs.empty:
        lines.extend(["", "## Runs", ""])
        for _, row in validation_runs.sort_values("run_id").iterrows():
            lines.append(
                f"- `{row['run_id']}` | events `{row['total_events']}` | eligible `{row['avg_watchlist_eligible_rate']}` | "
                f"other `{row['avg_other_rate']}` | fresh `{row.get('fresh_sync_windows', 0)}` | "
                f"reuse `{row.get('archive_reuse_windows', 0)}` | failed `{row.get('failed_windows', 0)}` | "
                f"governance `{row.get('governance_status')}`"
            )
    with report_md.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")

    print("[OK] Validation trend report completed.")
    print(f"Runs: {trend_summary['n_runs']}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from operations.operator_summary import build_operator_summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a compact operator summary from recent watchlist and validation runs.")
    parser.add_argument(
        "--watchlist-run",
        default="",
        help="Path to a live_marketaux_watchlist or watchlist run. Defaults to the latest live_marketaux_watchlist run.",
    )
    parser.add_argument(
        "--validation-run",
        default="",
        help="Optional live_validation run. Defaults to the latest one.",
    )
    parser.add_argument(
        "--validation-governance-run",
        default="",
        help="Optional live_validation_governance run. Defaults to the latest one.",
    )
    parser.add_argument(
        "--trend-governance-run",
        default="",
        help="Optional validation_trend_governance run. Defaults to the latest one.",
    )
    parser.add_argument(
        "--capital-sandbox-run",
        default="",
        help="Optional capital_sandbox run. Defaults to the latest one if available.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "operator_summary"),
        help="Output directory for operator summary artifacts.",
    )
    return parser


def _resolve_latest_run(root: Path) -> Path:
    candidates = sorted(path for path in root.glob("*") if path.is_dir())
    if not candidates:
        raise FileNotFoundError(f"No runs found under {root}")
    return candidates[-1]


def _resolve_latest_run_optional(root: Path) -> Path | None:
    candidates = sorted(path for path in root.glob("*") if path.is_dir())
    return candidates[-1] if candidates else None


def main() -> int:
    args = _build_parser().parse_args()
    watchlist_run = Path(args.watchlist_run) if args.watchlist_run else _resolve_latest_run(PROJECT_ROOT / "output" / "live_marketaux_watchlist")
    validation_run = Path(args.validation_run) if args.validation_run else _resolve_latest_run(PROJECT_ROOT / "output" / "live_validation")
    validation_governance_run = Path(args.validation_governance_run) if args.validation_governance_run else _resolve_latest_run(PROJECT_ROOT / "output" / "live_validation_governance")
    trend_governance_run = Path(args.trend_governance_run) if args.trend_governance_run else _resolve_latest_run(PROJECT_ROOT / "output" / "validation_trend_governance")
    capital_sandbox_run = Path(args.capital_sandbox_run) if args.capital_sandbox_run else _resolve_latest_run_optional(PROJECT_ROOT / "output" / "capital_sandbox")

    summary = build_operator_summary(
        watchlist_run=watchlist_run,
        validation_run=validation_run,
        validation_governance_run=validation_governance_run,
        trend_governance_run=trend_governance_run,
        capital_sandbox_run=capital_sandbox_run,
    )

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    summary_path = output_root / "operator_summary.json"
    report_path = output_root / "operator_summary.md"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    lines = [
        "# Operator Summary",
        "",
        f"Watchlist run: `{summary['watchlist_run']}`",
        f"Run origin: `{summary['watchlist_origin']}`",
        f"Live validation governance: `{summary['governance']['live_validation_status']}`",
        f"Trend governance: `{summary['governance']['trend_status']}`",
        "",
        "## Ops",
        "",
        f"- quota pressure: `{summary['ops']['quota_pressure']}`",
        f"- run log errors: `{summary['ops']['run_log_errors']}`",
        f"- reused validation windows: `{summary['ops']['reused_window_count']}`",
        f"- failed validation windows: `{summary['ops']['failed_window_count']}`",
        f"- zero-event validation windows: `{summary['ops']['zero_event_windows']}`",
        "",
        "## Validation Freshness",
        "",
        f"- fresh windows: `{summary['validation']['fresh_sync_windows']}`",
        f"- archive reuse windows: `{summary['validation']['archive_reuse_windows']}`",
        f"- failed windows: `{summary['validation']['failed_windows']}`",
        f"- quota blocked windows: `{summary['validation']['quota_blocked_windows']}`",
        f"- fresh sync dominant: `{summary['validation']['fresh_sync_dominant']}`",
        "",
        "## Capital Sandbox",
        "",
    ]

    capital_sandbox = summary.get("capital_sandbox", {})
    if capital_sandbox:
        lines.extend(
            [
                f"- run: `{capital_sandbox.get('run')}`",
                f"- status: `{capital_sandbox.get('status')}`",
                f"- best path: `{capital_sandbox.get('best_path')}`",
                f"- final capital: `{capital_sandbox.get('final_capital')}`",
                f"- news refreshes: `{capital_sandbox.get('news_refresh_successes')}` success / `{capital_sandbox.get('news_refresh_errors')}` error",
                f"- news refresh skips: `{capital_sandbox.get('news_refresh_skipped')}`",
                f"- quant blocked steps: `{capital_sandbox.get('quant_blocked_steps')}`",
                f"- stale price steps: `{capital_sandbox.get('stale_price_steps')}`",
                "",
            ]
        )
    else:
        lines.extend(["- No capital sandbox run resolved.", ""])

    capital_compare = summary.get("capital_compare", {})
    lines.extend(["## Capital Compare", ""])
    if capital_compare:
        lines.extend(
            [
                f"- compare run: `{capital_compare.get('run')}`",
                f"- best session: `{capital_compare.get('overall_best_session')}`",
                f"- best path: `{capital_compare.get('overall_best_path')}`",
                f"- final capital: `{capital_compare.get('overall_best_final_capital')}`",
                "",
            ]
        )
        for row in capital_compare.get("best_by_session", [])[:5]:
            lines.append(
                f"- `{row.get('session_label')}` best `{row.get('path_name')}` final `{row.get('final_capital')}`"
            )
        lines.append("")
    else:
        lines.extend(["- No capital compare run resolved.", ""])

    lines.extend(
        [
        "## Top Portfolios",
        "",
        ]
    )

    for row in summary["top_portfolios"][:5]:
        lines.append(
            f"- `{row['portfolio_id']}` delta VaR `{row['max_delta_normal_var_loss_1d_99']}` | "
            f"top event `{row['top_event_type']}` | why `{row.get('why_ranked')}`"
        )

    lines.extend(["", "## Top Events", ""])
    for row in summary["top_events"][:5]:
        lines.append(
            f"- `{row['portfolio_id']}` | `{row['event_type']}` | `{row.get('event_subtype')}` | "
            f"`{row.get('source_tier')}` | delta VaR `{row['delta_normal_var_loss_1d_99']}` | "
            f"why `{row.get('why_ranked')}` | {row['headline']}"
        )

    lines.extend(["", "## Rollups", ""])
    for section, rows in summary["rollups"].items():
        lines.append(f"- `{section}`:")
        for row in rows[:5]:
            label = row.get(section, "unknown")
            lines.append(
                f"  - `{label}` rows `{row['event_rows']}` portfolios `{row['unique_portfolios']}` max delta `{row['max_delta_var']}`"
            )

    with report_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")

    print("[OK] Operator summary completed.")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

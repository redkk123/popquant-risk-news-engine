from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.run_logging import append_run_event, write_failure_manifest
from event_engine.validation_backfill import (
    build_backfill_as_of_dates,
    load_suite_result,
    summarize_backfill_runs,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run multiple live-validation suites to accumulate governed history.")
    parser.add_argument(
        "--start-as-of",
        default=pd.Timestamp.today(tz="UTC").date().isoformat(),
        help="Newest as-of date to run.",
    )
    parser.add_argument(
        "--end-as-of",
        required=True,
        help="Oldest as-of date to run.",
    )
    parser.add_argument("--cadence-days", type=int, default=1, help="Days between successive as-of runs.")
    parser.add_argument("--windows", type=int, default=2, help="Number of windows per suite run.")
    parser.add_argument("--window-days", type=int, default=3, help="Window width in days.")
    parser.add_argument("--step-days", type=int, default=2, help="Gap between validation windows.")
    parser.add_argument("--symbols", nargs="+", default=None, help="Ticker symbols to query.")
    parser.add_argument(
        "--symbols-config",
        default=str(PROJECT_ROOT / "config" / "validation" / "live_validation_universe.yaml"),
        help="Optional YAML symbol universe config.",
    )
    parser.add_argument("--language", default="en", help="Language filter.")
    parser.add_argument("--limit", type=int, default=3, help="Articles per page.")
    parser.add_argument("--max-pages", type=int, default=2, help="Maximum pages fetched per validation window.")
    parser.add_argument(
        "--watchlist-config",
        default=str(PROJECT_ROOT / "config" / "watchlists" / "validation_watchlist.yaml"),
        help="Watchlist config forwarded to the suite runner.",
    )
    parser.add_argument("--event-map-config", default="", help="Optional event map override.")
    parser.add_argument(
        "--suite-output-dir",
        default=str(PROJECT_ROOT / "output" / "backfill_workspace" / "live_validation_suite"),
        help="Root directory for suite runs.",
    )
    parser.add_argument(
        "--validation-output-dir",
        default=str(PROJECT_ROOT / "output" / "backfill_workspace" / "live_validation"),
        help="Root directory for validation runs.",
    )
    parser.add_argument(
        "--governance-output-dir",
        default=str(PROJECT_ROOT / "output" / "backfill_workspace" / "live_validation_governance"),
        help="Root directory for validation-governance runs.",
    )
    parser.add_argument(
        "--trend-output-dir",
        default=str(PROJECT_ROOT / "output" / "backfill_workspace" / "validation_trends"),
        help="Root directory for validation-trend runs.",
    )
    parser.add_argument(
        "--trend-governance-output-dir",
        default=str(PROJECT_ROOT / "output" / "backfill_workspace" / "validation_trend_governance"),
        help="Root directory for validation-trend-governance runs.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "live_validation_backfill"),
        help="Root directory for backfill artifacts.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running later as-of dates if one suite fails.",
    )
    return parser


def _json_default(value: Any):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value)} is not JSON serializable")


def _parse_output_path(stdout: str) -> Path | None:
    for line in reversed(stdout.splitlines()):
        if line.startswith("Output: "):
            return Path(line.split("Output: ", 1)[1].strip())
    return None


def main() -> int:
    args = _build_parser().parse_args()
    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    run_log_path = output_root / "run_log.jsonl"

    as_of_dates = build_backfill_as_of_dates(
        start_as_of=args.start_as_of,
        end_as_of=args.end_as_of,
        cadence_days=args.cadence_days,
    )
    append_run_event(
        run_log_path,
        stage="backfill",
        status="start",
        details={"as_of_dates": as_of_dates},
    )

    rows: list[dict[str, Any]] = []
    suite_runner = PROJECT_ROOT / "scripts" / "run_live_validation_suite.py"
    try:
        for as_of_date in as_of_dates:
            command = [
                sys.executable,
                str(suite_runner),
                "--as-of",
                as_of_date,
                "--windows",
                str(args.windows),
                "--window-days",
                str(args.window_days),
                "--step-days",
                str(args.step_days),
                "--symbols-config",
                args.symbols_config,
                "--language",
                args.language,
                "--limit",
                str(args.limit),
                "--max-pages",
                str(args.max_pages),
                "--watchlist-config",
                args.watchlist_config,
                "--output-dir",
                args.suite_output_dir,
                "--validation-output-dir",
                args.validation_output_dir,
                "--governance-output-dir",
                args.governance_output_dir,
                "--trend-output-dir",
                args.trend_output_dir,
                "--trend-governance-output-dir",
                args.trend_governance_output_dir,
                "--promotion-scope",
                "backfill",
            ]
            if args.symbols:
                command.extend(["--symbols", *args.symbols])
            if args.event_map_config:
                command.extend(["--event-map-config", args.event_map_config])

            append_run_event(run_log_path, stage=as_of_date, status="start", details={"command": command})
            completed = subprocess.run(command, check=False, capture_output=True, text=True, cwd=PROJECT_ROOT)
            suite_run = _parse_output_path(completed.stdout)
            if completed.returncode != 0 or suite_run is None:
                row = {
                    "as_of": as_of_date,
                    "suite_status": "error",
                    "returncode": int(completed.returncode),
                    "stderr": completed.stderr.strip(),
                    "stdout": completed.stdout.strip(),
                    "suite_run": str(suite_run) if suite_run else None,
                }
                rows.append(row)
                append_run_event(
                    run_log_path,
                    stage=as_of_date,
                    status="error",
                    message=completed.stderr.strip() or "suite failed",
                    details={"returncode": int(completed.returncode)},
                )
                if not args.continue_on_error:
                    raise RuntimeError(f"Suite run failed for as-of {as_of_date}")
                continue

            suite_result = load_suite_result(suite_run)
            row = {
                "as_of": as_of_date,
                "suite_status": "success",
                **suite_result,
            }
            rows.append(row)
            append_run_event(
                run_log_path,
                stage=as_of_date,
                status="success",
                details={
                    "suite_run": str(suite_run),
                    "validation_status": row.get("validation_status"),
                    "trend_status": row.get("trend_status"),
                },
            )

        frame = pd.DataFrame(rows)
        summary = summarize_backfill_runs(frame)

        runs_csv = output_root / "backfill_runs.csv"
        summary_json = output_root / "backfill_summary.json"
        report_md = output_root / "backfill_report.md"

        frame.to_csv(runs_csv, index=False)
        with summary_json.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, default=_json_default)

        lines = [
            "# Live Validation Backfill Report",
            "",
            f"Requested runs: `{summary['requested_runs']}`",
            f"Successful suite runs: `{summary['successful_runs']}`",
            f"Validation PASS count: `{summary['validation_pass_count']}`",
            f"Trend PASS count: `{summary['trend_pass_count']}`",
            f"Latest suite run: `{summary['latest_suite_run']}`",
            "",
            "## Aggregate Metrics",
            "",
            f"- avg watchlist eligible rate: `{summary['avg_watchlist_eligible_rate']}`",
            f"- avg filtered rate: `{summary['avg_filtered_rate']}`",
            f"- avg other rate: `{summary['avg_other_rate']}`",
            f"- avg suspicious link rate: `{summary['avg_suspicious_link_rate']}`",
            f"- avg active other rate: `{summary['avg_active_other_rate']}`",
            f"- avg active suspicious-link rate: `{summary['avg_active_suspicious_link_rate']}`",
            "",
            "## Runs",
            "",
        ]
        for record in frame.to_dict(orient="records"):
            lines.append(
                f"- `{record.get('as_of')}` | suite `{record.get('suite_status')}` | "
                f"validation `{record.get('validation_status')}` | trend `{record.get('trend_status')}` | "
                f"events `{record.get('total_events')}`"
            )
        with report_md.open("w", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")

        append_run_event(
            run_log_path,
            stage="backfill",
            status="success",
            details={"summary_json": str(summary_json), "runs_csv": str(runs_csv)},
        )
        print("[OK] Live validation backfill completed.")
        print(f"Runs: {summary['successful_runs']}/{summary['requested_runs']}")
        print(f"Output: {output_root}")
        return 0
    except Exception as error:
        append_run_event(run_log_path, stage="backfill", status="error", message=str(error))
        write_failure_manifest(output_root=output_root, stage="backfill", error=error, log_path=run_log_path)
        raise


if __name__ == "__main__":
    raise SystemExit(main())

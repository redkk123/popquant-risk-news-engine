from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.run_logging import append_run_event, write_failure_manifest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the full live-validation suite and promotion gate.")
    parser.add_argument("--validation-run", default="", help="Reuse a specific live_validation run.")
    parser.add_argument("--skip-validation", action="store_true", help="Skip creating a new live_validation run.")
    parser.add_argument("--windows", type=int, default=3, help="Number of windows to evaluate.")
    parser.add_argument("--window-days", type=int, default=3, help="Window width in days.")
    parser.add_argument("--step-days", type=int, default=2, help="Gap between consecutive windows in days.")
    parser.add_argument(
        "--as-of",
        default=pd.Timestamp.today(tz="UTC").date().isoformat(),
        help="Anchor date for the most recent validation window.",
    )
    parser.add_argument("--symbols", nargs="+", default=None, help="Ticker symbols to query.")
    parser.add_argument(
        "--symbols-config",
        default=str(PROJECT_ROOT / "config" / "validation" / "live_validation_universe.yaml"),
        help="Optional YAML symbol-universe config used when --symbols is not provided.",
    )
    parser.add_argument(
        "--symbol-pack",
        default="",
        help="Optional thematic pack name forwarded to live_validation.",
    )
    parser.add_argument("--language", default="en", help="Language filter.")
    parser.add_argument(
        "--providers",
        nargs="*",
        default=["marketaux", "thenewsapi", "alphavantage"],
        help="Ordered providers forwarded to live_validation.",
    )
    parser.add_argument("--limit", type=int, default=3, help="Articles per page.")
    parser.add_argument("--max-pages", type=int, default=2, help="Maximum pages fetched per window.")
    parser.add_argument(
        "--symbol-batch-size",
        type=int,
        default=5,
        help="Maximum symbols per upstream provider query batch.",
    )
    parser.add_argument(
        "--watchlist-config",
        default=str(PROJECT_ROOT / "config" / "watchlists" / "validation_watchlist.yaml"),
        help="Watchlist config forwarded to the live runner.",
    )
    parser.add_argument("--event-map-config", default="", help="Optional event map override forwarded to the live runner.")
    parser.add_argument(
        "--validation-output-dir",
        default=str(PROJECT_ROOT / "output" / "live_validation"),
        help="Root directory for live_validation runs.",
    )
    parser.add_argument(
        "--governance-output-dir",
        default=str(PROJECT_ROOT / "output" / "live_validation_governance"),
        help="Root directory for live_validation_governance runs.",
    )
    parser.add_argument(
        "--trend-output-dir",
        default=str(PROJECT_ROOT / "output" / "validation_trends"),
        help="Root directory for validation_trends runs.",
    )
    parser.add_argument(
        "--trend-governance-output-dir",
        default=str(PROJECT_ROOT / "output" / "validation_trend_governance"),
        help="Root directory for validation_trend_governance runs.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "live_validation_suite"),
        help="Output directory for suite artifacts.",
    )
    parser.add_argument(
        "--promotion-scope",
        default="live",
        help="Scope label for downstream validation and trend artifacts (`live` or `backfill`).",
    )
    parser.add_argument(
        "--archive-only",
        action="store_true",
        help="Skip live sync and reuse archived validation windows only.",
    )
    return parser


def _resolve_latest_run(root: str | Path) -> Path:
    candidates = sorted(path for path in Path(root).glob("*") if path.is_dir())
    if not candidates:
        raise FileNotFoundError(f"No runs found under {root}")
    return candidates[-1]


def _parse_output_path(stdout: str) -> Path | None:
    for line in reversed(stdout.splitlines()):
        if line.startswith("Output: "):
            return Path(line.split("Output: ", 1)[1].strip())
    return None


def _run_stage(stage: str, command: list[str], run_log_path: Path) -> Path:
    append_run_event(run_log_path, stage=stage, status="start", details={"command": command})
    completed = subprocess.run(command, check=False, capture_output=True, text=True, cwd=PROJECT_ROOT)
    if completed.returncode != 0:
        append_run_event(
            run_log_path,
            stage=stage,
            status="error",
            message=completed.stderr.strip() or "stage failed",
            details={"returncode": completed.returncode, "stdout": completed.stdout.strip()},
        )
        raise RuntimeError(
            f"{stage} failed with return code {completed.returncode}: {completed.stderr.strip() or completed.stdout.strip()}"
        )

    output_path = _parse_output_path(completed.stdout)
    append_run_event(
        run_log_path,
        stage=stage,
        status="success",
        details={"output": str(output_path) if output_path else None},
    )
    return output_path if output_path is not None else Path()


def main() -> int:
    args = _build_parser().parse_args()
    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    run_log_path = output_root / "run_log.jsonl"

    try:
        validation_run = Path(args.validation_run) if args.validation_run else None
        if validation_run is None and not args.skip_validation:
            command = [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "run_live_validation.py"),
                "--windows",
                str(args.windows),
                "--window-days",
                str(args.window_days),
                "--step-days",
                str(args.step_days),
                "--as-of",
                args.as_of,
                "--symbols-config",
                args.symbols_config,
                "--symbol-pack",
                args.symbol_pack,
                "--language",
                args.language,
                "--providers",
                *args.providers,
                "--limit",
                str(args.limit),
                "--max-pages",
                str(args.max_pages),
                "--symbol-batch-size",
                str(args.symbol_batch_size),
                "--watchlist-config",
                args.watchlist_config,
                "--output-dir",
                args.validation_output_dir,
                "--promotion-scope",
                args.promotion_scope,
            ]
            if args.symbols:
                command.extend(["--symbols", *args.symbols])
            if args.event_map_config:
                command.extend(["--event-map-config", args.event_map_config])
            if args.archive_only:
                command.append("--archive-only")
            validation_run = _run_stage("live_validation", command, run_log_path)

        if validation_run is None:
            validation_run = _resolve_latest_run(args.validation_output_dir)
            append_run_event(
                run_log_path,
                stage="live_validation",
                status="reused",
                details={"validation_run": str(validation_run)},
            )

        validation_governance_run = _run_stage(
            "live_validation_governance",
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "run_live_validation_governance.py"),
                "--validation-run",
                str(validation_run),
                "--output-dir",
                args.governance_output_dir,
            ],
            run_log_path,
        )

        trend_run = _run_stage(
            "validation_trend_report",
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "run_validation_trend_report.py"),
                "--validation-root",
                args.validation_output_dir,
                "--governance-root",
                args.governance_output_dir,
                "--output-dir",
                args.trend_output_dir,
                "--promotion-scope",
                args.promotion_scope,
            ],
            run_log_path,
        )

        trend_governance_run = _run_stage(
            "validation_trend_governance",
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "run_validation_trend_governance.py"),
                "--trend-run",
                str(trend_run),
                "--output-dir",
                args.trend_governance_output_dir,
            ],
            run_log_path,
        )

        manifest_path = output_root / "live_validation_suite_manifest.json"
        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "validation_run": str(validation_run),
                    "validation_governance_run": str(validation_governance_run),
                    "trend_run": str(trend_run),
                    "trend_governance_run": str(trend_governance_run),
                    "promotion_scope": args.promotion_scope,
                    "run_log": str(run_log_path),
                },
                handle,
                indent=2,
            )

        print("[OK] Live validation suite completed.")
        print(f"Output: {output_root}")
        return 0
    except Exception as error:
        append_run_event(
            run_log_path,
            stage="suite",
            status="error",
            message=str(error),
        )
        write_failure_manifest(output_root=output_root, stage="suite", error=error, log_path=run_log_path)
        raise


if __name__ == "__main__":
    raise SystemExit(main())

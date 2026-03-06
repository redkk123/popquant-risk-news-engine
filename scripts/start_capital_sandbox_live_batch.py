from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch multiple real-time capital sandbox sessions in parallel.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Path to the portfolio JSON config.",
    )
    parser.add_argument(
        "--session-minutes",
        nargs="*",
        type=int,
        default=[5, 15, 30],
        help="Real-time session lengths to launch in parallel.",
    )
    parser.add_argument("--initial-capital", type=float, default=100.0, help="Initial simulated capital.")
    parser.add_argument(
        "--decision-interval-seconds",
        type=int,
        default=60,
        help="Decision cadence in seconds. Use 60 for live mode.",
    )
    parser.add_argument(
        "--providers",
        nargs="*",
        default=["marketaux", "thenewsapi", "alphavantage"],
        help="Ordered news providers to try.",
    )
    parser.add_argument(
        "--published-after",
        default=(date.today() - timedelta(days=2)).isoformat(),
        help="Lower date bound for live news sync.",
    )
    parser.add_argument(
        "--published-before",
        default=(date.today() + timedelta(days=1)).isoformat(),
        help="Upper date bound for live news sync.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "capital_sandbox_live_batch"),
        help="Output directory for the batch launcher manifest and session roots.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    launch_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ")
    batch_root = Path(args.output_dir) / launch_id
    batch_root.mkdir(parents=True, exist_ok=True)

    session_rows = []
    for session_minutes in sorted({int(value) for value in args.session_minutes if int(value) > 0}):
        session_root = batch_root / f"{session_minutes}m"
        session_root.mkdir(parents=True, exist_ok=True)
        log_path = session_root / "launcher_stdout.log"
        command = [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "run_capital_sandbox.py"),
            "--mode",
            "live_session_real_time",
            "--portfolio-config",
            str(args.portfolio_config),
            "--initial-capital",
            str(args.initial_capital),
            "--decision-interval-seconds",
            str(args.decision_interval_seconds),
            "--session-minutes",
            str(session_minutes),
            "--published-after",
            str(args.published_after),
            "--published-before",
            str(args.published_before),
            "--output-dir",
            str(session_root),
            "--providers",
            *list(args.providers),
        ]
        with log_path.open("w", encoding="utf-8") as handle:
            popen_kwargs = {
                "cwd": str(PROJECT_ROOT),
                "stdout": handle,
                "stderr": subprocess.STDOUT,
                "stdin": subprocess.DEVNULL,
            }
            creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            if creationflags:
                popen_kwargs["creationflags"] = creationflags
            process = subprocess.Popen(command, **popen_kwargs)

        session_rows.append(
            {
                "session_label": f"{session_minutes}m",
                "session_minutes": int(session_minutes),
                "pid": int(process.pid),
                "output_root": str(session_root),
                "stdout_log": str(log_path),
                "command": command,
            }
        )

    manifest_path = batch_root / "batch_manifest.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "launch_id": launch_id,
                "batch_root": str(batch_root),
                "sessions": session_rows,
            },
            handle,
            indent=2,
        )

    print("[OK] Capital sandbox live batch launched.")
    print(f"Manifest: {manifest_path}")
    for row in session_rows:
        print(f"{row['session_label']} pid={row['pid']} output={row['output_root']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

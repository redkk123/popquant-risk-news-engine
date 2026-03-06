from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from operations.scheduler import (
    DEFAULT_TASK_NAME,
    DEFAULT_TASK_TIME,
    build_schtasks_create_args,
    build_schtasks_delete_args,
    build_schtasks_query_args,
    build_task_runner_command,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the Windows scheduled task for the live watchlist runner.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--task-name", default=DEFAULT_TASK_NAME, help="Windows Task Scheduler task name.")
    common.add_argument("--python-launcher", default="py", help="Python launcher used by the task wrapper.")
    common.add_argument("--lookback-days", type=int, default=3, help="How many days of news the task should look back.")
    common.add_argument("--symbols", nargs="+", default=["AAPL", "MSFT", "SPY"], help="Ticker symbols queried by the task.")
    common.add_argument("--language", default="en", help="Language filter passed to the live runner.")
    common.add_argument("--limit", type=int, default=3, help="Articles per page.")
    common.add_argument("--max-pages", type=int, default=2, help="Maximum pages fetched by the task.")
    common.add_argument("--watchlist-config", default="", help="Optional watchlist config override.")
    common.add_argument("--event-map-config", default="", help="Optional event map override.")
    common.add_argument("--cache-dir", default="", help="Optional price cache override.")
    common.add_argument("--output-dir", default="", help="Optional output directory override.")

    create = subparsers.add_parser("create", parents=[common], help="Create or replace the scheduled task.")
    create.add_argument("--time", default=DEFAULT_TASK_TIME, help="Daily local execution time in HH:MM format.")
    create.add_argument("--print-only", action="store_true", help="Print the command without creating the task.")

    show = subparsers.add_parser("show", help="Show the current scheduled task definition.")
    show.add_argument("--task-name", default=DEFAULT_TASK_NAME, help="Windows Task Scheduler task name.")

    delete = subparsers.add_parser("delete", help="Delete the scheduled task.")
    delete.add_argument("--task-name", default=DEFAULT_TASK_NAME, help="Windows Task Scheduler task name.")
    delete.add_argument("--print-only", action="store_true", help="Print the delete command without executing it.")
    return parser


def _run(command: list[str]) -> int:
    completed = subprocess.run(command, check=False, text=True, capture_output=True)
    if completed.stdout:
        print(completed.stdout.rstrip())
    if completed.stderr:
        print(completed.stderr.rstrip(), file=sys.stderr)
    return completed.returncode


def main() -> int:
    args = _build_parser().parse_args()

    if args.command == "create":
        task_command = build_task_runner_command(
            project_root=PROJECT_ROOT,
            python_launcher=args.python_launcher,
            lookback_days=args.lookback_days,
            symbols=args.symbols,
            language=args.language,
            limit=args.limit,
            max_pages=args.max_pages,
            watchlist_config=args.watchlist_config or None,
            event_map_config=args.event_map_config or None,
            cache_dir=args.cache_dir or None,
            output_dir=args.output_dir or None,
        )
        create_args = build_schtasks_create_args(
            task_name=args.task_name,
            task_time=args.time,
            task_command=task_command,
        )
        print("Task command:")
        print(task_command)
        print("")
        print("schtasks invocation:")
        print(" ".join(create_args))
        if args.print_only:
            return 0
        return _run(create_args)

    if args.command == "show":
        return _run(build_schtasks_query_args(task_name=args.task_name))

    if args.command == "delete":
        delete_args = build_schtasks_delete_args(task_name=args.task_name)
        print("schtasks invocation:")
        print(" ".join(delete_args))
        if args.print_only:
            return 0
        return _run(delete_args)

    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

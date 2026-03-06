from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.ops_workbench import run_ops_analytics_workbench


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build historical operational analytics over watchlist and validation outputs.")
    parser.add_argument(
        "--project-root",
        default=str(PROJECT_ROOT),
        help="Project root used to discover output histories.",
    )
    parser.add_argument(
        "--recent-runs",
        type=int,
        default=5,
        help="Number of most recent runs used for recent trend rollups.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "ops_analytics"),
        help="Directory for ops analytics outputs.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_ops_analytics_workbench(
        project_root=args.project_root,
        recent_runs=args.recent_runs,
        output_dir=args.output_dir,
    )
    print("[OK] Ops analytics completed.")
    print(f"Output: {result['output_root']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

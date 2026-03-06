from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from operations.retention import RetentionPolicy, prune_runs


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply output retention policy across operational run folders.")
    parser.add_argument(
        "--roots",
        nargs="+",
        default=[
            str(PROJECT_ROOT / "output" / "watchlist"),
            str(PROJECT_ROOT / "output" / "watchlist_probe"),
            str(PROJECT_ROOT / "output" / "live_validation"),
            str(PROJECT_ROOT / "output" / "live_validation_suite"),
            str(PROJECT_ROOT / "output" / "operator_summary"),
        ],
        help="Run roots to evaluate for retention.",
    )
    parser.add_argument("--keep-latest", type=int, default=5, help="Always preserve this many latest runs per root.")
    parser.add_argument("--min-age-days", type=int, default=7, help="Do not prune runs newer than this age.")
    parser.add_argument("--apply", action="store_true", help="Delete prunable runs. Defaults to dry-run.")
    parser.add_argument(
        "--output-path",
        default=str(PROJECT_ROOT / "output" / "retention" / "retention_plan.json"),
        help="Path for the retention report JSON.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    policy = RetentionPolicy(keep_latest=args.keep_latest, min_age_days=args.min_age_days)

    report = []
    for root in args.roots:
        pruned = prune_runs(root, policy=policy, dry_run=not args.apply)
        report.append(
            {
                "root": root,
                "dry_run": not args.apply,
                "candidate_count": len(pruned),
                "candidates": [str(path) for path in pruned],
            }
        )

    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    print("[OK] Retention pass completed.")
    print(f"Output: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

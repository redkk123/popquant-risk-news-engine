from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.live_validation import load_json
from event_engine.validation_governance import LiveValidationThresholds, assess_live_validation


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Assess a live-validation run against explicit health thresholds.")
    parser.add_argument(
        "--validation-run",
        default="",
        help="Path to a live_validation run directory. Defaults to the latest run.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "live_validation_governance"),
        help="Output directory for governance artifacts.",
    )
    return parser


def _resolve_latest_validation_run() -> Path:
    root = PROJECT_ROOT / "output" / "live_validation"
    candidates = sorted(path for path in root.glob("*") if path.is_dir())
    if not candidates:
        raise FileNotFoundError("No live_validation runs found.")
    return candidates[-1]


def main() -> int:
    args = _build_parser().parse_args()
    validation_root = Path(args.validation_run) if args.validation_run else _resolve_latest_validation_run()
    summary_payload = load_json(validation_root / "validation_summary.json")
    window_frame = pd.read_csv(validation_root / "validation_window_summary.csv")

    decision = assess_live_validation(
        summary=summary_payload["aggregate"],
        window_frame=window_frame,
        thresholds=LiveValidationThresholds(),
    )

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    decision_path = output_root / "live_validation_governance.json"
    report_path = output_root / "live_validation_governance.md"
    with decision_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "validation_run": str(validation_root),
                "decision": decision,
            },
            handle,
            indent=2,
        )

    lines = [
        "# Live Validation Governance",
        "",
        f"Validation run: `{validation_root}`",
        f"Status: `{decision['status']}`",
        f"Rationale: {decision['rationale']}",
        "",
        "## Metrics",
        "",
    ]
    for key, value in decision["metrics"].items():
        lines.append(f"- `{key}`: `{value}`")
    if decision["findings"]:
        lines.extend(["", "## Findings", ""])
        for finding in decision["findings"]:
            lines.append(
                f"- `{finding['metric']}` actual `{finding['actual']}` must be `{finding['comparator']} {finding['limit']}`"
            )
    with report_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")

    print("[OK] Live validation governance completed.")
    print(f"Status: {decision['status']}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

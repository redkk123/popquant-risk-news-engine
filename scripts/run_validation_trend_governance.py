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
from event_engine.validation_trend_governance import ValidationTrendThresholds, assess_validation_trend


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Assess validation history as a promotion gate.")
    parser.add_argument(
        "--trend-run",
        default="",
        help="Path to a validation_trends run directory. Defaults to the latest run.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "validation_trend_governance"),
        help="Output directory for trend-governance artifacts.",
    )
    return parser


def _resolve_latest_trend_run() -> Path:
    root = PROJECT_ROOT / "output" / "validation_trends"
    candidates = sorted(path for path in root.glob("*") if path.is_dir())
    if not candidates:
        raise FileNotFoundError("No validation_trends runs found.")
    return candidates[-1]


def main() -> int:
    args = _build_parser().parse_args()
    trend_root = Path(args.trend_run) if args.trend_run else _resolve_latest_trend_run()
    trend_summary = load_json(trend_root / "validation_trend_summary.json")
    trend_runs = pd.read_csv(trend_root / "validation_trend_runs.csv")

    decision = assess_validation_trend(
        trend_summary=trend_summary,
        trend_runs=trend_runs,
        thresholds=ValidationTrendThresholds(),
    )

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    decision_path = output_root / "validation_trend_governance.json"
    report_path = output_root / "validation_trend_governance.md"
    with decision_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "trend_run": str(trend_root),
                "decision": decision,
            },
            handle,
            indent=2,
        )

    lines = [
        "# Validation Trend Governance",
        "",
        f"Trend run: `{trend_root}`",
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
    if decision["observations"]:
        lines.extend(["", "## Observations", ""])
        for observation in decision["observations"]:
            lines.append(f"- {observation}")
    with report_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")

    print("[OK] Validation trend governance completed.")
    print(f"Status: {decision['status']}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

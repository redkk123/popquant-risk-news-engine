from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fusion.backtest_guardrails import (
    build_backtest_guarded_mapping,
    load_event_type_guardrail_candidates,
    load_mapping,
    write_guarded_mapping,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply backtest-informed guardrails to an event scenario map.")
    parser.add_argument(
        "--mapping-config",
        required=True,
        help="Base mapping YAML used as the guardrail input.",
    )
    parser.add_argument(
        "--event-type-summary",
        required=True,
        help="CSV with per-event-type backtest results, including mae_improvement.",
    )
    parser.add_argument("--min-negative-horizons", type=int, default=2, help="Negative horizons needed to damp a family.")
    parser.add_argument("--dampening-factor", type=float, default=0.25, help="Multiplier applied to losing families.")
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "backtest_guardrails"),
        help="Output directory for the guarded mapping artifacts.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    mapping = load_mapping(args.mapping_config)
    event_type_summary = load_event_type_guardrail_candidates(args.event_type_summary)
    guarded_mapping, decisions = build_backtest_guarded_mapping(
        mapping_config=mapping,
        event_type_summary=event_type_summary,
        min_negative_horizons=args.min_negative_horizons,
        dampening_factor=args.dampening_factor,
    )

    mapping_path = write_guarded_mapping(
        mapping_config=guarded_mapping,
        output_path=output_root / "guarded_event_scenario_map.yaml",
    )
    decisions_frame = pd.DataFrame(decisions)
    decisions_csv = output_root / "guardrail_decisions.csv"
    decisions_frame.to_csv(decisions_csv, index=False)
    report = {
        "mapping_config": str(Path(args.mapping_config)),
        "event_type_summary": str(Path(args.event_type_summary)),
        "min_negative_horizons": int(args.min_negative_horizons),
        "dampening_factor": float(args.dampening_factor),
        "guarded_mapping": str(mapping_path),
        "decisions_csv": str(decisions_csv),
        "applied_event_types": sorted(
            row["event_type"] for row in decisions if row.get("guardrail_applied")
        ),
    }
    report_path = output_root / "guardrail_report.json"
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    print("[OK] Backtest guardrails applied.")
    print(f"Applied families: {len(report['applied_event_types'])}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

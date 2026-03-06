from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fusion.integrated_probe_compare import compare_probe_pairs, write_probe_compare_artifacts


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare base vs guarded integrated-risk probe runs.")
    parser.add_argument(
        "--pair",
        dest="pairs",
        action="append",
        nargs=3,
        metavar=("PORTFOLIO_ID", "BASE_RUN_DIR", "GUARDED_RUN_DIR"),
        required=True,
        help="Repeatable trio describing one portfolio comparison.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "integrated_risk_probe_compare"),
        help="Root directory for the comparison artifacts.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    pair_specs = [
        {
            "portfolio_id": portfolio_id,
            "base_run_dir": str(Path(base_run_dir)),
            "guarded_run_dir": str(Path(guarded_run_dir)),
        }
        for portfolio_id, base_run_dir, guarded_run_dir in args.pairs
    ]
    summary_frame, event_frame = compare_probe_pairs(
        (item["portfolio_id"], item["base_run_dir"], item["guarded_run_dir"]) for item in pair_specs
    )
    artifacts = write_probe_compare_artifacts(
        output_dir=output_root,
        summary_frame=summary_frame,
        event_frame=event_frame,
        pair_specs=pair_specs,
    )
    manifest = {
        "run_id": run_id,
        "pair_count": len(pair_specs),
        "artifacts": artifacts,
    }
    manifest_path = output_root / "probe_compare_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("[OK] Integrated probe comparison generated.")
    print(f"Pairs: {len(pair_specs)}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

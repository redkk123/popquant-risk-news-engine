from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fusion.calibration_registry import compare_calibration_snapshots, rebuild_calibration_registry


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rebuild or diff the calibration snapshot registry.")
    parser.add_argument(
        "--registry-root",
        default=str(PROJECT_ROOT / "output" / "event_calibration_registry"),
        help="Calibration registry root directory.",
    )
    parser.add_argument("--left-snapshot-id", help="Left snapshot ID for diff mode.")
    parser.add_argument("--right-snapshot-id", help="Right snapshot ID for diff mode.")
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "event_calibration_registry" / "compare"),
        help="Directory for diff outputs.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    registry_frame = rebuild_calibration_registry(args.registry_root)

    if not args.left_snapshot_id and not args.right_snapshot_id:
        print("[OK] Calibration registry rebuilt.")
        print(f"Snapshots: {len(registry_frame)}")
        return 0

    if not args.left_snapshot_id or not args.right_snapshot_id:
        raise ValueError("Both --left-snapshot-id and --right-snapshot-id are required for diff mode.")

    comparison = compare_calibration_snapshots(
        registry_root=args.registry_root,
        left_snapshot_id=args.left_snapshot_id,
        right_snapshot_id=args.right_snapshot_id,
    )

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    comparison_json = output_root / "calibration_snapshot_compare.json"
    comparison_md = output_root / "calibration_snapshot_compare.md"
    with comparison_json.open("w", encoding="utf-8") as handle:
        json.dump(comparison, handle, indent=2)

    lines = [
        "# Calibration Snapshot Compare",
        "",
        f"Left: `{comparison['left_snapshot_id']}`",
        f"Right: `{comparison['right_snapshot_id']}`",
        f"Changed event families: `{comparison['changed_family_count']}`",
        "",
    ]
    for change in comparison["family_changes"]:
        lines.append(
            f"- `{change['event_family']}` | left subtypes `{change['left_subtypes']}` | "
            f"right subtypes `{change['right_subtypes']}`"
        )
    with comparison_md.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines).strip() + "\n")

    print("[OK] Calibration registry diff completed.")
    print(f"Changed families: {comparison['changed_family_count']}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

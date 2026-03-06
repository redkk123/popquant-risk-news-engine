from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.pipeline import export_events_csv, process_raw_documents
from event_engine.storage.repository import NewsRepository


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Process raw news into canonical events.")
    parser.add_argument(
        "--alias-table",
        default=str(PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
        help="CSV alias table for fallback ticker linking.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "news_pipeline"),
        help="Output directory for pipeline manifests.",
    )
    return parser


def _json_default(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value)} is not JSON serializable")


def main() -> int:
    args = _build_parser().parse_args()
    repository = NewsRepository(PROJECT_ROOT)

    stats = process_raw_documents(repository, alias_path=args.alias_table)

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    manifest_path = output_root / "news_pipeline_manifest.json"
    events_csv_path = output_root / "events.csv"

    export_events_csv(repository, events_csv_path)
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(stats, handle, indent=2, default=_json_default)

    print("[OK] Event pipeline completed.")
    print(
        f"Raw={stats['raw_documents']} | Canonical={stats['canonical_documents']} | "
        f"Duplicates={stats['duplicates']} | Events={stats['events']}"
    )
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


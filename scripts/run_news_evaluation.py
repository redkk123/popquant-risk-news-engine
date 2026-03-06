from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.evaluation import evaluate_news_engine, load_labeled_events


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate NLP news engine on a labeled set.")
    parser.add_argument(
        "--labels",
        default=str(PROJECT_ROOT / "datasets" / "labeled_events" / "demo_labeled_events.jsonl"),
        help="Path to JSONL labeled events.",
    )
    parser.add_argument(
        "--alias-table",
        default=str(PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
        help="CSV alias table for fallback ticker linking.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "news_evaluation"),
        help="Output directory for evaluation artifacts.",
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
    labeled = load_labeled_events(args.labels)
    detail, summary = evaluate_news_engine(labeled, alias_path=args.alias_table)

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    detail_path = output_root / "news_evaluation_detail.csv"
    summary_path = output_root / "news_evaluation_summary.json"

    detail.to_csv(detail_path, index=False)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, default=_json_default)

    print("[OK] News evaluation completed.")
    print(
        f"Event accuracy={summary['event_type_accuracy']:.2%} | "
        f"Ticker accuracy={summary['ticker_link_accuracy']:.2%}"
    )
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


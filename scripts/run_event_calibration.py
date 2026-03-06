from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.research_workbench import run_event_calibration_workbench


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Calibrate event-to-risk mappings from historical event outcomes.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Portfolio JSON config used for benchmark selection.",
    )
    parser.add_argument(
        "--event-map-config",
        default=str(PROJECT_ROOT / "config" / "event_scenario_map.yaml"),
        help="Base event-scenario mapping YAML.",
    )
    parser.add_argument(
        "--alias-table",
        default=str(PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
        help="Alias table for the news pipeline.",
    )
    parser.add_argument(
        "--ticker-sector-map",
        default=str(PROJECT_ROOT / "config" / "ticker_sector_map.csv"),
        help="CSV mapping ticker to sector for sector-aware calibration.",
    )
    parser.add_argument(
        "--news-fixture",
        default=str(PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news_history.json"),
        help="Historical fixture used to refresh processed news before calibration.",
    )
    parser.add_argument("--start", default="2023-01-01", help="Price history start date (YYYY-MM-DD).")
    parser.add_argument(
        "--end",
        default=pd.Timestamp.today().date().isoformat(),
        help="Price history end date (YYYY-MM-DD).",
    )
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 3, 5], help="Forward return horizons.")
    parser.add_argument("--vol-window", type=int, default=10, help="Forward volatility window.")
    parser.add_argument("--min-observations", type=int, default=2, help="Minimum observations to update a rule.")
    parser.add_argument("--snapshot-label", default="default", help="Label used for the versioned snapshot.")
    parser.add_argument("--parent-snapshot-id", help="Optional parent snapshot ID.")
    parser.add_argument(
        "--registry-root",
        default=str(PROJECT_ROOT / "output" / "event_calibration_registry"),
        help="Root directory for the calibration snapshot registry.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Price cache directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "event_calibration"),
        help="Output directory for calibration artifacts.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_event_calibration_workbench(
        portfolio_config=args.portfolio_config,
        event_map_config=args.event_map_config,
        alias_table=args.alias_table,
        ticker_sector_map_path=args.ticker_sector_map,
        news_fixture=args.news_fixture,
        start=args.start,
        end=args.end,
        horizons=args.horizons,
        vol_window=args.vol_window,
        min_observations=args.min_observations,
        snapshot_label=args.snapshot_label,
        parent_snapshot_id=args.parent_snapshot_id,
        registry_root=args.registry_root,
        cache_dir=args.cache_dir,
        output_dir=args.output_dir,
    )
    snapshot = result["snapshot_metadata"]
    print("[OK] Event calibration completed.")
    print(f"Events: {snapshot['n_events']} | Observations: {snapshot['n_observations']}")
    print(f"Snapshot: {snapshot['snapshot_id']}")
    print(f"Output: {result['output_root']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

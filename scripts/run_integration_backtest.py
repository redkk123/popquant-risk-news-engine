from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.research_workbench import SUPPORTED_GROUP_COLUMNS, run_grouped_integration_backtest_workbench


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run grouped event-conditioned risk backtests.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Path to a single portfolio JSON config.",
    )
    parser.add_argument("--portfolio-configs", nargs="+", help="Explicit list of portfolio JSON configs.")
    parser.add_argument("--watchlist-config", help="Watchlist YAML used to load a portfolio set.")
    parser.add_argument(
        "--event-map-config",
        default=str(PROJECT_ROOT / "config" / "event_scenario_map.yaml"),
        help="Path to the base event-scenario mapping YAML.",
    )
    parser.add_argument(
        "--calibrated-event-map-config",
        help="Optional path to a calibrated mapping YAML. Defaults to latest governed map.",
    )
    parser.add_argument(
        "--mapping-variants",
        nargs="+",
        default=["configured"],
        help="Mapping variants to compare: configured, manual, calibrated, source_aware.",
    )
    parser.add_argument(
        "--group-by",
        nargs="+",
        default=list(SUPPORTED_GROUP_COLUMNS),
        help="Grouping dimensions to export.",
    )
    parser.add_argument("--min-events", type=int, default=1, help="Minimum unique events for grouped rows.")
    parser.add_argument(
        "--alias-table",
        default=str(PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
        help="Alias table for the event pipeline.",
    )
    parser.add_argument(
        "--ticker-sector-map",
        default=str(PROJECT_ROOT / "config" / "ticker_sector_map.csv"),
        help="CSV mapping ticker to sector for sector spillover scenarios.",
    )
    parser.add_argument(
        "--news-fixture",
        default=str(PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news_history.json"),
        help="Historical fixture used to refresh processed events before backtest.",
    )
    parser.add_argument("--start", default="2023-01-01", help="Price history start date (YYYY-MM-DD).")
    parser.add_argument(
        "--end",
        default=pd.Timestamp.today().date().isoformat(),
        help="Price history end date (YYYY-MM-DD).",
    )
    parser.add_argument("--alpha", type=float, default=0.01, help="Tail probability.")
    parser.add_argument("--lam", type=float, default=0.94, help="EWMA lambda.")
    parser.add_argument("--window", type=int, default=252, help="Rolling history window size.")
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 3, 5], help="Holding horizons in trading days.")
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Price cache directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "integration_backtest"),
        help="Output directory for backtest artifacts.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_grouped_integration_backtest_workbench(
        portfolio_config=args.portfolio_config,
        portfolio_configs=args.portfolio_configs,
        watchlist_config=args.watchlist_config,
        event_map_config=args.event_map_config,
        calibrated_event_map_config=args.calibrated_event_map_config,
        alias_table=args.alias_table,
        ticker_sector_map_path=args.ticker_sector_map,
        news_fixture=args.news_fixture,
        start=args.start,
        end=args.end,
        alpha=args.alpha,
        lam=args.lam,
        window=args.window,
        horizons=args.horizons,
        mapping_variants=args.mapping_variants,
        group_by=args.group_by,
        min_events=args.min_events,
        cache_dir=args.cache_dir,
        output_dir=args.output_dir,
    )
    summary = result["summary"]
    print("[OK] Integration backtest completed.")
    print(f"Event rows: {summary['n_event_rows']} | Event days: {summary['n_event_days']}")
    print(f"Output: {result['output_root']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

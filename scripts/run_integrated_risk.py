from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data.loaders import load_prices
from data.positions import load_portfolio_config, weights_series
from data.validation import validate_price_frame
from event_engine.ingestion.sync_news import ingest_fixture
from event_engine.pipeline import process_raw_documents
from event_engine.storage.repository import NewsRepository
from fusion.event_conditioned_risk import run_event_conditioned_risk
from fusion.reporting import write_integration_outputs
from fusion.scenario_mapper import load_event_mapping_config, map_event_to_scenario
from fusion.sector_mapping import load_ticker_sector_map


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the integrated event-conditioned risk workflow.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Path to the portfolio JSON config.",
    )
    parser.add_argument(
        "--event-map-config",
        default="",
        help="Optional path to event-scenario mapping YAML. Defaults to the latest selected governance map, or base config.",
    )
    parser.add_argument(
        "--alias-table",
        default=str(PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
        help="Alias table for event engine processing.",
    )
    parser.add_argument(
        "--ticker-sector-map",
        default=str(PROJECT_ROOT / "config" / "ticker_sector_map.csv"),
        help="CSV mapping ticker to sector for sector spillover scenarios.",
    )
    parser.add_argument(
        "--news-fixture",
        default=str(PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news.json"),
        help="Fixture path to refresh processed news before integration. Pass empty string to reuse existing processed events.",
    )
    parser.add_argument(
        "--start",
        default=(date.today() - timedelta(days=365 * 4)).isoformat(),
        help="Start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        default=date.today().isoformat(),
        help="End date (YYYY-MM-DD).",
    )
    parser.add_argument("--alpha", type=float, default=0.01, help="Tail probability.")
    parser.add_argument("--lam", type=float, default=0.94, help="EWMA lambda.")
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Price cache directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "integration"),
        help="Output directory for integrated artifacts.",
    )
    return parser


def _json_default(value: Any):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value)} is not JSON serializable")


def _resolve_as_of_timestamp(raw_end: str) -> pd.Timestamp:
    as_of = pd.Timestamp(raw_end)
    if as_of.tzinfo is None:
        if len(raw_end) <= 10:
            as_of = as_of + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        as_of = as_of.tz_localize("UTC")
    else:
        as_of = as_of.tz_convert("UTC")
    return as_of


def _resolve_latest_selected_map() -> Path:
    governance_root = PROJECT_ROOT / "output" / "integration_governance"
    selected_maps = sorted(governance_root.glob("*/selected_event_scenario_map.yaml"))
    if selected_maps:
        return selected_maps[-1]
    return PROJECT_ROOT / "config" / "event_scenario_map.yaml"


def main() -> int:
    args = _build_parser().parse_args()

    metadata, positions = load_portfolio_config(args.portfolio_config)
    weights = weights_series(positions)
    portfolio_tickers = weights.index.tolist()

    repository = NewsRepository(PROJECT_ROOT)
    if args.news_fixture:
        ingest_fixture(repository, args.news_fixture)
        process_raw_documents(repository, alias_path=args.alias_table)

    events_frame = repository.load_events_frame()
    events = events_frame.to_dict(orient="records")
    mapping_path = Path(args.event_map_config) if args.event_map_config else _resolve_latest_selected_map()
    mapping_config = load_event_mapping_config(mapping_path)
    ticker_sector_map = load_ticker_sector_map(args.ticker_sector_map)
    as_of = _resolve_as_of_timestamp(args.end)
    scenarios = []
    relevant_events = []
    for event in events:
        scenario = map_event_to_scenario(
            event,
            portfolio_tickers=portfolio_tickers,
            mapping_config=mapping_config,
            ticker_sector_map=ticker_sector_map,
            as_of=as_of,
        )
        if scenario:
            scenarios.append(scenario)
            relevant_events.append(event)

    requested_symbols = portfolio_tickers.copy()
    benchmark = metadata.get("benchmark")
    if benchmark:
        requested_symbols.append(benchmark)
    prices = load_prices(
        tickers=requested_symbols,
        start=args.start,
        end=args.end,
        cache_dir=args.cache_dir,
    )
    prices = validate_price_frame(prices)

    baseline_snapshot, integrated_summary, stress_detail = run_event_conditioned_risk(
        prices=prices,
        weights=weights,
        events=relevant_events,
        scenarios=scenarios,
        alpha=args.alpha,
        lam=args.lam,
        portfolio_id=metadata["portfolio_id"],
        benchmark_name=benchmark,
    )

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    paths = write_integration_outputs(
        output_root=output_root,
        baseline_snapshot=baseline_snapshot,
        integrated_summary=integrated_summary,
        stress_detail=stress_detail,
    )

    manifest_path = output_root / "integration_manifest.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "portfolio_id": metadata["portfolio_id"],
                "relevant_events": len(relevant_events),
                "scenarios": len(scenarios),
                "outputs": {key: str(path) for key, path in paths.items()},
            },
            handle,
            indent=2,
            default=_json_default,
        )

    print("[OK] Integrated risk workflow generated.")
    print(f"Portfolio: {metadata['portfolio_id']}")
    print(f"Relevant events: {len(relevant_events)} | Scenarios: {len(scenarios)}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

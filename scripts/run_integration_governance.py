from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data.loaders import load_prices
from data.positions import load_portfolio_config, weights_series
from data.validation import validate_price_frame
from event_engine.ingestion.sync_news import ingest_fixture
from event_engine.pipeline import process_raw_documents
from event_engine.storage.repository import NewsRepository
from fusion.calibration import (
    build_calibrated_event_mapping,
    build_event_impact_observations,
    summarize_event_impacts,
    summarize_sector_peer_impacts,
)
from fusion.integration_governance import compare_integration_variants
from fusion.scenario_mapper import load_event_mapping_config
from fusion.sector_mapping import load_ticker_sector_map, select_sector_peer_symbols


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Govern manual versus calibrated integration mappings.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Path to the portfolio JSON config.",
    )
    parser.add_argument(
        "--event-map-config",
        default=str(PROJECT_ROOT / "config" / "event_scenario_map.yaml"),
        help="Path to the base event-scenario mapping YAML.",
    )
    parser.add_argument(
        "--alias-table",
        default=str(PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
        help="Alias table for the news pipeline.",
    )
    parser.add_argument(
        "--ticker-sector-map",
        default=str(PROJECT_ROOT / "config" / "ticker_sector_map.csv"),
        help="CSV mapping ticker to sector for sector spillover calibration.",
    )
    parser.add_argument(
        "--news-fixture",
        default=str(PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news_history.json"),
        help="Historical fixture used to refresh processed events.",
    )
    parser.add_argument("--start", default="2023-01-01", help="Price history start date.")
    parser.add_argument(
        "--end",
        default=pd.Timestamp.today().date().isoformat(),
        help="Price history end date.",
    )
    parser.add_argument("--alpha", type=float, default=0.01, help="Tail probability.")
    parser.add_argument("--lam", type=float, default=0.94, help="EWMA lambda.")
    parser.add_argument("--window", type=int, default=252, help="Rolling history window.")
    parser.add_argument(
        "--horizons",
        nargs="+",
        type=int,
        default=[1, 3, 5],
        help="Forward return horizons used by calibration.",
    )
    parser.add_argument(
        "--vol-window",
        type=int,
        default=10,
        help="Forward volatility window used by calibration.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=2,
        help="Minimum observations required before updating a mapping rule.",
    )
    parser.add_argument(
        "--shrinkage-target-observations",
        type=int,
        default=5,
        help="Observation count where calibration reaches full weight versus the manual rule.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Price cache directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "integration_governance"),
        help="Output directory for governance artifacts.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    metadata, positions = load_portfolio_config(args.portfolio_config)
    weights = weights_series(positions)

    repository = NewsRepository(PROJECT_ROOT)
    if args.news_fixture:
        ingest_fixture(repository, args.news_fixture)
        process_raw_documents(repository, alias_path=args.alias_table)

    events_frame = repository.load_events_frame()
    if events_frame.empty:
        raise RuntimeError("No processed events available for integration governance.")

    benchmark = metadata.get("benchmark") or "SPY"
    ticker_sector_map = load_ticker_sector_map(args.ticker_sector_map)
    event_tickers = sorted(
        {
            str(ticker).upper()
            for tickers in events_frame["tickers"].dropna()
            for ticker in tickers
            if str(ticker).strip()
        }
    )
    sector_peer_symbols = select_sector_peer_symbols(
        event_tickers=event_tickers,
        ticker_sector_map=ticker_sector_map,
    )
    calibration_symbols = sorted(
        set(event_tickers)
        | set(sector_peer_symbols)
        | set(weights.index.tolist())
        | {benchmark}
    )

    prices = load_prices(
        tickers=calibration_symbols,
        start=args.start,
        end=args.end,
        cache_dir=args.cache_dir,
    )
    prices = validate_price_frame(prices)

    manual_mapping = load_event_mapping_config(args.event_map_config)
    observations = build_event_impact_observations(
        prices=prices,
        events=events_frame.to_dict(orient="records"),
        benchmark_ticker=benchmark,
        ticker_sector_map=ticker_sector_map,
        horizons=args.horizons,
        vol_window=args.vol_window,
    )
    summary = summarize_event_impacts(
        observations,
        horizons=args.horizons,
        vol_window=args.vol_window,
    )
    sector_summary = summarize_sector_peer_impacts(
        observations,
        horizons=args.horizons,
        vol_window=args.vol_window,
    )
    calibrated_mapping = build_calibrated_event_mapping(
        summary=summary,
        base_mapping_config=manual_mapping,
        sector_summary=sector_summary,
        min_observations=args.min_observations,
        return_horizon=min(args.horizons),
        vol_window=args.vol_window,
        shrinkage_target_observations=args.shrinkage_target_observations,
    )

    comparison = compare_integration_variants(
        prices=prices.loc[:, weights.index.tolist()],
        weights=weights,
        events=events_frame.to_dict(orient="records"),
        manual_mapping_config=manual_mapping,
        calibrated_mapping_config=calibrated_mapping,
        ticker_sector_map=ticker_sector_map,
        alpha=args.alpha,
        lam=args.lam,
        window=args.window,
        portfolio_id=metadata["portfolio_id"],
        benchmark_name=metadata.get("benchmark"),
    )

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    summary_path = output_root / "event_calibration_summary.csv"
    sector_summary_path = output_root / "event_sector_calibration_summary.csv"
    observations_path = output_root / "event_impact_observations.csv"
    manual_path = output_root / "manual_backtest.csv"
    calibrated_path = output_root / "calibrated_backtest.csv"
    decision_path = output_root / "integration_governance_decision.json"
    selected_map_path = output_root / "selected_event_scenario_map.yaml"
    calibrated_map_path = output_root / "calibrated_event_scenario_map.yaml"

    summary.to_csv(summary_path, index=False)
    sector_summary.to_csv(sector_summary_path, index=False)
    observations.to_csv(observations_path, index=False)
    comparison["manual_backtest"].to_csv(manual_path, index=False)
    comparison["calibrated_backtest"].to_csv(calibrated_path, index=False)

    with decision_path.open("w", encoding="utf-8") as handle:
        json.dump(comparison["decision"], handle, indent=2)
    with selected_map_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(comparison["selected_mapping"], handle, sort_keys=False)
    with calibrated_map_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(calibrated_mapping, handle, sort_keys=False)

    print("[OK] Integration governance completed.")
    print(f"Selected variant: {comparison['decision']['selected_variant']}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

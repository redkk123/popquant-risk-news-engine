from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

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
from fusion.event_conditioned_risk import run_event_conditioned_risk
from fusion.scenario_mapper import load_event_mapping_config, map_event_to_scenario
from fusion.sector_mapping import load_ticker_sector_map
from fusion.watchlist_reporting import build_watchlist_rows, write_watchlist_outputs


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a multi-portfolio daily watchlist report.")
    parser.add_argument(
        "--watchlist-config",
        default=str(PROJECT_ROOT / "config" / "watchlists" / "demo_watchlist.yaml"),
        help="YAML file listing portfolio config paths.",
    )
    parser.add_argument(
        "--event-map-config",
        default="",
        help="Optional path to the event-scenario map. Defaults to the latest selected governance map, or base config.",
    )
    parser.add_argument(
        "--alias-table",
        default=str(PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
        help="Alias table for the news pipeline.",
    )
    parser.add_argument(
        "--ticker-sector-map",
        default=str(PROJECT_ROOT / "config" / "ticker_sector_map.csv"),
        help="CSV mapping ticker to sector for sector spillover scenarios.",
    )
    parser.add_argument(
        "--news-fixture",
        default=str(PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news.json"),
        help="Fixture path to refresh processed news before the report.",
    )
    parser.add_argument("--start", default="2022-01-01", help="Price history start date.")
    parser.add_argument(
        "--end",
        default=pd.Timestamp.today().date().isoformat(),
        help="Price history end date.",
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
        default=str(PROJECT_ROOT / "output" / "watchlist"),
        help="Output directory for watchlist artifacts.",
    )
    return parser


def _load_watchlist_paths(path: str | Path) -> list[Path]:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    entries = payload.get("portfolios", [])
    if not entries:
        raise ValueError("Watchlist config must contain a non-empty portfolios list.")
    resolved = []
    for item in entries:
        raw_path = Path(item["path"])
        resolved.append(raw_path if raw_path.is_absolute() else (PROJECT_ROOT / raw_path))
    return resolved


def _resolve_latest_selected_map() -> Path:
    governance_root = PROJECT_ROOT / "output" / "integration_governance"
    selected_maps = sorted(governance_root.glob("*/selected_event_scenario_map.yaml"))
    if selected_maps:
        return selected_maps[-1]
    return PROJECT_ROOT / "config" / "event_scenario_map.yaml"


def _resolve_as_of_timestamp(raw_end: str) -> pd.Timestamp:
    as_of = pd.Timestamp(raw_end)
    if as_of.tzinfo is None:
        if len(raw_end) <= 10:
            as_of = as_of + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        as_of = as_of.tz_localize("UTC")
    else:
        as_of = as_of.tz_convert("UTC")
    return as_of


def main() -> int:
    args = _build_parser().parse_args()

    watchlist_paths = _load_watchlist_paths(args.watchlist_config)
    mapping_path = Path(args.event_map_config) if args.event_map_config else _resolve_latest_selected_map()
    mapping_config = load_event_mapping_config(mapping_path)
    ticker_sector_map = load_ticker_sector_map(args.ticker_sector_map)
    repository = NewsRepository(PROJECT_ROOT)
    if args.news_fixture:
        ingest_fixture(repository, args.news_fixture)
        process_raw_documents(repository, alias_path=args.alias_table)
    events_frame = repository.load_events_frame()
    if "watchlist_eligible" in events_frame.columns:
        events_frame = events_frame.loc[events_frame["watchlist_eligible"].fillna(False)].copy()
    events = events_frame.to_dict(orient="records")

    portfolio_specs = []
    requested_symbols: set[str] = set()
    for portfolio_path in watchlist_paths:
        metadata, positions = load_portfolio_config(portfolio_path)
        weights = weights_series(positions)
        benchmark = metadata.get("benchmark")
        portfolio_specs.append(
            {
                "metadata": metadata,
                "weights": weights,
                "path": str(portfolio_path),
            }
        )
        requested_symbols.update(weights.index.tolist())
        if benchmark:
            requested_symbols.add(benchmark)

    prices = load_prices(
        tickers=sorted(requested_symbols),
        start=args.start,
        end=args.end,
        cache_dir=args.cache_dir,
    )
    prices = validate_price_frame(prices)
    as_of = _resolve_as_of_timestamp(args.end)

    summary_rows = []
    event_frames = []
    portfolio_reports = []
    for spec in portfolio_specs:
        metadata = spec["metadata"]
        weights = spec["weights"]
        portfolio_tickers = weights.index.tolist()
        relevant_events = []
        scenarios = []
        for event in events:
            scenario = map_event_to_scenario(
                event,
                portfolio_tickers=portfolio_tickers,
                mapping_config=mapping_config,
                ticker_sector_map=ticker_sector_map,
                as_of=as_of,
            )
            if scenario is None:
                continue
            relevant_events.append(event)
            scenarios.append(scenario)

        needed_symbols = portfolio_tickers.copy()
        benchmark = metadata.get("benchmark")
        if benchmark:
            needed_symbols.append(benchmark)
        ordered_symbols = list(dict.fromkeys(symbol for symbol in needed_symbols if symbol in prices.columns))
        portfolio_prices = prices.loc[:, ordered_symbols]

        baseline_snapshot, integrated_summary, stress_detail = run_event_conditioned_risk(
            prices=portfolio_prices,
            weights=weights,
            events=relevant_events,
            scenarios=scenarios,
            alpha=args.alpha,
            lam=args.lam,
            portfolio_id=metadata["portfolio_id"],
            benchmark_name=benchmark,
        )

        summary_row, event_frame = build_watchlist_rows(
            portfolio_id=metadata["portfolio_id"],
            baseline_snapshot=baseline_snapshot,
            integrated_summary=integrated_summary,
        )
        summary_rows.append(summary_row)
        if not event_frame.empty:
            event_frames.append(event_frame)
        portfolio_reports.append(
            {
                "portfolio_id": metadata["portfolio_id"],
                "baseline_snapshot": baseline_snapshot,
                "event_conditioned_summary": integrated_summary.to_dict(orient="records"),
                "stress_detail": stress_detail.to_dict(orient="records"),
            }
        )

    summary_frame = pd.DataFrame(summary_rows).sort_values(
        ["max_delta_normal_var_loss_1d_99", "stressed_normal_var_loss_1d_99"],
        ascending=[False, False],
    )
    event_frame = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    outputs = write_watchlist_outputs(
        output_root=output_root,
        summary_frame=summary_frame,
        event_frame=event_frame,
        portfolio_reports=portfolio_reports,
    )
    manifest_path = output_root / "watchlist_manifest.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "portfolio_count": int(len(summary_frame)),
                "event_rows": int(len(event_frame)),
                "outputs": {key: str(path) for key, path in outputs.items()},
            },
            handle,
            indent=2,
        )

    print("[OK] Daily watchlist generated.")
    print(f"Portfolios: {len(summary_frame)} | Event rows: {len(event_frame)}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

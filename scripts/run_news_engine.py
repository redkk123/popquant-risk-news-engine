from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.ingestion.sync_news import ingest_fixture, sync_news
from event_engine.pipeline import export_events_csv, process_raw_documents
from event_engine.storage.repository import NewsRepository

DEFAULT_PROVIDERS = ["marketaux", "thenewsapi", "newsapi", "alphavantage"]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the full NLP news engine end to end.")
    parser.add_argument("--symbols", nargs="*", default=["AAPL", "MSFT", "SPY"], help="Ticker symbols to query.")
    parser.add_argument(
        "--published-after",
        default=(date.today() - timedelta(days=7)).isoformat(),
        help="Lower timestamp/date bound for news sync.",
    )
    parser.add_argument(
        "--published-before",
        default=date.today().isoformat(),
        help="Upper timestamp/date bound for news sync.",
    )
    parser.add_argument("--language", default="en", help="Language filter.")
    parser.add_argument("--limit", type=int, default=3, help="Articles per page.")
    parser.add_argument("--max-pages", type=int, default=1, help="Maximum pages to fetch.")
    parser.add_argument(
        "--providers",
        nargs="*",
        default=DEFAULT_PROVIDERS,
        help="Ordered news providers to try. Defaults to Marketaux -> The News API -> NewsAPI.org -> Alpha Vantage.",
    )
    parser.add_argument(
        "--symbol-batch-size",
        type=int,
        default=5,
        help="Maximum symbols per upstream query batch.",
    )
    parser.add_argument(
        "--fixture",
        default=str(PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news.json"),
        help="Local JSON fixture path. Pass empty string to force API sync.",
    )
    parser.add_argument(
        "--fixture-provider",
        default="marketaux",
        help="Provider label to associate with fixture payloads.",
    )
    parser.add_argument(
        "--alias-table",
        default=str(PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
        help="CSV alias table for fallback ticker linking.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "news_engine"),
        help="Output directory for end-to-end manifests.",
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

    if args.fixture:
        sync_stats = ingest_fixture(repository, args.fixture, provider=args.fixture_provider)
    else:
        sync_stats = sync_news(
            repository,
            providers=args.providers,
            symbols=args.symbols,
            language=args.language,
            published_after=args.published_after,
            published_before=args.published_before,
            limit=args.limit,
            max_pages=args.max_pages,
            symbol_batch_size=args.symbol_batch_size,
        )

    pipeline_stats = process_raw_documents(repository, alias_path=args.alias_table)

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    report_path = output_root / "news_engine_report.json"
    events_csv_path = output_root / "events.csv"
    export_events_csv(repository, events_csv_path)

    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "sync": sync_stats,
                "pipeline": pipeline_stats,
                "events_csv": str(events_csv_path),
            },
            handle,
            indent=2,
            default=_json_default,
        )

    print("[OK] News engine completed.")
    print(
        f"Inserted={sync_stats['inserted']} | Events={pipeline_stats['events']} | "
        f"Duplicates={pipeline_stats['duplicates']}"
    )
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


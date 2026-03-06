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
from event_engine.run_logging import append_run_event, write_failure_manifest
from event_engine.storage.repository import NewsRepository

DEFAULT_PROVIDERS = ["marketaux", "thenewsapi", "alphavantage"]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync raw financial news into local storage.")
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
        help="Ordered news providers to try. Defaults to Marketaux -> The News API -> Alpha Vantage.",
    )
    parser.add_argument(
        "--symbol-batch-size",
        type=int,
        default=5,
        help="Maximum symbols per upstream query batch.",
    )
    parser.add_argument(
        "--fixture",
        default="",
        help="Optional local JSON fixture path. If set, sync uses the fixture instead of the API.",
    )
    parser.add_argument(
        "--fixture-provider",
        default="marketaux",
        help="Provider label to associate with fixture payloads.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "news_sync"),
        help="Output directory for sync manifests.",
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
    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    run_log_path = output_root / "run_log.jsonl"

    try:
        repository = NewsRepository(PROJECT_ROOT)
        append_run_event(
            run_log_path,
            stage="sync",
            status="start",
            details={
                "fixture": args.fixture,
                "providers": args.providers,
                "symbols": args.symbols,
                "published_after": args.published_after,
                "published_before": args.published_before,
            },
        )
        if args.fixture:
            stats = ingest_fixture(repository, args.fixture, provider=args.fixture_provider)
        else:
            stats = sync_news(
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

        manifest_path = output_root / "news_sync_manifest.json"
        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(
                {**stats, "run_log": str(run_log_path)},
                handle,
                indent=2,
                default=_json_default,
            )
        append_run_event(run_log_path, stage="sync", status="success", details=stats)

        print("[OK] News sync completed.")
        print(f"Inserted: {stats['inserted']} | Skipped: {stats['skipped']}")
        print(f"Output: {manifest_path}")
        return 0
    except Exception as exc:
        append_run_event(
            run_log_path,
            stage="sync",
            status="error",
            message=str(exc),
            details={"error_type": type(exc).__name__},
        )
        failure_path = write_failure_manifest(
            output_root=output_root,
            stage="sync",
            error=exc,
            log_path=run_log_path,
        )
        print("[ERROR] News sync failed.")
        print(f"Failure manifest: {failure_path}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

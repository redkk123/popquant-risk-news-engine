from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.capital_workbench import (
    run_capital_sandbox_compare_workbench,
    run_capital_sandbox_workbench,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a paper-trading capital sandbox.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Path to the portfolio JSON config.",
    )
    parser.add_argument(
        "--mode",
        default="live_session_real_time",
        choices=["live_session_real_time", "replay_intraday", "historical_daily"],
        help="Sandbox mode.",
    )
    parser.add_argument("--initial-capital", type=float, default=100.0, help="Initial simulated capital.")
    parser.add_argument(
        "--decision-interval-seconds",
        type=int,
        default=60,
        help="Decision cadence in seconds. Live mode uses at least 60 seconds.",
    )
    parser.add_argument(
        "--session-minutes",
        type=int,
        default=5,
        help="Session length in minutes for live or replay mode.",
    )
    parser.add_argument(
        "--news-refresh-minutes",
        type=int,
        default=2,
        help="Live-mode news refresh cadence in minutes. Ignored outside live mode.",
    )
    parser.add_argument(
        "--compare-session-minutes",
        nargs="*",
        type=int,
        default=[],
        help="Optional list of session presets to compare in one run, e.g. 5 15 30.",
    )
    parser.add_argument(
        "--start",
        default=(date.today() - timedelta(days=365)).isoformat(),
        help="Historical start date for daily mode.",
    )
    parser.add_argument(
        "--end",
        default=date.today().isoformat(),
        help="Historical end date for daily mode.",
    )
    parser.add_argument(
        "--providers",
        nargs="*",
        default=["marketaux", "thenewsapi", "newsapi", "alphavantage"],
        help="Ordered news providers to try.",
    )
    parser.add_argument(
        "--news-fixture",
        default="",
        help="Optional local JSON fixture path. If set, the sandbox uses the fixture instead of live sync.",
    )
    parser.add_argument(
        "--fixture-provider",
        default="marketaux",
        help="Provider label to associate with fixture payloads.",
    )
    parser.add_argument(
        "--published-after",
        default=(date.today() - timedelta(days=2)).isoformat(),
        help="Lower date bound for live news sync.",
    )
    parser.add_argument(
        "--published-before",
        default=(date.today() + timedelta(days=1)).isoformat(),
        help="Upper date bound for live news sync.",
    )
    parser.add_argument("--limit", type=int, default=3, help="Articles per page.")
    parser.add_argument("--max-pages", type=int, default=1, help="Maximum pages per provider.")
    parser.add_argument("--symbol-batch-size", type=int, default=5, help="Symbol batch size for provider sync.")
    parser.add_argument("--fee-rate", type=float, default=0.001, help="Transaction fee rate.")
    parser.add_argument("--slippage-rate", type=float, default=0.0005, help="Slippage rate.")
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "capital_sandbox"),
        help="Output directory for sandbox artifacts.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if args.mode == "live_session_real_time" and args.compare_session_minutes:
        raise SystemExit("compare-session-minutes is not supported in live_session_real_time mode.")
    if args.compare_session_minutes:
        result = run_capital_sandbox_compare_workbench(
            portfolio_config=args.portfolio_config,
            mode=args.mode,
            initial_capital=args.initial_capital,
            decision_interval_seconds=args.decision_interval_seconds,
            session_minutes_list=args.compare_session_minutes,
            start=args.start,
            end=args.end,
            news_fixture=args.news_fixture or None,
            fixture_provider=args.fixture_provider,
            providers=args.providers,
            fee_rate=args.fee_rate,
            slippage_rate=args.slippage_rate,
            symbol_batch_size=args.symbol_batch_size,
            limit=args.limit,
            max_pages=args.max_pages,
            published_after=args.published_after,
            published_before=args.published_before,
            output_dir=args.output_dir,
        )
    else:
        result = run_capital_sandbox_workbench(
            portfolio_config=args.portfolio_config,
            mode=args.mode,
            initial_capital=args.initial_capital,
            decision_interval_seconds=args.decision_interval_seconds,
            session_minutes=args.session_minutes,
            news_refresh_minutes=args.news_refresh_minutes,
            start=args.start,
            end=args.end,
            news_fixture=args.news_fixture or None,
            fixture_provider=args.fixture_provider,
            providers=args.providers,
            fee_rate=args.fee_rate,
            slippage_rate=args.slippage_rate,
            symbol_batch_size=args.symbol_batch_size,
            limit=args.limit,
            max_pages=args.max_pages,
            published_after=args.published_after,
            published_before=args.published_before,
            output_dir=args.output_dir,
        )
    summary = result["summary_frame"]
    best = summary.iloc[0]

    print("[OK] Capital sandbox completed.")
    print(f"Portfolio: {result['metadata']['portfolio_id']}")
    print(f"Mode: {args.mode}")
    if args.mode == "live_session_real_time":
        print(f"News refresh cadence: {args.news_refresh_minutes}m")
    if args.compare_session_minutes:
        print(f"Sessions: {', '.join(f'{int(value)}m' for value in sorted(set(args.compare_session_minutes)))}")
    print(
        f"Best path: {best['path_name']} | Final capital: {best['final_capital']:.2f} | "
        f"Return: {best['total_return']:.4f}"
    )
    print(f"Output: {result['output_root']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


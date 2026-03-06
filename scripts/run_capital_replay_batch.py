from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.capital_replay_batch import run_capital_replay_batch_workbench


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run multiple capital sandbox as-of replays in one batch.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Path to the portfolio JSON config.",
    )
    parser.add_argument(
        "--as-of-timestamps",
        nargs="+",
        required=True,
        help="One or more replay cutoff timestamps, e.g. 2026-03-05T15:30:00-03:00.",
    )
    parser.add_argument("--initial-capital", type=float, default=100.0, help="Initial simulated capital.")
    parser.add_argument(
        "--decision-interval-seconds",
        type=int,
        default=60,
        help="Decision cadence in seconds.",
    )
    parser.add_argument(
        "--session-minutes",
        type=int,
        default=5,
        help="Replay length in minutes before each as-of timestamp.",
    )
    parser.add_argument(
        "--providers",
        nargs="*",
        default=["newsapi"],
        help="Ordered providers to use for replay-as-of runs.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "capital_replay_batch"),
        help="Output directory for the replay batch.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_capital_replay_batch_workbench(
        portfolio_config=args.portfolio_config,
        as_of_timestamps=args.as_of_timestamps,
        initial_capital=args.initial_capital,
        decision_interval_seconds=args.decision_interval_seconds,
        session_minutes=args.session_minutes,
        providers=args.providers,
        output_dir=args.output_dir,
    )
    summary = result["summary_frame"]
    if summary.empty:
        print("[OK] Replay batch completed with no rows.")
        print(f"Output: {result['output_root']}")
        return 0

    best = summary.sort_values("best_final_capital", ascending=False).iloc[0]
    print("[OK] Capital replay batch completed.")
    print(f"Portfolio: {result['portfolio_id']}")
    print(f"Best replay: {best['as_of_timestamp']} | Path: {best['best_path']} | Final capital: {best['best_final_capital']:.2f}")
    print(f"Output: {result['output_root']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

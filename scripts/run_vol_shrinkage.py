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

from data.loaders import load_prices
from data.positions import load_portfolio_config, weights_series
from data.returns import compute_log_returns
from data.validation import validate_price_frame
from models.hierarchical_vol import hierarchical_shrinkage_vol


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run cross-asset volatility shrinkage on a portfolio universe."
    )
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Path to the portfolio JSON config.",
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
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Price cache directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "vol_shrinkage"),
        help="Output directory for shrinkage artifacts.",
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

    metadata, positions = load_portfolio_config(args.portfolio_config)
    weights = weights_series(positions)

    prices = load_prices(
        tickers=weights.index.tolist(),
        start=args.start,
        end=args.end,
        cache_dir=args.cache_dir,
    )
    prices = validate_price_frame(prices)
    asset_returns = compute_log_returns(prices[weights.index.tolist()])

    report, summary = hierarchical_shrinkage_vol(asset_returns)

    run_id = (
        f"{pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%SZ')}_"
        f"{metadata['portfolio_id']}"
    )
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    report_path = output_root / "vol_shrinkage.csv"
    summary_path = output_root / "vol_shrinkage_summary.json"
    report.to_csv(report_path, index=False)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, default=_json_default)

    print("[OK] Volatility shrinkage generated.")
    print(f"Portfolio: {metadata['portfolio_id']}")
    print(f"Assets: {', '.join(weights.index.tolist())}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


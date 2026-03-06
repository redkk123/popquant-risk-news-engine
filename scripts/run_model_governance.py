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

from backtest.christoffersen import christoffersen_independence_test
from backtest.kupiec import kupiec_test
from backtest.rolling import rolling_var_backtest
from backtest.scoring import summarize_model_backtest
from data.loaders import load_prices
from data.positions import load_portfolio_config, weights_series
from data.returns import compute_log_returns, weighted_portfolio_returns
from data.validation import validate_price_frame
from risk.model_registry import choose_governed_model


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Select an active risk model by governance rules.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Path to the portfolio JSON config.",
    )
    parser.add_argument(
        "--start",
        default=(date.today() - timedelta(days=365 * 5)).isoformat(),
        help="Start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        default=date.today().isoformat(),
        help="End date (YYYY-MM-DD).",
    )
    parser.add_argument("--alpha", type=float, default=0.01, help="Tail probability.")
    parser.add_argument("--lam", type=float, default=0.94, help="EWMA lambda.")
    parser.add_argument("--window", type=int, default=252, help="Rolling lookback window.")
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Price cache directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "governance"),
        help="Output directory for governance artifacts.",
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
    portfolio_returns = weighted_portfolio_returns(asset_returns, weights)

    backtest = rolling_var_backtest(
        portfolio_returns, alpha=args.alpha, lam=args.lam, window=args.window
    )
    summary = summarize_model_backtest(backtest, alpha=args.alpha)

    violation_columns = {
        "historical": "historical_violation",
        "filtered_historical": "filtered_historical_violation",
        "normal": "normal_violation",
        "ewma_normal": "ewma_normal_violation",
        "student_t": "student_t_violation",
    }

    formal_rows = []
    for _, row in summary.iterrows():
        model = row["model"]
        seq = backtest[violation_columns[model]].astype(int).tolist()
        formal = row.to_dict()
        formal.update(
            kupiec_test(int(row["violations"]), int(row["observations"]), alpha=args.alpha)
        )
        formal.update(christoffersen_independence_test(seq))
        formal_rows.append(formal)

    formal_summary = pd.DataFrame(formal_rows)
    governance = choose_governed_model(formal_summary)

    run_id = (
        f"{pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%SZ')}_"
        f"{metadata['portfolio_id']}"
    )
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    summary_path = output_root / "governance_summary.csv"
    decision_path = output_root / "governance_decision.json"

    formal_summary.to_csv(summary_path, index=False)
    with decision_path.open("w", encoding="utf-8") as handle:
        json.dump(governance, handle, indent=2, default=_json_default)

    print("[OK] Model governance generated.")
    print(f"Portfolio: {metadata['portfolio_id']}")
    print(
        f"Selected model: {governance['selected_model']} "
        f"({governance['status']})"
    )
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


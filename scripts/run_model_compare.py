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

from backtest.rolling import rolling_var_backtest
from backtest.scoring import summarize_model_backtest
from data.loaders import load_prices
from data.positions import load_portfolio_config, weights_series
from data.returns import compute_log_returns, weighted_portfolio_returns
from data.validation import validate_price_frame
from models.student_t import student_t_summary
from risk.es import es_loss_student_t
from risk.var import var_loss_student_t


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare risk models using a rolling VaR backtest."
    )
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
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.01,
        help="Tail probability for VaR/ES.",
    )
    parser.add_argument(
        "--lam",
        type=float,
        default=0.94,
        help="EWMA lambda.",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=252,
        help="Rolling lookback window.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Price cache directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "model_compare"),
        help="Output directory for model comparison artifacts.",
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

    requested_symbols = weights.index.tolist()
    prices = load_prices(
        tickers=requested_symbols,
        start=args.start,
        end=args.end,
        cache_dir=args.cache_dir,
    )
    prices = validate_price_frame(prices)
    asset_returns = compute_log_returns(prices[weights.index.tolist()])
    portfolio_returns = weighted_portfolio_returns(asset_returns, weights)

    backtest = rolling_var_backtest(
        portfolio_returns,
        alpha=args.alpha,
        lam=args.lam,
        window=args.window,
    )
    summary = summarize_model_backtest(backtest, alpha=args.alpha)

    student = student_t_summary(portfolio_returns)
    student_snapshot = {
        "df": student["df"],
        "loc": student["loc"],
        "scale": student["scale"],
        "student_t_var_loss_1d_99": float(
            var_loss_student_t(student["df"], student["loc"], student["scale"], args.alpha)
        ),
        "student_t_es_loss_1d_99": float(
            es_loss_student_t(student["df"], student["loc"], student["scale"], args.alpha)
        ),
    }

    run_id = (
        f"{pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%SZ')}_"
        f"{metadata['portfolio_id']}"
    )
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    summary_path = output_root / "model_compare_summary.csv"
    backtest_path = output_root / "model_compare_backtest.csv"
    report_path = output_root / "model_compare_report.json"

    summary.to_csv(summary_path, index=False)
    backtest.to_csv(backtest_path, index_label="date")

    report = {
        "metadata": {
            "portfolio_id": metadata["portfolio_id"],
            "alpha": args.alpha,
            "lambda": args.lam,
            "window": args.window,
            "start_date": portfolio_returns.index.min().date().isoformat(),
            "end_date": portfolio_returns.index.max().date().isoformat(),
            "n_observations": int(portfolio_returns.shape[0]),
            "n_backtest_observations": int(backtest.shape[0]),
        },
        "student_t_snapshot": student_snapshot,
        "ranking": summary.to_dict(orient="records"),
    }
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, default=_json_default)

    best_model = summary.iloc[0]
    print("[OK] Model comparison generated.")
    print(f"Portfolio: {metadata['portfolio_id']}")
    print(f"Date range: {args.start} to {args.end}")
    print(f"Rolling window: {args.window}")
    print(f"Best model by coverage error: {best_model['model']}")
    print(
        f"Best model stats: violations={int(best_model['violations'])}, "
        f"rate={best_model['violation_rate']:.2%}, "
        f"coverage_error={best_model['coverage_error']:.2%}"
    )
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


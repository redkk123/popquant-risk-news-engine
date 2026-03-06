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
from risk.stress import load_scenarios, run_stress_scenarios


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run portfolio stress scenarios.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"),
        help="Path to the portfolio JSON config.",
    )
    parser.add_argument(
        "--scenario-config",
        default=str(PROJECT_ROOT / "config" / "scenarios.yaml"),
        help="Path to the scenario YAML config.",
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
        "--alpha",
        type=float,
        default=0.01,
        help="Tail probability for VaR/ES.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Price cache directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "stresses"),
        help="Output directory for stress artifacts.",
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
    scenarios = load_scenarios(args.scenario_config)

    prices = load_prices(
        tickers=weights.index.tolist(),
        start=args.start,
        end=args.end,
        cache_dir=args.cache_dir,
    )
    prices = validate_price_frame(prices)
    asset_returns = compute_log_returns(prices[weights.index.tolist()])

    summary, detail = run_stress_scenarios(
        asset_returns=asset_returns,
        weights=weights,
        scenarios=scenarios,
        alpha=args.alpha,
    )

    run_id = (
        f"{pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%SZ')}_"
        f"{metadata['portfolio_id']}"
    )
    output_root = Path(args.output_dir) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    summary_path = output_root / "stress_summary.csv"
    detail_path = output_root / "stress_asset_detail.csv"
    report_path = output_root / "stress_report.json"

    summary.to_csv(summary_path, index=False)
    detail.to_csv(detail_path, index=False)
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "portfolio_id": metadata["portfolio_id"],
                "alpha": args.alpha,
                "scenario_count": int(summary.shape[0]),
                "scenarios": summary.to_dict(orient="records"),
            },
            handle,
            indent=2,
            default=_json_default,
        )

    worst = summary.sort_values("delta_normal_var_loss_1d_99", ascending=False).iloc[0]
    print("[OK] Stress scenarios generated.")
    print(f"Portfolio: {metadata['portfolio_id']}")
    print(f"Scenarios: {summary.shape[0]}")
    print(
        f"Worst scenario by delta VaR: {worst['scenario']} "
        f"(delta={worst['delta_normal_var_loss_1d_99']:.4f})"
    )
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


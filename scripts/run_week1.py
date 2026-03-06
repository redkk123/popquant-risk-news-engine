from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data.loaders import load_prices
from data.returns import compute_log_returns, equal_weight_portfolio_returns
from models.ewma import ewma_volatility
from risk.es import es_return_normal
from risk.var import var_cutoff_normal


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run PopQuant Week 1 baseline.")
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=["AAPL", "MSFT", "SPY"],
        help="List of ticker symbols.",
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
        "--lam",
        type=float,
        default=0.94,
        help="EWMA lambda (0,1).",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.01,
        help="Tail probability for VaR/ES (e.g., 0.01).",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Where cached prices are stored.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output"),
        help="Where outputs are saved.",
    )
    return parser


def build_week1_timeseries(
    tickers: list[str], start: str, end: str, lam: float, alpha: float, cache_dir: str
) -> pd.DataFrame:
    prices = load_prices(tickers=tickers, start=start, end=end, cache_dir=cache_dir)
    asset_returns = compute_log_returns(prices)
    portfolio_returns = equal_weight_portfolio_returns(asset_returns)

    sigma = ewma_volatility(portfolio_returns, lam=lam).rename("sigma_ewma")
    sigma_forecast = sigma.shift(1).rename("sigma_forecast")
    mu = pd.Series(0.0, index=portfolio_returns.index, name="mu")
    var_cutoff = var_cutoff_normal(mu, sigma_forecast, alpha=alpha).rename("var_cutoff")
    es_return = es_return_normal(mu, sigma_forecast, alpha=alpha).rename("es_return")

    ts = pd.concat(
        [
            portfolio_returns.rename("portfolio_return"),
            sigma,
            sigma_forecast,
            var_cutoff,
            es_return,
        ],
        axis=1,
    ).dropna()
    ts["violation"] = (ts["portfolio_return"] < ts["var_cutoff"]).astype(int)
    return ts


def summarize_week1(ts: pd.DataFrame, alpha: float, tickers: list[str]) -> pd.DataFrame:
    n_obs = int(ts.shape[0])
    n_viol = int(ts["violation"].sum())
    viol_rate = n_viol / n_obs if n_obs else np.nan
    expected_viol = alpha * n_obs

    summary = pd.DataFrame(
        {
            "metric": [
                "tickers",
                "observations",
                "alpha",
                "violations",
                "violation_rate",
                "expected_violations",
                "avg_abs_return",
                "avg_sigma_forecast",
            ],
            "value": [
                ",".join(tickers),
                n_obs,
                alpha,
                n_viol,
                viol_rate,
                expected_viol,
                ts["portfolio_return"].abs().mean(),
                ts["sigma_forecast"].mean(),
            ],
        }
    )
    return summary


def make_week1_figure(ts: pd.DataFrame, alpha: float, figure_path: Path) -> None:
    figure_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

    ax1 = axes[0]
    ax1.plot(ts.index, ts["portfolio_return"], lw=0.9, label="Portfolio Return")
    ax1.plot(
        ts.index,
        ts["var_cutoff"],
        lw=1.1,
        color="#c1121f",
        label=f"VaR Cutoff ({alpha:.1%})",
    )
    viol_idx = ts["violation"] == 1
    ax1.scatter(
        ts.index[viol_idx],
        ts.loc[viol_idx, "portfolio_return"],
        s=12,
        color="black",
        label="Violations",
        zorder=3,
    )
    ax1.set_ylabel("Log-return")
    ax1.set_title("Portfolio Returns vs. VaR Cutoff")
    ax1.grid(alpha=0.25)
    ax1.legend(loc="lower left")

    ax2 = axes[1]
    ax2.plot(ts.index, ts["sigma_forecast"], lw=1.0, color="#005f73")
    ax2.set_ylabel("Sigma")
    ax2.set_title("EWMA Forecast Volatility")
    ax2.grid(alpha=0.25)

    ax3 = axes[2]
    actual = ts["violation"].cumsum()
    expected = alpha * np.arange(1, len(ts) + 1)
    ax3.plot(ts.index, actual, lw=1.4, label="Observed cumulative violations")
    ax3.plot(ts.index, expected, lw=1.1, linestyle="--", label="Expected cumulative")
    ax3.set_ylabel("Count")
    ax3.set_title("VaR Violation Tracking")
    ax3.grid(alpha=0.25)
    ax3.legend(loc="upper left")

    plt.tight_layout()
    fig.savefig(figure_path, dpi=160)
    plt.close(fig)


def main() -> int:
    args = _build_parser().parse_args()

    output_dir = Path(args.output_dir)
    tables_dir = output_dir / "tables"
    figures_dir = output_dir / "figures"
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    try:
        ts = build_week1_timeseries(
            tickers=args.tickers,
            start=args.start,
            end=args.end,
            lam=args.lam,
            alpha=args.alpha,
            cache_dir=args.cache_dir,
        )
    except Exception as exc:
        print(f"[ERROR] Failed to build week1 baseline: {exc}")
        return 1

    summary = summarize_week1(ts, alpha=args.alpha, tickers=args.tickers)

    ts_path = tables_dir / "week1_timeseries.csv"
    summary_path = tables_dir / "week1_summary.csv"
    fig_path = figures_dir / "week1_baseline.png"

    ts.to_csv(ts_path, index_label="date")
    summary.to_csv(summary_path, index=False)
    make_week1_figure(ts, alpha=args.alpha, figure_path=fig_path)

    n_obs = ts.shape[0]
    n_viol = int(ts["violation"].sum())
    viol_rate = n_viol / n_obs if n_obs else 0.0
    print("[OK] Week 1 baseline generated.")
    print(f"Tickers: {', '.join(args.tickers)}")
    print(f"Date range: {args.start} to {args.end}")
    print(f"Observations: {n_obs}")
    print(f"Violations: {n_viol} ({viol_rate:.2%}) | Expected: {args.alpha:.2%}")
    print(f"Saved timeseries: {ts_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Saved figure: {fig_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.risk_workbench import run_risk_snapshot_workbench


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a baseline risk snapshot.")
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
    parser.add_argument("--alpha", type=float, default=0.01, help="Tail probability for VaR/ES.")
    parser.add_argument("--lam", type=float, default=0.94, help="EWMA lambda.")
    parser.add_argument(
        "--ticker-sector-map",
        default=str(PROJECT_ROOT / "config" / "ticker_sector_map.csv"),
        help="CSV mapping ticker to sector for risk_v2 factor aggregation.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / "data" / "cache"),
        help="Price cache directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "risk_snapshots"),
        help="Output directory for snapshot artifacts.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_risk_snapshot_workbench(
        portfolio_config=args.portfolio_config,
        start=args.start,
        end=args.end,
        alpha=args.alpha,
        lam=args.lam,
        cache_dir=args.cache_dir,
        ticker_sector_map_path=args.ticker_sector_map,
        output_dir=args.output_dir,
    )
    snapshot = result["snapshot"]
    positions = result["positions"]

    print("[OK] Risk snapshot generated.")
    print(f"Portfolio: {snapshot['metadata']['portfolio_id']}")
    print(f"Date range: {args.start} to {args.end}")
    print(f"Assets: {', '.join(positions['ticker'].tolist())}")
    print(f"Output: {result['output_root']}")
    print(
        "Key metrics: "
        f"hist_var_1d={snapshot['models']['historical_var_loss_1d_99']:.4f}, "
        f"normal_var_1d={snapshot['models']['normal_var_loss_1d_99']:.4f}, "
        f"ewma_var_1d={snapshot['models']['ewma_normal_var_loss_1d_99']:.4f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

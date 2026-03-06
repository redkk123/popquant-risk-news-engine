from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from data.loaders import load_prices
from data.positions import load_portfolio_config, weights_series
from data.returns import compute_log_returns
from data.validation import validate_price_frame
from fusion.sector_mapping import load_ticker_sector_map
from risk.portfolio import build_risk_snapshot_bundle
from services.pathing import PROJECT_ROOT


def _json_default(value: Any):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value)} is not JSON serializable")


def run_risk_snapshot_workbench(
    *,
    portfolio_config: str | Path,
    start: str,
    end: str,
    alpha: float = 0.01,
    lam: float = 0.94,
    cache_dir: str | Path | None = None,
    ticker_sector_map_path: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    metadata, positions = load_portfolio_config(portfolio_config)
    weights = weights_series(positions)

    requested_symbols = weights.index.tolist()
    benchmark = metadata.get("benchmark")
    if benchmark:
        requested_symbols.append(benchmark)

    prices = load_prices(
        tickers=requested_symbols,
        start=start,
        end=end,
        cache_dir=cache_dir or (PROJECT_ROOT / "data" / "cache"),
    )
    prices = validate_price_frame(prices)
    asset_prices = prices.loc[:, weights.index.tolist()]
    asset_returns = compute_log_returns(asset_prices)

    benchmark_returns = None
    if benchmark:
        benchmark_prices = validate_price_frame(prices[[benchmark]])
        benchmark_returns = compute_log_returns(benchmark_prices)[benchmark]

    snapshot, contribution_table, model_table, correlation, extras = build_risk_snapshot_bundle(
        asset_returns=asset_returns,
        weights=weights,
        alpha=alpha,
        lam=lam,
        benchmark_returns=benchmark_returns,
        portfolio_id=metadata["portfolio_id"],
        benchmark_name=benchmark,
        ticker_sector_map=load_ticker_sector_map(
            ticker_sector_map_path or (PROJECT_ROOT / "config" / "ticker_sector_map.csv")
        ),
    )

    result = {
        "metadata": metadata,
        "positions": positions,
        "snapshot": snapshot,
        "contribution_table": contribution_table,
        "model_table": model_table,
        "correlation": correlation,
        "sector_contributions": extras["sector_contributions"],
        "covariance_model_compare": extras["covariance_model_compare"],
        "regime_state": extras["regime_state"],
        "output_root": None,
    }

    if output_dir is not None:
        run_id = f"{pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%SZ')}_{metadata['portfolio_id']}"
        output_root = Path(output_dir) / run_id
        output_root.mkdir(parents=True, exist_ok=True)

        with (output_root / "risk_snapshot.json").open("w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2, default=_json_default)
        positions.to_csv(output_root / "positions_used.csv", index=False)
        model_table.to_csv(output_root / "model_metrics.csv", index=False)
        contribution_table.to_csv(output_root / "risk_contributions.csv", index=False)
        correlation.to_csv(output_root / "correlation_matrix.csv", index_label="ticker")
        extras["sector_contributions"].to_csv(output_root / "sector_risk_contributions.csv", index=False)
        extras["covariance_model_compare"].to_csv(output_root / "covariance_model_compare.csv", index=False)
        with (output_root / "regime_state.json").open("w", encoding="utf-8") as handle:
            json.dump(extras["regime_state"], handle, indent=2, default=_json_default)
        result["output_root"] = str(output_root)

    return result

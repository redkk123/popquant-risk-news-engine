from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from data.returns import aggregate_log_returns, weighted_portfolio_returns
from models.covariance import (
    constant_correlation_shrinkage_covariance,
    covariance_from_correlation_and_vols,
    portfolio_volatility,
    sample_correlation,
    sample_covariance,
)
from models.ewma import ewma_next_volatility
from models.filtered_historical import filtered_historical_horizon_loss
from models.hierarchical_vol import hierarchical_shrinkage_vol
from models.historical import historical_es_loss, historical_var_loss
from models.student_t import fit_student_t
from risk.decomposition import decompose_portfolio_risk
from risk.es import es_loss_normal, es_loss_student_t
from risk.factors import build_factor_summary, sector_risk_contributions
from risk.regime import classify_risk_regime
from risk.var import var_loss_normal, var_loss_student_t


def max_drawdown_from_returns(returns: pd.Series) -> float:
    """Maximum drawdown from a log-return series."""
    wealth = np.exp(returns.cumsum())
    running_peak = wealth.cummax()
    drawdown = wealth / running_peak - 1.0
    return float(drawdown.min())


def beta_against_benchmark(
    portfolio_returns: pd.Series, benchmark_returns: pd.Series | None
) -> float | None:
    """Full-sample beta against a benchmark return series."""
    if benchmark_returns is None:
        return None

    aligned = pd.concat(
        [
            portfolio_returns.rename("portfolio"),
            benchmark_returns.rename("benchmark"),
        ],
        axis=1,
    ).dropna()
    if aligned.empty or aligned["benchmark"].var(ddof=1) == 0.0:
        return None
    covariance = aligned["portfolio"].cov(aligned["benchmark"])
    variance = aligned["benchmark"].var(ddof=1)
    return float(covariance / variance)


def _horizon_model_metrics(
    portfolio_returns: pd.Series,
    *,
    alpha: float,
    lam: float,
    horizon_days: int,
) -> dict[str, float]:
    horizon_returns = aggregate_log_returns(portfolio_returns, horizon_days)
    mu = float(horizon_returns.mean())
    sigma = float(horizon_returns.std(ddof=1))
    student = fit_student_t(horizon_returns)

    ewma_sigma_1d = ewma_next_volatility(portfolio_returns, lam=lam)
    ewma_sigma_h = float(ewma_sigma_1d * np.sqrt(horizon_days))
    filtered_historical_var_loss, filtered_historical_es_loss = (
        filtered_historical_horizon_loss(
            portfolio_returns,
            alpha=alpha,
            lam=lam,
            horizon_days=horizon_days,
        )
        if horizon_days > 1
        else (
            filtered_historical_horizon_loss(
                portfolio_returns,
                alpha=alpha,
                lam=lam,
                horizon_days=1,
                n_bootstrap=4000,
            )
        )
    )

    alpha_pct = int(round((1.0 - alpha) * 100))
    key_suffix = f"{horizon_days}d_{alpha_pct}"

    return {
        f"historical_var_loss_{key_suffix}": historical_var_loss(horizon_returns, alpha=alpha),
        f"historical_es_loss_{key_suffix}": historical_es_loss(horizon_returns, alpha=alpha),
        f"filtered_historical_var_loss_{key_suffix}": float(filtered_historical_var_loss),
        f"filtered_historical_es_loss_{key_suffix}": float(filtered_historical_es_loss),
        f"normal_var_loss_{key_suffix}": float(var_loss_normal(mu, sigma, alpha=alpha)),
        f"normal_es_loss_{key_suffix}": float(es_loss_normal(mu, sigma, alpha=alpha)),
        f"student_t_var_loss_{key_suffix}": float(
            var_loss_student_t(student.df, student.loc, student.scale, alpha=alpha)
        ),
        f"student_t_es_loss_{key_suffix}": float(
            es_loss_student_t(student.df, student.loc, student.scale, alpha=alpha)
        ),
        f"ewma_normal_var_loss_{key_suffix}": float(var_loss_normal(0.0, ewma_sigma_h, alpha=alpha)),
        f"ewma_normal_es_loss_{key_suffix}": float(es_loss_normal(0.0, ewma_sigma_h, alpha=alpha)),
        f"student_t_df_{key_suffix}": float(student.df),
    }


def _hierarchical_vol_adjusted_covariance(asset_returns: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    correlation = sample_correlation(asset_returns)
    vol_report, vol_summary = hierarchical_shrinkage_vol(asset_returns)
    shrink_vols = vol_report.set_index("ticker")["shrink_sigma"]
    covariance = covariance_from_correlation_and_vols(correlation, shrink_vols)
    return covariance, {"vol_summary": vol_summary, "avg_shrink_sigma": float(shrink_vols.mean())}


def build_risk_snapshot_bundle(
    *,
    asset_returns: pd.DataFrame,
    weights: pd.Series,
    alpha: float = 0.01,
    lam: float = 0.94,
    benchmark_returns: pd.Series | None = None,
    portfolio_id: str = "unknown",
    benchmark_name: str | None = None,
    ticker_sector_map: dict[str, str] | None = None,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Compute the legacy risk snapshot plus v2 artifacts."""
    portfolio_returns = weighted_portfolio_returns(asset_returns, weights)
    covariance_daily = sample_covariance(asset_returns, annualize=False)
    covariance_annual = sample_covariance(asset_returns, annualize=True)
    correlation = sample_correlation(asset_returns)

    model_metrics = {}
    for horizon in (1, 10):
        model_metrics.update(_horizon_model_metrics(portfolio_returns, alpha=alpha, lam=lam, horizon_days=horizon))

    portfolio_vol_daily = portfolio_volatility(weights, covariance_daily)
    portfolio_vol_annual = portfolio_volatility(weights, covariance_annual)
    beta = beta_against_benchmark(portfolio_returns, benchmark_returns)
    contribution_table = decompose_portfolio_risk(
        weights,
        covariance_daily,
        var_loss=model_metrics["normal_var_loss_1d_99"],
        es_loss=model_metrics["normal_es_loss_1d_99"],
    )

    sample_cov = covariance_daily
    cc_cov, cc_meta = constant_correlation_shrinkage_covariance(asset_returns)
    hierarchical_cov, hierarchical_meta = _hierarchical_vol_adjusted_covariance(asset_returns)
    covariance_model_compare = pd.DataFrame(
        [
            {
                "model": "sample",
                "portfolio_volatility": portfolio_volatility(weights, sample_cov),
                "annualized_portfolio_volatility": portfolio_volatility(weights, sample_cov * 252.0),
                "metadata": {},
            },
            {
                "model": "constant_correlation_shrinkage",
                "portfolio_volatility": portfolio_volatility(weights, cc_cov),
                "annualized_portfolio_volatility": portfolio_volatility(weights, cc_cov * 252.0),
                "metadata": cc_meta,
            },
            {
                "model": "hierarchical_vol_adjusted",
                "portfolio_volatility": portfolio_volatility(weights, hierarchical_cov),
                "annualized_portfolio_volatility": portfolio_volatility(weights, hierarchical_cov * 252.0),
                "metadata": hierarchical_meta,
            },
        ]
    )

    sector_contributions = sector_risk_contributions(
        weights=weights,
        covariance=covariance_daily,
        ticker_sector_map=ticker_sector_map,
    )
    factor_summary = build_factor_summary(
        sector_contributions=sector_contributions,
        beta=beta,
        benchmark_name=benchmark_name,
    )
    regime_state = classify_risk_regime(
        portfolio_returns=portfolio_returns,
        benchmark_returns=benchmark_returns,
        lam=lam,
    )

    exposures = [{"ticker": ticker, "weight": float(weight)} for ticker, weight in weights.items()]
    snapshot = {
        "metadata": {
            "portfolio_id": portfolio_id,
            "alpha": alpha,
            "lambda": lam,
            "n_assets": int(asset_returns.shape[1]),
            "n_observations": int(asset_returns.shape[0]),
            "start_date": asset_returns.index.min().date().isoformat(),
            "end_date": asset_returns.index.max().date().isoformat(),
        },
        "exposures": exposures,
        "portfolio_stats": {
            "mean_daily_return": float(portfolio_returns.mean()),
            "daily_volatility": float(portfolio_vol_daily),
            "annualized_volatility": float(portfolio_vol_annual),
            "max_drawdown": max_drawdown_from_returns(portfolio_returns),
            "realized_return_total": float(np.exp(portfolio_returns.sum()) - 1.0),
        },
        "models": model_metrics,
        "benchmark": {
            "name": benchmark_name,
            "beta": beta,
        },
        "top_risk_contributors": contribution_table.head(5).to_dict(orient="records"),
        "risk_v2": {
            "factor_summary": factor_summary,
            "regime": regime_state,
            "covariance_models": covariance_model_compare.to_dict(orient="records"),
        },
    }

    model_table = pd.DataFrame({"metric": list(model_metrics.keys()), "value": list(model_metrics.values())})
    extras = {
        "sector_contributions": sector_contributions,
        "covariance_model_compare": covariance_model_compare,
        "regime_state": regime_state,
    }
    return snapshot, contribution_table, model_table, correlation, extras


def build_risk_snapshot(
    *,
    asset_returns: pd.DataFrame,
    weights: pd.Series,
    alpha: float = 0.01,
    lam: float = 0.94,
    benchmark_returns: pd.Series | None = None,
    portfolio_id: str = "unknown",
    benchmark_name: str | None = None,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute the legacy risk snapshot contract."""
    snapshot, contribution_table, model_table, correlation, _ = build_risk_snapshot_bundle(
        asset_returns=asset_returns,
        weights=weights,
        alpha=alpha,
        lam=lam,
        benchmark_returns=benchmark_returns,
        portfolio_id=portfolio_id,
        benchmark_name=benchmark_name,
    )
    return snapshot, contribution_table, model_table, correlation

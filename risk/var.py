from __future__ import annotations

from scipy.stats import norm, t


def var_cutoff_normal(mu, sigma, alpha: float = 0.01):
    """Return-side VaR cutoff for a normal model."""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1).")
    return mu + sigma * norm.ppf(alpha)


def var_loss_normal(mu, sigma, alpha: float = 0.01):
    """Loss-side VaR (positive values mean larger losses)."""
    return -var_cutoff_normal(mu, sigma, alpha=alpha)


def var_cutoff_student_t(df, loc, scale, alpha: float = 0.01):
    """Return-side VaR cutoff for a fitted Student-t model."""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1).")
    if df <= 2.0:
        raise ValueError("df must be > 2 for stable Student-t risk metrics.")
    return loc + scale * t.ppf(alpha, df)


def var_loss_student_t(df, loc, scale, alpha: float = 0.01):
    """Loss-side VaR under a fitted Student-t model."""
    return -var_cutoff_student_t(df, loc, scale, alpha=alpha)

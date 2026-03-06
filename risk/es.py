from __future__ import annotations

from scipy.stats import norm, t


def es_return_normal(mu, sigma, alpha: float = 0.01):
    """Expected shortfall on returns under normality."""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1).")
    z_alpha = norm.ppf(alpha)
    return mu - sigma * (norm.pdf(z_alpha) / alpha)


def es_loss_normal(mu, sigma, alpha: float = 0.01):
    """Loss-side ES (positive values mean larger losses)."""
    return -es_return_normal(mu, sigma, alpha=alpha)


def es_return_student_t(df, loc, scale, alpha: float = 0.01):
    """Expected shortfall on returns under a fitted Student-t model."""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1).")
    if df <= 1.0:
        raise ValueError("df must be > 1 for Student-t ES.")

    q = t.ppf(alpha, df)
    density = t.pdf(q, df)
    tail_mean_std = -((df + q**2) / (df - 1.0)) * (density / alpha)
    return loc + scale * tail_mean_std


def es_loss_student_t(df, loc, scale, alpha: float = 0.01):
    """Loss-side ES under a fitted Student-t model."""
    return -es_return_student_t(df, loc, scale, alpha=alpha)

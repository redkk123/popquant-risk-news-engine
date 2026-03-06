from __future__ import annotations

import math

from scipy.stats import chi2


def kupiec_test(violations: int, observations: int, alpha: float = 0.01) -> dict[str, float]:
    """Kupiec unconditional coverage test."""
    if observations <= 0:
        raise ValueError("observations must be positive.")
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1).")
    if not 0 <= violations <= observations:
        raise ValueError("violations must be between 0 and observations.")

    pi_hat = violations / observations
    if pi_hat in (0.0, 1.0):
        lr_uc = float("inf")
        p_value = 0.0
    else:
        log_l_null = (observations - violations) * math.log(1.0 - alpha) + violations * math.log(alpha)
        log_l_alt = (observations - violations) * math.log(1.0 - pi_hat) + violations * math.log(pi_hat)
        lr_uc = float(-2.0 * (log_l_null - log_l_alt))
        p_value = float(1.0 - chi2.cdf(lr_uc, df=1))

    return {
        "violations": int(violations),
        "observations": int(observations),
        "expected_rate": float(alpha),
        "observed_rate": float(pi_hat),
        "lr_uc": lr_uc,
        "p_value_uc": p_value,
    }


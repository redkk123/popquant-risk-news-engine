from __future__ import annotations

import math

from scipy.stats import chi2


def _safe_term(probability: float, count: int) -> float:
    if count == 0:
        return 0.0
    if probability <= 0.0:
        return float("-inf")
    return count * math.log(probability)


def christoffersen_independence_test(violations: list[int] | tuple[int, ...]) -> dict[str, float]:
    """Christoffersen independence test on a binary violation sequence."""
    if len(violations) < 2:
        raise ValueError("At least two observations are required.")

    sequence = [int(v) for v in violations]
    if any(v not in (0, 1) for v in sequence):
        raise ValueError("Violation sequence must contain only 0 and 1.")

    n00 = n01 = n10 = n11 = 0
    for prev, curr in zip(sequence[:-1], sequence[1:]):
        if prev == 0 and curr == 0:
            n00 += 1
        elif prev == 0 and curr == 1:
            n01 += 1
        elif prev == 1 and curr == 0:
            n10 += 1
        else:
            n11 += 1

    total_transitions = n00 + n01 + n10 + n11
    pi = (n01 + n11) / total_transitions if total_transitions else 0.0
    pi01 = n01 / (n00 + n01) if (n00 + n01) else 0.0
    pi11 = n11 / (n10 + n11) if (n10 + n11) else 0.0

    log_l_null = _safe_term(1.0 - pi, n00 + n10) + _safe_term(pi, n01 + n11)
    log_l_alt = (
        _safe_term(1.0 - pi01, n00)
        + _safe_term(pi01, n01)
        + _safe_term(1.0 - pi11, n10)
        + _safe_term(pi11, n11)
    )

    if not math.isfinite(log_l_null) or not math.isfinite(log_l_alt):
        lr_ind = float("inf")
        p_value = 0.0
    else:
        lr_ind = float(-2.0 * (log_l_null - log_l_alt))
        p_value = float(1.0 - chi2.cdf(lr_ind, df=1))

    return {
        "n00": n00,
        "n01": n01,
        "n10": n10,
        "n11": n11,
        "lr_ind": lr_ind,
        "p_value_ind": p_value,
    }


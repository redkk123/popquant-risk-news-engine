from __future__ import annotations

from backtest.christoffersen import christoffersen_independence_test
from backtest.kupiec import kupiec_test


def test_kupiec_outputs_expected_keys() -> None:
    result = kupiec_test(violations=12, observations=500, alpha=0.01)

    assert result["observations"] == 500
    assert "lr_uc" in result
    assert "p_value_uc" in result


def test_christoffersen_outputs_expected_keys() -> None:
    result = christoffersen_independence_test([0, 0, 1, 0, 0, 1, 0, 0, 0, 1])

    assert "lr_ind" in result
    assert "p_value_ind" in result
    assert result["n01"] >= 0


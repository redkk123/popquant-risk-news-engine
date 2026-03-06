from __future__ import annotations

from capital.policy import decide_target_exposure


def test_decide_target_exposure_holds_when_neutral_and_turnover_small() -> None:
    result = decide_target_exposure(
        signal_score=0.0002,
        regime="normal",
        positive_events=0,
        negative_events=0,
        eligible_event_count=1,
        current_exposure=0.45,
        neutral_signal_band=0.0025,
        min_rebalance_turnover=0.20,
    )
    assert result["target_exposure"] == 0.45
    assert result["action"] == "hold_existing"


def test_decide_target_exposure_stays_in_cash_without_eligible_event() -> None:
    result = decide_target_exposure(
        signal_score=0.01,
        regime="calm",
        positive_events=2,
        negative_events=0,
        eligible_event_count=0,
        current_exposure=0.0,
    )
    assert result["target_exposure"] == 0.0
    assert result["reason"] == "no_eligible_event"


def test_decide_target_exposure_blocks_positive_signal_without_quant_confirmation() -> None:
    result = decide_target_exposure(
        signal_score=0.01,
        regime="normal",
        positive_events=2,
        negative_events=0,
        eligible_event_count=2,
        current_exposure=0.0,
        quant_confirmation="neutral",
        confirmation_score=0.0001,
    )
    assert result["target_exposure"] == 0.0
    assert result["reason"] == "positive_event_unconfirmed"
    assert result["risk_on_allowed"] is False


def test_decide_target_exposure_allows_positive_signal_with_quant_confirmation() -> None:
    result = decide_target_exposure(
        signal_score=0.01,
        regime="calm",
        positive_events=2,
        negative_events=0,
        eligible_event_count=2,
        current_exposure=0.0,
        quant_confirmation="confirmed_long",
        confirmation_score=0.003,
    )
    assert result["target_exposure"] == 1.0
    assert result["action"] == "add_risk"
    assert result["risk_on_allowed"] is True


def test_decide_target_exposure_blocks_when_path_is_underperforming() -> None:
    result = decide_target_exposure(
        signal_score=0.01,
        regime="normal",
        positive_events=2,
        negative_events=0,
        eligible_event_count=2,
        current_exposure=0.5,
        quant_confirmation="confirmed_long",
        confirmation_score=0.002,
        path_confirmation="underperforming",
        path_confirmation_score=-0.004,
    )

    assert result["target_exposure"] == 0.0
    assert result["reason"] == "path_underperforming"
    assert result["risk_on_allowed"] is False

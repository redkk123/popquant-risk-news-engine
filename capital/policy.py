from __future__ import annotations

from typing import Any


def decide_target_exposure(
    *,
    signal_score: float,
    regime: str,
    positive_events: int,
    negative_events: int,
    eligible_event_count: int = 0,
    current_exposure: float = 0.0,
    quant_confirmation: str = "neutral",
    confirmation_score: float = 0.0,
    path_confirmation: str = "neutral",
    path_confirmation_score: float = 0.0,
    neutral_signal_band: float = 0.0025,
    min_rebalance_turnover: float = 0.20,
) -> dict[str, Any]:
    """Map event and risk signals into a simple paper-trading exposure decision."""
    risk_on_allowed = str(quant_confirmation) == "confirmed_long" and str(path_confirmation) != "underperforming"

    def _finalize(target_exposure: float, action: str, reason: str) -> dict[str, Any]:
        turnover = abs(float(target_exposure) - float(current_exposure))
        if turnover < float(min_rebalance_turnover):
            return {
                "target_exposure": float(current_exposure),
                "action": "hold_existing",
                "reason": f"{reason}_turnover_deadband",
                "risk_on_allowed": bool(risk_on_allowed),
                "quant_confirmation": str(quant_confirmation),
                "confirmation_score": float(confirmation_score),
                "path_confirmation": str(path_confirmation),
                "path_confirmation_score": float(path_confirmation_score),
            }
        return {
            "target_exposure": float(target_exposure),
            "action": action,
            "reason": reason,
            "risk_on_allowed": bool(risk_on_allowed),
            "quant_confirmation": str(quant_confirmation),
            "confirmation_score": float(confirmation_score),
            "path_confirmation": str(path_confirmation),
            "path_confirmation_score": float(path_confirmation_score),
        }

    if int(eligible_event_count) <= 0:
        if float(current_exposure) <= 0.0:
            return {
                "target_exposure": 0.0,
                "action": "hold_existing",
                "reason": "no_eligible_event",
                "risk_on_allowed": False,
                "quant_confirmation": str(quant_confirmation),
                "confirmation_score": float(confirmation_score),
                "path_confirmation": str(path_confirmation),
                "path_confirmation_score": float(path_confirmation_score),
            }
        return _finalize(0.0, "unwind_to_cash", "no_eligible_event")

    if str(path_confirmation) == "underperforming":
        if float(current_exposure) <= 0.0:
            return {
                "target_exposure": 0.0,
                "action": "hold_existing",
                "reason": "path_underperforming",
                "risk_on_allowed": False,
                "quant_confirmation": str(quant_confirmation),
                "confirmation_score": float(confirmation_score),
                "path_confirmation": str(path_confirmation),
                "path_confirmation_score": float(path_confirmation_score),
            }
        return _finalize(0.0, "move_to_cash", "path_underperforming")

    if str(quant_confirmation) == "risk_off":
        if float(current_exposure) <= 0.0:
            return {
                "target_exposure": 0.0,
                "action": "hold_existing",
                "reason": "quant_risk_off",
                "risk_on_allowed": False,
                "quant_confirmation": str(quant_confirmation),
                "confirmation_score": float(confirmation_score),
                "path_confirmation": str(path_confirmation),
                "path_confirmation_score": float(path_confirmation_score),
            }
        return _finalize(0.0, "move_to_cash", "quant_risk_off")

    if signal_score >= 0.003 and not risk_on_allowed:
        if float(current_exposure) <= 0.0:
            return {
                "target_exposure": 0.0,
                "action": "hold_existing",
                "reason": "positive_event_unconfirmed",
                "risk_on_allowed": False,
                "quant_confirmation": str(quant_confirmation),
                "confirmation_score": float(confirmation_score),
                "path_confirmation": str(path_confirmation),
                "path_confirmation_score": float(path_confirmation_score),
            }
        return _finalize(float(current_exposure), "hold_existing", "positive_event_unconfirmed")

    if abs(signal_score) < float(neutral_signal_band) and abs(positive_events - negative_events) <= 1:
        if not risk_on_allowed:
            if float(current_exposure) <= 0.0:
                return {
                    "target_exposure": 0.0,
                    "action": "hold_existing",
                    "reason": "neutral_event_unconfirmed",
                    "risk_on_allowed": False,
                    "quant_confirmation": str(quant_confirmation),
                    "confirmation_score": float(confirmation_score),
                    "path_confirmation": str(path_confirmation),
                    "path_confirmation_score": float(path_confirmation_score),
                }
            return _finalize(float(current_exposure), "hold_existing", "neutral_event_unconfirmed")
        if regime == "stress":
            neutral_target = min(float(current_exposure), 0.25)
            return _finalize(neutral_target, "defensive_hold", "stress_regime_neutral_signal")
        if regime == "calm":
            neutral_target = max(float(current_exposure), 0.60)
            return _finalize(neutral_target, "measured_long", "calm_regime_neutral_signal")
        neutral_target = min(max(float(current_exposure), 0.40), 0.60)
        return _finalize(neutral_target, "hold_partial_risk", "normal_regime_neutral_signal")

    if regime == "stress":
        if signal_score <= -0.002 or negative_events > positive_events:
            return _finalize(0.0, "move_to_cash", "stress_regime_negative_signal")
        return _finalize(0.25, "defensive_hold", "stress_regime_limited_risk")

    if signal_score >= 0.003:
        return _finalize(1.0 if regime == "calm" else 0.75, "add_risk", "positive_event_signal")

    if signal_score <= -0.003:
        return _finalize(
            0.0 if negative_events >= positive_events else 0.25,
            "cut_risk",
            "negative_event_signal",
        )

    if regime == "calm":
        return _finalize(max(float(current_exposure), 0.6), "measured_long", "calm_regime_neutral_signal")

    return _finalize(min(max(float(current_exposure), 0.4), 0.6), "hold_partial_risk", "normal_regime_neutral_signal")

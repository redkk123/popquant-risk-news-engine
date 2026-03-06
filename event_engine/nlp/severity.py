from __future__ import annotations

from typing import Any


EVENT_BASE_SEVERITY = {
    "earnings": 0.55,
    "guidance": 0.72,
    "downgrade": 0.68,
    "upgrade": 0.48,
    "macro": 0.70,
    "legal": 0.78,
    "m_and_a": 0.75,
    "management": 0.58,
    "product": 0.44,
    "other": 0.30,
}

HIGH_INTENSITY_TERMS = {
    "warning",
    "crash",
    "plunge",
    "cuts guidance",
    "investigation",
    "antitrust",
    "misses",
    "slump",
    "downgrade",
    "lawsuit",
}


def _severity_label(score: float) -> str:
    if score >= 0.8:
        return "critical"
    if score >= 0.6:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"


def score_severity(
    document: dict[str, Any],
    *,
    event_type: str,
    event_confidence: float,
    link_confidence: float,
    polarity: float,
) -> dict[str, Any]:
    """Score how relevant and severe an article should be for risk workflows."""
    text = " ".join(
        [
            str(document.get("title", "")),
            str(document.get("description", "")),
            str(document.get("snippet", "")),
        ]
    ).lower()

    base = EVENT_BASE_SEVERITY.get(event_type, 0.30)
    intensity_hits = [term for term in HIGH_INTENSITY_TERMS if term in text]
    intensity_bonus = min(0.15, 0.04 * len(intensity_hits))
    symbol_bonus = min(0.10, 0.03 * len(document.get("symbols", []) or []))
    polarity_bonus = 0.15 * abs(polarity)
    confidence_bonus = 0.10 * min(event_confidence, link_confidence)

    score = min(
        1.0,
        base + intensity_bonus + symbol_bonus + polarity_bonus + confidence_bonus,
    )

    reasons = [
        f"base:{base:.2f}",
        f"event_confidence:{event_confidence:.2f}",
        f"link_confidence:{link_confidence:.2f}",
        f"abs_polarity:{abs(polarity):.2f}",
    ]
    if intensity_hits:
        reasons.append("intensity:" + ",".join(sorted(intensity_hits)))

    return {
        "severity": float(score),
        "severity_label": _severity_label(score),
        "severity_reasons": reasons,
    }


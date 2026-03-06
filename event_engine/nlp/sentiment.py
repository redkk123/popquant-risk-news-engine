from __future__ import annotations

from typing import Any


POSITIVE_TERMS = {
    "beats",
    "beat",
    "raises",
    "growth",
    "surge",
    "strong",
    "upgrade",
    "record",
    "expands",
    "optimistic",
}

NEGATIVE_TERMS = {
    "misses",
    "miss",
    "cuts",
    "cut",
    "downgrade",
    "lawsuit",
    "probe",
    "weak",
    "decline",
    "slump",
    "drops",
    "crash",
}


def score_polarity(document: dict[str, Any]) -> dict[str, Any]:
    """Score article polarity using deterministic lexical and entity cues."""
    text = " ".join(
        [
            str(document.get("title", "")),
            str(document.get("description", "")),
            str(document.get("snippet", "")),
        ]
    ).lower()

    positive_hits = [term for term in POSITIVE_TERMS if term in text]
    negative_hits = [term for term in NEGATIVE_TERMS if term in text]

    entity_scores = []
    for entity in document.get("entities", []) or []:
        score = entity.get("sentiment_score")
        if score is not None:
            try:
                entity_scores.append(float(score))
            except (TypeError, ValueError):
                continue

    lexical_score = 0.0
    if positive_hits or negative_hits:
        lexical_score = (len(positive_hits) - len(negative_hits)) / max(
            len(positive_hits) + len(negative_hits), 1
        )

    entity_component = sum(entity_scores) / len(entity_scores) if entity_scores else 0.0
    polarity = max(-1.0, min(1.0, 0.7 * lexical_score + 0.3 * entity_component))

    return {
        "polarity": float(polarity),
        "polarity_reasons": {
            "positive_hits": positive_hits,
            "negative_hits": negative_hits,
            "entity_sentiment_count": len(entity_scores),
        },
    }


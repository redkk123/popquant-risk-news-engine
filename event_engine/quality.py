from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from event_engine.source_policy import resolve_source_policy


PRESS_RELEASE_PATTERNS = [
    r"\bpress release\b",
    r"\bglobenewswire\b",
    r"\bnewswire\b",
]

GENERIC_CONTENT_PATTERNS = [
    r"\blong[- ]term buy\b",
    r"\bshould you\b",
    r"\bhere(?:'| i)?s the proof\b",
    r"\bhow to think about\b",
    r"\bstocks always rebound\b",
    r"\bpresents at\b",
    r"\bconference transcript\b",
    r"\btranscript\b",
    r"\bwhat to do about it\b",
    r"\bjoin in\b",
]


def _quality_label(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


def assess_event_quality(
    document: dict[str, Any],
    *,
    event_type: str,
    event_confidence: float,
    link_confidence: float,
    event_subtype: str | None = None,
    story_bucket: str | None = None,
    source_policy_path: str | Path | None = None,
) -> dict[str, Any]:
    """Score whether an event is reliable enough for the watchlist layer."""
    title = str(document.get("title", "")).strip()
    description = str(document.get("description", "")).strip()
    source = str(document.get("source", "")).strip().lower()
    url = str(document.get("url", "")).strip().lower()
    source_profile = resolve_source_policy(document, policy_path=source_policy_path)
    story_bucket = str(story_bucket or "").strip().lower() or None
    event_subtype = str(event_subtype or "").strip().lower() or None

    score = 0.35 * min(max(float(event_confidence), 0.0), 1.0)
    reasons = [f"event_confidence:{float(event_confidence):.2f}"]

    if event_type == "macro":
        score += 0.30
        reasons.append("macro_event_bonus")
    else:
        score += 0.35 * min(max(float(link_confidence), 0.0), 1.0)
        reasons.append(f"link_confidence:{float(link_confidence):.2f}")

    title_word_count = len(re.findall(r"\w+", title))
    if title_word_count >= 5:
        score += 0.10
        reasons.append("headline_anchor")

    if len(description) >= 60:
        score += 0.05
        reasons.append("description_present")

    score += source_profile["source_adjustment"]
    reasons.append(f"source_tier:{source_profile['source_tier']}")
    reasons.append(f"source_bucket:{source_profile['source_bucket']}")
    reasons.append(f"source_adjustment:{source_profile['source_adjustment']:+.2f}")

    low_signal_source = source_profile["source_low_signal"] or any(
        re.search(pattern, source) or re.search(pattern, url) for pattern in PRESS_RELEASE_PATTERNS
    )
    if low_signal_source:
        score -= 0.05
        reasons.append("low_signal_source_penalty")

    generic_content = any(re.search(pattern, title.lower()) for pattern in GENERIC_CONTENT_PATTERNS)
    if generic_content or event_type == "commentary":
        score -= 0.20
        reasons.append("generic_content_penalty")
    if story_bucket in {"opinion", "recap"}:
        score -= 0.10
        reasons.append(f"story_bucket_penalty:{story_bucket}")
    elif story_bucket == "market_color" and event_type != "macro":
        score -= 0.05
        reasons.append("story_bucket_penalty:market_color")

    score = min(1.0, max(0.0, score))
    quality_label = _quality_label(score)

    watchlist_base = (
        event_type not in {"other", "commentary"}
        and event_confidence >= 0.55
        and (event_type == "macro" or link_confidence >= 0.65)
        and quality_label != "low"
    )
    low_signal_bucket = source_profile["source_bucket"] in {"press_release_wire", "recap_aggregator", "opinion_commentary"}
    low_signal_gate_passed = (
        watchlist_base
        and quality_label == "high"
        and event_confidence >= 0.75
        and (event_type == "macro" or link_confidence >= 0.85)
    )
    if source_profile["source_block_watchlist"]:
        watchlist_eligible = False
        reasons.append("source_blocked")
    elif low_signal_bucket and not low_signal_gate_passed:
        watchlist_eligible = False
        reasons.append("low_signal_hard_filtered")
    elif source_profile["source_strict_watchlist"]:
        strict_gate_passed = watchlist_base and quality_label == "high" and event_confidence >= 0.75 and (
            event_type == "macro" or link_confidence >= 0.85
        )
        watchlist_eligible = strict_gate_passed
        reasons.append("source_strict_gate_passed" if strict_gate_passed else "source_strict_gate_failed")
    else:
        watchlist_eligible = watchlist_base
    if watchlist_eligible:
        reasons.append("watchlist_eligible")
    else:
        reasons.append("watchlist_filtered")

    return {
        "source_domain": source_profile["source_domain"],
        "source_tier": source_profile["source_tier"],
        "source_bucket": source_profile["source_bucket"],
        "source_adjustment": float(source_profile["source_adjustment"]),
        "source_rule_pattern": source_profile["source_rule_pattern"],
        "source_low_signal": bool(source_profile["source_low_signal"]),
        "quality_score": float(score),
        "quality_label": quality_label,
        "quality_reasons": reasons,
        "watchlist_eligible": bool(watchlist_eligible),
    }

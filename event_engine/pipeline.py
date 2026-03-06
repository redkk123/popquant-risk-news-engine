from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from event_engine.nlp.entity_linking import link_document_tickers, load_alias_table
from event_engine.nlp.sentiment import score_polarity
from event_engine.nlp.severity import score_severity
from event_engine.nlp.taxonomy import classify_event_type
from event_engine.parsing.dedupe import canonicalize_url, normalized_title_key
from event_engine.parsing.normalize import normalize_raw_record
from event_engine.quality import assess_event_quality
from event_engine.source_policy import DEFAULT_SOURCE_POLICY_PATH, resolve_source_policy
from event_engine.storage.repository import NewsRepository


def deduplicate_documents(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mark canonical documents as duplicates by URL or title key."""
    seen_urls: dict[str, str] = {}
    seen_titles: dict[str, str] = {}
    deduped: list[dict[str, Any]] = []

    for document in sorted(documents, key=lambda item: (item.get("published_at") or "", item["document_id"])):
        canonical_url = canonicalize_url(document.get("url", ""))
        title_key = normalized_title_key(document.get("title", ""))

        duplicate_of = None
        dedupe_reason = None
        if canonical_url and canonical_url in seen_urls:
            duplicate_of = seen_urls[canonical_url]
            dedupe_reason = "canonical_url"
        elif title_key in seen_titles:
            duplicate_of = seen_titles[title_key]
            dedupe_reason = "title_hash"

        if duplicate_of:
            document["is_duplicate"] = True
            document["duplicate_of"] = duplicate_of
            document["dedupe_reason"] = dedupe_reason
        else:
            document["is_duplicate"] = False
            document["duplicate_of"] = None
            document["dedupe_reason"] = None
            if canonical_url:
                seen_urls[canonical_url] = document["document_id"]
            seen_titles[title_key] = document["document_id"]

        deduped.append(document)

    return deduped


def build_events(
    canonical_documents: list[dict[str, Any]],
    *,
    alias_path: str | Path,
    source_policy_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Build event records from canonical documents."""
    alias_table = load_alias_table(alias_path)
    events: list[dict[str, Any]] = []

    for document in canonical_documents:
        if document.get("is_duplicate"):
            continue

        source_profile = resolve_source_policy(document, policy_path=source_policy_path)
        if source_profile.get("source_block_event_engine", False):
            continue

        link_result = link_document_tickers(document, alias_table)
        event_result = classify_event_type(document)
        polarity_result = score_polarity(document)
        severity_result = score_severity(
            document,
            event_type=event_result["event_type"],
            event_confidence=event_result["event_confidence"],
            link_confidence=link_result["link_confidence"],
            polarity=polarity_result["polarity"],
        )
        quality_result = assess_event_quality(
            document,
            event_type=event_result["event_type"],
            event_confidence=event_result["event_confidence"],
            link_confidence=link_result["link_confidence"],
            event_subtype=event_result.get("event_subtype"),
            story_bucket=event_result.get("story_bucket"),
            source_policy_path=source_policy_path,
        )

        event_id = document["document_id"].replace("doc_", "evt_", 1)
        events.append(
            {
                "event_id": event_id,
                "document_id": document["document_id"],
                "published_at": document.get("published_at"),
                "source": document.get("source"),
                "source_domain": quality_result["source_domain"],
                "source_tier": quality_result["source_tier"],
                "source_bucket": quality_result["source_bucket"],
                "source_adjustment": quality_result["source_adjustment"],
                "source_rule_pattern": quality_result["source_rule_pattern"],
                "source_low_signal": quality_result["source_low_signal"],
                "headline": document.get("title"),
                "summary": document.get("description") or document.get("snippet"),
                "tickers": link_result["tickers"],
                "link_confidence": link_result["link_confidence"],
                "link_details": link_result["link_details"],
                "provider_symbols": link_result.get("provider_symbols", []),
                "anchored_provider_symbols": link_result.get("anchored_provider_symbols", []),
                "unanchored_provider_symbols": link_result.get("unanchored_provider_symbols", []),
                "event_type": event_result["event_type"],
                "event_subtype": event_result.get("event_subtype"),
                "story_bucket": event_result.get("story_bucket"),
                "event_confidence": event_result["event_confidence"],
                "event_reasons": event_result["event_reasons"],
                "polarity": polarity_result["polarity"],
                "polarity_reasons": polarity_result["polarity_reasons"],
                "severity": severity_result["severity"],
                "severity_label": severity_result["severity_label"],
                "severity_reasons": severity_result["severity_reasons"],
                "quality_score": quality_result["quality_score"],
                "quality_label": quality_result["quality_label"],
                "quality_reasons": quality_result["quality_reasons"],
                "watchlist_eligible": quality_result["watchlist_eligible"],
            }
        )

    return events


def process_raw_documents(
    repository: NewsRepository,
    *,
    alias_path: str | Path,
    source_policy_path: str | Path | None = None,
) -> dict[str, Any]:
    """Normalize raw documents, deduplicate them, and build event outputs."""
    raw_documents = repository.load_raw_documents()
    canonical_documents: list[dict[str, Any]] = []

    for raw_record in raw_documents:
        canonical_documents.append(normalize_raw_record(raw_record))

    deduped = deduplicate_documents(canonical_documents)
    events = build_events(deduped, alias_path=alias_path, source_policy_path=source_policy_path)

    canonical_path = repository.write_canonical_documents(deduped)
    events_path = repository.write_events(events)

    return {
        "raw_documents": len(raw_documents),
        "canonical_documents": len(deduped),
        "duplicates": sum(1 for doc in deduped if doc.get("is_duplicate")),
        "events": len(events),
        "canonical_path": str(canonical_path),
        "events_path": str(events_path),
        "source_policy_path": str(Path(source_policy_path) if source_policy_path else DEFAULT_SOURCE_POLICY_PATH),
    }


def export_events_csv(repository: NewsRepository, output_path: str | Path) -> Path:
    """Export processed events to CSV for easier downstream inspection."""
    frame = repository.load_events_frame()
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path

from __future__ import annotations

RAW_DOCUMENT_FIELDS = (
    "document_id",
    "provider",
    "provider_document_id",
    "fetched_at",
    "raw_payload_path",
)

CANONICAL_DOCUMENT_FIELDS = (
    "document_id",
    "provider",
    "provider_document_id",
    "source",
    "published_at",
    "url",
    "canonical_url",
    "title",
    "description",
    "snippet",
    "language",
    "symbols",
    "entity_names",
    "fetched_at",
    "raw_payload_path",
    "is_duplicate",
    "duplicate_of",
    "dedupe_reason",
)

EVENT_FIELDS = (
    "event_id",
    "document_id",
    "published_at",
    "source",
    "headline",
    "summary",
    "tickers",
    "link_confidence",
    "event_type",
    "event_confidence",
    "polarity",
    "severity",
    "severity_label",
    "severity_reasons",
    "event_reasons",
)


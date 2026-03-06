from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from event_engine.pipeline import build_events, deduplicate_documents
from event_engine.parsing.normalize import normalize_raw_record


def load_labeled_events(path: str | Path) -> list[dict[str, Any]]:
    """Load a JSONL labeled dataset for event evaluation."""
    labeled_path = Path(path)
    rows: list[dict[str, Any]] = []
    with labeled_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def evaluate_news_engine(
    labeled_records: list[dict[str, Any]],
    *,
    alias_path: str | Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Evaluate event type and ticker linking against a labeled set."""
    canonical_documents = []
    labels_by_document: dict[str, dict[str, Any]] = {}

    for record in labeled_records:
        raw_article = record["raw_article"]
        canonical = normalize_raw_record(
            {
                "provider": record.get("provider", "marketaux"),
                "payload": raw_article,
                "fetched_at": record.get("fetched_at", "1970-01-01T00:00:00Z"),
                "raw_payload_path": record.get("raw_payload_path", "fixture"),
            }
        )
        canonical_documents.append(canonical)
        labels_by_document[canonical["document_id"]] = {
            "expected_event_type": record["expected_event_type"],
            "expected_tickers": sorted(record.get("expected_tickers", [])),
        }

    canonical_documents = deduplicate_documents(canonical_documents)
    events = build_events(canonical_documents, alias_path=alias_path)

    rows = []
    event_hits = 0
    ticker_hits = 0
    for event in events:
        label = labels_by_document[event["document_id"]]
        predicted_tickers = sorted(event["tickers"])
        expected_tickers = label["expected_tickers"]
        event_match = event["event_type"] == label["expected_event_type"]
        ticker_match = predicted_tickers == expected_tickers
        event_hits += int(event_match)
        ticker_hits += int(ticker_match)

        rows.append(
            {
                "document_id": event["document_id"],
                "headline": event["headline"],
                "expected_event_type": label["expected_event_type"],
                "predicted_event_type": event["event_type"],
                "event_match": event_match,
                "expected_tickers": expected_tickers,
                "predicted_tickers": predicted_tickers,
                "ticker_match": ticker_match,
            }
        )

    detail = pd.DataFrame(rows)
    n = len(rows)
    summary = {
        "observations": n,
        "event_type_accuracy": (event_hits / n) if n else 0.0,
        "ticker_link_accuracy": (ticker_hits / n) if n else 0.0,
    }
    return detail, summary

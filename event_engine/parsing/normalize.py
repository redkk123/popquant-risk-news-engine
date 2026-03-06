from __future__ import annotations

import hashlib
from typing import Any

from event_engine.parsing.dedupe import canonicalize_url


def _coerce_symbols(article: dict[str, Any]) -> list[str]:
    symbols = set()
    for entity in article.get("entities", []) or []:
        symbol = str(entity.get("symbol", "")).upper().strip()
        if symbol:
            symbols.add(symbol)
    for symbol in article.get("symbols", []) or []:
        symbol_text = str(symbol).upper().strip()
        if symbol_text:
            symbols.add(symbol_text)
    return sorted(symbols)


def _coerce_entity_names(article: dict[str, Any]) -> list[str]:
    names = set()
    for entity in article.get("entities", []) or []:
        name = str(entity.get("name", "")).strip()
        if name:
            names.add(name)
    return sorted(names)


def _coerce_source(article: dict[str, Any]) -> str:
    source = article.get("source", "")
    if isinstance(source, dict):
        source = source.get("name") or source.get("domain") or source.get("title") or ""
    return str(source).strip() or "unknown"


def _coerce_description(article: dict[str, Any]) -> str:
    for key in ("description", "summary", "snippet"):
        value = str(article.get(key, "")).strip()
        if value:
            return value
    return ""


def _coerce_alpha_symbols(article: dict[str, Any]) -> list[str]:
    symbols = set()
    for row in article.get("ticker_sentiment", []) or []:
        symbol = str(row.get("ticker", "")).upper().strip()
        if symbol:
            symbols.add(symbol)
    return sorted(symbols)


def _coerce_alpha_entities(article: dict[str, Any]) -> list[dict[str, Any]]:
    entities = []
    for row in article.get("ticker_sentiment", []) or []:
        symbol = str(row.get("ticker", "")).upper().strip()
        if not symbol:
            continue
        entities.append(
            {
                "symbol": symbol,
                "name": symbol,
                "sentiment_score": row.get("ticker_sentiment_score"),
                "sentiment_label": row.get("ticker_sentiment_label"),
            }
        )
    return entities


def _coerce_alpha_published_at(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 15:
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}T{text[9:11]}:{text[11:13]}:{text[13:15]}Z"
    return text


def build_document_id(provider: str, provider_document_id: str | None, url: str, title: str) -> str:
    """Build a stable local document id."""
    base = provider_document_id or canonicalize_url(url) or title
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"doc_{provider}_{digest}"


def normalize_marketaux_article(
    article: dict[str, Any],
    *,
    fetched_at: str,
    raw_payload_path: str,
) -> dict[str, Any]:
    """Normalize a Marketaux article payload into a canonical document schema."""
    provider_document_id = (
        article.get("uuid")
        or article.get("id")
        or article.get("slug")
        or article.get("url")
    )
    title = str(article.get("title", "")).strip()
    url = str(article.get("url", "")).strip()
    canonical_url = canonicalize_url(url)

    return {
        "document_id": build_document_id(
            "marketaux",
            str(provider_document_id) if provider_document_id else None,
            url,
            title,
        ),
        "provider": "marketaux",
        "provider_document_id": provider_document_id,
        "source": str(article.get("source", "")).strip() or "unknown",
        "published_at": article.get("published_at"),
        "url": url,
        "canonical_url": canonical_url,
        "title": title,
        "description": str(article.get("description", "")).strip(),
        "snippet": str(article.get("snippet", "")).strip(),
        "language": str(article.get("language", "")).strip() or "en",
        "symbols": _coerce_symbols(article),
        "entity_names": _coerce_entity_names(article),
        "entities": article.get("entities", []) or [],
        "fetched_at": fetched_at,
        "raw_payload_path": raw_payload_path,
        "is_duplicate": False,
        "duplicate_of": None,
        "dedupe_reason": None,
    }


def normalize_thenewsapi_article(
    article: dict[str, Any],
    *,
    fetched_at: str,
    raw_payload_path: str,
) -> dict[str, Any]:
    provider_document_id = article.get("uuid") or article.get("id") or article.get("url")
    title = str(article.get("title", "")).strip()
    url = str(article.get("url", "")).strip()
    canonical_url = canonicalize_url(url)

    return {
        "document_id": build_document_id(
            "thenewsapi",
            str(provider_document_id) if provider_document_id else None,
            url,
            title,
        ),
        "provider": "thenewsapi",
        "provider_document_id": provider_document_id,
        "source": _coerce_source(article),
        "published_at": article.get("published_at"),
        "url": url,
        "canonical_url": canonical_url,
        "title": title,
        "description": _coerce_description(article),
        "snippet": str(article.get("snippet", "")).strip(),
        "language": str(article.get("language", "")).strip() or "en",
        "symbols": _coerce_symbols(article),
        "entity_names": _coerce_entity_names(article),
        "entities": article.get("entities", []) or [],
        "fetched_at": fetched_at,
        "raw_payload_path": raw_payload_path,
        "is_duplicate": False,
        "duplicate_of": None,
        "dedupe_reason": None,
    }


def normalize_alphavantage_article(
    article: dict[str, Any],
    *,
    fetched_at: str,
    raw_payload_path: str,
) -> dict[str, Any]:
    provider_document_id = article.get("url") or article.get("title")
    title = str(article.get("title", "")).strip()
    url = str(article.get("url", "")).strip()
    canonical_url = canonicalize_url(url)
    entities = _coerce_alpha_entities(article)

    return {
        "document_id": build_document_id(
            "alphavantage",
            str(provider_document_id) if provider_document_id else None,
            url,
            title,
        ),
        "provider": "alphavantage",
        "provider_document_id": provider_document_id,
        "source": _coerce_source(article),
        "published_at": _coerce_alpha_published_at(article.get("time_published")),
        "url": url,
        "canonical_url": canonical_url,
        "title": title,
        "description": _coerce_description(article),
        "snippet": str(article.get("summary", "")).strip(),
        "language": "en",
        "symbols": _coerce_alpha_symbols(article),
        "entity_names": sorted({entity["name"] for entity in entities}),
        "entities": entities,
        "fetched_at": fetched_at,
        "raw_payload_path": raw_payload_path,
        "is_duplicate": False,
        "duplicate_of": None,
        "dedupe_reason": None,
    }


def normalize_raw_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    provider = str(raw_record.get("provider", "marketaux")).strip().lower()
    payload = raw_record["payload"]
    fetched_at = raw_record["fetched_at"]
    raw_payload_path = raw_record["raw_payload_path"]

    if provider == "marketaux":
        return normalize_marketaux_article(
            payload,
            fetched_at=fetched_at,
            raw_payload_path=raw_payload_path,
        )
    if provider == "thenewsapi":
        return normalize_thenewsapi_article(
            payload,
            fetched_at=fetched_at,
            raw_payload_path=raw_payload_path,
        )
    if provider == "alphavantage":
        return normalize_alphavantage_article(
            payload,
            fetched_at=fetched_at,
            raw_payload_path=raw_payload_path,
        )
    raise ValueError(f"Unsupported raw provider for normalization: {provider}")

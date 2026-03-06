from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from event_engine.ingestion.providers import (
    NewsProvider,
    NewsProviderError,
    NewsProviderQuotaError,
    NewsProviderUnavailableError,
    build_news_provider,
)
from event_engine.parsing.normalize import build_document_id
from event_engine.storage.repository import NewsRepository


def _chunk_symbols(symbols: list[str] | None, batch_size: int) -> list[list[str] | None]:
    normalized = [str(symbol).upper() for symbol in (symbols or []) if str(symbol).strip()]
    if not normalized:
        return [None]
    if batch_size < 1:
        raise ValueError("symbol_batch_size must be at least 1")
    return [normalized[index : index + batch_size] for index in range(0, len(normalized), batch_size)]


def _coerce_provider_names(providers: list[str] | tuple[str, ...] | None) -> list[str]:
    requested = providers or ["marketaux"]
    normalized: list[str] = []
    for name in requested:
        provider_name = str(name).strip().lower()
        if provider_name and provider_name not in normalized:
            normalized.append(provider_name)
    if not normalized:
        raise ValueError("At least one provider must be specified.")
    return normalized


def _summarize_failures(failures: list[dict[str, Any]]) -> str:
    if not failures:
        return "none"
    summary_parts: list[str] = []
    for item in failures:
        provider = str(item.get("provider", "unknown"))
        error_type = str(item.get("error_type", "UnknownError"))
        symbols = ",".join(item.get("symbols", []) or [])
        summary_parts.append(f"{provider}:{error_type}:{symbols}")
    return "; ".join(summary_parts)


def _build_raw_records(
    articles: list[dict[str, Any]],
    *,
    provider: NewsProvider,
    fetched_at: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for article in articles:
        provider_document_id = provider.provider_document_id(article)
        document_id = build_document_id(
            provider.name,
            provider_document_id,
            str(article.get("url", "")),
            str(article.get("title", "")),
        )
        records.append(
            {
                "document_id": document_id,
                "provider": provider.name,
                "provider_document_id": provider_document_id,
                "fetched_at": fetched_at,
                "payload": article,
            }
        )
    return records


def _fetch_batch_pages(
    provider: NewsProvider,
    *,
    symbol_batch: list[str] | None,
    language: str,
    published_after: str | None,
    published_before: str | None,
    limit: int,
    max_pages: int,
    fetched_at: str,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    page_article_counts: list[dict[str, Any]] = []
    pages_fetched = 0

    for page in range(1, max_pages + 1):
        payload = provider.fetch_page(
            symbols=symbol_batch,
            language=language,
            published_after=published_after,
            published_before=published_before,
            limit=limit,
            page=page,
        )
        articles = payload.articles or []
        if not articles:
            break
        page_article_counts.append(
            {
                "provider": provider.name,
                "page": page,
                "articles": int(len(articles)),
                "symbols": ",".join(symbol_batch or []),
            }
        )
        records.extend(_build_raw_records(articles, provider=provider, fetched_at=fetched_at))
        pages_fetched += 1
        if not provider.supports_paging:
            break

    return {
        "records": records,
        "page_article_counts": page_article_counts,
        "pages_fetched": pages_fetched,
        "resolved_batches": [list(symbol_batch or [])],
    }


def _fetch_batch_with_split(
    provider: NewsProvider,
    *,
    symbol_batch: list[str] | None,
    language: str,
    published_after: str | None,
    published_before: str | None,
    limit: int,
    max_pages: int,
    fetched_at: str,
) -> dict[str, Any]:
    try:
        return _fetch_batch_pages(
            provider,
            symbol_batch=symbol_batch,
            language=language,
            published_after=published_after,
            published_before=published_before,
            limit=limit,
            max_pages=max_pages,
            fetched_at=fetched_at,
        )
    except NewsProviderQuotaError:
        if provider.supports_symbol_batch_split and symbol_batch and len(symbol_batch) > 1:
            midpoint = max(1, len(symbol_batch) // 2)
            left = _fetch_batch_with_split(
                provider,
                symbol_batch=symbol_batch[:midpoint],
                language=language,
                published_after=published_after,
                published_before=published_before,
                limit=limit,
                max_pages=max_pages,
                fetched_at=fetched_at,
            )
            right = _fetch_batch_with_split(
                provider,
                symbol_batch=symbol_batch[midpoint:],
                language=language,
                published_after=published_after,
                published_before=published_before,
                limit=limit,
                max_pages=max_pages,
                fetched_at=fetched_at,
            )
            return {
                "records": left["records"] + right["records"],
                "page_article_counts": left["page_article_counts"] + right["page_article_counts"],
                "pages_fetched": int(left["pages_fetched"]) + int(right["pages_fetched"]),
                "resolved_batches": left["resolved_batches"] + right["resolved_batches"],
            }
        raise


def sync_news(
    repository: NewsRepository,
    *,
    providers: list[str] | tuple[str, ...] | None = None,
    provider_tokens: dict[str, str | None] | None = None,
    symbols: list[str] | None = None,
    language: str = "en",
    published_after: str | None = None,
    published_before: str | None = None,
    limit: int = 3,
    max_pages: int = 1,
    symbol_batch_size: int = 5,
) -> dict[str, Any]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    provider_sequence = _coerce_provider_names(providers)
    provider_tokens = {str(key).lower(): value for key, value in (provider_tokens or {}).items()}
    unresolved_batches = _chunk_symbols(symbols, symbol_batch_size)

    stored_records: list[dict[str, Any]] = []
    page_article_counts: list[dict[str, Any]] = []
    provider_stats: dict[str, dict[str, Any]] = {}
    provider_errors: list[dict[str, Any]] = []
    total_pages_fetched = 0
    final_failed_batches: list[dict[str, Any]] = []

    for provider_name in provider_sequence:
        provider_stats[provider_name] = {
            "attempted_batches": 0,
            "resolved_batches": 0,
            "pages_fetched": 0,
            "articles_seen": 0,
            "failed_batches": 0,
            "status": "unused",
        }
        if not unresolved_batches:
            break

        try:
            provider = build_news_provider(
                provider_name,
                api_token=provider_tokens.get(provider_name),
            )
        except NewsProviderUnavailableError as exc:
            provider_stats[provider_name]["status"] = "unavailable"
            provider_errors.append(
                {
                    "provider": provider_name,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "symbols": sorted({symbol for batch in unresolved_batches for symbol in (batch or [])}),
                }
            )
            continue

        next_unresolved: list[list[str] | None] = []
        provider_stats[provider_name]["status"] = "active"
        for symbol_batch in unresolved_batches:
            provider_stats[provider_name]["attempted_batches"] += 1
            try:
                batch_result = _fetch_batch_with_split(
                    provider,
                    symbol_batch=symbol_batch,
                    language=language,
                    published_after=published_after,
                    published_before=published_before,
                    limit=limit,
                    max_pages=max_pages,
                    fetched_at=fetched_at,
                )
                total_pages_fetched += int(batch_result["pages_fetched"])
                provider_stats[provider_name]["pages_fetched"] += int(batch_result["pages_fetched"])
                provider_stats[provider_name]["resolved_batches"] += int(len(batch_result["resolved_batches"]))
                provider_stats[provider_name]["articles_seen"] += int(len(batch_result["records"]))
                stored_records.extend(batch_result["records"])
                page_article_counts.extend(batch_result["page_article_counts"])
            except NewsProviderError as exc:
                provider_stats[provider_name]["failed_batches"] += 1
                failure_entry = {
                    "provider": provider_name,
                    "symbols": list(symbol_batch or []),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                provider_errors.append(failure_entry)
                next_unresolved.append(symbol_batch)

        unresolved_batches = next_unresolved
        if provider_stats[provider_name]["articles_seen"] > 0 and provider_stats[provider_name]["failed_batches"] > 0:
            provider_stats[provider_name]["status"] = "partial_success"
        elif provider_stats[provider_name]["articles_seen"] > 0:
            provider_stats[provider_name]["status"] = "success"
        elif provider_stats[provider_name]["failed_batches"] > 0:
            provider_stats[provider_name]["status"] = "failed"

    for batch in unresolved_batches:
        final_failed_batches.append(
            {
                "provider": provider_sequence[-1],
                "symbols": list(batch or []),
                "error_type": "UnresolvedBatch",
                "error": "Batch unresolved after exhausting provider chain.",
            }
        )

    resolved_batch_count = int(
        sum(int(details.get("resolved_batches", 0)) for details in provider_stats.values())
    )

    if not stored_records and final_failed_batches:
        raise RuntimeError(
            "News sync failed for all providers and symbol batches. "
            f"Providers attempted: {', '.join(provider_sequence)}. "
            f"Failures: {_summarize_failures(provider_errors + final_failed_batches)}"
        )
    if not stored_records and provider_errors and resolved_batch_count == 0:
        raise RuntimeError(
            "News sync failed for all providers and symbol batches. "
            f"Providers attempted: {', '.join(provider_sequence)}. "
            f"Failures: {_summarize_failures(provider_errors)}"
        )

    stats = repository.upsert_raw_documents(stored_records)
    providers_used = [
        name for name, details in provider_stats.items() if int(details.get("articles_seen", 0)) > 0
    ]
    failed_batches = provider_errors + final_failed_batches

    return {
        "provider": providers_used[0] if len(providers_used) == 1 else "multi",
        "providers_requested": provider_sequence,
        "providers_used": providers_used,
        "fetched_at": fetched_at,
        "request": {
            "symbols": sorted({symbol.upper() for symbol in (symbols or [])}),
            "language": language,
            "published_after": published_after,
            "published_before": published_before,
            "limit": int(limit),
            "max_pages": int(max_pages),
            "symbol_batch_size": int(symbol_batch_size),
            "symbol_batch_count": int(len(_chunk_symbols(symbols, symbol_batch_size))),
        },
        "pages_fetched": int(total_pages_fetched),
        "page_article_counts": page_article_counts,
        "resolved_symbol_batch_count": resolved_batch_count,
        "failed_batch_count": int(len(failed_batches)),
        "failed_symbols": sorted({symbol for batch in failed_batches for symbol in batch.get("symbols", [])}),
        "partial_success": bool(failed_batches and stored_records),
        "failed_batches": failed_batches,
        "provider_stats": provider_stats,
        "articles_seen": len(stored_records),
        **stats,
    }


def sync_marketaux_news(
    repository: NewsRepository,
    *,
    symbols: list[str] | None = None,
    language: str = "en",
    published_after: str | None = None,
    published_before: str | None = None,
    limit: int = 3,
    max_pages: int = 1,
    api_token: str | None = None,
    symbol_batch_size: int = 5,
) -> dict[str, Any]:
    return sync_news(
        repository,
        providers=["marketaux"],
        provider_tokens={"marketaux": api_token},
        symbols=symbols,
        language=language,
        published_after=published_after,
        published_before=published_before,
        limit=limit,
        max_pages=max_pages,
        symbol_batch_size=symbol_batch_size,
    )


def ingest_fixture(
    repository: NewsRepository,
    fixture_path: str | Path,
    *,
    provider: str = "marketaux",
) -> dict[str, Any]:
    """Load local fixture articles into raw storage for deterministic demos and tests."""
    import json

    provider_name = str(provider).strip().lower()
    path = Path(fixture_path)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    provider_instance = build_news_provider(provider_name, api_token="fixture-token")
    if isinstance(payload, dict):
        articles = payload.get("data", payload.get("feed", payload))
    else:
        articles = payload
    fetched_at = datetime.now(timezone.utc).isoformat()
    records = _build_raw_records(
        [article for article in articles if isinstance(article, dict)],
        provider=provider_instance,
        fetched_at=fetched_at,
    )

    stats = repository.upsert_raw_documents(records)
    return {
        "provider": f"fixture:{provider_name}",
        "fetched_at": fetched_at,
        "articles_seen": len(records),
        **stats,
    }

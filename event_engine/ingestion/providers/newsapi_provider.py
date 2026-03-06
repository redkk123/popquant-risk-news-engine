from __future__ import annotations

import os
import re
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

from event_engine.ingestion.providers.base import (
    NewsProvider,
    NewsProviderAuthError,
    NewsProviderQuotaError,
    NewsProviderTransientError,
    ProviderFetchResult,
)
from event_engine.nlp.entity_linking import load_alias_table
from event_engine.redaction import redact_text

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ALIAS_PATH = PROJECT_ROOT / "config" / "news_entity_aliases.csv"
MACRO_PATTERNS = [
    r"\bwall street\b",
    r"\bstock(?:s| market)?\b",
    r"\bmarket(?:s)?\b",
    r"\bnasdaq\b",
    r"\bs&p 500\b",
    r"\bdow jones\b",
    r"\bfederal reserve\b",
    r"\bfed\b",
    r"\binflation\b",
    r"\btariff(?:s)?\b",
    r"\byield(?:s)?\b",
    r"\btreasur(?:y|ies)\b",
    r"\bcrude\b",
    r"\boil\b",
    r"\bgeopolitic(?:al)?\b",
]
MACRO_QUERY = '"Wall Street" OR "stock market" OR inflation OR oil OR Fed'
BROAD_MARKET_SYMBOLS = {"SPY", "QQQ", "DIA", "IWM", "XLE", "XLF", "XLK"}


def _text_blob(article: dict[str, Any]) -> str:
    return " ".join(
        [
            str(article.get("title", "")),
            str(article.get("description", "")),
            str(article.get("content", "")),
            str((article.get("source") or {}).get("name", "")),
        ]
    ).lower()


@lru_cache(maxsize=1)
def _aliases_by_ticker() -> dict[str, list[str]]:
    aliases: dict[str, list[str]] = {}
    for row in load_alias_table(ALIAS_PATH):
        aliases.setdefault(row["ticker"], []).append(row["alias"])
    return aliases


def _contains_phrase(text: str, phrase: str) -> bool:
    candidate = phrase.strip().lower()
    if not candidate:
        return False
    return bool(re.search(r"\b" + re.escape(candidate) + r"\b", text))


def _looks_like_macro_story(text: str) -> bool:
    return any(re.search(pattern, text) for pattern in MACRO_PATTERNS)


def _matches_requested_universe(text: str, symbols: list[str] | None) -> bool:
    if not symbols:
        return False

    aliases_by_ticker = _aliases_by_ticker()
    for symbol in symbols:
        ticker = str(symbol).upper().strip()
        if not ticker:
            continue
        if _contains_phrase(text, ticker):
            return True
        for alias in aliases_by_ticker.get(ticker, []):
            if _contains_phrase(text, alias):
                return True
    return False


def _symbol_alias_terms(symbols: list[str] | None, *, limit: int = 4) -> list[str]:
    if not symbols:
        return []
    aliases_by_ticker = _aliases_by_ticker()
    terms: list[str] = []
    for symbol in symbols:
        ticker = str(symbol).upper().strip()
        if not ticker:
            continue
        aliases = [alias for alias in aliases_by_ticker.get(ticker, []) if alias]
        preferred = aliases[:2] if aliases else [ticker]
        for alias in preferred:
            candidate = str(alias).strip()
            if candidate and candidate not in terms:
                terms.append(candidate)
            if len(terms) >= limit:
                return terms
    return terms


def _build_query(symbols: list[str] | None) -> str:
    alias_terms = _symbol_alias_terms(symbols)
    normalized_symbols = {str(symbol).upper().strip() for symbol in (symbols or []) if str(symbol).strip()}

    if alias_terms:
        quoted = [f'"{term}"' if " " in term else term for term in alias_terms]
        query = " OR ".join(quoted)
        if normalized_symbols & BROAD_MARKET_SYMBOLS:
            return f"({query}) OR ({MACRO_QUERY})"
        return query
    return MACRO_QUERY


def _filter_relevant_articles(
    articles: list[dict[str, Any]],
    *,
    symbols: list[str] | None,
) -> list[dict[str, Any]]:
    if not symbols:
        return [article for article in articles if isinstance(article, dict)]

    filtered: list[dict[str, Any]] = []
    for article in articles:
        if not isinstance(article, dict):
            continue
        text = _text_blob(article)
        if _matches_requested_universe(text, symbols):
            filtered.append(article)
            continue
        if _looks_like_macro_story(text):
            filtered.append(article)
    return filtered


class NewsApiOrgProvider(NewsProvider):
    name = "newsapi"
    env_var = "NEWSAPI_API_KEY"
    supports_paging = True
    supports_symbol_batch_split = False
    base_url = "https://newsapi.org/v2/everything"

    def __init__(
        self,
        api_token: str | None = None,
        *,
        timeout: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        super().__init__(api_token=api_token)
        self.ensure_available()
        self.timeout = timeout
        self.session = session or requests.Session()

    def _read_token(self) -> str | None:
        return os.getenv("NEWSAPI_API_KEY") or os.getenv("NEWSAPI_API_TOKEN")

    def _raise_for_response(self, response: requests.Response) -> None:
        if response.status_code in (401, 403):
            raise NewsProviderAuthError(f"{self.name} auth failed: {redact_text(response.text)}")
        if response.status_code in (426, 429):
            raise NewsProviderQuotaError(f"{self.name} quota failed: {redact_text(response.text)}")
        if response.status_code >= 500:
            raise NewsProviderTransientError(f"{self.name} upstream failed: {redact_text(response.text)}")
        response.raise_for_status()

    def _request_articles(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        headers = {"X-Api-Key": self.api_token}
        for attempt in range(1, 4):
            try:
                response = self.session.get(self.base_url, params=params, headers=headers, timeout=self.timeout)
                self._raise_for_response(response)
                payload = response.json()
                if payload.get("status") == "error":
                    code = str(payload.get("code", ""))
                    message = redact_text(str(payload.get("message", "")))
                    lowered = code.lower()
                    if lowered in {"apiKeyInvalid".lower(), "apiKeyMissing".lower(), "apiKeyDisabled".lower()}:
                        raise NewsProviderAuthError(f"{self.name} auth failed: {message}")
                    if lowered in {"rateLimited".lower(), "maximumResultsReached".lower()}:
                        raise NewsProviderQuotaError(f"{self.name} quota failed: {message}")
                    raise NewsProviderTransientError(f"{self.name} request failed: {message}")
                return payload.get("articles", []) or []
            except (NewsProviderAuthError, NewsProviderQuotaError):
                raise
            except requests.RequestException as exc:
                last_error = exc
                if attempt == 3:
                    break
                time.sleep(float(attempt))
            except ValueError as exc:
                raise NewsProviderTransientError(
                    f"{self.name} response parse failed: {redact_text(str(exc))}"
                ) from exc
        raise NewsProviderTransientError(
            f"{self.name} fetch failed after 3 attempts: {redact_text(str(last_error))}"
        )

    def fetch_page(
        self,
        *,
        symbols: list[str] | None = None,
        language: str = "en",
        published_after: str | None = None,
        published_before: str | None = None,
        limit: int = 3,
        page: int = 1,
    ) -> ProviderFetchResult:
        params: dict[str, Any] = {
            "q": _build_query(symbols),
            "language": language,
            "sortBy": "publishedAt",
            "pageSize": int(limit),
            "page": int(page),
            "searchIn": "title,description,content",
        }
        if published_after:
            params["from"] = published_after
        if published_before:
            params["to"] = published_before

        articles = self._request_articles(params)
        filtered_articles = _filter_relevant_articles(articles, symbols=symbols)
        page_meta: dict[str, Any] = {
            "article_count": len(articles),
            "kept_count": len(filtered_articles),
            "query": params["q"],
            "used_macro_fallback": False,
        }

        if symbols and not filtered_articles and params["q"] != MACRO_QUERY:
            fallback_params = dict(params)
            fallback_params["q"] = MACRO_QUERY
            fallback_articles = self._request_articles(fallback_params)
            fallback_filtered = _filter_relevant_articles(fallback_articles, symbols=symbols)
            page_meta.update(
                {
                    "used_macro_fallback": True,
                    "fallback_query": fallback_params["q"],
                    "fallback_article_count": len(fallback_articles),
                    "fallback_kept_count": len(fallback_filtered),
                }
            )
            if fallback_filtered:
                filtered_articles = fallback_filtered

        return ProviderFetchResult(
            articles=filtered_articles,
            page=page,
            page_meta=page_meta,
        )

    def provider_document_id(self, article: dict[str, Any]) -> str | None:
        value = article.get("url") or article.get("title")
        return str(value) if value not in (None, "") else None

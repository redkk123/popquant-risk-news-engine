from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path
import re
import time
from typing import Any

import pandas as pd
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
ALPHA_TOPIC_FALLBACKS = ["financial_markets", "economy_macro"]
MACRO_TOPIC_LABELS = {
    "economy - macro",
    "economy_macro",
    "financial markets",
    "financial_markets",
}
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


def _format_alpha_timestamp(value: str) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.strftime("%Y%m%dT%H%M")


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


def _text_blob(article: dict[str, Any]) -> str:
    return " ".join(
        [
            str(article.get("title", "")),
            str(article.get("summary", "")),
            str(article.get("source", "")),
        ]
    ).lower()


def _coerce_topic_names(article: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for row in article.get("topics", []) or []:
        if isinstance(row, dict):
            topic_name = row.get("topic")
        else:
            topic_name = row
        normalized = str(topic_name or "").strip().lower()
        if normalized:
            names.add(normalized)
    return names


def _coerce_ticker_symbols(article: dict[str, Any]) -> set[str]:
    tickers: set[str] = set()
    for row in article.get("ticker_sentiment", []) or []:
        ticker = str(row.get("ticker", "")).upper().strip()
        if ticker:
            tickers.add(ticker)
    return tickers


def _matches_requested_universe(article: dict[str, Any], *, symbols: list[str] | None, text: str) -> bool:
    if not symbols:
        return False

    article_tickers = _coerce_ticker_symbols(article)
    requested = {str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()}
    if article_tickers & requested:
        return True

    aliases_by_ticker = _aliases_by_ticker()
    for ticker in requested:
        for alias in aliases_by_ticker.get(ticker, []):
            if _contains_phrase(text, alias):
                return True
    return False


def _looks_like_macro_story(article: dict[str, Any], *, text: str) -> bool:
    topic_names = _coerce_topic_names(article)
    if topic_names & MACRO_TOPIC_LABELS:
        return True
    return any(re.search(pattern, text) for pattern in MACRO_PATTERNS)


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
        if _matches_requested_universe(article, symbols=symbols, text=text):
            filtered.append(article)
            continue
        if _looks_like_macro_story(article, text=text):
            filtered.append(article)
    return filtered


class AlphaVantageProvider(NewsProvider):
    name = "alphavantage"
    env_var = "ALPHAVANTAGE_API_KEY"
    supports_paging = False
    supports_symbol_batch_split = False
    base_url = "https://www.alphavantage.co/query"

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
        self.min_request_interval_seconds = 1.1
        self._last_request_started_at = 0.0

    def _read_token(self) -> str | None:
        return os.getenv("ALPHAVANTAGE_API_KEY") or os.getenv("ALPHAVANTAGE_API_TOKEN")

    def _raise_for_response(self, response: requests.Response) -> None:
        if response.status_code in (401, 403):
            raise NewsProviderAuthError(f"{self.name} auth failed: {redact_text(response.text)}")
        if response.status_code in (402, 429):
            raise NewsProviderQuotaError(f"{self.name} quota failed: {redact_text(response.text)}")
        if response.status_code >= 500:
            raise NewsProviderTransientError(f"{self.name} upstream failed: {redact_text(response.text)}")
        response.raise_for_status()

    def _request_articles(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                elapsed = time.monotonic() - self._last_request_started_at
                if self._last_request_started_at and elapsed < self.min_request_interval_seconds:
                    time.sleep(self.min_request_interval_seconds - elapsed)
                self._last_request_started_at = time.monotonic()
                response = self.session.get(self.base_url, params=params, timeout=self.timeout)
                self._raise_for_response(response)
                payload = response.json()
                note_text = f"{payload.get('Note', '')} {payload.get('Information', '')}".strip()
                if note_text:
                    lowered = note_text.lower()
                    if "frequency" in lowered or "rate limit" in lowered or "call" in lowered:
                        raise NewsProviderQuotaError(f"{self.name} quota failed: {redact_text(note_text)}")
                    raise NewsProviderTransientError(f"{self.name} note: {redact_text(note_text)}")
                if payload.get("Error Message"):
                    raise NewsProviderAuthError(
                        f"{self.name} request failed: {redact_text(str(payload['Error Message']))}"
                    )
                articles = payload.get("feed", []) or []
                return [article for article in articles if isinstance(article, dict)]
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

    def _build_base_params(
        self,
        *,
        published_after: str | None,
        published_before: str | None,
        limit: int,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "function": "NEWS_SENTIMENT",
            "apikey": self.api_token,
            "limit": max(int(limit), 1),
            "sort": "LATEST",
        }
        if published_after:
            params["time_from"] = _format_alpha_timestamp(published_after)
        if published_before:
            params["time_to"] = _format_alpha_timestamp(published_before)
        return params

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
        del language
        if page > 1:
            return ProviderFetchResult(articles=[], page=page, page_meta={"article_count": 0})

        params = self._build_base_params(
            published_after=published_after,
            published_before=published_before,
            limit=limit,
        )
        if symbols:
            params["tickers"] = ",".join(sorted({str(symbol).upper() for symbol in symbols}))

        articles = self._request_articles(params)
        filtered_articles = _filter_relevant_articles(articles, symbols=symbols)
        page_meta: dict[str, Any] = {
            "article_count": len(articles),
            "kept_count": len(filtered_articles),
            "used_topics_fallback": False,
        }

        if symbols and not filtered_articles:
            fallback_params = self._build_base_params(
                published_after=published_after,
                published_before=published_before,
                limit=limit,
            )
            fallback_params["topics"] = ",".join(ALPHA_TOPIC_FALLBACKS)
            fallback_articles = self._request_articles(fallback_params)
            fallback_filtered = _filter_relevant_articles(fallback_articles, symbols=symbols)
            page_meta.update(
                {
                    "used_topics_fallback": True,
                    "topics_query": fallback_params["topics"],
                    "topics_article_count": len(fallback_articles),
                    "topics_kept_count": len(fallback_filtered),
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

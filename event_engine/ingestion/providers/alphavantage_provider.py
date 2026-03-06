from __future__ import annotations

import os
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
from event_engine.redaction import redact_text


def _format_alpha_timestamp(value: str) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.strftime("%Y%m%dT%H%M")


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

    def _read_token(self) -> str | None:
        return os.getenv("ALPHAVANTAGE_API_KEY") or os.getenv("ALPHAVANTAGE_API_TOKEN")

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

        params: dict[str, Any] = {
            "function": "NEWS_SENTIMENT",
            "apikey": self.api_token,
            "limit": max(int(limit), 1),
            "sort": "LATEST",
        }
        if symbols:
            params["tickers"] = ",".join(sorted({str(symbol).upper() for symbol in symbols}))
        if published_after:
            params["time_from"] = _format_alpha_timestamp(published_after)
        if published_before:
            params["time_to"] = _format_alpha_timestamp(published_before)

        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = self.session.get(self.base_url, params=params, timeout=self.timeout)
                if response.status_code in (401, 403):
                    raise NewsProviderAuthError(f"{self.name} auth failed: {redact_text(response.text)}")
                if response.status_code in (402, 429):
                    raise NewsProviderQuotaError(f"{self.name} quota failed: {redact_text(response.text)}")
                if response.status_code >= 500:
                    raise NewsProviderTransientError(f"{self.name} upstream failed: {redact_text(response.text)}")
                response.raise_for_status()
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
                return ProviderFetchResult(
                    articles=[article for article in articles if isinstance(article, dict)],
                    page=page,
                    page_meta={"article_count": len(articles)},
                )
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

    def provider_document_id(self, article: dict[str, Any]) -> str | None:
        value = article.get("url") or article.get("title")
        return str(value) if value not in (None, "") else None

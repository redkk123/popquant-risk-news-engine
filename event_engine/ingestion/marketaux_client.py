from __future__ import annotations

import os
import time
from typing import Any

import requests

from event_engine.redaction import redact_text


class MarketauxClient:
    """Thin client for Marketaux financial news API."""

    BASE_URL = "https://api.marketaux.com/v1/news/all"

    def __init__(
        self,
        api_token: str | None = None,
        *,
        timeout: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        self.api_token = api_token or os.getenv("MARKETAUX_API_TOKEN")
        self.timeout = timeout
        self.session = session or requests.Session()
        if not self.api_token:
            raise ValueError(
                "Marketaux API token not found. Set MARKETAUX_API_TOKEN or pass api_token."
            )

    def fetch_news(
        self,
        *,
        symbols: list[str] | None = None,
        language: str = "en",
        published_after: str | None = None,
        published_before: str | None = None,
        limit: int = 3,
        page: int = 1,
        must_have_entities: bool = True,
        retries: int = 3,
        sleep_seconds: float = 1.0,
    ) -> dict[str, Any]:
        """Fetch a page of news from Marketaux."""
        params: dict[str, Any] = {
            "api_token": self.api_token,
            "language": language,
            "limit": limit,
            "page": page,
            "filter_entities": "true" if must_have_entities else "false",
        }
        if symbols:
            params["symbols"] = ",".join(sorted({symbol.upper() for symbol in symbols}))
        if published_after:
            params["published_after"] = published_after
        if published_before:
            params["published_before"] = published_before

        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # pragma: no cover - network failure path
                last_error = exc
                if attempt == retries:
                    break
                time.sleep(sleep_seconds * attempt)
        raise RuntimeError(
            f"Marketaux fetch failed after {retries} attempts: {redact_text(str(last_error))}"
        )

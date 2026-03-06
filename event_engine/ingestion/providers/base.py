from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class NewsProviderError(RuntimeError):
    """Base error for provider fetch failures."""


class NewsProviderUnavailableError(NewsProviderError):
    """Raised when a provider cannot be used in the current environment."""


class NewsProviderAuthError(NewsProviderError):
    """Raised when provider credentials are missing or invalid."""


class NewsProviderQuotaError(NewsProviderError):
    """Raised when provider quota or plan limits are hit."""


class NewsProviderTransientError(NewsProviderError):
    """Raised for network or upstream transient failures."""


@dataclass(frozen=True)
class ProviderFetchResult:
    articles: list[dict[str, Any]]
    page: int
    page_meta: dict[str, Any] = field(default_factory=dict)


def classify_error_message(message: str) -> type[NewsProviderError]:
    lowered = (message or "").lower()
    if any(fragment in lowered for fragment in ("401", "403", "unauthorized", "forbidden", "invalid api", "invalid token")):
        return NewsProviderAuthError
    if any(fragment in lowered for fragment in ("402", "429", "quota", "rate limit", "frequency", "payment required", "limit reached")):
        return NewsProviderQuotaError
    if any(fragment in lowered for fragment in ("timeout", "temporarily unavailable", "connection", "server error", "503", "500", "502", "504")):
        return NewsProviderTransientError
    return NewsProviderError


class NewsProvider(ABC):
    name = "unknown"
    env_var = ""
    supports_paging = True
    supports_symbol_batch_split = False

    def __init__(self, api_token: str | None = None) -> None:
        self.api_token = api_token or self._read_token()

    def _read_token(self) -> str | None:
        return os.getenv(self.env_var) if self.env_var else None

    def ensure_available(self) -> None:
        if not self.api_token:
            raise NewsProviderUnavailableError(
                f"{self.name} provider token not available. Set {self.env_var}."
            )

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def provider_document_id(self, article: dict[str, Any]) -> str | None:
        raise NotImplementedError

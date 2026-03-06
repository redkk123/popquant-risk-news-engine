from __future__ import annotations

from typing import Any

from event_engine.ingestion.marketaux_client import MarketauxClient
from event_engine.ingestion.providers.base import (
    NewsProvider,
    NewsProviderUnavailableError,
    ProviderFetchResult,
    classify_error_message,
)


class MarketauxProvider(NewsProvider):
    name = "marketaux"
    env_var = "MARKETAUX_API_TOKEN"
    supports_paging = True
    supports_symbol_batch_split = True

    def __init__(self, api_token: str | None = None) -> None:
        super().__init__(api_token=api_token)
        self.ensure_available()
        self.client = MarketauxClient(api_token=self.api_token)

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
        try:
            payload = self.client.fetch_news(
                symbols=symbols,
                language=language,
                published_after=published_after,
                published_before=published_before,
                limit=limit,
                page=page,
            )
        except ValueError as exc:
            raise NewsProviderUnavailableError(str(exc)) from exc
        except RuntimeError as exc:
            error_cls = classify_error_message(str(exc))
            raise error_cls(str(exc)) from exc

        articles = payload.get("data", []) or []
        return ProviderFetchResult(
            articles=[article for article in articles if isinstance(article, dict)],
            page=page,
            page_meta={"article_count": len(articles)},
        )

    def provider_document_id(self, article: dict[str, Any]) -> str | None:
        value = article.get("uuid") or article.get("id") or article.get("slug") or article.get("url")
        return str(value) if value not in (None, "") else None

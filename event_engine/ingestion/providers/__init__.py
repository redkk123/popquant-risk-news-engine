from event_engine.ingestion.providers.alphavantage_provider import AlphaVantageProvider
from event_engine.ingestion.providers.base import (
    NewsProvider,
    NewsProviderAuthError,
    NewsProviderError,
    NewsProviderQuotaError,
    NewsProviderTransientError,
    NewsProviderUnavailableError,
    ProviderFetchResult,
)
from event_engine.ingestion.providers.marketaux_provider import MarketauxProvider
from event_engine.ingestion.providers.thenewsapi_provider import TheNewsApiProvider

PROVIDER_REGISTRY = {
    "marketaux": MarketauxProvider,
    "thenewsapi": TheNewsApiProvider,
    "alphavantage": AlphaVantageProvider,
}


def build_news_provider(name: str, *, api_token: str | None = None) -> NewsProvider:
    provider_name = str(name).strip().lower()
    if provider_name not in PROVIDER_REGISTRY:
        raise ValueError(f"Unsupported news provider: {name}")
    provider_cls = PROVIDER_REGISTRY[provider_name]
    return provider_cls(api_token=api_token)


__all__ = [
    "AlphaVantageProvider",
    "MarketauxProvider",
    "NewsProvider",
    "NewsProviderAuthError",
    "NewsProviderError",
    "NewsProviderQuotaError",
    "NewsProviderTransientError",
    "NewsProviderUnavailableError",
    "ProviderFetchResult",
    "TheNewsApiProvider",
    "PROVIDER_REGISTRY",
    "build_news_provider",
]

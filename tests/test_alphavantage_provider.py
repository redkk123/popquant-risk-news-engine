from __future__ import annotations

from event_engine.ingestion.providers.alphavantage_provider import AlphaVantageProvider


def test_alphavantage_provider_accepts_api_token_env_alias(monkeypatch) -> None:
    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
    monkeypatch.setenv("ALPHAVANTAGE_API_TOKEN", "demo-token")

    provider = AlphaVantageProvider()

    assert provider.api_token == "demo-token"

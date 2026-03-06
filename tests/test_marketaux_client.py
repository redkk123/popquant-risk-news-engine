from __future__ import annotations

import requests

from event_engine.ingestion.marketaux_client import MarketauxClient


class _FailingResponse:
    def raise_for_status(self) -> None:
        raise requests.HTTPError(
            "402 Client Error: Payment Required for url: "
            "https://api.marketaux.com/v1/news/all?api_token=secret123&limit=3"
        )


class _FailingSession:
    def get(self, *args, **kwargs):
        return _FailingResponse()


def test_marketaux_client_redacts_token_from_runtime_error() -> None:
    client = MarketauxClient(api_token="secret123", session=_FailingSession())
    try:
        client.fetch_news(symbols=["AAPL"], retries=1)
    except RuntimeError as exc:
        message = str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected RuntimeError")

    assert "secret123" not in message
    assert "<redacted>" in message

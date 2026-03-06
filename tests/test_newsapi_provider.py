from __future__ import annotations

from event_engine.ingestion.providers.newsapi_provider import (
    MACRO_QUERY,
    NewsApiOrgProvider,
    _filter_relevant_articles,
)


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = ""

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict] = []

    def get(self, url: str, *, params: dict, headers: dict, timeout: int):
        self.calls.append({"url": url, "params": dict(params), "headers": dict(headers), "timeout": timeout})
        if not self.responses:
            raise AssertionError("No fake responses left.")
        return self.responses.pop(0)


def test_newsapi_provider_accepts_api_token_env_alias(monkeypatch) -> None:
    monkeypatch.delenv("NEWSAPI_API_KEY", raising=False)
    monkeypatch.setenv("NEWSAPI_API_TOKEN", "demo-token")

    provider = NewsApiOrgProvider()

    assert provider.api_token == "demo-token"


def test_newsapi_filter_keeps_universe_anchor_and_macro_story() -> None:
    articles = [
        {
            "title": "Wall Street tumbles as oil prices surge",
            "description": "Markets reacted to geopolitical pressure.",
            "content": "",
            "source": {"name": "Reuters"},
        },
        {
            "title": "Apple readies a new product launch",
            "description": "Apple and its supply chain gear up.",
            "content": "",
            "source": {"name": "Bloomberg"},
        },
        {
            "title": "Celebrity interview of the week",
            "description": "No public-company relevance.",
            "content": "",
            "source": {"name": "Entertainment Weekly"},
        },
    ]

    filtered = _filter_relevant_articles(articles, symbols=["AAPL", "SPY"])

    assert len(filtered) == 2
    assert filtered[0]["title"].startswith("Wall Street tumbles")
    assert filtered[1]["title"].startswith("Apple readies")


def test_newsapi_fetch_page_uses_macro_fallback_when_initial_query_returns_zero() -> None:
    session = _FakeSession(
        [
            _FakeResponse({"status": "ok", "articles": []}),
            _FakeResponse(
                {
                    "status": "ok",
                    "articles": [
                        {
                            "title": "Wall Street sinks as oil rises on geopolitical fears",
                            "description": "Macro pressure hit stocks broadly.",
                            "content": "Investors sold equities after oil jumped.",
                            "url": "https://example.com/macro-story",
                            "publishedAt": "2026-03-06T15:00:00Z",
                            "source": {"id": None, "name": "Reuters"},
                        }
                    ],
                }
            ),
        ]
    )
    provider = NewsApiOrgProvider(api_token="demo-token", session=session)

    result = provider.fetch_page(
        symbols=["AAPL", "MSFT", "SPY"],
        published_after="2026-03-04",
        published_before="2026-03-06",
        limit=3,
        page=1,
    )

    assert len(result.articles) == 1
    assert result.articles[0]["title"].startswith("Wall Street sinks")
    assert result.page_meta["used_macro_fallback"] is True
    assert result.page_meta["fallback_query"] == MACRO_QUERY
    assert session.calls[0]["headers"]["X-Api-Key"] == "demo-token"
    assert session.calls[1]["params"]["q"] == MACRO_QUERY

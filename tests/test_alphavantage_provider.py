from __future__ import annotations

from event_engine.ingestion.providers.alphavantage_provider import (
    ALPHA_TOPIC_FALLBACKS,
    AlphaVantageProvider,
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

    def get(self, url: str, *, params: dict, timeout: int):
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        if not self.responses:
            raise AssertionError("No fake responses left.")
        return self.responses.pop(0)


def test_alphavantage_provider_accepts_api_token_env_alias(monkeypatch) -> None:
    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
    monkeypatch.setenv("ALPHAVANTAGE_API_TOKEN", "demo-token")

    provider = AlphaVantageProvider()

    assert provider.api_token == "demo-token"


def test_alphavantage_filter_keeps_universe_anchor_and_macro_topics() -> None:
    articles = [
        {
            "title": "Wall Street slides as oil spikes on geopolitical fears",
            "summary": "Financial markets reacted to macro pressure.",
            "topics": [{"topic": "Financial Markets"}],
            "ticker_sentiment": [],
        },
        {
            "title": "Apple supplier ramps production for next device cycle",
            "summary": "Apple-related supply chain commentary.",
            "topics": [{"topic": "Technology"}],
            "ticker_sentiment": [{"ticker": "AAPL"}],
        },
        {
            "title": "Celebrity gossip roundup for the weekend",
            "summary": "No public-company relevance.",
            "topics": [{"topic": "Entertainment"}],
            "ticker_sentiment": [],
        },
    ]

    filtered = _filter_relevant_articles(articles, symbols=["AAPL", "SPY", "QQQ"])

    assert len(filtered) == 2
    assert filtered[0]["title"].startswith("Wall Street slides")
    assert filtered[1]["title"].startswith("Apple supplier")


def test_alphavantage_fetch_page_uses_topics_fallback_when_ticker_query_returns_zero() -> None:
    session = _FakeSession(
        [
            _FakeResponse({"feed": []}),
            _FakeResponse(
                {
                    "feed": [
                        {
                            "title": "Wall Street slumps as oil rises on geopolitical tension",
                            "summary": "Financial markets moved lower as crude climbed.",
                            "url": "https://example.com/macro-story",
                            "source": "Reuters",
                            "source_domain": "reuters.com",
                            "time_published": "20260306T150000",
                            "topics": [{"topic": "Financial Markets"}],
                            "ticker_sentiment": [{"ticker": "SPY"}],
                        }
                    ]
                }
            ),
        ]
    )
    provider = AlphaVantageProvider(api_token="demo-token", session=session)

    result = provider.fetch_page(
        symbols=["AAPL", "MSFT", "SPY", "QQQ"],
        published_after="2026-03-04",
        published_before="2026-03-06",
        limit=3,
        page=1,
    )

    assert len(result.articles) == 1
    assert result.articles[0]["title"].startswith("Wall Street slumps")
    assert result.page_meta["used_topics_fallback"] is True
    assert result.page_meta["topics_kept_count"] == 1
    assert session.calls[0]["params"]["tickers"] == "AAPL,MSFT,QQQ,SPY"
    assert session.calls[1]["params"]["topics"] == ",".join(ALPHA_TOPIC_FALLBACKS)
    assert "tickers" not in session.calls[1]["params"]

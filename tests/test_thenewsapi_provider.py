from __future__ import annotations

from event_engine.ingestion.providers.thenewsapi_provider import (
    TheNewsApiProvider,
    _build_search_queries,
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


def test_thenewsapi_filter_keeps_macro_and_symbol_relevant_articles() -> None:
    articles = [
        {
            "title": "Wall Street tumbles as oil spikes on geopolitical tensions",
            "description": "Markets moved lower as crude rose and Treasury yields climbed.",
            "categories": ["business"],
        },
        {
            "title": "Apple launches new AI features for iPhone users",
            "description": "Apple said the rollout would expand across its device base.",
            "categories": ["tech"],
        },
        {
            "title": "Queen Latifah Denies Online Death Hoax, Says She's '100% A-OK'",
            "description": "Entertainment coverage unrelated to public markets.",
            "categories": ["entertainment"],
        },
        {
            "title": "Dreamy Lavender & Bergamot Pillow Spray",
            "description": "Gift guide coverage unrelated to any requested symbol.",
            "categories": ["lifestyle"],
        },
    ]

    filtered = _filter_relevant_articles(articles, symbols=["AAPL", "SPY", "QQQ"])

    assert len(filtered) == 2
    assert filtered[0]["title"].startswith("Wall Street tumbles")
    assert filtered[1]["title"].startswith("Apple launches")


def test_thenewsapi_filter_drops_tech_story_without_universe_anchor() -> None:
    articles = [
        {
            "title": "Grab the 4 MacBook Neo Default Wallpapers",
            "description": "Wallpaper download story focused on device customization only.",
            "categories": ["tech"],
        }
    ]

    filtered = _filter_relevant_articles(articles, symbols=["AAPL", "SPY", "QQQ"])

    assert filtered == []


def test_build_search_queries_emits_alias_and_macro_fallbacks_for_broad_market_pack() -> None:
    queries = _build_search_queries(["AAPL", "MSFT", "NVDA", "SPY", "QQQ"])

    assert len(queries) == 2
    assert "apple" in queries[0].lower()
    assert "microsoft" in queries[0].lower()
    assert "Wall Street" in queries[1]


def test_thenewsapi_fetch_page_uses_search_fallback_when_symbol_batch_filters_to_zero() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                {
                    "data": [
                        {
                            "title": "Zendaya Pairs Gold Band With Tom Holland Engagement Ring Amid Marriage News",
                            "description": "Entertainment coverage unrelated to markets.",
                            "categories": ["entertainment"],
                        }
                    ]
                }
            ),
            _FakeResponse(
                {
                    "data": [
                        {
                            "title": "Dow plunges more than 1,000 points as surging oil prices renew inflation fears",
                            "description": "Wall Street and oil moved sharply together.",
                            "categories": ["business"],
                        }
                    ]
                }
            ),
        ]
    )
    provider = TheNewsApiProvider(api_token="demo-token", session=session)

    result = provider.fetch_page(
        symbols=["AAPL", "MSFT", "SPY", "QQQ"],
        published_after="2026-03-04",
        published_before="2026-03-06",
        limit=3,
        page=1,
    )

    assert len(result.articles) == 1
    assert result.articles[0]["title"].startswith("Dow plunges")
    assert result.page_meta["used_search_fallback"] is True
    assert result.page_meta["search_kept_count"] == 1
    assert "search" in session.calls[1]["params"]

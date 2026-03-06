from __future__ import annotations

from pathlib import Path

import pytest

from event_engine.ingestion import sync_news as sync_news_module
from event_engine.ingestion.providers.base import NewsProviderQuotaError, ProviderFetchResult
from event_engine.ingestion.sync_news import ingest_fixture
from event_engine.pipeline import process_raw_documents
from event_engine.storage.repository import NewsRepository


def test_news_pipeline_builds_events_from_fixture(tmp_path: Path) -> None:
    repo_root = tmp_path / "workspace"
    repository = NewsRepository(repo_root)

    fixture = Path("D:/Playground/popquant_1_month/datasets/fixtures/sample_marketaux_news.json")
    alias_table = Path("D:/Playground/popquant_1_month/config/news_entity_aliases.csv")

    sync_stats = ingest_fixture(repository, fixture)
    pipeline_stats = process_raw_documents(repository, alias_path=alias_table)

    assert sync_stats["articles_seen"] == 6
    assert pipeline_stats["raw_documents"] == 6
    assert pipeline_stats["duplicates"] == 1
    assert pipeline_stats["events"] == 5

    events_frame = repository.load_events_frame()
    assert {"event_type", "tickers", "severity", "polarity"}.issubset(events_frame.columns)
    assert "guidance" in set(events_frame["event_type"])
    assert "macro" in set(events_frame["event_type"])


def test_news_pipeline_drops_blocked_general_interest_source(tmp_path: Path) -> None:
    repository = NewsRepository(tmp_path / "workspace")
    repository.upsert_raw_documents(
        [
            {
                "document_id": "doc_thenewsapi_blocked",
                "provider": "thenewsapi",
                "provider_document_id": "blocked_1",
                "fetched_at": "2026-03-06T15:00:00Z",
                "payload": {
                    "uuid": "blocked_1",
                    "url": "https://www.eonline.com/news/example",
                    "title": "Zendaya Pairs Gold Band With Tom Holland Engagement Ring Amid Marriage News",
                    "source": "eonline.com",
                    "published_at": "2026-03-06T14:00:00Z",
                    "language": "en",
                    "description": "Celebrity coverage unrelated to public markets.",
                },
            }
        ]
    )

    pipeline_stats = process_raw_documents(
        repository,
        alias_path=Path("D:/Playground/popquant_1_month/config/news_entity_aliases.csv"),
    )

    assert pipeline_stats["events"] == 0
    assert repository.load_events_frame().empty


def test_sync_marketaux_news_batches_symbol_queries(tmp_path: Path, monkeypatch) -> None:
    repository = NewsRepository(tmp_path / "workspace")

    class DummyProvider:
        name = "marketaux"
        supports_paging = True
        supports_symbol_batch_split = True

        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def provider_document_id(self, article):
            return str(article.get("uuid"))

        def fetch_page(self, **kwargs):
            self.calls.append(kwargs)
            symbols = kwargs.get("symbols") or []
            symbol_label = "-".join(symbols) if symbols else "ALL"
            return ProviderFetchResult(
                articles=[
                    {
                        "uuid": f"{symbol_label}-page-{kwargs['page']}",
                        "url": f"https://example.com/{symbol_label}/{kwargs['page']}",
                        "title": f"{symbol_label} headline",
                        "source": "reuters.com",
                    }
                ],
                page=kwargs["page"],
            )

    dummy_provider = DummyProvider()
    monkeypatch.setattr(
        sync_news_module,
        "build_news_provider",
        lambda name, api_token=None: dummy_provider,
    )

    stats = sync_news_module.sync_marketaux_news(
        repository,
        symbols=["AAPL", "MSFT", "SPY", "QQQ", "XLE"],
        limit=1,
        max_pages=1,
        symbol_batch_size=2,
        api_token="token",
    )

    assert stats["request"]["symbol_batch_count"] == 3
    assert stats["articles_seen"] == 3
    assert len(dummy_provider.calls) == 3
    assert dummy_provider.calls[0]["symbols"] == ["AAPL", "MSFT"]
    assert dummy_provider.calls[1]["symbols"] == ["SPY", "QQQ"]
    assert dummy_provider.calls[2]["symbols"] == ["XLE"]


def test_sync_marketaux_news_splits_batch_after_402(tmp_path: Path, monkeypatch) -> None:
    repository = NewsRepository(tmp_path / "workspace")

    class SplittingProvider:
        name = "marketaux"
        supports_paging = True
        supports_symbol_batch_split = True

        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def provider_document_id(self, article):
            return str(article.get("uuid"))

        def fetch_page(self, **kwargs):
            symbols = list(kwargs.get("symbols") or [])
            self.calls.append(symbols)
            if len(symbols) > 1:
                raise NewsProviderQuotaError("402 Client Error")
            symbol = symbols[0]
            return ProviderFetchResult(
                articles=[
                    {
                        "uuid": f"{symbol}-page-{kwargs['page']}",
                        "url": f"https://example.com/{symbol}/{kwargs['page']}",
                        "title": f"{symbol} headline",
                        "source": "reuters.com",
                    }
                ],
                page=kwargs["page"],
            )

    splitting_provider = SplittingProvider()
    monkeypatch.setattr(
        sync_news_module,
        "build_news_provider",
        lambda name, api_token=None: splitting_provider,
    )

    stats = sync_news_module.sync_marketaux_news(
        repository,
        symbols=["AAPL", "MSFT", "SPY", "QQQ"],
        limit=1,
        max_pages=1,
        symbol_batch_size=4,
        api_token="token",
    )

    assert stats["failed_batch_count"] == 0
    assert stats["articles_seen"] == 4
    assert stats["resolved_symbol_batch_count"] == 4
    assert splitting_provider.calls[0] == ["AAPL", "MSFT", "SPY", "QQQ"]
    assert ["AAPL"] in splitting_provider.calls
    assert ["QQQ"] in splitting_provider.calls


def test_sync_news_falls_back_to_second_provider(tmp_path: Path, monkeypatch) -> None:
    repository = NewsRepository(tmp_path / "workspace")

    class FailingProvider:
        name = "marketaux"
        supports_paging = True
        supports_symbol_batch_split = False

        def provider_document_id(self, article):
            return str(article.get("uuid"))

        def fetch_page(self, **kwargs):
            raise NewsProviderQuotaError("quota hit")

    class WorkingProvider:
        name = "thenewsapi"
        supports_paging = True
        supports_symbol_batch_split = False

        def __init__(self) -> None:
            self.calls = 0

        def provider_document_id(self, article):
            return str(article.get("uuid"))

        def fetch_page(self, **kwargs):
            self.calls += 1
            return ProviderFetchResult(
                articles=[
                    {
                        "uuid": f"tn-{kwargs['page']}",
                        "url": f"https://example.com/tn/{kwargs['page']}",
                        "title": "fallback headline",
                        "source": "reuters.com",
                    }
                ],
                page=kwargs["page"],
            )

    working_provider = WorkingProvider()

    def _build_provider(name, api_token=None):
        if name == "marketaux":
            return FailingProvider()
        if name == "thenewsapi":
            return working_provider
        raise AssertionError(f"Unexpected provider: {name}")

    monkeypatch.setattr(sync_news_module, "build_news_provider", _build_provider)

    stats = sync_news_module.sync_news(
        repository,
        providers=["marketaux", "thenewsapi"],
        symbols=["AAPL", "MSFT"],
        limit=1,
        max_pages=1,
        symbol_batch_size=2,
    )

    assert stats["articles_seen"] == 1
    assert stats["providers_used"] == ["thenewsapi"]
    assert stats["failed_batch_count"] >= 1
    assert working_provider.calls == 1


def test_sync_news_treats_zero_article_successful_fallback_as_success(tmp_path: Path, monkeypatch) -> None:
    repository = NewsRepository(tmp_path / "workspace")

    class FailingProvider:
        name = "marketaux"
        supports_paging = True
        supports_symbol_batch_split = False

        def provider_document_id(self, article):
            return str(article.get("uuid"))

        def fetch_page(self, **kwargs):
            raise NewsProviderQuotaError("quota hit")

    class EmptyButSuccessfulProvider:
        name = "thenewsapi"
        supports_paging = True
        supports_symbol_batch_split = False

        def provider_document_id(self, article):
            return str(article.get("uuid"))

        def fetch_page(self, **kwargs):
            return ProviderFetchResult(articles=[], page=kwargs["page"])

    def _build_provider(name, api_token=None):
        if name == "marketaux":
            return FailingProvider()
        if name == "thenewsapi":
            return EmptyButSuccessfulProvider()
        raise AssertionError(f"Unexpected provider: {name}")

    monkeypatch.setattr(sync_news_module, "build_news_provider", _build_provider)

    stats = sync_news_module.sync_news(
        repository,
        providers=["marketaux", "thenewsapi"],
        symbols=["AAPL", "MSFT"],
        limit=1,
        max_pages=1,
        symbol_batch_size=2,
    )

    assert stats["articles_seen"] == 0
    assert stats["resolved_symbol_batch_count"] == 1
    assert stats["failed_batch_count"] == 1


def test_sync_news_failure_message_includes_provider_failure_summary(tmp_path: Path, monkeypatch) -> None:
    repository = NewsRepository(tmp_path / "workspace")

    class FailingProvider:
        name = "marketaux"
        supports_paging = True
        supports_symbol_batch_split = False

        def provider_document_id(self, article):
            return str(article.get("uuid"))

        def fetch_page(self, **kwargs):
            raise NewsProviderQuotaError("quota hit")

    monkeypatch.setattr(
        sync_news_module,
        "build_news_provider",
        lambda name, api_token=None: FailingProvider(),
    )

    with pytest.raises(RuntimeError) as exc_info:
        sync_news_module.sync_news(
            repository,
            providers=["marketaux"],
            symbols=["AAPL", "MSFT"],
            limit=1,
            max_pages=1,
            symbol_batch_size=2,
        )

    message = str(exc_info.value)
    assert "Failures:" in message
    assert "marketaux:NewsProviderQuotaError:AAPL,MSFT" in message

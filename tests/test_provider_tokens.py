from __future__ import annotations

import os

from services.provider_tokens import (
    clear_provider_tokens,
    load_provider_tokens,
    provider_token_config_path,
    save_provider_tokens,
    temporary_provider_token_env,
)


def test_save_and_load_provider_tokens_round_trip(tmp_path) -> None:
    save_provider_tokens(
        {
            "newsapi": "news-token",
            "alphavantage": "alpha-token",
            "ignored": "nope",
        },
        project_root=tmp_path,
    )

    loaded = load_provider_tokens(project_root=tmp_path)

    assert loaded == {
        "newsapi": "news-token",
        "alphavantage": "alpha-token",
    }
    assert provider_token_config_path(tmp_path).exists()


def test_clear_provider_tokens_removes_file(tmp_path) -> None:
    save_provider_tokens({"newsapi": "news-token"}, project_root=tmp_path)

    clear_provider_tokens(project_root=tmp_path)

    assert not provider_token_config_path(tmp_path).exists()


def test_temporary_provider_token_env_sets_and_restores(monkeypatch) -> None:
    monkeypatch.delenv("NEWSAPI_API_KEY", raising=False)
    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "old-alpha")

    with temporary_provider_token_env({"newsapi": "news-token", "alphavantage": "new-alpha"}):
        assert os.getenv("NEWSAPI_API_KEY") == "news-token"
        assert os.getenv("ALPHAVANTAGE_API_KEY") == "new-alpha"

    assert os.getenv("NEWSAPI_API_KEY") is None
    assert os.getenv("ALPHAVANTAGE_API_KEY") == "old-alpha"

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from services.pathing import PROJECT_ROOT


PROVIDER_ENV_VARS = {
    "marketaux": "MARKETAUX_API_TOKEN",
    "thenewsapi": "THENEWSAPI_API_TOKEN",
    "newsapi": "NEWSAPI_API_KEY",
    "alphavantage": "ALPHAVANTAGE_API_KEY",
}


def provider_token_config_path(project_root: str | Path | None = None) -> Path:
    root = Path(project_root) if project_root else PROJECT_ROOT
    return root / "config" / "local" / "provider_tokens.json"


def load_provider_tokens(project_root: str | Path | None = None) -> dict[str, str]:
    path = provider_token_config_path(project_root)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle) or {}
    tokens: dict[str, str] = {}
    for provider, value in payload.items():
        normalized = str(provider).strip().lower()
        token = str(value).strip()
        if normalized in PROVIDER_ENV_VARS and token:
            tokens[normalized] = token
    return tokens


def save_provider_tokens(tokens: dict[str, str], project_root: str | Path | None = None) -> Path:
    path = provider_token_config_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned = {
        str(provider).strip().lower(): str(token).strip()
        for provider, token in tokens.items()
        if str(provider).strip().lower() in PROVIDER_ENV_VARS and str(token).strip()
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(cleaned, handle, indent=2)
    return path


def clear_provider_tokens(project_root: str | Path | None = None) -> None:
    path = provider_token_config_path(project_root)
    if path.exists():
        path.unlink()


@contextmanager
def temporary_provider_token_env(tokens: dict[str, str] | None) -> Iterator[None]:
    active_tokens = {
        str(provider).strip().lower(): str(token).strip()
        for provider, token in (tokens or {}).items()
        if str(provider).strip().lower() in PROVIDER_ENV_VARS and str(token).strip()
    }
    previous: dict[str, str | None] = {}
    try:
        for provider, env_var in PROVIDER_ENV_VARS.items():
            previous[env_var] = os.getenv(env_var)
            token = active_tokens.get(provider)
            if token:
                os.environ[env_var] = token
        yield
    finally:
        for env_var, old_value in previous.items():
            if old_value is None:
                os.environ.pop(env_var, None)
            else:
                os.environ[env_var] = old_value

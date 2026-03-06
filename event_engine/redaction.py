from __future__ import annotations

import re
from typing import Any


REDACTION_PATTERNS = [
    re.compile(r"([?&]api_token=)[^&\s]+", flags=re.IGNORECASE),
    re.compile(r"(MARKETAUX_API_TOKEN=)[^\s]+", flags=re.IGNORECASE),
]


def redact_text(value: str | None) -> str | None:
    """Redact known secret patterns from loggable text."""
    if value is None:
        return None
    text = str(value)
    for pattern in REDACTION_PATTERNS:
        text = pattern.sub(r"\1<redacted>", text)
    return text


def redact_value(value: Any) -> Any:
    """Recursively redact secrets from nested log payloads."""
    if isinstance(value, dict):
        return {key: redact_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_value(item) for item in value)
    if isinstance(value, str):
        return redact_text(value)
    return value

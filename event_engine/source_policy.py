from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_POLICY_PATH = PROJECT_ROOT / "config" / "news_source_policy.yaml"
LOW_SIGNAL_BUCKETS = {"press_release_wire", "recap_aggregator", "opinion_commentary", "niche_recap"}


def _clean_domain(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    domain = parsed.netloc or parsed.path
    domain = domain.lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain.split("/")[0]


def infer_source_domain(source: str | None, url: str | None) -> str:
    """Return the best-effort source domain from source text or article URL."""
    source_domain = _clean_domain(source or "")
    if source_domain and "." in source_domain:
        return source_domain
    url_domain = _clean_domain(url or "")
    if url_domain:
        return url_domain
    return source_domain or "unknown"


@lru_cache(maxsize=8)
def _load_policy(path_text: str) -> dict[str, Any]:
    path = Path(path_text)
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    payload.setdefault("default", {})
    payload.setdefault("rules", [])
    return payload


def load_source_policy(path: str | Path | None = None) -> dict[str, Any]:
    """Load the configured source-quality policy."""
    resolved = Path(path) if path else DEFAULT_SOURCE_POLICY_PATH
    return _load_policy(str(resolved.resolve()))


def resolve_source_policy(
    document: dict[str, Any],
    *,
    policy_path: str | Path | None = None,
) -> dict[str, Any]:
    """Map a document source to a source-tier profile."""
    policy = load_source_policy(policy_path)
    default_rule = policy.get("default") or {}

    source = str(document.get("source", "")).strip()
    url = str(document.get("url", "")).strip()
    source_domain = infer_source_domain(source, url)
    search_text = " ".join(part for part in [source_domain, source.lower(), url.lower()] if part).lower()

    matched_rule = None
    rules = sorted(
        policy.get("rules", []),
        key=lambda item: len(str(item.get("pattern", "")).strip()),
        reverse=True,
    )
    for rule in rules:
        pattern = str(rule.get("pattern", "")).strip().lower()
        if pattern and pattern in search_text:
            matched_rule = rule
            break

    applied = matched_rule or default_rule
    source_tier = str(applied.get("tier", default_rule.get("tier", "tier2"))).strip() or "tier2"
    source_bucket = str(applied.get("bucket", default_rule.get("bucket", "secondary_reporting"))).strip()
    score_adjustment = float(applied.get("score_adjustment", default_rule.get("score_adjustment", 0.0)) or 0.0)
    strict_watchlist = bool(applied.get("strict_watchlist", default_rule.get("strict_watchlist", False)))
    block_watchlist = bool(applied.get("block_watchlist", default_rule.get("block_watchlist", False)))
    block_event_engine = bool(applied.get("block_event_engine", default_rule.get("block_event_engine", False)))
    source_low_signal = source_bucket in LOW_SIGNAL_BUCKETS or source_tier == "tier4"

    return {
        "source_domain": source_domain,
        "source_tier": source_tier,
        "source_bucket": source_bucket,
        "source_adjustment": score_adjustment,
        "source_rule_pattern": str(matched_rule.get("pattern")) if matched_rule else None,
        "source_strict_watchlist": strict_watchlist,
        "source_block_watchlist": block_watchlist,
        "source_block_event_engine": block_event_engine,
        "source_low_signal": source_low_signal,
    }

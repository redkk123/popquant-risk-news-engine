from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

GENERIC_CONTENT_PATTERNS = [
    r"\blong[- ]term buy\b",
    r"\bshould you\b",
    r"\bhere(?:'| i)?s the proof\b",
    r"\bhow to think about\b",
    r"\bstocks always rebound\b",
    r"\bpresents at\b",
    r"\bconference transcript\b",
    r"\btranscript\b",
    r"\bwhat to do about it\b",
    r"\bjoin in\b",
]


def load_alias_table(path: str | Path) -> list[dict[str, str]]:
    """Load alias table used for fallback entity linking."""
    alias_path = Path(path)
    aliases: list[dict[str, str]] = []
    with alias_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            aliases.append(
                {
                    "ticker": row["ticker"].upper().strip(),
                    "alias": row["alias"].strip().lower(),
                    "exchange": row.get("exchange", "").strip(),
                    "sector": row.get("sector", "").strip(),
                }
            )
    return aliases


def _text_blob(document: dict[str, Any]) -> str:
    return " ".join(
        [
            str(document.get("title", "")),
            str(document.get("description", "")),
            str(document.get("snippet", "")),
        ]
    ).lower()


def _front_text(document: dict[str, Any]) -> str:
    return " ".join(
        [
            str(document.get("title", "")),
            str(document.get("description", "")),
        ]
    ).lower()


def _title_text(document: dict[str, Any]) -> str:
    return str(document.get("title", "")).lower()


def _has_text_anchor(text: str, phrases: list[str]) -> bool:
    for phrase in phrases:
        candidate = phrase.strip().lower()
        if not candidate:
            continue
        if re.search(r"\b" + re.escape(candidate) + r"\b", text):
            return True
    return False


def _provider_entity_anchor_diagnostics(
    document: dict[str, Any],
    aliases_by_ticker: dict[str, list[str]],
) -> dict[str, list[str]]:
    front_text = _front_text(document)
    generic_content = any(re.search(pattern, front_text) for pattern in GENERIC_CONTENT_PATTERNS)
    provider_symbols = sorted(
        {
            str(symbol).upper().strip()
            for symbol in (document.get("symbols") or [])
            if str(symbol).strip()
        }
    )
    anchored_provider_symbols: set[str] = set()
    for entity in document.get("entities", []) or []:
        symbol = str(entity.get("symbol", "")).upper().strip()
        if not symbol:
            continue
        entity_name = str(entity.get("name", "")).strip().lower()
        anchor_phrases = [symbol.lower(), entity_name] + aliases_by_ticker.get(symbol, [])
        if not _has_text_anchor(front_text, anchor_phrases):
            continue
        if generic_content and entity_name and not re.search(r"\b" + re.escape(entity_name) + r"\b", front_text):
            continue
        anchored_provider_symbols.add(symbol)

    if not provider_symbols and anchored_provider_symbols:
        provider_symbols = sorted(anchored_provider_symbols)

    return {
        "provider_symbols": provider_symbols,
        "anchored_provider_symbols": sorted(anchored_provider_symbols),
        "unanchored_provider_symbols": sorted(set(provider_symbols) - anchored_provider_symbols),
    }


def link_document_tickers(
    document: dict[str, Any],
    alias_table: list[dict[str, str]],
) -> dict[str, Any]:
    """Link canonical documents to tickers using API entities and alias fallback."""
    linked: dict[str, float] = {}
    aliases_by_ticker: dict[str, list[str]] = defaultdict(list)
    ticker_group: dict[str, str] = {}
    for row in alias_table:
        aliases_by_ticker[row["ticker"]].append(row["alias"])
        ticker_group.setdefault(row["ticker"], row.get("sector", "").strip().lower())

    front_text = _front_text(document)
    title_text = _title_text(document)
    generic_content = any(re.search(pattern, front_text) for pattern in GENERIC_CONTENT_PATTERNS)
    provider_diagnostics = _provider_entity_anchor_diagnostics(document, aliases_by_ticker)
    for entity in document.get("entities", []) or []:
        symbol = str(entity.get("symbol", "")).upper().strip()
        if not symbol:
            continue
        entity_name = str(entity.get("name", "")).strip().lower()
        anchor_phrases = [symbol.lower(), entity_name] + aliases_by_ticker.get(symbol, [])
        if not _has_text_anchor(front_text, anchor_phrases):
            continue
        if generic_content and entity_name and not re.search(r"\b" + re.escape(entity_name) + r"\b", front_text):
            continue
        linked[symbol] = max(linked.get(symbol, 0.0), 0.95)

    text_blob = _text_blob(document)
    for alias in alias_table:
        alias_text = alias["alias"]
        if not alias_text:
            continue
        pattern = r"\b" + re.escape(alias_text) + r"\b"
        if generic_content:
            continue
        if re.search(pattern, front_text):
            ticker = alias["ticker"]
            confidence = 0.95 if re.search(pattern, title_text) else 0.65
            linked[ticker] = max(linked.get(ticker, 0.0), confidence)

    strong_single_name = any(
        ticker_group.get(ticker) != "index" and score >= 0.95
        for ticker, score in linked.items()
    )
    if strong_single_name:
        linked = {
            ticker: score
            for ticker, score in linked.items()
            if not (ticker_group.get(ticker) == "index" and score < 0.95)
        }

    ranked = sorted(linked.items(), key=lambda item: (-item[1], item[0]))
    tickers = [ticker for ticker, _ in ranked]
    confidence = max((score for _, score in ranked), default=0.0)

    return {
        "tickers": tickers,
        "link_confidence": float(confidence),
        "link_details": [{"ticker": ticker, "confidence": score} for ticker, score in ranked],
        "provider_symbols": provider_diagnostics["provider_symbols"],
        "anchored_provider_symbols": provider_diagnostics["anchored_provider_symbols"],
        "unanchored_provider_symbols": provider_diagnostics["unanchored_provider_symbols"],
    }

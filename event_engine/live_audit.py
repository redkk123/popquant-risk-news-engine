from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


SUSPICIOUS_SOURCE_PATTERNS = [
    r"globenewswire",
    r"newswire",
    r"press release",
]
LOW_SIGNAL_BUCKETS = {"press_release_wire", "recap_aggregator", "opinion_commentary", "niche_recap"}


def _contains_suspicious_source(text: str) -> bool:
    return any(re.search(pattern, text) for pattern in SUSPICIOUS_SOURCE_PATTERNS)


def _prepare_frame(events_frame: pd.DataFrame) -> pd.DataFrame:
    if events_frame.empty:
        return pd.DataFrame()

    frame = events_frame.copy()
    if "tickers" not in frame.columns:
        frame["tickers"] = [[] for _ in range(len(frame))]
    frame["tickers"] = frame["tickers"].apply(lambda value: value if isinstance(value, list) else [])
    frame["ticker_count"] = frame["tickers"].apply(len)
    frame["source_text"] = frame["source"].fillna("").astype(str).str.lower()
    if "watchlist_eligible" not in frame.columns:
        frame["watchlist_eligible"] = [False] * len(frame)
    frame["watchlist_eligible"] = frame["watchlist_eligible"].fillna(False).astype(bool)
    if "quality_label" not in frame.columns:
        frame["quality_label"] = ["unknown"] * len(frame)
    frame["quality_label"] = frame["quality_label"].fillna("unknown").astype(str)
    if "source_tier" not in frame.columns:
        frame["source_tier"] = ["unknown"] * len(frame)
    frame["source_tier"] = frame["source_tier"].fillna("unknown").astype(str)
    if "source_bucket" not in frame.columns:
        frame["source_bucket"] = ["unknown"] * len(frame)
    frame["source_bucket"] = frame["source_bucket"].fillna("unknown").astype(str)
    if "source_low_signal" not in frame.columns:
        frame["source_low_signal"] = [False] * len(frame)
    frame["source_low_signal"] = frame["source_low_signal"].fillna(False).astype(bool)
    if "link_confidence" not in frame.columns:
        frame["link_confidence"] = [0.0] * len(frame)
    frame["link_confidence"] = pd.to_numeric(frame["link_confidence"], errors="coerce").fillna(0.0)
    if "event_confidence" not in frame.columns:
        frame["event_confidence"] = [0.0] * len(frame)
    frame["event_confidence"] = pd.to_numeric(frame["event_confidence"], errors="coerce").fillna(0.0)
    return frame


def build_live_event_audit(events_frame: pd.DataFrame) -> dict[str, Any]:
    """Build a compact QA view over a processed live event batch."""
    frame = _prepare_frame(events_frame)
    if frame.empty:
        empty = pd.DataFrame()
        summary = {
            "total_events": 0,
            "watchlist_eligible_events": 0,
            "filtered_events": 0,
            "zero_link_events": 0,
            "zero_link_non_macro_events": 0,
            "suspicious_link_events": 0,
            "eligible_suspicious_link_events": 0,
            "event_type_distribution": {},
            "eligible_event_type_distribution": {},
            "source_distribution": {},
            "source_tier_distribution": {},
            "source_bucket_distribution": {},
            "quality_distribution": {},
            "eligible_quality_distribution": {},
            "eligible_source_tier_distribution": {},
            "low_signal_source_events": 0,
            "eligible_low_signal_source_events": 0,
            "ticker_distribution": {},
        }
        return {
            "summary": summary,
            "zero_link_events": empty,
            "filtered_events": empty,
            "suspicious_link_events": empty,
        }

    zero_link_events = frame.loc[frame["ticker_count"] == 0].copy()
    filtered_events = frame.loc[~frame["watchlist_eligible"]].copy()
    eligible_frame = frame.loc[frame["watchlist_eligible"]].copy()
    suspicious_source_mask = (
        frame["source_low_signal"].astype(bool)
        | frame["source_bucket"].isin(LOW_SIGNAL_BUCKETS).astype(bool)
        | frame["source_text"].apply(_contains_suspicious_source).astype(bool)
    ).astype(bool)
    weak_link_mask = ((frame["event_type"] != "macro") & (frame["link_confidence"] < 0.8)).astype(bool)
    low_quality_mask = (frame["quality_label"].str.lower() == "low").astype(bool)
    suspicious_mask = (
        (frame["event_type"] != "commentary").astype(bool)
        & (frame["ticker_count"] > 0).astype(bool)
        & (weak_link_mask | low_quality_mask | suspicious_source_mask)
    )
    suspicious_link_events = frame.loc[suspicious_mask].copy()

    eligible_suspicious_source_mask = (
        eligible_frame["source_low_signal"].astype(bool)
        | eligible_frame["source_bucket"].isin(LOW_SIGNAL_BUCKETS).astype(bool)
        | eligible_frame["source_text"].apply(_contains_suspicious_source).astype(bool)
    ).astype(bool)
    eligible_weak_link_mask = (
        ((eligible_frame["event_type"] != "macro") & (eligible_frame["link_confidence"] < 0.8)).astype(bool)
    )
    eligible_low_quality_mask = (eligible_frame["quality_label"].str.lower() == "low").astype(bool)
    eligible_suspicious_mask = (
        (eligible_frame["event_type"] != "commentary").astype(bool)
        & (eligible_frame["ticker_count"] > 0).astype(bool)
        & (eligible_weak_link_mask | eligible_low_quality_mask | eligible_suspicious_source_mask)
    )
    eligible_suspicious_link_events = eligible_frame.loc[eligible_suspicious_mask].copy()

    ticker_distribution: dict[str, int] = {}
    if not frame.empty:
        exploded = frame[["tickers"]].explode("tickers")
        exploded = exploded.loc[exploded["tickers"].notna() & (exploded["tickers"] != "")]
        ticker_distribution = (
            exploded["tickers"].value_counts().sort_values(ascending=False).to_dict()
            if not exploded.empty
            else {}
        )

    summary = {
        "total_events": int(len(frame)),
        "watchlist_eligible_events": int(frame["watchlist_eligible"].sum()),
        "filtered_events": int((~frame["watchlist_eligible"]).sum()),
        "zero_link_events": int(len(zero_link_events)),
        "zero_link_non_macro_events": int(len(zero_link_events.loc[zero_link_events["event_type"] != "macro"])),
        "suspicious_link_events": int(len(suspicious_link_events)),
        "eligible_suspicious_link_events": int(len(eligible_suspicious_link_events)),
        "event_type_distribution": frame["event_type"].value_counts().sort_values(ascending=False).to_dict(),
        "eligible_event_type_distribution": (
            eligible_frame["event_type"].value_counts().sort_values(ascending=False).to_dict()
            if not eligible_frame.empty
            else {}
        ),
        "source_distribution": frame["source"].value_counts().sort_values(ascending=False).to_dict(),
        "source_tier_distribution": frame["source_tier"].value_counts().sort_values(ascending=False).to_dict(),
        "source_bucket_distribution": frame["source_bucket"].value_counts().sort_values(ascending=False).to_dict(),
        "quality_distribution": frame["quality_label"].value_counts().sort_values(ascending=False).to_dict(),
        "eligible_quality_distribution": (
            eligible_frame["quality_label"].value_counts().sort_values(ascending=False).to_dict()
            if not eligible_frame.empty
            else {}
        ),
        "eligible_source_tier_distribution": (
            eligible_frame["source_tier"].value_counts().sort_values(ascending=False).to_dict()
            if not eligible_frame.empty
            else {}
        ),
        "low_signal_source_events": int(suspicious_source_mask.sum()),
        "eligible_low_signal_source_events": int(eligible_suspicious_source_mask.sum()),
        "ticker_distribution": ticker_distribution,
    }
    return {
        "summary": summary,
        "zero_link_events": zero_link_events,
        "filtered_events": filtered_events,
        "suspicious_link_events": suspicious_link_events,
    }


def write_live_audit_outputs(
    *,
    output_root: str | Path,
    audit_bundle: dict[str, Any],
) -> dict[str, Path]:
    """Write QA outputs for a processed live event batch."""
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)

    summary_path = root / "live_event_audit_summary.json"
    zero_link_path = root / "live_zero_link_events.csv"
    filtered_path = root / "live_filtered_events.csv"
    suspicious_path = root / "live_suspicious_link_events.csv"

    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(audit_bundle["summary"], handle, indent=2)

    audit_bundle["zero_link_events"].to_csv(zero_link_path, index=False)
    audit_bundle["filtered_events"].to_csv(filtered_path, index=False)
    audit_bundle["suspicious_link_events"].to_csv(suspicious_path, index=False)

    return {
        "summary_json": summary_path,
        "zero_link_csv": zero_link_path,
        "filtered_csv": filtered_path,
        "suspicious_csv": suspicious_path,
    }

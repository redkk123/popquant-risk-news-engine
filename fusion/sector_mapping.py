from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_ticker_sector_map(path: str | Path | None) -> dict[str, str]:
    """Load a simple ticker-to-sector mapping from CSV."""
    if path in (None, ""):
        return {}

    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Ticker sector map not found: {csv_path}")

    frame = pd.read_csv(csv_path)
    required_columns = {"ticker", "sector"}
    if not required_columns.issubset(frame.columns):
        raise ValueError("Ticker sector map must contain ticker and sector columns.")

    mapping: dict[str, str] = {}
    for record in frame.to_dict(orient="records"):
        ticker = str(record.get("ticker", "")).strip().upper()
        sector = str(record.get("sector", "")).strip().lower()
        if ticker and sector and sector != "nan":
            mapping[ticker] = sector
    return mapping


def select_sector_peer_symbols(
    *,
    event_tickers: list[str],
    ticker_sector_map: dict[str, str],
) -> list[str]:
    """Return symbols in the same sectors as the provided event tickers."""
    if not ticker_sector_map:
        return []

    event_sectors = {
        ticker_sector_map[str(ticker).upper()]
        for ticker in event_tickers
        if str(ticker).upper() in ticker_sector_map
    }
    return sorted(
        ticker
        for ticker, sector in ticker_sector_map.items()
        if sector in event_sectors
    )

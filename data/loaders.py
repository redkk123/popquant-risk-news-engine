from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    import yfinance as yf
except ImportError as exc:  # pragma: no cover - runtime dependency message
    raise ImportError(
        "yfinance is required to download prices. Install dependencies with "
        "`pip install -r requirements.txt`."
    ) from exc


def _build_cache_path(
    cache_dir: Path, tickers: Iterable[str], start: str, end: str
) -> Path:
    ticker_key = "_".join(sorted(ticker.upper() for ticker in tickers))
    return cache_dir / f"prices_{ticker_key}_{start}_{end}.csv"


def load_prices(
    tickers: Iterable[str],
    start: str,
    end: str,
    cache_dir: str | Path = "data/cache",
) -> pd.DataFrame:
    """Load adjusted close prices from cache or Yahoo Finance."""
    symbols = list(
        dict.fromkeys(ticker.upper().strip() for ticker in tickers if ticker.strip())
    )
    if not symbols:
        raise ValueError("At least one ticker is required.")

    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    price_file = _build_cache_path(cache_path, symbols, start, end)

    if price_file.exists():
        return pd.read_csv(price_file, index_col=0, parse_dates=True)

    raw = yf.download(
        tickers=symbols,
        start=start,
        end=end,
        auto_adjust=True,
        progress=False,
    )

    if raw.empty:
        raise RuntimeError(
            f"No price data found for tickers={symbols} in range [{start}, {end}]."
        )

    if isinstance(raw.columns, pd.MultiIndex):
        if "Close" not in raw.columns.get_level_values(0):
            raise RuntimeError("Downloaded data does not contain 'Close' prices.")
        prices = raw.xs("Close", axis=1, level=0, drop_level=True)
    else:
        if "Close" not in raw.columns:
            raise RuntimeError("Downloaded data does not contain a 'Close' column.")
        prices = raw[["Close"]].rename(columns={"Close": symbols[0]})

    prices = prices.sort_index().dropna(how="all")

    # Keep column order aligned with input symbols when available.
    kept = [ticker for ticker in symbols if ticker in prices.columns]
    if kept:
        prices = prices[kept]

    prices.to_csv(price_file)
    return prices


def load_intraday_prices(
    tickers: Iterable[str],
    *,
    period: str = "1d",
    interval: str = "1m",
) -> pd.DataFrame:
    """Load recent intraday adjusted close prices from Yahoo Finance."""
    symbols = list(
        dict.fromkeys(ticker.upper().strip() for ticker in tickers if ticker.strip())
    )
    if not symbols:
        raise ValueError("At least one ticker is required.")

    raw = yf.download(
        tickers=symbols,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
        prepost=False,
    )

    if raw.empty:
        raise RuntimeError(
            f"No intraday price data found for tickers={symbols} with period={period} interval={interval}."
        )

    if isinstance(raw.columns, pd.MultiIndex):
        if "Close" not in raw.columns.get_level_values(0):
            raise RuntimeError("Downloaded intraday data does not contain 'Close' prices.")
        prices = raw.xs("Close", axis=1, level=0, drop_level=True)
    else:
        if "Close" not in raw.columns:
            raise RuntimeError("Downloaded intraday data does not contain a 'Close' column.")
        prices = raw[["Close"]].rename(columns={"Close": symbols[0]})

    prices = prices.sort_index().dropna(how="all")
    kept = [ticker for ticker in symbols if ticker in prices.columns]
    if kept:
        prices = prices[kept]
    return prices

from __future__ import annotations

from pathlib import Path

import yaml

from data.positions import load_portfolio_config
from event_engine.live_validation import load_symbol_universe


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_validation_symbol_packs_are_loadable() -> None:
    config_path = PROJECT_ROOT / "config" / "validation" / "live_validation_universe.yaml"

    for pack_name in (
        "core_market_pack",
        "financial_energy_pack",
        "health_industrials_pack",
        "consumer_internet_pack",
        "semis_software_pack",
        "defensives_pack",
        "rates_sensitive_pack",
    ):
        symbols = load_symbol_universe(config_path, pack=pack_name)
        assert symbols


def test_new_portfolios_load_cleanly() -> None:
    for path in (
        PROJECT_ROOT / "config" / "portfolios" / "semis_sector_portfolio.json",
        PROJECT_ROOT / "config" / "portfolios" / "software_sector_portfolio.json",
        PROJECT_ROOT / "config" / "portfolios" / "defensives_sector_portfolio.json",
        PROJECT_ROOT / "config" / "portfolios" / "rates_sensitive_portfolio.json",
    ):
        metadata, positions = load_portfolio_config(path)
        assert metadata["portfolio_id"]
        assert abs(float(positions["weight"].sum()) - 1.0) < 1e-9


def test_validation_watchlist_has_at_least_fifteen_portfolios() -> None:
    watchlist_path = PROJECT_ROOT / "config" / "watchlists" / "validation_watchlist.yaml"
    with watchlist_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    assert len(payload.get("portfolios", [])) >= 15

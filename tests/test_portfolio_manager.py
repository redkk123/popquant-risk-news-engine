from __future__ import annotations

from services.portfolio_manager import load_portfolio_payload, save_portfolio_payload


def test_save_portfolio_payload_round_trip(tmp_path) -> None:
    payload = {
        "portfolio_id": "ui_test_book",
        "description": "Created from UI flow",
        "base_currency": "USD",
        "benchmark": "SPY",
        "positions": [
            {"ticker": "AAPL", "weight": 40},
            {"ticker": "MSFT", "weight": 35},
            {"ticker": "JPM", "weight": 25},
        ],
    }

    output_path = save_portfolio_payload(payload, project_root=tmp_path)
    loaded = load_portfolio_payload(output_path)

    assert output_path.exists()
    assert loaded["portfolio_id"] == "ui_test_book"
    assert abs(sum(position["weight"] for position in loaded["positions"]) - 1.0) < 1e-9
    assert loaded["positions"][0]["ticker"] == "AAPL"

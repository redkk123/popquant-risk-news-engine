from __future__ import annotations

import pandas as pd

from fusion.watchlist_reporting import build_watchlist_rows


def test_build_watchlist_rows_selects_top_event_for_portfolio() -> None:
    baseline_snapshot = {
        "models": {
            "normal_var_loss_1d_99": 0.03,
            "normal_es_loss_1d_99": 0.04,
        }
    }
    integrated_summary = pd.DataFrame(
        [
            {
                "event_id": "evt_1",
                "event_type": "guidance",
                "headline": "AAPL cuts guidance",
                "published_at": "2026-03-05T13:30:00Z",
                "tickers": ["AAPL"],
                "severity": 1.0,
                "recency_decay": 0.9,
                "shock_scale": 0.9,
                "delta_normal_var_loss_1d_99": 0.02,
                "delta_normal_es_loss_1d_99": 0.03,
                "stressed_normal_var_loss_1d_99": 0.05,
                "stressed_normal_es_loss_1d_99": 0.07,
            },
            {
                "event_id": "evt_2",
                "event_type": "earnings",
                "headline": "MSFT beats earnings",
                "published_at": "2026-03-05T14:15:00Z",
                "tickers": ["MSFT"],
                "severity": 0.7,
                "recency_decay": 0.92,
                "shock_scale": 0.7,
                "delta_normal_var_loss_1d_99": 0.01,
                "delta_normal_es_loss_1d_99": 0.015,
                "stressed_normal_var_loss_1d_99": 0.04,
                "stressed_normal_es_loss_1d_99": 0.055,
            },
        ]
    )

    summary_row, event_rows = build_watchlist_rows(
        portfolio_id="demo_book",
        baseline_snapshot=baseline_snapshot,
        integrated_summary=integrated_summary,
    )

    assert summary_row["portfolio_id"] == "demo_book"
    assert summary_row["top_event_type"] == "guidance"
    assert summary_row["max_delta_normal_var_loss_1d_99"] == 0.02
    assert len(event_rows) == 2

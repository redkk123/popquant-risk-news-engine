from __future__ import annotations

PORTFOLIO_REQUIRED_FIELDS = ("portfolio_id", "positions")
POSITION_REQUIRED_FIELDS = ("ticker", "weight")

RISK_SNAPSHOT_KEYS = (
    "metadata",
    "exposures",
    "portfolio_stats",
    "models",
    "benchmark",
    "top_risk_contributors",
)


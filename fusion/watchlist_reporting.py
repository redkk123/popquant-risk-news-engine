from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def _safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def build_watchlist_rows(
    *,
    portfolio_id: str,
    baseline_snapshot: dict[str, Any],
    integrated_summary: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame]:
    """Build portfolio-level and event-level rows for the watchlist report."""
    baseline_var = float(baseline_snapshot["models"]["normal_var_loss_1d_99"])
    baseline_es = float(baseline_snapshot["models"]["normal_es_loss_1d_99"])

    if integrated_summary.empty:
        portfolio_row = {
            "portfolio_id": portfolio_id,
            "baseline_normal_var_loss_1d_99": baseline_var,
            "baseline_normal_es_loss_1d_99": baseline_es,
            "scenario_count": 0,
            "event_count": 0,
            "top_event_type": None,
            "top_event_headline": None,
            "top_tickers": None,
            "max_delta_normal_var_loss_1d_99": 0.0,
            "max_delta_normal_es_loss_1d_99": 0.0,
            "stressed_normal_var_loss_1d_99": baseline_var,
            "stressed_normal_es_loss_1d_99": baseline_es,
        }
        return portfolio_row, pd.DataFrame()

    working = integrated_summary.copy()
    working["portfolio_id"] = portfolio_id
    working["tickers_label"] = working["tickers"].apply(lambda values: ",".join(values))
    if "event_sectors" in working.columns:
        working["event_sectors_label"] = working["event_sectors"].apply(
            lambda values: ",".join(values) if isinstance(values, list) else ""
        )
    else:
        working["event_sectors_label"] = ""
    if "direct_tickers" in working.columns:
        working["direct_tickers_label"] = working["direct_tickers"].apply(
            lambda values: ",".join(values) if isinstance(values, list) else ""
        )
    else:
        working["direct_tickers_label"] = ""
    if "sector_peer_tickers" in working.columns:
        working["sector_peer_tickers_label"] = working["sector_peer_tickers"].apply(
            lambda value: json.dumps(value, sort_keys=True) if isinstance(value, dict) else "{}"
        )
    else:
        working["sector_peer_tickers_label"] = "{}"

    top = working.sort_values(
        ["delta_normal_var_loss_1d_99", "shock_scale"],
        ascending=[False, False],
    ).iloc[0]

    portfolio_row = {
        "portfolio_id": portfolio_id,
        "baseline_normal_var_loss_1d_99": baseline_var,
        "baseline_normal_es_loss_1d_99": baseline_es,
        "scenario_count": int(len(working)),
        "event_count": int(working["event_id"].nunique()),
        "top_event_type": top["event_type"],
        "top_event_headline": top["headline"],
        "top_tickers": top["tickers_label"],
        "max_delta_normal_var_loss_1d_99": float(working["delta_normal_var_loss_1d_99"].max()),
        "max_delta_normal_es_loss_1d_99": float(working["delta_normal_es_loss_1d_99"].max()),
        "stressed_normal_var_loss_1d_99": float(working["stressed_normal_var_loss_1d_99"].max()),
        "stressed_normal_es_loss_1d_99": float(working["stressed_normal_es_loss_1d_99"].max()),
    }

    event_rows = working.reindex(
        columns=[
            "portfolio_id",
            "event_id",
            "event_type",
            "event_subtype",
            "story_bucket",
            "headline",
            "published_at",
            "source",
            "source_tier",
            "source_bucket",
            "tickers_label",
            "direct_tickers_label",
            "event_sectors_label",
            "sector_peer_tickers_label",
            "severity",
            "quality_score",
            "quality_label",
            "recency_decay",
            "shock_scale",
            "source_scale",
            "spillover_confidence_scale",
            "delta_normal_var_loss_1d_99",
            "delta_normal_es_loss_1d_99",
            "stressed_normal_var_loss_1d_99",
            "stressed_normal_es_loss_1d_99",
        ]
    ).rename(
        columns={
            "tickers_label": "tickers",
            "direct_tickers_label": "direct_tickers",
            "event_sectors_label": "event_sectors",
            "sector_peer_tickers_label": "sector_peer_tickers",
        }
    )
    return portfolio_row, event_rows


def write_watchlist_outputs(
    *,
    output_root: str | Path,
    summary_frame: pd.DataFrame,
    event_frame: pd.DataFrame,
    portfolio_reports: list[dict[str, Any]],
) -> dict[str, Path]:
    """Write consolidated outputs for the multi-portfolio watchlist workflow."""
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)

    summary_path = root / "watchlist_summary.csv"
    events_path = root / "watchlist_events.csv"
    report_path = root / "watchlist_report.json"
    markdown_path = root / "watchlist_report.md"

    summary_frame.to_csv(summary_path, index=False)
    event_frame.to_csv(events_path, index=False)

    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "summary": summary_frame.to_dict(orient="records"),
                "events": event_frame.to_dict(orient="records"),
                "portfolio_reports": portfolio_reports,
            },
            handle,
            indent=2,
        )

    lines = [
        "# Daily Watchlist Report",
        "",
        f"Portfolios covered: `{len(summary_frame)}`",
        "",
        "## Ranked Portfolios",
        "",
    ]
    if summary_frame.empty:
        lines.append("- No portfolios were processed.")
    else:
        ranked = summary_frame.sort_values(
            ["max_delta_normal_var_loss_1d_99", "stressed_normal_var_loss_1d_99"],
            ascending=[False, False],
        )
        for _, row in ranked.iterrows():
            lines.append(
                f"- `{row['portfolio_id']}` top delta VaR `{row['max_delta_normal_var_loss_1d_99']:.4f}` "
                f"from `{row['top_event_type']}` on `{row['top_tickers']}`"
            )

    lines.extend(["", "## Top Events", ""])
    if event_frame.empty:
        lines.append("- No relevant events matched the watchlist portfolios.")
    else:
        top_events = event_frame.sort_values(
            ["delta_normal_var_loss_1d_99", "shock_scale"],
            ascending=[False, False],
        ).head(10)
        for _, row in top_events.iterrows():
            lines.append(
                f"- `{row['portfolio_id']}` | `{row['event_type']}` | `{row['tickers']}` | "
                f"`{row['source_tier']}` `{row['source']}` | delta VaR `{row['delta_normal_var_loss_1d_99']:.4f}` | "
                f"{row['headline']}"
            )

    with markdown_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")

    return {
        "summary_csv": summary_path,
        "events_csv": events_path,
        "report_json": report_path,
        "report_md": markdown_path,
    }

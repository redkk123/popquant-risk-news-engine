from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def write_integration_outputs(
    *,
    output_root: str | Path,
    baseline_snapshot: dict[str, Any],
    integrated_summary: pd.DataFrame,
    stress_detail: pd.DataFrame,
) -> dict[str, Path]:
    """Write JSON, CSV, and Markdown outputs for the integration layer."""
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)

    report_path = root / "integrated_report.json"
    summary_path = root / "integrated_summary.csv"
    detail_path = root / "integrated_stress_detail.csv"
    markdown_path = root / "integrated_report.md"

    integrated_summary.to_csv(summary_path, index=False)
    stress_detail.to_csv(detail_path, index=False)

    report_payload = {
        "baseline_snapshot": baseline_snapshot,
        "event_conditioned_summary": integrated_summary.to_dict(orient="records"),
        "stress_detail": stress_detail.to_dict(orient="records"),
    }
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report_payload, handle, indent=2)

    lines = [
        "# Integrated Risk Report",
        "",
        f"Portfolio: `{baseline_snapshot['metadata']['portfolio_id']}`",
        f"Date range: `{baseline_snapshot['metadata']['start_date']}` to `{baseline_snapshot['metadata']['end_date']}`",
        "",
        "## Baseline",
        "",
        f"- Annualized volatility: `{baseline_snapshot['portfolio_stats']['annualized_volatility']:.4f}`",
        f"- Normal VaR 1d 99: `{baseline_snapshot['models']['normal_var_loss_1d_99']:.4f}`",
        f"- Filtered historical VaR 1d 99: `{baseline_snapshot['models']['filtered_historical_var_loss_1d_99']:.4f}`",
        "",
        "## Event-conditioned scenarios",
        "",
    ]

    if integrated_summary.empty:
        lines.append("- No relevant events matched the current portfolio.")
    else:
        for _, row in integrated_summary.iterrows():
            age_fragment = ""
            if pd.notna(row.get("event_age_days")):
                age_fragment = (
                    f", age `{row['event_age_days']:.2f}d`, decay `{row['recency_decay']:.2f}`, "
                    f"shock scale `{row['shock_scale']:.2f}`"
                )
            lines.append(
                f"- `{row['event_type']}` on `{','.join(row['tickers'])}` changed normal VaR by "
                f"`{row['delta_normal_var_loss_1d_99']:.4f}`{age_fragment} from headline: {row['headline']}"
            )

    with markdown_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")

    return {
        "json": report_path,
        "summary_csv": summary_path,
        "detail_csv": detail_path,
        "markdown": markdown_path,
    }

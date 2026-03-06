from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from services.capital_workbench import run_capital_sandbox_workbench
from services.pathing import PROJECT_ROOT


def _coerce_as_of_timestamps(values: list[str] | tuple[str, ...]) -> list[str]:
    rows: list[str] = []
    seen: set[str] = set()
    for value in values:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        normalized = timestamp.isoformat()
        if normalized not in seen:
            rows.append(normalized)
            seen.add(normalized)
    return rows


def _build_replay_batch_report(*, batch_summary: pd.DataFrame, batch_paths: pd.DataFrame, metadata: dict[str, Any]) -> str:
    lines = [
        "# Capital Replay Batch Report",
        "",
        f"Portfolio: `{metadata.get('portfolio_id', 'unknown')}`",
        f"Mode: `replay_as_of_timestamp`",
        f"Session minutes: `{metadata.get('session_minutes')}`",
        f"Decision interval seconds: `{metadata.get('decision_interval_seconds')}`",
        f"Providers: `{', '.join(metadata.get('providers', [])) or 'none'}`",
        "",
        "## Replay Windows",
        "",
    ]

    if batch_summary.empty:
        lines.append("- No replay windows were generated.")
        return "\n".join(lines) + "\n"

    for _, row in batch_summary.sort_values("as_of_timestamp").iterrows():
        lines.append(
            f"- as_of `{row['as_of_timestamp']}` anchor `{row['replay_anchor_timestamp']}` "
            f"best `{row['best_path']}` final `{row['best_final_capital']:.2f}` "
            f"strategy `{row['provider_strategy']}` providers `{row['providers_used']}`"
        )

    lines.extend(["", "## Best Windows", ""])
    for _, row in batch_summary.sort_values("best_final_capital", ascending=False).head(5).iterrows():
        lines.append(
            f"- `{row['as_of_timestamp']}` best `{row['best_path']}` final `{row['best_final_capital']:.2f}`"
        )

    lines.extend(["", "## Path Leaderboard", ""])
    if batch_paths.empty:
        lines.append("- No path rows.")
    else:
        leaderboard = (
            batch_paths.groupby("path_name", as_index=False)
            .agg(
                run_count=("path_name", "size"),
                avg_final_capital=("final_capital", "mean"),
                best_final_capital=("final_capital", "max"),
            )
            .sort_values(["avg_final_capital", "best_final_capital"], ascending=False)
        )
        for _, row in leaderboard.iterrows():
            lines.append(
                f"- `{row['path_name']}` runs `{int(row['run_count'])}` avg `{row['avg_final_capital']:.2f}` "
                f"best `{row['best_final_capital']:.2f}`"
            )

    return "\n".join(lines) + "\n"


def run_capital_replay_batch_workbench(
    *,
    portfolio_config: str | Path,
    as_of_timestamps: list[str] | tuple[str, ...],
    initial_capital: float = 100.0,
    decision_interval_seconds: int = 60,
    session_minutes: int = 5,
    providers: list[str] | tuple[str, ...] = ("newsapi",),
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    normalized_as_of = _coerce_as_of_timestamps(as_of_timestamps)
    if not normalized_as_of:
        raise ValueError("at least one as_of timestamp is required")

    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ")
    root = Path(output_dir or (PROJECT_ROOT / "output" / "capital_replay_batch")) / run_id
    root.mkdir(parents=True, exist_ok=True)

    batch_rows: list[dict[str, Any]] = []
    path_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    portfolio_id = "unknown"

    for index, as_of in enumerate(normalized_as_of, start=1):
        replay_root = root / f"replay_{index:02d}"
        result = run_capital_sandbox_workbench(
            portfolio_config=portfolio_config,
            mode="replay_as_of_timestamp",
            initial_capital=initial_capital,
            decision_interval_seconds=decision_interval_seconds,
            session_minutes=session_minutes,
            providers=list(providers),
            as_of_timestamp=as_of,
            output_dir=replay_root,
        )
        results.append(result)
        portfolio_id = result["metadata"]["portfolio_id"]

        summary = result["summary_frame"].copy()
        if summary.empty:
            continue
        summary["as_of_timestamp"] = as_of
        summary["replay_anchor_timestamp"] = result.get("replay_anchor_timestamp")
        summary["provider_strategy"] = result.get("provider_strategy", "unknown")
        summary["providers_used"] = ",".join(result["sync_stats"].get("providers_used", [])) or "none"
        summary["run_output_root"] = result["output_root"]
        path_rows.extend(summary.to_dict(orient="records"))

        best = summary.sort_values("final_capital", ascending=False).iloc[0]
        batch_rows.append(
            {
                "as_of_timestamp": as_of,
                "replay_anchor_timestamp": best["replay_anchor_timestamp"],
                "provider_strategy": best["provider_strategy"],
                "providers_used": best["providers_used"],
                "best_path": best["path_name"],
                "best_final_capital": float(best["final_capital"]),
                "best_total_return": float(best["total_return"]),
                "run_output_root": best["run_output_root"],
            }
        )

    batch_summary = pd.DataFrame(batch_rows)
    batch_paths = pd.DataFrame(path_rows)
    report_markdown = _build_replay_batch_report(
        batch_summary=batch_summary,
        batch_paths=batch_paths,
        metadata={
            "portfolio_id": portfolio_id,
            "session_minutes": int(session_minutes),
            "decision_interval_seconds": int(decision_interval_seconds),
            "providers": list(providers),
        },
    )

    outputs = {
        "summary_csv": root / "replay_batch_summary.csv",
        "paths_csv": root / "replay_batch_paths.csv",
        "report_md": root / "replay_batch_report.md",
        "manifest_json": root / "replay_batch_manifest.json",
    }
    batch_summary.to_csv(outputs["summary_csv"], index=False)
    batch_paths.to_csv(outputs["paths_csv"], index=False)
    outputs["report_md"].write_text(report_markdown, encoding="utf-8")
    outputs["manifest_json"].write_text(
        json.dumps(
            {
                "run_id": run_id,
                "portfolio_id": portfolio_id,
                "session_minutes": int(session_minutes),
                "decision_interval_seconds": int(decision_interval_seconds),
                "providers": list(providers),
                "as_of_timestamps": normalized_as_of,
                "replay_count": len(normalized_as_of),
                "replay_runs": [result["output_root"] for result in results],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "portfolio_id": portfolio_id,
        "output_root": str(root),
        "summary_frame": batch_summary,
        "paths_frame": batch_paths,
        "report_markdown": report_markdown,
        "outputs": {key: str(path) for key, path in outputs.items()},
        "results": results,
    }

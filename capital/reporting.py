from __future__ import annotations

from pathlib import Path
from typing import Any

import json
import matplotlib.pyplot as plt
import pandas as pd


def build_capital_sandbox_report(
    *,
    summary_frame: pd.DataFrame,
    journal_frame: pd.DataFrame,
    snapshot_frame: pd.DataFrame,
    metadata: dict[str, Any],
) -> str:
    best_path = summary_frame.sort_values("final_capital", ascending=False).iloc[0] if not summary_frame.empty else None
    session_meta = metadata.get("session_meta", {}) or {}
    lines = [
        "# Capital Sandbox Report",
        "",
        f"Portfolio: `{metadata.get('portfolio_id', 'unknown')}`",
        f"Mode: `{metadata.get('mode', 'unknown')}`",
        f"Initial capital: `{metadata.get('initial_capital', 0.0):.2f}`",
        f"Decision interval seconds: `{metadata.get('decision_interval_seconds', 0)}`",
        f"Providers used: `{', '.join(metadata.get('providers_used', [])) or 'none'}`",
        f"Provider strategy: `{metadata.get('provider_strategy', 'n/a')}`",
        f"As-of timestamp: `{metadata.get('as_of_timestamp') or 'n/a'}`",
        f"Replay anchor timestamp: `{metadata.get('replay_anchor_timestamp') or 'n/a'}`",
        f"Intraday period: `{metadata.get('intraday_period', 'n/a')}`",
        f"News refresh attempts: `{int(session_meta.get('news_refresh_attempts', 0) or 0)}`",
        f"News refresh successes: `{int(session_meta.get('news_refresh_successes', 0) or 0)}`",
        f"News refresh errors: `{int(session_meta.get('news_refresh_errors', 0) or 0)}`",
        f"News refresh skips: `{int(session_meta.get('news_refresh_skipped', 0) or 0)}`",
        f"Stale price steps: `{int(session_meta.get('stale_price_steps', 0) or 0)}`",
        "",
        "## Result",
        "",
    ]
    if best_path is None:
        lines.append("- No paths were generated.")
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            f"- Best path: `{best_path['path_name']}`",
            f"- Final capital: `{best_path['final_capital']:.2f}`",
            f"- Total return: `{best_path['total_return']:.4f}`",
            f"- Max drawdown: `{best_path['max_drawdown']:.4f}`",
            "",
            "## Path Compare",
            "",
        ]
    )

    for _, row in summary_frame.sort_values("final_capital", ascending=False).iterrows():
        lines.append(
            f"- `{row['path_name']}`: final `{row['final_capital']:.2f}`, "
            f"return `{row['total_return']:.4f}`, max drawdown `{row['max_drawdown']:.4f}`, "
            f"trades `{int(row['trade_count'])}`"
        )

    lines.extend(["", "## Recent Decisions", ""])
    if journal_frame.empty:
        lines.append("- No decision journal rows.")
    else:
        recent = journal_frame.tail(10)
        for _, row in recent.iterrows():
            lines.append(
                f"- `{row['timestamp']}` action `{row['action']}` exposure `{row['target_exposure']:.2f}` "
                f"capital `{row['capital_after_costs']:.2f}` regime `{row['regime']}` "
                f"signal `{row['signal_score']:.4f}` quant `{row.get('quant_confirmation')}` "
                f"path `{row.get('path_confirmation')}` refresh `{row.get('refresh_status', 'n/a')}` "
                f"top event `{row['top_event_type']}`"
            )

    lines.extend(["", "## Minute Snapshots", ""])
    if snapshot_frame.empty:
        lines.append("- No snapshot rows.")
    else:
        for _, row in snapshot_frame.tail(20).iterrows():
            lines.append(
                f"- `{row['snapshot_time']}` `{row['path_name']}` capital `{row['capital']:.2f}`"
            )

    return "\n".join(lines) + "\n"


def build_capital_compare_report(
    *,
    summary_frame: pd.DataFrame,
    snapshot_frame: pd.DataFrame,
    metadata: dict[str, Any],
) -> str:
    lines = [
        "# Capital Sandbox Session Compare",
        "",
        f"Portfolio: `{metadata.get('portfolio_id', 'unknown')}`",
        f"Mode: `{metadata.get('mode', 'unknown')}`",
        f"Initial capital: `{metadata.get('initial_capital', 0.0):.2f}`",
        f"Decision interval seconds: `{metadata.get('decision_interval_seconds', 0)}`",
        f"Session presets: `{', '.join(metadata.get('session_labels', []))}`",
        f"Provider strategy: `{metadata.get('provider_strategy', 'n/a')}`",
        f"As-of timestamp: `{metadata.get('as_of_timestamp') or 'n/a'}`",
        f"Replay anchor timestamp: `{metadata.get('replay_anchor_timestamp') or 'n/a'}`",
        f"Intraday period: `{metadata.get('intraday_period', 'n/a')}`",
        "",
        "## Best By Session",
        "",
    ]
    if summary_frame.empty:
        lines.append("- No session rows.")
        return "\n".join(lines) + "\n"

    best_by_session = (
        summary_frame.sort_values(["session_minutes", "final_capital"], ascending=[True, False])
        .groupby("session_label", as_index=False)
        .first()
    )
    for _, row in best_by_session.iterrows():
        lines.append(
            f"- `{row['session_label']}` best path `{row['path_name']}` final `{row['final_capital']:.2f}` "
            f"return `{row['total_return']:.4f}` trades `{int(row['trade_count'])}`"
        )

    overall_best = summary_frame.sort_values("final_capital", ascending=False).iloc[0]
    lines.extend(
        [
            "",
            "## Overall Best",
            "",
            f"- Session: `{overall_best['session_label']}`",
            f"- Path: `{overall_best['path_name']}`",
            f"- Final capital: `{overall_best['final_capital']:.2f}`",
            "",
            "## Session Compare",
            "",
        ]
    )
    for _, row in summary_frame.sort_values(["session_minutes", "final_capital"], ascending=[True, False]).iterrows():
        lines.append(
            f"- `{row['session_label']}` / `{row['path_name']}`: final `{row['final_capital']:.2f}`, "
            f"return `{row['total_return']:.4f}`, drawdown `{row['max_drawdown']:.4f}`, trades `{int(row['trade_count'])}`"
        )

    if not snapshot_frame.empty:
        lines.extend(["", "## Snapshot Tail", ""])
        for _, row in snapshot_frame.tail(24).iterrows():
            lines.append(
                f"- `{row['session_label']}` `{row['snapshot_time']}` `{row['path_name']}` capital `{row['capital']:.2f}`"
            )

    return "\n".join(lines) + "\n"


def write_capital_sandbox_outputs(
    *,
    output_root: str | Path,
    summary_frame: pd.DataFrame,
    journal_frame: pd.DataFrame,
    equity_frame: pd.DataFrame,
    snapshot_frame: pd.DataFrame,
    report_markdown: str,
) -> dict[str, Path]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    summary_path = root / "capital_sandbox_summary.csv"
    journal_path = root / "decision_journal.csv"
    equity_path = root / "path_equity_curve.csv"
    snapshot_path = root / "capital_minute_snapshots.csv"
    report_path = root / "capital_sandbox_report.md"
    equity_png_path = root / "capital_sandbox_equity_curve.png"

    summary_frame.to_csv(summary_path, index=False)
    journal_frame.to_csv(journal_path, index=False)
    equity_frame.to_csv(equity_path, index=False)
    snapshot_frame.to_csv(snapshot_path, index=False)
    with report_path.open("w", encoding="utf-8") as handle:
        handle.write(report_markdown)
    _write_equity_curve_png(snapshot_frame=snapshot_frame, output_path=equity_png_path, title="Capital Sandbox Equity Curve")

    return {
        "summary_csv": summary_path,
        "journal_csv": journal_path,
        "equity_curve_csv": equity_path,
        "minute_snapshots_csv": snapshot_path,
        "report_md": report_path,
        "equity_curve_png": equity_png_path,
    }


def write_capital_compare_outputs(
    *,
    output_root: str | Path,
    summary_frame: pd.DataFrame,
    journal_frame: pd.DataFrame,
    equity_frame: pd.DataFrame,
    snapshot_frame: pd.DataFrame,
    report_markdown: str,
) -> dict[str, Path]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    summary_path = root / "capital_compare_summary.csv"
    journal_path = root / "capital_compare_journal.csv"
    equity_path = root / "capital_compare_equity_curve.csv"
    snapshot_path = root / "capital_compare_snapshots.csv"
    report_path = root / "capital_compare_report.md"
    equity_png_path = root / "capital_compare_equity_curve.png"
    summary_png_path = root / "capital_compare_final_capital.png"

    summary_frame.to_csv(summary_path, index=False)
    journal_frame.to_csv(journal_path, index=False)
    equity_frame.to_csv(equity_path, index=False)
    snapshot_frame.to_csv(snapshot_path, index=False)
    with report_path.open("w", encoding="utf-8") as handle:
        handle.write(report_markdown)
    _write_equity_curve_png(
        snapshot_frame=snapshot_frame,
        output_path=equity_png_path,
        title="Capital Sandbox Session Compare",
        include_session=True,
    )
    _write_summary_bar_png(summary_frame=summary_frame, output_path=summary_png_path)

    return {
        "summary_csv": summary_path,
        "journal_csv": journal_path,
        "equity_curve_csv": equity_path,
        "snapshots_csv": snapshot_path,
        "report_md": report_path,
        "equity_curve_png": equity_png_path,
        "final_capital_png": summary_png_path,
    }


def write_capital_live_progress(
    *,
    output_root: str | Path,
    status_payload: dict[str, Any],
    journal_frame: pd.DataFrame,
    equity_frame: pd.DataFrame,
    snapshot_frame: pd.DataFrame,
) -> dict[str, Path]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    status_path = root / "live_session_status.json"
    journal_path = root / "decision_journal.live.csv"
    equity_path = root / "path_equity_curve.live.csv"
    snapshot_path = root / "capital_minute_snapshots.live.csv"

    with status_path.open("w", encoding="utf-8") as handle:
        json.dump(status_payload, handle, indent=2, default=str)
    journal_frame.to_csv(journal_path, index=False)
    equity_frame.to_csv(equity_path, index=False)
    snapshot_frame.to_csv(snapshot_path, index=False)

    return {
        "status_json": status_path,
        "journal_csv": journal_path,
        "equity_curve_csv": equity_path,
        "minute_snapshots_csv": snapshot_path,
    }


def _write_equity_curve_png(
    *,
    snapshot_frame: pd.DataFrame,
    output_path: str | Path,
    title: str,
    include_session: bool = False,
) -> None:
    output = Path(output_path)
    if snapshot_frame.empty:
        return

    frame = snapshot_frame.copy()
    frame["snapshot_time"] = pd.to_datetime(frame["snapshot_time"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["snapshot_time"])
    if frame.empty:
        return

    label_column = "path_name"
    if include_session and "session_label" in frame.columns:
        frame["series_label"] = frame["session_label"].astype(str) + " | " + frame["path_name"].astype(str)
        label_column = "series_label"

    pivot = frame.pivot_table(
        index="snapshot_time",
        columns=label_column,
        values="capital",
        aggfunc="last",
    ).sort_index()
    if pivot.empty:
        return

    plt.figure(figsize=(12, 6))
    for column in pivot.columns:
        plt.plot(pivot.index, pivot[column], label=str(column), linewidth=2)
    plt.title(title)
    plt.xlabel("Time")
    plt.ylabel("Capital")
    plt.grid(True, alpha=0.3)
    plt.legend(loc="best", fontsize=8)
    plt.tight_layout()
    plt.savefig(output, dpi=140)
    plt.close()


def _write_summary_bar_png(
    *,
    summary_frame: pd.DataFrame,
    output_path: str | Path,
) -> None:
    output = Path(output_path)
    if summary_frame.empty:
        return

    frame = summary_frame.copy()
    if "session_label" in frame.columns:
        frame["label"] = frame["session_label"].astype(str) + " | " + frame["path_name"].astype(str)
    else:
        frame["label"] = frame["path_name"].astype(str)

    ordered = frame.sort_values("final_capital", ascending=False)
    plt.figure(figsize=(12, 6))
    plt.bar(ordered["label"], ordered["final_capital"])
    plt.title("Final Capital By Path")
    plt.xlabel("Path")
    plt.ylabel("Final Capital")
    plt.xticks(rotation=45, ha="right")
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output, dpi=140)
    plt.close()

from __future__ import annotations

from pathlib import Path
import re
from typing import Any
from html import escape

import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

LIVE_JOURNAL_COLUMNS = [
    "timestamp",
    "capture_timestamp",
    "session_step",
    "path_name",
    "portfolio_return",
    "benchmark_return",
    "capital_after_costs",
    "target_exposure",
    "action",
    "decision_reason",
    "regime",
    "signal_score",
    "quant_confirmation",
    "confirmation_score",
    "path_confirmation",
    "path_confirmation_score",
    "refresh_status",
]

LIVE_EQUITY_COLUMNS = [
    "timestamp",
    "capture_timestamp",
    "session_step",
    "path_name",
    "capital",
]

LIVE_SNAPSHOT_COLUMNS = [
    "snapshot_time",
    "tracking_time",
    "session_step",
    "path_name",
    "capital",
]


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
    tracking_html_path = root / "capital_sandbox_tracking_log.html"
    tracking_html_versioned_path = root / _build_versioned_tracking_filename(
        root=root,
        base_name="capital_sandbox_tracking_log",
        suffix=".html",
    )

    summary_frame.to_csv(summary_path, index=False)
    journal_frame.to_csv(journal_path, index=False)
    equity_frame.to_csv(equity_path, index=False)
    snapshot_frame.to_csv(snapshot_path, index=False)
    with report_path.open("w", encoding="utf-8") as handle:
        handle.write(report_markdown)
    _write_equity_curve_png(snapshot_frame=snapshot_frame, output_path=equity_png_path, title="Capital Sandbox Equity Curve")
    _write_capital_tracking_html(
        output_path=tracking_html_path,
        title="Capital Sandbox Tracking Log",
        summary_frame=summary_frame,
        journal_frame=journal_frame,
        snapshot_frame=snapshot_frame,
        status_payload=None,
        live_equity_curve_png=equity_png_path,
        minute_snapshot_images=[],
        auto_refresh_seconds=None,
    )
    if tracking_html_versioned_path != tracking_html_path:
        tracking_html_versioned_path.write_text(tracking_html_path.read_text(encoding="utf-8"), encoding="utf-8")

    return {
        "summary_csv": summary_path,
        "journal_csv": journal_path,
        "equity_curve_csv": equity_path,
        "minute_snapshots_csv": snapshot_path,
        "report_md": report_path,
        "equity_curve_png": equity_png_path,
        "tracking_html": tracking_html_path,
        "tracking_html_versioned": tracking_html_versioned_path,
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
    tracking_html_path = root / "capital_compare_tracking_log.html"
    tracking_html_versioned_path = root / _build_versioned_tracking_filename(
        root=root,
        base_name="capital_compare_tracking_log",
        suffix=".html",
    )

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
    _write_capital_tracking_html(
        output_path=tracking_html_path,
        title="Capital Sandbox Compare Tracking Log",
        summary_frame=summary_frame,
        journal_frame=journal_frame,
        snapshot_frame=snapshot_frame,
        status_payload=None,
        live_equity_curve_png=equity_png_path,
        minute_snapshot_images=[summary_png_path],
        auto_refresh_seconds=None,
    )
    if tracking_html_versioned_path != tracking_html_path:
        tracking_html_versioned_path.write_text(tracking_html_path.read_text(encoding="utf-8"), encoding="utf-8")

    return {
        "summary_csv": summary_path,
        "journal_csv": journal_path,
        "equity_curve_csv": equity_path,
        "snapshots_csv": snapshot_path,
        "report_md": report_path,
        "equity_curve_png": equity_png_path,
        "final_capital_png": summary_png_path,
        "tracking_html": tracking_html_path,
        "tracking_html_versioned": tracking_html_versioned_path,
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
    live_equity_png_path = root / "capital_sandbox_equity_curve.live.png"
    minute_snapshot_image_dir = root / "minute_snapshot_images"
    tracking_html_path = root / "capital_sandbox_tracking_log.live.html"
    tracking_html_versioned_path = root / _build_versioned_tracking_filename(
        root=root,
        base_name="capital_sandbox_tracking_log",
        suffix=".live.html",
    )

    journal_frame = _ensure_live_frame_schema(journal_frame, LIVE_JOURNAL_COLUMNS)
    equity_frame = _ensure_live_frame_schema(equity_frame, LIVE_EQUITY_COLUMNS)
    snapshot_frame = _ensure_live_frame_schema(snapshot_frame, LIVE_SNAPSHOT_COLUMNS)

    with status_path.open("w", encoding="utf-8") as handle:
        json.dump(status_payload, handle, indent=2, default=str)
    journal_frame.to_csv(journal_path, index=False)
    equity_frame.to_csv(equity_path, index=False)
    snapshot_frame.to_csv(snapshot_path, index=False)
    _write_equity_curve_png(
        snapshot_frame=snapshot_frame,
        output_path=live_equity_png_path,
        title="Capital Sandbox Live Equity Curve",
    )
    archived_snapshot_png = _write_live_snapshot_archive_png(
        snapshot_frame=snapshot_frame,
        output_dir=minute_snapshot_image_dir,
    )
    minute_snapshot_images = sorted(minute_snapshot_image_dir.glob("*.png"), reverse=True) if minute_snapshot_image_dir.exists() else []
    _write_capital_tracking_html(
        output_path=tracking_html_path,
        title="Capital Sandbox Live Tracking Log",
        summary_frame=pd.DataFrame([status_payload.get("best_path", {})]) if status_payload.get("best_path") else pd.DataFrame(),
        journal_frame=journal_frame,
        snapshot_frame=snapshot_frame,
        status_payload=status_payload,
        live_equity_curve_png=live_equity_png_path if live_equity_png_path.exists() else None,
        minute_snapshot_images=minute_snapshot_images[:12],
        auto_refresh_seconds=5 if status_payload.get("status") in {"running", "completing"} else None,
    )
    if tracking_html_versioned_path != tracking_html_path:
        tracking_html_versioned_path.write_text(tracking_html_path.read_text(encoding="utf-8"), encoding="utf-8")

    return {
        "status_json": status_path,
        "journal_csv": journal_path,
        "equity_curve_csv": equity_path,
        "minute_snapshots_csv": snapshot_path,
        "live_equity_curve_png": live_equity_png_path,
        "latest_minute_snapshot_png": archived_snapshot_png,
        "minute_snapshot_image_dir": minute_snapshot_image_dir,
        "tracking_html": tracking_html_path,
        "tracking_html_versioned": tracking_html_versioned_path,
    }


def _ensure_live_frame_schema(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty and len(frame.columns) == 0:
        return pd.DataFrame(columns=columns)
    working = frame.copy()
    for column in columns:
        if column not in working.columns:
            working[column] = pd.NA
    ordered = [column for column in columns if column in working.columns]
    remainder = [column for column in working.columns if column not in ordered]
    return working.loc[:, ordered + remainder]


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
    if "tracking_time" in frame.columns:
        frame["tracking_time"] = pd.to_datetime(frame["tracking_time"], utc=True, errors="coerce")
    time_column = "tracking_time" if "tracking_time" in frame.columns and frame["tracking_time"].notna().any() else "snapshot_time"
    frame = frame.dropna(subset=[time_column])
    if frame.empty:
        return

    label_column = "path_name"
    if include_session and "session_label" in frame.columns:
        frame["series_label"] = frame["session_label"].astype(str) + " | " + frame["path_name"].astype(str)
        label_column = "series_label"

    pivot = frame.pivot_table(
        index=time_column,
        columns=label_column,
        values="capital",
        aggfunc="last",
    ).sort_index()
    if pivot.empty:
        return

    plt.figure(figsize=(12, 6))
    single_snapshot = len(pivot.index) == 1
    for column in pivot.columns:
        series = pivot[column].dropna()
        if series.empty:
            continue
        plt.plot(
            series.index,
            series.values,
            label=str(column),
            linewidth=2,
            marker="o" if len(series.index) <= 2 else None,
            markersize=7 if len(series.index) <= 2 else None,
        )
    plt.title(title)
    plt.xlabel("Time")
    plt.ylabel("Capital")
    plt.grid(True, alpha=0.3)
    if single_snapshot:
        anchor = pivot.index[0]
        plt.xlim(anchor - pd.Timedelta(minutes=1), anchor + pd.Timedelta(minutes=1))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
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


def _write_live_snapshot_archive_png(
    *,
    snapshot_frame: pd.DataFrame,
    output_dir: str | Path,
) -> Path | None:
    if snapshot_frame.empty:
        return None

    frame = snapshot_frame.copy()
    frame["snapshot_time"] = pd.to_datetime(frame["snapshot_time"], utc=True, errors="coerce")
    if "tracking_time" in frame.columns:
        frame["tracking_time"] = pd.to_datetime(frame["tracking_time"], utc=True, errors="coerce")
    time_column = "tracking_time" if "tracking_time" in frame.columns and frame["tracking_time"].notna().any() else "snapshot_time"
    frame = frame.dropna(subset=[time_column])
    if frame.empty:
        return None

    latest_snapshot_time = frame[time_column].max()
    latest_rows = frame.loc[frame[time_column] == latest_snapshot_time].copy()
    if latest_rows.empty:
        return None

    image_dir = Path(output_dir)
    image_dir.mkdir(parents=True, exist_ok=True)
    snapshot_label = _sanitize_snapshot_label(str(latest_snapshot_time.isoformat()))
    output_path = image_dir / f"{len(frame[time_column].drop_duplicates()):04d}_{snapshot_label}.png"
    _write_equity_curve_png(
        snapshot_frame=frame,
        output_path=output_path,
        title=f"Capital Sandbox Live Snapshot {latest_snapshot_time.isoformat()}",
    )
    return output_path


def _sanitize_snapshot_label(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z_-]+", "_", value).strip("_")


def _build_versioned_tracking_filename(*, root: Path, base_name: str, suffix: str) -> str:
    session_label = _sanitize_snapshot_label(root.name)
    return f"{base_name}.{session_label}{suffix}"


def _write_capital_tracking_html(
    *,
    output_path: str | Path,
    title: str,
    summary_frame: pd.DataFrame,
    journal_frame: pd.DataFrame,
    snapshot_frame: pd.DataFrame,
    status_payload: dict[str, Any] | None,
    live_equity_curve_png: str | Path | None,
    minute_snapshot_images: list[Path],
    auto_refresh_seconds: int | None,
) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    refresh_tag = (
        f'<meta http-equiv="refresh" content="{int(auto_refresh_seconds)}">'
        if auto_refresh_seconds is not None
        else ""
    )
    status_html = _render_dict_table(status_payload or {})
    summary_html = _render_dataframe_html(summary_frame, max_rows=20)
    journal_html = _render_dataframe_html(journal_frame.tail(20), max_rows=20)
    snapshot_html = _render_dataframe_html(snapshot_frame.tail(30), max_rows=30)

    curve_html = ""
    if live_equity_curve_png is not None:
        curve_path = Path(live_equity_curve_png)
        if curve_path.exists():
            curve_html = (
                "<section><h2>Live Equity Curve</h2>"
                f'<img src="{escape(curve_path.name)}" alt="Live equity curve" style="max-width:100%;border:1px solid #333;border-radius:8px;">'
                "</section>"
            )

    images_html = ""
    if minute_snapshot_images:
        parts = ["<section><h2>Minute Snapshot Images</h2><div class='grid'>"]
        for image_path in minute_snapshot_images:
            rel = escape(str(image_path.relative_to(output.parent)).replace("\\", "/"))
            parts.append(
                "<figure>"
                f'<img src="{rel}" alt="{escape(image_path.name)}" style="width:100%;border:1px solid #333;border-radius:8px;">'
                f"<figcaption>{escape(image_path.name)}</figcaption>"
                "</figure>"
            )
        parts.append("</div></section>")
        images_html = "".join(parts)

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  {refresh_tag}
  <title>{escape(title)}</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; background: #0b1020; color: #f3f4f6; margin: 0; padding: 24px; }}
    h1, h2 {{ margin: 0 0 12px 0; }}
    section {{ margin: 0 0 24px 0; padding: 16px; background: #121a2c; border: 1px solid #22304d; border-radius: 10px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border: 1px solid #2c3d60; padding: 6px 8px; text-align: left; vertical-align: top; }}
    th {{ background: #17233b; }}
    code {{ background: #111827; padding: 2px 6px; border-radius: 4px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }}
    .muted {{ opacity: 0.75; }}
  </style>
</head>
<body>
  <h1>{escape(title)}</h1>
  <p class="muted">Generated at {escape(pd.Timestamp.now(tz="UTC").isoformat())}</p>
  <section><h2>Status</h2>{status_html}</section>
  {curve_html}
  {images_html}
  <section><h2>Summary</h2>{summary_html}</section>
  <section><h2>Recent Quant / Journal Rows</h2>{journal_html}</section>
  <section><h2>Recent Snapshot Rows</h2>{snapshot_html}</section>
</body>
</html>
"""
    output.write_text(html, encoding="utf-8")


def _render_dataframe_html(frame: pd.DataFrame, *, max_rows: int) -> str:
    if frame is None or frame.empty:
        return "<p>No rows.</p>"
    limited = frame.head(max_rows).copy()
    safe = limited.fillna("").astype(str)
    return safe.to_html(index=False, escape=True, border=0)


def _render_dict_table(payload: dict[str, Any]) -> str:
    if not payload:
        return "<p>No status payload.</p>"
    rows = []
    for key, value in payload.items():
        rows.append(
            "<tr>"
            f"<th>{escape(str(key))}</th>"
            f"<td><code>{escape(str(value))}</code></td>"
            "</tr>"
        )
    return f"<table>{''.join(rows)}</table>"

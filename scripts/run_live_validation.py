from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.live_validation import (
    build_validation_windows,
    collect_gap_samples,
    load_events_frame,
    load_json,
    load_symbol_universe,
    summarize_validation_runs,
)
from event_engine.run_logging import append_run_event

DEFAULT_PROVIDERS = ["marketaux", "thenewsapi", "alphavantage"]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a larger-sample validation over multiple live Marketaux windows.")
    parser.add_argument("--windows", type=int, default=3, help="Number of windows to evaluate.")
    parser.add_argument("--window-days", type=int, default=3, help="Window width in days.")
    parser.add_argument("--step-days", type=int, default=2, help="Gap between consecutive windows in days.")
    parser.add_argument(
        "--as-of",
        default=pd.Timestamp.today(tz="UTC").date().isoformat(),
        help="Anchor date for the most recent validation window.",
    )
    parser.add_argument("--symbols", nargs="+", default=None, help="Ticker symbols to query.")
    parser.add_argument(
        "--symbols-config",
        default=str(PROJECT_ROOT / "config" / "validation" / "live_validation_universe.yaml"),
        help="Optional YAML symbol-universe config used when --symbols is not provided.",
    )
    parser.add_argument(
        "--symbol-pack",
        default="",
        help="Optional thematic pack name loaded from the symbol config when --symbols is not provided.",
    )
    parser.add_argument("--language", default="en", help="Language filter.")
    parser.add_argument(
        "--providers",
        nargs="*",
        default=DEFAULT_PROVIDERS,
        help="Ordered providers forwarded to the live watchlist runner.",
    )
    parser.add_argument("--limit", type=int, default=3, help="Articles per page.")
    parser.add_argument("--max-pages", type=int, default=2, help="Maximum pages fetched per window.")
    parser.add_argument(
        "--symbol-batch-size",
        type=int,
        default=5,
        help="Maximum symbols per upstream Marketaux query batch.",
    )
    parser.add_argument(
        "--watchlist-config",
        default=str(PROJECT_ROOT / "config" / "watchlists" / "validation_watchlist.yaml"),
        help="Watchlist config forwarded to the live runner.",
    )
    parser.add_argument(
        "--event-map-config",
        default="",
        help="Optional event map override forwarded to the live runner.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "output" / "live_validation"),
        help="Output directory for validation artifacts.",
    )
    parser.add_argument(
        "--archive-only",
        action="store_true",
        help="Skip live sync and reuse matching archived windows only.",
    )
    parser.add_argument(
        "--promotion-scope",
        default="live",
        help="Scope label written into the validation summary (`live` or `backfill`).",
    )
    return parser


def _json_default(value: Any):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value)} is not JSON serializable")


def _failure_allows_archive_reuse(run_dir: Path | None) -> bool:
    classification = _classify_failure(run_dir)
    return classification["allows_archive_reuse"]


def _classify_failure(run_dir: Path | None, stderr: str = "", stdout: str = "") -> dict[str, Any]:
    text_parts = [stderr or "", stdout or ""]
    if run_dir is not None:
        failure_manifest = run_dir / "failure_manifest.json"
        if failure_manifest.exists():
            payload = load_json(failure_manifest)
            text_parts.extend(
                [
                    str(payload.get("error_type", "")),
                    str(payload.get("error_message", "")),
                    str(payload.get("traceback", "")),
                ]
            )
    text = " ".join(text_parts).lower()
    quota_blocked = (
        "payment required" in text
        or "402" in text
        or "quota" in text
        or "limit reached" in text
        or "daily limit" in text
    )
    allows_archive_reuse = quota_blocked or (
        "marketaux sync failed for all symbol batches" in text or "marketaux fetch failed after" in text
    )
    return {
        "quota_blocked": quota_blocked,
        "allows_archive_reuse": allows_archive_reuse,
        "failure_text": text.strip(),
    }


def _find_reusable_live_run(
    *,
    window: dict[str, str],
    symbols: list[str],
    current_output_root: Path,
) -> Path | None:
    requested_symbols = sorted({str(symbol).upper() for symbol in symbols})
    candidate_roots = [
        PROJECT_ROOT / "output" / "live_validation",
        PROJECT_ROOT / "output" / "live_marketaux_watchlist",
    ]
    candidates: list[Path] = []

    for root in candidate_roots:
        for manifest_path in root.rglob("live_marketaux_manifest.json"):
            if current_output_root in manifest_path.parents:
                continue
            manifest = load_json(manifest_path)
            request = manifest.get("sync_stats", {}).get("request", {})
            manifest_symbols = sorted({str(symbol).upper() for symbol in (request.get("symbols") or [])})
            if request.get("published_after") != window["published_after"]:
                continue
            if request.get("published_before") != window["published_before"]:
                continue
            if manifest_symbols != requested_symbols:
                continue
            if int(manifest.get("pipeline_stats", {}).get("events", 0) or 0) < 1:
                continue
            candidates.append(manifest_path.parent)

    return sorted(candidates)[-1] if candidates else None


def main() -> int:
    args = _build_parser().parse_args()
    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_dir) / run_id
    runs_root = output_root / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    run_log_path = output_root / "run_log.jsonl"
    symbols = [str(symbol).upper() for symbol in (args.symbols or []) if str(symbol).strip()]
    if not symbols:
        symbols = load_symbol_universe(args.symbols_config, pack=args.symbol_pack or None)

    windows = build_validation_windows(
        as_of=args.as_of,
        windows=args.windows,
        window_days=args.window_days,
        step_days=args.step_days,
    )
    append_run_event(
        run_log_path,
        stage="validation",
        status="start",
        details={
            "windows": args.windows,
            "window_days": args.window_days,
            "step_days": args.step_days,
            "as_of": args.as_of,
            "symbols": symbols,
            "symbol_pack": args.symbol_pack or None,
            "providers": args.providers,
        },
    )

    summary_rows: list[dict[str, Any]] = []
    gap_frames: list[pd.DataFrame] = []

    runner_path = PROJECT_ROOT / "scripts" / "run_live_marketaux_watchlist.py"
    for window in windows:
        window_root = runs_root / window["window_label"]
        window_root.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(runner_path),
            "--published-after",
            window["published_after"],
            "--published-before",
            window["published_before"],
            "--symbols",
            *symbols,
            "--providers",
            *args.providers,
            "--language",
            args.language,
            "--limit",
            str(args.limit),
            "--max-pages",
            str(args.max_pages),
            "--symbol-batch-size",
            str(args.symbol_batch_size),
            "--watchlist-config",
            args.watchlist_config,
            "--output-dir",
            str(window_root),
        ]
        if args.event_map_config:
            command.extend(["--event-map-config", args.event_map_config])

        append_run_event(
            run_log_path,
            stage=window["window_label"],
            status="start",
            details={"command": None if args.archive_only else command, "archive_only": args.archive_only},
        )
        if args.archive_only:
            completed = subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")
            run_dir = None
            reused_run_dir = _find_reusable_live_run(
                window=window,
                symbols=symbols,
                current_output_root=output_root,
            )
            failure_context = {"quota_blocked": False, "allows_archive_reuse": bool(reused_run_dir), "failure_text": ""}
        else:
            completed = subprocess.run(command, check=False, capture_output=True, text=True)
            created_runs = sorted(window_root.glob("*"))
            run_dir = created_runs[-1] if created_runs else None
            reused_run_dir = None
            failure_context = _classify_failure(run_dir, completed.stderr, completed.stdout)

        if (not args.archive_only) and (completed.returncode != 0 or run_dir is None):
            if failure_context["allows_archive_reuse"]:
                reused_run_dir = _find_reusable_live_run(
                    window=window,
                    symbols=symbols,
                    current_output_root=output_root,
                )
            if reused_run_dir is None:
                summary_rows.append(
                    {
                        **window,
                        "status": "failed",
                        "window_origin": "failed",
                        "fresh_sync_requested": not args.archive_only,
                        "quota_blocked": bool(failure_context["quota_blocked"]),
                        "returncode": int(completed.returncode),
                        "stderr": completed.stderr.strip(),
                        "stdout": completed.stdout.strip(),
                        "failure_text": failure_context["failure_text"],
                        "run_dir": str(run_dir) if run_dir else None,
                        "total_events": 0,
                        "event_rows": 0,
                        "watchlist_eligible_rate": 0.0,
                        "filtered_rate": 0.0,
                        "other_rate": 0.0,
                        "suspicious_link_rate": 0.0,
                        "event_type_distribution": {},
                        "quality_distribution": {},
                    }
                )
                append_run_event(
                    run_log_path,
                    stage=window["window_label"],
                    status="error",
                    message=completed.stderr.strip() or "validation window failed",
                    details={"returncode": int(completed.returncode)},
                )
                continue
            run_dir = reused_run_dir
        elif args.archive_only and reused_run_dir is None:
            summary_rows.append(
                {
                    **window,
                    "status": "failed",
                    "window_origin": "failed",
                    "fresh_sync_requested": False,
                    "quota_blocked": False,
                    "returncode": 0,
                    "stderr": "",
                    "stdout": "",
                    "failure_text": "archive_only_no_matching_run",
                    "run_dir": None,
                    "total_events": 0,
                    "event_rows": 0,
                    "watchlist_eligible_rate": 0.0,
                    "filtered_rate": 0.0,
                    "other_rate": 0.0,
                    "suspicious_link_rate": 0.0,
                    "event_type_distribution": {},
                    "quality_distribution": {},
                }
            )
            append_run_event(
                run_log_path,
                stage=window["window_label"],
                status="error",
                message="archive-only validation could not find a matching archived run",
            )
            continue
        elif args.archive_only:
            run_dir = reused_run_dir

        manifest = load_json(run_dir / "live_marketaux_manifest.json")
        audit = manifest.get("live_audit_summary", {})
        event_type_distribution = audit.get("event_type_distribution", {})
        quality_distribution = audit.get("quality_distribution", {})
        total_events = int(audit.get("total_events", 0))
        watchlist_eligible_events = int(audit.get("watchlist_eligible_events", 0))
        filtered_events = int(audit.get("filtered_events", 0))
        suspicious_link_events = int(audit.get("suspicious_link_events", 0))
        eligible_suspicious_link_events = int(audit.get("eligible_suspicious_link_events", 0))
        other_events = int(event_type_distribution.get("other", 0))
        eligible_event_type_distribution = audit.get("eligible_event_type_distribution", {})
        active_other_events = int(eligible_event_type_distribution.get("other", 0))

        watchlist_summary_path = Path(manifest["outputs"]["summary_csv"])
        watchlist_summary = pd.read_csv(watchlist_summary_path) if watchlist_summary_path.exists() else pd.DataFrame()
        top_portfolio = None
        top_delta_var = None
        if not watchlist_summary.empty:
            top_row = watchlist_summary.sort_values(
                ["max_delta_normal_var_loss_1d_99", "stressed_normal_var_loss_1d_99"],
                ascending=[False, False],
            ).iloc[0]
            top_portfolio = top_row.get("portfolio_id")
            top_delta_var = float(top_row.get("max_delta_normal_var_loss_1d_99", 0.0))

        events_path = run_dir / "repository" / "datasets" / "processed_news" / "events.jsonl"
        events_frame = load_events_frame(events_path) if events_path.exists() else pd.DataFrame()
        gap_frame = collect_gap_samples(
            events_frame=events_frame,
            window_label=window["window_label"],
            run_dir=run_dir,
        )
        if not gap_frame.empty:
            gap_frames.append(gap_frame)

        row = {
            **window,
            "status": "success",
            "window_origin": "archive_reuse" if reused_run_dir else "fresh_sync",
            "fresh_sync_requested": not args.archive_only,
            "quota_blocked": bool(failure_context["quota_blocked"]) if reused_run_dir else False,
            "returncode": int(completed.returncode),
            "run_dir": str(run_dir),
            "reused_from_archive": bool(reused_run_dir),
            "reused_run_dir": str(reused_run_dir) if reused_run_dir else None,
            "inserted": int(manifest.get("sync_stats", {}).get("inserted", 0)),
            "events": int(manifest.get("pipeline_stats", {}).get("events", 0)),
            "portfolio_count": int(manifest.get("portfolio_count", 0)),
            "event_rows": int(manifest.get("event_rows", 0)),
            "symbols": ",".join(symbols),
            "total_events": total_events,
            "watchlist_eligible_events": watchlist_eligible_events,
            "filtered_events": filtered_events,
            "suspicious_link_events": suspicious_link_events,
            "eligible_suspicious_link_events": eligible_suspicious_link_events,
            "zero_link_non_macro_events": int(audit.get("zero_link_non_macro_events", 0)),
            "other_events": other_events,
            "watchlist_eligible_rate": float(watchlist_eligible_events / total_events) if total_events else 0.0,
            "filtered_rate": float(filtered_events / total_events) if total_events else 0.0,
            "other_rate": float(other_events / total_events) if total_events else 0.0,
            "suspicious_link_rate": float(suspicious_link_events / total_events) if total_events else 0.0,
            "active_other_rate": (
                float(active_other_events / watchlist_eligible_events) if watchlist_eligible_events else 0.0
            ),
            "active_suspicious_link_rate": (
                float(eligible_suspicious_link_events / watchlist_eligible_events)
                if watchlist_eligible_events
                else 0.0
            ),
            "top_portfolio": top_portfolio,
            "top_delta_var": top_delta_var,
            "event_type_distribution": event_type_distribution,
            "quality_distribution": quality_distribution,
            "eligible_event_type_distribution": eligible_event_type_distribution,
        }
        summary_rows.append(row)
        append_run_event(
            run_log_path,
            stage=window["window_label"],
            status="success",
            details={
                "run_dir": str(run_dir),
                "reused_from_archive": bool(reused_run_dir),
                "total_events": total_events,
                "event_rows": int(manifest.get("event_rows", 0)),
            },
        )

    summary_frame = pd.DataFrame(summary_rows)
    gap_frame = pd.concat(gap_frames, ignore_index=True) if gap_frames else pd.DataFrame()
    aggregate_summary = summarize_validation_runs(summary_frame)

    summary_csv = output_root / "validation_window_summary.csv"
    gaps_csv = output_root / "taxonomy_gap_samples.csv"
    summary_json = output_root / "validation_summary.json"
    report_md = output_root / "validation_report.md"

    summary_frame.to_csv(summary_csv, index=False)
    gap_frame.to_csv(gaps_csv, index=False)
    with summary_json.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "aggregate": aggregate_summary,
                "windows": summary_rows,
                "gap_sample_count": int(len(gap_frame)),
                "symbols": symbols,
                "as_of": args.as_of,
                "promotion_scope": args.promotion_scope,
                "run_log": str(run_log_path),
            },
            handle,
            indent=2,
            default=_json_default,
        )

    lines = [
        "# Live Validation Report",
        "",
        f"Windows requested: `{aggregate_summary['n_windows']}`",
        f"Successful windows: `{aggregate_summary['successful_windows']}`",
        f"Total events: `{aggregate_summary['total_events']}`",
        f"Total watchlist event rows: `{aggregate_summary['total_event_rows']}`",
        "",
        "## Aggregate Rates",
        "",
        f"- avg watchlist eligible rate: `{aggregate_summary['avg_watchlist_eligible_rate']}`",
        f"- avg filtered rate: `{aggregate_summary['avg_filtered_rate']}`",
        f"- avg other rate: `{aggregate_summary['avg_other_rate']}`",
        f"- avg suspicious link rate: `{aggregate_summary['avg_suspicious_link_rate']}`",
        "",
        "## Windows",
        "",
    ]
    for _, row in summary_frame.iterrows():
        lines.append(
            f"- `{row['window_label']}` `{row['published_after']} -> {row['published_before']}` | "
            f"status `{row['status']}` | origin `{row.get('window_origin')}` | "
            f"events `{row.get('total_events', 0)}` | "
            f"other rate `{row.get('other_rate', 0.0)}` | top portfolio `{row.get('top_portfolio')}`"
        )
    if not gap_frame.empty:
        lines.extend(["", "## Gap Samples", ""])
        for _, row in gap_frame.head(10).iterrows():
            lines.append(
                f"- `{row['window_label']}` | `{row['gap_reason']}` | `{row['event_type']}` | {row['headline']}"
            )
    with report_md.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")

    append_run_event(
        run_log_path,
        stage="validation",
        status="success",
        details={"summary_json": str(summary_json), "gap_sample_count": int(len(gap_frame))},
    )
    print("[OK] Live validation completed.")
    print(f"Windows: {aggregate_summary['successful_windows']}/{aggregate_summary['n_windows']}")
    print(f"Output: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

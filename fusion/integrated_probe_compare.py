from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


def _load_summary_frame(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if frame.empty:
        return frame
    numeric_columns = [
        "delta_normal_var_loss_1d_99",
        "stressed_normal_var_loss_1d_99",
        "delta_normal_es_loss_1d_99",
        "stressed_normal_es_loss_1d_99",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _load_report(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _baseline_var_1d(report: dict[str, Any]) -> float:
    return float(report["baseline_snapshot"]["models"]["normal_var_loss_1d_99"])


def _portfolio_id(report: dict[str, Any]) -> str:
    return str(report["baseline_snapshot"]["metadata"]["portfolio_id"])


def _safe_float(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return float(value)


def compare_probe_pair(
    *,
    portfolio_id: str,
    base_run_dir: str | Path,
    guarded_run_dir: str | Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    base_run_dir = Path(base_run_dir)
    guarded_run_dir = Path(guarded_run_dir)

    base_summary = _load_summary_frame(base_run_dir / "integrated_summary.csv")
    guarded_summary = _load_summary_frame(guarded_run_dir / "integrated_summary.csv")
    base_report = _load_report(base_run_dir / "integrated_report.json")
    guarded_report = _load_report(guarded_run_dir / "integrated_report.json")

    canonical_portfolio_id = portfolio_id or _portfolio_id(base_report)
    if canonical_portfolio_id != _portfolio_id(base_report):
        raise ValueError("Portfolio id does not match base probe report.")
    if canonical_portfolio_id != _portfolio_id(guarded_report):
        raise ValueError("Portfolio id does not match guarded probe report.")

    base_frame = base_summary.copy()
    guarded_frame = guarded_summary.copy()
    if "event_subtype" not in base_frame.columns:
        base_frame["event_subtype"] = "unknown"
    if "event_subtype" not in guarded_frame.columns:
        guarded_frame["event_subtype"] = "unknown"
    base_frame["event_subtype"] = base_frame["event_subtype"].fillna("unknown")
    guarded_frame["event_subtype"] = guarded_frame["event_subtype"].fillna("unknown")

    base_view = base_frame.loc[
        :,
        [
            "event_id",
            "event_type",
            "event_subtype",
            "headline",
            "delta_normal_var_loss_1d_99",
            "stressed_normal_var_loss_1d_99",
        ],
    ].rename(
        columns={
            "delta_normal_var_loss_1d_99": "base_delta_normal_var_loss_1d_99",
            "stressed_normal_var_loss_1d_99": "base_stressed_normal_var_loss_1d_99",
        }
    )
    guarded_view = guarded_frame.loc[
        :,
        [
            "event_id",
            "event_type",
            "event_subtype",
            "headline",
            "delta_normal_var_loss_1d_99",
            "stressed_normal_var_loss_1d_99",
        ],
    ].rename(
        columns={
            "delta_normal_var_loss_1d_99": "guarded_delta_normal_var_loss_1d_99",
            "stressed_normal_var_loss_1d_99": "guarded_stressed_normal_var_loss_1d_99",
        }
    )

    events = base_view.merge(
        guarded_view,
        on=["event_id", "event_type", "event_subtype", "headline"],
        how="outer",
    )
    events.insert(0, "portfolio_id", canonical_portfolio_id)
    events["base_delta_normal_var_loss_1d_99"] = pd.to_numeric(
        events["base_delta_normal_var_loss_1d_99"], errors="coerce"
    )
    events["guarded_delta_normal_var_loss_1d_99"] = pd.to_numeric(
        events["guarded_delta_normal_var_loss_1d_99"], errors="coerce"
    )
    events["base_stressed_normal_var_loss_1d_99"] = pd.to_numeric(
        events["base_stressed_normal_var_loss_1d_99"], errors="coerce"
    )
    events["guarded_stressed_normal_var_loss_1d_99"] = pd.to_numeric(
        events["guarded_stressed_normal_var_loss_1d_99"], errors="coerce"
    )
    events["abs_delta_reduction"] = (
        events["base_delta_normal_var_loss_1d_99"].abs()
        - events["guarded_delta_normal_var_loss_1d_99"].abs()
    )
    events["avg_stressed_reduction"] = (
        events["base_stressed_normal_var_loss_1d_99"] - events["guarded_stressed_normal_var_loss_1d_99"]
    )
    events["comparison_status"] = "matched"
    events.loc[
        events["base_delta_normal_var_loss_1d_99"].isna(),
        "comparison_status",
    ] = "guarded_only"
    events.loc[
        events["guarded_delta_normal_var_loss_1d_99"].isna(),
        "comparison_status",
    ] = "base_only"

    summary = {
        "portfolio_id": canonical_portfolio_id,
        "base_run_dir": str(base_run_dir),
        "guarded_run_dir": str(guarded_run_dir),
        "baseline_var_1d": _baseline_var_1d(base_report),
        "base_event_count": int(len(base_frame)),
        "guarded_event_count": int(len(guarded_frame)),
        "base_avg_delta_normal_var_loss_1d_99": _safe_float(base_frame["delta_normal_var_loss_1d_99"].mean()),
        "guarded_avg_delta_normal_var_loss_1d_99": _safe_float(guarded_frame["delta_normal_var_loss_1d_99"].mean()),
        "base_avg_stressed_normal_var_loss_1d_99": _safe_float(base_frame["stressed_normal_var_loss_1d_99"].mean()),
        "guarded_avg_stressed_normal_var_loss_1d_99": _safe_float(
            guarded_frame["stressed_normal_var_loss_1d_99"].mean()
        ),
        "base_sum_delta_normal_var_loss_1d_99": _safe_float(base_frame["delta_normal_var_loss_1d_99"].sum()),
        "guarded_sum_delta_normal_var_loss_1d_99": _safe_float(guarded_frame["delta_normal_var_loss_1d_99"].sum()),
        "avg_delta_improvement": _safe_float(
            base_frame["delta_normal_var_loss_1d_99"].mean() - guarded_frame["delta_normal_var_loss_1d_99"].mean()
        ),
        "avg_stressed_improvement": _safe_float(
            base_frame["stressed_normal_var_loss_1d_99"].mean()
            - guarded_frame["stressed_normal_var_loss_1d_99"].mean()
        ),
        "matched_event_count": int((events["comparison_status"] == "matched").sum()),
        "guarded_only_event_count": int((events["comparison_status"] == "guarded_only").sum()),
        "base_only_event_count": int((events["comparison_status"] == "base_only").sum()),
        "top_reduction_event_id": None,
        "top_reduction_event_type": None,
        "top_reduction_headline": None,
        "top_reduction_abs_delta": None,
    }

    matched = events.loc[events["comparison_status"] == "matched"].sort_values(
        "abs_delta_reduction", ascending=False, na_position="last"
    )
    if not matched.empty:
        top = matched.iloc[0]
        summary["top_reduction_event_id"] = str(top["event_id"])
        summary["top_reduction_event_type"] = str(top["event_type"])
        summary["top_reduction_headline"] = str(top["headline"])
        summary["top_reduction_abs_delta"] = _safe_float(top["abs_delta_reduction"])

    return summary, events


def compare_probe_pairs(
    pairs: Iterable[tuple[str, str | Path, str | Path]]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    summaries: list[dict[str, Any]] = []
    event_frames: list[pd.DataFrame] = []
    for portfolio_id, base_run_dir, guarded_run_dir in pairs:
        summary, events = compare_probe_pair(
            portfolio_id=portfolio_id,
            base_run_dir=base_run_dir,
            guarded_run_dir=guarded_run_dir,
        )
        summaries.append(summary)
        event_frames.append(events)

    summary_frame = pd.DataFrame(summaries)
    event_frame = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()
    if not summary_frame.empty:
        summary_frame = summary_frame.sort_values("portfolio_id").reset_index(drop=True)
    if not event_frame.empty:
        event_frame = event_frame.sort_values(
            ["portfolio_id", "comparison_status", "abs_delta_reduction"],
            ascending=[True, True, False],
            na_position="last",
        ).reset_index(drop=True)
    return summary_frame, event_frame


def build_probe_compare_report(summary_frame: pd.DataFrame, event_frame: pd.DataFrame) -> str:
    def _fmt(value: Any) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "n/a"
        return f"{float(value):.4f}"

    lines = ["# Integrated Probe Compare", ""]
    if summary_frame.empty:
        lines.extend(["No probe pairs were provided.", ""])
        return "\n".join(lines)

    improved = summary_frame["avg_stressed_improvement"].fillna(0.0) > 0
    lines.append(f"- Portfolios compared: `{int(len(summary_frame))}`")
    lines.append(f"- Portfolios improved by guarded map: `{int(improved.sum())}`")
    lines.append("")

    for row in summary_frame.to_dict(orient="records"):
        lines.append(f"## {row['portfolio_id']}")
        lines.append("")
        lines.append(f"- Baseline normal VaR 1d 99: `{row['baseline_var_1d']:.4f}`")
        lines.append(
            f"- Avg stressed VaR 1d 99: base `{_fmt(row['base_avg_stressed_normal_var_loss_1d_99'])}` "
            f"-> guarded `{_fmt(row['guarded_avg_stressed_normal_var_loss_1d_99'])}`"
        )
        lines.append(
            f"- Avg delta improvement: `{_fmt(row['avg_delta_improvement'])}` | "
            f"Avg stressed improvement: `{_fmt(row['avg_stressed_improvement'])}`"
        )
        lines.append(
            f"- Event counts: base `{row['base_event_count']}`, guarded `{row['guarded_event_count']}`, "
            f"matched `{row['matched_event_count']}`, guarded-only `{row['guarded_only_event_count']}`"
        )
        if row.get("top_reduction_event_id"):
            lines.append(
                f"- Largest damped event: `{row['top_reduction_event_type']}` "
                f"reduced abs delta by `{_fmt(row['top_reduction_abs_delta'])}` from headline: "
                f"{row['top_reduction_headline']}"
            )
        lines.append("")

    macro_guidance = event_frame.loc[
        event_frame["event_type"].isin(["macro", "guidance"]) & (event_frame["comparison_status"] == "matched")
    ]
    if not macro_guidance.empty:
        rollup = macro_guidance.groupby("event_type", as_index=False).agg(
            count=("abs_delta_reduction", "count"),
            mean=("abs_delta_reduction", "mean"),
            max=("abs_delta_reduction", "max"),
        )
        lines.append("## Macro/Guidance Rollup")
        lines.append("")
        for row in rollup.to_dict(orient="records"):
            lines.append(
                f"- `{row['event_type']}`: count `{int(row['count'])}`, "
                f"mean abs delta reduction `{_fmt(row['mean'])}`, max `{_fmt(row['max'])}`"
            )
        lines.append("")

    return "\n".join(lines)


def write_probe_compare_artifacts(
    *,
    output_dir: str | Path,
    summary_frame: pd.DataFrame,
    event_frame: pd.DataFrame,
    pair_specs: list[dict[str, str]],
) -> dict[str, str]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_csv = output_dir / "probe_compare_summary.csv"
    events_csv = output_dir / "probe_compare_events.csv"
    report_md = output_dir / "probe_compare_report.md"
    summary_json = output_dir / "probe_compare_summary.json"

    summary_frame.to_csv(summary_csv, index=False)
    event_frame.to_csv(events_csv, index=False)
    report_md.write_text(build_probe_compare_report(summary_frame, event_frame), encoding="utf-8")
    summary_json.write_text(
        json.dumps(
            {
                "pairs": pair_specs,
                "portfolio_count": int(len(summary_frame)),
                "improved_portfolio_count": int((summary_frame["avg_stressed_improvement"].fillna(0.0) > 0).sum())
                if not summary_frame.empty
                else 0,
                "summary": summary_frame.to_dict(orient="records"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "summary_csv": str(summary_csv),
        "events_csv": str(events_csv),
        "report_md": str(report_md),
        "summary_json": str(summary_json),
    }

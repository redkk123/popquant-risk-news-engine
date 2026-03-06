from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from fusion.integrated_probe_compare import compare_probe_pair, compare_probe_pairs


def _write_probe(run_dir: Path, *, portfolio_id: str, deltas: list[tuple[str, str, str, float, float]]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    summary = pd.DataFrame(
        [
            {
                "event_id": event_id,
                "event_type": event_type,
                "event_subtype": event_subtype,
                "headline": headline,
                "delta_normal_var_loss_1d_99": delta,
                "stressed_normal_var_loss_1d_99": stressed,
            }
            for event_id, event_type, event_subtype, headline, delta, stressed in deltas
        ],
        columns=[
            "event_id",
            "event_type",
            "event_subtype",
            "headline",
            "delta_normal_var_loss_1d_99",
            "stressed_normal_var_loss_1d_99",
        ],
    )
    summary.to_csv(run_dir / "integrated_summary.csv", index=False)
    report = {
        "baseline_snapshot": {
            "metadata": {"portfolio_id": portfolio_id},
            "models": {"normal_var_loss_1d_99": 0.02},
        }
    }
    (run_dir / "integrated_report.json").write_text(json.dumps(report), encoding="utf-8")


def test_compare_probe_pair_detects_dampening_and_guarded_only(tmp_path: Path) -> None:
    base_dir = tmp_path / "base"
    guarded_dir = tmp_path / "guarded"
    _write_probe(
        base_dir,
        portfolio_id="demo_book",
        deltas=[
            ("evt1", "macro", "oil_geopolitical", "Macro hit", 0.03, 0.05),
            ("evt2", "guidance", "unknown", "Guidance hit", 0.02, 0.04),
        ],
    )
    _write_probe(
        guarded_dir,
        portfolio_id="demo_book",
        deltas=[
            ("evt1", "macro", "oil_geopolitical", "Macro hit", 0.01, 0.03),
            ("evt2", "guidance", "unknown", "Guidance hit", 0.005, 0.025),
            ("evt3", "m_and_a", "unknown", "Extra deal", -0.001, 0.019),
        ],
    )

    summary, events = compare_probe_pair(
        portfolio_id="demo_book",
        base_run_dir=base_dir,
        guarded_run_dir=guarded_dir,
    )

    assert summary["matched_event_count"] == 2
    assert summary["guarded_only_event_count"] == 1
    assert summary["avg_stressed_improvement"] > 0
    assert summary["top_reduction_event_type"] == "macro"
    guarded_only = events.loc[events["comparison_status"] == "guarded_only"]
    assert len(guarded_only) == 1
    assert guarded_only.iloc[0]["event_id"] == "evt3"


def test_compare_probe_pairs_builds_sorted_summary(tmp_path: Path) -> None:
    base_a = tmp_path / "base_a"
    guarded_a = tmp_path / "guarded_a"
    base_b = tmp_path / "base_b"
    guarded_b = tmp_path / "guarded_b"

    _write_probe(
        base_a,
        portfolio_id="z_book",
        deltas=[("evt1", "macro", "unknown", "A", 0.01, 0.03)],
    )
    _write_probe(
        guarded_a,
        portfolio_id="z_book",
        deltas=[("evt1", "macro", "unknown", "A", 0.005, 0.025)],
    )
    _write_probe(
        base_b,
        portfolio_id="a_book",
        deltas=[("evt2", "earnings", "unknown", "B", -0.004, 0.016)],
    )
    _write_probe(
        guarded_b,
        portfolio_id="a_book",
        deltas=[("evt2", "earnings", "unknown", "B", -0.006, 0.014)],
    )

    summary_frame, event_frame = compare_probe_pairs(
        [
            ("z_book", base_a, guarded_a),
            ("a_book", base_b, guarded_b),
        ]
    )

    assert summary_frame["portfolio_id"].tolist() == ["a_book", "z_book"]
    assert set(event_frame["portfolio_id"].tolist()) == {"a_book", "z_book"}

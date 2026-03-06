from __future__ import annotations

from operations.retention import RetentionPolicy, list_prunable_runs


def test_list_prunable_runs_keeps_latest_and_protected(tmp_path) -> None:
    keep_1 = tmp_path / "20260306T000000Z"
    keep_2 = tmp_path / "20260306T010000Z"
    old_prunable = tmp_path / "20260220T000000Z"
    old_protected = tmp_path / "20260221T000000Z"
    for path in (keep_1, keep_2, old_prunable, old_protected):
        path.mkdir()
    (old_protected / "live_validation_governance.json").write_text("{}", encoding="utf-8")

    prunable = list_prunable_runs(
        tmp_path,
        policy=RetentionPolicy(keep_latest=2, min_age_days=7),
    )

    assert old_prunable in prunable
    assert old_protected not in prunable
    assert keep_1 not in prunable
    assert keep_2 not in prunable

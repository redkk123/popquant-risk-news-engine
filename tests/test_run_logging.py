from __future__ import annotations

import json

from event_engine.run_logging import append_run_event, write_failure_manifest


def test_append_run_event_writes_jsonl_record(tmp_path) -> None:
    log_path = tmp_path / "run_log.jsonl"
    append_run_event(
        log_path,
        stage="sync",
        status="success",
        message="completed",
        details={"inserted": 3},
    )

    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["stage"] == "sync"
    assert payload["status"] == "success"
    assert payload["details"]["inserted"] == 3


def test_write_failure_manifest_captures_stage_and_error(tmp_path) -> None:
    try:
        raise ValueError("bad token")
    except ValueError as exc:
        manifest_path = write_failure_manifest(
            output_root=tmp_path,
            stage="sync",
            error=exc,
            log_path=tmp_path / "run_log.jsonl",
        )

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["stage"] == "sync"
    assert payload["error_type"] == "ValueError"
    assert payload["error_message"] == "bad token"


def test_logging_redacts_marketaux_token_patterns(tmp_path) -> None:
    log_path = tmp_path / "run_log.jsonl"
    append_run_event(
        log_path,
        stage="sync",
        status="error",
        message="request failed: https://api.marketaux.com/v1/news/all?api_token=secret123&limit=3",
        details={"url": "https://api.marketaux.com/v1/news/all?api_token=secret123&limit=3"},
    )

    payload = json.loads(log_path.read_text(encoding="utf-8").strip())
    assert "secret123" not in payload["message"]
    assert "<redacted>" in payload["message"]
    assert "secret123" not in payload["details"]["url"]


def test_failure_manifest_redacts_marketaux_token_patterns(tmp_path) -> None:
    try:
        raise RuntimeError("402 from https://api.marketaux.com/v1/news/all?api_token=secret123&limit=3")
    except RuntimeError as exc:
        manifest_path = write_failure_manifest(
            output_root=tmp_path,
            stage="sync",
            error=exc,
            log_path=tmp_path / "run_log.jsonl",
        )

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "secret123" not in payload["error_message"]
    assert "<redacted>" in payload["error_message"]

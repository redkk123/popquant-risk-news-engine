from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from event_engine.redaction import redact_text, redact_value


def _json_default(value: Any):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value)} is not JSON serializable")


def append_run_event(
    log_path: str | Path,
    *,
    stage: str,
    status: str,
    message: str | None = None,
    details: dict[str, Any] | None = None,
) -> Path:
    """Append a JSONL log event for an operational run."""
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "status": status,
        "message": redact_text(message),
        "details": redact_value(details or {}),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, default=_json_default) + "\n")
    return path


def write_failure_manifest(
    *,
    output_root: str | Path,
    stage: str,
    error: Exception,
    log_path: str | Path | None = None,
) -> Path:
    """Persist a compact failure manifest for easier debugging."""
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "failure_manifest.json"
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "error_type": type(error).__name__,
        "error_message": redact_text(str(error)),
        "traceback": redact_text(traceback.format_exc()),
        "run_log": str(log_path) if log_path else None,
    }
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return manifest_path

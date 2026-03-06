from __future__ import annotations

from pathlib import Path
from typing import Sequence


DEFAULT_TASK_NAME = "PopQuantLiveWatchlist"
DEFAULT_TASK_TIME = "08:30"


def normalize_task_time(value: str) -> str:
    """Validate and normalize a local scheduler time in HH:MM 24h format."""
    text = str(value).strip()
    parts = text.split(":")
    if len(parts) != 2:
        raise ValueError("Task time must be in HH:MM 24h format.")
    hour, minute = parts
    if not (hour.isdigit() and minute.isdigit()):
        raise ValueError("Task time must be numeric HH:MM.")
    hour_value = int(hour)
    minute_value = int(minute)
    if hour_value < 0 or hour_value > 23 or minute_value < 0 or minute_value > 59:
        raise ValueError("Task time must be within 00:00 and 23:59.")
    return f"{hour_value:02d}:{minute_value:02d}"


def _quote_for_windows(arg: str) -> str:
    text = str(arg)
    if not text or any(char in text for char in (' ', '"', "&", "(", ")", "[", "]", "{", "}", ";", ",")):
        escaped = text.replace('"', '\\"')
        return f'"{escaped}"'
    return text


def build_task_runner_command(
    *,
    project_root: str | Path,
    python_launcher: str = "py",
    lookback_days: int = 3,
    symbols: Sequence[str] = ("AAPL", "MSFT", "SPY"),
    limit: int = 3,
    max_pages: int = 2,
    language: str = "en",
    watchlist_config: str | Path | None = None,
    event_map_config: str | Path | None = None,
    cache_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> str:
    """Build the command executed by Windows Task Scheduler."""
    root = Path(project_root)
    runner_path = root / "scripts" / "run_live_watchlist_task.ps1"
    command_parts = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(runner_path),
        "-ProjectRoot",
        str(root),
        "-PythonLauncher",
        python_launcher,
        "-LookbackDays",
        str(int(lookback_days)),
        "-Language",
        language,
        "-Limit",
        str(int(limit)),
        "-MaxPages",
        str(int(max_pages)),
    ]

    if watchlist_config:
        command_parts.extend(["-WatchlistConfig", str(watchlist_config)])
    if event_map_config:
        command_parts.extend(["-EventMapConfig", str(event_map_config)])
    if cache_dir:
        command_parts.extend(["-CacheDir", str(cache_dir)])
    if output_dir:
        command_parts.extend(["-OutputDir", str(output_dir)])
    if symbols:
        command_parts.append("-Symbols")
        command_parts.extend(str(symbol).upper() for symbol in symbols)

    return " ".join(_quote_for_windows(part) for part in command_parts)


def build_schtasks_create_args(
    *,
    task_name: str,
    task_time: str,
    task_command: str,
) -> list[str]:
    """Build `schtasks /Create` arguments for a daily task."""
    normalized_time = normalize_task_time(task_time)
    return [
        "schtasks",
        "/Create",
        "/SC",
        "DAILY",
        "/TN",
        str(task_name),
        "/TR",
        task_command,
        "/ST",
        normalized_time,
        "/F",
    ]


def build_schtasks_delete_args(*, task_name: str) -> list[str]:
    """Build `schtasks /Delete` arguments."""
    return ["schtasks", "/Delete", "/TN", str(task_name), "/F"]


def build_schtasks_query_args(*, task_name: str) -> list[str]:
    """Build `schtasks /Query` arguments."""
    return ["schtasks", "/Query", "/TN", str(task_name), "/V", "/FO", "LIST"]

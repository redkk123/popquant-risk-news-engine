from __future__ import annotations

from operations.scheduler import (
    build_schtasks_create_args,
    build_task_runner_command,
    normalize_task_time,
)


def test_normalize_task_time_zero_pads_values() -> None:
    assert normalize_task_time("8:5") == "08:05"


def test_build_task_runner_command_includes_wrapper_and_symbols() -> None:
    command = build_task_runner_command(
        project_root="D:/Playground/popquant_1_month",
        symbols=["AAPL", "MSFT"],
        lookback_days=4,
    )

    assert "run_live_watchlist_task.ps1" in command
    assert "-LookbackDays 4" in command
    assert "-Symbols AAPL MSFT" in command


def test_build_schtasks_create_args_uses_daily_schedule() -> None:
    args = build_schtasks_create_args(
        task_name="PopQuantLiveWatchlist",
        task_time="08:30",
        task_command="powershell.exe -File test.ps1",
    )

    assert args[:4] == ["schtasks", "/Create", "/SC", "DAILY"]
    assert "/TR" in args
    assert "/ST" in args

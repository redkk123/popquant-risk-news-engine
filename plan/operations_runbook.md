# Operations Runbook

## Goal

Run the live `Marketaux -> NLP -> risk -> watchlist` flow reliably by hand or on a Windows schedule.

## Manual Live Run

```powershell
$env:MARKETAUX_API_TOKEN=[Environment]::GetEnvironmentVariable("MARKETAUX_API_TOKEN","User")
py D:\Playground\popquant_1_month\scripts\run_live_marketaux_watchlist.py
```

Outputs land in:

- `D:\Playground\popquant_1_month\output\live_marketaux_watchlist\<run_id>`

Key files:

- `live_marketaux_manifest.json`
- `run_log.jsonl`
- `watchlist_report.md`
- `live_event_audit_summary.json`

## Manual Scheduled-Task Wrapper Run

```powershell
$env:MARKETAUX_API_TOKEN=[Environment]::GetEnvironmentVariable("MARKETAUX_API_TOKEN","User")
powershell -NoProfile -ExecutionPolicy Bypass -File D:\Playground\popquant_1_month\scripts\run_live_watchlist_task.ps1 -ProjectRoot D:\Playground\popquant_1_month
```

Wrapper log location:

- `D:\Playground\popquant_1_month\output\scheduled_task_logs`

## Preview The Windows Task

```powershell
py D:\Playground\popquant_1_month\scripts\manage_live_watchlist_task.py create --print-only
```

## Create The Windows Task

Default task:

- name: `PopQuantLiveWatchlist`
- schedule: daily at `08:30` local time

```powershell
py D:\Playground\popquant_1_month\scripts\manage_live_watchlist_task.py create
```

Custom schedule:

```powershell
py D:\Playground\popquant_1_month\scripts\manage_live_watchlist_task.py create --time 18:15 --symbols AAPL MSFT SPY NVDA --lookback-days 3
```

## Inspect The Windows Task

```powershell
py D:\Playground\popquant_1_month\scripts\manage_live_watchlist_task.py show
```

## Delete The Windows Task

```powershell
py D:\Playground\popquant_1_month\scripts\manage_live_watchlist_task.py delete
```

## Failure Handling

If a run fails, inspect:

1. `failure_manifest.json` in the run output folder
2. `run_log.jsonl` in the same folder
3. wrapper logs in `output\scheduled_task_logs`

## Expected Healthy Run

Healthy live runs should produce:

1. a non-empty `watchlist_summary.csv`
2. a `live_event_audit_summary.json` with low suspicious-link count
3. a `run_log.jsonl` ending with `report/success`

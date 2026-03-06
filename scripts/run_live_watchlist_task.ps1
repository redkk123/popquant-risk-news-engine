param(
    [string]$ProjectRoot = "D:\Playground\popquant_1_month",
    [string]$PythonLauncher = "py",
    [int]$LookbackDays = 3,
    [string[]]$Symbols = @("AAPL", "MSFT", "SPY"),
    [string]$Language = "en",
    [int]$Limit = 3,
    [int]$MaxPages = 2,
    [string]$WatchlistConfig = "",
    [string]$EventMapConfig = "",
    [string]$CacheDir = "",
    [string]$OutputDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectPath = [System.IO.Path]::GetFullPath($ProjectRoot)
$scriptPath = Join-Path $projectPath "scripts\run_live_marketaux_watchlist.py"
$logDir = Join-Path $projectPath "output\scheduled_task_logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$timestamp = [DateTimeOffset]::UtcNow.ToString("yyyyMMddTHHmmssZ")
$logPath = Join-Path $logDir "scheduled_live_watchlist_$timestamp.log"

$publishedAfter = [DateTimeOffset]::UtcNow.AddDays(-1 * $LookbackDays).ToString("yyyy-MM-dd")
$publishedBefore = [DateTimeOffset]::UtcNow.AddDays(1).ToString("yyyy-MM-dd")

$arguments = @(
    $scriptPath,
    "--published-after", $publishedAfter,
    "--published-before", $publishedBefore,
    "--language", $Language,
    "--limit", $Limit.ToString(),
    "--max-pages", $MaxPages.ToString()
)

if ($WatchlistConfig) {
    $arguments += @("--watchlist-config", $WatchlistConfig)
}
if ($EventMapConfig) {
    $arguments += @("--event-map-config", $EventMapConfig)
}
if ($CacheDir) {
    $arguments += @("--cache-dir", $CacheDir)
}
if ($OutputDir) {
    $arguments += @("--output-dir", $OutputDir)
}
if ($Symbols -and $Symbols.Count -gt 0) {
    $arguments += "--symbols"
    $arguments += $Symbols
}

Push-Location $projectPath
try {
    & $PythonLauncher @arguments 2>&1 | Tee-Object -FilePath $logPath
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}

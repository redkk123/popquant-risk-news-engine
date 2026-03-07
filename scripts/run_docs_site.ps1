param(
    [string]$HostAddress = "127.0.0.1",
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

try {
    py -c "import mkdocs, mkdocs_material" | Out-Null
}
catch {
    Write-Host "[docs] Installing MkDocs dependencies..."
    py -m pip install -r "$projectRoot\requirements-docs.txt"
}

Write-Host "[docs] Starting local docs site..."
Write-Host "[docs] URL: http://$HostAddress`:$Port"
py -m mkdocs serve --dev-addr "$HostAddress`:$Port"

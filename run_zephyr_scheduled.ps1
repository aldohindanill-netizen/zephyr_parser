# Scheduled wrapper for run_zephyr.ps1 (Task Scheduler / manual).
# Logs to reports\logs\scheduled_YYYY-MM-DD.log; skips if previous run still active.

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogDir = Join-Path $RepoRoot "reports\logs"
$LockFile = Join-Path $RepoRoot "reports\.zephyr_scheduled.lock"
$Runner = Join-Path $RepoRoot "run_zephyr.ps1"
$LogFile = Join-Path $LogDir ("scheduled_{0:yyyy-MM-dd}.log" -f (Get-Date))

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] $Message"
    Add-Content -LiteralPath $LogFile -Value $line -Encoding UTF8
    Write-Host $line
}

if (-not (Test-Path -LiteralPath $Runner)) {
    Write-Log "ERROR: run_zephyr.ps1 not found at $Runner"
    exit 1
}

$lock = $null
try {
    $lock = [System.IO.File]::Open(
        $LockFile,
        [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::None
    )
}
catch [System.IO.IOException] {
    Write-Log "SKIP: previous run still active (lock: $LockFile)"
    exit 0
}

try {
    Write-Log "START cwd=$RepoRoot"
    Set-Location -LiteralPath $RepoRoot

    & $Runner *>&1 | ForEach-Object { Write-Log $_.ToString() }
    $code = if ($null -ne $LASTEXITCODE) { $LASTEXITCODE } else { 0 }

    Write-Log "END exit=$code"
    exit $code
}
finally {
    if ($null -ne $lock) {
        $lock.Dispose()
    }
    if (Test-Path -LiteralPath $LockFile) {
        Remove-Item -LiteralPath $LockFile -Force -ErrorAction SilentlyContinue
    }
}

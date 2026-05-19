# Scheduled wrapper for run_zephyr.ps1 (Task Scheduler / manual).
# Logs to reports\logs\scheduled_YYYY-MM-DD.log; skips if previous run still active.
# Runs run_zephyr.ps1 in a child PowerShell process (not Start-Job) so Python is not
# killed prematurely. Full stdout/stderr: logs\zephyr_*.log

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogDir = Join-Path $RepoRoot "reports\logs"
$LockFile = Join-Path $RepoRoot "reports\.zephyr_scheduled.lock"
$Runner = Join-Path $RepoRoot "run_zephyr.ps1"
$EnvFile = Join-Path $RepoRoot ".env"
$LogFile = Join-Path $LogDir ("scheduled_{0:yyyy-MM-dd}.log" -f (Get-Date))

function Import-RepoDotEnv {
    if (-not (Test-Path -LiteralPath $EnvFile)) { return }
    Get-Content -LiteralPath $EnvFile | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith('#') -or -not $line.Contains('=')) { return }
        $parts = $line -split '=', 2
        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if (($value.StartsWith("'") -and $value.EndsWith("'")) -or
            ($value.StartsWith('"') -and $value.EndsWith('"'))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}

function Get-RunTimeoutMinutes {
    $raw = $env:ZEPHYR_RUN_TIMEOUT_MINUTES
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return 90
    }
    $minutes = 0
    if (-not [int]::TryParse($raw.Trim(), [ref]$minutes) -or $minutes -lt 1) {
        return 90
    }
    return $minutes
}

Import-RepoDotEnv

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

$exitCode = 1
try {
    $timeoutMinutes = Get-RunTimeoutMinutes
    Write-Log "START cwd=$RepoRoot timeout=${timeoutMinutes}m (details: logs\zephyr_*.log)"
    Set-Location -LiteralPath $RepoRoot

    $powershell = (Get-Command powershell.exe).Source
    $arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`""
    $timeoutMs = $timeoutMinutes * 60 * 1000

    $proc = Start-Process -FilePath $powershell `
        -ArgumentList $arguments `
        -WorkingDirectory $RepoRoot `
        -PassThru -NoNewWindow

    $completed = $proc.WaitForExit($timeoutMs)
    if (-not $completed) {
        & taskkill.exe /PID $proc.Id /T /F 2>$null | Out-Null
        Write-Log "TIMEOUT: run exceeded ${timeoutMinutes} minute(s); stopped process tree pid=$($proc.Id)"
        $exitCode = 124
    }
    else {
        $proc.Refresh()
        $exitCode = if ($null -ne $proc.ExitCode) { $proc.ExitCode } else { 0 }
    }

    Write-Log "END exit=$exitCode"
}
finally {
    if ($null -ne $lock) {
        $lock.Dispose()
    }
    if (Test-Path -LiteralPath $LockFile) {
        Remove-Item -LiteralPath $LockFile -Force -ErrorAction SilentlyContinue
    }
}

exit $exitCode

# Local launcher for universal report web UI.
# Loads .env, .env.secrets, then .env.local (same as run_zephyr_local.ps1).

$ErrorActionPreference = "Stop"

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
[Console]::InputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

function Import-RepoDotEnv {
    param([Parameter(Mandatory = $true)][string]$Path)

    Get-Content -LiteralPath $Path -Encoding utf8 | ForEach-Object {
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

function Test-EnvEnabled {
    param(
        [string]$Value,
        [bool]$Default = $false
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $Default
    }
    return @("1", "true", "yes", "y", "on") -contains $Value.Trim().ToLowerInvariant()
}

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$envPath = Join-Path $RepoRoot ".env"
$envSecretsPath = Join-Path $RepoRoot ".env.secrets"
$envLocalPath = Join-Path $RepoRoot ".env.local"
$envLocalExamplePath = Join-Path $RepoRoot ".env.local.example"
$reqPath = Join-Path $RepoRoot "requirements-universal.txt"

if (-not (Test-Path -LiteralPath $envPath)) {
    Write-Host "Missing .env — copy from .env.example first." -ForegroundColor Yellow
    exit 1
}

Import-RepoDotEnv -Path $envPath
if (Test-Path -LiteralPath $envSecretsPath) {
    Import-RepoDotEnv -Path $envSecretsPath
}
else {
    Write-Host "Missing .env.secrets — copy from .env.secrets.example first." -ForegroundColor Yellow
    exit 1
}

if (Test-Path -LiteralPath $envLocalPath) {
    Import-RepoDotEnv -Path $envLocalPath
    Write-Host "Loaded local overrides: $envLocalPath"
}
else {
    Write-Host @"

Missing .env.local for local sandbox paths.
Copy the template:
  Copy-Item -LiteralPath '$envLocalExamplePath' -Destination '$envLocalPath'
"@ -ForegroundColor Yellow
    exit 1
}

$env:ZEPHYR_USE_LOCAL_ENV = "true"
$env:PYTHONUNBUFFERED = "1"

if (-not $env:ZEPHYR_UNIVERSAL_DRAFTS_DIR) {
    $env:ZEPHYR_UNIVERSAL_DRAFTS_DIR = Join-Path $RepoRoot "reports_local\universal_drafts"
}
if (-not $env:ZEPHYR_UNIVERSAL_READABLE_DIR) {
    $env:ZEPHYR_UNIVERSAL_READABLE_DIR = Join-Path $RepoRoot "reports_local\universal_readable"
}
if (-not $env:ZEPHYR_UNIVERSAL_PORT) {
    $env:ZEPHYR_UNIVERSAL_PORT = "8765"
}

function Get-UniversalPythonLabel {
    if ($env:PYTHON_BIN -and (Test-Path -LiteralPath $env:PYTHON_BIN)) {
        return "PYTHON_BIN=$($env:PYTHON_BIN)"
    }
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return "py -3"
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return "python"
    }
    return "python (not found)"
}

function Invoke-UniversalPython {
    param([string[]]$PythonArgs)

    if ($env:PYTHON_BIN -and (Test-Path -LiteralPath $env:PYTHON_BIN)) {
        & $env:PYTHON_BIN @PythonArgs
    }
    elseif (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3 @PythonArgs
    }
    elseif (Get-Command python -ErrorAction SilentlyContinue) {
        & python @PythonArgs
    }
    else {
        throw "Python not found. Install Python 3.10+ (py launcher or python on PATH), or set PYTHON_BIN to python.exe."
    }
    return $LASTEXITCODE
}

function Stop-ListenerOnPort {
    param([int]$Port)

    $matches = netstat -ano | Select-String "LISTENING" | Select-String ":$Port\s"
    foreach ($line in $matches) {
        $parts = ($line.ToString().Trim() -split '\s+')
        $procId = [int]$parts[-1]
        if ($procId -le 0) { continue }
        try {
            $proc = Get-Process -Id $procId -ErrorAction Stop
            Write-Host "Stopping previous server on port $Port (PID $procId, $($proc.ProcessName))..."
            Stop-Process -Id $procId -Force -ErrorAction Stop
        }
        catch {
            Write-Host "WARN: could not stop PID $procId on port ${Port}: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
    if ($matches) {
        Start-Sleep -Seconds 1
    }
}

Write-Host "Python: $(Get-UniversalPythonLabel)"
$pipExit = Invoke-UniversalPython -PythonArgs @("-m", "pip", "install", "-r", $reqPath, "--quiet")
if ($pipExit -ne 0) {
    throw "pip install -r requirements-universal.txt failed (exit $pipExit)"
}

$port = [int]$env:ZEPHYR_UNIVERSAL_PORT
Stop-ListenerOnPort -Port $port
$url = "http://127.0.0.1:$port"
Write-Host "Starting universal report UI at $url"
Start-Process $url | Out-Null

Push-Location $RepoRoot
try {
    $exitCode = Invoke-UniversalPython -PythonArgs @("-m", "universal_report") + $args
    if ($null -eq $exitCode) { $exitCode = 1 }
    exit $exitCode
}
finally {
    Pop-Location
}

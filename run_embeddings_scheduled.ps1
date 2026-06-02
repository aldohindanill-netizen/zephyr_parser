# Nightly bug-duplicate embeddings (Task Scheduler / manual).
param([switch]$UseLocalEnv)
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$EnvFile = Join-Path $RepoRoot ".env"
$EnvSecretsFile = Join-Path $RepoRoot ".env.secrets"
$EnvLocalFile = Join-Path $RepoRoot ".env.local"
$VenvDir = Join-Path $RepoRoot ".venv-embeddings"
$Requirements = Join-Path $RepoRoot "requirements-embeddings.txt"
function Import-RepoDotEnv {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return }
    Get-Content -LiteralPath $Path -Encoding utf8 | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith('#') -or -not $line.Contains('=')) { return }
        $parts = $line -split '=', 2
        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if (($value.StartsWith("'") -and $value.EndsWith("'")) -or ($value.StartsWith('"') -and $value.EndsWith('"'))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}
function Test-EnvEnabled { param([string]$Value, [bool]$Default = $false)
    if ([string]::IsNullOrWhiteSpace($Value)) { return $Default }
    return @("1","true","yes","y","on") -contains $Value.Trim().ToLowerInvariant()
}
function Resolve-RepoRelativePath { param([string]$Relative)
    $p = $Relative.Trim()
    if ([System.IO.Path]::IsPathRooted($p)) { return $p }
    return Join-Path $RepoRoot $p
}
function Get-ReportsLogsDir {
    $rollup = if ($env:ZEPHYR_BUGS_ROLLUP_DIR) { $env:ZEPHYR_BUGS_ROLLUP_DIR.Trim() } else { "reports/bugs_rollup" }
    return Join-Path (Split-Path -Parent (Resolve-RepoRelativePath $rollup)) "logs"
}
function Write-Log { param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] $Message"
    Add-Content -LiteralPath $script:LogFile -Value $line -Encoding UTF8
    Write-Host $line
}
function Get-SystemPythonExe {
    if ($env:PYTHON_BIN -and (Test-Path -LiteralPath $env:PYTHON_BIN)) { return $env:PYTHON_BIN }
    if (Get-Command py -ErrorAction SilentlyContinue) { return "py" }
    return "python"
}
function Invoke-RepoPython { param([string]$Code)
    $exe = Get-SystemPythonExe
    Push-Location -LiteralPath $RepoRoot
    try {
        if ($exe -eq "py") { & py -3 -c $Code } else { & $exe -c $Code }
        if ($LASTEXITCODE -ne 0) { throw "Python exit $LASTEXITCODE" }
    } finally { Pop-Location }
}
function Write-EmbeddingsAuditStart { try { Invoke-RepoPython "import zephyr_audit as za; za.audit_embeddings_start()" } catch { Write-Log "WARN: embeddings audit start failed: $($_.Exception.Message)" } }
function Write-EmbeddingsAuditFinish { param([int]$ExitCode)
    try { Invoke-RepoPython "import zephyr_audit as za; za.audit_embeddings_finish($ExitCode)" } catch { Write-Log "WARN: embeddings audit finish failed: $($_.Exception.Message)" }
}
function Update-PipelineHealthHtml { try { Invoke-RepoPython "import zephyr_pipeline_health as ph; ph.write_pipeline_health_html()"; Write-Log "Updated pipeline_health.html" } catch { Write-Log "WARN: pipeline health HTML failed: $($_.Exception.Message)" } }
function Get-EmbeddingsVenvLauncher {
    if ($env:PYTHON_BIN -and (Test-Path -LiteralPath $env:PYTHON_BIN)) { return $env:PYTHON_BIN }
    if (Get-Command py -ErrorAction SilentlyContinue) {
        foreach ($tag in @("3.12","3.11","3.10")) { & py -$tag -c "import sys" 2>$null | Out-Null; if ($LASTEXITCODE -eq 0) { return "py -$tag" } }
        return "py -3"
    }
    return "python"
}
function Invoke-VenvLauncher { param([string]$Launcher, [string[]]$LauncherArgs)
    if ($Launcher -match '^py -(\d+\.\d+)$') { & py "-$($Matches[1])" @LauncherArgs }
    elseif ($Launcher -eq "py -3") { & py -3 @LauncherArgs }
    else { & $Launcher @LauncherArgs }
}
function Test-VenvPythonVersionSupported { param([string]$VenvPython)
    $minor = & $VenvPython -c "import sys; print(sys.version_info.minor)" 2>$null
    if (-not $minor) { return $true }
    return [int]$minor -lt 13
}
function Test-TorchLoads { param([string]$VenvPython)
    $savedEap = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    try {
        & $VenvPython -c "import warnings; warnings.filterwarnings('ignore'); import torch" 2>&1 | Out-Null
        return $LASTEXITCODE -eq 0
    } finally {
        $ErrorActionPreference = $savedEap
    }
}
function Install-WindowsCpuTorch { param([string]$VenvPython)
    if (Test-TorchLoads -VenvPython $VenvPython) { Write-Log "torch already importable; skip pip install"; return }
    Write-Log "pip install torch (CPU wheel) for Windows"
    $savedEap = $ErrorActionPreference; $ErrorActionPreference = "Continue"
    try {
        & $VenvPython -m pip install -q torch --index-url https://download.pytorch.org/whl/cpu 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "pip install torch (CPU) failed with exit $LASTEXITCODE" }
    } finally { $ErrorActionPreference = $savedEap }
}
function Assert-TorchLoads { param([string]$VenvPython)
    if (-not (Test-TorchLoads -VenvPython $VenvPython)) {
        throw "torch failed to import; install VC++ 2015-2022 x64 or recreate .venv-embeddings with py -3.12"
    }
}
if (-not (Test-Path -LiteralPath $EnvFile)) { throw "Missing $EnvFile" }
Import-RepoDotEnv -Path $EnvFile
if (-not (Test-Path -LiteralPath $EnvSecretsFile)) { throw "Missing $EnvSecretsFile" }
Import-RepoDotEnv -Path $EnvSecretsFile
$useLocalOverlay = $UseLocalEnv.IsPresent -or (Test-EnvEnabled $env:ZEPHYR_USE_LOCAL_ENV $false)
if ($useLocalOverlay) {
    if (Test-Path -LiteralPath $EnvLocalFile) { Import-RepoDotEnv -Path $EnvLocalFile; Write-Host "Loaded local overrides: $EnvLocalFile" }
    elseif ($UseLocalEnv.IsPresent) { throw "-UseLocalEnv requested but missing: $EnvLocalFile" }
}
$LogDir = Get-ReportsLogsDir
$script:LogFile = Join-Path $LogDir ("embeddings_{0:yyyy-MM-dd}.log" -f (Get-Date))
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location -LiteralPath $RepoRoot
if (-not $env:ZEPHYR_AUDIT_RUN_ID) { [Environment]::SetEnvironmentVariable("ZEPHYR_AUDIT_RUN_ID", [guid]::NewGuid().ToString(), "Process") }
$rollupDir = if ($env:ZEPHYR_BUGS_ROLLUP_DIR) { $env:ZEPHYR_BUGS_ROLLUP_DIR.Trim() } else { "reports/bugs_rollup" }
$exitCode = 1
Write-EmbeddingsAuditStart
try {
    Write-Log "START rollup=$rollupDir"
    $venvPy = Join-Path $VenvDir "Scripts\python.exe"
    if ((Test-Path -LiteralPath $VenvDir) -and (Test-Path -LiteralPath $venvPy)) {
        if (-not (Test-VenvPythonVersionSupported -VenvPython $venvPy)) {
            Write-Log "Removing .venv-embeddings (Python 3.13+ unreliable with torch on Windows)"
            Remove-Item -LiteralPath $VenvDir -Recurse -Force
        }
    }
    if (-not (Test-Path -LiteralPath $VenvDir)) {
        $basePy = Get-EmbeddingsVenvLauncher
        Write-Log "Creating venv at $VenvDir (base: $basePy)"
        Invoke-VenvLauncher -Launcher $basePy -LauncherArgs @("-m", "venv", $VenvDir)
        if ($LASTEXITCODE -ne 0) { throw "venv creation failed with exit $LASTEXITCODE" }
    }
    if (-not (Test-Path -LiteralPath $venvPy)) { throw "Missing venv python: $venvPy" }
    if ($env:OS -match 'Windows') { Install-WindowsCpuTorch -VenvPython $venvPy; Assert-TorchLoads -VenvPython $venvPy }
    if (Test-Path -LiteralPath $Requirements) {
        Write-Log "pip install -r requirements-embeddings.txt"
        $savedEap = $ErrorActionPreference; $ErrorActionPreference = "Continue"
        try { & $venvPy -m pip install -q -r $Requirements 2>&1 | Out-Null; if ($LASTEXITCODE -ne 0) { throw "pip install failed with exit $LASTEXITCODE" } }
        finally { $ErrorActionPreference = $savedEap }
    }
    Write-Log "compute_bug_embeddings.py"
    & $venvPy (Join-Path $RepoRoot "scripts\compute_bug_embeddings.py") --from-rollup-dir $rollupDir
    if ($LASTEXITCODE -ne 0) { throw "compute_bug_embeddings exit $LASTEXITCODE" }
    Write-Log "refresh_bugs_rollup_duplicates.py"
    & $venvPy (Join-Path $RepoRoot "scripts\refresh_bugs_rollup_duplicates.py")
    if ($LASTEXITCODE -ne 0) { throw "refresh_bugs_rollup_duplicates exit $LASTEXITCODE" }
    $exitCode = 0
    Write-Log "END exit=0"
} catch {
    $detail = $_.Exception.Message; if (-not $detail) { $detail = $_.ToString() }
    Write-Log "ERROR: $detail"; $exitCode = 1; Write-Log "END exit=$exitCode"
} finally {
    Write-EmbeddingsAuditFinish -ExitCode $exitCode
    Update-PipelineHealthHtml
}
exit $exitCode

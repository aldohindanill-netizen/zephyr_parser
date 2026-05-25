# Create or update a separate local dev clone (sibling folder by default).
# Production Task Scheduler must stay on the original repo path only.
#
# Usage (from production repo):
#   .\setup_dev_clone.ps1
#   .\setup_dev_clone.ps1 -TargetPath D:\work\zephyr_parser_dev
#   .\setup_dev_clone.ps1 -Branch feature/my-change -CopyEnv
#   .\setup_dev_clone.ps1 -RemoteUrl https://github.com/YOU/zephyr_parser_dev.git

param(
    [string]$TargetPath = "",
    [string]$Branch = "",
    [string]$RemoteUrl = "",
    [switch]$CopyEnv,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$ProdRoot = (Resolve-Path (Split-Path -Parent $MyInvocation.MyCommand.Path)).Path
$ParentDir = Split-Path -Parent $ProdRoot

if ([string]::IsNullOrWhiteSpace($TargetPath)) {
    $TargetPath = Join-Path $ParentDir "zephyr_parser_dev"
}
$TargetPath = [System.IO.Path]::GetFullPath($TargetPath)

function Get-ProdOriginUrl {
    Push-Location $ProdRoot
    try {
        return (git remote get-url origin 2>$null)
    }
    finally {
        Pop-Location
    }
}

$SyncFromProductionFiles = @(
    "run_zephyr.ps1",
    "run_zephyr_local.ps1",
    "setup_dev_clone.ps1",
    ".env.local.example",
    ".gitignore",
    "README.md"
)

$cloneUrl = if ($RemoteUrl) { $RemoteUrl.Trim() } else { Get-ProdOriginUrl }
if (-not $cloneUrl) {
    throw "Cannot resolve git remote. Pass -RemoteUrl explicitly."
}

Write-Host "Production repo: $ProdRoot"
Write-Host "Dev clone path:  $TargetPath"
Write-Host "Clone URL:       $cloneUrl"

if ((Test-Path -LiteralPath $TargetPath) -and -not (Test-Path -LiteralPath (Join-Path $TargetPath ".git"))) {
    if ($Force) {
        Remove-Item -LiteralPath $TargetPath -Recurse -Force
    }
    else {
        throw "Target exists but is not a git repo: $TargetPath (use -Force to remove)"
    }
}

if (Test-Path -LiteralPath (Join-Path $TargetPath ".git")) {
    Write-Host "Updating existing dev clone..."
    Push-Location $TargetPath
    try {
        git fetch origin
        if ($Branch) {
            git checkout $Branch
        }
        $tracking = "origin/main"
        if ($Branch) {
            git fetch origin $Branch 2>$null
            $tracking = "origin/$Branch"
        }
        git reset --hard $tracking
    }
    finally {
        Pop-Location
    }
}
else {
    Write-Host "Cloning..."
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $TargetPath) | Out-Null
    git clone $cloneUrl $TargetPath
    Push-Location $TargetPath
    try {
        if ($Branch) {
            git checkout $Branch
        }
    }
    finally {
        Pop-Location
    }
}

$envPath = Join-Path $TargetPath ".env"
$envExamplePath = Join-Path $TargetPath ".env.example"
$envLocalExamplePath = Join-Path $TargetPath ".env.local.example"
$envLocalPath = Join-Path $TargetPath ".env.local"
$prodEnvPath = Join-Path $ProdRoot ".env"

if ($CopyEnv -and (Test-Path -LiteralPath $prodEnvPath)) {
    Write-Host "Copying secrets from production .env"
    Copy-Item -LiteralPath $prodEnvPath -Destination $envPath -Force
}
elseif (-not (Test-Path -LiteralPath $envPath)) {
    if (-not (Test-Path -LiteralPath $envExamplePath)) {
        throw "Missing $envExamplePath in dev clone"
    }
    Copy-Item -LiteralPath $envExamplePath -Destination $envPath
    Write-Host "Created .env from .env.example (fill ZEPHYR_API_TOKEN)"
}

if (Test-Path -LiteralPath $envLocalExamplePath) {
    if (-not (Test-Path -LiteralPath $envLocalPath)) {
        Copy-Item -LiteralPath $envLocalExamplePath -Destination $envLocalPath
        Write-Host "Created .env.local from template (set sandbox Confluence page id)"
    }
}

foreach ($rel in $SyncFromProductionFiles) {
    $src = Join-Path $ProdRoot $rel
    if (Test-Path -LiteralPath $src) {
        Copy-Item -LiteralPath $src -Destination (Join-Path $TargetPath $rel) -Force
    }
}
Write-Host "Synced dev-isolation files from production working tree"

$devCloneDoc = @"
# Dev clone

This directory is a **local development clone**. It is isolated from the production pipeline.

| | Production | This dev clone |
|---|------------|----------------|
| Path | ``$ProdRoot`` | ``$TargetPath`` |
| Task Scheduler | yes (do not register here) | **no** |
| Reports | ``reports/`` | ``reports_local/`` (via ``.env.local``) |
| Typical run | ``run_zephyr_scheduled.ps1`` | ``.\run_zephyr_local.ps1`` |

## Commands

``````powershell
cd "$TargetPath"
.\run_zephyr_local.ps1
.\run_zephyr_local.ps1 --regenerate-last-n-days 1
``````

Branches: checkout any branch here; production folder is unaffected until you merge and pull there.

**Do not run** ``install_zephyr_scheduled_task.ps1`` in this clone.
"@

Set-Content -LiteralPath (Join-Path $TargetPath "DEV_CLONE.md") -Value $devCloneDoc -Encoding UTF8

Write-Host ""
Write-Host "Done. Dev clone ready at:"
Write-Host "  $TargetPath"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  cd `"$TargetPath`""
if (-not (Test-Path -LiteralPath $prodEnvPath) -or -not $CopyEnv) {
    Write-Host "  # Ensure .env has ZEPHYR_API_TOKEN (copy from production or edit)"
}
Write-Host "  # Edit .env.local -> ZEPHYR_CONFLUENCE_PARENT_PAGE_ID (sandbox)"
Write-Host "  .\run_zephyr_local.ps1"

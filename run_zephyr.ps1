$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$envPath = Join-Path $RepoRoot ".env"
$envExamplePath = Join-Path $RepoRoot ".env.example"

if (-not (Test-Path -LiteralPath $envPath)) {
    Write-Host @"

Missing configuration file: $envPath

Copy the template and fill in secrets (at least ZEPHYR_API_TOKEN):
  Copy-Item -LiteralPath '$envExamplePath' -Destination '$envPath'
Or copy .env.example to .env in File Explorer, then edit .env.
"@ -ForegroundColor Yellow
    exit 1
}

Get-Content -LiteralPath $envPath | ForEach-Object {
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

$env:ZEPHYR_CONFLUENCE_AUTH_SCHEME = "bearer"

$extraArgs = @($args)

$reportScript = Join-Path $RepoRoot "zephyr_weekly_report.py"
$readableTemplateDir = Join-Path $RepoRoot "report_templates\readable"

$cmdArgs = @(
  $reportScript,
  "--base-url", "$env:ZEPHYR_BASE_URL",
  "--endpoint", "$env:ZEPHYR_ENDPOINT",
  "--discover-folders",
  "--discovery-mode", "$env:ZEPHYR_DISCOVERY_MODE",
  "--folder-endpoint", "$env:ZEPHYR_FOLDER_ENDPOINT",
  "--folder-search-endpoint", "$env:ZEPHYR_FOLDER_SEARCH_ENDPOINT",
  "--foldertree-endpoint", "$env:ZEPHYR_FOLDERTREE_ENDPOINT",
  "--project-id", "$env:ZEPHYR_PROJECT_ID",
  "--query-template", "$env:ZEPHYR_QUERY_TEMPLATE",
  "--project-query", "$env:ZEPHYR_PROJECT_QUERY",
  "--extra-param", "fields=$env:ZEPHYR_FIELDS",
  "--extra-param", "maxResults=$env:ZEPHYR_MAX_RESULTS",
  "--extra-param", "startAt=$env:ZEPHYR_START_AT",
  "--extra-param", "archived=$env:ZEPHYR_ARCHIVED",
  "--date-field", "$env:ZEPHYR_DATE_FIELD",
  "--status-field", "$env:ZEPHYR_STATUS_FIELD",
  "--output", "$env:ZEPHYR_OUTPUT",
  "--per-folder-dir", "$env:ZEPHYR_PER_FOLDER_DIR",
  "--root-folder-id", "$env:ZEPHYR_ROOT_FOLDER_IDS",
  "--tree-leaf-only",
  "--tree-name-regex", "$env:ZEPHYR_TREE_NAME_REGEX",
  "--folder-name-endpoint-template", "$env:ZEPHYR_FOLDER_NAME_ENDPOINT_TEMPLATE",
  "--cycles-cases-output", "$env:ZEPHYR_CYCLES_CASES_OUTPUT",
  "--testcase-endpoint-template", "$env:ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE",
  "--case-steps-output", "$env:ZEPHYR_CASE_STEPS_OUTPUT",
  "--daily-readable-dir", "$env:ZEPHYR_DAILY_READABLE_DIR",
  "--readable-template-dir", $readableTemplateDir,
  "--continue-on-folder-error"
)

if ($env:ZEPHYR_FOLDER_WORKERS) {
  $cmdArgs += @("--folder-workers", "$env:ZEPHYR_FOLDER_WORKERS")
}

if ($env:ZEPHYR_DETAIL_WORKERS) {
  $cmdArgs += @("--detail-workers", "$env:ZEPHYR_DETAIL_WORKERS")
}

if (Test-EnvEnabled $env:ZEPHYR_EXPORT_CYCLES_CASES $true) {
  $cmdArgs += @("--export-cycles-cases")
}

if (Test-EnvEnabled $env:ZEPHYR_SYNTHETIC_CYCLE_IDS $true) {
  $cmdArgs += @("--synthetic-cycle-ids")
}

if (Test-EnvEnabled $env:ZEPHYR_EXPORT_CASE_STEPS $true) {
  $cmdArgs += @("--export-case-steps")
}

if (Test-EnvEnabled $env:ZEPHYR_EXPORT_DAILY_READABLE $true) {
  $cmdArgs += @("--export-daily-readable")
  $dailyFmtRaw = if ($env:ZEPHYR_DAILY_READABLE_FORMATS) { $env:ZEPHYR_DAILY_READABLE_FORMATS } else { "html,wiki" }
  foreach ($part in ($dailyFmtRaw -split ',')) {
    $f = $part.Trim().ToLowerInvariant()
    if ($f -eq 'html' -or $f -eq 'wiki') {
      $cmdArgs += @("--daily-readable-format", $f)
    }
  }
}

if (Test-EnvEnabled $env:ZEPHYR_EXPORT_WEEKLY_READABLE $false) {
  $cmdArgs += @("--export-weekly-readable")
  if ($env:ZEPHYR_WEEKLY_READABLE_DIR) {
    $cmdArgs += @("--weekly-readable-dir", "$env:ZEPHYR_WEEKLY_READABLE_DIR")
  }
  $weeklyFmtRaw = if ($env:ZEPHYR_WEEKLY_READABLE_FORMATS) { $env:ZEPHYR_WEEKLY_READABLE_FORMATS } else { "html,wiki" }
  foreach ($part in ($weeklyFmtRaw -split ',')) {
    $f = $part.Trim().ToLowerInvariant()
    if ($f -eq 'html' -or $f -eq 'wiki') {
      $cmdArgs += @("--weekly-readable-format", $f)
    }
  }
}

if ($extraArgs.Count -gt 0) {
  $cmdArgs += $extraArgs
}

if ($env:PYTHON_BIN) {
  & $env:PYTHON_BIN @cmdArgs
  exit $LASTEXITCODE
}

if (Get-Command py -ErrorAction SilentlyContinue) {
  & py -3 @cmdArgs
  exit $LASTEXITCODE
}

if (Get-Command python -ErrorAction SilentlyContinue) {
  & python @cmdArgs
  exit $LASTEXITCODE
}

throw "Python not found. Install Python 3.10+ (with py launcher or python on PATH), or set PYTHON_BIN to your python.exe path."

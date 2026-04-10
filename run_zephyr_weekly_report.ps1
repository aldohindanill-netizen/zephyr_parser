param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonBin = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "py" }
$PythonBinArgs = @()
if (-not $env:PYTHON_BIN) {
    $PythonBinArgs += "-3"
}

$BaseUrl = if ($env:ZEPHYR_BASE_URL) { $env:ZEPHYR_BASE_URL } else { "https://api.zephyrscale.smartbear.com" }
$Endpoint = if ($env:ZEPHYR_ENDPOINT) { $env:ZEPHYR_ENDPOINT } else { "/v2/testexecutions" }
$Output = if ($env:ZEPHYR_OUTPUT) { $env:ZEPHYR_OUTPUT } else { "weekly_zephyr_report.csv" }
$PageSize = if ($env:ZEPHYR_PAGE_SIZE) { $env:ZEPHYR_PAGE_SIZE } else { "100" }
$TokenHeader = if ($env:ZEPHYR_TOKEN_HEADER) { $env:ZEPHYR_TOKEN_HEADER } else { "Authorization" }
$TokenPrefix = if ($env:ZEPHYR_TOKEN_PREFIX) { $env:ZEPHYR_TOKEN_PREFIX } else { "Bearer" }

if (-not $env:ZEPHYR_API_TOKEN) {
    throw "Set ZEPHYR_API_TOKEN environment variable before running."
}

$ArgsList = @(
    "$ScriptDir/zephyr_weekly_report.py"
    "--base-url", $BaseUrl
    "--endpoint", $Endpoint
    "--token", $env:ZEPHYR_API_TOKEN
    "--token-header", $TokenHeader
    "--token-prefix", $TokenPrefix
    "--page-size", $PageSize
    "--output", $Output
)

if ($env:ZEPHYR_FROM_DATE) {
    $ArgsList += @("--from-date", $env:ZEPHYR_FROM_DATE)
}
if ($env:ZEPHYR_TO_DATE) {
    $ArgsList += @("--to-date", $env:ZEPHYR_TO_DATE)
}

if ($env:ZEPHYR_EXTRA_PARAMS) {
    $env:ZEPHYR_EXTRA_PARAMS.Split(",") | ForEach-Object {
        $param = $_.Trim()
        if ($param) {
            $ArgsList += @("--extra-param", $param)
        }
    }
}

if ($env:ZEPHYR_DATE_FIELDS) {
    $env:ZEPHYR_DATE_FIELDS.Split(",") | ForEach-Object {
        $field = $_.Trim()
        if ($field) {
            $ArgsList += @("--date-field", $field)
        }
    }
}

if ($env:ZEPHYR_STATUS_FIELDS) {
    $env:ZEPHYR_STATUS_FIELDS.Split(",") | ForEach-Object {
        $field = $_.Trim()
        if ($field) {
            $ArgsList += @("--status-field", $field)
        }
    }
}

if ($ExtraArgs) {
    $ArgsList += $ExtraArgs
}

& $PythonBin @PythonBinArgs @ArgsList

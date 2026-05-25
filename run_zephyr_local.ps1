# Local debug launcher: loads .env then .env.local overrides (see .env.local.example).
# Does not affect Task Scheduler runs (they use run_zephyr.ps1 without -UseLocalEnv).

$ErrorActionPreference = "Stop"

$Runner = Join-Path $PSScriptRoot "run_zephyr.ps1"
if (-not (Test-Path -LiteralPath $Runner)) {
    throw "Missing $Runner"
}

& $Runner -UseLocalEnv @args
exit $LASTEXITCODE

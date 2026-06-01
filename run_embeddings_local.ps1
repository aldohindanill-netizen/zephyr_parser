# Local embeddings: loads .env then .env.local (same isolation as run_zephyr_local.ps1).

$ErrorActionPreference = "Stop"

$Runner = Join-Path $PSScriptRoot "run_embeddings_scheduled.ps1"
if (-not (Test-Path -LiteralPath $Runner)) {
    throw "Missing $Runner"
}

& $Runner -UseLocalEnv @args
exit $LASTEXITCODE

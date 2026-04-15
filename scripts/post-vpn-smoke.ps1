param(
    [Parameter(Mandatory = $false)]
    [string]$TestResultId = "112522",
    [Parameter(Mandatory = $false)]
    [int]$WaitSeconds = 120,
    [Parameter(Mandatory = $false)]
    [string]$EnvFile = "infra/.env.nocodb-n8n",
    [Parameter(Mandatory = $false)]
    [string]$ComposeFile = "infra/docker-compose.nocodb-n8n.yml"
)

$ErrorActionPreference = "Stop"

function Invoke-PgQuery {
    param([string]$Sql)
    @"
$Sql
"@ | docker compose --env-file "$EnvFile" -f "$ComposeFile" exec -T postgres psql -U zephyr -d zephyr_ops
}

Write-Host "== Pre-check for test_result_id=$TestResultId ==" -ForegroundColor Cyan
Invoke-PgQuery @"
SELECT test_result_id, desired_status_id, desired_comment, sync_state, updated_at
FROM test_results
WHERE test_result_id = '$TestResultId';

SELECT id, test_result_id, status, attempt_count, created_at, updated_at
FROM sync_queue
WHERE test_result_id = '$TestResultId'
ORDER BY created_at DESC
LIMIT 5;
"@

Write-Host "Waiting $WaitSeconds seconds for writeback tick..." -ForegroundColor Yellow
Start-Sleep -Seconds $WaitSeconds

Write-Host "== Post-check ==" -ForegroundColor Cyan
Invoke-PgQuery @"
SELECT id, test_result_id, status, attempt_count, last_error, updated_at
FROM sync_queue
WHERE test_result_id = '$TestResultId'
ORDER BY updated_at DESC
LIMIT 5;

SELECT queue_id, test_result_id, request_method, request_url, response_status, success, executed_at
FROM sync_audit
WHERE test_result_id = '$TestResultId'
ORDER BY executed_at DESC
LIMIT 5;

SELECT id, status, "startedAt", "stoppedAt"
FROM execution_entity
WHERE "workflowId" = 'zephyr_writeback_realtime'
ORDER BY id DESC
LIMIT 10;
"@

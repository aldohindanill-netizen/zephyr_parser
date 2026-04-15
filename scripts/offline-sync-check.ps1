param(
    [Parameter(Mandatory = $false)]
    [string]$TestResultId = "112522",
    [Parameter(Mandatory = $false)]
    [int]$LookbackMinutes = 20,
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

Write-Host "== Workflow health (last ticks) ==" -ForegroundColor Cyan
Invoke-PgQuery @"
SELECT id, "workflowId", status, "startedAt", "stoppedAt"
FROM execution_entity
WHERE "workflowId" IN ('grist_to_postgres_sync', 'postgres_to_grist_status_sync')
ORDER BY id DESC
LIMIT 12;
"@

Write-Host "== Offline path check (Grist -> Postgres -> sync_queue) ==" -ForegroundColor Cyan
Invoke-PgQuery @"
SELECT ru.folder_id, tr.test_result_id, ru.run_name, tr.test_case_name,
       tr.desired_status_id, tr.desired_comment, tr.sync_state, tr.updated_at
FROM test_results tr
JOIN test_runs ru ON ru.test_run_id = tr.test_run_id
WHERE tr.test_result_id = '$TestResultId'
ORDER BY tr.updated_at DESC
LIMIT 10;

SELECT id, test_result_id, status, attempt_count, created_at, updated_at
FROM sync_queue
WHERE test_result_id = '$TestResultId'
ORDER BY created_at DESC
LIMIT 10;
"@

Write-Host "== Queue pressure / recent errors ==" -ForegroundColor Cyan
Invoke-PgQuery @"
SELECT status, COUNT(*) AS cnt
FROM sync_queue
GROUP BY status
ORDER BY status;

SELECT id, test_result_id, status, attempt_count, last_error, updated_at
FROM sync_queue
WHERE updated_at > NOW() - INTERVAL '$LookbackMinutes minutes'
  AND (last_error IS NOT NULL AND last_error <> '')
ORDER BY updated_at DESC
LIMIT 20;
"@

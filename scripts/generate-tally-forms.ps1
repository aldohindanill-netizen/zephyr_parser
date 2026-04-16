param(
    [Parameter(Mandatory = $false)]
    [string]$EnvFile = "infra/.env.tally-n8n",
    [Parameter(Mandatory = $false)]
    [string]$ComposeFile = "infra/docker-compose.nocodb-n8n.yml",
    [Parameter(Mandatory = $false)]
    [string]$FolderId = "",
    [Parameter(Mandatory = $false)]
    [string]$ScenarioKey = "",
    [Parameter(Mandatory = $false)]
    [string]$LandingPageUrl = "",
    [Parameter(Mandatory = $false)]
    [switch]$IncludeIngest
)

$ErrorActionPreference = "Stop"
$script:EnvMap = @{}
$script:Utf8Encoding = [System.Text.UTF8Encoding]::new($false)

[Console]::InputEncoding = $script:Utf8Encoding
[Console]::OutputEncoding = $script:Utf8Encoding
$OutputEncoding = $script:Utf8Encoding
[System.Environment]::SetEnvironmentVariable("PGCLIENTENCODING", "UTF8", "Process")

function Get-EnvValueFromFile {
    param(
        [string]$Path,
        [string]$Name
    )
    if (-not (Test-Path $Path)) {
        return $null
    }

    foreach ($rawLine in Get-Content -Path $Path -Encoding utf8) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            continue
        }
        $parts = $line -split "=", 2
        if ($parts.Count -eq 2 -and $parts[0] -eq $Name) {
            return $parts[1]
        }
    }

    return $null
}

function Load-EnvFile {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        throw "Env file not found: $Path"
    }

    Get-Content -Path $Path -Encoding utf8 | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $parts = $line -split "=", 2
            if ($parts.Count -eq 2) {
                $script:EnvMap[$parts[0]] = $parts[1]
                [System.Environment]::SetEnvironmentVariable($parts[0], $parts[1], "Process")
            }
        }
    }
}

function Get-RequiredEnv {
    param([string]$Name)
    $value = [System.Environment]::GetEnvironmentVariable($Name, "Process")
    if ([string]::IsNullOrWhiteSpace($value) -and $script:EnvMap.ContainsKey($Name)) {
        $value = [string]$script:EnvMap[$Name]
    }
    if ([string]::IsNullOrWhiteSpace($value)) {
        $value = Get-EnvValueFromFile -Path $EnvFile -Name $Name
    }
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Required environment variable is missing: $Name"
    }
    return $value
}

function Invoke-PgJson {
    param([string]$Sql)

    $result = @"
$Sql
"@ | docker compose --env-file "$EnvFile" -f "$ComposeFile" exec -T -e PGCLIENTENCODING=UTF8 postgres `
        psql -U zephyr -d zephyr_ops -t -A

    $json = ($result | Out-String).Trim()
    if (-not $json) {
        return $null
    }
    return $json | ConvertFrom-Json
}

function Invoke-PgQuery {
    param([string]$Sql)
    @"
$Sql
"@ | docker compose --env-file "$EnvFile" -f "$ComposeFile" exec -T -e PGCLIENTENCODING=UTF8 postgres `
        psql -U zephyr -d zephyr_ops
}

function Escape-SqlLiteral {
    param($Value)
    if ($null -eq $Value) {
        return "NULL"
    }
    $text = [string]$Value
    $escaped = $text -replace "'", "''"
    return "'$escaped'"
}

function New-Block {
    param(
        [string]$Type,
        [string]$GroupUuid,
        [string]$GroupType,
        [hashtable]$Payload
    )
    return @{
        uuid = ([guid]::NewGuid()).ToString()
        type = $Type
        groupUuid = $GroupUuid
        groupType = $GroupType
        payload = $Payload
    }
}

function Build-TallyBlocks {
    param(
        [string]$ScenarioTitle,
        [array]$CaseRows,
        [string]$LandingPageUrl
    )

    $blocks = New-Object System.Collections.Generic.List[object]

    $titleGroup = ([guid]::NewGuid()).ToString()
    $blocks.Add((New-Block -Type "FORM_TITLE" -GroupUuid $titleGroup -GroupType "TEXT" -Payload @{
        title = $ScenarioTitle
        html = $ScenarioTitle
    }))

    $introGroup = ([guid]::NewGuid()).ToString()
    $blocks.Add((New-Block -Type "TEXT" -GroupUuid $introGroup -GroupType "TEXT" -Payload @{
        html = "Select PASS or FAIL for each case and add an optional comment when needed."
    }))

    if (-not [string]::IsNullOrWhiteSpace($LandingPageUrl)) {
        $backLinkGroup = ([guid]::NewGuid()).ToString()
        $blocks.Add((New-Block -Type "TEXT" -GroupUuid $backLinkGroup -GroupType "TEXT" -Payload @{
            html = "<a href=""$LandingPageUrl"">Back to scenario list</a>"
        }))
    }

    $hiddenGroup = ([guid]::NewGuid()).ToString()
    $blocks.Add((New-Block -Type "HIDDEN_FIELDS" -GroupUuid $hiddenGroup -GroupType "HIDDEN_FIELDS" -Payload @{
        hiddenFields = @(
            @{ uuid = ([guid]::NewGuid()).ToString(); name = "folder_id" },
            @{ uuid = ([guid]::NewGuid()).ToString(); name = "scenario_key" },
            @{ uuid = ([guid]::NewGuid()).ToString(); name = "scenario_title" }
        )
    }))

    $groupedByCycle = $CaseRows | Group-Object cycle_ref
    foreach ($cycleGroup in $groupedByCycle) {
        $firstRow = $cycleGroup.Group[0]
        $cycleTextGroup = ([guid]::NewGuid()).ToString()
        $cycleHtml = "<strong>$($firstRow.cycle_name)</strong>"
        if (-not [string]::IsNullOrWhiteSpace($firstRow.cycle_objective)) {
            $cycleHtml = $cycleHtml + "<br/>" + $firstRow.cycle_objective
        }
        $blocks.Add((New-Block -Type "TEXT" -GroupUuid $cycleTextGroup -GroupType "TEXT" -Payload @{
            html = $cycleHtml
        }))

        foreach ($row in $cycleGroup.Group) {
            $resultLabel = "result__{0}" -f $row.test_result_id
            $resultQuestionGroup = ([guid]::NewGuid()).ToString()
            $resultOptionsGroup = ([guid]::NewGuid()).ToString()
            $statusHint = ""
            if (-not [string]::IsNullOrWhiteSpace($row.current_status_name)) {
                $statusHint = " (current: $($row.current_status_name))"
            }

            $blocks.Add((New-Block -Type "TITLE" -GroupUuid $resultQuestionGroup -GroupType "QUESTION" -Payload @{
                html = "$resultLabel - $($row.test_case_key) $($row.test_case_name)$statusHint"
            }))
            $blocks.Add((New-Block -Type "MULTIPLE_CHOICE_OPTION" -GroupUuid $resultOptionsGroup -GroupType "MULTIPLE_CHOICE" -Payload @{
                text = "PASS"
                index = 0
                isFirst = $true
                isLast = $false
                isRequired = $true
            }))
            $blocks.Add((New-Block -Type "MULTIPLE_CHOICE_OPTION" -GroupUuid $resultOptionsGroup -GroupType "MULTIPLE_CHOICE" -Payload @{
                text = "FAIL"
                index = 1
                isFirst = $false
                isLast = $true
                isRequired = $true
            }))

            $commentLabel = "comment__{0}" -f $row.test_result_id
            $commentGroup = ([guid]::NewGuid()).ToString()
            $blocks.Add((New-Block -Type "TITLE" -GroupUuid $commentGroup -GroupType "QUESTION" -Payload @{
                html = $commentLabel
            }))
            $blocks.Add((New-Block -Type "TEXTAREA" -GroupUuid $commentGroup -GroupType "TEXTAREA" -Payload @{
                isRequired = $false
                placeholder = "Optional comment"
            }))
        }
    }

    return $blocks.ToArray()
}

function Invoke-TallyApi {
    param(
        [string]$Method,
        [string]$Path,
        $Body
    )

    $headers = @{
        Authorization = "Bearer $script:TallyApiKey"
        Accept        = "application/json"
    }

    $params = @{
        Method      = $Method
        Uri         = ($script:TallyApiBaseUrl.TrimEnd("/") + "/" + $Path.TrimStart("/"))
        Headers     = $headers
        ErrorAction = "Stop"
    }

    if ($null -ne $Body) {
        $jsonBody = $Body | ConvertTo-Json -Depth 20
        $params.ContentType = "application/json; charset=utf-8"
        $params.Body = $script:Utf8Encoding.GetBytes($jsonBody)
    }

    return Invoke-RestMethod @params
}

function Test-TallyFormMissingError {
    param($ErrorRecord)

    if ($null -eq $ErrorRecord) {
        return $false
    }

    $message = [string]$ErrorRecord.Exception.Message
    $details = [string]($ErrorRecord | Out-String)
    $combined = $message + "`n" + $details
    return $combined -like "*FORM_NOT_FOUND*" -or $combined -like "*Form was not found*"
}

Load-EnvFile -Path $EnvFile

$script:TallyApiBaseUrl = Get-RequiredEnv "TALLY_API_BASE_URL"
$script:TallyApiKey = Get-RequiredEnv "TALLY_API_KEY"
$tallyPublicBaseUrl = Get-RequiredEnv "TALLY_PUBLIC_BASE_URL"
$tallyWorkspaceId = [System.Environment]::GetEnvironmentVariable("TALLY_WORKSPACE_ID", "Process")
if ([string]::IsNullOrWhiteSpace($tallyWorkspaceId) -and $script:EnvMap.ContainsKey("TALLY_WORKSPACE_ID")) {
    $tallyWorkspaceId = [string]$script:EnvMap["TALLY_WORKSPACE_ID"]
}
if ([string]::IsNullOrWhiteSpace($LandingPageUrl) -and $script:EnvMap.ContainsKey("TALLY_LANDING_PAGE_URL")) {
    $LandingPageUrl = [string]$script:EnvMap["TALLY_LANDING_PAGE_URL"]
}

if ($IncludeIngest) {
    Write-Host "== Running zephyr_ingest_15m first ==" -ForegroundColor Cyan
    docker compose --env-file "$EnvFile" -f "$ComposeFile" exec -T n8n `
        n8n execute --id zephyr_ingest_15m --rawOutput | Out-Host
}

$scenarioWhere = @()
if (-not [string]::IsNullOrWhiteSpace($FolderId)) {
    $scenarioWhere += "folder_id = $(Escape-SqlLiteral $FolderId)"
}
if (-not [string]::IsNullOrWhiteSpace($ScenarioKey)) {
    $scenarioWhere += "scenario_key = $(Escape-SqlLiteral $ScenarioKey)"
}

$scenarioFilterSql = ""
if ($scenarioWhere.Count -gt 0) {
    $scenarioFilterSql = "WHERE " + ($scenarioWhere -join " AND ")
}

$scenariosSql = @"
SELECT COALESCE(json_agg(t ORDER BY t.folder_id, t.scenario_order, t.scenario_key), '[]'::json)
FROM (
    SELECT folder_id, scenario_key, scenario_title, tally_form_id, tally_form_url, execution_day, scenario_order
    FROM tally_scenario_index
    $scenarioFilterSql
) t;
"@

$scenarios = Invoke-PgJson -Sql $scenariosSql
if (-not $scenarios -or $scenarios.Count -eq 0) {
    Write-Host "No scenarios found in tally_scenario_index." -ForegroundColor Yellow
    exit 0
}

$dedupedScenarios = [ordered]@{}
foreach ($scenario in $scenarios) {
    $identityKey = "{0}|{1}" -f [string]$scenario.folder_id, [string]$scenario.scenario_key
    if (-not $dedupedScenarios.Contains($identityKey)) {
        $dedupedScenarios[$identityKey] = $scenario
        continue
    }

    $existingScenario = $dedupedScenarios[$identityKey]
    if ([string]::IsNullOrWhiteSpace([string]$existingScenario.tally_form_id) -and -not [string]::IsNullOrWhiteSpace([string]$scenario.tally_form_id)) {
        $dedupedScenarios[$identityKey] = $scenario
    }
}
$scenarios = @($dedupedScenarios.Values)

foreach ($scenario in $scenarios) {
    $caseSql = @"
SELECT COALESCE(json_agg(t ORDER BY t.cycle_order, t.test_case_name, t.test_case_key), '[]'::json)
FROM (
    SELECT
        folder_id,
        scenario_key,
        scenario_title,
        cycle_ref,
        cycle_order,
        test_run_id,
        cycle_id,
        cycle_name,
        cycle_objective,
        test_result_id,
        test_case_key,
        test_case_name,
        current_status_name
    FROM tally_case_export
    WHERE folder_id = $(Escape-SqlLiteral $scenario.folder_id)
      AND scenario_key = $(Escape-SqlLiteral $scenario.scenario_key)
) t;
"@

    $caseRows = Invoke-PgJson -Sql $caseSql
    if (-not $caseRows -or $caseRows.Count -eq 0) {
        Write-Host "Skipping empty scenario $($scenario.scenario_key)" -ForegroundColor Yellow
        continue
    }

    $blocks = Build-TallyBlocks -ScenarioTitle $scenario.scenario_title -CaseRows $caseRows -LandingPageUrl $LandingPageUrl
    $body = @{
        name = [string]$scenario.scenario_title
        status = "PUBLISHED"
        blocks = $blocks
    }
    if (-not [string]::IsNullOrWhiteSpace($tallyWorkspaceId)) {
        $body.workspaceId = $tallyWorkspaceId
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$scenario.tally_form_id)) {
        try {
            $form = Invoke-TallyApi -Method "PATCH" -Path ("forms/" + $scenario.tally_form_id) -Body $body
        } catch {
            if (-not (Test-TallyFormMissingError -ErrorRecord $_)) {
                throw
            }

            Write-Host ("Missing remote form for {0}; creating a new form." -f $scenario.scenario_key) -ForegroundColor Yellow
            $form = Invoke-TallyApi -Method "POST" -Path "forms" -Body $body
        }
    } else {
        $form = Invoke-TallyApi -Method "POST" -Path "forms" -Body $body
    }

    $formId = [string]$form.id
    $formName = [string]$form.name
    $formUrl = "{0}/{1}" -f $tallyPublicBaseUrl.TrimEnd("/"), $formId

    $upsertSql = @"
INSERT INTO tally_scenario_forms (
    folder_id,
    scenario_key,
    scenario_title,
    tally_form_id,
    tally_form_name,
    tally_form_url,
    is_active,
    last_synced_at,
    updated_at
)
VALUES (
    $(Escape-SqlLiteral $scenario.folder_id),
    $(Escape-SqlLiteral $scenario.scenario_key),
    $(Escape-SqlLiteral $scenario.scenario_title),
    $(Escape-SqlLiteral $formId),
    $(Escape-SqlLiteral $formName),
    $(Escape-SqlLiteral $formUrl),
    TRUE,
    NOW(),
    NOW()
)
ON CONFLICT (folder_id, scenario_key)
DO UPDATE SET
    scenario_title = EXCLUDED.scenario_title,
    tally_form_id = EXCLUDED.tally_form_id,
    tally_form_name = EXCLUDED.tally_form_name,
    tally_form_url = EXCLUDED.tally_form_url,
    is_active = TRUE,
    last_synced_at = NOW(),
    updated_at = NOW();
"@
    Invoke-PgQuery -Sql $upsertSql | Out-Host

    Write-Host ("OK {0} -> {1}" -f $scenario.scenario_key, $formUrl) -ForegroundColor Green
}

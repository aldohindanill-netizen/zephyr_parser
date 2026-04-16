param(
    [Parameter(Mandatory = $false)]
    [string]$EnvFile = "infra/.env.tally-n8n",
    [Parameter(Mandatory = $false)]
    [string]$ComposeFile = "infra/docker-compose.nocodb-n8n.yml",
    [Parameter(Mandatory = $false)]
    [string]$FolderId = "",
    [Parameter(Mandatory = $false)]
    [string]$FormId = "",
    [Parameter(Mandatory = $false)]
    [string]$OutputPath = "docs/tally-aggregator.json"
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

function New-FieldRef {
    param(
        [string]$Uuid,
        [string]$FieldType,
        [string]$QuestionType,
        [string]$BlockGroupUuid,
        [string]$Title,
        [string]$CalculatedFieldType = ""
    )

    $field = @{
        uuid = $Uuid
        type = $FieldType
        questionType = $QuestionType
        blockGroupUuid = $BlockGroupUuid
        title = $Title
    }

    if (-not [string]::IsNullOrWhiteSpace($CalculatedFieldType)) {
        $field.calculatedFieldType = $CalculatedFieldType
    }

    return $field
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
        $jsonBody = $Body | ConvertTo-Json -Depth 30
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

function Build-AggregatorDefinition {
    param([array]$ScenarioRows)

    $blocks = New-Object System.Collections.Generic.List[object]

    $titleGroup = ([guid]::NewGuid()).ToString()
    $blocks.Add((New-Block -Type "FORM_TITLE" -GroupUuid $titleGroup -GroupType "TEXT" -Payload @{
        title = "Scenario selector"
        html = "Scenario selector"
    }))

    $introGroup = ([guid]::NewGuid()).ToString()
    $blocks.Add((New-Block -Type "TEXT" -GroupUuid $introGroup -GroupType "TEXT" -Payload @{
        html = "Choose a scenario and submit to open the matching Tally form."
    }))

    $questionTitle = "Scenario"
    $questionGroup = ([guid]::NewGuid()).ToString()
    $optionsGroup = ([guid]::NewGuid()).ToString()
    $blocks.Add((New-Block -Type "TITLE" -GroupUuid $questionGroup -GroupType "QUESTION" -Payload @{
        html = $questionTitle
    }))

    $questionField = New-FieldRef -Uuid $optionsGroup -FieldType "INPUT_FIELD" -QuestionType "MULTIPLE_CHOICE_OPTION" -BlockGroupUuid $optionsGroup -Title $questionTitle

    $scenarioOptions = New-Object System.Collections.Generic.List[object]
    foreach ($scenario in $ScenarioRows) {
        $optionBlock = New-Block -Type "MULTIPLE_CHOICE_OPTION" -GroupUuid $optionsGroup -GroupType "MULTIPLE_CHOICE" -Payload @{
            text = [string]$scenario.scenario_title
            index = [int]$scenario.option_index
            isFirst = ([int]$scenario.option_index -eq 0)
            isLast = ([int]$scenario.option_index -eq ($ScenarioRows.Count - 1))
            isRequired = $true
        }
        $blocks.Add($optionBlock)
        $scenarioOptions.Add([pscustomobject]@{
            scenario_key = [string]$scenario.scenario_key
            tally_prefilled_url = [string]$scenario.tally_prefilled_url
            option_uuid = [string]$optionBlock.uuid
        })
    }

    $calculatedGroup = ([guid]::NewGuid()).ToString()
    $calculatedFieldUuid = ([guid]::NewGuid()).ToString()
    $blocks.Add((New-Block -Type "CALCULATED_FIELDS" -GroupUuid $calculatedGroup -GroupType "CALCULATED_FIELDS" -Payload @{
        calculatedFields = @(
            @{
                uuid = $calculatedFieldUuid
                name = "URL"
                type = "TEXT"
                value = ""
            }
        )
    }))

    $calculatedField = New-FieldRef -Uuid $calculatedFieldUuid -FieldType "CALCULATED_FIELD" -QuestionType "CALCULATED_FIELDS" -BlockGroupUuid $calculatedGroup -Title "URL" -CalculatedFieldType "TEXT"

    foreach ($scenario in $scenarioOptions) {
        $logicGroup = ([guid]::NewGuid()).ToString()
        $blocks.Add((New-Block -Type "CONDITIONAL_LOGIC" -GroupUuid $logicGroup -GroupType "CONDITIONAL_LOGIC" -Payload @{
            logicalOperator = "AND"
            conditionals = @(
                @{
                    uuid = ([guid]::NewGuid()).ToString()
                    type = "SINGLE"
                    payload = @{
                        field = $questionField
                        comparison = "IS"
                        value = [string]$scenario.option_uuid
                    }
                }
            )
            actions = @(
                @{
                    uuid = ([guid]::NewGuid()).ToString()
                    type = "CALCULATE"
                    payload = @{
                        calculate = @{
                            field = $calculatedField
                            operator = "ASSIGNMENT"
                            value = [string]$scenario.tally_prefilled_url
                        }
                    }
                }
            )
        }))
    }

    $mentionUuid = ([guid]::NewGuid()).ToString()
    $settings = @{
        redirectOnCompletion = @{
            html = "<span class=""mention"" data-uuid=""$mentionUuid"">@URL</span>"
            mentions = @(
                @{
                    uuid = $mentionUuid
                    field = $calculatedField
                }
            )
        }
    }

    return @{
        blocks = $blocks.ToArray()
        settings = $settings
    }
}

Load-EnvFile -Path $EnvFile

$script:TallyApiBaseUrl = Get-RequiredEnv "TALLY_API_BASE_URL"
$script:TallyApiKey = Get-RequiredEnv "TALLY_API_KEY"
$tallyPublicBaseUrl = Get-RequiredEnv "TALLY_PUBLIC_BASE_URL"
$tallyWorkspaceId = [System.Environment]::GetEnvironmentVariable("TALLY_WORKSPACE_ID", "Process")
if ([string]::IsNullOrWhiteSpace($tallyWorkspaceId) -and $script:EnvMap.ContainsKey("TALLY_WORKSPACE_ID")) {
    $tallyWorkspaceId = [string]$script:EnvMap["TALLY_WORKSPACE_ID"]
}
if ([string]::IsNullOrWhiteSpace($FormId) -and $script:EnvMap.ContainsKey("TALLY_AGGREGATOR_FORM_ID")) {
    $FormId = [string]$script:EnvMap["TALLY_AGGREGATOR_FORM_ID"]
}

$folderFilterSql = ""
if (-not [string]::IsNullOrWhiteSpace($FolderId)) {
    $folderFilterSql = "WHERE tsi.folder_id = $(Escape-SqlLiteral $FolderId)"
}

$scenarioSql = @"
WITH links AS (
    SELECT DISTINCT ON (folder_id, scenario_key)
        folder_id,
        scenario_key,
        tally_prefilled_url
    FROM tally_case_export
    WHERE COALESCE(tally_prefilled_url, '') <> ''
    ORDER BY folder_id, scenario_key, cycle_order, test_case_name, test_case_key
)
SELECT COALESCE(json_agg(t ORDER BY t.folder_id, t.scenario_order, t.scenario_key), '[]'::json)
FROM (
    SELECT
        tsi.folder_id,
        tsi.scenario_key,
        tsi.scenario_title,
        tsi.scenario_order,
        links.tally_prefilled_url,
        ROW_NUMBER() OVER (ORDER BY tsi.folder_id, tsi.scenario_order, tsi.scenario_key) - 1 AS option_index
    FROM tally_scenario_index tsi
    JOIN links
        ON links.folder_id = tsi.folder_id
       AND links.scenario_key = tsi.scenario_key
    $folderFilterSql
) t;
"@

$scenarios = Invoke-PgJson -Sql $scenarioSql
if (-not $scenarios -or $scenarios.Count -eq 0) {
    throw "No scenario rows found for aggregator generation."
}

$definition = Build-AggregatorDefinition -ScenarioRows $scenarios
$body = @{
    name = "Scenario selector"
    status = "PUBLISHED"
    blocks = $definition.blocks
    settings = $definition.settings
}
if (-not [string]::IsNullOrWhiteSpace($tallyWorkspaceId)) {
    $body.workspaceId = $tallyWorkspaceId
}

if (-not [string]::IsNullOrWhiteSpace($FormId)) {
    try {
        $form = Invoke-TallyApi -Method "PATCH" -Path ("forms/" + $FormId) -Body $body
    } catch {
        if (-not (Test-TallyFormMissingError -ErrorRecord $_)) {
            throw
        }

        Write-Host "Missing remote aggregator form; creating a new form." -ForegroundColor Yellow
        $form = Invoke-TallyApi -Method "POST" -Path "forms" -Body $body
    }
} else {
    $form = Invoke-TallyApi -Method "POST" -Path "forms" -Body $body
}

$resolvedFormId = [string]$form.id
$resolvedFormUrl = "{0}/{1}" -f $tallyPublicBaseUrl.TrimEnd("/"), $resolvedFormId
$outputFilePath = [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $OutputPath))

$outputObject = @{
    form_id = $resolvedFormId
    form_url = $resolvedFormUrl
    folder_id = if ($FolderId) { $FolderId } else { $null }
    generated_at = (Get-Date).ToString("o")
    scenario_count = $scenarios.Count
}

[System.IO.File]::WriteAllText(
    $outputFilePath,
    ($outputObject | ConvertTo-Json -Depth 10),
    $script:Utf8Encoding
)

Write-Host ("Aggregator form -> {0}" -f $resolvedFormUrl) -ForegroundColor Green

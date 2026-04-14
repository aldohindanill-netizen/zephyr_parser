param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$EnvFile = Join-Path $ScriptDir ".env"

function Import-DotEnvFile {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        return
    }
    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            return
        }
        $pair = $line.Split("=", 2)
        $name = $pair[0].Trim()
        $value = $pair[1].Trim()
        if ($value.StartsWith("'") -and $value.EndsWith("'")) {
            $value = $value.Substring(1, $value.Length - 2)
        } elseif ($value.StartsWith('"') -and $value.EndsWith('"')) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

Import-DotEnvFile -Path $EnvFile

$PythonBin = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "py" }
$PythonBinArgs = @()
if (-not $env:PYTHON_BIN) {
    $PythonBinArgs += "-3"
}

$BaseUrl = if ($env:ZEPHYR_BASE_URL) { $env:ZEPHYR_BASE_URL } else { "https://jira.navio.auto" }
$Endpoint = if ($env:ZEPHYR_ENDPOINT) { $env:ZEPHYR_ENDPOINT } else { "rest/tests/1.0/testrun/search" }
$FolderEndpoint = if ($env:ZEPHYR_FOLDER_ENDPOINT) { $env:ZEPHYR_FOLDER_ENDPOINT } else { "rest/tests/1.0/foldertree" }
$FolderSearchEndpoint = if ($env:ZEPHYR_FOLDER_SEARCH_ENDPOINT) { $env:ZEPHYR_FOLDER_SEARCH_ENDPOINT } else { "rest/tests/1.0/folder/search" }
$ProjectId = if ($env:ZEPHYR_PROJECT_ID) { $env:ZEPHYR_PROJECT_ID } else { "10904" }
$FolderTreeEndpoint = if ($env:ZEPHYR_FOLDERTREE_ENDPOINT) { $env:ZEPHYR_FOLDERTREE_ENDPOINT } else { "rest/tests/1.0/project/$ProjectId/foldertree/testrun" }
$Output = if ($env:ZEPHYR_OUTPUT) { $env:ZEPHYR_OUTPUT } else { "weekly_zephyr_report.csv" }
$PerFolderDir = if ($env:ZEPHYR_PER_FOLDER_DIR) { $env:ZEPHYR_PER_FOLDER_DIR } else { "reports/by_folder" }
$PageSize = if ($env:ZEPHYR_PAGE_SIZE) { $env:ZEPHYR_PAGE_SIZE } else { "100" }
$MaxResults = if ($env:ZEPHYR_MAX_RESULTS) { $env:ZEPHYR_MAX_RESULTS } else { "40" }
$StartAt = if ($env:ZEPHYR_START_AT) { $env:ZEPHYR_START_AT } else { "0" }
$Archived = if ($env:ZEPHYR_ARCHIVED) { $env:ZEPHYR_ARCHIVED } else { "false" }
$DateField = if ($env:ZEPHYR_DATE_FIELD) { $env:ZEPHYR_DATE_FIELD } else { "updatedOn" }
$StatusField = if ($env:ZEPHYR_STATUS_FIELD) { $env:ZEPHYR_STATUS_FIELD } else { "status.name" }
$DiscoveryMode = if ($env:ZEPHYR_DISCOVERY_MODE) { $env:ZEPHYR_DISCOVERY_MODE } else { "tree" }
$RootFolderIds = if ($env:ZEPHYR_ROOT_FOLDER_IDS) { $env:ZEPHYR_ROOT_FOLDER_IDS } else { "" }
$FolderNameRegex = if ($env:ZEPHYR_FOLDER_NAME_REGEX) { $env:ZEPHYR_FOLDER_NAME_REGEX } else { "" }
$FolderPathRegex = if ($env:ZEPHYR_FOLDER_PATH_REGEX) { $env:ZEPHYR_FOLDER_PATH_REGEX } else { "" }
$TreeNameRegex = if ($env:ZEPHYR_TREE_NAME_REGEX) { $env:ZEPHYR_TREE_NAME_REGEX } else { "" }
$TreeRootPathRegex = if ($env:ZEPHYR_TREE_ROOT_PATH_REGEX) { $env:ZEPHYR_TREE_ROOT_PATH_REGEX } else { "" }
$TreeLeafOnly = if ($env:ZEPHYR_TREE_LEAF_ONLY) { $env:ZEPHYR_TREE_LEAF_ONLY } else { "true" }
$TreeAutoProbe = if ($env:ZEPHYR_TREE_AUTOPROBE) { $env:ZEPHYR_TREE_AUTOPROBE } else { "true" }
$QueryTemplate = if ($env:ZEPHYR_QUERY_TEMPLATE) { $env:ZEPHYR_QUERY_TEMPLATE } else { "testRun.projectId IN ($ProjectId) AND testRun.folderTreeId IN ({folder_id}) ORDER BY testRun.name ASC" }
$ProjectQuery = if ($env:ZEPHYR_PROJECT_QUERY) { $env:ZEPHYR_PROJECT_QUERY } else { "testRun.projectId IN ($ProjectId) ORDER BY testRun.name ASC" }
$WeeklyCycleMatrixOutput = if ($env:ZEPHYR_WEEKLY_CYCLE_MATRIX_OUTPUT) { $env:ZEPHYR_WEEKLY_CYCLE_MATRIX_OUTPUT } else { "reports/weekly_cycle_matrix.csv" }
$ExportWeeklyReadable = if ($env:ZEPHYR_EXPORT_WEEKLY_READABLE) { $env:ZEPHYR_EXPORT_WEEKLY_READABLE } else { "true" }
$WeeklyReadableDir = if ($env:ZEPHYR_WEEKLY_READABLE_DIR) { $env:ZEPHYR_WEEKLY_READABLE_DIR } else { "reports/weekly_readable" }
$WeeklyReadableFormats = if ($env:ZEPHYR_WEEKLY_READABLE_FORMATS) { $env:ZEPHYR_WEEKLY_READABLE_FORMATS } else { "html,wiki" }
$ExportDailyReadable = if ($env:ZEPHYR_EXPORT_DAILY_READABLE) { $env:ZEPHYR_EXPORT_DAILY_READABLE } else { "false" }
$DailyReadableDir = if ($env:ZEPHYR_DAILY_READABLE_DIR) { $env:ZEPHYR_DAILY_READABLE_DIR } else { "reports/daily_readable" }
$DailyReadableFormats = if ($env:ZEPHYR_DAILY_READABLE_FORMATS) { $env:ZEPHYR_DAILY_READABLE_FORMATS } else { "html,wiki" }
$CycleProgressOutput = if ($env:ZEPHYR_CYCLE_PROGRESS_OUTPUT) { $env:ZEPHYR_CYCLE_PROGRESS_OUTPUT } else { "reports/cycle_progress.csv" }
$Fields = if ($env:ZEPHYR_FIELDS) { $env:ZEPHYR_FIELDS } else { "id,key,name,folderId,iterationId,projectVersionId,environmentId,userKeys,environmentIds,plannedStartDate,plannedEndDate,executionTime,estimatedTime,testResultStatuses,testCaseCount,issueCount,status(id,name,i18nKey,color),customFieldValues,createdOn,createdBy,updatedOn,updatedBy,owner" }
$TokenHeader = if ($env:ZEPHYR_TOKEN_HEADER) { $env:ZEPHYR_TOKEN_HEADER } else { "Authorization" }
$TokenPrefix = if ($env:ZEPHYR_TOKEN_PREFIX) { $env:ZEPHYR_TOKEN_PREFIX } else { "Bearer" }
$ConfluencePublishDaily = if ($env:CONFLUENCE_PUBLISH_DAILY) { $env:CONFLUENCE_PUBLISH_DAILY } else { "false" }
$ConfluencePublishWeekly = if ($env:CONFLUENCE_PUBLISH_WEEKLY) { $env:CONFLUENCE_PUBLISH_WEEKLY } else { "false" }
$ConfluenceBaseUrl = if ($env:CONFLUENCE_BASE_URL) { $env:CONFLUENCE_BASE_URL } else { "" }
$ConfluenceSpaceKey = if ($env:CONFLUENCE_SPACE_KEY) { $env:CONFLUENCE_SPACE_KEY } else { "" }
$ConfluenceParentPageId = if ($env:CONFLUENCE_PARENT_PAGE_ID) { $env:CONFLUENCE_PARENT_PAGE_ID } else { "" }
$ConfluenceUsername = if ($env:CONFLUENCE_USERNAME) { $env:CONFLUENCE_USERNAME } else { "" }
$ConfluenceApiToken = if ($env:CONFLUENCE_API_TOKEN) { $env:CONFLUENCE_API_TOKEN } else { "" }
$ConfluenceAuthMode = if ($env:CONFLUENCE_AUTH_MODE) { $env:CONFLUENCE_AUTH_MODE.ToLowerInvariant() } else { "auto" }
$ConfluenceVerifySsl = if ($env:CONFLUENCE_VERIFY_SSL) { $env:CONFLUENCE_VERIFY_SSL } else { "true" }
$ConfluenceDryRun = if ($env:CONFLUENCE_DRY_RUN) { $env:CONFLUENCE_DRY_RUN } else { "false" }
$ConfluenceUpdateExisting = if ($env:CONFLUENCE_UPDATE_EXISTING) { $env:CONFLUENCE_UPDATE_EXISTING } else { "false" }

if (-not $env:ZEPHYR_API_TOKEN) {
    throw "Set ZEPHYR_API_TOKEN environment variable before running."
}

$ArgsList = @(
    "$ScriptDir/zephyr_weekly_report.py"
    "--base-url", $BaseUrl
    "--endpoint", $Endpoint
    "--discover-folders"
    "--discovery-mode", $DiscoveryMode
    "--folder-endpoint", $FolderEndpoint
    "--folder-search-endpoint", $FolderSearchEndpoint
    "--foldertree-endpoint", $FolderTreeEndpoint
    "--project-id", $ProjectId
    "--query-template", $QueryTemplate
    "--project-query", $ProjectQuery
    "--token", $env:ZEPHYR_API_TOKEN
    "--token-header", $TokenHeader
    "--token-prefix", $TokenPrefix
    "--page-size", $PageSize
    "--output", $Output
    "--per-folder-dir", $PerFolderDir
    "--extra-param", "fields=$Fields"
    "--extra-param", "maxResults=$MaxResults"
    "--extra-param", "startAt=$StartAt"
    "--extra-param", "archived=$Archived"
    "--date-field", $DateField
    "--status-field", $StatusField
    "--cycle-progress-output", $CycleProgressOutput
    "--weekly-cycle-matrix-output", $WeeklyCycleMatrixOutput
)

if ($env:ZEPHYR_FROM_DATE) {
    $ArgsList += @("--from-date", $env:ZEPHYR_FROM_DATE)
}
if ($env:ZEPHYR_TO_DATE) {
    $ArgsList += @("--to-date", $env:ZEPHYR_TO_DATE)
}

if ($TreeLeafOnly -eq "true") {
    $ArgsList += "--tree-leaf-only"
}
if ($TreeAutoProbe -eq "true") {
    $ArgsList += "--tree-autoprobe"
}
if ($TreeNameRegex) {
    $ArgsList += @("--tree-name-regex", $TreeNameRegex)
}
if ($TreeRootPathRegex) {
    $ArgsList += @("--tree-root-path-regex", $TreeRootPathRegex)
}
if ($FolderNameRegex) {
    $ArgsList += @("--folder-name-regex", $FolderNameRegex)
}
if ($FolderPathRegex) {
    $ArgsList += @("--folder-path-regex", $FolderPathRegex)
}
if ($RootFolderIds) {
    $RootFolderIds.Split(",") | ForEach-Object {
        $rootId = $_.Trim()
        if ($rootId) {
            $ArgsList += @("--root-folder-id", $rootId)
        }
    }
}

if ($ExportWeeklyReadable -eq "true") {
    $ArgsList += @("--export-weekly-readable", "--weekly-readable-dir", $WeeklyReadableDir)
    $WeeklyReadableFormats.Split(",") | ForEach-Object {
        $fmt = $_.Trim()
        if ($fmt) {
            $ArgsList += @("--weekly-readable-format", $fmt)
        }
    }
}

if ($ExportDailyReadable -eq "true") {
    $ArgsList += @("--export-daily-readable", "--daily-readable-dir", $DailyReadableDir)
    $DailyReadableFormats.Split(",") | ForEach-Object {
        $fmt = $_.Trim()
        if ($fmt) {
            $ArgsList += @("--daily-readable-format", $fmt)
        }
    }
}

if ($ConfluencePublishDaily -eq "true") {
    $ArgsList += "--publish-confluence-daily"
}

if ($ConfluencePublishWeekly -eq "true") {
    $ArgsList += "--publish-confluence-weekly"
}

if ($ConfluenceBaseUrl) {
    $ArgsList += @("--confluence-base-url", $ConfluenceBaseUrl)
}

if ($ConfluenceSpaceKey) {
    $ArgsList += @("--confluence-space-key", $ConfluenceSpaceKey)
}

if ($ConfluenceParentPageId) {
    $ArgsList += @("--confluence-parent-page-id", $ConfluenceParentPageId)
}

if ($ConfluenceUsername) {
    $ArgsList += @("--confluence-username", $ConfluenceUsername)
}

if ($ConfluenceApiToken) {
    $ArgsList += @("--confluence-api-token", $ConfluenceApiToken)
}

if ($ConfluenceAuthMode -in @("auto", "basic", "bearer")) {
    $ArgsList += @("--confluence-auth-mode", $ConfluenceAuthMode)
}

if ($ConfluenceVerifySsl -in @("true", "false")) {
    $ArgsList += @("--confluence-verify-ssl", $ConfluenceVerifySsl)
}

if ($ConfluenceDryRun -eq "true") {
    $ArgsList += "--confluence-dry-run"
}

if ($ConfluenceUpdateExisting -eq "true") {
    $ArgsList += "--confluence-update-existing"
}

if ($env:ZEPHYR_EXTRA_PARAMS) {
    $env:ZEPHYR_EXTRA_PARAMS.Split(",") | ForEach-Object {
        $param = $_.Trim()
        if ($param) {
            $ArgsList += @("--extra-param", $param)
        }
    }
}

if ($ExtraArgs) {
    $ArgsList += $ExtraArgs
}

& $PythonBin @PythonBinArgs @ArgsList

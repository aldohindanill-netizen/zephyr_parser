param(
    [Parameter(Mandatory = $false)]
    [string]$EnvFile = "infra/.env.tally-n8n",
    [Parameter(Mandatory = $false)]
    [string]$ComposeFile = "infra/docker-compose.nocodb-n8n.yml",
    [Parameter(Mandatory = $false)]
    [string]$FolderId = "",
    [Parameter(Mandatory = $false)]
    [string]$OutputPath = "docs/tally-landing-page.html"
)

$ErrorActionPreference = "Stop"
$script:Utf8Encoding = [System.Text.UTF8Encoding]::new($false)

[Console]::InputEncoding = $script:Utf8Encoding
[Console]::OutputEncoding = $script:Utf8Encoding
$OutputEncoding = $script:Utf8Encoding
[System.Environment]::SetEnvironmentVariable("PGCLIENTENCODING", "UTF8", "Process")

function Load-EnvFile {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        throw "Env file not found: $Path"
    }

    Get-Content -Path $Path -Encoding utf8 | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            return
        }

        $parts = $line -split "=", 2
        if ($parts.Count -eq 2) {
            [System.Environment]::SetEnvironmentVariable($parts[0], $parts[1], "Process")
        }
    }
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

function HtmlEncode {
    param([string]$Value)

    return [System.Net.WebUtility]::HtmlEncode([string]$Value)
}

Load-EnvFile -Path $EnvFile

$folderFilterSql = ""
if (-not [string]::IsNullOrWhiteSpace($FolderId)) {
    $folderFilterSql = "WHERE tsi.folder_id = $(Escape-SqlLiteral $FolderId)"
}

$linksSql = @"
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
        tsi.folder_name,
        tsi.scenario_key,
        tsi.scenario_title,
        tsi.scenario_order,
        tsi.cycle_count,
        tsi.test_case_count,
        links.tally_prefilled_url
    FROM tally_scenario_index tsi
    LEFT JOIN links
        ON links.folder_id = tsi.folder_id
       AND links.scenario_key = tsi.scenario_key
    $folderFilterSql
) t;
"@

$rows = Invoke-PgJson -Sql $linksSql
if (-not $rows -or $rows.Count -eq 0) {
    throw "No rows found in tally_scenario_index for landing page generation."
}

$resolvedOutputPath = [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $OutputPath))
$outputDirectory = Split-Path -Parent $resolvedOutputPath
if (-not (Test-Path $outputDirectory)) {
    throw "Output directory not found: $outputDirectory"
}

$generatedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss K")
$cards = New-Object System.Collections.Generic.List[string]

foreach ($row in $rows) {
    $scenarioTitle = HtmlEncode $row.scenario_title
    $folderName = HtmlEncode $row.folder_name
    $scenarioKey = HtmlEncode $row.scenario_key
    $scenarioUrl = HtmlEncode $row.tally_prefilled_url
    $cycleCount = HtmlEncode ([string]$row.cycle_count)
    $caseCount = HtmlEncode ([string]$row.test_case_count)

    $cards.Add(@"
    <a class="scenario-card" href="$scenarioUrl">
      <span class="scenario-key">Scenario $scenarioKey</span>
      <strong>$scenarioTitle</strong>
      <span class="scenario-meta">$folderName</span>
      <span class="scenario-meta">$cycleCount cycles, $caseCount cases</span>
    </a>
"@)
}

$cardsHtml = [string]::Join([Environment]::NewLine, $cards)

$html = @"
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Tally scenario list</title>
  <style>
    :root {
      color-scheme: light dark;
      font-family: Inter, Segoe UI, Arial, sans-serif;
    }
    body {
      margin: 0;
      padding: 32px 20px 48px;
      background: #f5f7fb;
      color: #18212f;
    }
    main {
      max-width: 960px;
      margin: 0 auto;
    }
    h1 {
      margin: 0 0 8px;
      font-size: 32px;
      line-height: 1.2;
    }
    p {
      margin: 0 0 16px;
      color: #536076;
    }
    .scenario-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 16px;
      margin-top: 24px;
    }
    .scenario-card {
      display: flex;
      flex-direction: column;
      gap: 8px;
      padding: 18px;
      border-radius: 14px;
      background: #ffffff;
      color: inherit;
      text-decoration: none;
      border: 1px solid #d7dfeb;
      box-shadow: 0 8px 24px rgba(24, 33, 47, 0.08);
    }
    .scenario-card:hover {
      border-color: #7aa2ff;
      box-shadow: 0 12px 28px rgba(63, 112, 214, 0.16);
      transform: translateY(-1px);
    }
    .scenario-key {
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: #4b6bba;
    }
    .scenario-meta {
      font-size: 14px;
      color: #536076;
    }
    .footer {
      margin-top: 24px;
      font-size: 13px;
      color: #6c7890;
    }
    @media (prefers-color-scheme: dark) {
      body {
        background: #0f1722;
        color: #edf2ff;
      }
      p, .scenario-meta, .footer {
        color: #b7c3d8;
      }
      .scenario-card {
        background: #172233;
        border-color: #2c3a52;
        box-shadow: none;
      }
      .scenario-key {
        color: #8db0ff;
      }
    }
  </style>
</head>
<body>
  <main>
    <h1>Scenario list</h1>
    <p>Choose one of the scenarios below to open the matching Tally form.</p>
    <section class="scenario-grid">
$cardsHtml
    </section>
    <div class="footer">Updated: $generatedAt</div>
  </main>
</body>
</html>
"@

[System.IO.File]::WriteAllText($resolvedOutputPath, $html, $script:Utf8Encoding)
Write-Host "Landing page saved to $resolvedOutputPath" -ForegroundColor Green

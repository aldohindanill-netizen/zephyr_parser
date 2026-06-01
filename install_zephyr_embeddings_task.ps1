# Register daily embeddings task (default 13:00 local time; prod agreed schedule).
#   .\install_zephyr_embeddings_task.ps1
# Remove:
#   .\install_zephyr_embeddings_task.ps1 -Uninstall

param(
    [string]$TaskName = "ZephyrParserEmbeddingsDaily",
    [string]$DailyAt = "13:00",
    [switch]$Uninstall
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Split-Path -Parent $MyInvocation.MyCommand.Path)).Path
$Wrapper = Join-Path $RepoRoot "run_embeddings_scheduled.ps1"

if (-not (Test-Path -LiteralPath $Wrapper)) {
    throw "Missing $Wrapper"
}

$powershell = (Get-Command powershell.exe).Source
$arguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$Wrapper`""

if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Removed scheduled task: $TaskName"
    exit 0
}

try {
    $triggerTime = [DateTime]::ParseExact($DailyAt, "HH:mm", $null)
}
catch {
    throw "DailyAt must be HH:mm (e.g. 13:00), got: $DailyAt"
}

$action = New-ScheduledTaskAction -Execute $powershell -Argument $arguments -WorkingDirectory $RepoRoot

$now = Get-Date
$start = Get-Date -Year $now.Year -Month $now.Month -Day $now.Day `
    -Hour $triggerTime.Hour -Minute $triggerTime.Minute -Second 0
if ($start -le $now) {
    $start = $start.AddDays(1)
}

$trigger = New-ScheduledTaskTrigger -Daily -At $start

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host @"

Embeddings scheduled task registered.

  Name:     $TaskName
  Daily at: $DailyAt (local)
  Script:   $Wrapper
  Logs:     $RepoRoot\reports\logs\embeddings_YYYY-MM-DD.log
  Venv:     $RepoRoot\.venv-embeddings

Test run:  Start-ScheduledTask -TaskName '$TaskName'
Remove:    .\install_zephyr_embeddings_task.ps1 -Uninstall

"@

# Register Windows Task Scheduler job: run zephyr pipeline every N minutes (default 30).
# Run once in PowerShell (current user; no admin required for /IT self):
#   .\install_zephyr_scheduled_task.ps1
# Remove:
#   .\install_zephyr_scheduled_task.ps1 -Uninstall

param(
    [string]$TaskName = "ZephyrParserEvery30Min",
    [int]$IntervalMinutes = 30,
    [switch]$Uninstall
)

$ErrorActionPreference = "Stop"

if ($IntervalMinutes -lt 1 -or $IntervalMinutes -gt 1440) {
    throw "IntervalMinutes must be between 1 and 1440."
}

$RepoRoot = (Resolve-Path (Split-Path -Parent $MyInvocation.MyCommand.Path)).Path
$Wrapper = Join-Path $RepoRoot "run_zephyr_scheduled.ps1"

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

$action = New-ScheduledTaskAction -Execute $powershell -Argument $arguments -WorkingDirectory $RepoRoot

# First run 1 minute after registration, then every IntervalMinutes (indefinitely).
$startAt = (Get-Date).AddMinutes(1)
$trigger = New-ScheduledTaskTrigger -Once -At $startAt `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4)

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host @"

Scheduled task registered.

  Name:     $TaskName
  Every:    $IntervalMinutes minute(s)
  Script:   $Wrapper
  Logs:     $RepoRoot\reports\logs\scheduled_YYYY-MM-DD.log

Open Task Scheduler: taskschd.msc -> Task Scheduler Library -> $TaskName
Test run now:       Start-ScheduledTask -TaskName '$TaskName'
Remove:             .\install_zephyr_scheduled_task.ps1 -Uninstall

Note: PC must be on and you should be logged in (task uses Interactive principal).
For run-when-logged-off, re-register with a stored password / service account.

"@

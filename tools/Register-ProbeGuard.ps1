$ErrorActionPreference = 'Stop'
$name = 'MeowX Traffic Probe Guard'
$exe = Join-Path $env:LOCALAPPDATA 'Programs\MeowX Traffic\probe-guard.exe'
if (-not (Test-Path -LiteralPath $exe -PathType Leaf)) { throw 'Probe guardian is not installed.' }
if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
    throw 'A task with this name already exists; refusing to replace it.'
}
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction -Execute $exe
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$periodic = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1)
$periodic.Repetition = New-CimInstance -Namespace Root/Microsoft/Windows/TaskScheduler `
    -ClassName MSFT_TaskRepetitionPattern -ClientOnly `
    -Property @{ Interval = 'PT1M'; StopAtDurationEnd = $false }
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
    -MultipleInstances IgnoreNew -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -Hidden
Register-ScheduledTask -TaskName $name -Action $action -Trigger @($trigger, $periodic) `
    -Principal $principal -Settings $settings | Out-Null
Get-ScheduledTask -TaskName $name

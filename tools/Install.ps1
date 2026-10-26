$ErrorActionPreference = 'Stop'
$source = Join-Path $PSScriptRoot 'MeowX Traffic.exe'
$destination = Join-Path $env:LOCALAPPDATA 'Programs\MeowX Traffic'
if (-not (Test-Path -LiteralPath $source)) { throw 'MeowX Traffic.exe is missing.' }
if (Get-Process 'MeowX Traffic' -ErrorAction SilentlyContinue) {
    throw 'Exit MeowX Traffic from its tray menu before installing.'
}
New-Item -ItemType Directory -Path $destination -Force | Out-Null
Copy-Item -LiteralPath $source -Destination (Join-Path $destination 'MeowX Traffic.exe') -Force
$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut((Join-Path ([Environment]::GetFolderPath('Programs')) 'MeowX Traffic.lnk'))
$shortcut.TargetPath = Join-Path $destination 'MeowX Traffic.exe'
$shortcut.Arguments = '--show'
$shortcut.WorkingDirectory = $destination
$shortcut.Save()
Start-Process -FilePath $shortcut.TargetPath -ArgumentList '--enable-autostart','--silent' -WindowStyle Hidden
Write-Output "Installed: $destination"
Write-Output 'Runs silently at Windows sign-in. Open from Start menu or tray.'

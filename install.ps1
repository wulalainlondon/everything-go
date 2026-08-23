$ErrorActionPreference = "Stop"

$Repo = if ($env:EVERYTHING_GO_REPO) { $env:EVERYTHING_GO_REPO } else { "wulalainlondon/averything-bridge" }
$Port = if ($env:EVERYTHING_GO_PORT) { [int]$env:EVERYTHING_GO_PORT } else { 8766 }
$RuntimeDir = if ($env:EVERYTHING_GO_HOME) { $env:EVERYTHING_GO_HOME } else { Join-Path $env:LOCALAPPDATA "AverythingBridge" }
$Arch = if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -eq "Arm64") { "arm64" } else { "amd64" }
$Asset = "everything-go-windows-$Arch.exe"
$Binary = Join-Path $RuntimeDir "everything-go.exe"

New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null
$Release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest"
$Base = "https://github.com/$Repo/releases/download/$($Release.tag_name)"
$TempBinary = "$Binary.download"
$Sums = Join-Path $RuntimeDir "SHA256SUMS"
Invoke-WebRequest -UseBasicParsing -Uri "$Base/$Asset" -OutFile $TempBinary
Invoke-WebRequest -UseBasicParsing -Uri "$Base/SHA256SUMS" -OutFile $Sums

$ExpectedLine = Get-Content $Sums | Where-Object { $_ -match "\s+$([regex]::Escape($Asset))$" } | Select-Object -First 1
if (-not $ExpectedLine) { throw "SHA256SUMS does not contain $Asset" }
$Expected = ($ExpectedLine -split '\s+')[0].ToLowerInvariant()
$Actual = (Get-FileHash -Algorithm SHA256 $TempBinary).Hash.ToLowerInvariant()
if ($Actual -ne $Expected) { throw "Checksum mismatch for $Asset" }
Move-Item -Force $TempBinary $Binary
Remove-Item -Force $Sums

if (-not (Get-Command claude -ErrorAction SilentlyContinue) -and -not (Get-Command codex -ErrorAction SilentlyContinue)) {
  throw "Install and sign in to Claude Code or Codex CLI before installing the bridge."
}

$Arguments = "--port $Port --data-dir `"$RuntimeDir`" --discovery --mdns --tunnel --instance-name `"$env:COMPUTERNAME`""
$Action = New-ScheduledTaskAction -Execute $Binary -Argument $Arguments -WorkingDirectory $RuntimeDir
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$Settings = New-ScheduledTaskSettingsSet -RestartCount 20 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName "Averything Bridge" -Action $Action -Trigger $Trigger -Settings $Settings -Description "Native Go bridge for Averything" -Force | Out-Null
Start-ScheduledTask -TaskName "Averything Bridge"

Write-Host "Averything Bridge installed and started on port $Port."
Write-Host "Runtime: $RuntimeDir"

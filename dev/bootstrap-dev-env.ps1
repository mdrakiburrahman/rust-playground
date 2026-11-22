<#

.SYNOPSIS
  Bootstraps a Windows DevBox with WSL pre-reqs.

.NOTES

  - Non-interactive: recreates WSL and clones -Branch (default: this checkout's branch) into /workspaces/rust-playground.
  - The script uninstalls Docker Desktop as it interferes with WSL2.
  - Must be run as Administrator in PowerShell 7+.

#>

#Requires -RunAsAdministrator

param(
    [string]$Branch = (git -C $PSScriptRoot rev-parse --abbrev-ref HEAD),
    [string]$LinuxUser = ($env:USERNAME.ToLower() -replace '[^a-z0-9_-]', '')
)

. (Join-Path $PSScriptRoot 'initialize-vscode.ps1')

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Error "This script requires PowerShell 7+. You are running PowerShell $($PSVersionTable.PSVersion).`nTo launch PowerShell 7 as Administrator:`n  Start Menu > search 'pwsh' > right-click 'PowerShell 7' > 'Run as administrator'"
    exit 1
}

# Git Credential Manager lets git inside WSL reuse this checkout's sign-in; install it if Git for Windows lacks it.
if (-not (git credential-manager --version 2>$null)) {
    winget install -e --id Git.GCM --silent --accept-package-agreements --accept-source-agreements --disable-interactivity
    $env:PATH = [Environment]::GetEnvironmentVariable('PATH', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('PATH', 'User')
}
$gcm = @((Get-Command git-credential-manager -ErrorAction SilentlyContinue).Source, "$(Split-Path (Split-Path (git --exec-path)))\bin\git-credential-manager.exe") |
    Where-Object { $_ -and (Test-Path $_) } | Select-Object -First 1

# Fail before deleting WSL if the branch can't be cloned without a prompt.
$origin = git -C $PSScriptRoot remote get-url origin
$env:GCM_INTERACTIVE = 'never'
$env:GIT_TERMINAL_PROMPT = '0'
git ls-remote --exit-code --heads $origin $Branch 2>$null | Out-Null
if ($LASTEXITCODE) { throw "Cannot read $Branch from origin without a prompt; push it, or run 'git fetch' in $PSScriptRoot once to sign in, then rerun." }

# See: https://github.com/microsoft/vscode-remote-release/issues/11480
$sshDir = Join-Path $env:USERPROFILE ".ssh"
$knownHosts = Join-Path $sshDir "known_hosts"
if (-not (Test-Path $sshDir)) {
    New-Item -ItemType Directory -Path $sshDir -Force | Out-Null
}
if (-not (Test-Path $knownHosts)) {
    Write-Host "Creating missing $knownHosts to avoid VS Code Remote-Containers known_hosts bug"
    New-Item -ItemType File -Path $knownHosts -Force | Out-Null
}
if ((Get-Item $knownHosts).Length -eq 0) {
    Write-Host "$knownHosts is empty; writing placeholder line to avoid VS Code Remote-Containers known_hosts bug"
    Set-Content -Path $knownHosts -Value "# placeholder entry to avoid https://github.com/microsoft/vscode-remote-release/issues/11480" -NoNewline:$false
}
icacls $knownHosts /inheritance:r /grant:r "$($env:USERNAME):F" | Out-Null

$dockerProcesses = @("Docker Desktop")
foreach ($process in $dockerProcesses) {
    try {
        Get-Process -Name $process -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    } catch {

    }
}

winget uninstall "Docker Desktop" --silent --force --accept-source-agreements 2>$null
$pkg = Get-ChildItem 'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall','HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall' -ErrorAction SilentlyContinue |
    Get-ItemProperty -ErrorAction SilentlyContinue |
    Where-Object {
        $displayName = $_.PSObject.Properties.Match('DisplayName') | Select-Object -First 1
        $displayName -and $displayName.Value -like "Docker Desktop*"
    }
if ($pkg) {
    $cmd = $pkg.UninstallString
    Start-Process "cmd.exe" -ArgumentList "/c $cmd /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /FORCECLOSEAPPLICATIONS" -Wait -ErrorAction SilentlyContinue
}
Remove-Item -Path "$env:PROGRAMFILES\Docker", "$env:PROGRAMDATA\Docker*", "$env:LOCALAPPDATA\Docker*", "$env:APPDATA\Docker*" -Recurse -Force -ErrorAction SilentlyContinue

# VS Code windows attached to WSL go stale when it is recreated, and VS Code would focus them instead of reconnecting.
Add-Type -AssemblyName UIAutomationClient
[Windows.Automation.AutomationElement]::RootElement.FindAll('Children', [Windows.Automation.Condition]::TrueCondition) |
    Where-Object { $_.Current.Name -match '\[(Dev Container|WSL)\b.*Visual Studio Code' } |
    ForEach-Object { Write-Host "Closing VS Code window: $($_.Current.Name)"; $_.GetCurrentPattern([Windows.Automation.WindowPattern]::Pattern).Close() }

$distros = (wsl -l -q) | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim().Replace("`0", "") } | Where-Object { $_ }
foreach ($distro in $distros) {
    Write-Host "Unregistering WSL distro: $distro"
    wsl --unregister $distro
}

$RECOMMENDED_CORES = 8
$RECOMMENDED_FREE_GB = 50

$memGB=[math]::Floor((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB)
$cpu=[Environment]::ProcessorCount
$swap=[math]::Floor($memGB/4)

if ($cpu -lt $RECOMMENDED_CORES) {
    Write-Host "WARNING: This machine has $cpu cores, which is below the recommended $RECOMMENDED_CORES cores." -ForegroundColor DarkYellow
} else {
    Write-Host "(detected $cpu cores, ${memGB}GB RAM)" -ForegroundColor Green
}

$driveOptions = @(Get-CimInstance Win32_LogicalDisk -Filter "DriveType=3" |
    Where-Object { $_.VolumeName -notmatch 'Temporary' } |
    Sort-Object -Property FreeSpace -Descending |
    ForEach-Object {
        [PSCustomObject]@{
            Path   = "$($_.DeviceID)\VHD"
            FreeGB = [math]::Floor($_.FreeSpace / 1GB)
        }
    })

if ($driveOptions.Count -eq 0) {
    Write-Error "No local fixed drives found to host the WSL VHD."
    exit 1
}

$wslLocation = $driveOptions[0]

if ($wslLocation.FreeGB -lt $RECOMMENDED_FREE_GB) {
    Write-Host "WARNING: $($wslLocation.Path) has only $($wslLocation.FreeGB) GB free, which is below the recommended ${RECOMMENDED_FREE_GB} GB." -ForegroundColor DarkYellow
}
Write-Host "Using $($wslLocation.Path), the drive with the most free space, for the WSL VHD." -ForegroundColor Green

New-Item -ItemType Directory -Path $wslLocation.Path -Force | Out-Null

Write-Host "Configuring Windows Defender exclusions for WSL/Docker..." -ForegroundColor Cyan

$existingPaths = @((Get-MpPreference).ExclusionPath)
if ($wslLocation.Path -notin $existingPaths) {
    Add-MpPreference -ExclusionPath $wslLocation.Path
    Write-Host "  Path exclusion added: $($wslLocation.Path)" -ForegroundColor Green
} else {
    Write-Host "  Path exclusion already exists: $($wslLocation.Path)" -ForegroundColor DarkGray
}

$defenderProcessExclusions = @(
    "wsl.exe",
    "wslhost.exe",
    "wslservice.exe",
    "vmmem",
    "vmmemWSL",
    "dockerd",
    "containerd",
    "containerd-shim-runc-v2",
    "com.docker.backend.exe"
)
$existingProcesses = @((Get-MpPreference).ExclusionProcess)
$added = @()
$skipped = @()
foreach ($proc in $defenderProcessExclusions) {
    if ($proc -notin $existingProcesses) {
        Add-MpPreference -ExclusionProcess $proc
        $added += $proc
    } else {
        $skipped += $proc
    }
}
if ($added)   { Write-Host "  Process exclusions added: $($added -join ', ')" -ForegroundColor Green }
if ($skipped) { Write-Host "  Process exclusions already exist: $($skipped -join ', ')" -ForegroundColor DarkGray }

@"
[wsl2]
memory=${memGB}GB
processors=$cpu
swap=${swap}GB
networkingMode=NAT

[general]
instanceIdleTimeout=-1
"@ | Set-Content -Path "$env:USERPROFILE\.wslconfig"

Write-Host "Restarting WSL to apply settings"
wsl --shutdown

Write-Host "Updating WSL (the --location flag requires WSL 2.4.4+)"
wsl --update

$minWslVersion = [version]'2.4.4'
$prevEncoding = [Console]::OutputEncoding
try {
    [Console]::OutputEncoding = [System.Text.Encoding]::Unicode
    $wslVersionRaw = (wsl --version) 2>$null
} finally {
    [Console]::OutputEncoding = $prevEncoding
}
$versionMatch = [regex]::Match(($wslVersionRaw -join "`n"), '(\d+)\.(\d+)\.(\d+)')
if (-not $versionMatch.Success) {
    Write-Error "Could not determine WSL version from 'wsl --version'. Run 'wsl --update' manually and re-run this script."
    exit 1
}
$wslVersion = [version]("$($versionMatch.Groups[1].Value).$($versionMatch.Groups[2].Value).$($versionMatch.Groups[3].Value)")
if ($wslVersion -lt $minWslVersion) {
    Write-Error "Detected WSL $wslVersion, but $minWslVersion is required for 'wsl --install --location'. Run 'wsl --update' manually and re-run this script."
    exit 1
}
Write-Host "WSL version $wslVersion detected." -ForegroundColor Green

Write-Host "Installing Ubuntu to $($wslLocation.Path)"
wsl --install -d Ubuntu-24.04 --location $wslLocation.Path --no-launch --web-download
if ($LASTEXITCODE) { throw "wsl --install failed with exit code $LASTEXITCODE." }

Write-Host "Creating default Linux user $LinuxUser with passwordless sudo"
wsl -d Ubuntu-24.04 -u root --exec bash -c 'useradd -m -s /bin/bash -G sudo "$1" && echo "$1 ALL=(ALL) NOPASSWD:ALL" > "/etc/sudoers.d/$1" && install -d -o "$1" /workspaces' bash $LinuxUser
if ($LASTEXITCODE) { throw "Could not create Linux user $LinuxUser." }
wsl --manage Ubuntu-24.04 --set-default-user $LinuxUser
if ($LASTEXITCODE) { throw "Could not make $LinuxUser the default WSL user." }

Write-Host "Cloning $Branch into /workspaces/rust-playground"
$cloneScript = @'
set -u
git config --global user.name "$1" &&
git config --global user.email "$2" &&
git config --global credential.helper "$(printf %q "$(wslpath -u "$5")")" || exit $?
target=$6
error_file=$(mktemp) || exit 1
trap 'rm -f -- "$error_file"' EXIT
for attempt in 1 2 3 4; do
    if [ -e "$target" ] || [ -L "$target" ]; then
        echo "Clone destination already exists; refusing to overwrite it." >&2
        exit 1
    fi
    if GIT_TERMINAL_PROMPT=0 git clone --recurse-submodules --branch "$3" "$4" "$target" >"$error_file" 2>&1; then
        exit 0
    else
        status=$?
    fi
    if ! grep -qiE 'could not resolve host|could not resolve hostname|temporary failure in name resolution' "$error_file"; then
        echo "Git clone failed with exit code $status; check the branch and authentication." >&2
        exit "$status"
    fi
    if [ -e "$target" ] || [ -L "$target" ]; then
        echo "Git clone failed on DNS and left a destination; refusing to overwrite it." >&2
        exit "$status"
    fi
    if [ "$attempt" -eq 4 ]; then
        echo "Git clone failed on DNS after $attempt attempts." >&2
        exit "$status"
    fi
    echo "Git clone failed on DNS; retrying (attempt $attempt of 4)." >&2
    sleep $((2 ** attempt))
done
'@
wsl -d Ubuntu-24.04 --cd ~ --exec bash -c $cloneScript bash (git -C $PSScriptRoot config user.name) (git -C $PSScriptRoot config user.email) $Branch $origin $gcm /workspaces/rust-playground
if ($LASTEXITCODE) { throw "Could not clone $Branch into /workspaces/rust-playground." }
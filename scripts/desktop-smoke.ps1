param(
  [Parameter(Mandatory = $true)][string]$Installer,
  [Parameter(Mandatory = $true)][string]$InstallDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ExpectedExecutableName = "cy-kaf-client-desktop.exe"
$ExpectedDisplayName = "Cy KafClient"

$InstallerPath = (Resolve-Path -LiteralPath $Installer).Path
$InstallPath = [System.IO.Path]::GetFullPath($InstallDir)
$TempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
if (
  -not $InstallPath.StartsWith($TempRoot, [System.StringComparison]::OrdinalIgnoreCase) -or
  $InstallPath.Equals($TempRoot.TrimEnd("\"), [System.StringComparison]::OrdinalIgnoreCase)
) {
  throw "InstallDir must be a dedicated directory below the system temp directory"
}
if (Test-Path -LiteralPath $InstallPath) {
  $ExistingEntries = @(Get-ChildItem -LiteralPath $InstallPath -Force)
  if ($ExistingEntries.Count -ne 0) {
    throw "InstallDir must not contain existing files"
  }
}

$RecordedProcesses = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()
$ShellProcess = $null
$ShellProcessId = 0
$SidecarProcessId = 0
$SmokeDirectory = Join-Path $TempRoot ("cy-kaf-desktop-smoke-" + [System.Guid]::NewGuid())
$ConfigPath = Join-Path $SmokeDirectory "config.yaml"
$PreviousTestConfig = [System.Environment]::GetEnvironmentVariable(
  "CY_KAF_DESKTOP_TEST_CONFIG",
  [System.EnvironmentVariableTarget]::Process
)

function Add-RecordedProcess {
  param([System.Diagnostics.Process]$Process)
  $script:RecordedProcesses.Add($Process)
}

function Wait-RecordedProcess {
  param(
    [System.Diagnostics.Process]$Process,
    [int]$TimeoutMilliseconds,
    [string]$Description
  )
  if (-not $Process.WaitForExit($TimeoutMilliseconds)) {
    throw "$Description did not exit within $TimeoutMilliseconds milliseconds"
  }
  if ($Process.ExitCode -ne 0) {
    throw "$Description exited with code $($Process.ExitCode)"
  }
}

try {
  New-Item -ItemType Directory -Path $SmokeDirectory | Out-Null
  Set-Content -LiteralPath $ConfigPath -Value "kafka:`n  clusters: []`n" -Encoding Ascii -NoNewline
  [System.Environment]::SetEnvironmentVariable(
    "CY_KAF_DESKTOP_TEST_CONFIG",
    $ConfigPath,
    [System.EnvironmentVariableTarget]::Process
  )

  $InstallerProcess = Start-Process `
    -FilePath $InstallerPath `
    -ArgumentList @("/S", "/D=$InstallPath") `
    -PassThru
  Add-RecordedProcess $InstallerProcess
  Wait-RecordedProcess $InstallerProcess 120000 "NSIS installer"

  $ApplicationPath = Join-Path $InstallPath $ExpectedExecutableName
  if (-not (Test-Path -LiteralPath $ApplicationPath -PathType Leaf)) {
    throw "installed desktop executable not found: $ApplicationPath"
  }

  $ProductRegistrationPath = (
    "HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\" +
    $ExpectedDisplayName
  )
  if (-not (Test-Path -LiteralPath $ProductRegistrationPath)) {
    throw "visible product registration not found: $ExpectedDisplayName"
  }
  $ProductRegistration = Get-ItemProperty -LiteralPath $ProductRegistrationPath
  if ($ProductRegistration.DisplayName -ne $ExpectedDisplayName) {
    throw "installed product does not expose the expected display name"
  }
  if ($ProductRegistration.MainBinaryName -ne $ExpectedExecutableName) {
    throw "installed product does not retain the protected executable name"
  }

  $DesktopDirectory = [System.Environment]::GetFolderPath(
    [System.Environment+SpecialFolder]::Desktop
  )
  $DesktopShortcutPath = Join-Path $DesktopDirectory "$ExpectedDisplayName.lnk"
  if (-not (Test-Path -LiteralPath $DesktopShortcutPath -PathType Leaf)) {
    throw "visible desktop shortcut not found: $DesktopShortcutPath"
  }
  $WindowsScriptHost = New-Object -ComObject WScript.Shell
  $DesktopShortcut = $WindowsScriptHost.CreateShortcut($DesktopShortcutPath)
  $ShortcutTargetPath = [System.IO.Path]::GetFullPath($DesktopShortcut.TargetPath)
  if (
    -not $ShortcutTargetPath.Equals(
      $ApplicationPath,
      [System.StringComparison]::OrdinalIgnoreCase
    )
  ) {
    throw "visible desktop shortcut does not target the protected executable"
  }

  $ShellProcess = Start-Process -FilePath $ApplicationPath -PassThru
  Add-RecordedProcess $ShellProcess
  $ShellProcessId = $ShellProcess.Id

  $ReadyDeadline = [System.DateTime]::UtcNow.AddSeconds(30)
  $SidecarProcess = $null
  while ([System.DateTime]::UtcNow -lt $ReadyDeadline) {
    $ShellProcess.Refresh()
    if ($ShellProcess.HasExited) {
      throw "desktop shell exited before its window became ready"
    }
    $Children = @(
      Get-CimInstance Win32_Process -Filter "ParentProcessId = $ShellProcessId" |
        Where-Object {
          $_.CommandLine -and
          $_.CommandLine.Contains("--desktop") -and
          $_.CommandLine.Contains("--no-browser")
        }
    )
    if (
      $Children.Count -eq 1 -and
      $ShellProcess.MainWindowHandle -ne 0 -and
      $ShellProcess.MainWindowTitle -eq $ExpectedDisplayName
    ) {
      $SidecarProcess = $Children[0]
      break
    }
    Start-Sleep -Milliseconds 250
  }
  if ($null -eq $SidecarProcess) {
    throw "desktop shell did not expose one visible window and one direct sidecar within 30 seconds"
  }

  $SidecarProcessId = [int]$SidecarProcess.ProcessId
  Add-RecordedProcess (Get-Process -Id $SidecarProcessId)
  $ShellProcess.Refresh()
  if (
    $ShellProcess.MainWindowHandle -eq 0 -or
    $ShellProcess.MainWindowTitle -ne $ExpectedDisplayName
  ) {
    throw "desktop shell does not own the expected visible main window"
  }

  if (-not $ShellProcess.CloseMainWindow()) {
    throw "desktop shell rejected a normal main-window close"
  }

  $ClosedDeadline = [System.DateTime]::UtcNow.AddSeconds(5)
  do {
    $ShellAlive = $null -ne (Get-Process -Id $ShellProcessId -ErrorAction SilentlyContinue)
    $SidecarAlive = $null -ne (Get-Process -Id $SidecarProcessId -ErrorAction SilentlyContinue)
    if (-not $ShellAlive -and -not $SidecarAlive) {
      break
    }
    Start-Sleep -Milliseconds 100
  } while ([System.DateTime]::UtcNow -lt $ClosedDeadline)
  if ($ShellAlive -or $SidecarAlive) {
    throw "desktop shell or sidecar survived five seconds after normal close"
  }

  $Uninstallers = @(
    Get-ChildItem -LiteralPath $InstallPath -Filter "uninstall*.exe" -File
  )
  if ($Uninstallers.Count -ne 1) {
    throw "expected exactly one NSIS uninstaller"
  }
  $UninstallerProcess = Start-Process `
    -FilePath $Uninstallers[0].FullName `
    -ArgumentList "/S" `
    -PassThru
  Add-RecordedProcess $UninstallerProcess
  Wait-RecordedProcess $UninstallerProcess 120000 "NSIS uninstaller"

  $InstallPrefix = $InstallPath.TrimEnd("\") + "\"
  $Unlocked = $false
  $UnlockDeadline = [System.DateTime]::UtcNow.AddSeconds(30)
  while ([System.DateTime]::UtcNow -lt $UnlockDeadline) {
    $ProcessesInsideInstall = @(
      Get-CimInstance Win32_Process |
        Where-Object {
          $_.ExecutablePath -and
          $_.ExecutablePath.StartsWith(
            $InstallPrefix,
            [System.StringComparison]::OrdinalIgnoreCase
          )
        }
    )
    if ($ProcessesInsideInstall.Count -eq 0) {
      if (-not (Test-Path -LiteralPath $InstallPath)) {
        $Unlocked = $true
        break
      }
      $ProbePath = "$InstallPath.smoke-probe"
      if (Test-Path -LiteralPath $ProbePath) {
        throw "temporary lock-probe path already exists"
      }
      try {
        Move-Item -LiteralPath $InstallPath -Destination $ProbePath
        Move-Item -LiteralPath $ProbePath -Destination $InstallPath
        $Unlocked = $true
        break
      }
      catch {
        if (Test-Path -LiteralPath $ProbePath) {
          Move-Item -LiteralPath $ProbePath -Destination $InstallPath
        }
      }
    }
    Start-Sleep -Milliseconds 250
  }
  if (-not $Unlocked) {
    throw "InstallDir still contains a running or locked executable after uninstall"
  }

  Write-Output "DESKTOP WINDOWS SMOKE OK"
}
finally {
  [System.Environment]::SetEnvironmentVariable(
    "CY_KAF_DESKTOP_TEST_CONFIG",
    $PreviousTestConfig,
    [System.EnvironmentVariableTarget]::Process
  )
  foreach ($RecordedProcess in $RecordedProcesses) {
    try {
      if (-not $RecordedProcess.HasExited) {
        $RecordedProcess.Kill()
      }
    }
    catch {
      # The recorded process exited between HasExited and Kill.
    }
  }
  if (
    (Test-Path -LiteralPath $SmokeDirectory) -and
    $SmokeDirectory.StartsWith($TempRoot, [System.StringComparison]::OrdinalIgnoreCase)
  ) {
    [System.IO.Directory]::Delete($SmokeDirectory, $true)
  }
}

@echo off
setlocal EnableExtensions DisableDelayedExpansion
set "PYZKPCLI_PYTHON=%~dp0.venv\Scripts\python.exe"

rem Match the Python invocation used by start.bat in this project.
powershell.exe -NoProfile -NonInteractive -Command ^
    "$ErrorActionPreference = 'Stop';" ^
    "$launchPattern = '^\x22\.venv\\Scripts\\python\.exe\x22\s+\x22app\.py\x22(?:\s|$)';" ^
    "$instances = @(Get-CimInstance Win32_Process -Filter 'Name = ''python.exe''' |" ^
    "    Where-Object { $_.ExecutablePath -eq $env:PYZKPCLI_PYTHON -and $_.CommandLine -match $launchPattern });" ^
    "if ($instances.Count -eq 0) { Write-Output 'pyZkpCli is not running.'; exit 0 };" ^
    "$failed = $false;" ^
    "foreach ($instance in $instances) {" ^
    "    taskkill.exe /PID $instance.ProcessId /T /F;" ^
    "    if ($LASTEXITCODE -ne 0 -and (Get-Process -Id $instance.ProcessId -ErrorAction SilentlyContinue)) { $failed = $true }" ^
    "};" ^
    "if ($failed) { exit 1 };" ^
    "Write-Output 'pyZkpCli stopped.'; exit 0"
exit /b %ERRORLEVEL%
pause

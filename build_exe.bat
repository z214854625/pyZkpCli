@echo off
setlocal EnableExtensions DisableDelayedExpansion
pushd "%~dp0" || exit /b 1

set "VENV_PYTHON=.venv\Scripts\python.exe"
if not exist "%VENV_PYTHON%" (
    python -m venv ".venv"
    if errorlevel 1 goto :build_failed
)

"%VENV_PYTHON%" -m pip --version >nul 2>&1
if errorlevel 1 (
    "%VENV_PYTHON%" -m ensurepip
    if errorlevel 1 goto :build_failed
)

"%VENV_PYTHON%" -m pip --disable-pip-version-check install -r "requirements-build.txt"
if errorlevel 1 goto :build_failed

"%VENV_PYTHON%" -m PyInstaller --name "pyZkpCli" --onefile --console --noupx --noconfirm ^
    --specpath "build" --add-data "%CD%\templates:templates" --add-data "%CD%\static:static" "app.py"
if errorlevel 1 goto :build_failed

echo Built "%CD%\dist\pyZkpCli.exe"
popd
exit /b 0

:build_failed
echo Failed to build pyZkpCli.exe. See the output above for details.
popd
exit /b 1

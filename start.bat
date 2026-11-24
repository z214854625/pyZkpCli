@echo off
setlocal EnableExtensions DisableDelayedExpansion
pushd "%~dp0" || exit /b 1

title pyZkpCli

set "VENV_PYTHON=.venv\Scripts\python.exe"
if not exist "%VENV_PYTHON%" (
    echo Creating Python virtual environment...
    python -m venv ".venv"
    if errorlevel 1 goto :setup_failed
)

"%VENV_PYTHON%" -m pip --version >nul 2>&1
if errorlevel 1 (
    echo Installing pip in the virtual environment...
    "%VENV_PYTHON%" -m ensurepip
    if errorlevel 1 goto :setup_failed
)

echo Checking project dependencies...
"%VENV_PYTHON%" -m pip --disable-pip-version-check install -r "requirements.txt"
if errorlevel 1 goto :setup_failed

echo Starting pyZkpCli...
"%VENV_PYTHON%" "app.py" %*
set "APP_EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %APP_EXIT_CODE%

:setup_failed
echo Failed to set up the Python environment. See the error above. 1>&2
popd
exit /b 1

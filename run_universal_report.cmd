@echo off
setlocal
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_universal_report.ps1" %*
set ERR=%ERRORLEVEL%
if %ERR% neq 0 (
    echo.
    echo Universal report failed with exit code %ERR%.
    echo See the message above. Press any key to close...
    pause >nul
)
exit /b %ERR%

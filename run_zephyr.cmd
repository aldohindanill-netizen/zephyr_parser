@echo off
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_zephyr.ps1" %*
exit /b %ERRORLEVEL%

@echo off
REM Same as run_zephyr.ps1: full stdout/stderr is also copied to logs\zephyr_*.log by zephyr_weekly_report.py (see ZEPHYR_LOG_* in .env).
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_zephyr.ps1" %*
exit /b %ERRORLEVEL%

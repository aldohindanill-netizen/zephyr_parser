# Dev clone

This directory is a **local development clone**. It is isolated from the production pipeline.

| | Production | This dev clone |
|---|------------|----------------|
| Path | `C:\Users\qa\python_app\zephyr_parser` | `C:\Users\qa\python_app\zephyr_parser_dev` |
| Task Scheduler | yes (do not register here) | **no** |
| Reports | `reports/` | `reports_local/` (via `.env.local`) |
| Typical run | `run_zephyr_scheduled.ps1` | `.\run_zephyr_local.ps1` |

## Commands

```powershell
cd "C:\Users\qa\python_app\zephyr_parser_dev"
.\run_zephyr_local.ps1
.\run_zephyr_local.ps1 --regenerate-last-n-days 1
```

Branches: checkout any branch here; production folder is unaffected until you merge and pull there.

**Do not run** `install_zephyr_scheduled_task.ps1` in this clone.

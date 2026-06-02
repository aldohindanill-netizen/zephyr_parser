# Script Registry

This registry classifies project scripts (excluding `.venv*` and third-party packages) into:

- `keep-prod` - required for production runtime/scheduler flow.
- `keep-manual` - manual/operator or local verification utilities documented in runbooks.
- `deprecate-candidate` - no confirmed external usage in scheduler/CI/runbook.

## keep-prod

- `run_zephyr_scheduled.ps1`
- `run_zephyr.ps1`
- `run_zephyr.cmd`
- `run_zephyr.sh`
- `zephyr_weekly_report.py`
- `run_embeddings_scheduled.ps1`
- `install_zephyr_scheduled_task.ps1`
- `install_zephyr_embeddings_task.ps1`
- `scripts/compute_bug_embeddings.py`
- `scripts/refresh_bugs_rollup_duplicates.py`
- `zephyr_pipeline_health.py`
- `zephyr_audit.py`
- `repo_env.py`
- `bug_duplicate_detection.py`

## keep-manual

- `run_zephyr_local.ps1`
- `run_embeddings_local.ps1`
- `setup_dev_clone.ps1`
- `scripts/confluence_delete_children.py`
- `scripts/debug_jira_description.py`
- `scripts/publish_local_confluence.py`
- `reports_local/logs/run_main_verify.ps1`
- `reports_local/logs/run_embeddings_verify.ps1`
- `tests/test_bug_duplicate_detection.py`
- `tests/test_confluence_publish_titles.py`
- `tests/test_daily_aggregate.py`
- `tests/test_daily_confluence_week.py`
- `tests/test_pipeline_health.py`
- `tests/test_repo_env.py`
- `tests/test_zephyr_security_audit.py`

## deprecate-candidate

- `scripts/deprecated/calibrate_bug_duplicates.py` (archived ad-hoc calibration helper; no documented operational usage)
- `reports_local/logs/_run_main_now.ps1` (duplicate local verify launcher)
- `reports_local/logs/_run_verify.ps1` (duplicate local verify launcher)
- `zephyr_weekly_analytics.py` (compatibility re-export module, not used in runtime chain)

## Decisions Applied

- `scripts/calibrate_bug_duplicates.py` moved to `scripts/deprecated/calibrate_bug_duplicates.py`.

## Review Rule

Delete or archive only scripts from `deprecate-candidate`, and only after checking that:

1. It is not used by scheduler/CI/runtime imports.
2. It is not required by documented manual runbook tasks.

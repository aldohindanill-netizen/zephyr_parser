# NocoDB operator form setup

This setup replaces the former Google Sheet `Run` editing step.

## 1) Connect NocoDB to `zephyr_ops` database

Connect NocoDB to the `zephyr_ops` Postgres database and expose these tables/views:

- `folders`
- `test_runs`
- `test_results`
- `sync_queue`
- `sync_audit`
- `operator_run_form` (view: all test results joined with runs and folders)
- `operator_daily_form` (view: last 7 days of `operator_run_form`)
- `operator_day_summary` (view: per-day/folder aggregates for overview)

## 2) Create NocoDB views

### Overview: Day Summary

Create a Grid view on `operator_day_summary`. This is the landing page showing
aggregated stats per test day and folder:

- `execution_day`, `folder_name`
- `total_cases`, `with_desired_status`
- `pending_sync`, `done_sync`, `failed_sync`
- `last_updated_at`

Freshest days appear first. Use this view to pick which day to work on.

### Per-day drill-down: Run Form

Create a Grid view on `operator_run_form` **grouped by `execution_day`**.
Each group collapses/expands, giving a per-day page effect.

Alternatively, create a Grid view on `operator_daily_form` (already filtered
to the last 7 days) if you only need the recent window.

Column setup:

- editable:
  - `desired_status_id` (single source of truth for Pass/Fail/etc)
  - `desired_comment`
- read-only:
  - `scenario`
  - `test_case_name`
  - `test_case_key`
  - `resolved_case_key`
  - `execution_id`
  - `current_status_name`
  - `pass_checked`
  - `fail_checked`
  - `sync_state`
  - `sync_status`
  - `sync_error`
  - `last_synced_status_name`
  - `last_synced_comment`
  - `synced_at`
  - `updated_at`

Hide technical columns (`id`, internal relation ids, etc.). Keep `test_result_id`
visible for troubleshooting and targeted replays.

### Navigation pattern

1. Open **Day Summary** view to see all test days at a glance.
2. Note the `execution_day` you want to work on.
3. Switch to the **Run Form** view and filter or scroll to that day's group.
4. Edit `desired_status_id` / `desired_comment` for each test case.

## 3) Queue insertion is automatic

Database trigger `trg_enqueue_sync_queue_on_change` is installed by
`infra/sql/init_zephyr_ops.sql`.

Whenever operator edits `desired_status_id` or `desired_comment`,
a queue item is inserted into `sync_queue`.

## 4) Status ID mapping

The view `operator_run_form` computes `pass_checked` and `fail_checked` boolean
columns from `desired_status_id`. Canonical IDs in this repo are `145` (Pass)
and `146` (Fail).

If your Zephyr instance uses different IDs, update all mapping points together:

- `infra/sql/init_zephyr_ops.sql` (`operator_run_form` computed booleans)
- `workflows/grist_to_postgres_sync.json` (`done/not_done -> desired_status_id`)
- `workflows/postgres_to_grist_status_sync.json` (`desired_status_id -> done/not_done`)
- `infra/.env.nocodb-n8n` (`ZEPHYR_PASS_STATUS_ID` / `ZEPHYR_FAIL_STATUS_ID`)

## 5) Realtime path payload contract

If you enable a NocoDB webhook automation to call n8n realtime endpoint, send:

```json
{
  "enable_realtime": true,
  "test_result_id": "res-demo-1",
  "desired_status_id": 145,
  "desired_comment": "Passed on rerun",
  "base_url": "https://jira.navio.auto",
  "zephyr_api_token": "YOUR_REAL_TOKEN",
  "update_endpoint_template": "rest/tests/1.0/testresult/{test_result_id}",
  "update_method": "PUT",
  "status_field": "testResultStatusId",
  "comment_field": "comment"
}
```

Use `145` for Pass and `146` for Fail in realtime payloads.

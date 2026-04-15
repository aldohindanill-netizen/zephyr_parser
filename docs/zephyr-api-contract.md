# Zephyr API Contract for NocoDB + n8n

This document defines the minimal HTTP contract and data model used by local
`NocoDB + n8n` workflows to ingest test data and write operator decisions back
to Zephyr.

## Authentication

- Header name: `Authorization`
- Header value: `Bearer ${ZEPHYR_API_TOKEN}`
- Base URL: `${ZEPHYR_BASE_URL}`

## Read Endpoints

- Search test runs:
  - `GET|POST ${ZEPHYR_ENDPOINT}` (default `rest/tests/1.0/testrun/search`)
  - Query params/body must include project filter and folder id.
- Discover folders:
  - `POST ${ZEPHYR_FOLDER_SEARCH_ENDPOINT}` (default `rest/tests/1.0/folder/search`)

## Write Endpoint

- Update test result:
  - `${ZEPHYR_UPDATE_METHOD} ${ZEPHYR_UPDATE_ENDPOINT_TEMPLATE}`
  - Default template: `rest/tests/1.0/testresult/{test_result_id}`
  - Path variable: `test_result_id` from `test_results.test_result_id`
  - JSON payload fields:
    - `${ZEPHYR_UPDATE_STATUS_ID_FIELD}` (default `testResultStatusId`)
    - `${ZEPHYR_UPDATE_COMMENT_FIELD}` (default `comment`)
    - optional merge object from `${ZEPHYR_UPDATE_EXTRA_BODY_JSON}`

## NocoDB Data Model

All tables can live in one project/database.

### 1) `folders`

- `id` (uuid, pk)
- `folder_id` (text, unique)
- `folder_name` (text)
- `folder_path` (text)
- `is_active` (bool, default true)
- `last_ingested_at` (timestamp)

### 2) `test_runs`

- `id` (uuid, pk)
- `test_run_id` (text, unique)
- `folder_id` (text, fk-like to `folders.folder_id`)
- `cycle_id` (text)
- `run_name` (text)
- `execution_day` (date)
- `source_status_name` (text)
- `last_ingested_at` (timestamp)

### 3) `test_results`

- `id` (uuid, pk)
- `test_result_id` (text, unique, required)
- `test_run_id` (text, fk-like to `test_runs.test_run_id`)
- `test_case_key` (text)
- `test_case_name` (text)
- `current_status_name` (text)
- `desired_status_name` (text, nullable)
- `desired_status_id` (text, nullable)
- `desired_comment` (text, nullable)
- `last_synced_status_name` (text, nullable)
- `last_synced_comment` (text, nullable)
- `sync_state` (text: `pending|in_progress|done|failed|dead_letter`)
- `sync_error` (text, nullable)
- `updated_at` (timestamp)

### 6) Operator Views

- `operator_run_form`:
  - read-optimized joined view over `test_results + test_runs + folders`
  - includes service fields `scenario`, `execution_id`, `resolved_case_key`,
    `sync_status`, `synced_at`, `pass_checked`, `fail_checked`
- `operator_daily_form`:
  - filtered subset of `operator_run_form` for `execution_day = current_date`
  - primary source for day-to-day operator edits in NocoDB

### 4) `sync_queue`

- `id` (uuid, pk)
- `test_result_id` (text, required)
- `operation_type` (text, default `writeback`)
- `operation_hash` (text, required)
- `payload_json` (json)
- `status` (text: `queued|processing|done|failed|dead_letter`)
- `attempt_count` (int, default 0)
- `next_retry_at` (timestamp, nullable)
- `last_error` (text, nullable)
- `created_at` (timestamp)
- `updated_at` (timestamp)

Index/uniqueness:

- Unique composite key: `(test_result_id, operation_hash)`
- This is the idempotency barrier that prevents duplicate writes.

### 5) `sync_audit`

- `id` (uuid, pk)
- `queue_id` (uuid/text)
- `test_result_id` (text)
- `request_method` (text)
- `request_url` (text)
- `request_body_json` (json)
- `response_status` (int)
- `response_body` (text)
- `success` (bool)
- `executed_at` (timestamp)

## Retry/Backoff Policy

- First failure: retry in 1 minute
- Then 2, 4, 8 minutes
- Max attempts: 5
- After max attempts mark queue row `dead_letter` and write full error into
  `sync_audit`

## Reconciliation Contract

Periodic n8n job (for example every 6 hours):

- Read recently changed rows from `test_results`
- Re-fetch status from Zephyr for same `test_result_id`
- If mismatch between Zephyr and `last_synced_*`, create an item in
  `sync_queue` with a fresh operation hash

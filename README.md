# zephyr_parser

CLI utility to fetch Zephyr test executions and build a weekly summary table.

## What it does

- downloads paginated execution data from Zephyr API
- groups runs by week (week starts on Monday)
- calculates totals by status (passed/failed/blocked/not executed/other)
- writes CSV report and prints a console table

## Usage

Create local config file and set real token:

```bash
cp .env.example .env
# edit .env and set ZEPHYR_API_TOKEN
```

Run report via launcher:

```bash
bash ./run_navio_folder_report.sh
```

The launcher runs in tree-first mode by default:

- tries custom tree source first (`ZEPHYR_TREE_SOURCE_*`) when configured
- tries `POST` on `ZEPHYR_FOLDER_SEARCH_ENDPOINT`
- falls back to `GET` on `ZEPHYR_FOLDERTREE_ENDPOINT` (working source from HAR: `rest/tests/1.0/project/10904/foldertree/testrun`)
- selects only matching tree nodes (leaf + regex/path filters)
- fetches executions for each selected folder id

Outputs:

- summary CSV: `ZEPHYR_OUTPUT` (default `weekly_zephyr_report.csv`)
- per-folder CSV files: `ZEPHYR_PER_FOLDER_DIR` (default `reports/by_folder`)
- keep `ZEPHYR_QUERY_TEMPLATE` in quotes in `.env` (contains spaces and parentheses)
- tree-first config for 2026 folders:
  - `ZEPHYR_DISCOVERY_MODE=tree`
  - `ZEPHYR_FOLDERTREE_ENDPOINT=rest/tests/1.0/project/10904/foldertree/testrun`
  - `ZEPHYR_ROOT_FOLDER_IDS=10545`
  - `ZEPHYR_TREE_LEAF_ONLY=true`
  - `ZEPHYR_TREE_NAME_REGEX='^2026\.\d{2}\.\d{2}$'`
  - `ZEPHYR_TREE_ROOT_PATH_REGEX=` (empty)
  - `ZEPHYR_TREE_AUTOPROBE=false`
- when HAR reveals the real backend request, set:
  - `ZEPHYR_TREE_SOURCE_ENDPOINT=...`
  - `ZEPHYR_TREE_SOURCE_METHOD=GET|POST`
  - `ZEPHYR_TREE_SOURCE_QUERY_JSON='{"projectId":10904}'` (optional)
  - `ZEPHYR_TREE_SOURCE_BODY_JSON='{"projectId":10904}'` (optional)
- execution-mode fallback (legacy):
  - `ZEPHYR_DISCOVERY_MODE=executions`
  - `ZEPHYR_PROJECT_QUERY='testRun.projectId IN (10904) ORDER BY testRun.name ASC'`
  - `ZEPHYR_FOLDER_NAME_ENDPOINT_TEMPLATE=rest/tests/1.0/folder/{folder_id}`
- date range can be enforced with:
  - `ZEPHYR_FROM_DATE=2026-01-01`
  - `ZEPHYR_TO_DATE=2026-12-31`
- set `ZEPHYR_DEBUG_FOLDER_FIELDS=true` to print raw folder fields for diagnostics

## Token storage

Use local `.env` plus environment variable `ZEPHYR_API_TOKEN` loaded by launcher.

- Do not pass token via CLI argument (`--token`) in normal usage.
- Do not commit `.env` into git.
- Keep `.env.example` in repo as a safe template.

Windows tip (for manual session export, if needed):

```powershell
[System.Environment]::SetEnvironmentVariable("ZEPHYR_API_TOKEN", "your_token", "User")
```

## Notes

- Default auth header is `Authorization: Bearer <token>`.
- Tree discovery endpoints are configurable via:
  - `ZEPHYR_FOLDER_SEARCH_ENDPOINT`
  - `ZEPHYR_FOLDERTREE_ENDPOINT`
- Set `ZEPHYR_TREE_AUTOPROBE=true` only for diagnostics; keep it `false` in stable mode.
- Folder query template must include `{folder_id}` placeholder.
- If your Zephyr instance uses different fields for date/status, pass custom paths:
  - `--date-field "some.path.to.date"`
  - `--status-field "some.path.to.status"`
- You can pass multiple `--date-field` or `--status-field` values.

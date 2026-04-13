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
- optional detailed CSV `folder -> cycle -> test case`:
  - `ZEPHYR_EXPORT_CYCLES_CASES=true`
  - `ZEPHYR_CYCLES_CASES_OUTPUT=reports/cycles_and_cases.csv`
  - `ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE=rest/tests/1.0/testrun/{cycle_id}/testcase/search`
  - `ZEPHYR_SYNTHETIC_CYCLE_IDS=true` (fills `cycle_id` as `v:{folder_id}:{yyyy-mm-dd}` when API does not return real cycle id)
  - synthetic `cycle_id` is used only in CSV for analytics and is not sent to Zephyr API endpoints
- optional step-level CSV `test case -> steps -> status`:
  - `ZEPHYR_EXPORT_CASE_STEPS=true`
  - `ZEPHYR_CASE_STEPS_OUTPUT=reports/case_steps.csv`
  - uses Zephyr endpoints discovered from HAR:
    - `rest/tests/1.0/testrun/{test_run_id}/testrunitems`
    - `rest/tests/1.0/testrun/{test_run_id}/testresults?itemId=...`
    - `rest/tests/1.0/project/{project_id}/testresultstatus`
- readable daily reports for Confluence (one file per folder/day):
  - `ZEPHYR_EXPORT_DAILY_READABLE=true`
  - `ZEPHYR_DAILY_READABLE_DIR=reports/daily_readable`
  - `ZEPHYR_DAILY_READABLE_FORMATS=html,wiki`
  - enabling daily readable also fetches step-level data internally (same API calls as case steps); set `ZEPHYR_EXPORT_CASE_STEPS=true` if you also want `reports/case_steps.csv`
  - report title format:
    - `Daily report: nightly-dev-<folder_name> (<most_popular_step_execution_date>)`
  - each test cycle section lists real test case keys (`QA-T...`) with result (step/execution status), execution date, and comment from the first non-empty step comment
  - output files:
    - `nightly-dev-<slug(folder_name)>_<most_popular_step_execution_date>_<folder_id>.html` (copy/paste into Confluence editor)
    - `nightly-dev-<slug(folder_name)>_<most_popular_step_execution_date>_<folder_id>.confluence.txt` (wiki-markup format)
- weekly cycle matrix CSV (built from daily cycle summaries of target week):
  - `ZEPHYR_WEEKLY_CYCLE_MATRIX_OUTPUT=reports/weekly_cycle_matrix.csv`
  - columns:
    - `Тестовый цикл` (`cycle_key | cycle_name`)
    - `Всего кейсов` (max value across daily summaries of the week for this cycle)
    - dynamic columns `nightly-dev-YYYY.MM.DD`
  - source for weekly columns: daily “Сводка по тестовым циклам”
  - daily summaries are joined by `Тестовый цикл`; missing values are filled as `0`
  - labels with `(...cloned...)` are merged into base cycle label; base cycle has priority, cloned is fallback
  - week selection is based on date from daily report title (`most_popular_step_execution_date`); latest week is exported
- weekly readable reports for Confluence/HTML:
  - `ZEPHYR_EXPORT_WEEKLY_READABLE=true`
  - `ZEPHYR_WEEKLY_READABLE_DIR=reports/weekly_readable`
  - `ZEPHYR_WEEKLY_READABLE_FORMATS=html,wiki`
  - output files:
    - `weekly_cycle_matrix_<week_start>.html`
    - `weekly_cycle_matrix_<week_start>.confluence.txt`
- optional auto-publish to Confluence (creates new pages, skips existing titles):
  - `CONFLUENCE_PUBLISH_DAILY=true` (publishes each generated daily HTML report)
  - `CONFLUENCE_PUBLISH_WEEKLY=true` (publishes generated weekly HTML report)
  - required:
    - `CONFLUENCE_BASE_URL` (Cloud example: `https://<org>.atlassian.net/wiki`)
    - `CONFLUENCE_SPACE_KEY`
    - `CONFLUENCE_PARENT_PAGE_ID`
    - `CONFLUENCE_API_TOKEN`
  - auth mode:
    - `CONFLUENCE_AUTH_MODE=auto|basic|bearer` (default: `auto`)
    - `bearer` is recommended for Confluence Server/Data Center when PAT works as `Authorization: Bearer ...`
    - for `basic` set `CONFLUENCE_USERNAME` (Cloud: Atlassian account email)
  - optional:
    - `CONFLUENCE_VERIFY_SSL=true|false`
    - `CONFLUENCE_DRY_RUN=true|false` (prints intended actions without API calls)
  - title format used for new pages:
    - daily: `Daily report: nightly-dev-<slug(folder_name)>_<most_popular_step_execution_date>_<folder_id>`
    - weekly: `Weekly cycle matrix: <week_start>`
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
- Do not commit `CONFLUENCE_API_TOKEN` to git.

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

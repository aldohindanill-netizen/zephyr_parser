# zephyr_parser

CLI-утилита и Redis-воркер для генерации отчётов по тест-экзекьюшенам Zephyr.

Связанный репозиторий: **[zephyr-bot](https://github.com/aldohindanill-netizen/zephyr-bot)** — Telegram-бот для ввода результатов.

---

## Что делает

- Скачивает пагинированные данные экзекьюшенов из Zephyr API
- Обнаруживает дерево папок (tree-режим) или выводит папки из экзекьюшенов
- Агрегирует по ISO-неделям (понедельник) и считает totals по статусам
- Экспортирует CSV-отчёты и HTML/wiki-страницы для Confluence
- Предоставляет Redis-воркер (`redis_runner.py`) для управления запусками через очередь

---

## Запуск локально

```bash
cp .env.example .env
# установить ZEPHYR_API_TOKEN и другие переменные

bash ./run_navio_folder_report.sh
```

Run Google Sheets pipeline launcher:

```bash
bash ./run_zephyr_google_pipeline.sh --help
```

The launcher runs in tree-first mode by default:

- пробует кастомный источник (`ZEPHYR_TREE_SOURCE_*`) если настроен
- делает `POST` на `ZEPHYR_FOLDER_SEARCH_ENDPOINT`
- fallback: `GET` на `ZEPHYR_FOLDERTREE_ENDPOINT`
- выбирает совпадающие узлы (leaf + regex/path фильтры)
- скачивает экзекьюшены для каждой папки

---

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
    - `<folder_name>_<folder_id>.html` (copy/paste into Confluence editor)
    - `<folder_name>_<folder_id>.confluence.txt` (wiki-markup format)
- readable weekly reports:
  - `ZEPHYR_EXPORT_WEEKLY_READABLE=true`
  - `ZEPHYR_WEEKLY_READABLE_DIR=reports/weekly_readable`
  - `ZEPHYR_WEEKLY_READABLE_FORMATS=html,wiki`
  - when Autofleet AB-test branch is resolved from Jira (`labels = autofleet_abtest`, build from `description` point `A`), weekly render adds a first column/card `Лучшая ветка: <branch>` in both `Общий score` and `Score по сценариям`
  - insertion rule is strict: only for weeks with `week_start < best_branch_week_start` (week of that branch and later weeks are not modified)
- Jira Autofleet AB-test extraction helpers (for downstream automation):
  - `ZEPHYR_AUTOFLEET_ABTEST_ENABLED=true`
  - `ZEPHYR_AUTOFLEET_ABTEST_JQL='labels = autofleet_abtest'`
  - `ZEPHYR_AUTOFLEET_ABTEST_MAX_RESULTS=100`
  - helper logic parses date from issue `summary`, compares it with Jira `created`, picks latest issue by max(date), then extracts build name from point `A` in `description` (`A)`, `A.`, `A:`, `А)` variants).
  - by current behavior this value is not rendered into weekly HTML/wiki yet (explicitly disabled for now).
- optional first step: create target Zephyr folder before report generation:
  - `ZEPHYR_CREATE_FOLDER_FIRST=true`
  - set one of:
    - `ZEPHYR_CREATE_FOLDER_NAME=2026.04.14`
    - `ZEPHYR_CREATE_FOLDER_NAME_TEMPLATE=%Y.%m.%d`
  - optional placement and endpoint/body mapping:
    - `ZEPHYR_CREATE_FOLDER_PARENT_ID=...`
    - `ZEPHYR_CREATE_FOLDER_ENDPOINT=rest/tests/1.0/folder`
    - `ZEPHYR_CREATE_FOLDER_NAME_FIELD=name`
    - `ZEPHYR_CREATE_FOLDER_PROJECT_ID_FIELD=projectId`
    - `ZEPHYR_CREATE_FOLDER_PARENT_ID_FIELD=parentId`
    - `ZEPHYR_CREATE_FOLDER_BODY_JSON='{"type":"TEST_RUN"}'` (example)
  - optional safe mode:
    - `ZEPHYR_CREATE_FOLDER_DRY_RUN=true` (prints payload, no POST)
  - optional scope override:
    - `ZEPHYR_CREATE_FOLDER_USE_AS_ROOT=true` (use created/existing folder as the only root filter in current run)
- Google Sheets daily pipeline:
  - service account setup:
    - create Google Cloud service account
    - enable Google Sheets API + Google Drive API
    - download JSON key and set `GOOGLE_SERVICE_ACCOUNT_FILE=/abs/path/key.json`
    - share spreadsheet (or parent Drive folder) with service account email
  - install dependencies:
    - `pip install google-api-python-client google-auth`
  - generate/update sheet from Zephyr folder:
    - `ZEPHYR_GSHEET_BRANCH_NAME_TEMPLATE=%Y.%m.%d` (or set fixed `ZEPHYR_BRANCH_NAME`)
    - `ZEPHYR_GSHEET_SPREADSHEET_ID=` (empty to auto-create)
    - run: `bash ./run_zephyr_google_pipeline.sh generate`
  - sync checked Pass/Fail + Comment back to Zephyr:
    - `ZEPHYR_GSHEET_SPREADSHEET_ID=<existing_sheet_id>`
    - run: `bash ./run_zephyr_google_pipeline.sh sync`
  - realtime on checkbox edit:
    - use Apps Script from `google_apps_script/Code.gs`
    - set script properties:
      - `ZEPHYR_BASE_URL`
      - `ZEPHYR_API_TOKEN`
      - optional: `RUN_SHEET_NAME` (default `Run`)
      - optional: `UPDATE_ENDPOINT_TEMPLATE` (default `rest/tests/1.0/testresult/{test_result_id}`)
      - optional: `UPDATE_METHOD` (default `PUT`)
      - optional: `UPDATE_STATUS_ID_FIELD` (default `testResultStatusId`)
      - optional: `UPDATE_COMMENT_FIELD` (default `comment`)
    - behavior:
      - Pass/Fail are mutually exclusive
      - on Pass/Fail/Comment edit Apps Script reads `pass_status_id/fail_status_id` from `Config`
      - writes status + comment directly to Zephyr API
      - writes result to columns M:N (`sync_status`, `synced_at`)
- new helper scripts:
  - `zephyr_google_sheets_pipeline.py`
    - `generate-sheet`: creates/updates Config + Run tabs with grouped daily rows
    - `sync-sheet`: sends Pass/Fail + Comment into Zephyr update endpoint
  - `run_zephyr_google_pipeline.sh`: environment-based launcher for both modes
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

## Выходные файлы

| Переменная | Путь по умолчанию | Содержимое |
|------------|------------------|-----------|
| `ZEPHYR_OUTPUT` | `weekly_zephyr_report.csv` | Сводный отчёт по неделям |
| `ZEPHYR_PER_FOLDER_DIR` | `reports/by_folder/` | Отчёт по каждой папке |
| `ZEPHYR_CYCLES_CASES_OUTPUT` | `reports/cycles_and_cases.csv` | Папка → цикл → кейс |
| `ZEPHYR_CASE_STEPS_OUTPUT` | `reports/case_steps.csv` | Кейс → шаги → статус |
| `ZEPHYR_DAILY_READABLE_DIR` | `reports/daily_readable/` | HTML и wiki для Confluence |

---

- Do not pass token via CLI argument (`--token`) in normal usage.
- Do not commit `.env` into git.
- Keep `.env.example` in repo as a safe template.
- Do not commit `CONFLUENCE_API_TOKEN` to git.

## Деплой на Amvera (Redis-воркер)

`redis_runner.py` — постоянно работающий воркер. Принимает задания из Redis-очереди и выполняет одно из трёх действий:

| `action` | Что делает |
|----------|-----------|
| `run_report` | Запускает полный пайплайн отчёта, пишет CSV в `/data` |
| `list_folders` | Возвращает дерево папок как JSON-массив |
| `upload_result` | POST/PUT результат тест-кейса обратно в Zephyr |

### Деплой

1. Создать **преднастроенный сервис Redis** в Amvera.  
   В разделе «Переменные» добавить секрет `REDIS_ARGS=--requirepass <пароль>`.

2. Создать **проект приложения** из этого репозитория.  
   В разделе «Переменные» добавить:

   | Переменная | Тип | Значение |
   |------------|-----|---------|
   | `ZEPHYR_API_TOKEN` | секрет | токен Zephyr API |
   | `ZEPHYR_BASE_URL` | переменная | `https://jira.example.com` |
   | `ZEPHYR_PROJECT_ID` | переменная | `10904` |
   | `ZEPHYR_ROOT_FOLDER_IDS` | переменная | `10545` |
   | `REDIS_HOST` | переменная | `amvera-<логин>-run-<имя-redis>` |
   | `REDIS_PASSWORD` | секрет | пароль Redis |

3. После сохранения переменных — **перезапустить** контейнер.

### Отправить задание из клиента

```python
import json, redis

r = redis.Redis(host="amvera-user-run-my-redis", port=6379,
                password="...", decode_responses=True)

# Запустить отчёт
r.lpush("zephyr:jobs", json.dumps({
    "action": "run_report",
    "job_id": "r01",
    "ZEPHYR_FROM_DATE": "2026-04-01",
    "ZEPHYR_TO_DATE":   "2026-04-30",
}))

# Получить результат
_, raw = r.blpop("zephyr:results")
print(json.loads(raw)["exit_code"])   # 0 = успех
```

### Redis env vars воркера

| Переменная | По умолчанию | Описание |
|------------|-------------|---------|
| `REDIS_HOST` | `localhost` | хост Redis |
| `REDIS_PORT` | `6379` | порт |
| `REDIS_PASSWORD` | — | пароль |
| `REDIS_DB` | `0` | номер БД |
| `REDIS_JOB_QUEUE` | `zephyr:jobs` | ключ очереди заданий |
| `REDIS_RESULT_KEY` | `zephyr:results` | ключ списка результатов |
| `REDIS_RESULT_CHANNEL` | `zephyr:done` | pub/sub канал |
| `REDIS_RESULT_TTL` | `3600` | TTL результатов (сек) |
| `REDIS_HEARTBEAT_KEY` | `zephyr:heartbeat` | ключ heartbeat |
| `REDIS_HEARTBEAT_INTERVAL` | `30` | интервал heartbeat (сек) |

---

## Хранение токена

- Используйте `.env` + переменная `ZEPHYR_API_TOKEN`, загружаемая лаунчером.
- Не передавайте токен через `--token` в обычном использовании.
- Не коммитьте `.env` в git.
- `.env.example` — безопасный шаблон, хранится в репозитории.

---

## Устранение неполадок

### TelegramConflictError: can't use getUpdates while webhook is active

Если Telegram-бот падает с ошибкой вида:

```
TelegramConflictError: Conflict: can't use getUpdates method while webhook is active;
use deleteWebhook to delete the webhook first
```

Это значит, что ранее для бота был зарегистрирован webhook, и теперь он мешает работе в режиме polling.

**Быстрое решение — удалить webhook один раз:**

```bash
TELEGRAM_BOT_TOKEN=<токен> python delete_webhook.py
```

Или через curl:

```bash
curl "https://api.telegram.org/bot<TOKEN>/deleteWebhook?drop_pending_updates=true"
```

**Постоянное решение** — добавить удаление webhook в стартап бота перед запуском polling.
Для aiogram 3.x это делается через `await bot.delete_webhook(drop_pending_updates=True)`
перед вызовом `await dp.start_polling(bot)`:

```python
import asyncio
from aiogram import Bot, Dispatcher

BOT_TOKEN = "YOUR_BOT_TOKEN"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
```

Это гарантирует, что даже если ранее был зарегистрирован webhook (например, при деплое
на Amvera или в другой webhook-среде), при каждом запуске бот корректно переключится
в режим polling без `TelegramConflictError`.

---

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
- For Zephyr write-back endpoint tune:
  - `ZEPHYR_GSHEET_UPDATE_ENDPOINT_TEMPLATE` (default `rest/tests/1.0/testresult/{test_result_id}`)
  - `ZEPHYR_GSHEET_UPDATE_METHOD` (`PUT|POST|PATCH`)
  - `ZEPHYR_GSHEET_UPDATE_STATUS_ID_FIELD` (default `testResultStatusId`)
  - `ZEPHYR_GSHEET_UPDATE_COMMENT_FIELD` (default `comment`)
  - `ZEPHYR_GSHEET_UPDATE_EXTRA_BODY_JSON` (optional extra JSON object)

## NocoDB + n8n local migration

Google Sheets flow can be migrated to a local operations stack:

- `NocoDB` for operator edits
- `n8n` for ingest/sync orchestration
- `Postgres` for state and queue persistence

Migration assets in this repo:

- `infra/docker-compose.nocodb-n8n.yml`
- `infra/.env.nocodb-n8n.example`
- `workflows/zephyr_ingest_15m.json`
- `workflows/zephyr_writeback_15m.json`
- `workflows/zephyr_writeback_realtime.json` (optional path, disabled by default)
- `docs/zephyr-api-contract.md`
- `docs/nocodb-operator-form.md`
- `docs/n8n-postgres-credential.md`
- `docs/production-cutover-checklist.md`

### 1) Bootstrap local stack

```bash
cp infra/.env.nocodb-n8n.example infra/.env.nocodb-n8n
# edit secrets and token
docker compose --env-file infra/.env.nocodb-n8n -f infra/docker-compose.nocodb-n8n.yml up -d
```

Open:

- n8n: `http://localhost:5678`
- NocoDB: `http://localhost:8080`

### 2) Import n8n workflows

Import JSON workflows from `workflows/` in this order:

1. `zephyr_ingest_15m.json`
2. `zephyr_writeback_15m.json`
3. `zephyr_writeback_realtime.json` (keep inactive for prod baseline)

Set environment variables in n8n container from `infra/.env.nocodb-n8n`.

### 3) Create NocoDB tables

Create tables according to `docs/zephyr-api-contract.md`:

- `folders`
- `test_runs`
- `test_results`
- `sync_queue`
- `sync_audit`

Required idempotency constraint:

- unique key on `sync_queue(test_result_id, operation_hash)`

Operator form details:

- use `operator_daily_form` as daily editable source (`execution_day = current_date`)
- keep `desired_status_id` as the single editable status field (Pass/Fail is derived)
- queue items are auto-created by DB trigger on desired status/comment changes
- see `docs/nocodb-operator-form.md`

### 4) Production baseline mode

Use batch mode first:

- `SYNC_INTERVAL_MIN=15`
- `SYNC_MODE=batch`
- `ENABLE_REALTIME_SYNC=false`

This keeps write-back controlled and replayable via queue rows.

Batch workflow DB wiring:

- workflow `workflows/zephyr_writeback_15m.json` uses Postgres nodes for queue read/write
- create n8n credential `PostgresZephyrOps` as described in `docs/n8n-postgres-credential.md`

### Operations runbook

- Pause sync:
  - disable `zephyr_writeback_15m` workflow in n8n
- Resume sync:
  - re-enable workflow
- Replay failed queue:
  - in NocoDB move selected `sync_queue.status` from `failed` to `queued`
  - clear `next_retry_at` for immediate next cycle
- Dead-letter handling:
  - inspect `sync_audit.response_body`
  - fix payload/data mapping
  - enqueue a new operation with a fresh `operation_hash`
- Monitoring SQL (Postgres):
  - `infra/sql/query_sync_health.sql` - queue/state snapshot + 24h success rate
  - `infra/sql/query_sync_recent_errors.sql` - recent non-2xx / failed attempts
  - `infra/sql/query_sync_sla.sql` - backlog, latency p50/p95/p99, retry and dead-letter rates
  - `infra/sql/query_sync_audit_inconsistencies.sql` - success flag vs HTTP status mismatches
  - `infra/sql/fix_sync_audit_success_flag.sql` - normalize historical `sync_audit.success`
  - `infra/sql/query_sync_health_since_cutover.sql` - clean snapshot from cutover timestamp
  - `infra/sql/query_sync_recent_errors_since_cutover.sql` - errors from cutover timestamp

### Grist helper scripts

Use these PowerShell helpers from repo root:

- Offline check (no VPN, validates `Grist -> Postgres -> sync_queue`):
  - `.\scripts\offline-sync-check.ps1 -TestResultId 112522`
- Post-VPN smoke (final hop validation to Zephyr via queue/writeback):
  - `.\scripts\post-vpn-smoke.ps1 -TestResultId 112522 -WaitSeconds 120`

Both scripts support optional overrides:

- `-EnvFile infra/.env.nocodb-n8n`
- `-ComposeFile infra/docker-compose.nocodb-n8n.yml`

VPN continuation checklist:

- `docs/vpn-resume-checklist.md`

### Optional near-realtime path

When ready:

- set `ENABLE_REALTIME_SYNC=true`
- activate `zephyr_writeback_realtime`
- configure NocoDB automation/webhook to send `test_result_id`, `desired_status_id`, `desired_comment`

Keep the 15-minute batch workflow active as fallback.

### Rollback to script mode

If migration flow is degraded:

1. Disable n8n write-back workflows (`zephyr_writeback_15m` and realtime).
2. Continue operations via existing script launcher:
   - `bash ./run_zephyr_google_pipeline.sh generate`
   - `bash ./run_zephyr_google_pipeline.sh sync`
3. Keep NocoDB as read-only until queue/audit issues are fixed.

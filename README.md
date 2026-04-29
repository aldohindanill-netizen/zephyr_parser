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

Launcher запускает в tree-режиме:

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
    - `CONFLUENCE_UPDATE_EXISTING=true|false` (when true, existing pages with same title are updated instead of skipped)
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

- Заголовок авторизации по умолчанию: `Authorization: Bearer <token>`.
- Эндпоинты обнаружения папок настраиваются через `ZEPHYR_FOLDER_SEARCH_ENDPOINT` / `ZEPHYR_FOLDERTREE_ENDPOINT`.
- `ZEPHYR_TREE_AUTOPROBE=true` — только для диагностики.
- `ZEPHYR_QUERY_TEMPLATE` должен содержать плейсхолдер `{folder_id}`.
- Для кастомных полей даты/статуса: `--date-field` / `--status-field` (можно передавать несколько).

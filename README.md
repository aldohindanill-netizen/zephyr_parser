# zephyr_parser

CLI для генерации отчётов по тест-экзекьюшенам Zephyr (Jira-hosted API): CSV, HTML/wiki для Confluence, weekly matrix.

Связанный репозиторий: **[zephyr-bot](https://github.com/aldohindanill-netizen/zephyr-bot)** — Telegram-бот для ввода результатов (отдельный проект).

**Стек:** Windows CMD / bash → launcher (`run_zephyr.*`) → Python 3.10+ (stdlib) → REST Jira/Zephyr (+ опционально Confluence). Внешние pip-зависимости не требуются.

---

## Что делает

- Скачивает пагинированные данные экзекьюшенов из Zephyr API
- Обнаруживает дерево папок (tree-режим) или папки из экзекьюшенов
- Агрегирует по ISO-неделям и считает totals по статусам
- Экспортирует CSV, daily/weekly readable (HTML + wiki), build-log страницы
- Опционально публикует отчёты в Confluence через REST

---

## Запуск

```bash
cp .env.example .env
# установить ZEPHYR_API_TOKEN и другие переменные
```

### Windows (ярлык / cmd)

```cmd
run_zephyr.cmd
```

Цепочка: `run_zephyr.cmd` → `run_zephyr.ps1` → `python -u zephyr_weekly_report.py`

### PowerShell

```powershell
.\run_zephyr.ps1
```

### Linux / macOS

```bash
chmod +x run_zephyr.sh
./run_zephyr.sh
```

Python: `PYTHON_BIN`, затем `python3`, затем `python` (Windows: `py -3`).

### Task Scheduler (каждые 30 минут)

```powershell
.\install_zephyr_scheduled_task.ps1
# test: Start-ScheduledTask -TaskName ZephyrParserEvery30Min
```

- Wrapper: `run_zephyr_scheduled.ps1` → `run_zephyr.ps1` → `zephyr_weekly_report.py`
- Wrapper log: `reports/logs/scheduled_YYYY-MM-DD.log`
- Python log: `logs/zephyr_YYYY-MM-DD_HH-MM-SS.log`
- Lock: `ZEPHYR_RUN_LOCK_FILE` (default `reports/.zephyr_weekly_report.lock`)
- Timeout: `ZEPHYR_RUN_TIMEOUT_MINUTES` (default 90)

---

## Tree discovery (по умолчанию)

1. Кастомный источник (`ZEPHYR_TREE_SOURCE_*`), если настроен
2. `POST` на `ZEPHYR_FOLDER_SEARCH_ENDPOINT`
3. Fallback: `GET` на `ZEPHYR_FOLDERTREE_ENDPOINT`
4. Фильтры leaf + regex/path + окно дат (`ZEPHYR_REGENERATE_LAST_N_DAYS`)
5. Параллельная загрузка папок (`ZEPHYR_FOLDER_WORKERS`, `ZEPHYR_DETAIL_WORKERS`)

Держите `ZEPHYR_QUERY_TEMPLATE` в кавычках в `.env` (пробелы и скобки).

---

## Выходные файлы

| Переменная | Путь по умолчанию | Содержимое |
|------------|-------------------|------------|
| `ZEPHYR_OUTPUT` | `weekly_zephyr_report.csv` | Сводный отчёт по неделям |
| `ZEPHYR_PER_FOLDER_DIR` | `reports/by_folder/` | CSV по каждой папке |
| `ZEPHYR_CYCLES_CASES_OUTPUT` | `reports/cycles_and_cases.csv` | Папка → цикл → кейс |
| `ZEPHYR_CASE_STEPS_OUTPUT` | `reports/case_steps.csv` | Кейс → шаги → статус |
| `ZEPHYR_CYCLE_PROGRESS_OUTPUT` | `reports/cycle_progress.csv` | Прогресс по циклам |
| `ZEPHYR_WEEKLY_CYCLE_MATRIX_OUTPUT` | `reports/weekly_cycle_matrix.csv` | Недельная матрица (+ dated copies) |
| `ZEPHYR_DAILY_READABLE_DIR` | `reports/daily_readable/` | HTML и wiki для Confluence |
| `ZEPHYR_WEEKLY_READABLE_DIR` | `reports/weekly_readable/` | Недельные HTML/wiki |
| `ZEPHYR_BUILD_LOG_REPORT_DIR` | `reports/build_log_reports/` | Build-log по Jira issue |

Шаблоны: `report_templates/readable/`

---

## Confluence publish

Включить в `.env`:

- `ZEPHYR_CONFLUENCE_PUBLISH_DAILY=true` и/или `ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY=true`
- `ZEPHYR_CONFLUENCE_BASE_URL`, `ZEPHYR_CONFLUENCE_API_TOKEN`, `ZEPHYR_CONFLUENCE_SPACE_KEY`
- Для basic auth: `ZEPHYR_CONFLUENCE_USER`

`run_zephyr.ps1` выставляет `ZEPHYR_CONFLUENCE_AUTH_SCHEME=bearer` для процесса Python.

---

## Безопасность

- Не передавайте токен через `--token` в обычном использовании — только `ZEPHYR_API_TOKEN` в `.env`.
- Не коммитьте `.env` в git.
- `.env.example` — безопасный шаблон в репозитории.

---

## Notes

- Default auth: `Authorization: Bearer <token>`.
- `ZEPHYR_TREE_AUTOPROBE=true` — только для диагностики.
- Шаблон запроса папки должен содержать `{folder_id}`.
- Daemon loop: `ZEPHYR_LOOP_INTERVAL_MINUTES` (см. `deploy/zephyr-weekly-report.service.example`).

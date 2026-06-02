# zephyr_parser

**Версия:** 1.5.0 (`PIPELINE_VERSION`)

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
cp .env.secrets.example .env.secrets
# заполнить секреты в .env.secrets
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

Python: `PYTHON_BIN`, затем `python3`, затем `python` (Windows: `py -3`). Для ручных команд и тестов на Windows используйте **`py -3`**, не `python` (в PATH может быть Python 3.6).

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

## Локальная отладка

### Отдельный dev-клон (рекомендуется для веток)

Второй каталог на диске = отдельная рабочая копия: можно переключать ветки и ломать код, **не трогая** production-папку, откуда ходит Task Scheduler.

Из production-репозитория (один раз):

```powershell
.\setup_dev_clone.ps1 -CopyEnv
```

По умолчанию клонирует в соседнюю папку `..\zephyr_parser_dev` (тот же GitHub `origin`). При `-CopyEnv` копируются и `.env`, и `.env.secrets`; изоляция путей — через `.env.local`.

```powershell
cd ..\zephyr_parser_dev
# заполнить sandbox page id в .env.local
.\run_zephyr_local.ps1
```

| | `zephyr_parser` (production) | `zephyr_parser_dev` |
|---|------------------------------|---------------------|
| Task Scheduler | да | **нет** (не запускать `install_zephyr_scheduled_task.ps1`) |
| `git checkout` feature | влияет на Scheduler | безопасно |
| Отчёты | `reports/` | `reports_local/` |

Свой URL (форк на GitHub): `.\setup_dev_clone.ps1 -RemoteUrl https://github.com/YOU/zephyr_parser_dev.git -CopyEnv`

Подробности после установки: `DEV_CLONE.md` в dev-клоне.

### Тот же каталог, другие пути (`run_zephyr_local.ps1`)

Отладка на этой же машине **без влияния** на Task Scheduler и каталог `reports/`:

1. Скопировать шаблон: `Copy-Item .env.local.example .env.local`
2. В `.env.local` указать `ZEPHYR_CONFLUENCE_PARENT_PAGE_ID` sandbox-страницы (токены остаются в `.env.secrets`)
3. Запускать **только** локальный launcher:

```powershell
.\run_zephyr_local.ps1
```

Цепочка: `run_zephyr_local.ps1` → `run_zephyr.ps1 -UseLocalEnv` → загрузка `.env` + `.env.secrets` + overlay `.env.local`.

| Что изолировано | Production (Scheduler) | Local debug |
|-----------------|------------------------|-------------|
| Env-файл | `.env` + `.env.secrets` | `.env` + `.env.secrets` + `.env.local` |
| Отчёты | `reports/` | `reports_local/` |
| Lock | `reports/.zephyr_weekly_report.lock` | `reports_local/.zephyr_weekly_report.lock` |
| Confluence | `ZEPHYR_CONFLUENCE_PARENT_PAGE_ID` из `.env` | sandbox id + `[LOCAL]` prefix |

**Не использовать** `.\run_zephyr.ps1` для отладки — перезапишет production `reports/` и может опубликовать в prod Confluence.

Быстрый прогон за 1 день:

```powershell
.\run_zephyr_local.ps1 --regenerate-last-n-days 1
```

Только файлы, без Confluence: в `.env.local` выставить `ZEPHYR_CONFLUENCE_PUBLISH_DAILY=false`, `ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY=false`, `ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY_ANALYTICS=false`.

Проверка HTML: открыть `reports_local/daily_readable/*.html` в браузере.

Скрипты в `scripts/` читают `.env`; для dev-клона добавьте **`--use-local-env`** (подхватит `.env.local` / `reports_local/`):

```powershell
py -3 scripts/refresh_bugs_rollup_duplicates.py --use-local-env
py -3 scripts/compute_bug_embeddings.py --from-rollup-dir reports_local/bugs_rollup --use-local-env
```

Частичная отладка: `py -3 scripts/debug_jira_description.py CSD-12345 --use-local-env`, `py -3 -m unittest discover -s tests`.

### Ревизия скриптов

Актуальная классификация скриптов хранится в `docs/script_registry.md` (`keep-prod`, `keep-manual`, `deprecate-candidate`).

Экспериментальный калибровочный скрипт перенесен в архив:

- `scripts/deprecated/calibrate_bug_duplicates.py`

### Разработка (feature-ветка)

```powershell
git checkout -b feature/v1.4-full-roadmap
# ... коммиты ...
# PR → main, затем git pull в production zephyr_parser
```

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
| `ZEPHYR_WEEKLY_READABLE_DIR` | `reports/weekly_readable/` | Недельные HTML/wiki (операционный отчёт) |
| `ZEPHYR_WEEKLY_ANALYTICS_DIR` | `reports/weekly_analytics/` | Аналитика: тренд, rolling, по неделям |
| `ZEPHYR_BUILD_LOG_REPORT_DIR` | `reports/build_log_reports/` | Build-log по Jira issue |

Шаблоны: `report_templates/readable/`

---

## Confluence publish

Включить в `.env` и `.env.secrets`:

- `ZEPHYR_CONFLUENCE_PUBLISH_DAILY=true`, `ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY=true` и/или `ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY_ANALYTICS=true`
- `ZEPHYR_CONFLUENCE_PUBLISH_BUGS=true` — build-log по Jira issue (по умолчанию как weekly, если не задано)
- Для analytics: одна страница с фиксированным title (`ZEPHYR_CONFLUENCE_WEEKLY_ANALYTICS_TITLE`)
- `ZEPHYR_CONFLUENCE_BASE_URL`, `ZEPHYR_CONFLUENCE_SPACE_KEY` в `.env`
- `ZEPHYR_CONFLUENCE_API_TOKEN` (и при basic auth `ZEPHYR_CONFLUENCE_USER`) в `.env.secrets`
- `ZEPHYR_CONFLUENCE_PARENT_PAGE_ID` — корневая страница в Confluence
- Для basic auth: `ZEPHYR_CONFLUENCE_USER`

Под корневой страницей (`ZEPHYR_CONFLUENCE_PARENT_PAGE_ID`):

| Уровень | Содержимое |
|---------|------------|
| **Week YYYY-wNN** (по умолчанию `Week {year}-w{week:02d}`, шаблон `ZEPHYR_CONFLUENCE_WEEK_FOLDER_TITLE_TEMPLATE`) | Daily-отчёты за дни этой ISO-недели + weekly matrix за ту же неделю |
| **Баги** (`ZEPHYR_CONFLUENCE_BUGS_PARENT_TITLE`) | Папка-контейнер; внутри страница **Сводка багов** (заголовок h1 «Баги», два раздела weekly + Summary) и build-log по Jira issue |

Неделя для daily берётся из даты в имени файла; для weekly — из `weekly_cycle_matrix_YYYY-MM-DD` в имени файла.

`run_zephyr.ps1` выставляет `ZEPHYR_CONFLUENCE_AUTH_SCHEME=bearer` для процесса Python.

### Очистка дерева под корневой страницей

Скрипт [`scripts/confluence_delete_children.py`](scripts/confluence_delete_children.py) рекурсивно удаляет **все дочерние** страницы под `ZEPHYR_CONFLUENCE_PARENT_PAGE_ID` (сама корневая страница не удаляется). Страницы попадают в корзину Confluence. Флаги `ZEPHYR_CONFLUENCE_PUBLISH_*` могут быть `false` — нужны только URL, токен, space и parent id (и `USER` для basic auth).

```powershell
# План удаления (по умолчанию)
python scripts/confluence_delete_children.py

# Реальное удаление
python scripts/confluence_delete_children.py --execute

# Другой родитель
python scripts/confluence_delete_children.py --parent-page-id 123456 --execute -v
```

Папка «Баги» этим скриптом не затрагивается (удаляются только прямые дети корневой страницы).

Сводка багов: одна страница `reports/bugs_rollup/bugs_index.html`.

- **Последние N недель** (`ZEPHYR_BUGS_ROLLUP_LAST_WEEKS=4` в prod): данные из текущего rolling-окна (`ZEPHYR_REGENERATE_LAST_N_DAYS`).
- **Все заведённые баги**: накопительный snapshot `reports/bugs_rollup/defect_analytics_snapshot.json` (обновляется каждым прогоном; при первом запуске можно подтянуть ключи из `build_log_reports/`). В HTML/Confluence матрица и сводка по билдам ограничены последними `ZEPHYR_BUGS_ROLLUP_ALL_MAX_BUILD_COLUMNS` (по умолчанию 52); полный список ключей и счётчики `Кейсов`/`Билдов` — в snapshot. Merge в snapshot — **max** по ячейкам (high-water mark: повторный прогон не уменьшает счётчики). Jira metadata и duplicate detection используют union ключей из snapshot + текущего окна (растёт с историей).

Разовый backfill snapshot без Zephyr:

```bash
py -3 scripts/backfill_bugs_rollup_snapshot.py
```

Полный backfill за период `ZEPHYR_FROM_DATE`..`ZEPHYR_TO_DATE` (долгий прогон):

```bash
py -3 scripts/backfill_bugs_rollup_snapshot.py --mode full
```

### Возможные дубликаты багов

В таблице сводки добавлена колонка **«Возможно дубль»** (ссылка на другой Jira-ключ).

- Rule-first: основной авто-сигнал — совпадение **Expected result** и **Actual result** из Jira `description`: `min(expected_sim, actual_sim) >= ZEPHYR_BUGS_DUPLICATE_TEXT_THRESHOLD` (по умолчанию **0.78**).
- Для пограничных сценариев (например похожий `actual`, немного слабее `expected`) есть soft-gate: `expected_sim >= ZEPHYR_BUGS_DUPLICATE_TEXT_SOFT_EXPECTED_THRESHOLD` и `actual_sim >= ZEPHYR_BUGS_DUPLICATE_TEXT_SOFT_ACTUAL_THRESHOLD` при совпадении доменных тегов.
- Embeddings используются как candidate-сигнал (Expected+Actual), но финальная публикация в колонку ограничена confidence-гейтом `ZEPHYR_BUGS_DUPLICATE_PUBLISH_MIN_CONFIDENCE` (рекомендуется `high` в prod).
- Domain gate (`ZEPHYR_BUGS_DUPLICATE_DOMAIN_GATE=true`) блокирует автодубли для конфликтующих классов дефектов (например, localization vs pedestrian).
- Summary-only совпадения по умолчанию остаются в debug как medium confidence (`ZEPHYR_BUGS_DUPLICATE_SUMMARY_HIGH=false`).
- Fallback: если Expected/Actual не распарсились — сравнение по summary (старое поведение).
- Ручные правила (опционально): `reports/bugs_rollup/duplicate_overrides.json` (`merge` / `split`); prod по умолчанию не использует.
- Семантика (prod): задача `ZephyrParserEmbeddingsDaily` в **13:00** (`install_zephyr_embeddings_task.ps1`), venv `.venv-embeddings`, `run_embeddings_scheduled.ps1`; в `.env`: `ZEPHYR_BUGS_DUPLICATE_EMBEDDINGS=true` (порог **0.85**). Векторы по Expected+Actual, не по summary.

**Runbook (prod):** см. [PRODUCTION_RELEASE.md](PRODUCTION_RELEASE.md) § Task 4.1.7.

**Runbook (dev):**

1. Полный прогон `run_zephyr` (rollup + `duplicate_rollup_keys.json`)  
2. `py -3 scripts/refresh_bugs_rollup_duplicates.py` (`run_embeddings_local.ps1` в dev-клоне)  
3. `run_embeddings_local.ps1` или `run_embeddings_scheduled.ps1` → включить `ZEPHYR_BUGS_DUPLICATE_EMBEDDINGS=true` в `.env.local` для проверки

Отладка: `reports/bugs_rollup/duplicate_candidates.json` (поля `expected_sim`, `actual_sim`), `duplicate_rollup_keys.json`.

Оффлайн калибровка порогов по размеченным `merge/split`: `py -3 scripts/calibrate_bug_duplicates.py --rollup-dir reports/bugs_rollup --target-precision 0.90`.

---

## Безопасность

- Не передавайте токен через `--token` в обычном использовании — только `ZEPHYR_API_TOKEN` в `.env.secrets`.
- Production: `ZEPHYR_ENFORCE_ENV_TOKEN=true` (запрет `--token` в CLI).
- Не коммитьте `.env.secrets` в git; ограничьте ACL на файл service account.
- **Audit:** `reports/audit/audit.jsonl` — события run/export/publish (`ZEPHYR_AUDIT_*`, см. `.env.example`).
- **Logviewer:** только URL из `ZEPHYR_LOGVIEWER_URL_REGEX` (`ZEPHYR_LOGVIEWER_STRICT=true`).
- **TLS:** по умолчанию системный контекст SSL (без принудительного TLS 1.3). Для жёсткого пола: `ZEPHYR_SSL_MIN_VERSION=1.2` или `1.3`.
- Документация: `docs/security-passport.md`, `docs/security-topology.md`, `docs/security-deploy.md`.

---

## Notes

- Default auth: `Authorization: Bearer <token>`.
- `ZEPHYR_TREE_AUTOPROBE=true` — только для диагностики.
- Шаблон запроса папки должен содержать `{folder_id}`.
- Daemon loop: `ZEPHYR_LOOP_INTERVAL_MINUTES` (см. `deploy/zephyr-weekly-report.service.example`).

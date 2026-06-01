# PRD: zephyr_parser — production

**Версия продукта:** 1.5.0 (файл `PIPELINE_VERSION`, например `zephyr-parser-v1.5.0`; отображается в health HTML как есть)  
**Документ:** техническое задание на разработку и ввод в эксплуатацию  
**Целевая prod-среда:** Windows, Task Scheduler, каталог `C:\Users\qa\python_app\zephyr_parser`

---

## 1. Назначение и пользователи

**zephyr_parser** — пакетный сервис (CLI), который по расписанию забирает данные тест-экзекьюшенов из Zephyr (Jira-hosted API), строит отчёты (CSV, HTML, wiki) и публикует их в Confluence. Операторы не вводят данные в этот сервис; ввод результатов тестов — отдельная система **zephyr-bot** (Telegram), в scope этого ТЗ не входит.

**Пользователи и потребности:**

| Роль | Что получает |
|------|----------------|
| QA-инженер | Актуальные daily/weekly отчёты и аналитика в Confluence по своим папкам Zephyr |
| QA lead / аналитик | Недельная матрица, weekly analytics, CSV на диске для углублённого разбора |
| Дежурный QA | HTML-страница «здоровье пайплайна» на диске prod — без входа в Confluence и без чтения логов вручную |
| Аудитор / ИБ (при проверке) | Журнал `reports/audit/audit.jsonl` — факты запусков, экспортов и публикаций |

**Бизнес-цель:** автоматическая, предсказуемая отчётность по полигонным ежедневным прогонам с актуальной сводкой багов и подсказками по возможным дублям, без ручного копирования из Zephyr.

---

## 2. Среда, границы и репозитории

**Production:**

- Каталог: `C:\Users\qa\python_app\zephyr_parser` (единственная рабочая копия для Scheduler).
- Запуск: Windows Task Scheduler, задача `ZephyrParserEvery30Min` → `run_zephyr_scheduled.ps1` → `run_zephyr.ps1` → `zephyr_weekly_report.py`.
- Секреты: файл `.env` в корне prod-репозитория; ACL для учётной записи QA-оператора (под которой крутятся задачи Scheduler) и администраторов.
- Артефакты: `reports/`, `logs/`, `weekly_zephyr_report.csv` в корне (пути настраиваются через `.env`).

**Windows Task Scheduler (prod):**

- Задачи `ZephyrParserEvery30Min` и `ZephyrParserEmbeddingsDaily` регистрируются под **учёткой QA-оператора** (`Interactive`, текущий `$env:USERNAME` при установке через `install_*_task.ps1`).
- Требование: хост включён; пользователь залогинен или срабатывает `StartWhenAvailable` (см. комментарии в install-скриптах).
- Отдельный Windows service account без интерактивного логона — **вне scope** этого релиза.

**Разработка (не prod):**

- Каталог: `zephyr_parser_dev` (dev-клон), `run_zephyr_local.ps1`, overlay `.env.local`, выход в `reports_local/`, sandbox Confluence с префиксом `[LOCAL]`.
- В prod **не** запускать `install_zephyr_scheduled_task.ps1` из dev-клона и **не** переключать prod-каталог на feature-ветки.

**Вне scope:**

- zephyr-bot, n8n, NocoDB, Google Sheets;
- email/Telegram/корпоративные алерты при сбоях;
- перенос production на Docker/Amvera (Dockerfile — для CI, dev parity и будущего запасного деплоя);
- веб-UI для просмотра отчётов;
- публикация health dashboard в Confluence;
- SIEM-forward audit;
- автоматическая очистка `reports/` по возрасту.

**Сеть (egress с prod-хоста):** `ZEPHYR_BASE_URL`, `ZEPHYR_JIRA_BASE_URL` (если задан), `ZEPHYR_CONFLUENCE_BASE_URL`, опционально URL logviewer по allowlist.

---

## 3. Расписание, надёжность и критерий приёмки

**Основной пайплайн**

- Интервал: **каждые 30 минут** (задача Scheduler, см. `install_zephyr_scheduled_task.ps1`).
- Блокировка параллельных прогонов: `ZEPHYR_RUN_LOCK_FILE` (по умолчанию `reports/.zephyr_weekly_report.lock`).
- Таймаут одного прогона (Python): **90 минут** (`ZEPHYR_RUN_TIMEOUT_MINUTES`).
- Лимит задачи Scheduler (внешний): основной пайплайн **4 часа**, nightly embeddings **2 часа** (`ExecutionTimeLimit` в `install_zephyr_scheduled_task.ps1` / `install_zephyr_embeddings_task.ps1`).
- Логи Python: `logs/zephyr_YYYY-MM-DD_HH-MM-SS.log`, хранение **7 дней** (`ZEPHYR_LOG_RETENTION_DAYS`).
- Лог обёртки Scheduler: `reports/logs/scheduled_YYYY-MM-DD.log`.

**Поведение при сбое**

- Ошибка фиксируется в логе прогона и в audit (`run_finish` с ненулевым `exit_code`).
- **Автоматических уведомлений** (email, Telegram, мессенджеры) **нет** — дежурный QA смотрит health HTML и при необходимости лог/audit.

**Критерий «production готов»**

- **7 календарных дней** подряд задача `ZephyrParserEvery30Min` отрабатывает без ручного вмешательства: без удаления lock вручную, без правок `.env` на prod, без перезапуска задачи из-за сбоев приложения.
- За этот период в Confluence обновляются ожидаемые разделы (daily, weekly, analytics, баги), health HTML обновляется после каждого прогона, nightly embeddings хотя бы раз успешно отработали в каждые сутки.

---

## 4. Сбор данных Zephyr и отчёты на диске

**Источник данных:** Zephyr Scale API на `ZEPHYR_BASE_URL`, проект `ZEPHYR_PROJECT_ID`.

**Discovery папок (режим tree по умолчанию):**

1. Опционально кастомный источник (`ZEPHYR_TREE_SOURCE_*`).
2. `POST` `ZEPHYR_FOLDER_SEARCH_ENDPOINT`.
3. Fallback `GET` `ZEPHYR_FOLDERTREE_ENDPOINT`.
4. Фильтры: leaf-only, regex имени/пути, окно дат.

**Окно пересчёта в production:** последние **14 дней** (`ZEPHYR_REGENERATE_LAST_N_DAYS=14`). Границы `ZEPHYR_FROM_DATE` / `ZEPHYR_TO_DATE` задаются в `.env` и не сужают окно сильнее, чем требуется для отчётности.

**Обязательные артефакты на диске (prod):**

| Артефакт | Назначение для пользователя |
|----------|----------------------------|
| `weekly_zephyr_report.csv` | Сводка по ISO-неделям и статусам |
| `reports/by_folder/*.csv` | Детализация по папке/дню |
| `reports/cycles_and_cases.csv` | Папка → цикл → кейс |
| `reports/case_steps.csv` | Шаги кейсов и статусы |
| `reports/cycle_progress.csv` | Прогресс по циклам |
| `reports/weekly_cycle_matrix.csv` (+ dated copies) | Недельная матрица для weekly readable |
| `reports/daily_readable/` | HTML + wiki для daily Confluence |
| `reports/weekly_readable/` | HTML + wiki для weekly Confluence |
| `reports/weekly_analytics/` | Тренды, rolling, расширенная аналитика дефектов |
| `reports/build_log_reports/` | Build-log по Jira issue |
| `reports/bugs_rollup/` | Индекс багов, кандидаты дублей, cache embeddings |

**Retention на диске**

- `reports/` (включая readable, by_folder, build_log, bugs_rollup): **не удалять автоматически** (`ZEPHYR_REPORTS_RETENTION_DAYS=0`).
- `logs/`: удалять файлы старше **7 дней**.
- `reports/audit/audit.jsonl`: ротация/очистка по **186 дням** (`ZEPHYR_AUDIT_RETENTION_DAYS`).

Шаблоны вёрстки: `report_templates/readable/`.

---

## 5. Публикация в Confluence (обязательно в prod)

В production **все** перечисленные типы публикации **включены**:

| Переменная | Значение в prod |
|------------|-----------------|
| `ZEPHYR_CONFLUENCE_PUBLISH_DAILY` | `true` |
| `ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY` | `true` |
| `ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY_ANALYTICS` | `true` |
| `ZEPHYR_CONFLUENCE_PUBLISH_BUGS` | `true` |

**Структура под корневой страницей** (`ZEPHYR_CONFLUENCE_PARENT_PAGE_ID`):

1. **Папки недель** — имя по шаблону `Week {year}-w{week:02d}` (`ZEPHYR_CONFLUENCE_WEEK_FOLDER_TITLE_TEMPLATE`). Внутри недели: daily-страницы за каждый день ISO-недели из имён файлов и weekly matrix за ту же неделю.
2. **Weekly Analytics** — одна страница с фиксированным заголовком (`ZEPHYR_CONFLUENCE_WEEKLY_ANALYTICS_TITLE`, по умолчанию «Zephyr Weekly Analytics»), обновляется при каждом прогоне с включённой аналитикой.
3. **Папка «Баги»** (`ZEPHYR_CONFLUENCE_BUGS_PARENT_TITLE`) — не внутри Week wNN. Содержит:
   - страницу **сводки багов** (rollup за последние **4** ISO-недели, `ZEPHYR_BUGS_ROLLUP_LAST_WEEKS=4`);
   - build-log страницы по Jira issue.

**Поведение обновления:** при совпадении title — обновление существующей страницы (`ZEPHYR_CONFLUENCE_UPDATE_EXISTING=true`). Excerpt для daily/weekly — по флагам `ZEPHYR_CONFLUENCE_DAILY_EXCERPT` / `ZEPHYR_CONFLUENCE_WEEKLY_EXCERPT`.

**Аутентификация Confluence:** `ZEPHYR_CONFLUENCE_BASE_URL`, `ZEPHYR_CONFLUENCE_SPACE_KEY`, токен в `.env`; для basic — `ZEPHYR_CONFLUENCE_USER`. Launcher выставляет `ZEPHYR_CONFLUENCE_AUTH_SCHEME=bearer` для процесса Python, если не переопределено.

**Продуктовый результат для QA:** один стабильный URL-дерево в Confluence, где за любой рабочий день виден daily-отчёт, за неделю — сводная матрица и аналитика, в «Багах» — актуальная таблица с колонкой возможных дублей.

---

## 6. Сводка багов и детекция дублей

**Сводка багов**

- Каталог rollup: `reports/bugs_rollup/`.
- Источник: Jira issues типа Bug (фильтр `ZEPHYR_DEFECT_TYPE_FILTER`), поля из description (Expected/Actual в таблице).
- В Confluence публикуется `bugs_index.html` и связанные build-log.

**Колонка «Возможно дубль»**

- **Text similarity** по Expected и Actual: пара считается кандидатом, если `min(expected_sim, actual_sim) >= 0.78` (`ZEPHYR_BUGS_DUPLICATE_TEXT_THRESHOLD`). Похожие только summary без совпадения результатов — не дубль.
- **Embeddings** (обязательно в prod): `ZEPHYR_BUGS_DUPLICATE_EMBEDDINGS=true`, порог **0.85** (`ZEPHYR_BUGS_DUPLICATE_EMBED_THRESHOLD`). Векторы строятся по Expected+Actual, не по summary.
- **Ручные правила (опционально):** `reports/bugs_rollup/duplicate_overrides.json` — операции `merge` / `split`; в prod по умолчанию не используются (ложные подсказки игнорируются).
- **Свежесть подсказок:** text — каждые 30 мин (основной пайплайн); semantic (embeddings) — после ежедневного прогона в **13:00** (см. §9).

**Модель embeddings (prod):** `ZEPHYR_BUGS_EMBED_MODEL` (по умолчанию `paraphrase-multilingual-MiniLM-L12-v2`). Используется nightly-задачей и записывается в `duplicate_embeddings_cache.json` (поле `model`).

**Отладочные артефакты (не для Confluence):** `duplicate_candidates.json`, `duplicate_rollup_keys.json`.

---

## 7. Health dashboard (новая разработка)

**Для кого:** дежурный QA.

**Где:** файл `reports/pipeline_health.html` на prod-хосте (открытие с диска или через общий доступ к `reports/`). В Confluence **не** публикуется.

**Когда обновляется:** в конце каждого прогона основного пайплайна (успех или ошибка); после nightly embeddings — обновление в конце `run_embeddings_scheduled.ps1` (чтобы статус embeddings был виден в тот же день).

**Содержание страницы (минимум):**

- Статус последнего прогона: время `run_start` и `run_finish`, `exit_code`, длительность в минутах/секундах.
- Текущий статус lock-файла: свободен / занят / stale (если lock старше порога — «возможно зависший прогон»).
- Блок **Nightly embeddings:** последнее событие `embeddings_finish` в audit (`timestamp_utc`, `result`, `exit_code`); путь и mtime последнего `reports/logs/embeddings_YYYY-MM-DD.log`.
- Последние **20** записей из `reports/audit/audit.jsonl` (новые сверху), с выделением событий с ошибками (`run_finish` / `embeddings_finish` с `exit_code != 0`, неуспешный `publish_confluence`, `integration_call` с `result=error`).
- Путь к последнему файлу `logs/zephyr_*.log` (имя файла, время изменения).
- Версия пайплайна: содержимое файла `PIPELINE_VERSION`.
- Время генерации страницы (локальное время хоста).

| Поле | Источник / настройка |
|------|----------------------|
| Lock stale | `ZEPHYR_HEALTH_LOCK_STALE_MINUTES`; если пусто — `ZEPHYR_RUN_TIMEOUT_MINUTES` (по умолчанию 90) |
| Путь health HTML | `ZEPHYR_PIPELINE_HEALTH_HTML` или `reports/pipeline_health.html` относительно корня reports |
| Корень reports для health | `ZEPHYR_HEALTH_REPORT_DIR` (опционально) |

**Продуктовый сценарий:** дежурный открывает один HTML утром или после инцидента и за 30 секунд понимает, был ли успешный прогон, не завис ли lock, куда смотреть полный лог.

---

## 8. Dockerfile (новая разработка)

**Назначение образа (не замена Windows prod):**

- Проверка в CI: `docker build` проходит в GitHub Actions.
- Локальная разработка на Linux/macOS с тем же entrypoint, что prod-скрипты.
- Запасной деплой на Amvera по `amvera.yaml` при необходимости.

**Требования к образу:**

- Базовый образ: `python:3.12-slim`.
- Пользователь в контейнере: non-root `zephyr`.
- Основной parser: **только stdlib** (без pip-зависимостей в образе по умолчанию).
- Точка монтирования данных: `/data` → в контейнере `reports/` и `logs/` лежат под `/data` (соответствие `amvera.yaml` `persistenceMount: /data`).
- Entrypoint: эквивалент `python -u zephyr_weekly_report.py` с подгрузкой env из смонтированного секрета/файла.

**Embeddings в Docker:** отдельный образ или documented optional stage с `requirements-embeddings.txt`; основной slim-образ embeddings **не** включает. Production embeddings остаются на Windows venv (раздел 9).

---

## 9. Автоматизация embeddings (новая разработка)

**Расписание:** отдельная задача Windows Task Scheduler **один раз в сутки в 13:00** (локальное время хоста). Имя задачи: `ZephyrParserEmbeddingsDaily`. Установка: `.\install_zephyr_embeddings_task.ps1` (по умолчанию `-DailyAt "13:00"`).

**Цепочка выполнения** (`run_embeddings_scheduled.ps1`):

1. Создание venv при первом запуске (`.venv-embeddings/`), `pip install -r requirements-embeddings.txt`.
2. Audit: `embeddings_start`.
3. `scripts/compute_bug_embeddings.py --from-rollup-dir reports/bugs_rollup` (модель из `ZEPHYR_BUGS_EMBED_MODEL` или CLI `--model`).
4. `scripts/refresh_bugs_rollup_duplicates.py`.
5. Лог `reports/logs/embeddings_YYYY-MM-DD.log`; audit: `embeddings_finish` с `exit_code` и `result` (`failure` при ошибке); обновление `reports/pipeline_health.html`.

**Связь с основным пайплайном (30 мин):**

- Nightly embeddings обновляет cache; основной parser при следующих прогонах использует актуальные embeddings при `ZEPHYR_BUGS_DUPLICATE_EMBEDDINGS=true`.
- Если nightly embeddings упал: основной parser **продолжает работать** с последним успешным cache; факт сбоя виден в audit (`embeddings_finish`, `exit_code != 0`), в health HTML (блок embeddings) и в логе `embeddings_*.log`.

**Идемпотентность:** повторный запуск за сутки безопасен, пересчитывает только изменившиеся issue по логике скриптов.

**Установка:** `.\install_zephyr_embeddings_task.ps1` (по аналогии с `install_zephyr_scheduled_task.ps1`).

---

## 10. Безопасность (production minimum)

| Требование | Реализация |
|------------|------------|
| Токены не в CLI | `ZEPHYR_ENFORCE_ENV_TOKEN=true`; Scheduler **не** передаёт `--token` |
| Audit | `ZEPHYR_AUDIT_ENABLED=true`, `ZEPHYR_AUDIT_REASON=scheduled QA reporting`, путь `reports/audit/audit.jsonl` |
| Logviewer | `ZEPHYR_LOGVIEWER_STRICT=true`; при появлении ссылок в отчётах — задать `ZEPHYR_LOGVIEWER_URL_REGEX` |
| Секреты | `.env` не в git; ACL на `.env`, `reports/`, `logs/` для учётной записи QA-оператора, под которой зарегистрированы задачи Scheduler |
| TLS | По умолчанию системный SSL; опционально `ZEPHYR_SSL_MIN_VERSION` |

Документация ИБ: `docs/security-passport.md`, `docs/security-deploy.md` — справочно; полный корпоративный пакет SIEM/AD в этом релизе **не** требуется.

---

## 11. Выкат в production

**Порядок:**

1. Слить изменения в `main`, в prod-каталоге: `git checkout main`, `git pull origin main`.
2. Обновить `.env` (см. раздел 12).
3. Установить/проверить задачу `ZephyrParserEvery30Min` (`.\install_zephyr_scheduled_task.ps1`).
4. Установить задачу `ZephyrParserEmbeddingsDaily` (`.\install_zephyr_embeddings_task.ps1`).
5. После первого `ZephyrParserEmbeddingsDaily` (или ручного `.\run_embeddings_scheduled.ps1`): убедиться, что есть `.venv-embeddings`, успешный `embeddings_*.log` и `embeddings_finish` в audit.
6. Ручной smoke: `Start-ScheduledTask -TaskName ZephyrParserEvery30Min`; дождаться завершения.
7. Проверить: `reports/audit/audit.jsonl` (цепочка `run_start` → `export_file` → `publish_confluence` → `run_finish`; для embeddings — `embeddings_start` → `embeddings_finish`); дерево Confluence; `reports/pipeline_health.html` (в т.ч. блок embeddings); после **13:00** — обновлённый rollup и cache embeddings.
8. Начать отсчёт **7 дней** стабильной работы (критерий приёмки, раздел 3).

**Запрещено на prod:** запуск из `zephyr_parser_dev`, публикация с `[LOCAL]` prefix, ручное удаление lock без записи причины в runbook инцидента.

---

## 12. Конфигурация production `.env` (обязательные значения)

Помимо существующих параметров проекта/папок/Confluence из `.env.example`:

```env
ZEPHYR_REGENERATE_LAST_N_DAYS=14
ZEPHYR_ENFORCE_ENV_TOKEN=true
ZEPHYR_AUDIT_ENABLED=true
ZEPHYR_AUDIT_REASON=scheduled QA reporting
ZEPHYR_LOGVIEWER_STRICT=true
ZEPHYR_REPORTS_RETENTION_DAYS=0
ZEPHYR_LOG_RETENTION_DAYS=7
ZEPHYR_AUDIT_RETENTION_DAYS=186
ZEPHYR_CONFLUENCE_PUBLISH_DAILY=true
ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY=true
ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY_ANALYTICS=true
ZEPHYR_CONFLUENCE_PUBLISH_BUGS=true
ZEPHYR_BUGS_DUPLICATE_DETECT=true
ZEPHYR_BUGS_DUPLICATE_EMBEDDINGS=true
ZEPHYR_BUGS_DUPLICATE_TEXT_THRESHOLD=0.78
ZEPHYR_BUGS_DUPLICATE_EMBED_THRESHOLD=0.85
ZEPHYR_BUGS_ROLLUP_LAST_WEEKS=4
ZEPHYR_RUN_TIMEOUT_MINUTES=90
# Пусто = fallback на ZEPHYR_RUN_TIMEOUT_MINUTES (90)
ZEPHYR_HEALTH_LOCK_STALE_MINUTES=
ZEPHYR_BUGS_EMBED_MODEL=paraphrase-multilingual-MiniLM-L12-v2
```

Confluence-URL, space, parent page id, токены Zephyr/Jira/Confluence — production-значения, не sandbox dev-клона.

---

## 13. Объём разработки по этому PRD

| ID | Фича | Статус |
|----|------|--------|
| R1 | Основной пайплайн v1.4, Confluence, bugs rollup, text duplicates | Реализовано |
| R2 | Health HTML `reports/pipeline_health.html` (`zephyr_pipeline_health.py`) | Реализовано (включая блок embeddings и lock stale по env) |
| R3 | Dockerfile + CI `docker build` | Реализовано |
| R4 | `requirements-embeddings.txt`, venv, nightly task, `install_zephyr_embeddings_task.ps1` | Реализовано |
| R4b | Observability embeddings: audit `embeddings_*`, health-блок, `ZEPHYR_HEALTH_LOCK_STALE_MINUTES`, `ZEPHYR_BUGS_EMBED_MODEL` | Реализовано; локальная верификация 2026-06-01 (`reports_local/`, [VERIFICATION_REPORT.md](VERIFICATION_REPORT.md)) |
| R5 | Prod rollout, 7-day soak | Операции на prod-хосте |

**Зависимости:** R2 и R4 можно вести параллельно; R3 независим; R4b после R4; R5 после merge R2, R4, R4b в `main` и обновления prod-хоста.

---

## 14. Вне scope (явный отказ)

- Уведомления при сбоях (email, Telegram, Mattermost/Slack/Teams).
- Интеграция с zephyr-bot и общий PRD на экосистему.
- Веб-интерфейс для reports.
- Health dashboard в Confluence.
- `ZEPHYR_REPORTS_RETENTION_DAYS > 0` (автоудаление старых отчётов).
- Production runtime на Docker/Amvera вместо Windows Scheduler.
- SIEM-forward `audit.jsonl`.
- Семантические embeddings без nightly job (только ручной прогон) — **не** допускается в prod; embeddings обязательны по расписанию.
- Windows service account без интерактивного логона для Task Scheduler (отдельный runbook при необходимости).

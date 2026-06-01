# TASKS: zephyr_parser production (из PRD.md)

**Источник:** [PRD.md](PRD.md) v1.5.0  
**Prod-каталог:** `C:\Users\qa\python_app\zephyr_parser`  
**Dev-каталог:** `zephyr_parser_dev`

Легенда статусов: ✅ готово · 🔧 в работе · ⏳ не начато · 📋 операции (не код)

---

## Сводка этапов

| Этап | Название | PRD | Фичи | Зависимости |
|------|----------|-----|------|-------------|
| 1 | Основной пайплайн и отчёты | §3–§6 | R1 | — |
| 2 | Health dashboard | §7 | R2, R4b | Этап 4 (для блока embeddings) |
| 3 | Docker и CI | §8 | R3 | — |
| 4 | Nightly embeddings | §9 | R4, R4b | — |
| 5 | Безопасность и prod `.env` | §10, §12 | — | Этапы 1–4 |
| 6 | Выкат в production | §11 | R5 | Merge R2, R4, R4b в `main` |
| 7 | Приёмка (7-day soak) | §3 | R5 | Этап 6 |

**Параллельность (PRD §13):** этапы 2 и 4 можно вести параллельно; этап 3 независим; R4b — после R4; этап 6 — после merge доработок R4b в `main`.

---

## Этап 1. Основной пайплайн и отчёты (R1)

**Цель:** каждые 30 минут забирать данные Zephyr, строить артефакты на диске и публиковать в Confluence.

**Критерий готовности этапа:** smoke-прогон на prod даёт полный набор файлов в `reports/` и обновлённые страницы Confluence без ручного вмешательства.

### 1.1 Scheduler и обёртки

| ID | Задача | Статус |
|----|--------|--------|
| 1.1.1 | Зарегистрировать задачу `ZephyrParserEvery30Min` через `install_zephyr_scheduled_task.ps1` под учёткой QA-оператора | 📋 |
| 1.1.2 | Проверить цепочку: `run_zephyr_scheduled.ps1` → `run_zephyr.ps1` → `zephyr_weekly_report.py` | 📋 |
| 1.1.3 | Убедиться в lock-файле `ZEPHYR_RUN_LOCK_FILE` (по умолчанию `reports/.zephyr_weekly_report.lock`) — нет параллельных прогонов | 📋 |
| 1.1.4 | Проверить таймаут Python 90 мин (`ZEPHYR_RUN_TIMEOUT_MINUTES`) и лимит Scheduler 4 ч | 📋 |

### 1.2 Сбор данных Zephyr

| ID | Задача | Статус |
|----|--------|--------|
| 1.2.1 | Настроить `ZEPHYR_BASE_URL`, `ZEPHYR_PROJECT_ID`, discovery tree (folder search → foldertree fallback) | ✅ |
| 1.2.2 | Задать окно пересчёта: `ZEPHYR_REGENERATE_LAST_N_DAYS=14` | 📋 |
| 1.2.3 | Проверить фильтры папок (leaf-only, regex имени/пути, окно дат) на prod | 📋 |

### 1.3 Артефакты на диске

| ID | Задача | Статус |
|----|--------|--------|
| 1.3.1 | `weekly_zephyr_report.csv` в корне | ✅ |
| 1.3.2 | `reports/by_folder/*.csv`, `cycles_and_cases.csv`, `case_steps.csv`, `cycle_progress.csv` | ✅ |
| 1.3.3 | `reports/weekly_cycle_matrix.csv` (+ dated copies) | ✅ |
| 1.3.4 | `reports/daily_readable/`, `weekly_readable/`, `weekly_analytics/` | ✅ |
| 1.3.5 | `reports/build_log_reports/`, `reports/bugs_rollup/` | ✅ |
| 1.3.6 | Retention: `ZEPHYR_REPORTS_RETENTION_DAYS=0`; логи 7 дней; audit 186 дней | 📋 |

### 1.4 Публикация Confluence

| ID | Задача | Статус |
|----|--------|--------|
| 1.4.1 | Включить все флаги publish: daily, weekly, weekly analytics, bugs (`true` в prod `.env`) | 📋 |
| 1.4.2 | Проверить дерево: Week wNN → daily + weekly matrix; отдельно Weekly Analytics и папка «Баги» | 📋 |
| 1.4.3 | Проверить обновление существующих страниц (`ZEPHYR_CONFLUENCE_UPDATE_EXISTING=true`) | 📋 |
| 1.4.4 | Проверить excerpt-флаги daily/weekly при необходимости | 📋 |

### 1.5 Сводка багов и дубликаты (text)

| ID | Задача | Статус |
|----|--------|--------|
| 1.5.1 | Rollup в `reports/bugs_rollup/`, публикация `bugs_index.html` и build-log | ✅ |
| 1.5.2 | Text similarity: порог 0.78 по Expected/Actual (`ZEPHYR_BUGS_DUPLICATE_TEXT_THRESHOLD`) | ✅ |
| 1.5.3 | Поддержка `duplicate_overrides.json` (merge/split) | ✅ |
| 1.5.4 | Отладочные JSON: `duplicate_candidates.json`, `duplicate_rollup_keys.json` — не в Confluence | ✅ |

### 1.6 Логи и audit (основной пайплайн)

| ID | Задача | Статус |
|----|--------|--------|
| 1.6.1 | Логи Python: `logs/zephyr_YYYY-MM-DD_HH-MM-SS.log` | ✅ |
| 1.6.2 | Лог обёртки: `reports/logs/scheduled_YYYY-MM-DD.log` | ✅ |
| 1.6.3 | Audit: `run_start` → `export_file` → `publish_confluence` → `run_finish` в `reports/audit/audit.jsonl` | ✅ |
| 1.6.4 | Генерация `pipeline_health.html` в конце каждого прогона основного пайплайна | ✅ |

---

## Этап 2. Health dashboard (R2 + R4b)

**Цель:** дежурный QA открывает один HTML и за ~30 с понимает состояние пайплайна, lock и embeddings.

**PRD:** §7. Файл: `reports/pipeline_health.html` (`zephyr_pipeline_health.py`).

### 2.1 Базовый health (R2)

| ID | Задача | Статус |
|----|--------|--------|
| 2.1.1 | Статус последнего прогона: `run_start`, `run_finish`, `exit_code`, длительность | ✅ |
| 2.1.2 | Статус lock: свободен / занят / stale | ✅ |
| 2.1.3 | Последние 20 записей audit (новые сверху) | ✅ |
| 2.1.4 | Выделение ошибок в audit (`run_finish` с `exit_code != 0`, и т.д.) | ✅ |
| 2.1.5 | Путь и mtime последнего `logs/zephyr_*.log` | ✅ |
| 2.1.6 | Версия из `PIPELINE_VERSION`, время генерации (локальное) | ✅ |
| 2.1.7 | Env: `ZEPHYR_PIPELINE_HEALTH_HTML`, `ZEPHYR_HEALTH_REPORT_DIR` | ✅ |
| 2.1.8 | Unit-тесты `tests/test_pipeline_health.py` | ✅ |

### 2.2 Доработки observability (R4b)

| ID | Задача | Статус |
|----|--------|--------|
| 2.2.1 | Lock stale из `ZEPHYR_HEALTH_LOCK_STALE_MINUTES`, fallback на `ZEPHYR_RUN_TIMEOUT_MINUTES` (90) | ✅ |
| 2.2.2 | Блок **Nightly embeddings:** последний `embeddings_finish` в audit (`timestamp_utc`, `result`, `exit_code`) | ✅ |
| 2.2.3 | В блоке embeddings: путь и mtime последнего `reports/logs/embeddings_YYYY-MM-DD.log` | ✅ |
| 2.2.4 | Подсветка ошибок: `embeddings_finish` с `exit_code != 0`, неуспешный `publish_confluence`, `integration_call` с `result=error` | ✅ |
| 2.2.5 | Обновление health в конце `run_embeddings_scheduled.ps1` (после nightly) | ✅ |
| 2.2.6 | Документировать `ZEPHYR_HEALTH_LOCK_STALE_MINUTES` в `.env.example` | ✅ |
| 2.2.7 | Локальная верификация R4b (`run_zephyr_local.ps1`, `run_embeddings_local.ps1`, `reports_local/`) — см. [VERIFICATION_REPORT.md](VERIFICATION_REPORT.md) | ✅ |

---

## Этап 3. Docker и CI (R3)

**Цель:** образ для CI, dev parity на Linux/macOS; **не** замена Windows prod.

**PRD:** §8.

| ID | Задача | Статус |
|----|--------|--------|
| 3.1 | `Dockerfile`: `python:3.12-slim`, пользователь `zephyr` (non-root) | ✅ |
| 3.2 | Основной parser без pip-зависимостей в slim-образе | ✅ |
| 3.3 | Точка монтирования `/data` для `reports/` и `logs/` | ✅ |
| 3.4 | Entrypoint ≈ `python -u zephyr_weekly_report.py` + env из смонтированного файла | ✅ |
| 3.5 | Embeddings: отдельный stage/документация с `requirements-embeddings.txt`, не в slim по умолчанию | ✅ |
| 3.6 | CI: `docker build` проходит в GitHub Actions | 📋 |
| 3.7 | `.dockerignore` актуален | ✅ |

---

## Этап 4. Nightly embeddings (R4 + R4b)

**Цель:** раз в сутки в **13:00** обновлять embedding-cache для колонки «Возможно дубль»; основной пайплайн продолжает работать при сбое nightly.

**PRD:** §6 (embeddings), §9.

### 4.1 Скрипты и зависимости (R4)

| ID | Задача | Статус |
|----|--------|--------|
| 4.1.1 | `requirements-embeddings.txt` и модель по умолчанию `paraphrase-multilingual-MiniLM-L12-v2` | ✅ |
| 4.1.2 | `run_embeddings_scheduled.ps1`: venv `.venv-embeddings`, pip install, compute + refresh duplicates | ✅ |
| 4.1.3 | `scripts/compute_bug_embeddings.py --from-rollup-dir reports/bugs_rollup` | ✅ |
| 4.1.4 | `scripts/refresh_bugs_rollup_duplicates.py` после compute | ✅ |
| 4.1.5 | Лог `reports/logs/embeddings_YYYY-MM-DD.log` | ✅ |
| 4.1.6 | `install_zephyr_embeddings_task.ps1` → задача `ZephyrParserEmbeddingsDaily`, **13:00**, лимит 2 ч | ✅ |
| 4.1.7 | Embeddings в prod: дефолты **13:00**, rollup **4** нед., runbook §4.1.7; prod `.env`/`Scheduler` — [PRODUCTION_RELEASE.md](PRODUCTION_RELEASE.md) (при выкате этап 6) | ✅ |

### 4.2 Audit и связь с основным пайплайном (R4b)

| ID | Задача | Статус |
|----|--------|--------|
| 4.2.1 | Запись `embeddings_start` в audit в начале nightly | ✅ |
| 4.2.2 | Запись `embeddings_finish` с `exit_code` и `result` (`failure` при ошибке) | ✅ |
| 4.2.3 | Поле `model` в `duplicate_embeddings_cache.json` из `ZEPHYR_BUGS_EMBED_MODEL` | ✅ |
| 4.2.4 | Идемпотентность: повторный запуск за сутки безопасен | ✅ |
| 4.2.5 | Проверка: при падении nightly основной parser использует последний cache | ✅ |

---

## Этап 5. Безопасность и конфигурация production

**Цель:** минимальный production security baseline и согласованный `.env` на prod-хосте.

**PRD:** §10, §12.

| ID | Задача | Статус |
|----|--------|--------|
| 5.1 | `ZEPHYR_ENFORCE_ENV_TOKEN=true`; Scheduler не передаёт `--token` | 📋 |
| 5.2 | `ZEPHYR_AUDIT_ENABLED=true`, `ZEPHYR_AUDIT_REASON=scheduled QA reporting` | 📋 |
| 5.3 | `ZEPHYR_LOGVIEWER_STRICT=true`; при ссылках в отчётах — `ZEPHYR_LOGVIEWER_URL_REGEX` | 📋 |
| 5.4 | ACL на `.env`, `reports/`, `logs/` для учётки QA-оператора Scheduler | 📋 |
| 5.5 | Сверить prod `.env` с шаблоном PRD §12 (все обязательные ключи) | 📋 |
| 5.6 | Confluence/Zephyr токены — production, не sandbox dev | 📋 |
| 5.7 | Справочно: `docs/security-passport.md`, `docs/security-deploy.md` | 📋 |

---

## Этап 6. Выкат в production (R5)

**Цель:** обновить prod-репозиторий, задачи Scheduler, smoke и начать отсчёт 7 дней.

**PRD:** §11. **Не делать:** запуск из `zephyr_parser_dev`, префикс `[LOCAL]`, удаление lock без runbook.

| ID | Задача | Статус |
|----|--------|--------|
| 6.1 | Слить R2/R4/R4b в `main`; в prod: `git checkout main`, `git pull origin main` | ⏳ |
| 6.2 | Обновить prod `.env` (этап 5) | 📋 |
| 6.3 | Установить/проверить `ZephyrParserEvery30Min` | 📋 |
| 6.4 | Установить `ZephyrParserEmbeddingsDaily` | 📋 |
| 6.5 | Первый nightly или ручной `.\run_embeddings_scheduled.ps1`: `.venv-embeddings`, `embeddings_*.log`, `embeddings_finish` в audit | 📋 |
| 6.6 | Smoke: `Start-ScheduledTask -TaskName ZephyrParserEvery30Min`, дождаться завершения | 📋 |
| 6.7 | Проверить audit-цепочки, Confluence, `pipeline_health.html` (включая embeddings), rollup после 13:00 | 📋 |
| 6.8 | Начать отсчёт **7 календарных дней** стабильной работы | 📋 |

---

## Этап 7. Приёмка — 7-day soak

**Цель:** подтвердить критерий «production готов» из PRD §3.

**Условия (все одновременно):**

- [ ] 7 календарных дней подряд `ZephyrParserEvery30Min` без ручного удаления lock, правок `.env` на prod, перезапуска из-за сбоев приложения
- [ ] Confluence: daily, weekly, analytics, баги обновляются ожидаемо
- [ ] `pipeline_health.html` обновляется после каждого прогона основного пайплайна
- [ ] Nightly embeddings успешно хотя бы раз в каждые сутки (или зафиксирован сбой в audit/health/log)

| ID | Задача | Статус |
|----|--------|--------|
| 7.1 | Ежедневная проверка health HTML (дежурный QA) | 📋 |
| 7.2 | Выборочная проверка Confluence по рабочим дням недели | 📋 |
| 7.3 | Проверка audit на `exit_code != 0` и embeddings | 📋 |
| 7.4 | Фиксация даты начала/окончания soak в runbook или PRODUCTION_RELEASE.md | 📋 |

---

## Матрица PRD → этапы

| ID PRD | Описание | Этап |
|--------|----------|------|
| R1 | Пайплайн v1.4, Confluence, bugs rollup, text duplicates | 1 |
| R2 | Health HTML (базовый) | 2.1 |
| R3 | Dockerfile + CI | 3 |
| R4 | Embeddings venv, nightly task | 4.1 |
| R4b | Audit embeddings, health-блок, lock stale env | 2.2, 4.2 |
| R5 | Prod rollout, 7-day soak | 6, 7 |

---

## Вне scope (не создавать задачи)

По PRD §2, §14:

- zephyr-bot, n8n, NocoDB, Google Sheets
- Email/Telegram/корпоративные алерты при сбоях
- Production на Docker/Amvera вместо Windows Scheduler
- Веб-UI отчётов; health в Confluence
- SIEM-forward audit; автоочистка `reports/` по возрасту
- Windows service account без интерактивного логона
- Embeddings только вручную (в prod обязателен nightly)

---

## Чеклист перед закрытием релиза 1.4

- [x] Все задачи этапов 2.2 и 4.2 (R4b) — ✅ (локальная верификация 2026-06-01)
- [ ] Этап 6 выполнен на prod-хосте
- [ ] Этап 7 — 7 дней без инцидентов по критерию §3 PRD
- [x] `.env.example` и PRODUCTION_RELEASE.md синхронизированы с фактическим поведением (4.1.7: 13:00, rollup 4 нед., runbook)
- [x] Задача **4.1.7** закрыта в dev-репо (2026-06-01); применение на prod-хосте — этап 6

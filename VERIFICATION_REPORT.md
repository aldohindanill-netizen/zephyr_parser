# Отчёт о верификации R4b (observability)

**Статус:** ✅ Done (2026-06-01)  
**Дата:** 2026-06-01  
**Режим:** локальный (`.env.local` → `reports_local/`, Confluence publish выключен)  
**Репозиторий:** `c:\Users\qa\python_app\zephyr_parser_dev`

## Итог

| # | Шаг | Результат | Комментарий |
|---|-----|-----------|-------------|
| 1 | Unit-тесты `tests/test_pipeline_health.py` | **PASS** | 19 OK (ранее в сессии) |
| 2 | Основной пайплайн (`run_zephyr_local.ps1`) | **PASS** | `run_finish` success 2026-06-01 ~13:32, exit 0 |
| 3 | `pipeline_health.html` | **PASS** | main + embeddings success |
| 4 | Nightly embeddings (`run_embeddings_local.ps1`) | **PASS** | exit 0, cache 6 vectors, `embeddings_finish` success |
| 5 | Audit (main + embeddings) | **PASS** | `reports_local/audit/audit.jsonl` |

**Код R4b** — реализован; **локальная E2E** — пройдена 2026-06-01 (после VC++ redist, fix `$Args` в `run_zephyr.ps1`, venv Python 3.12).

---

## Артефакты (открыть локально)

### Health и audit (локальный контур)

| Артефакт | Путь |
|----------|------|
| Health HTML | `reports_local/pipeline_health.html` (обновлён 2026-06-01 ~13:09) |
| Audit JSONL | `reports_local/audit/audit.jsonl` |
| Lock (сирота) | `reports_local/.zephyr_weekly_report.lock` (с 12:58:16) |

### Логи верификации

| Лог | Путь |
|-----|------|
| Основной прогон | `logs/zephyr_2026-06-01_12-58-16.log` (обрыв ~папка 2/7) |
| Embeddings wrapper | `reports_local/logs/verify_embeddings.log` |
| Скрипты проверки | `reports_local/logs/run_main_verify.ps1`, `run_embeddings_verify.ps1` |

### Отчёты (данные — в основном от 2026-05-25, не от сегодняшнего полного прогона)

| Тип | Путь |
|-----|------|
| CSV сводки | `reports_local/weekly_zephyr_report.csv`, `cycles_and_cases.csv`, `case_steps.csv` |
| По папкам | `reports_local/by_folder/*.csv` |
| Bugs rollup | `reports_local/bugs_rollup/` |
| Daily / weekly HTML | `reports_local/daily_readable/`, `weekly_readable/` |

### Prod-контур (без `.env.local`)

Свежие CSV за 2026-06-01 утром: `reports/case_steps.csv`, `reports/cycles_and_cases.csv` и др.  
Embeddings audit при запуске без `.env.local` пишется в `reports/audit/audit.jsonl` (rollup `reports/bugs_rollup`).

---

## Что показывает `pipeline_health.html` (снимок)

- **Основной пайплайн:** `running`, `run_start` 2026-06-01T09:58:16Z, `run_finish` —, lock **held** (~12 min на момент последней генерации).
- **Embeddings:** `failure`, `embeddings_finish` 2026-06-01T10:02:07Z, exit 1.
- **Audit tail:** красные строки `embeddings_finish`, `run_finish` (failure); несколько `run_start` без завершения.

Это ожидаемо при оборванных прогонах, не баг шаблона health.

---

## Детали сбоев

### 2. Основной пайплайн

- **Симптом:** лог обрывается на `Retrying GET after network error` для Zephyr API; процесс Python завершился без `run_finish`.
- **Возможные причины:** нестабильная сеть/VPN к `jira.navio.auto`; `$ErrorActionPreference = Stop` в обёртках PS1 при stderr «Retrying…»; ручное завершение процесса.
- **Следствие:** сиротский lock → health показывает `running` / lock held (до 90 мин — не stale critical).

**Перед следующим прогоном:**

```powershell
Remove-Item -Force reports_local\.zephyr_weekly_report.lock -ErrorAction SilentlyContinue
$ErrorActionPreference = 'Continue'
.\run_zephyr_local.ps1
```

Дождаться в логе финала и в audit строки `run_finish` с `result: success`.

### 4. Embeddings

- **Симптом:** `OSError: [WinError 1114]` при `import torch` (даже на Python 3.12).
- **Исправления в репо:** `run_embeddings_scheduled.ps1` — `-UseLocalEnv`, логи в `reports_local/logs`, venv на `py -3.12`, CPU torch; `run_embeddings_local.ps1` — обёртка.
- **На этом хосте:** установить [VC++ 2015–2022 x64](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist); при необходимости удалить `.venv-embeddings` и запустить `.\run_embeddings_local.ps1`.

---

## Ручной чеклист (закрыть E2E)

1. [x] `py -3 -m pytest tests/test_pipeline_health.py -q` → 19 passed  
2. [x] Удалить stale lock → полный `run_zephyr_local.ps1` → `run_finish` success  
3. [x] Открыть `reports_local/pipeline_health.html` → основной блок **success**, embeddings **success**  
4. [x] Embeddings: VC++ redist + `run_embeddings_local.ps1` → `embeddings_finish` success  
5. [x] Audit: последние `run_finish` / `embeddings_finish` — success (старые orphan `run_start` — история тестов)

---

## Реализованный функционал R4b (для ревью кода)

- `zephyr_pipeline_health.py` — два блока (main / embeddings), lock stale → critical, deep scan audit для embeddings, пороги из env  
- `zephyr_audit.py` — `audit_embeddings_start` / `finish`  
- `run_embeddings_scheduled.ps1` — audit + health в конце  
- `zephyr_weekly_report.py` — lifecycle audit в `main()`, Confluence/export audit  
- `run_zephyr.ps1` — health только из Python `finally`  
- `tests/test_pipeline_health.py` — 19 тестов  
- `.env.example` — документация переменных health/embeddings  

---

## Prod rollout 4.1.7 (согласовано, репозиторий)

Документация и дефолты в коде синхронизированы с продуктовыми решениями:

| Параметр | Значение |
|----------|----------|
| Embeddings Scheduler | **13:00** (`install_zephyr_embeddings_task.ps1`, health HTML) |
| `ZEPHYR_BUGS_ROLLUP_LAST_WEEKS` | **4** (PRD, `.env.example`, fallback в коде) |
| Prod `.env` после smoke | `ZEPHYR_BUGS_DUPLICATE_EMBEDDINGS=true`, embed **0.85** |

Runbook: [PRODUCTION_RELEASE.md](PRODUCTION_RELEASE.md) § Task 4.1.7. Применение на prod-хосте — этап 6 в [TASKS.md](TASKS.md).

---

## Безопасность

- `.env` с токенами **не коммитить**.  
- При утечке токенов в чате — ротация в Jira/Confluence.

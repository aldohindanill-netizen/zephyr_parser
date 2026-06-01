# Production release checklist (v1.5.0)

After merging `feature/v1.4-full-roadmap` into `main`:

## Git

```powershell
cd C:\Users\qa\python_app\zephyr_parser
git checkout main
git pull origin main
```

Do **not** run Task Scheduler from `zephyr_parser_dev` or checkout the feature branch in production.

## `.env` (production)

Recommended additions or confirmations:

```env
ZEPHYR_ENFORCE_ENV_TOKEN=true
ZEPHYR_AUDIT_ENABLED=true
ZEPHYR_AUDIT_REASON=scheduled QA reporting
ZEPHYR_LOGVIEWER_URL_REGEX=
ZEPHYR_LOGVIEWER_STRICT=true
```

Ensure the scheduled task does **not** pass `--token` on the CLI.

## Smoke test (main pipeline)

```powershell
Start-ScheduledTask -TaskName ZephyrParserEvery30Min
```

Verify:

- `reports/audit/audit.jsonl` — `run_start` / `export_file` / `publish_confluence` / `run_finish`
- Confluence: Week folders, daily/weekly pages, **Баги** rollup (if `ZEPHYR_CONFLUENCE_PUBLISH_BUGS=true`)
- `logs/zephyr_*.log` for the run

## Task 4.1.7 — Embeddings and «Возможно дубль» (prod)

**Order (A+C):** successful embeddings smoke **before** enabling the flag in `.env`.

### 1. Scheduler (daily 13:00 local)

```powershell
cd C:\Users\qa\python_app\zephyr_parser
.\install_zephyr_embeddings_task.ps1 -DailyAt "13:00"
```

Re-run install if the task was previously registered at 02:00.

### 2. Smoke embeddings (manual)

```powershell
.\run_embeddings_scheduled.ps1
```

Verify:

- `reports/logs/embeddings_YYYY-MM-DD.log` — success
- `reports/audit/audit.jsonl` — `embeddings_start` → `embeddings_finish`, `exit_code=0`
- `reports/bugs_rollup/duplicate_embeddings_cache.json` — non-empty `vectors`
- `reports/pipeline_health.html` — green embeddings block

### 3. Prod `.env` (after smoke)

```env
ZEPHYR_BUGS_DUPLICATE_EMBEDDINGS=true
ZEPHYR_BUGS_DUPLICATE_EMBED_THRESHOLD=0.85
ZEPHYR_BUGS_DUPLICATE_TEXT_THRESHOLD=0.78
ZEPHYR_BUGS_ROLLUP_LAST_WEEKS=4
ZEPHYR_BUGS_EMBED_MODEL=paraphrase-multilingual-MiniLM-L12-v2
```

### 4. Main pipeline + Confluence

Trigger `ZephyrParserEvery30Min` or wait for the next run. Spot-check Confluence **Баги** — column **«Возможно дубль»** (link to another Jira key).

### Runbook (QA / дежурный)

| Signal | Refresh |
|--------|---------|
| Text duplicate hint | Every ~30 min (main pipeline) |
| Semantic (embeddings) hint | After daily run **13:00** (or manual `run_embeddings_scheduled.ps1`) |

- **False positives:** treat as a hint; ignore stable noise. Do **not** maintain `duplicate_overrides.json` unless policy changes.
- **Nightly failure:** no email/Telegram alerts — check `reports/pipeline_health.html` (red embeddings block). Confluence still updates (text + last good cache).
- **Threshold tune:** start **0.85**; after **2 weeks** soak, adjust `ZEPHYR_BUGS_DUPLICATE_EMBED_THRESHOLD` in `.env` if needed.

## Optional

- `ZEPHYR_REPORTS_RETENTION_DAYS` > 0 only if old report pruning is desired

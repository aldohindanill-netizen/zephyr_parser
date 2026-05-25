# Production release checklist (v1.4.0)

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

## Smoke test

```powershell
Start-ScheduledTask -TaskName ZephyrParserEvery30Min
```

Verify:

- `reports/audit/audit.jsonl` — `run_start` / `export_file` / `publish_confluence` / `run_finish`
- Confluence: Week folders, daily/weekly pages, **Баги** rollup (if `ZEPHYR_CONFLUENCE_PUBLISH_BUGS=true`)
- `logs/zephyr_*.log` for the run

## Optional (later)

- `ZEPHYR_BUGS_DUPLICATE_EMBEDDINGS=true` after `compute_bug_embeddings.py` on production rollup
- `ZEPHYR_REPORTS_RETENTION_DAYS` > 0 only if old report pruning is desired

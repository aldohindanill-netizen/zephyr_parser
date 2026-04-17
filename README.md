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
  - each test cycle section lists real test case keys (`QA-T...`) with result (step/execution status), execution date, and comment from the first non-empty step comment
  - output files:
    - `<folder_name>_<folder_id>.html` (copy/paste into Confluence editor)
    - `<folder_name>_<folder_id>.confluence.txt` (wiki-markup format)
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

Windows tip (for manual session export, if needed):

```powershell
[System.Environment]::SetEnvironmentVariable("ZEPHYR_API_TOKEN", "your_token", "User")
```

## Deployment on Amvera with Redis

This section covers how to run the report as a persistent **Redis-queue worker**
inside an Amvera container.  The worker process (`redis_runner.py`) blocks on a
Redis list; any external service (a Telegram bot, a cron job, another script)
pushes a JSON job message to trigger a report run.

### Architecture overview

```
[any client]
     │  LPUSH zephyr:jobs '{"ZEPHYR_FROM_DATE":"2026-04-01", ...}'
     ▼
  Redis (Amvera pre-configured service)
     │  BLPOP
     ▼
  zephyr_parser container (Amvera app)
  └─ redis_runner.py
       ├─ calls zephyr_weekly_report.main() in-process
       └─ RPUSH result → zephyr:results
          PUBLISH   → zephyr:done
```

Reports are written to `/data` (Amvera persistent storage).

### Step 1 — Create a Redis service in Amvera

1. In your Amvera dashboard choose **Pre-configured services → Create**.
2. Set **Type: Redis**, choose a plan no lower than *Начальный*.
3. In the service's **Variables** section add a secret:
   - Name: `REDIS_ARGS`
   - Value: `--requirepass <your-password>`
4. Note the internal hostname shown on the **Info** page — it looks like
   `amvera-<username>-run-<project-name>`.

### Step 2 — Create the bot/app project in Amvera

1. Create a new project, connect this git repository.
2. Amvera will detect `Dockerfile` and `amvera.yaml` automatically.
3. In the project's **Variables** section add at minimum:

   | Name | Value | Secret? |
   |------|-------|---------|
   | `ZEPHYR_API_TOKEN` | your Zephyr token | ✓ secret |
   | `REDIS_HOST` | `amvera-<user>-run-<redis-project>` | |
   | `REDIS_PORT` | `6379` | |
   | `REDIS_PASSWORD` | the password you set above | ✓ secret |
   | `ZEPHYR_BASE_URL` | `https://jira.example.com` | |
   | *(other `ZEPHYR_*` vars)* | as needed | |

4. After saving variables **restart the container** for them to apply.

### Step 3 — Push a job from a client

From any machine or service that can reach the Redis instance
(only allowed from other Amvera projects in the same account):

```python
import json, redis

r = redis.Redis(
    host="amvera-<user>-run-<redis-project>",
    port=6379,
    password="<your-password>",
    decode_responses=True,
)

# Minimal job — uses all ZEPHYR_* env vars already set in the container
r.lpush("zephyr:jobs", json.dumps({}))

# Override specific variables for this run only
r.lpush("zephyr:jobs", json.dumps({
    "job_id": "my-run-001",
    "ZEPHYR_FROM_DATE": "2026-04-01",
    "ZEPHYR_TO_DATE":   "2026-04-30",
}))
```

### Step 4 — Read results

```python
# Wait for the next result (blocking)
_, raw = r.blpop("zephyr:results")
result = json.loads(raw)
print(result["exit_code"])   # 0 = success
print(result["stdout"])
```

Or subscribe to the pub/sub channel:

```python
pubsub = r.pubsub()
pubsub.subscribe("zephyr:done")
for message in pubsub.listen():
    if message["type"] == "message":
        result = json.loads(message["data"])
        print(result)
```

### Worker Redis env vars reference

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | *(none)* | Redis password |
| `REDIS_DB` | `0` | Redis logical DB index |
| `REDIS_JOB_QUEUE` | `zephyr:jobs` | List key the worker pops from |
| `REDIS_RESULT_KEY` | `zephyr:results` | List key results are appended to |
| `REDIS_RESULT_CHANNEL` | `zephyr:done` | Pub/Sub channel for result notifications |
| `REDIS_RESULT_TTL` | `3600` | Seconds to keep the results list |
| `REDIS_HEARTBEAT_KEY` | `zephyr:heartbeat` | Key refreshed by the heartbeat thread |
| `REDIS_HEARTBEAT_INTERVAL` | `30` | Heartbeat refresh interval (seconds) |

### Local testing

```bash
# 1. Start a local Redis
docker run -d -p 6379:6379 redis

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set required env vars
export ZEPHYR_API_TOKEN=...
export ZEPHYR_BASE_URL=https://jira.example.com
# ... other ZEPHYR_* vars ...

# 4. Run the worker
python redis_runner.py

# 5. In another terminal, push a job
python -c "
import json, redis
r = redis.Redis(decode_responses=True)
r.lpush('zephyr:jobs', json.dumps({'job_id': 'test-1'}))
"
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

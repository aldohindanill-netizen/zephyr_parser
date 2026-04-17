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

This section covers how to run the tool as a persistent **Redis-queue worker**
inside an Amvera container.  The worker process (`redis_runner.py`) blocks on a
Redis list; any external service (a Telegram bot, a cron job, another script)
pushes a JSON job message to trigger an action — reading from Zephyr or
writing results back.

### Full pipeline overview

```
┌──────────────────────────────────────────────────────────────────────┐
│  External client (bot / cron / script)                               │
│                                                                      │
│  1.  LPUSH zephyr:jobs  {"action":"list_folders", "job_id":"f01"}   │
│  2.  LPUSH zephyr:jobs  {"action":"run_report", ...}                │
│  3.  LPUSH zephyr:jobs  {"action":"upload_result", "test_run_id":…} │
└───────────────────────────┬──────────────────────────────────────────┘
                            │
                     Redis (Amvera pre-configured service, port 6379)
                            │  BLPOP zephyr:jobs
                            ▼
              ┌─────────────────────────────────────────┐
              │   zephyr_parser container (Amvera app)  │
              │   redis_runner.py                       │
              │                                         │
              │  action = "list_folders"                │
              │    → zephyr_weekly_report --list-       │
              │      folders-json                       │
              │    → GET foldertree / folder/search     │
              │    ← folders[] JSON                     │
              │                                         │
              │  action = "run_report"                  │
              │    → zephyr_weekly_report --discover-   │
              │      folders (full pipeline)            │
              │    → GET executions, steps, cases       │
              │    ← CSV files written to /data         │
              │      stdout / stderr in result          │
              │                                         │
              │  action = "upload_result"               │
              │    → POST /testresults   (new result)   │
              │    → PUT  /testresults/… (update)       │
              │    → PUT  /…/testscriptresults/… (step) │
              │    ← Zephyr API response JSON           │
              └───────────────┬─────────────────────────┘
                              │  RPUSH  zephyr:results
                              │  PUBLISH zephyr:done
                              ▼
                      Redis result list / pub-sub channel
```

Reports and CSV files are written to `/data` (Amvera persistent storage).

### Worker actions

#### `list_folders` — get the folder tree from Zephyr

Discovers the folder tree (respecting all `ZEPHYR_TREE_*` filters) and
returns a JSON array in the result's `"folders"` key.

```python
r.lpush("zephyr:jobs", json.dumps({
    "action": "list_folders",
    "job_id": "folders-001",
}))

_, raw = r.blpop("zephyr:results")
data = json.loads(raw)
# data["folders"] → [{"id":"10545","name":"2026.04.17","parent_id":…}, …]
```

#### `run_report` — run the full weekly report

Fetches all executions for the configured folders, aggregates them, and
writes CSV reports to `/data`.  All `ZEPHYR_*` env-var overrides from the
job message are applied only for the duration of this run.

```python
r.lpush("zephyr:jobs", json.dumps({
    "action": "run_report",
    "job_id": "report-001",
    "ZEPHYR_FROM_DATE": "2026-04-01",
    "ZEPHYR_TO_DATE":   "2026-04-30",
}))

_, raw = r.blpop("zephyr:results")
data = json.loads(raw)
# data["exit_code"], data["stdout"], data["stderr"]
```

#### `upload_result` — write a test result back to Zephyr

Three sub-modes, chosen automatically based on the fields present:

| Fields present | Operation |
|----------------|-----------|
| `test_run_id` + `item_id` + `status_id` | POST new result |
| `test_run_id` + `result_id` + `status_id` | PUT update result |
| `test_run_id` + `result_id` + `step_result_id` + `status_id` | PUT update step |

```python
# POST a new test result
r.lpush("zephyr:jobs", json.dumps({
    "action": "upload_result",
    "job_id": "upload-001",
    "test_run_id": "12345",
    "item_id":     "67890",
    "status_id":   "1",          # use a status id from Zephyr
    "comment":     "Automated run via bot",
    "execution_date": "2026-04-17T10:00:00",
}))

# PUT update an existing result
r.lpush("zephyr:jobs", json.dumps({
    "action": "upload_result",
    "job_id": "upload-002",
    "test_run_id": "12345",
    "result_id":   "99999",
    "status_id":   "2",
    "comment":     "Retested — passed",
}))

_, raw = r.blpop("zephyr:results")
data = json.loads(raw)
# data["exit_code"] == 0 on success
# data["response"]  contains the Zephyr API response body
```

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
   | `ZEPHYR_BASE_URL` | `https://jira.example.com` | |
   | `REDIS_HOST` | `amvera-<user>-run-<redis-project>` | |
   | `REDIS_PORT` | `6379` | |
   | `REDIS_PASSWORD` | the password you set above | ✓ secret |
   | *(other `ZEPHYR_*` vars)* | as needed | |

4. After saving variables **restart the container** for them to apply.

### Step 3 — Read results

```python
import json, redis

r = redis.Redis(
    host="amvera-<user>-run-<redis-project>",
    port=6379,
    password="<your-password>",
    decode_responses=True,
)

# Blocking pop — waits for the next result
_, raw = r.blpop("zephyr:results")
result = json.loads(raw)
print(result["action"])      # "run_report" / "list_folders" / "upload_result"
print(result["exit_code"])   # 0 = success
```

Or subscribe to the pub/sub channel for non-blocking delivery:

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

# 3. Set required env vars (minimum to start the worker)
export ZEPHYR_API_TOKEN=your_token
export ZEPHYR_BASE_URL=https://jira.example.com
export ZEPHYR_PROJECT_ID=10904
export ZEPHYR_ROOT_FOLDER_IDS=10545
# ... other ZEPHYR_* vars from .env.example ...

# 4. Run the worker
python redis_runner.py

# 5. In another terminal — list folders
python - <<'EOF'
import json, redis
r = redis.Redis(decode_responses=True)
r.lpush("zephyr:jobs", json.dumps({"action": "list_folders", "job_id": "t1"}))
_, raw = r.blpop("zephyr:results")
import pprint; pprint.pprint(json.loads(raw))
EOF

# 6. Upload a test result
python - <<'EOF'
import json, redis
r = redis.Redis(decode_responses=True)
r.lpush("zephyr:jobs", json.dumps({
    "action": "upload_result",
    "job_id": "t2",
    "test_run_id": "12345",
    "item_id": "67890",
    "status_id": "1",
    "comment": "passed via bot",
}))
_, raw = r.blpop("zephyr:results")
import pprint; pprint.pprint(json.loads(raw))
EOF
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

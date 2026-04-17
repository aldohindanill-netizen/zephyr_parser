#!/usr/bin/env python3
"""Redis-queue worker for zephyr_weekly_report.

Lifecycle
---------
1. Connect to Redis using REDIS_HOST / REDIS_PORT / REDIS_PASSWORD env vars.
2. Block on BLPOP against REDIS_JOB_QUEUE (default "zephyr:jobs").
3. Each dequeued message is a JSON object with:
   - ``"action"`` (optional, default ``"run_report"``) — what to do:
       * ``"run_report"``   — run the weekly report
       * ``"list_folders"`` — discover the folder tree and return JSON
       * ``"upload_result"`` — POST / PUT a test result back to Zephyr
   - ``"job_id"`` (optional string) — echoed back in the result for correlation
   - ``"ZEPHYR_*"`` keys    — env-var overrides applied only for this job
   - action-specific payload keys (see below)
4. After processing, the result JSON is:
   - RPUSH-ed to REDIS_RESULT_KEY (default "zephyr:results")
   - PUBLISH-ed to REDIS_RESULT_CHANNEL (default "zephyr:done")
5. A heartbeat key is refreshed every REDIS_HEARTBEAT_INTERVAL seconds.

----------------------------------------------------------------------
Action: "run_report"
----------------------------------------------------------------------
Runs zephyr_weekly_report.main() with a reconstructed sys.argv built
from ZEPHYR_* environment variables (same logic as run_navio_folder_report.sh).

Job message example::

    {
        "action": "run_report",
        "job_id": "my-run-001",
        "ZEPHYR_FROM_DATE": "2026-04-01",
        "ZEPHYR_TO_DATE":   "2026-04-30"
    }

Result keys: exit_code, stdout, stderr, started_at, finished_at.

----------------------------------------------------------------------
Action: "list_folders"
----------------------------------------------------------------------
Runs the folder-tree discovery and returns a JSON array of folder objects.

Job message example::

    {
        "action": "list_folders",
        "job_id": "folders-001"
    }

Result keys: exit_code, folders (list), stderr, started_at, finished_at.

----------------------------------------------------------------------
Action: "upload_result"
----------------------------------------------------------------------
POSTs a new test result (or PUTs an update to an existing one) to Zephyr.

Required job message fields::

    {
        "action": "upload_result",
        "job_id": "upload-001",
        "test_run_id": "12345",
        "item_id": "67890",          # required for POST (new result)
        "status_id": "1",            # Zephyr status id
        "comment": "Automated run",  # optional
        "execution_date": "2026-04-17T10:00:00"  # optional ISO-8601
    }

To update an *existing* result (PUT), also supply::

    {
        "result_id": "99999"
    }

To update a single step within a result, also supply::

    {
        "result_id": "99999",
        "step_result_id": "55555"
    }

Result keys: exit_code, response, stderr, started_at, finished_at.
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone

import redis

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("redis_runner")


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Build a Redis client from env vars
# ---------------------------------------------------------------------------

def build_redis_client() -> redis.Redis:
    host = _env("REDIS_HOST", "localhost")
    port = _env_int("REDIS_PORT", 6379)
    password = os.environ.get("REDIS_PASSWORD") or None
    db = _env_int("REDIS_DB", 0)
    log.info("Connecting to Redis at %s:%d db=%d", host, port, db)
    client = redis.Redis(
        host=host,
        port=port,
        password=password,
        db=db,
        socket_connect_timeout=10,
        socket_timeout=None,  # BLPOP blocks indefinitely
        decode_responses=True,
    )
    client.ping()
    log.info("Redis connection OK")
    return client


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def _heartbeat_loop(client: redis.Redis, key: str, interval: int) -> None:
    ttl = interval * 3
    while True:
        try:
            client.set(key, datetime.now(timezone.utc).isoformat(), ex=ttl)
        except Exception as exc:  # noqa: BLE001
            log.warning("Heartbeat update failed: %s", exc)
        time.sleep(interval)


def start_heartbeat(client: redis.Redis) -> None:
    key = _env("REDIS_HEARTBEAT_KEY", "zephyr:heartbeat")
    interval = _env_int("REDIS_HEARTBEAT_INTERVAL", 30)
    thread = threading.Thread(
        target=_heartbeat_loop,
        args=(client, key, interval),
        daemon=True,
        name="heartbeat",
    )
    thread.start()
    log.info("Heartbeat thread started (key=%s, interval=%ds)", key, interval)


# ---------------------------------------------------------------------------
# Build sys.argv from ZEPHYR_* env vars
# (mirrors the logic of run_navio_folder_report.sh)
# ---------------------------------------------------------------------------

def _build_argv_from_env() -> list[str]:
    """Construct sys.argv for zephyr_weekly_report.parse_args() from ZEPHYR_* env vars."""
    e = os.environ.get

    base_url = e("ZEPHYR_BASE_URL", "")
    if not base_url:
        raise RuntimeError("ZEPHYR_BASE_URL env var is required but not set")

    project_id = e("ZEPHYR_PROJECT_ID", "")
    endpoint = e("ZEPHYR_ENDPOINT", "rest/tests/1.0/testrun/search")
    folder_endpoint = e("ZEPHYR_FOLDER_ENDPOINT", "rest/tests/1.0/foldertree")
    folder_search_endpoint = e(
        "ZEPHYR_FOLDER_SEARCH_ENDPOINT", "rest/tests/1.0/folder/search"
    )
    foldertree_endpoint = e(
        "ZEPHYR_FOLDERTREE_ENDPOINT",
        f"rest/tests/1.0/project/{project_id}/foldertree/testrun" if project_id else "rest/tests/1.0/foldertree",
    )
    output = e("ZEPHYR_OUTPUT", "/data/weekly_zephyr_report.csv")
    per_folder_dir = e("ZEPHYR_PER_FOLDER_DIR", "/data/reports/by_folder")
    max_results = e("ZEPHYR_MAX_RESULTS", "40")
    start_at = e("ZEPHYR_START_AT", "0")
    archived = e("ZEPHYR_ARCHIVED", "false")
    date_field = e("ZEPHYR_DATE_FIELD", "updatedOn")
    status_field = e("ZEPHYR_STATUS_FIELD", "status.name")
    discovery_mode = e("ZEPHYR_DISCOVERY_MODE", "tree")
    query_template = e(
        "ZEPHYR_QUERY_TEMPLATE",
        f"testRun.projectId IN ({project_id}) AND testRun.folderTreeId IN ({{folder_id}}) ORDER BY testRun.name ASC"
        if project_id
        else "testRun.folderTreeId IN ({folder_id}) ORDER BY testRun.name ASC",
    )
    project_query = e(
        "ZEPHYR_PROJECT_QUERY",
        f"testRun.projectId IN ({project_id}) ORDER BY testRun.name ASC"
        if project_id
        else "testRun.projectId IN ({project_id}) ORDER BY testRun.name ASC",
    )
    fields = e(
        "ZEPHYR_FIELDS",
        "id,key,name,folderId,iterationId,projectVersionId,environmentId,"
        "userKeys,environmentIds,plannedStartDate,plannedEndDate,executionTime,"
        "estimatedTime,testResultStatuses,testCaseCount,issueCount,"
        "status(id,name,i18nKey,color),customFieldValues,createdOn,createdBy,"
        "updatedOn,updatedBy,owner",
    )

    argv = [
        sys.argv[0],
        "--base-url", base_url,
        "--endpoint", endpoint,
        "--discover-folders",
        "--discovery-mode", discovery_mode,
        "--folder-endpoint", folder_endpoint,
        "--folder-search-endpoint", folder_search_endpoint,
        "--foldertree-endpoint", foldertree_endpoint,
        "--query-template", query_template,
        "--project-query", project_query,
        "--extra-param", f"fields={fields}",
        "--extra-param", f"maxResults={max_results}",
        "--extra-param", f"startAt={start_at}",
        "--extra-param", f"archived={archived}",
        "--date-field", date_field,
        "--status-field", status_field,
        "--output", output,
        "--per-folder-dir", per_folder_dir,
    ]

    if project_id:
        argv += ["--project-id", project_id]

    if discovery_mode == "executions":
        argv.append("--discover-from-executions")

    if e("ZEPHYR_TREE_LEAF_ONLY", "true").lower() == "true":
        argv.append("--tree-leaf-only")

    tree_name_regex = e("ZEPHYR_TREE_NAME_REGEX", "")
    if tree_name_regex:
        argv += ["--tree-name-regex", tree_name_regex]

    tree_root_path_regex = e("ZEPHYR_TREE_ROOT_PATH_REGEX", "")
    if tree_root_path_regex:
        argv += ["--tree-root-path-regex", tree_root_path_regex]

    if e("ZEPHYR_TREE_AUTOPROBE", "false").lower() == "true":
        argv.append("--tree-autoprobe")

    tree_source_endpoint = e("ZEPHYR_TREE_SOURCE_ENDPOINT", "")
    if tree_source_endpoint:
        argv += [
            "--tree-source-endpoint", tree_source_endpoint,
            "--tree-source-method", e("ZEPHYR_TREE_SOURCE_METHOD", "GET"),
        ]

    tree_source_query_json = e("ZEPHYR_TREE_SOURCE_QUERY_JSON", "")
    if tree_source_query_json:
        argv += ["--tree-source-query-json", tree_source_query_json]

    tree_source_body_json = e("ZEPHYR_TREE_SOURCE_BODY_JSON", "")
    if tree_source_body_json:
        argv += ["--tree-source-body-json", tree_source_body_json]

    root_folder_ids = e("ZEPHYR_ROOT_FOLDER_IDS", "")
    for rid in root_folder_ids.split(","):
        rid = rid.strip()
        if rid:
            argv += ["--root-folder-id", rid]

    allowed_root_folder_ids = e("ZEPHYR_ALLOWED_ROOT_FOLDER_IDS", "")
    for rid in allowed_root_folder_ids.split(","):
        rid = rid.strip()
        if rid:
            argv += ["--allowed-root-folder-id", rid]

    folder_name_regex = e("ZEPHYR_FOLDER_NAME_REGEX", "")
    if folder_name_regex:
        argv += ["--folder-name-regex", folder_name_regex]

    folder_name_endpoint_template = e("ZEPHYR_FOLDER_NAME_ENDPOINT_TEMPLATE", "")
    if folder_name_endpoint_template:
        argv += ["--folder-name-endpoint-template", folder_name_endpoint_template]

    folder_path_regex = e("ZEPHYR_FOLDER_PATH_REGEX", "")
    if folder_path_regex:
        argv += ["--folder-path-regex", folder_path_regex]

    from_date = e("ZEPHYR_FROM_DATE", "")
    if from_date:
        argv += ["--from-date", from_date]

    to_date = e("ZEPHYR_TO_DATE", "")
    if to_date:
        argv += ["--to-date", to_date]

    testcase_endpoint_template = e("ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE", "")
    if testcase_endpoint_template:
        argv += ["--testcase-endpoint-template", testcase_endpoint_template]

    if e("ZEPHYR_EXPORT_CYCLES_CASES", "false").lower() == "true":
        cycles_cases_output = e(
            "ZEPHYR_CYCLES_CASES_OUTPUT", "/data/reports/cycles_and_cases.csv"
        )
        argv += ["--export-cycles-cases", "--cycles-cases-output", cycles_cases_output]

    if e("ZEPHYR_SYNTHETIC_CYCLE_IDS", "false").lower() == "true":
        argv.append("--synthetic-cycle-ids")

    if e("ZEPHYR_EXPORT_CASE_STEPS", "false").lower() == "true":
        case_steps_output = e(
            "ZEPHYR_CASE_STEPS_OUTPUT", "/data/reports/case_steps.csv"
        )
        argv += ["--export-case-steps", "--case-steps-output", case_steps_output]

    if e("ZEPHYR_EXPORT_DAILY_READABLE", "false").lower() == "true":
        daily_readable_dir = e(
            "ZEPHYR_DAILY_READABLE_DIR", "/data/reports/daily_readable"
        )
        argv += ["--export-daily-readable", "--daily-readable-dir", daily_readable_dir]
        for fmt in e("ZEPHYR_DAILY_READABLE_FORMATS", "html,wiki").split(","):
            fmt = fmt.strip()
            if fmt:
                argv += ["--daily-readable-format", fmt]

    if e("ZEPHYR_DEBUG_FOLDER_FIELDS", "false").lower() == "true":
        argv.append("--debug-folder-fields")

    return argv


def _build_list_folders_argv() -> list[str]:
    """Build sys.argv for the --list-folders-json action."""
    argv = _build_argv_from_env()
    # Remove --discover-folders since --list-folders-json takes over
    argv = [a for a in argv if a != "--discover-folders"]
    argv.append("--list-folders-json")
    return argv


# ---------------------------------------------------------------------------
# Action handlers
# ---------------------------------------------------------------------------

def _apply_env_overrides(overrides: dict) -> dict:
    originals: dict[str, str | None] = {}
    for key, value in overrides.items():
        originals[key] = os.environ.get(key)
        os.environ[key] = str(value)
    return originals


def _restore_env(originals: dict) -> None:
    for key, original_value in originals.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


def _run_main_with_argv(argv: list[str]) -> tuple[int, str, str]:
    """Call zephyr_weekly_report.main() with a given sys.argv."""
    import zephyr_weekly_report  # noqa: PLC0415

    saved_argv = sys.argv[:]
    sys.argv = argv
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()
    exit_code = 1
    try:
        with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            exit_code = zephyr_weekly_report.main()
    except SystemExit as exc:
        exit_code = int(exc.code) if exc.code is not None else 0
    except Exception as exc:  # noqa: BLE001
        stderr_buf.write(f"\nUnhandled exception: {exc}\n")
        exit_code = 1
    finally:
        sys.argv = saved_argv
    return exit_code, stdout_buf.getvalue(), stderr_buf.getvalue()


def handle_run_report(overrides: dict) -> dict:
    originals = _apply_env_overrides(overrides)
    try:
        argv = _build_argv_from_env()
        exit_code, stdout_text, stderr_text = _run_main_with_argv(argv)
    except Exception as exc:  # noqa: BLE001
        exit_code, stdout_text, stderr_text = 1, "", str(exc)
    finally:
        _restore_env(originals)
    return {"exit_code": exit_code, "stdout": stdout_text, "stderr": stderr_text}


def handle_list_folders(overrides: dict) -> dict:
    originals = _apply_env_overrides(overrides)
    try:
        argv = _build_list_folders_argv()
        exit_code, stdout_text, stderr_text = _run_main_with_argv(argv)
        folders: list = []
        if exit_code == 0 and stdout_text.strip():
            try:
                folders = json.loads(stdout_text.strip())
            except json.JSONDecodeError:
                pass
    except Exception as exc:  # noqa: BLE001
        exit_code, stdout_text, stderr_text = 1, "", str(exc)
        folders = []
    finally:
        _restore_env(originals)
    return {"exit_code": exit_code, "folders": folders, "stderr": stderr_text}


def handle_upload_result(job: dict, overrides: dict) -> dict:
    """POST or PUT a test result to Zephyr.

    Required job fields: test_run_id, status_id.
    For POST (new result): item_id must also be present.
    For PUT (update):      result_id must be present.
    For step PUT:          result_id + step_result_id must be present.
    """
    import zephyr_weekly_report as zwr  # noqa: PLC0415

    originals = _apply_env_overrides(overrides)
    stderr_text = ""
    response_data = None
    exit_code = 0

    try:
        token = os.environ.get("ZEPHYR_API_TOKEN") or ""
        if not token:
            raise ValueError("ZEPHYR_API_TOKEN is required for upload_result")
        base_url = os.environ.get("ZEPHYR_BASE_URL") or ""
        if not base_url:
            raise ValueError("ZEPHYR_BASE_URL is required for upload_result")

        token_header = os.environ.get("ZEPHYR_TOKEN_HEADER", "Authorization")
        token_prefix = os.environ.get("ZEPHYR_TOKEN_PREFIX", "Bearer")
        headers = zwr.build_headers(token_header, token_prefix, token)

        test_run_id = str(job.get("test_run_id", ""))
        item_id = str(job.get("item_id", ""))
        result_id = str(job.get("result_id", ""))
        step_result_id = str(job.get("step_result_id", ""))
        status_id = str(job.get("status_id", ""))
        comment = job.get("comment") or None
        execution_date = job.get("execution_date") or None

        if not test_run_id:
            raise ValueError("upload_result requires 'test_run_id'")
        if not status_id:
            raise ValueError("upload_result requires 'status_id'")

        if step_result_id and result_id:
            response_data = zwr.put_test_step_result(
                base_url=base_url,
                headers=headers,
                test_run_id=test_run_id,
                result_id=result_id,
                step_result_id=step_result_id,
                status_id=status_id,
                comment=comment,
            )
        elif result_id:
            response_data = zwr.put_test_result(
                base_url=base_url,
                headers=headers,
                test_run_id=test_run_id,
                result_id=result_id,
                status_id=status_id,
                comment=comment,
                execution_date=execution_date,
            )
        else:
            if not item_id:
                raise ValueError(
                    "upload_result POST (new result) requires 'item_id'; "
                    "or supply 'result_id' for an update (PUT)"
                )
            response_data = zwr.post_test_result(
                base_url=base_url,
                headers=headers,
                test_run_id=test_run_id,
                item_id=item_id,
                status_id=status_id,
                comment=comment,
                execution_date=execution_date,
            )

    except Exception as exc:  # noqa: BLE001
        stderr_text = str(exc)
        exit_code = 1
    finally:
        _restore_env(originals)

    return {"exit_code": exit_code, "response": response_data, "stderr": stderr_text}


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    client = build_redis_client()
    start_heartbeat(client)

    queue_key = _env("REDIS_JOB_QUEUE", "zephyr:jobs")
    result_key = _env("REDIS_RESULT_KEY", "zephyr:results")
    result_channel = _env("REDIS_RESULT_CHANNEL", "zephyr:done")
    result_ttl = _env_int("REDIS_RESULT_TTL", 3600)

    log.info("Worker ready — listening on queue '%s'", queue_key)

    while True:
        try:
            item = client.blpop(queue_key, timeout=0)
        except redis.exceptions.ConnectionError as exc:
            log.error("Redis connection lost: %s — reconnecting in 5s", exc)
            time.sleep(5)
            try:
                client = build_redis_client()
                start_heartbeat(client)
            except Exception as reconnect_exc:  # noqa: BLE001
                log.error("Reconnect failed: %s", reconnect_exc)
            continue

        if item is None:
            continue

        _queue, raw_message = item
        log.info("Received job message (%d bytes)", len(raw_message))

        try:
            job = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            log.error("Invalid JSON in job message: %s — skipping", exc)
            continue

        job_id = job.pop("job_id", None)
        action = job.pop("action", "run_report")
        overrides = {k: v for k, v in job.items() if k.startswith("ZEPHYR_")}
        # Remove overrides from job so action handlers only see payload keys
        for k in list(overrides):
            job.pop(k)

        log.info("Starting action=%s (job_id=%s, overrides=%s)", action, job_id, list(overrides.keys()))
        started_at = datetime.now(timezone.utc)

        if action == "run_report":
            result = handle_run_report(overrides)
        elif action == "list_folders":
            result = handle_list_folders(overrides)
        elif action == "upload_result":
            result = handle_upload_result(job, overrides)
        else:
            result = {"exit_code": 1, "stderr": f"Unknown action: {action!r}"}

        finished_at = datetime.now(timezone.utc)

        log.info(
            "Action=%s finished (job_id=%s, exit_code=%d, duration=%.1fs)",
            action,
            job_id,
            result.get("exit_code", -1),
            (finished_at - started_at).total_seconds(),
        )

        result_payload = json.dumps(
            {
                "job_id": job_id,
                "action": action,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                **result,
            },
            ensure_ascii=False,
        )

        try:
            client.rpush(result_key, result_payload)
            client.expire(result_key, result_ttl)
            client.publish(result_channel, result_payload)
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to push result to Redis: %s", exc)


if __name__ == "__main__":
    main()

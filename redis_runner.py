#!/usr/bin/env python3
"""Redis-queue worker for zephyr_weekly_report.

Lifecycle
---------
1. Connect to Redis using REDIS_HOST / REDIS_PORT / REDIS_PASSWORD env vars.
2. Block on BLPOP against REDIS_JOB_QUEUE (default "zephyr:jobs").
3. Each dequeued message is a JSON object whose keys map to ZEPHYR_* env-var
   overrides.  An empty object ``{}`` runs the report with the current
   environment unchanged.
4. The report is run in-process by calling zephyr_weekly_report.main()
   after patching sys.argv from the env-derived arguments.
5. Result (exit_code, stdout, stderr) is RPUSH-ed to REDIS_RESULT_KEY
   (default "zephyr:results") as a JSON string and also published to
   REDIS_RESULT_CHANNEL (default "zephyr:done").
6. A heartbeat key (REDIS_HEARTBEAT_KEY, default "zephyr:heartbeat") is
   refreshed every REDIS_HEARTBEAT_INTERVAL seconds (default 30) so
   monitoring tools can detect a dead worker.

Job message format (JSON)
-------------------------
Any subset of the following keys may be present; missing keys fall back to the
current process environment::

    {
        "ZEPHYR_API_TOKEN": "...",
        "ZEPHYR_BASE_URL": "https://jira.example.com",
        "ZEPHYR_FROM_DATE": "2026-04-01",
        "ZEPHYR_TO_DATE": "2026-04-30",
        ... any ZEPHYR_* variable ...
    }

A special key ``"job_id"`` (string) is echoed back in the result payload for
correlation; it is not forwarded to the report script.

Result payload (JSON pushed to REDIS_RESULT_KEY / published to channel)
-----------------------------------------------------------------------
::

    {
        "job_id": "<echoed from job or null>",
        "exit_code": 0,
        "stdout": "...",
        "stderr": "...",
        "started_at": "2026-04-17T12:00:00",
        "finished_at": "2026-04-17T12:01:23"
    }
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
        socket_timeout=None,  # BLPOP will block indefinitely
        decode_responses=True,
    )
    client.ping()
    log.info("Redis connection OK")
    return client


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def _heartbeat_loop(client: redis.Redis, key: str, interval: int) -> None:
    """Run forever, refreshing the heartbeat key every *interval* seconds."""
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
# Run the report
# ---------------------------------------------------------------------------

def _build_argv(overrides: dict) -> list[str]:
    """Translate env-var overrides from the job into sys.argv for the report."""
    # We rely on run_navio_folder_report.sh logic but re-implement it here in
    # pure Python so we can run in-process without spawning a subprocess.  The
    # approach: temporarily update os.environ with the override values, then
    # call zephyr_weekly_report.parse_args() which reads from os.environ via
    # defaults already wired in the script.  The simplest path is to forward
    # overrides as actual environment variables for the duration of the call.
    return list(overrides.items())  # returned for logging only


def _apply_env_overrides(overrides: dict) -> dict:
    """Apply overrides to os.environ and return a dict of original values."""
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


def run_report(overrides: dict) -> tuple[int, str, str]:
    """
    Run zephyr_weekly_report.main() with optional env overrides.

    Returns (exit_code, stdout_text, stderr_text).
    """
    # Import here so the module is only loaded once (cached by Python)
    import zephyr_weekly_report  # noqa: PLC0415

    originals = _apply_env_overrides(overrides)
    saved_argv = sys.argv[:]
    # Build argv: the script reads most config from env, so we only need the
    # minimal required positional that parse_args expects (none, actually,
    # since all args have defaults).  We do need to reconstruct the full arg
    # list the same way run_navio_folder_report.sh does, but since we're
    # calling main() which calls parse_args() internally, we just need to
    # ensure sys.argv doesn't carry leftover arguments from our own launcher.
    sys.argv = [sys.argv[0]]

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
        _restore_env(originals)

    return exit_code, stdout_buf.getvalue(), stderr_buf.getvalue()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    client = build_redis_client()
    start_heartbeat(client)

    queue_key = _env("REDIS_JOB_QUEUE", "zephyr:jobs")
    result_key = _env("REDIS_RESULT_KEY", "zephyr:results")
    result_channel = _env("REDIS_RESULT_CHANNEL", "zephyr:done")
    result_ttl = _env_int("REDIS_RESULT_TTL", 3600)  # seconds to keep results

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
            # timeout expired (shouldn't happen with timeout=0 but be safe)
            continue

        _queue, raw_message = item
        log.info("Received job message (%d bytes)", len(raw_message))

        try:
            job = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            log.error("Invalid JSON in job message: %s — skipping", exc)
            continue

        job_id = job.pop("job_id", None)
        overrides = {k: v for k, v in job.items() if k.startswith("ZEPHYR_")}
        unknown_keys = [k for k in job if not k.startswith("ZEPHYR_")]
        if unknown_keys:
            log.warning("Job contains unrecognized keys (ignored): %s", unknown_keys)

        log.info("Starting report (job_id=%s, overrides=%s)", job_id, list(overrides.keys()))
        started_at = datetime.now(timezone.utc)
        exit_code, stdout_text, stderr_text = run_report(overrides)
        finished_at = datetime.now(timezone.utc)

        log.info(
            "Report finished (job_id=%s, exit_code=%d, duration=%.1fs)",
            job_id,
            exit_code,
            (finished_at - started_at).total_seconds(),
        )

        result_payload = json.dumps(
            {
                "job_id": job_id,
                "exit_code": exit_code,
                "stdout": stdout_text,
                "stderr": stderr_text,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
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

#!/usr/bin/env python3
"""Generate a weekly Zephyr test execution summary.

The script fetches paginated execution data from a Zephyr API endpoint,
aggregates executions by ISO week (Monday start) using raw API statuses,
and computes normalized pass rate for reporting.
"""

from __future__ import annotations

import argparse
import atexit
import base64
import concurrent.futures
import io
import csv
import io
import html
import json
import math
import mimetypes
import os
import re
import signal
import sys
import threading
import time
import uuid
import zlib
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, TextIO


DEFAULT_DATE_FIELDS = [
    "executedOn",
    "executionDate",
    "executedAt",
    "createdOn",
    "createdAt",
    "updatedOn",
    "updatedAt",
]

DEFAULT_STATUS_FIELDS = [
    "testExecutionStatus.name",
    "status.name",
    "status",
    "executionStatus",
    "result",
]


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _env_csv_values(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return list(default)
    return [part.strip().lower() for part in str(raw).split(',') if part.strip()]


@dataclass
class FolderNode:
    folder_id: str
    folder_name: str
    parent_id: str | None
    full_path: str = ""
    is_leaf: bool = False


@dataclass
class ConfluencePublishConfig:
    base_url: str
    user: str
    api_token: str
    space_key: str
    parent_page_id: str | None = None
    bugs_parent_page_id: str | None = None
    bugs_parent_title: str = "Баги"
    week_folder_title_template: str = "Week {year}-w{week:02d}"
    title_prefix: str = ""
    api_prefix: str = "rest/api"
    auth_scheme: str = "basic"
    update_existing: bool = False
    publish_daily: bool = False
    publish_weekly: bool = False
    publish_bugs: bool = False


@dataclass
class ConfluencePublishRoots:
    root_parent: str | None = None
    bugs_parent: str | None = None


class ConfluenceWeekParentCache:
    """Lazy-create Confluence week folder pages (Week wNN) under the root parent."""

    def __init__(self, cfg: ConfluencePublishConfig, root_parent_id: str | None) -> None:
        self.cfg = cfg
        self.root_parent_id = root_parent_id
        self._by_week: dict[date, str] = {}

    def ensure(self, week_start: date) -> str:
        cached = self._by_week.get(week_start)
        if cached:
            return cached
        if not self.root_parent_id:
            raise RuntimeError(
                "Confluence week folder requires ZEPHYR_CONFLUENCE_PARENT_PAGE_ID."
            )
        title = _confluence_week_folder_title(self.cfg, week_start)
        page_id = _confluence_ensure_section_page(
            self.cfg,
            root_parent_page_id=self.root_parent_id,
            explicit_page_id=None,
            section_title=title,
        )
        if not page_id:
            raise RuntimeError(f"Confluence week folder create failed for '{title}'")
        self._by_week[week_start] = page_id
        return page_id


@dataclass
class StepTiming:
    name: str
    duration_seconds: float
    detail: str = ""


class TimingRecorder:
    def __init__(self) -> None:
        self.records: list[StepTiming] = []

    def record(self, name: str, duration_seconds: float, detail: str = "") -> None:
        self.records.append(StepTiming(name, duration_seconds, detail))
        suffix = f" ({detail})" if detail else ""
        print(f"Timing: {name} took {duration_seconds:.2f}s{suffix}")

    def summarize(self, limit: int = 12) -> None:
        if not self.records:
            return
        total = sum(item.duration_seconds for item in self.records)
        print(f"Timing summary: {len(self.records)} step(s), total measured {total:.2f}s")
        for item in sorted(
            self.records, key=lambda rec: rec.duration_seconds, reverse=True
        )[:limit]:
            suffix = f" ({item.detail})" if item.detail else ""
            print(f"- {item.name}: {item.duration_seconds:.2f}s{suffix}")


class _TimedStep:
    def __init__(self, recorder: TimingRecorder, name: str, detail: str = "") -> None:
        self.recorder = recorder
        self.name = name
        self.detail = detail
        self.started_at = 0.0

    def __enter__(self) -> "_TimedStep":
        self.started_at = time.perf_counter()
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        self.recorder.record(
            self.name, time.perf_counter() - self.started_at, self.detail
        )


def timed_step(recorder: TimingRecorder, name: str, detail: str = "") -> _TimedStep:
    return _TimedStep(recorder, name, detail)


def _bounded_worker_count(raw: int | None, item_count: int | None = None) -> int:
    try:
        workers = int(raw or 1)
    except (TypeError, ValueError):
        workers = 1
    workers = max(1, workers)
    if item_count is not None and item_count > 0:
        workers = min(workers, item_count)
    return workers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Zephyr executions and build weekly pass/fail report."
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("ZEPHYR_BASE_URL"),
        help=(
            "Zephyr base URL, e.g. https://api.zephyrscale.smartbear.com "
            "(default: ZEPHYR_BASE_URL from environment or .env)"
        ),
    )
    parser.add_argument(
        "--endpoint",
        default=os.getenv("ZEPHYR_ENDPOINT", "/v2/testexecutions"),
        help="API endpoint path (default: ZEPHYR_ENDPOINT or /v2/testexecutions)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="API token value (optional; otherwise ZEPHYR_API_TOKEN env var is used)",
    )
    parser.add_argument(
        "--token-header",
        default="Authorization",
        help="Header used for token (default: Authorization)",
    )
    parser.add_argument(
        "--token-prefix",
        default="Bearer",
        help='Token prefix (default: "Bearer"). Use empty string for raw token.',
    )
    parser.add_argument(
        "--extra-param",
        action="append",
        default=[],
        help='Additional query parameters in key=value format. Can repeat.',
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Page size for paginated APIs (default: 100)",
    )
    parser.add_argument(
        "--folder-workers",
        type=int,
        default=_env_int("ZEPHYR_FOLDER_WORKERS", 1),
        help=(
            "Number of folders to fetch/process concurrently in tree discovery mode "
            "(default: ZEPHYR_FOLDER_WORKERS or 1)."
        ),
    )
    parser.add_argument(
        "--detail-workers",
        type=int,
        default=_env_int("ZEPHYR_DETAIL_WORKERS", 1),
        help=(
            "Number of test-run item detail requests to process concurrently "
            "(default: ZEPHYR_DETAIL_WORKERS or 1)."
        ),
    )
    parser.add_argument(
        "--from-date",
        default=None,
        help="Start date inclusive (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        help="End date inclusive (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--rolling-days",
        type=int,
        default=_env_int("ZEPHYR_ROLLING_DAYS", 0),
        help=(
            "If >0, set from/to to a rolling window ending today (inclusive), "
            "overriding --from-date and --to-date, and filter aggregated readable "
            "report_data by resolved folder day. Default 0 (off); ZEPHYR_ROLLING_DAYS."
        ),
    )
    parser.add_argument(
        "--date-field",
        action="append",
        default=[],
        help=(
            "Field path for execution date (dot notation). "
            "Can repeat; if omitted, built-in date fields are used."
        ),
    )
    parser.add_argument(
        "--status-field",
        action="append",
        default=[],
        help=(
            "Field path for execution status (dot notation). "
            "Can repeat; if omitted, built-in status fields are used."
        ),
    )
    parser.add_argument(
        "--output",
        default="weekly_zephyr_report.csv",
        help="Output CSV path (default: weekly_zephyr_report.csv)",
    )
    parser.add_argument(
        "--discover-folders",
        action="store_true",
        help="Discover folder tree from API and aggregate per discovered folder.",
    )
    parser.add_argument(
        "--discovery-mode",
        choices=("tree", "executions"),
        default="tree",
        help="Folder discovery mode (default: tree).",
    )
    parser.add_argument(
        "--folder-endpoint",
        default="/rest/tests/1.0/foldertree",
        help="Folder tree API endpoint used for discovery mode.",
    )
    parser.add_argument(
        "--folder-search-endpoint",
        default="/rest/tests/1.0/folder/search",
        help="Folder search endpoint for tree discovery fallback.",
    )
    parser.add_argument(
        "--foldertree-endpoint",
        default="/rest/tests/1.0/foldertree",
        help="Folder tree endpoint for tree discovery fallback.",
    )
    parser.add_argument(
        "--project-id",
        default=None,
        help="Optional project id for folder discovery API.",
    )
    parser.add_argument(
        "--root-folder-id",
        action="append",
        default=[],
        help="Root folder id for recursive discovery. Can repeat.",
    )
    parser.add_argument(
        "--query-template",
        default="testRun.folderTreeId IN ({folder_id})",
        help="Template for query per folder in discovery mode; must contain {folder_id}.",
    )
    parser.add_argument(
        "--per-folder-dir",
        default="reports/by_folder",
        help="Directory path for per-folder CSV files in discovery mode.",
    )
    parser.add_argument(
        "--continue-on-folder-error",
        action="store_true",
        help="Continue with other folders if one folder request fails.",
    )
    parser.add_argument(
        "--discover-from-executions",
        action="store_true",
        help="Discover folders from fetched executions (uses folderId in test runs).",
    )
    parser.add_argument(
        "--project-query",
        default="testRun.projectId IN ({project_id}) ORDER BY testRun.name ASC",
        help="Query used by --discover-from-executions; supports {project_id}.",
    )
    parser.add_argument(
        "--allowed-root-folder-id",
        action="append",
        default=[],
        help="Allowed root folder id(s) for execution-discovery mode. Can repeat or be CSV.",
    )
    parser.add_argument(
        "--folder-name-regex",
        default=None,
        help="Regex filter for folderName in execution-discovery mode.",
    )
    parser.add_argument(
        "--folder-name-endpoint-template",
        action="append",
        default=[],
        help=(
            "Endpoint template to resolve folder name by id, e.g. "
            "'rest/tests/1.0/folder/{folder_id}'. Can repeat."
        ),
    )
    parser.add_argument(
        "--debug-folder-fields",
        action="store_true",
        help="Print sample folder fields (folderId/folderName/iterationId) in execution-discovery mode.",
    )
    parser.add_argument(
        "--folder-path-regex",
        default=None,
        help="Regex filter for resolved folder full path (fullName).",
    )
    parser.add_argument(
        "--tree-leaf-only",
        action="store_true",
        help="In tree discovery mode include only leaf folders.",
    )
    parser.add_argument(
        "--tree-name-regex",
        default=None,
        help="Regex filter for folder name in tree discovery mode.",
    )
    parser.add_argument(
        "--tree-root-path-regex",
        default=None,
        help="Regex filter for folder full path in tree discovery mode.",
    )
    parser.add_argument(
        "--tree-autoprobe",
        action="store_true",
        help="Auto-probe candidate tree endpoints if configured endpoints fail.",
    )
    parser.add_argument(
        "--tree-source-endpoint",
        default=None,
        help="Priority tree endpoint path (instance-specific backend source).",
    )
    parser.add_argument(
        "--tree-source-method",
        choices=("GET", "POST"),
        default="GET",
        help="HTTP method for --tree-source-endpoint (default: GET).",
    )
    parser.add_argument(
        "--tree-source-query-json",
        default=None,
        help="Optional JSON object with query params for custom tree source.",
    )
    parser.add_argument(
        "--tree-source-body-json",
        default=None,
        help="Optional JSON object body for custom tree source (POST).",
    )
    parser.add_argument(
        "--export-cycles-cases",
        action="store_true",
        help="Export detailed rows: folder -> test cycle -> test case.",
    )
    parser.add_argument(
        "--cycles-cases-output",
        default="reports/cycles_and_cases.csv",
        help="CSV path for detailed cycle/case export.",
    )
    parser.add_argument(
        "--testcase-endpoint-template",
        action="append",
        default=[],
        help=(
            "Endpoint template to fetch test cases per cycle, e.g. "
            "'rest/tests/1.0/testrun/{cycle_id}/testcase/search'. Can repeat."
        ),
    )
    parser.add_argument(
        "--synthetic-cycle-ids",
        action="store_true",
        help="Generate deterministic synthetic cycle_id when API does not provide one.",
    )
    parser.add_argument(
        "--export-case-steps",
        action="store_true",
        help="Export detailed step-level statuses for test cases.",
    )
    parser.add_argument(
        "--case-steps-output",
        default="reports/case_steps.csv",
        help="CSV path for case step export.",
    )
    parser.add_argument(
        "--export-daily-readable",
        action="store_true",
        help="Export one readable daily report per folder/day.",
    )
    parser.add_argument(
        "--daily-readable-dir",
        default="reports/daily_readable",
        help="Directory for daily readable reports.",
    )
    parser.add_argument(
        "--daily-readable-format",
        action="append",
        choices=("html", "wiki"),
        default=[],
        help="Readable report format. Can repeat; default is both html and wiki.",
    )
    parser.add_argument(
        "--export-build-log-report",
        dest="export_build_log_report",
        action="store_true",
        default=_env_bool("ZEPHYR_EXPORT_BUILD_LOG_REPORT", True),
        help=(
            "Per-Jira build log HTML/wiki export is on by default in folder discovery mode "
            "(one page per issue; see --no-export-build-log-report). "
            "Passing this flag is optional and only forces the feature on."
        ),
    )
    parser.add_argument(
        "--no-export-build-log-report",
        dest="export_build_log_report",
        action="store_false",
        help=(
            "Disable per-Jira build log export (standalone pages with "
            "\"Воспроизводится на nightly-dev-…\" blocks and logviewer links)."
        ),
    )
    parser.add_argument(
        "--build-log-report-dir",
        default=os.getenv("ZEPHYR_BUILD_LOG_REPORT_DIR", "reports/build_log_reports"),
        help="Output directory for standalone build/log reports.",
    )
    parser.add_argument(
        "--build-log-report-format",
        action="append",
        choices=("html", "wiki"),
        default=[],
        help="Standalone build log report format. Can repeat; default is both html and wiki.",
    )
    parser.add_argument(
        "--cycle-progress-output",
        default=None,
        help="Optional CSV path for per-cycle progress export (folder discovery mode).",
    )
    parser.add_argument(
        "--weekly-cycle-matrix-output",
        default=None,
        help="Optional CSV path for weekly cycle matrix export (folder discovery mode).",
    )
    parser.add_argument(
        "--export-weekly-readable",
        action="store_true",
        help="Export weekly readable HTML/wiki reports (folder discovery mode).",
    )
    parser.add_argument(
        "--weekly-readable-dir",
        default="reports/weekly_readable",
        help="Directory for weekly readable reports.",
    )
    parser.add_argument(
        "--weekly-readable-format",
        action="append",
        choices=("html", "wiki"),
        default=[],
        help="Weekly readable format. Can repeat; default is both html and wiki.",
    )
    parser.add_argument(
        "--readable-template-dir",
        default=None,
        help=(
            "Directory with report_templates/readable layout for HTML/wiki snippets. "
            "Overrides env ZEPHYR_READABLE_TEMPLATE_DIR when set. "
            "Daily Confluence publish inserts Zephyr TEST_RESULTS_SUMMARY_BY_STATUS from cycle keys in HTML "
            "(optional overrides: ZEPHYR_CONFLUENCE_ZEPHYR_APP_ID, ZEPHYR_PROJECT_ID, ZEPHYR_JIRA_PROJECT_KEY, "
            "ZEPHYR_JIRA_PROJECT_DISPLAY_NAME). "
            "Daily wiki only: optional ZEPHYR_CONFLUENCE_TEST_EXEC_MACRO overrides the execution macro snippet."
        ),
    )
    parser.add_argument(
        "--weekly-readable-per-folder",
        action="store_true",
        help=(
            "Also emit weekly readable reports per folder (separate files; "
            "does not replace merged all-folders output)."
        ),
    )
    parser.add_argument(
        "--regenerate-last-7-days",
        action="store_true",
        help=(
            "Same as --regenerate-last-n-days 7: last 7 calendar days (today inclusive) "
            "and rolling-days=7 when rolling-days is not already positive."
        ),
    )
    parser.add_argument(
        "--regenerate-last-n-days",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Set --from-date/--to-date to the last N calendar days (today inclusive) "
            "and apply --rolling-days=N when rolling-days is not already positive. "
            "Cannot combine with --from-date/--to-date. "
            "Same behavior via env: ZEPHYR_REGENERATE_LAST_N_DAYS (no flag required)."
        ),
    )
    parser.add_argument(
        "--create-folder-first",
        action="store_true",
        help="Before report generation, ensure target Zephyr folder exists (create if missing).",
    )
    parser.add_argument(
        "--create-folder-name",
        default=None,
        help="Folder name to create when --create-folder-first is enabled.",
    )
    parser.add_argument(
        "--create-folder-name-template",
        default=None,
        help=(
            "strftime template for folder name generation, e.g. '%%Y.%%m.%%d'. "
            "Used only when --create-folder-name is not provided."
        ),
    )
    parser.add_argument(
        "--create-folder-parent-id",
        default=None,
        help="Optional parent folder id for folder creation.",
    )
    parser.add_argument(
        "--create-folder-endpoint",
        default="/rest/tests/1.0/folder",
        help="Folder creation endpoint used with POST.",
    )
    parser.add_argument(
        "--create-folder-name-field",
        default="name",
        help="Field name in create-folder request body for folder name (default: name).",
    )
    parser.add_argument(
        "--create-folder-project-id-field",
        default="projectId",
        help="Field name in create-folder request body for project id (default: projectId).",
    )
    parser.add_argument(
        "--create-folder-parent-id-field",
        default="parentId",
        help="Field name in create-folder request body for parent id (default: parentId).",
    )
    parser.add_argument(
        "--create-folder-body-json",
        default=None,
        help="Optional extra JSON object merged into create-folder request body.",
    )
    parser.add_argument(
        "--create-folder-dry-run",
        action="store_true",
        help="Print folder creation payload without sending POST request.",
    )
    parser.add_argument(
        "--create-folder-use-as-root",
        action="store_true",
        help="Use created/existing folder id as the only --root-folder-id for this run.",
    )
    parser.add_argument(
        "--loop-interval-minutes",
        type=int,
        default=None,
        help=(
            "Run continuously with this many minutes between runs "
            "(default: ZEPHYR_LOOP_INTERVAL_MINUTES, off when unset)."
        ),
    )
    parser.add_argument(
        "--run-lock-file",
        default=os.getenv("ZEPHYR_RUN_LOCK_FILE"),
        help="Optional lock file path to prevent overlapping runs.",
    )
    return parser.parse_args()


def _resolve_loop_interval_minutes(args: argparse.Namespace) -> int | None:
    if args.loop_interval_minutes is not None:
        if args.loop_interval_minutes <= 0:
            raise ValueError("--loop-interval-minutes must be a positive integer")
        return args.loop_interval_minutes
    raw = os.getenv("ZEPHYR_LOOP_INTERVAL_MINUTES")
    if raw is None or not str(raw).strip():
        return None
    try:
        v = int(str(raw).strip())
    except ValueError:
        return None
    if v <= 0:
        return None
    return v


_run_lock_handle: Any = None


def _try_acquire_run_lock(lock_path: str) -> bool:
    """Return True if this process holds the lock; False if another instance is running."""
    global _run_lock_handle
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform == "win32":
        import msvcrt

        if not path.exists() or path.stat().st_size == 0:
            path.write_bytes(b"\0")
        binary = path.open("r+b")
        try:
            msvcrt.locking(binary.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            binary.close()
            return False
        _run_lock_handle = binary
        return True
    import fcntl

    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return False
    _run_lock_handle = handle
    return True


def _release_run_lock() -> None:
    global _run_lock_handle
    if _run_lock_handle is None:
        return
    try:
        if sys.platform == "win32":
            import msvcrt

            try:
                msvcrt.locking(_run_lock_handle.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl

            try:
                fcntl.flock(_run_lock_handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
    finally:
        try:
            _run_lock_handle.close()
        except OSError:
            pass
        _run_lock_handle = None


def _interruptible_sleep(total_seconds: float, stop: threading.Event) -> None:
    if total_seconds <= 0:
        return
    deadline = time.monotonic() + total_seconds
    while time.monotonic() < deadline:
        if stop.is_set():
            return
        remaining = deadline - time.monotonic()
        time.sleep(min(1.0, remaining))


def _run_loop(args: argparse.Namespace, interval_minutes: int) -> int:
    stop = threading.Event()

    def _handle_stop(_signum: int, _frame: Any) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _handle_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_stop)

    iteration = 0
    while not stop.is_set():
        iteration += 1
        print(f"Loop iteration {iteration} starting", file=sys.stderr)
        run_once(args)
        if stop.is_set():
            break
        print(f"Sleeping {interval_minutes} minutes until next run", file=sys.stderr)
        _interruptible_sleep(float(interval_minutes * 60), stop)
    print("Loop stopped (interrupt or signal)", file=sys.stderr)
    return 0


def parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Expected YYYY-MM-DD.") from exc


def parse_datetime(value: str) -> datetime:
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        # Fallback for common API formats without timezone.
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
    raise ValueError(f"Unable to parse datetime '{value}'")


def _parse_int_env(name: str, default: int, minimum: int = 1, maximum: int = 1000) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, parsed))


def _to_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return parse_datetime(text)
    except ValueError:
        return None


def _coerce_utc_naive(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


_SUMMARY_DATE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\b(\d{4})[._-](\d{2})[._-](\d{2})\b"),
    re.compile(r"\b(\d{2})[._-](\d{2})[._-](\d{4})\b"),
]


def parse_date_from_summary(summary: str | None) -> datetime | None:
    text = str(summary or "").strip()
    if not text:
        return None
    for pattern in _SUMMARY_DATE_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        g1, g2, g3 = match.groups()
        try:
            if len(g1) == 4:
                return datetime(int(g1), int(g2), int(g3))
            return datetime(int(g3), int(g2), int(g1))
        except ValueError:
            continue
    return None


def _extract_text_nodes(value: Any) -> list[str]:
    out: list[str] = []
    if isinstance(value, str):
        text = value.strip()
        if text:
            out.append(text)
        return out
    if isinstance(value, dict):
        text_value = value.get("text")
        if isinstance(text_value, str):
            text = text_value.strip()
            if text:
                out.append(text)
        for nested in value.values():
            out.extend(_extract_text_nodes(nested))
        return out
    if isinstance(value, list):
        for item in value:
            out.extend(_extract_text_nodes(item))
    return out


def _description_to_text(description: Any) -> str:
    if isinstance(description, str):
        return description
    if description is None:
        return ""
    parts = _extract_text_nodes(description)
    if parts:
        return "\n".join(parts)
    try:
        return json.dumps(description, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        return str(description)


_POINT_A_LINE_RE = re.compile(
    r"^\s*(?:[-*]\s*)?(?:[\(\[]\s*)?[AАaа](?:\s*[\)\]])?\s*[\)\.\:\-]\s*(.+?)\s*$"
)
_POINT_HEADER_RE = re.compile(
    r"^\s*(?:[-*]\s*)?(?:[\(\[]\s*)?[A-Za-zА-Яа-я](?:\s*[\)\]])?\s*[\)\.\:\-]\s*"
)
_BUILDS_SECTION_HEADER_RE = re.compile(r"(?:сборк|build)", re.IGNORECASE)
_BUILD_PREFIX_RE = re.compile(
    r"^\s*(?:название\s+сборки|сборка|build(?:\s+name)?)\s*[:\-]\s*",
    re.IGNORECASE,
)


def extract_build_from_description_point_a(description: Any) -> str:
    text = _description_to_text(description).replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        return ""
    lines = text.split("\n")
    section_start = 0
    for idx, raw_line in enumerate(lines):
        if _BUILDS_SECTION_HEADER_RE.search(raw_line or ""):
            section_start = idx
            break
    for idx, raw_line in enumerate(lines[section_start:], start=section_start):
        match = _POINT_A_LINE_RE.match(raw_line)
        if not match:
            continue
        fragments = [match.group(1).strip()]
        for next_line in lines[idx + 1 :]:
            clean_next = next_line.strip()
            if not clean_next:
                if fragments and fragments[-1]:
                    break
                continue
            if _POINT_HEADER_RE.match(clean_next) or _BUILDS_SECTION_HEADER_RE.search(clean_next):
                break
            fragments.append(clean_next)
        candidate = " ".join(part for part in fragments if part).strip()
        candidate = _BUILD_PREFIX_RE.sub("", candidate).strip()
        candidate = candidate.strip(" .,:;")
        if candidate:
            return candidate
    return ""


def pick_latest_issue(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    best_issue: dict[str, Any] | None = None
    best_key: tuple[datetime, datetime] | None = None
    min_dt = datetime.min
    for issue in candidates:
        if not isinstance(issue, dict):
            continue
        fields = issue.get("fields") or {}
        if not isinstance(fields, dict):
            fields = {}
        created_dt = _coerce_utc_naive(_to_datetime(fields.get("created")))
        summary_dt = _coerce_utc_naive(parse_date_from_summary(fields.get("summary")))
        effective_dt = max(created_dt or min_dt, summary_dt or min_dt)
        sort_key = (effective_dt, created_dt or min_dt)
        if best_key is None or sort_key > best_key:
            best_key = sort_key
            best_issue = issue
    return best_issue


def pick_latest_issue_with_point_a_build(
    candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    """Pick latest Jira issue among those that have build in description point A.

    Returns (issue, build_name). If no candidate has build in point A, returns
    (None, "") so caller can fallback to generic latest-issue selection.
    """
    best_issue: dict[str, Any] | None = None
    best_build = ""
    best_key: tuple[datetime, datetime] | None = None
    min_dt = datetime.min
    for issue in candidates:
        if not isinstance(issue, dict):
            continue
        fields = issue.get("fields") or {}
        if not isinstance(fields, dict):
            fields = {}
        build_name = extract_build_from_description_point_a(fields.get("description"))
        if not build_name:
            continue
        created_dt = _coerce_utc_naive(_to_datetime(fields.get("created")))
        summary_dt = _coerce_utc_naive(parse_date_from_summary(fields.get("summary")))
        effective_dt = max(created_dt or min_dt, summary_dt or min_dt)
        sort_key = (effective_dt, created_dt or min_dt)
        if best_key is None or sort_key > best_key:
            best_key = sort_key
            best_issue = issue
            best_build = build_name
    return best_issue, best_build


_AUTOFLEET_ABTEST_BUILD_CACHE: dict[str, str] = {}
_AUTOFLEET_ABTEST_CONTEXT_CACHE: dict[str, dict[str, Any]] = {}


def fetch_autofleet_abtest_candidates(
    *,
    base_url: str,
    auth_headers: dict[str, str] | None,
) -> list[dict[str, Any]]:
    if not base_url or not auth_headers:
        return []
    jql = str(os.getenv("ZEPHYR_AUTOFLEET_ABTEST_JQL", "labels = autofleet_abtest")).strip()
    if not jql:
        return []
    max_results = _parse_int_env("ZEPHYR_AUTOFLEET_ABTEST_MAX_RESULTS", 100, 1, 500)
    try:
        payload = request_json(
            base_url,
            "/rest/api/2/search",
            auth_headers,
            params={
                "jql": jql,
                "fields": "summary,description,created",
                "maxResults": str(max_results),
            },
        )
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"[weekly] Autofleet ABTest Jira lookup failed: {exc}\n")
        return []
    issues = payload.get("issues") if isinstance(payload, dict) else None
    if not isinstance(issues, list):
        return []
    return [issue for issue in issues if isinstance(issue, dict)]


def fetch_autofleet_abtest_build_name(
    *,
    base_url: str,
    auth_headers: dict[str, str] | None,
    issues: list[dict[str, Any]] | None = None,
) -> str:
    enabled = _parse_bool_env(os.getenv("ZEPHYR_AUTOFLEET_ABTEST_ENABLED", "true"))
    if not enabled:
        return ""
    jql = str(os.getenv("ZEPHYR_AUTOFLEET_ABTEST_JQL", "labels = autofleet_abtest")).strip()
    max_results = _parse_int_env("ZEPHYR_AUTOFLEET_ABTEST_MAX_RESULTS", 100, 1, 500)
    cache_key = f"{base_url}|{jql}|{max_results}"
    if cache_key in _AUTOFLEET_ABTEST_BUILD_CACHE:
        return _AUTOFLEET_ABTEST_BUILD_CACHE[cache_key]
    eff_issues = (
        issues
        if issues is not None
        else fetch_autofleet_abtest_candidates(base_url=base_url, auth_headers=auth_headers)
    )
    latest, build_name = pick_latest_issue_with_point_a_build(eff_issues)
    if not build_name and isinstance(latest, dict):
        fields = latest.get("fields") or {}
        if isinstance(fields, dict):
            build_name = extract_build_from_description_point_a(fields.get("description"))
    if not build_name:
        latest = pick_latest_issue(eff_issues)
        if isinstance(latest, dict):
            fields = latest.get("fields") or {}
            if isinstance(fields, dict):
                build_name = extract_build_from_description_point_a(fields.get("description"))
    # Do not cache an empty string from an empty/failed fetch — allows a later retry in-process.
    if build_name:
        _AUTOFLEET_ABTEST_BUILD_CACHE[cache_key] = build_name
    return build_name


def fetch_autofleet_abtest_best_branch_context(
    *,
    base_url: str,
    auth_headers: dict[str, str] | None,
) -> dict[str, Any]:
    """Return {'name': str, 'effective_date': date|None, 'week_start': date|None}."""
    enabled = _parse_bool_env(os.getenv("ZEPHYR_AUTOFLEET_ABTEST_ENABLED", "true"))
    if not enabled:
        return {}
    jql = str(os.getenv("ZEPHYR_AUTOFLEET_ABTEST_JQL", "labels = autofleet_abtest")).strip()
    max_results = _parse_int_env("ZEPHYR_AUTOFLEET_ABTEST_MAX_RESULTS", 100, 1, 500)
    cache_key = f"{base_url}|{jql}|{max_results}|context"
    cached = _AUTOFLEET_ABTEST_CONTEXT_CACHE.get(cache_key)
    if cached is not None:
        return cached
    issues = fetch_autofleet_abtest_candidates(base_url=base_url, auth_headers=auth_headers)
    latest, branch_name = pick_latest_issue_with_point_a_build(issues)
    if not isinstance(latest, dict):
        latest = pick_latest_issue(issues)
    if not isinstance(latest, dict):
        _AUTOFLEET_ABTEST_CONTEXT_CACHE[cache_key] = {}
        return {}
    fields = latest.get("fields") or {}
    if not isinstance(fields, dict):
        fields = {}
    created_dt = _coerce_utc_naive(_to_datetime(fields.get("created")))
    summary_dt = _coerce_utc_naive(parse_date_from_summary(fields.get("summary")))
    min_dt = datetime.min
    effective_dt = max(created_dt or min_dt, summary_dt or min_dt)
    if not branch_name:
        branch_name = extract_build_from_description_point_a(fields.get("description"))
    effective_date = effective_dt.date() if effective_dt != min_dt else None
    week_start = _release_week_start(effective_date) if effective_date is not None else None
    context = {
        "name": branch_name,
        "effective_date": effective_date,
        "week_start": week_start,
        "issue_key": str(latest.get("key") or "").strip(),
    }
    _AUTOFLEET_ABTEST_CONTEXT_CACHE[cache_key] = context
    return context


def get_by_path(data: dict[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def parse_extra_params(raw_items: list[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for item in raw_items:
        if "=" not in item:
            raise ValueError(
                f"Invalid --extra-param '{item}'. Expected format: key=value"
            )
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid --extra-param '{item}'. Empty key is not allowed")
        params[key] = value.strip()
    return params


def fill_template(raw: str, key: str, value: str, arg_name: str) -> str:
    placeholder = "{" + key + "}"
    if placeholder not in raw:
        raise ValueError(f"{arg_name} must contain '{placeholder}' placeholder")
    return raw.replace(placeholder, value)


def sanitize_tql_query(query: str) -> str:
    cleaned = query.strip().strip('"').strip("'").strip()
    # Guard against accidental duplicated ORDER BY fragments from copied templates.
    order_by_matches = list(re.finditer(r"\bORDER\s+BY\b", cleaned, flags=re.IGNORECASE))
    if len(order_by_matches) > 1:
        cleaned = cleaned[: order_by_matches[1].start()].rstrip()
    # Guard against stray trailing braces/parentheses caused by shell/env editing.
    while cleaned.endswith("}") and cleaned.count("{") == 0:
        cleaned = cleaned[:-1].rstrip()
    while cleaned.endswith(")") and cleaned.count(")") > cleaned.count("("):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


def build_headers(token_header: str, token_prefix: str, token: str) -> dict[str, str]:
    auth_value = f"{token_prefix.strip()} {token}".strip() if token_prefix else token
    return {
        token_header: auth_value,
        "Accept": "application/json",
    }


def extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidate_keys = ("values", "results", "executions", "items", "content")
    for key in candidate_keys:
        items = payload.get(key)
        if isinstance(items, list):
            return [i for i in items if isinstance(i, dict)]
    if isinstance(payload, list):
        return [i for i in payload if isinstance(i, dict)]
    return []


def request_json(
    base_url: str,
    endpoint: str,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
    method: str = "GET",
    body: dict[str, Any] | None = None,
) -> Any:
    query = urllib.parse.urlencode(params or {}, doseq=True)
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    if query and method.upper() == "GET":
        url = f"{url}?{query}"
    request_headers = dict(headers)
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url, headers=request_headers, method=method.upper(), data=payload
    )
    method_upper = method.upper()
    retries = _env_int("ZEPHYR_GET_RETRIES", 2) if method_upper == "GET" else 0
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            retryable = exc.code == 429 or 500 <= exc.code <= 599
            if method_upper == "GET" and retryable and attempt < retries:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                try:
                    delay = float(retry_after) if retry_after else 0.5 * (2**attempt)
                except ValueError:
                    delay = 0.5 * (2**attempt)
                print(
                    f"Retrying GET after HTTP {exc.code} in {delay:.1f}s: {url}",
                    file=sys.stderr,
                )
                time.sleep(delay)
                continue
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"HTTP {exc.code} while requesting '{url}' [{method_upper}]. Response: {body}"
            ) from exc
        except urllib.error.URLError as exc:
            if method_upper == "GET" and attempt < retries:
                delay = 0.5 * (2**attempt)
                print(
                    f"Retrying GET after network error in {delay:.1f}s: {url}",
                    file=sys.stderr,
                )
                time.sleep(delay)
                continue
            raise RuntimeError(f"Network error while requesting '{url}': {exc}") from exc
    raise RuntimeError(f"Unexpected retry exhaustion while requesting '{url}'")


def _parse_bool_env(value: str | None) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


# Parsed ``.env`` next to this script (lazy). Empty dict = file missing or no assignments.
_repo_dotenv_cache: dict[str, str] | None = None


def _get_repo_dotenv_parsed() -> dict[str, str]:
    """Return key/value pairs from repo ``.env`` (last assignment per key wins)."""
    global _repo_dotenv_cache
    if _repo_dotenv_cache is not None:
        return _repo_dotenv_cache
    from_file: dict[str, str] = {}
    env_path = Path(__file__).resolve().parent / ".env"
    if env_path.is_file():
        try:
            text = env_path.read_text(encoding="utf-8-sig")
        except OSError:
            _repo_dotenv_cache = from_file
            return from_file
        for raw_line in text.splitlines():
            line = raw_line.strip().replace("\r", "")
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("export "):
                line = line[7:].strip()
                if not line:
                    continue
            if "=" not in line:
                continue
            name, _, value = line.partition("=")
            name = name.strip()
            if not name:
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            from_file[name] = value
    _repo_dotenv_cache = from_file
    return from_file


def _load_repo_dotenv_if_absent() -> None:
    """Fill os.environ from ``.env`` next to this script for keys not already set.

    Launchers (PowerShell/bash) often load ``.env`` before Python; IDE / ``python``
    runs do not. Keys already present in the process environment are left unchanged;
    use :func:`_weekly_defect_extended_analytics_enabled` for flags where the repo
    ``.env`` must override a stale user/system variable.
    """
    for name, value in _get_repo_dotenv_parsed().items():
        if name not in os.environ:
            os.environ[name] = value


def _load_confluence_publish_config() -> ConfluencePublishConfig | None:
    base_url = (os.getenv("ZEPHYR_CONFLUENCE_BASE_URL") or "").strip().rstrip("/")
    user = (os.getenv("ZEPHYR_CONFLUENCE_USER") or "").strip()
    api_token = (os.getenv("ZEPHYR_CONFLUENCE_API_TOKEN") or "").strip()
    space_key = (os.getenv("ZEPHYR_CONFLUENCE_SPACE_KEY") or "").strip()
    parent_page_id = (os.getenv("ZEPHYR_CONFLUENCE_PARENT_PAGE_ID") or "").strip() or None
    title_prefix = (os.getenv("ZEPHYR_CONFLUENCE_TITLE_PREFIX") or "").strip()
    api_prefix = (os.getenv("ZEPHYR_CONFLUENCE_API_PREFIX") or "rest/api").strip().strip("/")
    auth_scheme = (os.getenv("ZEPHYR_CONFLUENCE_AUTH_SCHEME") or "basic").strip().lower()
    publish_daily = _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_PUBLISH_DAILY"))
    publish_weekly = _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY"))
    publish_bugs_raw = os.getenv("ZEPHYR_CONFLUENCE_PUBLISH_BUGS")
    if publish_bugs_raw is None or not str(publish_bugs_raw).strip():
        publish_bugs = publish_weekly
    else:
        publish_bugs = _parse_bool_env(publish_bugs_raw)
    update_existing = _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_UPDATE_EXISTING"))
    bugs_parent_page_id = (
        (os.getenv("ZEPHYR_CONFLUENCE_BUGS_PARENT_PAGE_ID") or "").strip() or None
    )
    bugs_parent_title = (
        (os.getenv("ZEPHYR_CONFLUENCE_BUGS_PARENT_TITLE") or "Баги").strip() or "Баги"
    )
    week_folder_title_template = (
        os.getenv("ZEPHYR_CONFLUENCE_WEEK_FOLDER_TITLE_TEMPLATE") or "Week {year}-w{week:02d}"
    ).strip() or "Week {year}-w{week:02d}"
    if not (publish_daily or publish_weekly or publish_bugs):
        return None
    if auth_scheme not in {"basic", "bearer"}:
        raise ValueError(
            "Unsupported ZEPHYR_CONFLUENCE_AUTH_SCHEME. Use 'basic' or 'bearer'."
        )
    required: list[tuple[str, str]] = [
        ("ZEPHYR_CONFLUENCE_BASE_URL", base_url),
        ("ZEPHYR_CONFLUENCE_API_TOKEN", api_token),
        ("ZEPHYR_CONFLUENCE_SPACE_KEY", space_key),
    ]
    if auth_scheme == "basic":
        required.append(("ZEPHYR_CONFLUENCE_USER", user))
    missing = [name for name, val in required if not val]
    if missing:
        raise ValueError(
            "Confluence publishing enabled but missing env vars: " + ", ".join(missing)
        )
    return ConfluencePublishConfig(
        base_url=base_url,
        user=user,
        api_token=api_token,
        space_key=space_key,
        parent_page_id=parent_page_id,
        bugs_parent_page_id=bugs_parent_page_id,
        bugs_parent_title=bugs_parent_title,
        week_folder_title_template=week_folder_title_template,
        title_prefix=title_prefix,
        api_prefix=api_prefix,
        auth_scheme=auth_scheme,
        update_existing=update_existing,
        publish_daily=publish_daily,
        publish_weekly=publish_weekly,
        publish_bugs=publish_bugs,
    )


def _confluence_auth_headers(cfg: ConfluencePublishConfig) -> dict[str, str]:
    scheme = (cfg.auth_scheme or "basic").lower()
    if scheme == "bearer":
        auth_value = f"Bearer {cfg.api_token}"
    else:
        token = base64.b64encode(f"{cfg.user}:{cfg.api_token}".encode("utf-8")).decode("ascii")
        auth_value = f"Basic {token}"
    return {
        "Authorization": auth_value,
        "Accept": "application/json",
    }


def _confluence_request_json(
    cfg: ConfluencePublishConfig,
    endpoint: str,
    *,
    method: str = "GET",
    params: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> Any:
    query = urllib.parse.urlencode(params or {}, doseq=True)
    url = f"{cfg.base_url}/{endpoint.lstrip('/')}"
    if query and method.upper() == "GET":
        url = f"{url}?{query}"
    headers = _confluence_auth_headers(cfg)
    if extra_headers:
        headers.update(extra_headers)
    payload: bytes | None = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, headers=headers, method=method.upper(), data=payload)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Confluence HTTP {exc.code} for '{url}' [{method.upper()}]. Response: {err_body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Confluence network error for '{url}': {exc}") from exc


def _confluence_page_title_for_file(path: str, cfg: ConfluencePublishConfig) -> str:
    base = os.path.basename(path)
    name, _ext = os.path.splitext(base)
    title = name.replace("_", " ")
    if cfg.title_prefix:
        return f"{cfg.title_prefix} {title}".strip()
    return title


def _extract_html_title(raw_html: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", raw_html, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    value = re.sub(r"\s+", " ", match.group(1)).strip()
    return html.unescape(value)


def _normalize_html_for_confluence_storage(raw_html: str) -> str:
    text = str(raw_html or "").strip()
    if not text:
        return ""
    # Confluence storage does not accept XML/DOCTYPE directives in the payload.
    text = re.sub(r"^\ufeff", "", text)
    text = re.sub(r"<\?xml[^>]*\?>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<!doctype[^>]*>", "", text, flags=re.IGNORECASE)
    body_match = re.search(r"<body\b[^>]*>(.*?)</body>", text, flags=re.IGNORECASE | re.DOTALL)
    if body_match:
        text = body_match.group(1)
    text = re.sub(r"<html\b[^>]*>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"</html>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<head\b[^>]*>.*?</head>", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<body\b[^>]*>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"</body>", "", text, flags=re.IGNORECASE)
    # Confluence storage parser is XHTML-like: void tags must be self-closing.
    for tag in ("area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"):
        text = re.sub(
            rf"<{tag}(\s[^>/]*?)?>",
            lambda m: f"<{tag}{m.group(1) or ''} />",
            text,
            flags=re.IGNORECASE,
        )
    return text.strip()


_WEEKLY_OVERALL_GRID_OPEN_RE = re.compile(
    r"<div\b[^>]*\bclass=(?:'|\")[^'\"]*\bweekly-overall-grid\b[^'\"]*(?:'|\")[^>]*>",
    flags=re.IGNORECASE,
)
_WEEKLY_OVERALL_CELL_OPEN_RE = re.compile(
    r"<div\b(?P<attrs>[^>]*\bclass=(?:'|\")[^'\"]*\bweekly-overall-cell\b[^'\"]*(?:'|\")[^>]*)>",
    flags=re.IGNORECASE,
)
_WEEKLY_DIV_TOKEN_RE = re.compile(r"<(/?)div\b[^>]*>", flags=re.IGNORECASE)
_WEEKLY_CYCLE_KEYS_ATTR_RE = re.compile(
    r'data-zephyr-cycle-keys=(?:"([^"]*)"|\'([^\']*)\')',
    flags=re.IGNORECASE,
)
_WEEKLY_CELL_H4_RE = re.compile(
    r"<h4[^>]*>(?P<text>.*?)</h4>", flags=re.DOTALL | re.IGNORECASE
)


def _find_matching_close_div(text: str, body_start: int) -> tuple[int, int]:
    """Return (body_end_index, end_of_close_index) for the </div> that closes
    a <div ...> opened just before `body_start`. (-1, -1) on malformed input."""
    depth = 1
    for token in _WEEKLY_DIV_TOKEN_RE.finditer(text, body_start):
        if token.group(1) == "/":
            depth -= 1
            if depth == 0:
                return token.start(), token.end()
        else:
            depth += 1
    return -1, -1


def _strip_inner_tags(html_fragment: str) -> str:
    return re.sub(r"<[^>]+>", "", html_fragment).strip()


def _replace_weekly_overall_cells_with_zephyr_macro(body_html: str) -> str:
    """For Confluence publish: rewrite the per-build pie grid into a single
    one-row table where each cell contains the Zephyr Reporting storage macro
    (built from data-zephyr-cycle-keys). The build label sits in the table
    header so all builds line up horizontally side by side.

    If a cell has no cycle keys it falls back to a placeholder text. The
    surrounding <div class='weekly-overall-grid'> is replaced entirely.
    """
    if not body_html or "weekly-overall-grid" not in body_html:
        return body_html

    out_parts: list[str] = []
    cursor = 0
    while True:
        grid_open = _WEEKLY_OVERALL_GRID_OPEN_RE.search(body_html, cursor)
        if grid_open is None:
            out_parts.append(body_html[cursor:])
            break
        out_parts.append(body_html[cursor:grid_open.start()])
        grid_body_start = grid_open.end()
        grid_body_end, grid_end = _find_matching_close_div(body_html, grid_body_start)
        if grid_body_end == -1:
            out_parts.append(body_html[grid_open.start():])
            break

        grid_inner = body_html[grid_body_start:grid_body_end]
        # Walk every weekly-overall-cell inside the grid.
        headers: list[str] = []
        cells: list[str] = []
        scan = 0
        while True:
            cell_open = _WEEKLY_OVERALL_CELL_OPEN_RE.search(grid_inner, scan)
            if cell_open is None:
                break
            attrs_text = cell_open.group("attrs") or ""
            cell_body_start = cell_open.end()
            cell_body_end, cell_end = _find_matching_close_div(grid_inner, cell_body_start)
            if cell_body_end == -1:
                break
            cell_inner = grid_inner[cell_body_start:cell_body_end]

            h4_match = _WEEKLY_CELL_H4_RE.search(cell_inner)
            label_text = (
                _strip_inner_tags(h4_match.group("text"))
                if h4_match
                else ""
            )
            headers.append(html.escape(label_text))

            macro = ""
            keys_attr = _WEEKLY_CYCLE_KEYS_ATTR_RE.search(attrs_text)
            if keys_attr:
                raw_json = keys_attr.group(1) or keys_attr.group(2) or ""
                try:
                    decoded = json.loads(html.unescape(raw_json))
                except json.JSONDecodeError:
                    decoded = None
                if isinstance(decoded, list) and decoded:
                    macro = _daily_zephyr_test_results_summary_storage_macro(decoded)
            if not macro:
                macro = "<p><em>Нет данных</em></p>"
            cells.append(macro)
            scan = cell_end

        if not headers:
            out_parts.append(body_html[grid_open.start():grid_end])
        else:
            n_cols = len(headers)
            col_pct = 100.0 / n_cols if n_cols else 100.0
            colgroup_html = "<colgroup>" + "".join(
                f'<col style="width:{col_pct:.4f}%;" />' for _ in range(n_cols)
            ) + "</colgroup>"
            cell_style = (
                f'style="width:{col_pct:.4f}%;'
                "vertical-align:top;text-align:center;"
                'padding:6px 8px;"'
            )
            th_style = (
                f'style="width:{col_pct:.4f}%;text-align:center;'
                'font-weight:600;"'
            )
            header_html = "".join(f"<th {th_style}>{h}</th>" for h in headers)
            cell_html = "".join(f"<td {cell_style}>{c}</td>" for c in cells)
            table_html = (
                '<table class="weekly-overall-table" '
                'style="table-layout:fixed;width:100%;">'
                + colgroup_html
                + "<thead><tr>" + header_html + "</tr></thead>"
                + "<tbody><tr>" + cell_html + "</tr></tbody>"
                + "</table>"
            )
            out_parts.append(table_html)
        cursor = grid_end
    return "".join(out_parts)


def _append_confluence_attachment_image(storage_html: str, file_name: str) -> str:
    safe_name = html.escape(str(file_name or "").strip(), quote=True)
    if not safe_name:
        return storage_html
    if safe_name in storage_html:
        return storage_html
    image_macro = (
        "<p><ac:image ac:width='260'>"
        f"<ri:attachment ri:filename='{safe_name}'/>"
        "</ac:image></p>"
    )
    return f"{storage_html}\n{image_macro}" if storage_html else image_macro


def _confluence_find_page_by_title(
    cfg: ConfluencePublishConfig, title: str
) -> tuple[str, int] | None:
    payload = _confluence_request_json(
        cfg,
        f"{cfg.api_prefix}/content",
        params={
            "spaceKey": cfg.space_key,
            "title": title,
            "status": "current",
            "expand": "version",
        },
    )
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return None
    first = results[0]
    page_id = str(first.get("id") or "").strip()
    version = int(first.get("version", {}).get("number") or 1)
    if not page_id:
        return None
    return (page_id, version)


def _confluence_find_child_page_by_title(
    cfg: ConfluencePublishConfig, parent_page_id: str, title: str
) -> tuple[str, int] | None:
    start = 0
    limit = 50
    while True:
        payload = _confluence_request_json(
            cfg,
            f"{cfg.api_prefix}/content/{parent_page_id}/child/page",
            params={"limit": str(limit), "start": str(start), "expand": "version"},
        )
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            return None
        for item in results:
            if not isinstance(item, dict):
                continue
            if str(item.get("title") or "").strip() != title:
                continue
            page_id = str(item.get("id") or "").strip()
            version = int(item.get("version", {}).get("number") or 1)
            if page_id:
                return (page_id, version)
        size = int(payload.get("size") or 0) if isinstance(payload, dict) else 0
        if size < limit:
            break
        start += limit
    return None


def _confluence_create_child_page(
    cfg: ConfluencePublishConfig,
    parent_page_id: str,
    title: str,
    *,
    storage_html: str = "<p></p>",
) -> str:
    create_body: dict[str, Any] = {
        "type": "page",
        "title": title,
        "space": {"key": cfg.space_key},
        "ancestors": [{"id": parent_page_id}],
        "body": {"storage": {"value": storage_html, "representation": "storage"}},
    }
    created = _confluence_request_json(
        cfg, f"{cfg.api_prefix}/content", method="POST", body=create_body
    )
    page_id = str(created.get("id") or "").strip()
    if not page_id:
        raise RuntimeError(
            f"Confluence section page create returned no id for title '{title}'"
        )
    return page_id


def _confluence_ensure_section_page(
    cfg: ConfluencePublishConfig,
    *,
    root_parent_page_id: str | None,
    explicit_page_id: str | None,
    section_title: str,
) -> str | None:
    if explicit_page_id:
        return explicit_page_id
    if not root_parent_page_id:
        return None
    existing = _confluence_find_child_page_by_title(
        cfg, root_parent_page_id, section_title
    )
    if existing:
        return existing[0]
    return _confluence_create_child_page(cfg, root_parent_page_id, section_title)


def _confluence_week_folder_title(cfg: ConfluencePublishConfig, week_start: date) -> str:
    iso = week_start.isocalendar()
    try:
        return cfg.week_folder_title_template.format(
            week=int(iso[1]),
            year=int(iso[0]),
            week_start=week_start.isoformat(),
        )
    except (KeyError, ValueError) as exc:
        raise ValueError(
            "Invalid ZEPHYR_CONFLUENCE_WEEK_FOLDER_TITLE_TEMPLATE. "
            "Use placeholders: {week}, {year}, {week_start}."
        ) from exc


def _confluence_week_start_from_publish_path(path: str) -> date | None:
    base = os.path.basename(path)
    if base.startswith("weekly_cycle_matrix_"):
        match = re.match(r"weekly_cycle_matrix_(\d{4}-\d{2}-\d{2})", base)
        if match:
            return datetime.strptime(match.group(1), "%Y-%m-%d").date()
        return None
    if _is_build_log_html_path(path):
        return None
    match = re.search(r"_(\d{4}-\d{2}-\d{2})_", base)
    if not match:
        return None
    try:
        report_day = datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None
    return _release_week_start(report_day)


def resolve_confluence_publish_roots(cfg: ConfluencePublishConfig) -> ConfluencePublishRoots:
    """Ensure the bugs folder under root; week folders (Week wNN) are created per publish batch."""
    root = cfg.parent_page_id
    bugs = _confluence_ensure_section_page(
        cfg,
        root_parent_page_id=root,
        explicit_page_id=cfg.bugs_parent_page_id,
        section_title=cfg.bugs_parent_title,
    )
    return ConfluencePublishRoots(
        root_parent=root,
        bugs_parent=bugs or root,
    )


def publish_reports_to_confluence_by_week(
    html_paths: list[str],
    cfg: ConfluencePublishConfig,
    *,
    week_parents: ConfluenceWeekParentCache,
    fallback_parent: str | None = None,
) -> list[str]:
    by_week: dict[date, list[str]] = {}
    ungrouped: list[str] = []
    for path in html_paths:
        if _is_build_log_html_path(path):
            continue
        week_start = _confluence_week_start_from_publish_path(path)
        if week_start is None:
            ungrouped.append(path)
            continue
        by_week.setdefault(week_start, []).append(path)
    outcomes: list[str] = []
    for week_start in sorted(by_week):
        parent_id = week_parents.ensure(week_start)
        folder_title = _confluence_week_folder_title(cfg, week_start)
        batch_outcomes = publish_reports_to_confluence(
            by_week[week_start], cfg, parent_page_id=parent_id
        )
        for line in batch_outcomes:
            outcomes.append(f"[{folder_title}] {line}")
    if ungrouped:
        batch_outcomes = publish_reports_to_confluence(
            ungrouped, cfg, parent_page_id=fallback_parent
        )
        outcomes.extend(batch_outcomes)
    return outcomes


def _is_build_log_html_path(path: str) -> bool:
    return os.path.basename(path).endswith("_build_log.html")


def _is_bugs_rollup_html_path(path: str) -> bool:
    return os.path.basename(path) == "bugs_index.html"


def _confluence_get_page_parent_id(cfg: ConfluencePublishConfig, page_id: str) -> str | None:
    payload = _confluence_request_json(
        cfg,
        f"{cfg.api_prefix}/content/{page_id}",
        params={"expand": "ancestors"},
    )
    if not isinstance(payload, dict):
        return None
    ancestors = payload.get("ancestors")
    if not isinstance(ancestors, list) or not ancestors:
        return None
    last = ancestors[-1]
    if not isinstance(last, dict):
        return None
    parent_id = str(last.get("id") or "").strip()
    return parent_id or None


def _confluence_lookup_page_for_upsert(
    cfg: ConfluencePublishConfig,
    title: str,
    *,
    parent_page_id: str | None,
    legacy_title: str | None = None,
) -> tuple[str, int, str | None] | None:
    """Return (page_id, version, current_parent_id) for update/move decisions."""
    if parent_page_id:
        hit = _confluence_find_child_page_by_title(cfg, parent_page_id, title)
        if hit:
            return (hit[0], hit[1], parent_page_id)
        if cfg.update_existing and legacy_title and legacy_title != title:
            hit = _confluence_find_child_page_by_title(cfg, parent_page_id, legacy_title)
            if hit:
                return (hit[0], hit[1], parent_page_id)
    if not cfg.update_existing:
        return None
    hit = _confluence_find_page_by_title(cfg, title)
    if hit:
        page_id, version = hit
        if parent_page_id and page_id == parent_page_id:
            return None
        return (page_id, version, _confluence_get_page_parent_id(cfg, page_id))
    if legacy_title and legacy_title != title:
        hit = _confluence_find_page_by_title(cfg, legacy_title)
        if hit:
            page_id, version = hit
            if parent_page_id and page_id == parent_page_id:
                return None
            return (page_id, version, _confluence_get_page_parent_id(cfg, page_id))
    return None


def _confluence_upsert_storage_page(
    cfg: ConfluencePublishConfig,
    title: str,
    storage_html: str,
    *,
    legacy_title: str | None = None,
    parent_page_id: str | None = None,
) -> tuple[str, str]:
    effective_parent = parent_page_id if parent_page_id is not None else cfg.parent_page_id
    existing = _confluence_lookup_page_for_upsert(
        cfg,
        title,
        parent_page_id=effective_parent,
        legacy_title=legacy_title,
    )
    if existing:
        page_id, current_version, current_parent = existing
        need_reparent = bool(
            effective_parent
            and (not current_parent or current_parent != effective_parent)
        )
        body: dict[str, Any] = {
            "id": page_id,
            "type": "page",
            "title": title,
            "space": {"key": cfg.space_key},
            "version": {"number": current_version + 1},
            "body": {"storage": {"value": storage_html, "representation": "storage"}},
        }
        if need_reparent:
            body["ancestors"] = [{"id": effective_parent}]
        try:
            _confluence_request_json(
                cfg, f"{cfg.api_prefix}/content/{page_id}", method="PUT", body=body
            )
        except RuntimeError as exc:
            if "HTTP 404" not in str(exc):
                raise
            # Stale id (deleted/trashed page): create a fresh page under the target parent.
            existing = None
        else:
            action = "moved+updated" if need_reparent else "updated"
            return page_id, action
    create_body: dict[str, Any] = {
        "type": "page",
        "title": title,
        "space": {"key": cfg.space_key},
        "body": {"storage": {"value": storage_html, "representation": "storage"}},
    }
    if effective_parent:
        create_body["ancestors"] = [{"id": effective_parent}]
    created = _confluence_request_json(
        cfg, f"{cfg.api_prefix}/content", method="POST", body=create_body
    )
    page_id = str(created.get("id") or "").strip()
    if not page_id:
        raise RuntimeError(f"Confluence page create returned no id for title '{title}'")
    return page_id, "created"


def _confluence_upload_attachment(
    cfg: ConfluencePublishConfig, page_id: str, file_path: str
) -> str:
    file_name = os.path.basename(file_path)
    boundary = f"----CursorForm{uuid.uuid4().hex}"
    ctype = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
    with open(file_path, "rb") as source:
        raw = source.read()
    body = (
        f"--{boundary}\r\n"
        f"Content-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\n"
        f"Content-Type: {ctype}\r\n\r\n"
    ).encode("utf-8") + raw + f"\r\n--{boundary}--\r\n".encode("utf-8")
    endpoint = f"{cfg.api_prefix}/content/{page_id}/child/attachment"
    headers = _confluence_auth_headers(cfg)
    headers.update(
        {
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "X-Atlassian-Token": "no-check",
        }
    )
    url = f"{cfg.base_url}/{endpoint}"
    req = urllib.request.Request(url, headers=headers, method="POST", data=body)
    try:
        with urllib.request.urlopen(req, timeout=30):
            return file_name
    except urllib.error.HTTPError as exc:
        # 409/400 can mean attachment already exists depending on instance settings; ignore as non-fatal.
        if exc.code in (400, 409):
            return file_name
        err_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Confluence attachment upload failed for '{file_name}' (HTTP {exc.code}): {err_body}"
        ) from exc


def _extract_zephyr_status_counts_from_html(raw_html: str) -> dict[str, int] | None:
    match = re.search(
        r'<div\s+id=["\']zephyr-status-counts-json["\'][^>]*>(.*?)</div>',
        raw_html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not match:
        return None
    try:
        data = json.loads(html.unescape(match.group(1).strip()))
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    out: dict[str, int] = {}
    for key, value in data.items():
        try:
            out[str(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return out or None


def _strip_zephyr_status_counts_json_div(body_html: str) -> str:
    return re.sub(
        r'<div\s+id=["\']zephyr-status-counts-json["\'][^>]*>.*?</div>',
        "",
        body_html,
        count=1,
        flags=re.DOTALL | re.IGNORECASE,
    )


def _extract_zephyr_cycle_key_objects_from_html(
    raw_html: str,
) -> list[dict[str, Any]] | None:
    match = re.search(
        r'<div\s+id=["\']zephyr-cycle-keys-json["\'][^>]*>(.*?)</div>',
        raw_html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not match:
        return None
    try:
        data = json.loads(html.unescape(match.group(1).strip()))
    except json.JSONDecodeError:
        return None
    if not isinstance(data, list):
        return None
    out: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict) and str(item.get("id") or "").strip():
            out.append(item)
    return out


def _strip_zephyr_cycle_keys_json_div(body_html: str) -> str:
    return re.sub(
        r'<div\s+id=["\']zephyr-cycle-keys-json["\'][^>]*>.*?</div>',
        "",
        body_html,
        count=1,
        flags=re.DOTALL | re.IGNORECASE,
    )


def _strip_daily_pie_visual_block(body_html: str) -> str:
    """Remove global summary pie (section 3) before inserting Zephyr/Chart macro on publish."""
    result = body_html
    for pattern in (
        "<div class='daily-pie-wrap daily-pie-strip-publish'>",
        '<div class="daily-pie-wrap daily-pie-strip-publish">',
    ):
        while True:
            start = result.find(pattern)
            if start == -1:
                break
            pos = start + len(pattern)
            depth = 1
            while depth > 0 and pos < len(result):
                sub = result[pos:]
                open_m = re.search(r"<div\b", sub, flags=re.IGNORECASE)
                close_m = re.search(r"</div>", sub, flags=re.IGNORECASE)
                if close_m is None:
                    return result
                open_pos = open_m.start() if open_m else len(sub) + 1
                close_pos = close_m.start()
                if open_m is not None and open_pos < close_pos:
                    depth += 1
                    pos += open_m.end()
                else:
                    depth -= 1
                    pos += close_m.end()
            result = result[:start] + result[pos:]
    result = re.sub(
        r"<p\s+class=['\"]pie-empty['\"][^>]*>.*?</p>",
        "",
        result,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return result


def _inject_confluence_anchor_macros(storage_html: str) -> str:
    """Ensure #fragment links work in Confluence storage by using anchor macros."""

    pattern = re.compile(
        r"<(?P<tag>h[1-6])(?P<before>[^>]*?)\s+id\s*=\s*['\"](?P<id>[^'\"]+)['\"](?P<after>[^>]*)>"
        r"(?P<text>.*?)</(?P=tag)>",
        flags=re.IGNORECASE | re.DOTALL,
    )

    def _repl(match: re.Match[str]) -> str:
        tag = match.group("tag")
        anchor_id = match.group("id")
        before = match.group("before") or ""
        after = match.group("after") or ""
        # Keep id on the heading so publish-time parsers (excerpt span, CSS) still work.
        attrs = f"{before} id=\"{anchor_id}\"{after}"
        text = match.group("text")
        anchor_macro = (
            '<ac:structured-macro ac:name="anchor">'
            f'<ac:parameter ac:name="">{html.escape(anchor_id)}</ac:parameter>'
            "</ac:structured-macro>"
        )
        return f"{anchor_macro}<{tag}{attrs}>{text}</{tag}>"

    return pattern.sub(_repl, storage_html)


def _convert_fragment_links_to_confluence(storage_html: str) -> str:
    """Convert local #fragment links to native Confluence anchor links."""
    pattern = re.compile(
        r"<a(?P<attrs>[^>]*?)\s+href\s*=\s*['\"]#(?P<anchor>[^'\"]+)['\"](?P<tail>[^>]*)>"
        r"(?P<body>.*?)</a>",
        flags=re.IGNORECASE | re.DOTALL,
    )

    def _repl(match: re.Match[str]) -> str:
        anchor = match.group("anchor").strip()
        body = (match.group("body") or "").strip()
        if not anchor or not body:
            return match.group(0)
        return (
            f'<ac:link ac:anchor="{html.escape(anchor, quote=True)}">'
            f"<ac:link-body>{body}</ac:link-body>"
            "</ac:link>"
        )

    return pattern.sub(_repl, storage_html)


def _insert_block_after_scenarios_heading(storage_html: str, block: str) -> str:
    # Preferred anchor: raw HTML heading before Confluence anchor conversion.
    pattern_raw = re.compile(
        r"(<h2\b[^>]*\bid\s*=\s*['\"]scenarios['\"][^>]*>.*?</h2>)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    match_raw = pattern_raw.search(storage_html)
    if match_raw:
        end = match_raw.end()
        return f"{storage_html[:end]}\n{block}{storage_html[end:]}"

    # Fallback: after _inject_confluence_anchor_macros, anchor macro precedes heading.
    pattern_anchor_macro = re.compile(
        r"(<ac:structured-macro\s+ac:name=['\"]anchor['\"][^>]*>"
        r".*?<ac:parameter\s+ac:name=['\"][^'\"]*['\"]>\s*scenarios\s*</ac:parameter>"
        r".*?</ac:structured-macro>\s*<h2\b[^>]*>.*?</h2>)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    match_macro = pattern_anchor_macro.search(storage_html)
    if match_macro:
        end = match_macro.end()
        return f"{storage_html[:end]}\n{block}{storage_html[end:]}"

    return f"{storage_html}\n{block}" if storage_html else block


def _replace_scenario_result_cells_with_zephyr_macro(storage_html: str) -> str:
    pattern = re.compile(
        r"(?P<open><td\b[^>]*\bclass\s*=\s*['\"][^'\"]*scenario-result-cell[^'\"]*['\"][^>]*>)"
        r"(?P<body>.*?)"
        r"<span\b[^>]*\bclass\s*=\s*['\"]scenario-result-macro-marker['\"][^>]*"
        r"\bdata-cycle-key\s*=\s*['\"](?P<key>[^'\"]+)['\"]"
        r"(?:[^>]*\bdata-cycle-name\s*=\s*['\"](?P<name>[^'\"]*)['\"])?[^>]*></span>"
        r".*?(?P<close></td>)",
        flags=re.IGNORECASE | re.DOTALL,
    )

    def _repl(match: re.Match[str]) -> str:
        key = html.unescape((match.group("key") or "").strip())
        name = html.unescape((match.group("name") or "").strip())
        if not key:
            return match.group(0)
        cycle_objects = [{"id": key, "name": name}] if name else [{"id": key}]
        macro = _daily_zephyr_test_results_summary_storage_macro(cycle_objects)
        if not macro:
            return match.group(0)
        return f"{match.group('open')}{macro}{match.group('close')}"

    return pattern.sub(_repl, storage_html)


def publish_reports_to_confluence(
    html_paths: list[str],
    cfg: ConfluencePublishConfig,
    *,
    parent_page_id: str | None = None,
) -> list[str]:
    outcomes: list[str] = []
    for html_path in html_paths:
        with open(html_path, encoding="utf-8") as source:
            raw_html = source.read()
        is_build_log = _is_build_log_html_path(html_path)
        is_bugs_rollup = _is_bugs_rollup_html_path(html_path)
        is_weekly = os.path.basename(html_path).startswith("weekly_cycle_matrix")
        if is_bugs_rollup:
            body_html = _normalize_html_for_confluence_storage(raw_html)
            body_html = _inject_confluence_anchor_macros(body_html)
            body_html = _convert_fragment_links_to_confluence(body_html)
            title_from_html = _extract_html_title(raw_html).strip()
            title_from_file = _confluence_page_title_for_file(html_path, cfg)
            primary_title = title_from_html or title_from_file or BUGS_ROLLUP_CONFLUENCE_TITLE
            legacy_title = None
            if primary_title == BUGS_ROLLUP_CONFLUENCE_TITLE:
                legacy_title = BUGS_ROLLUP_DISPLAY_TITLE
            elif title_from_html and title_from_file and title_from_html != title_from_file:
                legacy_title = title_from_file
            page_id, action = _confluence_upsert_storage_page(
                cfg,
                primary_title,
                body_html,
                legacy_title=legacy_title,
                parent_page_id=parent_page_id,
            )
            outcomes.append(f"{action}: {primary_title} (page id {page_id})")
            continue
        if is_build_log:
            body_html = _normalize_html_for_confluence_storage(raw_html)
            body_html = _inject_confluence_anchor_macros(body_html)
            body_html = _convert_fragment_links_to_confluence(body_html)
            title_from_html = _extract_html_title(raw_html).strip()
            title_from_file = _confluence_page_title_for_file(html_path, cfg)
            primary_title = title_from_html or title_from_file
            legacy_title = (
                title_from_file
                if title_from_html and title_from_html != title_from_file
                else None
            )
            page_id, action = _confluence_upsert_storage_page(
                cfg,
                primary_title,
                body_html,
                legacy_title=legacy_title,
                parent_page_id=parent_page_id,
            )
            outcomes.append(f"{action}: {primary_title} (page id {page_id})")
            continue
        if is_weekly:
            body_html = _normalize_html_for_confluence_storage(raw_html)
            body_html = _inject_confluence_anchor_macros(body_html)
            body_html = _convert_fragment_links_to_confluence(body_html)
            body_html = _replace_weekly_overall_cells_with_zephyr_macro(body_html)
            body_html = _replace_weekly_jira_key_spans_with_confluence_macro(body_html)
            body_html = _replace_legacy_weekly_table_macros_with_excerpt(body_html)
            body_html = _wrap_weekly_scenario_block_with_excerpt_macro(body_html)
            title_from_html = _extract_html_title(raw_html).strip()
            title_from_file = _confluence_page_title_for_file(html_path, cfg)
            primary_title = title_from_html or title_from_file
            legacy_title = (
                title_from_file
                if title_from_html and title_from_html != title_from_file
                else None
            )
            page_id, action = _confluence_upsert_storage_page(
                cfg,
                primary_title,
                body_html,
                legacy_title=legacy_title,
                parent_page_id=parent_page_id,
            )
            outcomes.append(f"{action}: {primary_title} (page id {page_id})")
            continue
        cycle_objects_raw = _extract_zephyr_cycle_key_objects_from_html(raw_html)
        cycle_objects: list[dict[str, Any]] = (
            cycle_objects_raw if cycle_objects_raw is not None else []
        )
        zephyr_storage = _daily_zephyr_test_results_summary_storage_macro(cycle_objects)
        counts = _extract_zephyr_status_counts_from_html(raw_html)
        chart_storage = _daily_status_chart_storage_macro(counts) if counts else ""
        use_zephyr_macro = bool(zephyr_storage)
        use_native_chart = bool(chart_storage) and not use_zephyr_macro

        body_html = _normalize_html_for_confluence_storage(raw_html)
        body_html = _inject_confluence_anchor_macros(body_html)
        body_html = _convert_fragment_links_to_confluence(body_html)
        body_html = _replace_scenario_result_cells_with_zephyr_macro(body_html)
        if use_zephyr_macro or use_native_chart:
            body_html = _strip_daily_pie_visual_block(body_html)
            body_html = _strip_zephyr_status_counts_json_div(body_html)
            body_html = _strip_zephyr_cycle_keys_json_div(body_html)
            if use_zephyr_macro:
                chart_block = "<p class='daily-tab'>\t</p>" + zephyr_storage
            else:
                chart_block = "<p class='daily-tab'>\t</p>" + chart_storage
            body_html = _insert_block_after_scenarios_heading(body_html, chart_block)

        chart_path = html_path.replace(".html", "_conclusion_pie.png")
        chart_name = os.path.basename(chart_path)
        if os.path.exists(chart_path) and not use_native_chart and not use_zephyr_macro:
            body_html = _append_confluence_attachment_image(body_html, chart_name)
        body_html = _wrap_daily_report_with_excerpt_macro(body_html)
        title_from_html = _extract_html_title(raw_html).strip()
        title_from_file = _confluence_page_title_for_file(html_path, cfg)
        primary_title = title_from_html or title_from_file
        legacy_title = (
            title_from_file
            if title_from_html and title_from_html != title_from_file
            else None
        )
        page_id, action = _confluence_upsert_storage_page(
            cfg,
            primary_title,
            body_html,
            legacy_title=legacy_title,
            parent_page_id=parent_page_id,
        )
        attachment_note = ""
        if os.path.exists(chart_path) and not use_native_chart and not use_zephyr_macro:
            attachment_name = _confluence_upload_attachment(cfg, page_id, chart_path)
            attachment_note = f", attachment: {attachment_name}"
        elif use_zephyr_macro:
            attachment_note = ", chart: Zephyr TEST_RESULTS_SUMMARY_BY_STATUS macro"
        elif use_native_chart:
            attachment_note = ", chart: native macro"
        outcomes.append(f"{action}: {primary_title} (page id {page_id}{attachment_note})")
    return outcomes


def fetch_executions(
    base_url: str,
    endpoint: str,
    headers: dict[str, str],
    extra_params: dict[str, str],
    page_size: int,
) -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []
    start_at = 0
    while True:
        params = dict(extra_params)
        params.setdefault("startAt", str(start_at))
        params.setdefault("maxResults", str(page_size))
        payload = request_json(base_url, endpoint, headers, params)

        if not isinstance(payload, dict):
            raise RuntimeError(
                "Unexpected API response: expected JSON object with executions list"
            )

        page_items = extract_items(payload)
        if not page_items:
            break

        all_items.extend(page_items)

        if payload.get("isLast") is True:
            break

        total = payload.get("total")
        if isinstance(total, int):
            start_at += len(page_items)
            if start_at >= total:
                break
            continue

        if len(page_items) < page_size:
            break
        start_at += len(page_items)
    return all_items


def parse_root_folder_ids(raw_items: list[str]) -> list[str]:
    parsed: list[str] = []
    for item in raw_items:
        for part in item.split(","):
            cleaned = part.strip()
            if cleaned:
                parsed.append(cleaned)
    deduped: list[str] = []
    seen: set[str] = set()
    for folder_id in parsed:
        if folder_id not in seen:
            deduped.append(folder_id)
            seen.add(folder_id)
    return deduped


def _to_folder_node(item: dict[str, Any]) -> FolderNode | None:
    folder_id_raw = item.get("id") or item.get("folderTreeId") or item.get("folderId")
    if folder_id_raw is None:
        return None
    name_raw = item.get("name") or item.get("folderName") or f"folder_{folder_id_raw}"
    parent_raw = item.get("parentId")
    if parent_raw is None and isinstance(item.get("parent"), dict):
        parent_raw = item["parent"].get("id")
    full_path_raw = item.get("fullName") or item.get("path") or ""
    return FolderNode(
        folder_id=str(folder_id_raw),
        folder_name=str(name_raw),
        parent_id=str(parent_raw) if parent_raw is not None else None,
        full_path=str(full_path_raw),
    )


def _collect_folder_nodes(payload: Any) -> list[FolderNode]:
    collected: list[FolderNode] = []
    if isinstance(payload, dict):
        node = _to_folder_node(payload)
        if node:
            collected.append(node)
        for key in (
            "values",
            "results",
            "items",
            "content",
            "folders",
            "children",
            "data",
            "result",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                for entry in value:
                    collected.extend(_collect_folder_nodes(entry))
            elif isinstance(value, dict):
                collected.extend(_collect_folder_nodes(value))
    elif isinstance(payload, list):
        for entry in payload:
            collected.extend(_collect_folder_nodes(entry))
    return collected


def discover_folders(
    base_url: str,
    folder_endpoint: str,
    headers: dict[str, str],
    project_id: str | None,
    root_folder_ids: list[str],
) -> list[FolderNode]:
    params: dict[str, str] = {}
    if project_id:
        params["projectId"] = project_id

    discovery_errors: list[str] = []
    payload = None

    # 1) Try plain GET first (works on some Zephyr deployments).
    try:
        payload = request_json(base_url, folder_endpoint, headers, params, method="GET")
    except RuntimeError as exc:
        discovery_errors.append(str(exc))

    # 2) If endpoint hints search API, try POST with common body shapes.
    if payload is None and "search" in folder_endpoint.lower():
        post_bodies: list[dict[str, Any]] = []
        if project_id:
            post_bodies.extend(
                [
                    {"projectId": project_id},
                    {"projectId": int(project_id)} if project_id.isdigit() else {"projectId": project_id},
                    {"query": f"projectId = {project_id}"},
                    {"projectIds": [int(project_id)]}
                    if project_id.isdigit()
                    else {"projectIds": [project_id]},
                ]
            )
        else:
            post_bodies.append({})

        for post_body in post_bodies:
            try:
                payload = request_json(
                    base_url,
                    folder_endpoint,
                    headers,
                    method="POST",
                    body=post_body,
                )
                break
            except RuntimeError as exc:
                discovery_errors.append(str(exc))

    if payload is None:
        joined = "\n".join(f"- {message}" for message in discovery_errors)
        raise RuntimeError(
            "Unable to discover folders from API. Checked GET/POST variants.\n"
            f"{joined}\n"
            "Set --folder-endpoint to your instance-specific folder API endpoint."
        )

    nodes = _collect_folder_nodes(payload)
    by_id: dict[str, FolderNode] = {node.folder_id: node for node in nodes}

    children_by_parent: dict[str, list[str]] = defaultdict(list)
    for node in nodes:
        if node.parent_id:
            children_by_parent[node.parent_id].append(node.folder_id)

    result_ids: set[str] = set()
    stack: list[str] = list(root_folder_ids)
    while stack:
        current = stack.pop()
        if current in result_ids:
            continue
        result_ids.add(current)
        stack.extend(children_by_parent.get(current, []))

    discovered: list[FolderNode] = []
    for folder_id in root_folder_ids:
        if folder_id in by_id:
            discovered.append(by_id[folder_id])
    for folder_id in sorted(result_ids):
        if folder_id in by_id and by_id[folder_id] not in discovered:
            discovered.append(by_id[folder_id])
    return discovered


def discover_folders_tree_fallback(
    base_url: str,
    headers: dict[str, str],
    project_id: str | None,
    folder_search_endpoint: str,
    foldertree_endpoint: str,
) -> tuple[list[FolderNode], str, list[str]]:
    errors: list[str] = []

    search_body_candidates: list[dict[str, Any]] = []
    if project_id:
        search_body_candidates.extend(
            [
                {"projectId": project_id},
                {"projectId": int(project_id)} if project_id.isdigit() else {"projectId": project_id},
                {"query": f"projectId = {project_id}"},
                {"projectIds": [int(project_id)]}
                if project_id.isdigit()
                else {"projectIds": [project_id]},
            ]
        )
    else:
        search_body_candidates.append({})

    for body in search_body_candidates:
        try:
            payload = request_json(
                base_url=base_url,
                endpoint=folder_search_endpoint,
                headers=headers,
                method="POST",
                body=body,
            )
            nodes = _collect_folder_nodes(payload)
            if nodes:
                return nodes, f"POST {folder_search_endpoint}", errors
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(str(exc))

    params: dict[str, str] = {}
    if project_id:
        params["projectId"] = project_id
    try:
        payload = request_json(
            base_url=base_url,
            endpoint=foldertree_endpoint,
            headers=headers,
            params=params,
            method="GET",
        )
        nodes = _collect_folder_nodes(payload)
        if nodes:
            return nodes, f"GET {foldertree_endpoint}", errors
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(str(exc))

    return [], "none", errors


def probe_tree_endpoints(
    base_url: str,
    headers: dict[str, str],
    project_id: str | None,
) -> tuple[list[FolderNode], str, list[str]]:
    attempts: list[str] = []
    post_candidates = [
        "rest/tests/1.0/folder/search",
        "rest/tests/1.0/foldertree/search",
        "rest/tests/1.0/folder/searches",
        "rest/tests/1.0/folders/search",
    ]
    get_candidates = [
        "rest/tests/1.0/foldertree",
        "rest/tests/1.0/folder",
        "rest/tests/1.0/folder/list",
        "rest/tests/1.0/folders",
    ]

    post_bodies: list[dict[str, Any]] = []
    if project_id:
        post_bodies.extend(
            [
                {"projectId": project_id},
                {"projectId": int(project_id)} if project_id.isdigit() else {"projectId": project_id},
                {"query": f"projectId = {project_id}"},
                {"projectIds": [int(project_id)]}
                if project_id.isdigit()
                else {"projectIds": [project_id]},
            ]
        )
    else:
        post_bodies.append({})

    for endpoint in post_candidates:
        for body in post_bodies:
            try:
                payload = request_json(
                    base_url=base_url,
                    endpoint=endpoint,
                    headers=headers,
                    method="POST",
                    body=body,
                )
                nodes = _collect_folder_nodes(payload)
                if nodes:
                    return nodes, f"POST {endpoint}", attempts
                attempts.append(f"POST {endpoint} -> empty nodes")
            except Exception as exc:  # pylint: disable=broad-except
                attempts.append(f"POST {endpoint} -> {exc}")

    for endpoint in get_candidates:
        params: dict[str, str] = {}
        if project_id:
            params["projectId"] = project_id
        try:
            payload = request_json(
                base_url=base_url,
                endpoint=endpoint,
                headers=headers,
                method="GET",
                params=params,
            )
            nodes = _collect_folder_nodes(payload)
            if nodes:
                return nodes, f"GET {endpoint}", attempts
            attempts.append(f"GET {endpoint} -> empty nodes")
        except Exception as exc:  # pylint: disable=broad-except
            attempts.append(f"GET {endpoint} -> {exc}")

    return [], "none", attempts


def _parse_json_object_arg(raw: str | None, arg_name: str) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{arg_name} must be valid JSON object") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{arg_name} must be JSON object")
    return parsed


def resolve_folder_creation_name(
    explicit_name: str | None, name_template: str | None
) -> str | None:
    if explicit_name and explicit_name.strip():
        return explicit_name.strip()
    if name_template and name_template.strip():
        candidate = date.today().strftime(name_template.strip()).strip()
        return candidate or None
    return None


def _normalize_project_or_parent(value: str | None) -> str | int | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    return int(raw) if raw.isdigit() else raw


def _find_existing_folder(
    nodes: list[FolderNode], name: str, parent_id: str | None
) -> FolderNode | None:
    normalized_name = name.strip()
    normalized_parent = parent_id.strip() if parent_id else None
    candidates: list[FolderNode] = []
    for node in nodes:
        if node.folder_name.strip() != normalized_name:
            continue
        if normalized_parent and (node.parent_id or "").strip() != normalized_parent:
            continue
        candidates.append(node)
    if not candidates:
        return None
    return sorted(candidates, key=lambda n: n.folder_id)[0]


def ensure_folder_created_or_existing(
    args: argparse.Namespace,
    headers: dict[str, str],
    tree_source_query: dict[str, Any],
    tree_source_body: dict[str, Any],
) -> FolderNode | None:
    if not args.create_folder_first:
        return None

    if not args.project_id:
        raise ValueError("--create-folder-first requires --project-id")

    target_name = resolve_folder_creation_name(
        args.create_folder_name, args.create_folder_name_template
    )
    if not target_name:
        raise ValueError(
            "--create-folder-first requires --create-folder-name or --create-folder-name-template"
        )

    parent_id = args.create_folder_parent_id.strip() if args.create_folder_parent_id else None
    discovered_nodes: list[FolderNode] = []
    discovery_errors: list[str] = []
    source = "none"
    try:
        if args.tree_source_endpoint:
            discovered_nodes, source = discover_folders_custom_tree_source(
                base_url=args.base_url,
                headers=headers,
                endpoint=args.tree_source_endpoint,
                method=args.tree_source_method,
                query_params=tree_source_query,
                body=tree_source_body,
            )
        if not discovered_nodes:
            discovered_nodes, source, fallback_errors = discover_folders_tree_fallback(
                base_url=args.base_url,
                headers=headers,
                project_id=args.project_id,
                folder_search_endpoint=args.folder_search_endpoint,
                foldertree_endpoint=args.foldertree_endpoint,
            )
            discovery_errors.extend(fallback_errors)
    except Exception as exc:  # pylint: disable=broad-except
        discovery_errors.append(str(exc))

    if discovered_nodes:
        existing = _find_existing_folder(discovered_nodes, target_name, parent_id)
        if existing:
            print(
                f"Folder already exists: id={existing.folder_id}, name='{existing.folder_name}', source={source}"
            )
            return existing
    elif discovery_errors:
        print("Folder existence check failed, continue with create attempt:")
        for error in discovery_errors[:10]:
            print(f"- {error}")

    extra_body = _parse_json_object_arg(
        args.create_folder_body_json, "--create-folder-body-json"
    )
    create_body = dict(extra_body)
    create_body.setdefault(args.create_folder_name_field, target_name)
    project_value = _normalize_project_or_parent(args.project_id)
    if project_value is not None:
        create_body.setdefault(args.create_folder_project_id_field, project_value)
    parent_value = _normalize_project_or_parent(parent_id)
    if parent_value is not None:
        create_body.setdefault(args.create_folder_parent_id_field, parent_value)

    if args.create_folder_dry_run:
        print("Folder create dry-run enabled.")
        print(f"Create endpoint: {args.create_folder_endpoint}")
        print(f"Create body: {json.dumps(create_body, ensure_ascii=False)}")
        return None

    payload = request_json(
        base_url=args.base_url,
        endpoint=args.create_folder_endpoint,
        headers=headers,
        method="POST",
        body=create_body,
    )
    nodes = _collect_folder_nodes(payload)
    created = _find_existing_folder(nodes, target_name, parent_id)
    if not created and nodes:
        created = sorted(nodes, key=lambda n: n.folder_id)[0]
    if not created:
        raise RuntimeError(
            "Folder creation response does not contain folder id/name. "
            "Set --create-folder-endpoint and field mappings for your Zephyr instance."
        )
    print(f"Created folder: id={created.folder_id}, name='{created.folder_name}'")
    return created


def discover_folders_custom_tree_source(
    base_url: str,
    headers: dict[str, str],
    endpoint: str,
    method: str,
    query_params: dict[str, Any],
    body: dict[str, Any],
) -> tuple[list[FolderNode], str]:
    params = {str(k): str(v) for k, v in query_params.items()}
    payload = request_json(
        base_url=base_url,
        endpoint=endpoint,
        headers=headers,
        params=params,
        method=method,
        body=body or None,
    )
    nodes = _collect_folder_nodes(payload)
    return nodes, f"{method} {endpoint}"


def select_tree_target_folders(
    nodes: list[FolderNode],
    root_folder_ids: list[str],
    leaf_only: bool,
    name_pattern: re.Pattern[str] | None,
    root_path_pattern: re.Pattern[str] | None,
) -> list[FolderNode]:
    by_id: dict[str, FolderNode] = {node.folder_id: node for node in nodes}
    children_by_parent: dict[str, list[str]] = defaultdict(list)
    for node in nodes:
        if node.parent_id:
            children_by_parent[node.parent_id].append(node.folder_id)

    # If API does not provide fullName/full_path, reconstruct path from parent links.
    cache: dict[str, str] = {}

    def build_path(folder_id: str) -> str:
        if folder_id in cache:
            return cache[folder_id]
        node = by_id.get(folder_id)
        if not node:
            return ""
        if node.full_path.strip():
            cache[folder_id] = node.full_path.strip()
            return cache[folder_id]
        if node.parent_id and node.parent_id in by_id:
            parent_path = build_path(node.parent_id)
            path = f"{parent_path}/{node.folder_name}".strip("/")
        else:
            path = node.folder_name
        cache[folder_id] = path
        return path

    for node in nodes:
        if not node.full_path.strip():
            node.full_path = build_path(node.folder_id)

    for node in nodes:
        node.is_leaf = len(children_by_parent.get(node.folder_id, [])) == 0

    allowed_ids: set[str]
    if root_folder_ids:
        allowed_ids = set()
        stack = list(root_folder_ids)
        while stack:
            current = stack.pop()
            if current in allowed_ids:
                continue
            allowed_ids.add(current)
            stack.extend(children_by_parent.get(current, []))
    else:
        allowed_ids = {node.folder_id for node in nodes}

    selected: list[FolderNode] = []
    for node in nodes:
        if node.folder_id not in allowed_ids:
            continue
        if leaf_only and not node.is_leaf:
            continue
        if name_pattern and not name_pattern.search(node.folder_name):
            continue
        if root_path_pattern and not root_path_pattern.search(node.full_path or ""):
            continue
        selected.append(node)
    return selected


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "_", value.strip().lower())
    return cleaned.strip("_") or "folder"


def _extract_folder_info(item: dict[str, Any]) -> tuple[str | None, str | None]:
    folder_id_raw = (
        item.get("folderId")
        or get_by_path(item, "folder.id")
        or item.get("folderTreeId")
    )
    if folder_id_raw is None:
        return None, None
    folder_name = (
        item.get("folderName")
        or get_by_path(item, "folder.name")
        or f"folder_{folder_id_raw}"
    )
    return str(folder_id_raw), str(folder_name)


def print_folder_field_debug(items: list[dict[str, Any]], limit: int = 15) -> None:
    samples = []
    for item in items[:limit]:
        samples.append(
            {
                "folderId": item.get("folderId"),
                "folderName": item.get("folderName"),
                "folder.id": get_by_path(item, "folder.id"),
                "folder.name": get_by_path(item, "folder.name"),
                "folderTreeId": item.get("folderTreeId"),
                "iterationId": item.get("iterationId"),
            }
        )
    print("Execution discovery field samples:")
    for sample in samples:
        print(f"- {sample}")


def print_resolved_folder_names(resolved: dict[str, str]) -> None:
    if not resolved:
        print("Resolved folder names: none")
        return
    print("Resolved folder names:")
    for folder_id, folder_name in sorted(resolved.items(), key=lambda item: item[1]):
        print(f"- {folder_id}: {folder_name}")


def print_resolved_folder_paths(resolved_paths: dict[str, str]) -> None:
    if not resolved_paths:
        print("Resolved folder paths: none")
        return
    print("Resolved folder paths:")
    for folder_id, path in sorted(resolved_paths.items(), key=lambda item: item[1]):
        print(f"- {folder_id}: {path}")


def aggregate_by_folder_from_executions(
    items: list[dict[str, Any]],
    date_fields: list[str],
    status_fields: list[str],
    from_date: date | None,
    to_date: date | None,
    root_folder_ids: list[str],
    allowed_root_folder_ids: set[str] | None = None,
    folder_name_pattern: re.Pattern[str] | None = None,
    resolved_folder_names: dict[str, str] | None = None,
    folder_path_pattern: re.Pattern[str] | None = None,
    resolved_folder_paths: dict[str, str] | None = None,
) -> tuple[list[tuple[FolderNode, dict[date, Counter[str]]]], Counter]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    names: dict[str, str] = {}
    stats = Counter()

    for item in items:
        folder_id, folder_name = _extract_folder_info(item)
        if not folder_id:
            stats["missing_folder"] += 1
            continue
        if root_folder_ids and folder_id not in root_folder_ids:
            stats["outside_discovered_root_ids"] += 1
            continue
        if allowed_root_folder_ids and folder_id not in allowed_root_folder_ids:
            stats["filtered_by_allowed_root_ids"] += 1
            continue
        grouped[folder_id].append(item)
        names.setdefault(folder_id, folder_name or f"folder_{folder_id}")

    rows: list[tuple[FolderNode, dict[date, Counter[str]]]] = []
    for folder_id in sorted(grouped.keys()):
        effective_folder_name = (
            (resolved_folder_names or {}).get(folder_id) or names[folder_id]
        )
        effective_folder_path = (resolved_folder_paths or {}).get(folder_id, "")
        if folder_name_pattern and not folder_name_pattern.search(effective_folder_name):
            stats["filtered_by_folder_name_regex"] += len(grouped[folder_id])
            continue
        if folder_path_pattern and not folder_path_pattern.search(effective_folder_path):
            stats["filtered_by_folder_path_regex"] += len(grouped[folder_id])
            continue
        weekly, _ = aggregate_weekly(
            items=grouped[folder_id],
            date_fields=date_fields,
            status_fields=status_fields,
            from_date=from_date,
            to_date=to_date,
        )
        rows.append(
            (
                FolderNode(
                    folder_id=folder_id, folder_name=effective_folder_name, parent_id=None
                ),
                weekly,
            )
        )
    stats["matched_folders"] = len(rows)
    return rows, stats


def resolve_folder_names_by_id(
    folder_ids: set[str],
    endpoint_templates: list[str],
    base_url: str,
    headers: dict[str, str],
) -> tuple[dict[str, str], dict[str, str], Counter]:
    def extract_folder_name(payload: Any) -> str | None:
        if isinstance(payload, dict):
            direct = payload.get("name") or payload.get("folderName")
            if isinstance(direct, str) and direct.strip():
                return direct.strip()
            full_name = payload.get("fullName")
            if isinstance(full_name, str) and full_name.strip():
                normalized = full_name.strip().rstrip("/")
                # API may return path-like fullName: '/root/subfolder'
                leaf = normalized.split("/")[-1].strip()
                if leaf:
                    return leaf
            nested = payload.get("folder")
            if isinstance(nested, dict):
                nested_name = nested.get("name") or nested.get("folderName")
                if isinstance(nested_name, str) and nested_name.strip():
                    return nested_name.strip()
            for key in ("values", "results", "items", "content", "folders"):
                value = payload.get(key)
                if isinstance(value, list) and value:
                    nested_name = extract_folder_name(value[0])
                    if nested_name:
                        return nested_name
        if isinstance(payload, list) and payload:
            return extract_folder_name(payload[0])
        return None

    resolved: dict[str, str] = {}
    resolved_paths: dict[str, str] = {}
    stats = Counter()
    unexpected_samples: list[str] = []
    if not folder_ids:
        return resolved, resolved_paths, stats
    if not endpoint_templates:
        stats["name_resolution_skipped_no_template"] = len(folder_ids)
        return resolved, resolved_paths, stats

    for folder_id in sorted(folder_ids):
        for template in endpoint_templates:
            endpoint = template.replace("{folder_id}", folder_id)
            try:
                payload = request_json(base_url, endpoint, headers, method="GET")
            except Exception:  # pylint: disable=broad-except
                stats["name_resolution_endpoint_failures"] += 1
                continue
            name = extract_folder_name(payload)
            if name:
                resolved[folder_id] = name
                if isinstance(payload, dict):
                    full_name = payload.get("fullName")
                    if isinstance(full_name, str) and full_name.strip():
                        resolved_paths[folder_id] = full_name.strip()
                stats["name_resolution_success"] += 1
                break
            stats["name_resolution_unexpected_payload"] += 1
            if len(unexpected_samples) < 1:
                text = str(payload)
                if len(text) > 300:
                    text = text[:300] + "..."
                unexpected_samples.append(text)
        if folder_id not in resolved:
            stats["name_resolution_missed_folder_ids"] += 1

    if unexpected_samples:
        print(f"Folder name resolution sample payload: {unexpected_samples[0]}")

    return resolved, resolved_paths, stats


def normalize_status(status_raw: str | None) -> str:
    if status_raw is None:
        return "other"
    value = status_raw.strip().lower()

    passed = {
        "pass",
        "passed",
        "success",
        "ok",
        "пройден",
        "пройдено",
    }
    failed = {
        "fail",
        "failed",
        "error",
        "провален",
        "не пройден",
        "непройден",
    }
    blocked = {
        "blocked",
        "заблокирован",
        "on hold",
        "can't test",
        "cant test",
        "cannot test",
    }
    not_executed = {
        "not executed",
        "not_executed",
        "untested",
        "to do",
        "todo",
        "wip",
        "in progress",
        "не выполнен",
        "не запускался",
        "not tested in this pi",
    }
    other = {
        "danger",
        "can't reproduce",
        "cant reproduce",
        "false positive",
    }

    if value in passed:
        return "passed"
    if value in failed:
        return "failed"
    if value in blocked:
        return "blocked"
    if value in not_executed:
        return "not_executed"
    if value in other:
        return "other"
    return "other"


def extract_first_str(item: dict[str, Any], field_paths: list[str]) -> str | None:
    for path in field_paths:
        value = get_by_path(item, path)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def extract_first_scalar_as_str(item: dict[str, Any], field_paths: list[str]) -> str | None:
    for path in field_paths:
        value = get_by_path(item, path)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return None


def _extract_test_case_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = extract_items(payload)
        if rows:
            return rows
        for key in ("testCases", "testcases", "cases"):
            nested = payload.get(key)
            if isinstance(nested, list):
                return [entry for entry in nested if isinstance(entry, dict)]
    if isinstance(payload, list):
        return [entry for entry in payload if isinstance(entry, dict)]
    return []


def _read_cycle_field(cycle: dict[str, Any], paths: list[str], default: str = "") -> str:
    value = extract_first_scalar_as_str(cycle, paths)
    return value or default


def _read_case_field(case: dict[str, Any], paths: list[str], default: str = "") -> str:
    value = extract_first_scalar_as_str(case, paths)
    return value or default


def _read_actual_start_date(cycle: dict[str, Any]) -> str:
    return _read_cycle_field(
        cycle,
        [
            "actualStartDate",
            "actualStart",
            "iteration.actualStartDate",
            "iteration.actualStart",
            "testRun.actualStartDate",
            "testRun.actualStart",
        ],
        "",
    )


def _normalize_display_date(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    try:
        return parse_datetime(value).date().isoformat()
    except ValueError:
        return value


def _resolve_display_date(actual_start_date: str, fallback_date: str) -> str:
    actual = _normalize_display_date(actual_start_date)
    if actual:
        return actual
    return _normalize_display_date(fallback_date)


def _parse_display_date(value: str) -> date | None:
    normalized = _normalize_display_date(value)
    if not normalized:
        return None
    try:
        return date.fromisoformat(normalized)
    except ValueError:
        return None


def _resolve_case_display_date(case: dict[str, Any]) -> str:
    return _resolve_display_date(
        str(case.get("actual_start_date") or ""),
        str(case.get("execution_date", case.get("cycle_updated_on", ""))),
    )


def _resolve_daily_title_date(cycles: dict[str, Any]) -> str:
    date_counter: Counter[str] = Counter()
    for cycle in cycles.values():
        if not isinstance(cycle, dict):
            continue
        for case in cycle.get("cases", {}).values():
            if not isinstance(case, dict):
                continue
            display_date = _resolve_case_display_date(case)
            if display_date:
                date_counter[display_date] += 1
    if not date_counter:
        return ""
    # "Most common" as requested; for ties use earliest date for deterministic output.
    top_count = max(date_counter.values())
    candidates = sorted(value for value, count in date_counter.items() if count == top_count)
    return candidates[0]


def _resolve_nightly_build_version_day(
    folder_name: str,
    cycles: dict[str, Any],
    *,
    allow_report_day_fallback: bool = True,
) -> date | None:
    """Calendar day for nightly-dev-YYYY.MM.DD build label.

    With a nightly-dev prefix in the folder name: prefix date + 1 day (daily title left).
    Otherwise, when allow_report_day_fallback: folder report day (weekly matrix logic).
    """
    left = _parse_weekly_column_label_from_folder_name(folder_name)
    if left:
        left_date = left.replace("nightly-dev-", "", 1)
        try:
            return datetime.strptime(left_date, "%Y.%m.%d").date() + timedelta(days=1)
        except ValueError:
            pass
    if allow_report_day_fallback:
        return _resolve_folder_report_day(folder_name, cycles)
    return None


def _build_daily_report_title(folder_name: str, cycles: dict[str, Any]) -> str:
    # Daily title format:
    # nightly-dev-YYYY.MM.DD, dow, dd.mm.yyyy
    left = _parse_weekly_column_label_from_folder_name(folder_name)
    left_date = left.replace("nightly-dev-", "", 1) if left else ""
    left_day = (
        _resolve_nightly_build_version_day(
            folder_name, cycles, allow_report_day_fallback=False
        )
        if left
        else None
    )
    right_day = _parse_report_day_from_folder_name(folder_name)
    if right_day is None:
        right_day = _resolve_folder_dominant_actual_date(cycles)
    if right_day is None:
        report_date = _resolve_daily_title_date(cycles)
        right_day = _parse_display_date(report_date) if report_date else None

    # Shift title dates by +1 day (titles/pages only; report columns unchanged).
    if right_day is not None:
        right_day = right_day + timedelta(days=1)

    dow_map = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
    dow = dow_map[right_day.weekday()] if right_day else ""
    right_str = right_day.strftime("%d.%m.%Y") if right_day else "unknown-date"
    left_str = (
        left_day.strftime("%Y.%m.%d")
        if left_day is not None
        else (left_date or str(folder_name or "").strip() or "unknown-date")
    )
    return f"nightly-dev-{left_str}, {dow}, {right_str}".strip().rstrip(",")


def _build_daily_report_base_name(folder_id: str, folder_name: str, cycles: dict[str, Any]) -> str:
    report_date = _resolve_daily_title_date(cycles) or "unknown-date"
    return f"nightly-dev-{slugify(folder_name)}_{report_date}_{folder_id}"


def build_cycle_case_rows(
    folder: FolderNode,
    cycles: list[dict[str, Any]],
    testcase_endpoint_templates: list[str],
    base_url: str,
    headers: dict[str, str],
    synthetic_cycle_ids: bool = False,
) -> list[list[str]]:
    rows: list[list[str]] = []
    for cycle in cycles:
        real_cycle_id = _read_cycle_field(
            cycle, ["iterationId", "iteration.id", "id", "testRunId"], ""
        )
        cycle_key = _read_cycle_field(cycle, ["iteration.key", "testRunKey", "key"], "")
        cycle_name = _read_cycle_field(cycle, ["iteration.name", "testRunName", "name"], "")
        cycle_status = _read_cycle_field(cycle, ["status.name", "status", "result"], "")
        cycle_updated_on = _read_cycle_field(cycle, ["updatedOn", "executedOn", "executionDate"], "")
        cycle_actual_start = _read_actual_start_date(cycle)
        cycle_id = real_cycle_id
        if not cycle_id and synthetic_cycle_ids:
            synthetic_date = "unknown_date"
            if cycle_updated_on:
                try:
                    synthetic_date = parse_datetime(cycle_updated_on).date().isoformat()
                except ValueError:
                    synthetic_date = "unknown_date"
            cycle_id = f"v:{folder.folder_id}:{synthetic_date}"
        cycle_case_id = _read_case_field(cycle, ["id", "testCase.id", "testCaseId", "key"], "")
        cycle_case_key = _read_case_field(cycle, ["key", "testCase.key", "testCaseKey"], "")
        cycle_case_name = _read_case_field(cycle, ["name", "testCase.name", "testCaseName"], "")
        cycle_case_status = _read_case_field(
            cycle,
            ["status.name", "status", "testExecutionStatus.name", "result"],
            "",
        )
        cycle_case_iteration_key = _read_case_field(
            cycle,
            [
                "testCase.iteration.key",
                "testCase.iterationKey",
                "testCase.iteration.id",
                "testCase.iterationId",
                "iteration.key",
                "iterationKey",
                "iteration.id",
                "iterationId",
            ],
            "",
        )

        if not real_cycle_id and not cycle_id:
            rows.append(
                [
                    folder.folder_id,
                    folder.folder_name,
                    cycle_id,
                    cycle_key,
                    cycle_name,
                    cycle_status,
                    cycle_updated_on,
                    cycle_case_id,
                    cycle_case_key,
                    cycle_case_name,
                    cycle_case_status,
                    cycle_actual_start,
                    cycle_case_iteration_key,
                ]
            )
            continue

        test_cases: list[dict[str, Any]] = []
        if real_cycle_id:
            for template in testcase_endpoint_templates:
                endpoint = template.replace("{cycle_id}", real_cycle_id)
                try:
                    payload = request_json(base_url, endpoint, headers, method="GET")
                except Exception:  # pylint: disable=broad-except
                    continue
                test_cases = _extract_test_case_rows(payload)
                if test_cases:
                    break

        if not test_cases:
            rows.append(
                [
                    folder.folder_id,
                    folder.folder_name,
                    cycle_id,
                    cycle_key,
                    cycle_name,
                    cycle_status,
                    cycle_updated_on,
                    cycle_case_id,
                    cycle_case_key,
                    cycle_case_name,
                    cycle_case_status,
                    cycle_actual_start,
                    cycle_case_iteration_key,
                ]
            )
            continue

        for case in test_cases:
            case_id = _read_case_field(case, ["id", "testCase.id", "testCaseId", "key"], "")
            case_key = _read_case_field(case, ["key", "testCase.key", "testCaseKey"], "")
            case_name = _read_case_field(case, ["name", "testCase.name", "testCaseName"], "")
            case_status = _read_case_field(
                case,
                ["status.name", "status", "testExecutionStatus.name", "result"],
                "",
            )
            case_iteration_key = _read_case_field(
                case,
                [
                    "testCase.iteration.key",
                    "testCase.iterationKey",
                    "testCase.iteration.id",
                    "testCase.iterationId",
                    "iteration.key",
                    "iterationKey",
                    "iteration.id",
                    "iterationId",
                ],
                "",
            )
            rows.append(
                [
                    folder.folder_id,
                    folder.folder_name,
                    cycle_id,
                    cycle_key,
                    cycle_name,
                    cycle_status,
                    cycle_updated_on,
                    case_id,
                    case_key,
                    case_name,
                    case_status,
                    cycle_actual_start,
                    case_iteration_key,
                ]
            )
    return rows


def fetch_test_result_status_names(
    base_url: str,
    headers: dict[str, str],
    project_id: str | None,
) -> dict[str, str]:
    if not project_id:
        return {}
    endpoint = f"rest/tests/1.0/project/{project_id}/testresultstatus"
    try:
        payload = request_json(base_url, endpoint, headers, method="GET")
    except Exception:  # pylint: disable=broad-except
        return {}
    statuses = payload if isinstance(payload, list) else []
    resolved: dict[str, str] = {}
    for status in statuses:
        if not isinstance(status, dict):
            continue
        status_id = status.get("id")
        status_name = status.get("name")
        if status_id is None or not isinstance(status_name, str):
            continue
        resolved[str(status_id)] = status_name
    return resolved


_TESTRUN_ITEMS_CACHE: dict[tuple[str, str], list[dict[str, Any]]] = {}
_TEST_RESULTS_CACHE: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
_DETAIL_CACHE_LOCK = threading.Lock()


def fetch_testrun_items(
    base_url: str, headers: dict[str, str], test_run_id: str
) -> list[dict[str, Any]]:
    cache_key = (base_url.rstrip("/"), str(test_run_id))
    with _DETAIL_CACHE_LOCK:
        cached = _TESTRUN_ITEMS_CACHE.get(cache_key)
    if cached is not None:
        return list(cached)

    endpoint = f"rest/tests/1.0/testrun/{test_run_id}/testrunitems"
    # Pull link-shaped fields too: depending on Zephyr deployment the
    # linked Jira issues may sit on the run item (`traceLinks`/`issueLinks`/
    # `defects`) instead of the test result. issueCount is kept as a
    # sanity check — non-zero with empty links == we still missed a field.
    params = {
        "fields": (
            "id,index,issueCount,traceLinks,issueLinks,defects,$lastTestResult"
        )
    }
    payload = request_json(base_url, endpoint, headers, params=params, method="GET")
    if isinstance(payload, dict):
        items = payload.get("testRunItems")
        if isinstance(items, list):
            out = [item for item in items if isinstance(item, dict)]
            with _DETAIL_CACHE_LOCK:
                _TESTRUN_ITEMS_CACHE[cache_key] = list(out)
            return out
    with _DETAIL_CACHE_LOCK:
        _TESTRUN_ITEMS_CACHE[cache_key] = []
    return []


def _fetch_links_endpoint_silent(
    base_url: str, headers: dict[str, str], endpoint: str
) -> Any:
    """GET helper that swallows network/HTTP errors for optional endpoints.

    Several Zephyr deployments expose linked-issue data via auxiliary
    endpoints whose exact path varies (`tracelink/testresult/{id}`,
    `testrunitem/{id}` without field filter, etc). We probe a few of them
    and ignore failures so the main flow stays unaffected.
    """
    try:
        return request_json(base_url, endpoint, headers, method="GET")
    except Exception:  # noqa: BLE001
        return None


def fetch_links_for_run_item(
    base_url: str,
    headers: dict[str, str],
    *,
    item_id: str,
    test_result_id: str,
) -> list[str]:
    """Try a handful of well-known Zephyr endpoints to collect linked issue keys.

    Returns whatever was found across all probes. Best-effort: empty list
    when nothing matches, never raises.
    """
    seen: set[str] = set()
    out: list[str] = []
    candidates: list[str] = []
    if test_result_id:
        candidates.extend(
            [
                f"rest/tests/1.0/tracelink/testresult/{test_result_id}",
                f"rest/tests/1.0/testresult/{test_result_id}",
                f"rest/tests/1.0/testresult/{test_result_id}/tracelinks",
                f"rest/tests/1.0/testresult/{test_result_id}/issuelinks",
            ]
        )
    if item_id:
        candidates.extend(
            [
                f"rest/tests/1.0/tracelink/testrunitem/{item_id}",
                f"rest/tests/1.0/testrunitem/{item_id}",
                f"rest/tests/1.0/testrunitem/{item_id}/tracelinks",
                f"rest/tests/1.0/testrunitem/{item_id}/issuelinks",
            ]
        )
    for endpoint in candidates:
        payload = _fetch_links_endpoint_silent(base_url, headers, endpoint)
        if payload is None:
            continue
        _maybe_debug_task_links_payload(f"probe {endpoint}", payload)
        candidate_links = _collect_task_links(payload)
        if not candidate_links and isinstance(payload, dict):
            for nested_key in ("traceLinks", "issueLinks", "links", "defects"):
                candidate_links.extend(_collect_task_links(payload.get(nested_key)))
        for key in candidate_links:
            if key and key not in seen:
                seen.add(key)
                out.append(key)
        if out:
            break
    return out


def fetch_test_results_for_item(
    base_url: str,
    headers: dict[str, str],
    test_run_id: str,
    item_id: str,
) -> list[dict[str, Any]]:
    cache_key = (base_url.rstrip("/"), str(test_run_id), str(item_id))
    with _DETAIL_CACHE_LOCK:
        cached = _TEST_RESULTS_CACHE.get(cache_key)
    if cached is not None:
        return list(cached)

    endpoint = f"rest/tests/1.0/testrun/{test_run_id}/testresults"
    # Some Zephyr deployments expose linked Jira issues as `traceLinks`,
    # others as `issueLinks`, `defects`, or even on the testCase itself
    # (e.g. when the user attached the issue at testCase level rather than
    # at execution level). Request all of them and let `_collect_task_links`
    # normalise the response.
    params = {
        "fields": (
            "id,testResultStatusId,executionDate,comment,"
            "traceLinks,issueLinks,defects,"
            "testCase(id,key,issueLinks,traceLinks,defects),"
            "testScriptResults("
            "id,testResultStatusId,executionDate,comment,index,description,"
            "expectedResult,testData,traceLinks,issueLinks,defects"
            ")"
        ),
        "itemId": item_id,
    }
    payload = request_json(base_url, endpoint, headers, params=params, method="GET")
    if isinstance(payload, list):
        out = [item for item in payload if isinstance(item, dict)]
        with _DETAIL_CACHE_LOCK:
            _TEST_RESULTS_CACHE[cache_key] = list(out)
        return out
    if isinstance(payload, dict):
        extracted = extract_items(payload)
        if extracted:
            with _DETAIL_CACHE_LOCK:
                _TEST_RESULTS_CACHE[cache_key] = list(extracted)
            return extracted
    with _DETAIL_CACHE_LOCK:
        _TEST_RESULTS_CACHE[cache_key] = []
    return []


_ISSUE_KEY_INLINE_PATTERN = re.compile(r"\b[A-Z][A-Z0-9]+-\d+\b")


def _extract_key_from_entry(entry: dict[str, Any]) -> str:
    """Try multiple Zephyr/Jira-style fields to find a Jira-issue key in a single link entry.

    Zephyr Squad / Scale return links in different shapes depending on
    whether the link sits on a testResult (`traceLinks`) or on a testCase
    (`issueLinks`/`defects`). We probe the common variants and finally fall
    back to scanning any URL/href for the canonical KEY-123 token.
    """
    direct_keys = (
        "issueKey",
        "displayKey",
        "key",
        "fromIssueKey",
        "toIssueKey",
        "backwardIssueKey",
        "forwardIssueKey",
    )
    for field in direct_keys:
        value = entry.get(field)
        if isinstance(value, str) and value.strip():
            text = value.strip()
            match = _ISSUE_KEY_INLINE_PATTERN.search(text)
            if match:
                return match.group(0)
            return text

    for nested_field in ("issue", "target", "source", "defect"):
        nested = entry.get(nested_field)
        if isinstance(nested, dict):
            nested_key = _extract_key_from_entry(nested)
            if nested_key:
                return nested_key

    for url_field in ("url", "href", "link", "self"):
        url_value = entry.get(url_field)
        if isinstance(url_value, str) and url_value.strip():
            match = _ISSUE_KEY_INLINE_PATTERN.search(url_value)
            if match:
                return match.group(0)

    name_value = entry.get("name") or entry.get("displayName")
    if isinstance(name_value, str):
        match = _ISSUE_KEY_INLINE_PATTERN.search(name_value)
        if match:
            return match.group(0)

    # Zephyr Test Player traceLinks deliver only the numeric Jira issue id
    # (e.g. {"issueId": 286915, "type": {...}}). Encode it as "id:N" so the
    # caller can batch-resolve via Jira REST and replace it with the key.
    for id_field in ("issueId", "issue_id", "fromIssueId", "toIssueId"):
        value = entry.get(id_field)
        if value is None:
            continue
        text = str(value).strip()
        if text.isdigit():
            return f"id:{text}"
    return ""


_TASK_LINKS_DEBUG_REMAINING = 3
_CASE_STEP_TASK_LINKS_INDEX = 22


def _task_links_fallback_enabled() -> bool:
    """Probe optional Zephyr link endpoints when inline fields do not expose defects."""
    return _parse_bool_env(os.getenv("ZEPHYR_FETCH_TASK_LINKS_FALLBACK", "true"))


def _maybe_debug_task_links_payload(label: str, payload: Any) -> None:
    """Dump the first few non-empty link payloads to stderr when debug is on.

    Enable with ZEPHYR_DEBUG_TASK_LINKS=true to investigate cases where
    Zephyr returns linked issues in a non-standard shape and our parser
    misses them.
    """
    global _TASK_LINKS_DEBUG_REMAINING
    if _TASK_LINKS_DEBUG_REMAINING <= 0:
        return
    if not _parse_bool_env(os.getenv("ZEPHYR_DEBUG_TASK_LINKS")):
        return
    if not payload:
        return
    try:
        rendered = json.dumps(payload, ensure_ascii=False, default=str)[:1200]
    except Exception:  # noqa: BLE001
        rendered = repr(payload)[:1200]
    sys.stderr.write(f"[task-links debug] {label}: {rendered}\n")
    _TASK_LINKS_DEBUG_REMAINING -= 1


def _collect_task_links(raw_links: Any) -> list[str]:
    links: list[str] = []
    if isinstance(raw_links, dict):
        for nested_key in ("traceLinks", "issueLinks", "links", "defects", "items"):
            nested = raw_links.get(nested_key)
            if isinstance(nested, list):
                links.extend(_collect_task_links(nested))
        return links
    if not isinstance(raw_links, list):
        return links
    _maybe_debug_task_links_payload("link list", raw_links)
    for entry in raw_links:
        if isinstance(entry, str):
            text = entry.strip()
            if not text:
                continue
            match = _ISSUE_KEY_INLINE_PATTERN.search(text)
            links.append(match.group(0) if match else text)
            continue
        if not isinstance(entry, dict):
            continue
        text = _extract_key_from_entry(entry)
        if text:
            links.append(text)
    return links


_JIRA_ID_TO_KEY_CACHE: dict[str, str] = {}


def _resolve_jira_issue_keys(
    base_url: str, headers: dict[str, str], ids: set[str]
) -> dict[str, str]:
    """Batch-resolve numeric Jira issue ids to keys. Cached per process.

    Zephyr Test Player traceLinks come with numeric `issueId` only, so we
    fan out a single `/rest/api/2/search?jql=id in (...)` per chunk to map
    them back to canonical keys (e.g. 286915 -> CSD-46501). Failures are
    swallowed: unresolved ids are kept as-is by the caller.
    """
    out: dict[str, str] = {}
    if not ids or not base_url:
        return out
    pending: list[str] = []
    for raw in ids:
        text = str(raw or "").strip()
        if not text or not text.isdigit():
            continue
        if text in _JIRA_ID_TO_KEY_CACHE:
            out[text] = _JIRA_ID_TO_KEY_CACHE[text]
            continue
        pending.append(text)
    if not pending:
        return out
    chunk_size = 100
    for start in range(0, len(pending), chunk_size):
        chunk = pending[start : start + chunk_size]
        try:
            payload = request_json(
                base_url,
                "/rest/api/2/search",
                headers,
                params={
                    "jql": f"id in ({','.join(chunk)})",
                    "fields": "summary",
                    "maxResults": str(len(chunk)),
                },
            )
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(
                f"[task-links] Jira id resolution failed for {len(chunk)} id(s): {exc}\n"
            )
            continue
        issues = payload.get("issues") if isinstance(payload, dict) else None
        if not isinstance(issues, list):
            continue
        for issue in issues:
            if not isinstance(issue, dict):
                continue
            issue_id = str(issue.get("id") or "").strip()
            issue_key = str(issue.get("key") or "").strip()
            if not issue_id or not issue_key:
                continue
            _JIRA_ID_TO_KEY_CACHE[issue_id] = issue_key
            out[issue_id] = issue_key
    return out


def _resolve_id_markers_in_links(
    text: str, id_to_key: dict[str, str]
) -> str:
    """Replace 'id:N' tokens inside a comma-joined task_links string."""
    if not text or "id:" not in text:
        return text
    parts: list[str] = []
    seen: set[str] = set()
    for raw in text.split(","):
        token = raw.strip()
        if not token:
            continue
        if token.startswith("id:"):
            num = token[3:]
            resolved = id_to_key.get(num, "")
            if resolved:
                token = resolved
            else:
                # keep id:N so the user can still see something rather than an
                # empty cell; downstream key extraction ignores non-KEY tokens.
                token = f"id:{num}"
        if token in seen:
            continue
        seen.add(token)
        parts.append(token)
    return ", ".join(parts)


def _join_unique(values: list[str]) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return ", ".join(out)


def build_case_step_rows(
    folder: FolderNode,
    cycles: list[dict[str, Any]],
    base_url: str,
    headers: dict[str, str],
    status_names: dict[str, str],
    synthetic_cycle_ids: bool = False,
    detail_workers: int = 1,
) -> list[list[str]]:
    worker_count = _bounded_worker_count(detail_workers, len(cycles))
    if worker_count > 1:
        indexed_rows: list[tuple[int, list[list[str]]]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_index = {
                executor.submit(
                    build_case_step_rows,
                    folder,
                    [cycle],
                    base_url,
                    headers,
                    status_names,
                    synthetic_cycle_ids,
                    1,
                ): idx
                for idx, cycle in enumerate(cycles)
            }
            for future in concurrent.futures.as_completed(future_to_index):
                indexed_rows.append((future_to_index[future], future.result()))
        rows: list[list[str]] = []
        for _idx, part in sorted(indexed_rows, key=lambda item: item[0]):
            rows.extend(part)
        return rows

    rows: list[list[str]] = []
    for cycle in cycles:
        test_run_id = _read_cycle_field(cycle, ["id", "testRunId"], "")
        if not test_run_id:
            continue
        cycle_real_id = _read_cycle_field(cycle, ["iterationId", "iteration.id"], "")
        cycle_key = _read_cycle_field(cycle, ["iteration.key", "testRunKey", "key"], "")
        cycle_name = _read_cycle_field(cycle, ["iteration.name", "testRunName", "name"], "")
        cycle_status = _read_cycle_field(cycle, ["status.name", "status", "result"], "")
        cycle_objective = _read_cycle_objective(cycle)
        cycle_actual_start = _read_actual_start_date(cycle)
        cycle_case_iteration_key = _read_case_field(
            cycle,
            [
                "testCase.iteration.key",
                "testCase.iterationKey",
                "testCase.iteration.id",
                "testCase.iterationId",
                "iteration.key",
                "iterationKey",
                "iteration.id",
                "iterationId",
            ],
            "",
        )
        cycle_updated_on = _read_cycle_field(
            cycle, ["updatedOn", "executedOn", "executionDate"], ""
        )
        cycle_id = cycle_real_id
        if not cycle_id and synthetic_cycle_ids:
            synthetic_date = "unknown_date"
            if cycle_updated_on:
                try:
                    synthetic_date = parse_datetime(cycle_updated_on).date().isoformat()
                except ValueError:
                    synthetic_date = "unknown_date"
            cycle_id = f"v:{folder.folder_id}:{synthetic_date}"

        try:
            run_items = fetch_testrun_items(base_url, headers, test_run_id)
        except Exception:  # pylint: disable=broad-except
            continue

        for run_item in run_items:
            item_id_raw = run_item.get("id")
            if item_id_raw is None:
                continue
            item_id = str(item_id_raw)
            last_result = run_item.get("$lastTestResult")
            case_id = ""
            case_key = ""
            case_name = ""
            case_objective = ""
            case_iteration_key = cycle_case_iteration_key
            if isinstance(last_result, dict):
                nested_case = last_result.get("testCase")
                if isinstance(nested_case, dict):
                    case_id = str(nested_case.get("id") or "")
                    case_key = str(nested_case.get("key") or "")
                    case_name = str(nested_case.get("name") or "")
                    case_objective = str(nested_case.get("objective") or "")
            # Issues attached at testRunItem level (some Zephyr deployments
            # surface "Defects" against the run item rather than the result).
            run_item_links = (
                _collect_task_links(run_item.get("traceLinks"))
                + _collect_task_links(run_item.get("issueLinks"))
                + _collect_task_links(run_item.get("defects"))
            )
            issue_count_raw = run_item.get("issueCount")
            try:
                issue_count_int = int(issue_count_raw) if issue_count_raw is not None else 0
            except (TypeError, ValueError):
                issue_count_int = 0
            if issue_count_int > 0 and not run_item_links:
                _maybe_debug_task_links_payload(
                    f"runItem {item_id} issueCount={issue_count_int} but no parsed links",
                    run_item,
                )

            try:
                test_results = fetch_test_results_for_item(
                    base_url=base_url,
                    headers=headers,
                    test_run_id=test_run_id,
                    item_id=item_id,
                )
            except Exception:  # pylint: disable=broad-except
                continue

            for test_result in test_results:
                test_result_id = str(test_result.get("id") or "")
                test_result_status_id = str(test_result.get("testResultStatusId") or "")
                test_result_status = status_names.get(
                    test_result_status_id, test_result_status_id
                )
                result_execution_date = str(test_result.get("executionDate") or "")
                test_result_links = (
                    _collect_task_links(test_result.get("traceLinks"))
                    + _collect_task_links(test_result.get("issueLinks"))
                    + _collect_task_links(test_result.get("defects"))
                )
                tr_test_case = test_result.get("testCase")
                if isinstance(tr_test_case, dict):
                    test_result_links.extend(
                        _collect_task_links(tr_test_case.get("traceLinks"))
                        + _collect_task_links(tr_test_case.get("issueLinks"))
                        + _collect_task_links(tr_test_case.get("defects"))
                    )
                test_result_links = run_item_links + test_result_links
                if not test_result_links and (
                    issue_count_int > 0
                    or _task_links_fallback_enabled()
                    or _parse_bool_env(os.getenv("ZEPHYR_DEBUG_TASK_LINKS"))
                ):
                    test_result_links = fetch_links_for_run_item(
                        base_url,
                        headers,
                        item_id=item_id,
                        test_result_id=test_result_id,
                    )
                script_results = test_result.get("testScriptResults")

                if isinstance(script_results, list) and script_results:
                    for step in script_results:
                        if not isinstance(step, dict):
                            continue
                        step_status_id = str(step.get("testResultStatusId") or "")
                        step_status = status_names.get(step_status_id, step_status_id)
                        step_links = (
                            _collect_task_links(step.get("traceLinks"))
                            + _collect_task_links(step.get("issueLinks"))
                            + _collect_task_links(step.get("defects"))
                        )
                        task_links = _join_unique(test_result_links + step_links)
                        rows.append(
                            [
                                folder.folder_id,
                                folder.folder_name,
                                cycle_id,
                                cycle_key,
                                cycle_name,
                                test_run_id,
                                case_id,
                                case_key,
                                case_name,
                                item_id,
                                test_result_id,
                                str(step.get("index") if step.get("index") is not None else ""),
                                str(step.get("description") or ""),
                                str(step.get("expectedResult") or ""),
                                str(step.get("comment") or ""),
                                step_status_id,
                                step_status,
                                str(step.get("executionDate") or result_execution_date),
                                cycle_status,
                                test_result_status,
                                cycle_objective,
                                case_objective,
                                task_links,
                                cycle_actual_start,
                                case_iteration_key,
                            ]
                        )
                else:
                    task_links = _join_unique(test_result_links)
                    rows.append(
                        [
                            folder.folder_id,
                            folder.folder_name,
                            cycle_id,
                            cycle_key,
                            cycle_name,
                            test_run_id,
                            case_id,
                            case_key,
                            case_name,
                            item_id,
                            test_result_id,
                            "",
                            "",
                            "",
                            str(test_result.get("comment") or ""),
                            test_result_status_id,
                            test_result_status,
                            result_execution_date,
                            cycle_status,
                            test_result_status,
                            cycle_objective,
                            case_objective,
                            task_links,
                            cycle_actual_start,
                            case_iteration_key,
                        ]
                    )

    # Final pass: collect all "id:N" markers from task_links, batch-resolve them
    # to Jira keys, and rewrite the column in place. This converts Zephyr-internal
    # numeric ids (returned in traceLinks[].issueId) into human-readable keys.
    pending_ids: set[str] = set()
    for row in rows:
        if len(row) <= _CASE_STEP_TASK_LINKS_INDEX:
            continue
        cell = str(row[_CASE_STEP_TASK_LINKS_INDEX] or "")
        if "id:" not in cell:
            continue
        for token in cell.split(","):
            token = token.strip()
            if token.startswith("id:") and token[3:].isdigit():
                pending_ids.add(token[3:])
    if pending_ids:
        id_to_key = _resolve_jira_issue_keys(base_url, headers, pending_ids)
        if id_to_key:
            for row in rows:
                if len(row) <= _CASE_STEP_TASK_LINKS_INDEX:
                    continue
                cell = str(row[_CASE_STEP_TASK_LINKS_INDEX] or "")
                if "id:" not in cell:
                    continue
                row[_CASE_STEP_TASK_LINKS_INDEX] = _resolve_id_markers_in_links(cell, id_to_key)
    return rows


def week_start(d: date) -> date:
    return d.fromordinal(d.toordinal() - d.weekday())


def aggregate_weekly(
    items: list[dict[str, Any]],
    date_fields: list[str],
    status_fields: list[str],
    from_date: date | None,
    to_date: date | None,
) -> tuple[dict[date, Counter[str]], Counter]:
    per_week: dict[date, Counter[str]] = defaultdict(Counter)
    skipped = Counter()

    for item in items:
        date_value = extract_first_str(item, date_fields)
        if not date_value:
            skipped["missing_date"] += 1
            continue
        try:
            execution_dt = parse_datetime(date_value)
        except ValueError:
            skipped["bad_date"] += 1
            continue

        execution_day = execution_dt.date()
        if from_date and execution_day < from_date:
            continue
        if to_date and execution_day > to_date:
            continue

        raw_status = extract_first_str(item, status_fields)
        status_label = raw_status if raw_status else "(no status)"
        per_week[week_start(execution_day)][status_label] += 1

    return per_week, skipped


def week_total(counter: Counter[str]) -> int:
    return int(sum(counter.values()))


def pass_count_for_week(counter: Counter[str]) -> int:
    return sum(
        count for status_label, count in counter.items() if normalize_status(status_label) == "passed"
    )


def all_status_labels(weekly: dict[date, Counter[str]]) -> list[str]:
    labels: set[str] = set()
    for counter in weekly.values():
        labels.update(counter.keys())
    return sorted(labels, key=str.lower)


def write_csv(path: str, weekly: dict[date, Counter[str]]) -> None:
    labels = all_status_labels(weekly)
    header = ["week_start", "total", *labels, "pass_rate_pct"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for week in sorted(weekly.keys()):
            counter = weekly[week]
            total = week_total(counter)
            passed = pass_count_for_week(counter)
            pass_rate = (passed / total * 100.0) if total else 0.0
            writer.writerow(
                [
                    week.isoformat(),
                    str(total),
                    *[str(counter.get(label, 0)) for label in labels],
                    f"{pass_rate:.2f}",
                ]
            )


def write_folder_summary_csv(
    path: str, folder_rows: list[tuple[FolderNode, dict[date, Counter[str]]]]
) -> None:
    header = [
        "folder_id",
        "folder_name",
        "week_start",
        "total",
        "passed",
        "failed",
        "blocked",
        "not_executed",
        "other",
        "pass_rate_pct",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for folder, weekly in folder_rows:
            for week in sorted(weekly.keys()):
                counter = weekly[week]
                total = week_total(counter)
                normalized = Counter(normalize_status(status) for status in counter.elements())
                pass_rate = (
                    normalized["passed"] / total * 100.0
                    if total
                    else 0.0
                )
                row = [
                    week.isoformat(),
                    str(total),
                    str(normalized["passed"]),
                    str(normalized["failed"]),
                    str(normalized["blocked"]),
                    str(normalized["not_executed"]),
                    str(
                        total
                        - normalized["passed"]
                        - normalized["failed"]
                        - normalized["blocked"]
                        - normalized["not_executed"]
                    ),
                    f"{pass_rate:.2f}",
                ]
                writer.writerow([folder.folder_id, folder.folder_name, *row])


def write_cycles_cases_csv(path: str, rows: list[list[str]]) -> None:
    header = [
        "folder_id",
        "folder_name",
        "cycle_id",
        "cycle_key",
        "cycle_name",
        "cycle_status",
        "cycle_updated_on",
        "test_case_id",
        "test_case_key",
        "test_case_name",
        "test_case_status",
        "cycle_actual_start_date",
        "test_case_iteration_key",
    ]
    output_dir = os.path.dirname(path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def write_case_steps_csv(path: str, rows: list[list[str]]) -> None:
    header = [
        "folder_id",
        "folder_name",
        "cycle_id",
        "cycle_key",
        "cycle_name",
        "test_run_id",
        "test_case_id",
        "test_case_key",
        "test_case_name",
        "test_run_item_id",
        "test_result_id",
        "step_index",
        "step_description",
        "step_expected_result",
        "step_comment",
        "step_status_id",
        "step_status_name",
        "step_execution_date",
        "cycle_status",
        "test_result_status_name",
        "cycle_objective",
        "objective",
        "task_links",
        "cycle_actual_start_date",
        "test_case_iteration_key",
    ]
    output_dir = os.path.dirname(path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def build_cycle_run_fallback_status(
    cycles_cases_rows: list[list[str]],
) -> dict[tuple[str, str], str]:
    """Map (folder_id, test_run_id) -> cycle-level status for fallback when step data lacks status."""
    out: dict[tuple[str, str], str] = {}
    for row in cycles_cases_rows:
        if len(row) < 11:
            continue
        folder_id, cycle_id, status = row[0], row[2], row[10]
        if not folder_id or not cycle_id or not status:
            continue
        if str(cycle_id).startswith("v:"):
            continue
        out[(folder_id, str(cycle_id))] = status
    return out


def build_case_iteration_key_fallback(
    cycles_cases_rows: list[list[str]],
) -> dict[tuple[str, str], str]:
    out: dict[tuple[str, str], str] = {}
    for row in cycles_cases_rows:
        if len(row) < 13:
            continue
        folder_id = str(row[0] or "").strip()
        test_case_id = str(row[7] or "").strip()
        test_case_key = str(row[8] or "").strip()
        cycle_key = str(row[3] or "").strip()
        iteration_key = str(row[12] or "").strip()
        if not folder_id or not iteration_key:
            continue
        if test_case_key:
            out[(folder_id, test_case_key)] = iteration_key
        if test_case_id:
            out[(folder_id, test_case_id)] = iteration_key
        if cycle_key:
            out[(folder_id, cycle_key)] = iteration_key
    return out


def aggregate_readable_daily_reports_from_steps(
    case_steps_rows: list[list[str]],
    cycles_cases_rows: list[list[str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Group folder -> test cycle (test_run_id) -> real test cases with result and comment from steps.
    Result priority: step_status_name, then test_result_status_name, then cycles CSV fallback.
    """
    fallback_status = build_cycle_run_fallback_status(cycles_cases_rows)
    fallback_iteration = build_case_iteration_key_fallback(cycles_cases_rows)
    case_merge: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for row in case_steps_rows:
        if len(row) < 20:
            continue
        folder_id = row[0]
        folder_name = row[1]
        cycle_key_disp = row[3]
        cycle_name_disp = row[4]
        test_run_id = str(row[5]).strip() if row[5] else ""
        test_case_key = row[7]
        test_case_name = row[8]
        step_comment = row[14] if len(row) > 14 else ""
        step_status_name = row[16] if len(row) > 16 else ""
        step_execution_date = row[17] if len(row) > 17 else ""
        test_result_status_name = row[19] if len(row) > 19 else ""
        cycle_objective = row[20] if len(row) > 20 else ""
        objective = row[21] if len(row) > 21 else ""
        task_links = row[22] if len(row) > 22 else ""
        cycle_actual_start_date = row[23] if len(row) > 23 else ""
        case_iteration_key = row[24] if len(row) > 24 else ""
        if not case_iteration_key:
            case_iteration_key = (
                fallback_iteration.get((folder_id, test_case_key))
                or fallback_iteration.get((folder_id, row[6] if len(row) > 6 else ""))
                or fallback_iteration.get((folder_id, cycle_key_disp))
                or ""
            )

        if not folder_id or not folder_name or not test_run_id or not test_case_key:
            continue

        mkey = (folder_id, folder_name, test_run_id, test_case_key)
        if mkey not in case_merge:
            parts_init: list[str] = []
            sc0 = step_comment.strip() if step_comment else ""
            if sc0:
                parts_init.append(sc0)
            case_merge[mkey] = {
                "test_case_name": test_case_name or "",
                "cycle_key": cycle_key_disp or "",
                "cycle_name": cycle_name_disp or "",
                "cycle_objective": cycle_objective or "",
                "step_status_name": step_status_name or "",
                "test_result_status_name": test_result_status_name or "",
                "step_comment": sc0,
                "logs_comment_parts": parts_init,
                "step_execution_date": step_execution_date or "",
                "actual_start_date": cycle_actual_start_date or "",
                "case_iteration_key": case_iteration_key or "",
                "objective": objective or "",
                "task_links": task_links or "",
            }
        else:
            m = case_merge[mkey]
            if test_case_name and not m["test_case_name"]:
                m["test_case_name"] = test_case_name
            if cycle_key_disp and not m["cycle_key"]:
                m["cycle_key"] = cycle_key_disp
            if cycle_name_disp and not m["cycle_name"]:
                m["cycle_name"] = cycle_name_disp
            if cycle_objective and not m["cycle_objective"]:
                m["cycle_objective"] = cycle_objective
            if step_status_name and not m["step_status_name"]:
                m["step_status_name"] = step_status_name
            if test_result_status_name and not m["test_result_status_name"]:
                m["test_result_status_name"] = test_result_status_name
            sc = step_comment.strip() if step_comment else ""
            if sc and sc not in m.setdefault("logs_comment_parts", []):
                m["logs_comment_parts"].append(sc)
            if sc and not m["step_comment"]:
                m["step_comment"] = sc
            if step_execution_date and step_execution_date > (m["step_execution_date"] or ""):
                m["step_execution_date"] = step_execution_date
            if cycle_actual_start_date and not m["actual_start_date"]:
                m["actual_start_date"] = cycle_actual_start_date
            if case_iteration_key and not m["case_iteration_key"]:
                m["case_iteration_key"] = case_iteration_key
            if objective and not m["objective"]:
                m["objective"] = objective
            if task_links:
                merged_links = _join_unique(
                    [item.strip() for item in f"{m['task_links']}, {task_links}".split(",")]
                )
                m["task_links"] = merged_links

    reports: dict[tuple[str, str], dict[str, Any]] = {}
    for (folder_id, folder_name, test_run_id, test_case_key), m in case_merge.items():
        report_key = (folder_id, folder_name)
        report = reports.setdefault(report_key, {"cycles": {}})
        cycle_bucket = report["cycles"].setdefault(
            test_run_id,
            {
                "cycle_id": test_run_id,
                "cycle_key": "",
                "cycle_name": "",
                "cycle_objective": "",
                "cases": {},
            },
        )
        if m["cycle_key"]:
            cycle_bucket["cycle_key"] = m["cycle_key"]
        if m["cycle_name"]:
            cycle_bucket["cycle_name"] = m["cycle_name"]
        if m["cycle_objective"]:
            cycle_bucket["cycle_objective"] = m["cycle_objective"]

        # Prefer test-result status over per-step status:
        # step rows are often "Not Executed" even when the overall case result is Pass/Fail.
        result = (
            m["test_result_status_name"]
            or m["step_status_name"]
            or fallback_status.get((folder_id, test_run_id), "")
        )
        parts = m.get("logs_comment_parts") or []
        logs_source = "\n".join(dict.fromkeys(parts)) if parts else (m.get("step_comment") or "")
        cycle_bucket["cases"][test_case_key] = {
            "test_case_key": test_case_key,
            "test_case_name": m["test_case_name"],
            "result": result,
            "execution_date": _resolve_display_date(m["actual_start_date"], m["step_execution_date"]),
            "actual_start_date": _normalize_display_date(m["actual_start_date"]),
            "case_iteration_key": m["case_iteration_key"],
            "comment": m["step_comment"],
            "objective": m["objective"],
            "tasks": m["task_links"],
            "logs_source_text": logs_source,
        }

    return reports


def aggregate_readable_daily_reports_legacy(
    cycles_cases_rows: list[list[str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Fallback when case_steps_rows is empty: one row per cycle-run as in cycles CSV."""
    reports: dict[tuple[str, str], dict[str, Any]] = {}
    for row in cycles_cases_rows:
        if len(row) < 11:
            continue
        folder_id, folder_name = row[0], row[1]
        cycle_id, cycle_key, cycle_name = row[2], row[3], row[4]
        cycle_updated_on = row[6]
        case_key, case_name, case_status = row[8], row[9], row[10]
        cycle_actual_start = row[11] if len(row) > 11 else ""
        case_iteration_key = row[12] if len(row) > 12 else ""

        if not folder_id or not folder_name:
            continue
        report_key = (folder_id, folder_name)
        report = reports.setdefault(report_key, {"cycles": {}})
        cycle_bucket = report["cycles"].setdefault(
            cycle_id or cycle_key or cycle_name or "unknown_cycle",
            {
                "cycle_id": cycle_id,
                "cycle_key": cycle_key,
                "cycle_name": cycle_name,
                "cycle_objective": "",
                "cases": {},
            },
        )
        case_bucket_key = case_key or case_name or "unknown_case"
        existing = cycle_bucket["cases"].get(case_bucket_key)
        candidate = {
            "test_case_key": case_key,
            "test_case_name": case_name,
            "result": case_status,
            "execution_date": _resolve_display_date(cycle_actual_start, cycle_updated_on),
            "actual_start_date": _normalize_display_date(cycle_actual_start),
            "case_iteration_key": case_iteration_key,
            "comment": "",
            "objective": "",
            "tasks": "",
            "logs_source_text": "",
        }
        if existing is None:
            cycle_bucket["cases"][case_bucket_key] = candidate
        else:
            prev_dt = existing["execution_date"] or ""
            next_dt = candidate["execution_date"] or ""
            if next_dt > prev_dt:
                cycle_bucket["cases"][case_bucket_key] = candidate
    return reports


def _wiki_escape(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "\\\\")


_LOGVIEWER_URL_DEFAULT_RE = re.compile(
    r"https://logviewer\.df\.sbauto\.tech/logs/[^\s)\"'<>]+",
    re.IGNORECASE,
)


def _logviewer_url_regex() -> re.Pattern[str]:
    raw = (os.getenv("ZEPHYR_LOGVIEWER_URL_REGEX") or "").strip()
    if raw:
        try:
            return re.compile(raw, re.IGNORECASE)
        except re.error:
            pass
    return _LOGVIEWER_URL_DEFAULT_RE


def extract_logviewer_urls(text: str) -> list[str]:
    """Return unique logviewer URLs in first-seen order."""
    if not text:
        return []
    seen_lower: set[str] = set()
    out: list[str] = []
    for match in _logviewer_url_regex().finditer(text):
        url = match.group(0).rstrip(".,;)>]")
        low = url.lower()
        if low in seen_lower:
            continue
        seen_lower.add(low)
        out.append(url)
    return out


def _build_log_folder_nightly_display_and_date(
    folder_name: str,
    cycles: dict[str, Any],
) -> tuple[str, date | None]:
    """Label like nightly-dev-YYYY.MM.DD (daily title left date) plus sortable date."""
    build_day = _resolve_nightly_build_version_day(folder_name, cycles)
    if build_day is not None:
        disp = f"nightly-dev-{build_day.strftime('%Y.%m.%d')}"
        return disp, build_day
    slug = str(folder_name or "").strip() or "unknown"
    return slug, None


def _parse_jira_keys_from_tasks_field(tasks: str) -> list[str]:
    if not tasks or not str(tasks).strip():
        return []
    seen: set[str] = set()
    out: list[str] = []
    for raw in str(tasks).replace(";", ",").split(","):
        token = raw.strip()
        if not token:
            continue
        match = _ISSUE_KEY_INLINE_PATTERN.search(token)
        key = match.group(0) if match else ""
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _gather_jira_issue_build_log_pages(
    report_data: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, list[tuple[str, date | None, list[str]]]]:
    """Map Jira issue key -> blocks (build_display, sort_date, urls), newest build first."""
    bucket: defaultdict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for (_folder_id, folder_name), payload in report_data.items():
        cycles = payload.get("cycles") or {}
        if not isinstance(cycles, dict):
            continue
        build_display, sort_d = _build_log_folder_nightly_display_and_date(folder_name, cycles)
        for cycle in cycles.values():
            if not isinstance(cycle, dict):
                continue
            for case in cycle.get("cases", {}).values():
                if not isinstance(case, dict):
                    continue
                text = str(case.get("logs_source_text") or case.get("comment") or "")
                urls = extract_logviewer_urls(text)
                if not urls:
                    continue
                keys = _parse_jira_keys_from_tasks_field(str(case.get("tasks") or ""))
                for ik in keys:
                    cell = bucket[ik].setdefault(
                        build_display,
                        {"sort_date": sort_d, "urls": []},
                    )
                    if sort_d and (
                        cell["sort_date"] is None or sort_d > cell["sort_date"]
                    ):
                        cell["sort_date"] = sort_d
                    lst: list[str] = cell["urls"]
                    seen_lower = {x.lower() for x in lst}
                    for u in urls:
                        low = u.lower()
                        if low not in seen_lower:
                            seen_lower.add(low)
                            lst.append(u)
    out: dict[str, list[tuple[str, date | None, list[str]]]] = {}
    for issue_key, by_build in bucket.items():
        blocks: list[tuple[str, date | None, list[str]]] = []
        for bdisp, info in by_build.items():
            ulist = info.get("urls") or []
            if not ulist:
                continue
            blocks.append((bdisp, info.get("sort_date"), ulist))
        if not blocks:
            continue
        blocks.sort(
            key=lambda row: (row[1] or date.min, row[0]),
            reverse=True,
        )
        out[issue_key] = blocks
    return out


def render_jira_issue_build_log_html(
    issue_key: str,
    summary: str,
    blocks: list[tuple[str, date | None, list[str]]],
) -> str:
    page_heading = summary.strip() if summary.strip() else issue_key
    doc_title = f"{page_heading} ({issue_key})" if summary.strip() else issue_key
    parts: list[str] = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        f"<title>{html.escape(doc_title)}</title>",
        (
            "<style>"
            "body{font-family:Arial,sans-serif;margin:24px;line-height:1.45;}"
            ".issue-key{color:#666;font-size:0.95rem;margin:-8px 0 20px;}"
            ".blk{margin:20px 0 28px;}"
            "ul{margin:8px 0;padding-left:22px;}"
            "li{margin:4px 0;}"
            "a{color:#0969da;word-break:break-all;}"
            "</style>"
        ),
        "</head><body>",
        f"<h1>{html.escape(page_heading)}</h1>",
    ]
    if summary.strip():
        parts.append(f"<p class='issue-key'>{html.escape(issue_key)}</p>")
    for build_display, _sd, urls in blocks:
        line = f"Воспроизводится на {build_display}:"
        parts.append("<div class='blk'>")
        parts.append(f"<p><strong>{html.escape(line)}</strong></p>")
        parts.append("<ul>")
        parts.extend(
            f"<li><a href='{html.escape(u, quote=True)}' rel='noopener' target='_blank'>"
            f"{html.escape(u)}</a></li>"
            for u in urls
        )
        parts.append("</ul>")
        parts.append("</div>")
    parts.append("</body></html>")
    return "\n".join(parts)


def render_jira_issue_build_log_wiki(
    issue_key: str,
    summary: str,
    blocks: list[tuple[str, date | None, list[str]]],
) -> str:
    page_heading = summary.strip() if summary.strip() else issue_key
    lines: list[str] = [f"h1. {_wiki_escape(page_heading)}"]
    if summary.strip():
        lines.append(_wiki_escape(issue_key))
        lines.append("")
    for build_display, _sd, urls in blocks:
        line = f"Воспроизводится на {build_display}:"
        lines.append(f"*{_wiki_escape(line)}*")
        lines.append("")
        for u in urls:
            lines.append(f"* [{_wiki_escape(u)}|{u}]")
        lines.append("")
    return "\n".join(lines)


def _load_readable_template_file(template_dir: str | None, *parts: str) -> str | None:
    if not template_dir:
        return None
    path = os.path.join(template_dir, *parts)
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as f:
        return f.read()


def _resolve_readable_template(
    template_dir: str | None,
    kind: str,
    format_name: str,
    folder_id: str | None,
) -> str | None:
    if not template_dir or format_name not in ("html", "wiki"):
        return None
    subdir = "html" if format_name == "html" else "wiki"
    ext = ".html" if format_name == "html" else ".confluence.txt"
    if folder_id:
        text = _load_readable_template_file(
            template_dir, kind, subdir, f"{folder_id}{ext}"
        )
        if text is not None:
            return text
    return _load_readable_template_file(template_dir, kind, subdir, f"default{ext}")


def _apply_readable_template_placeholders(raw: str, mapping: dict[str, str]) -> str:
    out = raw
    for key, value in mapping.items():
        out = out.replace("{" + key + "}", value)
    return out


def _readable_template_mapping(
    folder_id: str,
    folder_name: str,
    week_start: date | None,
    escape: Callable[[str], str],
    *,
    week_builds_html: str = "",
    week_builds_wiki: str = "",
) -> dict[str, str]:
    week_label = week_start.isoformat() if week_start else "N/A"
    return {
        "folder_id": escape(str(folder_id)),
        "folder_name": escape(str(folder_name)),
        "folder_name_slug": escape(slugify(folder_name)),
        "week_start": escape(week_label),
        "week_label": escape(week_label),
        "week_builds_html": week_builds_html,
        "week_builds_wiki": week_builds_wiki,
    }


def _format_readable_html_preamble(
    template_dir: str | None,
    kind: str,
    folder_id_resolve: str | None,
    folder_id_mapping: str,
    folder_name: str,
    week_start: date | None,
    *,
    week_builds_html: str = "",
    week_builds_wiki: str = "",
) -> str:
    raw = _resolve_readable_template(template_dir, kind, "html", folder_id_resolve)
    if not raw or not raw.strip():
        return ""
    mapping = _readable_template_mapping(
        folder_id_mapping,
        folder_name,
        week_start,
        html.escape,
        week_builds_html=week_builds_html,
        week_builds_wiki=week_builds_wiki,
    )
    body = _apply_readable_template_placeholders(raw.strip(), mapping)
    return f"<div class='report-preamble'>{body}</div>"


def _format_readable_wiki_preamble(
    template_dir: str | None,
    kind: str,
    folder_id_resolve: str | None,
    folder_id_mapping: str,
    folder_name: str,
    week_start: date | None,
    *,
    week_builds_html: str = "",
    week_builds_wiki: str = "",
) -> str:
    raw = _resolve_readable_template(template_dir, kind, "wiki", folder_id_resolve)
    if not raw or not raw.strip():
        return ""
    mapping = _readable_template_mapping(
        folder_id_mapping,
        folder_name,
        week_start,
        _wiki_escape,
        week_builds_html=week_builds_html,
        week_builds_wiki=week_builds_wiki,
    )
    return _apply_readable_template_placeholders(raw.strip(), mapping)


_URL_PATTERN = re.compile(r"(https?://[^\s<>'\"|]+)")


def _render_html_with_links(text: str) -> str:
    if not text:
        return ""
    parts = _URL_PATTERN.split(text)
    rendered: list[str] = []
    for part in parts:
        if not part:
            continue
        if _URL_PATTERN.fullmatch(part):
            safe_url = html.escape(part, quote=True)
            safe_text = html.escape(part)
            rendered.append(
                f"<a href='{safe_url}' target='_blank' rel='noopener'>{safe_text}</a>"
            )
        else:
            rendered.append(html.escape(part))
    return "".join(rendered)


def _html_comment_cell(text: str) -> str:
    """Escape comment but allow line breaks from Zephyr <br> tags."""
    if not text:
        return ""
    chunks = re.split(r"(?i)<br\s*/?>", text)
    return "<br/>".join(_render_html_with_links(chunk) for chunk in chunks)


def _wiki_text_with_links(text: str) -> str:
    if not text:
        return ""
    with_breaks = re.sub(r"(?i)<br\s*/?>", "\n", text)
    parts = _URL_PATTERN.split(with_breaks)
    rendered: list[str] = []
    for part in parts:
        if not part:
            continue
        if _URL_PATTERN.fullmatch(part):
            rendered.append(f"[{part}|{part}]")
        else:
            rendered.append(_wiki_escape(part))
    return "".join(rendered)


_JIRA_TASK_KEY_RE = re.compile(r"\b[A-Z][A-Z0-9]+-\d+\b")


def _html_tasks_render(chunk: str) -> str:
    """Render a tasks-cell fragment, turning Jira keys into clickable links.

    Behaves like _render_html_with_links for non-key text, plus wraps every
    KEY-123 token into an <a href="/browse/KEY"> with class daily-jira-key
    so the Confluence publisher can later swap it for the {jira} macro.
    """
    if not chunk:
        return ""
    parts = _JIRA_TASK_KEY_RE.split(chunk)
    keys = _JIRA_TASK_KEY_RE.findall(chunk)
    out: list[str] = []
    for idx, part in enumerate(parts):
        out.append(_render_html_with_links(part))
        if idx < len(keys):
            key = keys[idx]
            url = _jira_issue_url(key)
            out.append(
                f"<a class='daily-jira-key' href='{html.escape(url, quote=True)}' "
                f"target='_blank' rel='noopener'>{html.escape(key)}</a>"
            )
    return "".join(out)


def _html_tasks_cell(text: str) -> str:
    if not text:
        return ""
    chunks = re.split(r"(?i)<br\s*/?>", text)
    return "<br/>".join(_html_tasks_render(chunk) for chunk in chunks)


def _wiki_tasks_cell(text: str) -> str:
    """Wiki version of tasks-cell renderer with Jira keys as native links."""
    if not text:
        return ""
    use_plain = _parse_bool_env(os.getenv("ZEPHYR_WEEKLY_WIKI_PLAIN_JIRA_LINKS"))
    with_breaks = re.sub(r"(?i)<br\s*/?>", "\n", text)
    out_lines: list[str] = []
    for line in with_breaks.split("\n"):
        parts = _JIRA_TASK_KEY_RE.split(line)
        keys = _JIRA_TASK_KEY_RE.findall(line)
        rendered: list[str] = []
        for idx, part in enumerate(parts):
            if part:
                # URLs inside text -> clickable wiki link, rest -> escaped.
                url_parts = _URL_PATTERN.split(part)
                for sub in url_parts:
                    if not sub:
                        continue
                    if _URL_PATTERN.fullmatch(sub):
                        rendered.append(f"[{sub}|{sub}]")
                    else:
                        rendered.append(_wiki_escape(sub))
            if idx < len(keys):
                key = keys[idx]
                if use_plain:
                    rendered.append(f"[{_wiki_escape(key)}|{_jira_issue_url(key)}]")
                else:
                    rendered.append(f"{{jira:key={_wiki_escape(key)}}}")
        out_lines.append("".join(rendered))
    return "\n".join(out_lines)


def _jira_cycle_url(cycle_key: str) -> str:
    base_url = os.getenv("ZEPHYR_BASE_URL", "https://jira.navio.auto").rstrip("/")
    return f"{base_url}/secure/Tests.jspa#/testCycle/{urllib.parse.quote(cycle_key)}"


def _jira_issue_url(issue_key: str) -> str:
    base_url = (
        (os.getenv("ZEPHYR_JIRA_BASE_URL") or "").strip().rstrip("/")
        or (os.getenv("ZEPHYR_BASE_URL") or "https://jira.navio.auto").strip().rstrip("/")
    )
    return f"{base_url}/browse/{urllib.parse.quote(issue_key)}"


_JIRA_META_CACHE: dict[str, dict[str, str]] = {}


def _resolve_weekly_jira_metadata_base(cli_base_url: str) -> str:
    """REST base for Jira issue/search (may differ from Zephyr Scale API host)."""
    return (
        (os.getenv("ZEPHYR_JIRA_BASE_URL") or "").strip().rstrip("/")
        or (os.getenv("ZEPHYR_BASE_URL") or "").strip().rstrip("/")
        or (cli_base_url or "").strip().rstrip("/")
    )


def _jira_bug_metadata_auth_headers(
    zephyr_headers: dict[str, str] | None,
) -> dict[str, str] | None:
    """Use ``ZEPHYR_JIRA_API_TOKEN`` when Jira REST must not reuse the Zephyr token."""
    token = (os.getenv("ZEPHYR_JIRA_API_TOKEN") or "").strip()
    if token:
        header = (os.getenv("ZEPHYR_JIRA_TOKEN_HEADER") or "Authorization").strip()
        prefix = (os.getenv("ZEPHYR_JIRA_TOKEN_PREFIX") or "Bearer").strip()
        return build_headers(header, prefix, token)
    return zephyr_headers


def _fetch_jira_bug_metadata(
    keys: list[str],
    *,
    base_url: str,
    auth_headers: dict[str, str] | None,
) -> dict[str, dict[str, str]]:
    """Batch-fetch Jira issue metadata for the given keys.

    Returns {key: {summary, status, priority, issuetype}}.
    Errors and missing keys are silently skipped — the caller falls back to plain key links.
    """
    out: dict[str, dict[str, str]] = {}
    eff_headers = _jira_bug_metadata_auth_headers(auth_headers)
    if not keys or not base_url or not eff_headers:
        return out

    pending: list[str] = []
    for key in keys:
        clean = str(key or "").strip()
        if not clean:
            continue
        if clean in _JIRA_META_CACHE:
            out[clean] = _JIRA_META_CACHE[clean]
            continue
        if clean not in pending:
            pending.append(clean)
    if not pending:
        return out

    field_keys = ["summary", "status", "priority", "issuetype"]
    fields_csv = ",".join(field_keys)

    def _issue_to_entry(issue: dict[str, Any]) -> tuple[str, dict[str, str]] | None:
        key = str(issue.get("key") or "").strip()
        if not key:
            return None
        fields = issue.get("fields") or {}
        if not isinstance(fields, dict):
            fields = {}
        status_obj = fields.get("status") or {}
        priority_obj = fields.get("priority") or {}
        issuetype_obj = fields.get("issuetype") or {}
        entry = {
            "summary": str(fields.get("summary") or "").strip(),
            "status": str(
                (status_obj.get("name") if isinstance(status_obj, dict) else "") or ""
            ).strip(),
            "priority": str(
                (priority_obj.get("name") if isinstance(priority_obj, dict) else "") or ""
            ).strip(),
            "issuetype": str(
                (issuetype_obj.get("name") if isinstance(issuetype_obj, dict) else "") or ""
            ).strip(),
        }
        return key, entry

    def _fetch_jira_issue_by_key(issue_key: str) -> dict[str, Any] | None:
        """GET single issue (fallback when search returns nothing or omits keys)."""
        k = str(issue_key).strip()
        if not k:
            return None
        path_key = urllib.parse.quote(k, safe="")
        for prefix in ("/rest/api/2/issue/", "/rest/api/3/issue/"):
            try:
                payload = request_json(
                    base_url,
                    f"{prefix}{path_key}",
                    eff_headers,
                    params={"fields": fields_csv},
                )
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            fields = payload.get("fields")
            if not isinstance(fields, dict):
                continue
            return {"key": str(payload.get("key") or k).strip(), "fields": fields}
        return None

    def _search_chunk(chunk: list[str]) -> list[dict[str, Any]] | None:
        jql_keys = ",".join(chunk)
        jql = f"key in ({jql_keys})"
        post_body = {
            "jql": jql,
            "fields": field_keys,
            "maxResults": len(chunk),
        }
        last_exc: BaseException | None = None
        for endpoint in ("/rest/api/2/search", "/rest/api/3/search"):
            for _, call_kw in (
                ("POST", {"method": "POST", "body": post_body}),
                (
                    "GET",
                    {
                        "params": {
                            "jql": jql,
                            "fields": fields_csv,
                            "maxResults": str(len(chunk)),
                        },
                    },
                ),
            ):
                try:
                    payload = request_json(
                        base_url,
                        endpoint,
                        eff_headers,
                        **call_kw,
                    )
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    continue
                if not isinstance(payload, dict):
                    continue
                err_msgs = payload.get("errorMessages")
                if isinstance(err_msgs, list) and err_msgs:
                    last_exc = RuntimeError("; ".join(str(m) for m in err_msgs))
                    continue
                issues = payload.get("issues")
                if isinstance(issues, list) and len(issues) > 0:
                    return issues
                if isinstance(issues, list) and len(issues) == 0:
                    # Empty list: keep trying (e.g. GET can be empty while POST works).
                    last_exc = RuntimeError("Jira search returned zero issues for this chunk")
                else:
                    last_exc = RuntimeError(
                        f"Jira search response has no issues list (keys={chunk[:3]}…)"
                    )
        if last_exc is not None:
            sys.stderr.write(
                f"[weekly] Jira metadata lookup failed for {len(chunk)} key(s): {last_exc!r}\n"
            )
        return None

    chunk_size = 100
    for start in range(0, len(pending), chunk_size):
        chunk = pending[start : start + chunk_size]
        issues: list[dict[str, Any]] = list(_search_chunk(chunk) or [])
        got_keys = {
            str(i.get("key") or "").strip().upper()
            for i in issues
            if isinstance(i, dict) and str(i.get("key") or "").strip()
        }
        for req in chunk:
            r = str(req).strip()
            if not r or r.upper() in got_keys:
                continue
            single = _fetch_jira_issue_by_key(r)
            if isinstance(single, dict):
                issues.append(single)
                got_keys.add(r.upper())
        if not issues:
            continue
        for issue in issues:
            if not isinstance(issue, dict):
                continue
            parsed = _issue_to_entry(issue)
            if parsed is None:
                continue
            key, entry = parsed
            _JIRA_META_CACHE[key] = entry
            out[key] = entry
            # Map requested keys that match Jira key case-insensitively (same project).
            for req in chunk:
                if req.upper() == key.upper():
                    _JIRA_META_CACHE[req] = entry
                    out[req] = entry
    return out


_JIRA_SUMMARY_CACHE: dict[str, str] = {}


def _fetch_jira_issue_summaries(
    keys: list[str],
    *,
    base_url: str,
    auth_headers: dict[str, str] | None,
) -> dict[str, str]:
    """Return issue key -> summary (best-effort; cached)."""
    out: dict[str, str] = {}
    eff = _jira_bug_metadata_auth_headers(auth_headers) or auth_headers
    if not keys or not base_url or not eff:
        for k in keys:
            ck = str(k or "").strip()
            if ck and ck in _JIRA_SUMMARY_CACHE:
                out[ck] = _JIRA_SUMMARY_CACHE[ck]
        return out

    pending: list[str] = []
    seen_upper: set[str] = set()
    for k in keys:
        ck = str(k or "").strip()
        if not ck:
            continue
        if ck in _JIRA_SUMMARY_CACHE:
            out[ck] = _JIRA_SUMMARY_CACHE[ck]
            continue
        up = ck.upper()
        if up in seen_upper:
            continue
        seen_upper.add(up)
        pending.append(ck)

    chunk_size = 50
    for start in range(0, len(pending), chunk_size):
        chunk = pending[start : start + chunk_size]
        jql = "key in (" + ",".join(chunk) + ")"
        issues: list[dict[str, Any]] | None = None
        for endpoint, call_kw in (
            (
                "/rest/api/2/search",
                {
                    "params": {
                        "jql": jql,
                        "fields": "summary",
                        "maxResults": str(len(chunk)),
                    },
                },
            ),
            (
                "/rest/api/3/search",
                {
                    "method": "POST",
                    "body": {
                        "jql": jql,
                        "fields": ["summary"],
                        "maxResults": len(chunk),
                    },
                },
            ),
        ):
            try:
                payload = request_json(base_url, endpoint, eff, **call_kw)
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(payload, dict):
                continue
            raw_issues = payload.get("issues")
            if isinstance(raw_issues, list):
                issues = [i for i in raw_issues if isinstance(i, dict)]
                break
        if not issues:
            continue
        for issue in issues:
            key = str(issue.get("key") or "").strip()
            if not key:
                continue
            fields = issue.get("fields") or {}
            summary = ""
            if isinstance(fields, dict):
                summary = str(fields.get("summary") or "").strip()
            _JIRA_SUMMARY_CACHE[key] = summary
            out[key] = summary
            for req in chunk:
                if req.upper() == key.upper():
                    _JIRA_SUMMARY_CACHE[req] = summary
                    out[req] = summary
    return out


def _read_cycle_objective(cycle: dict[str, Any]) -> str:
    # Use only cycle-level fields; do not reuse case objective as cycle criterion.
    return _read_cycle_field(
        cycle,
        [
            "objective",
            "iteration.objective",
            "testRun.objective",
            "description",
            "iteration.description",
            "testRun.description",
        ],
        "",
    )


def _plain_from_html_like(text: str) -> str:
    """Convert stored HTML-ish text to readable plain text with line breaks."""
    if not text:
        return ""
    with_breaks = re.sub(r"(?i)<br\s*/?>", "\n", text)
    no_tags = re.sub(r"<[^>]+>", "", with_breaks)
    return html.unescape(no_tags).strip()


def _status_bucket_css_class(status_raw: str | None) -> str:
    raw = (status_raw or "").strip()
    raw_lower = raw.lower()
    exact = {
        "can't test": "st-cant-test",
        "cant test": "st-cant-test",
        "not tested in this pi": "st-not-tested-pi",
        "danger": "st-danger",
        "can't reproduce": "st-cant-reproduce",
        "cant reproduce": "st-cant-reproduce",
        "false positive": "st-false-positive",
    }
    if raw_lower in exact:
        return exact[raw_lower]
    bucket = normalize_status(status_raw)
    if bucket == "passed":
        return "st-pass"
    if bucket == "failed":
        return "st-fail"
    if bucket == "blocked":
        return "st-blocked"
    if bucket == "not_executed":
        if "progress" in raw_lower or raw_lower in {"wip", "in progress", "in_progress"}:
            return "st-in-progress"
        return "st-not-executed"
    return "st-unknown"


def _wiki_status_color_hex(status_raw: str | None) -> str:
    cls = _status_bucket_css_class(status_raw)
    return {
        "st-pass": "#33c24d",
        "st-fail": "#e53935",
        "st-not-executed": "#6c757d",
        "st-in-progress": "#f0ad4e",
        "st-blocked": "#4a90e2",
        "st-cant-test": "#9c27ff",
        "st-not-tested-pi": "#8d7cc3",
        "st-danger": "#4f6078",
        "st-cant-reproduce": "#f08f78",
        "st-false-positive": "#ecd96b",
        "st-unknown": "#adb5bd",
    }.get(cls, "#adb5bd")


def _wiki_status_macro_color(status_raw: str | None) -> str:
    cls = _status_bucket_css_class(status_raw)
    return {
        "st-pass": "Green",
        "st-fail": "Red",
        "st-blocked": "Blue",
        "st-not-executed": "Grey",
        "st-in-progress": "Yellow",
        "st-cant-test": "Purple",
        "st-not-tested-pi": "Purple",
        "st-danger": "Grey",
        "st-cant-reproduce": "Yellow",
        "st-false-positive": "Yellow",
    }.get(cls, "Grey")


def _status_text_color_hex(status_raw: str | None) -> str:
    cls = _status_bucket_css_class(status_raw)
    return {
        "st-not-executed": "#2f2f2f",
        "st-in-progress": "#2f2f2f",
        "st-cant-reproduce": "#2f2f2f",
        "st-false-positive": "#2f2f2f",
        "st-unknown": "#172b4d",
    }.get(cls, "#ffffff")


def _render_report_date(case: dict[str, Any]) -> str:
    raw = _resolve_case_display_date(case)
    day = _parse_display_date(raw)
    if day is not None:
        return day.strftime("%d.%m.%Y")
    return str(raw or "").strip()


def _wiki_status_markup(status_raw: str | None) -> str:
    raw_text = str(status_raw or "").strip()
    if not raw_text:
        return ""
    macro_color = _wiki_status_macro_color(status_raw)
    # Keep title minimally sanitized so status macro is parsed,
    # while avoiding wiki escaping that can break macro rendering.
    macro_title = raw_text.replace("|", "/").replace("}", ")")
    # Confluence status macro renders a rounded colored badge with bold text.
    return f"{{status:colour={macro_color}|title={macro_title}}}"


def _status_badge_html(status: str) -> str:
    raw = status or ""
    cls = _status_bucket_css_class(raw)
    bg = _wiki_status_color_hex(raw)
    fg = _status_text_color_hex(raw)
    return (
        f"<span class='status-badge {cls}' "
        "style='display:inline-block;"
        "padding:2px 8px;"
        "border-radius:10px;"
        "font-size:12px;"
        "font-weight:700;"
        "line-height:1.35;"
        f"background:{bg};color:{fg};'>"
        f"{html.escape(raw)}</span>"
    )


def _render_cycle_info_html(cycle: dict[str, Any]) -> str:
    cycle_key_value = str(cycle.get("cycle_key") or "")
    cycle_objective = _plain_from_html_like(str(cycle.get("cycle_objective") or ""))
    cycle_link_html = html.escape(cycle_key_value)
    if cycle_key_value:
        cycle_url = _jira_cycle_url(cycle_key_value)
        cycle_link_html = (
            f"<a href='{html.escape(cycle_url, quote=True)}' target='_blank' rel='noopener'>"
            f"{html.escape(cycle_key_value)}</a>"
        )
    parts = [f"<div><strong>Прогон:</strong> {cycle_link_html or '-'}</div>"]
    if cycle_objective:
        parts.append(
            f"<div style='margin-top:6px;'><strong>Критерий:</strong><br/>{_html_comment_cell(cycle_objective)}</div>"
        )
    return "".join(parts)


def _render_cycle_info_wiki(cycle: dict[str, Any]) -> str:
    cycle_key_value = str(cycle.get("cycle_key") or "")
    cycle_objective = _plain_from_html_like(str(cycle.get("cycle_objective") or ""))
    cycle_ref = _wiki_escape(cycle_key_value)
    if cycle_key_value:
        cycle_url = _jira_cycle_url(cycle_key_value)
        cycle_ref = f"[{cycle_key_value}|{cycle_url}]"
    if cycle_objective:
        return f"Прогон: {cycle_ref}\\\\Критерий: {_wiki_escape(cycle_objective)}"
    return f"Прогон: {cycle_ref}"


def _normalize_criterion_key(text: str) -> str:
    plain = _plain_from_html_like(text or "")
    collapsed = re.sub(r"\s+", " ", plain).strip().lower()
    return collapsed


def _prepare_cycle_cases_with_groups(cycle: dict[str, Any]) -> tuple[list[dict[str, Any]], list[int]]:
    cases = list(cycle["cases"].values())
    prepared: list[dict[str, Any]] = []
    for case in cases:
        criterion_display = _plain_from_html_like(str(case.get("objective", "")))
        prepared.append(
            {
                **case,
                "_criterion_display": criterion_display,
                "_criterion_key": _normalize_criterion_key(criterion_display),
            }
        )
    prepared.sort(
        key=lambda item: (
            item["_criterion_key"],
            item.get("test_case_name", ""),
            item.get("test_case_key", ""),
        )
    )
    spans: list[int] = [0] * len(prepared)
    i = 0
    while i < len(prepared):
        j = i + 1
        while j < len(prepared) and prepared[j]["_criterion_key"] == prepared[i]["_criterion_key"]:
            j += 1
        spans[i] = j - i
        i = j
    return prepared, spans


def _cycle_sort_key(cycle: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(cycle.get("cycle_key") or ""),
        str(cycle.get("cycle_name") or ""),
        str(cycle.get("cycle_id") or ""),
    )


_GROUP_TITLE_STOPWORDS = {
    "и",
    "или",
    "а",
    "но",
    "в",
    "во",
    "на",
    "по",
    "с",
    "со",
    "без",
    "для",
    "из",
    "под",
    "над",
    "к",
    "ко",
    "у",
    "о",
    "об",
    "обо",
    "при",
    "от",
    "до",
    "слева",
    "справа",
    "слева.",
    "справа.",
}


def _clean_cycle_name_for_grouping(name: str) -> str:
    plain = _plain_from_html_like(name or "")
    no_index = re.sub(r"(?<!\d)\d+\.\d+(?!\d)", " ", plain)
    no_iteration = re.sub(r"(?i)\bитерац(?:ия|ии)\s*\d+\b", " ", no_index)
    no_brackets = re.sub(r"[()\\[\\]{}:;,.!?\"']", " ", no_iteration)
    return re.sub(r"\s+", " ", no_brackets).strip()


def _title_tokens(cleaned_name: str) -> list[str]:
    return re.findall(r"[A-Za-zА-Яа-яЁё0-9-]+", cleaned_name)


def _common_prefix_tokens(token_rows: list[list[str]]) -> list[str]:
    if not token_rows:
        return []
    prefix = list(token_rows[0])
    for row in token_rows[1:]:
        limit = min(len(prefix), len(row))
        i = 0
        while i < limit and prefix[i].lower() == row[i].lower():
            i += 1
        prefix = prefix[:i]
        if not prefix:
            break
    return prefix


def _prettify_group_title(title: str) -> str:
    cleaned = re.sub(r"(?i)\bиз\s+под\b", "из-под", title or "")
    return re.sub(r"\s+", " ", cleaned).strip()


def _extract_meaningful_tokens(name: str) -> list[tuple[str, str]]:
    cleaned = _clean_cycle_name_for_grouping(name)
    raw_tokens = _title_tokens(cleaned)
    meaningful: list[tuple[str, str]] = []
    for token in raw_tokens:
        lowered = token.lower()
        if lowered in _GROUP_TITLE_STOPWORDS:
            continue
        normalized = lowered
        if normalized in _GROUP_TITLE_STOPWORDS:
            continue
        if len(normalized) < 2:
            continue
        meaningful.append((token, normalized))
    return meaningful


def _build_group_title(cycles: list[dict[str, Any]]) -> str:
    if not cycles:
        return "Тестовые циклы"

    cleaned_titles = [
        _clean_cycle_name_for_grouping(str(cycle.get("cycle_name") or cycle.get("cycle_key") or ""))
        for cycle in cycles
    ]
    token_rows = [_title_tokens(title) for title in cleaned_titles if title]
    prefix_tokens = _common_prefix_tokens(token_rows)
    while prefix_tokens and prefix_tokens[-1].lower() in _GROUP_TITLE_STOPWORDS:
        prefix_tokens.pop()
    if prefix_tokens:
        return _prettify_group_title(" ".join(prefix_tokens))

    token_sets: list[set[str]] = []
    first_tokens = _extract_meaningful_tokens(str(cycles[0].get("cycle_name") or ""))
    if not first_tokens:
        fallback = cleaned_titles[0] if cleaned_titles else ""
        return fallback or str(cycles[0].get("cycle_name") or cycles[0].get("cycle_key") or "Тестовые циклы")
    token_sets.append({norm for _, norm in first_tokens})
    for cycle in cycles[1:]:
        tokens = _extract_meaningful_tokens(str(cycle.get("cycle_name") or ""))
        token_sets.append({norm for _, norm in tokens})

    common_tokens = set.intersection(*token_sets) if token_sets else set()
    if not common_tokens:
        fallback = cleaned_titles[0] if cleaned_titles else ""
        return fallback or str(cycles[0].get("cycle_name") or cycles[0].get("cycle_key") or "Тестовые циклы")

    ordered_common: list[str] = []
    seen: set[str] = set()
    for original, normalized in first_tokens:
        if normalized in common_tokens and normalized not in seen:
            ordered_common.append(original)
            seen.add(normalized)

    if not ordered_common:
        fallback = cleaned_titles[0] if cleaned_titles else ""
        return fallback or str(cycles[0].get("cycle_name") or cycles[0].get("cycle_key") or "Тестовые циклы")

    return _prettify_group_title(" ".join(ordered_common))


def _build_summary_cycle_label(row: dict[str, Any]) -> str:
    cycle_index = str(row.get("cycle_index") or "").strip()
    cycle_title = str(row.get("cycle_title") or "").strip()
    if cycle_index and cycle_title:
        if re.match(rf"^{re.escape(cycle_index)}\b", cycle_title):
            return _prettify_group_title(cycle_title)
        return _prettify_group_title(f"{cycle_index} {cycle_title}")
    label = cycle_index or str(row.get("cycle_key") or "").strip() or cycle_title
    return _prettify_group_title(label)


def _summary_scenario_group(row: dict[str, Any]) -> str:
    cycle_index = str(row.get("cycle_index") or "").strip()
    match = re.match(r"^(\d+)\.\d+$", cycle_index)
    if match:
        return match.group(1)
    return ""


def _summary_group_title_from_labels(labels: list[str], fallback_group: str) -> str:
    meaningful_rows: list[list[tuple[str, str]]] = []
    for label in labels:
        cleaned = re.sub(r"^\s*\d+\.\d+\s*", "", str(label or "")).strip()
        tokens = _extract_meaningful_tokens(cleaned)
        if tokens:
            meaningful_rows.append(tokens)
    if not meaningful_rows:
        return fallback_group
    common_tokens = set(norm for _, norm in meaningful_rows[0])
    for token_row in meaningful_rows[1:]:
        common_tokens &= {norm for _, norm in token_row}
    if not common_tokens:
        first_clean = re.sub(r"^\s*\d+\.\d+\s*", "", str(labels[0] if labels else "")).strip()
        return first_clean or fallback_group
    ordered: list[str] = []
    used: set[str] = set()
    for original, normalized in meaningful_rows[0]:
        if normalized in common_tokens and normalized not in used:
            ordered.append(original)
            used.add(normalized)
    if not ordered:
        first_clean = re.sub(r"^\s*\d+\.\d+\s*", "", str(labels[0] if labels else "")).strip()
        raw_title = first_clean or fallback_group
    else:
        raw_title = _prettify_group_title(" ".join(ordered))
    # Human-friendly fixes for shortened Russian phrases in grouped totals.
    friendly = raw_title
    friendly = re.sub(r"(?i)\bчастично\s+полосе\b", "частично в полосе", friendly)
    friendly = re.sub(r"(?i)\bзоне\s+видимости\b", "в зоне видимости", friendly)
    return friendly


def _summary_sort_key(row: dict[str, Any]) -> tuple[int, int | str, int | str, str, str]:
    cycle_index = str(row.get("cycle_index") or "").strip()
    match = re.match(r"^(\d+)\.(\d+)$", cycle_index)
    if match:
        return (
            0,
            int(match.group(1)),
            int(match.group(2)),
            str(row.get("cycle_title") or ""),
            str(row.get("cycle_key") or ""),
        )
    return (
        1,
        str(row.get("cycle_key") or ""),
        str(row.get("cycle_title") or ""),
        str(row.get("cycle_index") or ""),
        "",
    )


def _extract_cycle_index(cycle: dict[str, Any]) -> str:
    """
    Extract dotted index like '1.1' from cycle name/key.
    Prefer cycle_name because business naming can contain the required index.
    """
    candidates = [
        str(cycle.get("cycle_name") or "").strip(),
        str(cycle.get("cycle_key") or "").strip(),
        str(cycle.get("cycle_id") or "").strip(),
    ]
    for value in candidates:
        if not value:
            continue
        match = re.search(r"(?<!\d)(\d+\.\d+)(?!\d)", value)
        if match:
            return match.group(1)
    return ""


def _parse_cycle_group_id(cycle: dict[str, Any]) -> str:
    cycle_index = _extract_cycle_index(cycle)
    match = re.match(r"^(\d+)\.\d+$", cycle_index)
    if match:
        return match.group(1)
    cycle_name = str(cycle.get("cycle_name") or "").strip()
    cycle_key = str(cycle.get("cycle_key") or "").strip()
    return cycle_name or cycle_key or str(cycle.get("cycle_id") or "")


def _group_cycles_by_prefix(cycles: dict[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for cycle in cycles.values():
        group_id = _parse_cycle_group_id(cycle)
        group_bucket = grouped.setdefault(group_id, {"group_id": group_id, "cycles": []})
        group_bucket["cycles"].append(cycle)

    def group_sort_key(item: dict[str, Any]) -> tuple[int, int | str]:
        group_id = str(item.get("group_id") or "")
        if group_id.isdigit():
            return (0, int(group_id))
        return (1, group_id.lower())

    result: list[dict[str, Any]] = []
    for group in grouped.values():
        group_cycles = sorted(group["cycles"], key=_cycle_sort_key)
        result.append(
            {
                "group_id": group["group_id"],
                "group_title": _build_group_title(group_cycles),
                "cycles": group_cycles,
            }
        )
    result.sort(key=group_sort_key)
    return result


def _build_cycle_progress_rows(cycles: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cycle in sorted(cycles.values(), key=_cycle_sort_key):
        cycle_title = cycle.get("cycle_name") or cycle.get("cycle_key") or cycle.get("cycle_id") or "Unnamed cycle"
        cycle_index = _extract_cycle_index(cycle)
        cycle_key = str(cycle.get("cycle_key") or "")
        total_cases = len(cycle.get("cases", {}))
        passed_cases = 0
        not_executed_cases = 0
        blocked_cases = 0
        for case in cycle.get("cases", {}).values():
            normalized = normalize_status(case.get("result", case.get("test_case_status", "")))
            if normalized == "passed":
                passed_cases += 1
            elif normalized == "not_executed":
                not_executed_cases += 1
            elif normalized == "blocked":
                blocked_cases += 1
        all_not_executed = total_cases > 0 and not_executed_cases == total_cases
        all_blocked = total_cases > 0 and blocked_cases == total_cases
        rows.append(
            {
                "cycle_title": str(cycle_title),
                "cycle_index": cycle_index,
                "cycle_key": cycle_key,
                "total_cases": total_cases,
                "passed_cases": passed_cases,
                "not_executed_cases": not_executed_cases,
                "blocked_cases": blocked_cases,
                "all_not_executed": all_not_executed,
                "all_blocked": all_blocked,
            }
        )
    return rows


def _passed_count_color(
    passed_cases: int, *, all_not_executed: bool = False, all_blocked: bool = False
) -> str:
    if passed_cases <= 0:
        if all_blocked:
            return "#4a90e2"
        if all_not_executed:
            return "#c9c9c2"
        return "#ff9074"
    if passed_cases == 1:
        return "#ffc402"
    if passed_cases == 2:
        return "#37b37e"
    return "#01875b"


def _passed_count_text_color(
    passed_cases: int, *, all_not_executed: bool = False, all_blocked: bool = False
) -> str:
    if passed_cases <= 0 and all_blocked:
        return "#ffffff"
    if passed_cases <= 0 and all_not_executed:
        return "#2f2f2f"
    return "#2f2f2f" if passed_cases <= 1 else "#ffffff"


def _cycle_progress_csv_rows(
    report_data: dict[tuple[str, str], dict[str, Any]],
) -> list[list[str]]:
    rows: list[list[str]] = []
    for (folder_id, folder_name), payload in sorted(report_data.items(), key=lambda item: item[0][1]):
        cycles = payload.get("cycles", {})
        for cycle in sorted(cycles.values(), key=_cycle_sort_key):
            cycle_key = str(cycle.get("cycle_key") or "")
            cycle_name = str(cycle.get("cycle_name") or "")
            total_cases = len(cycle.get("cases", {}))
            passed_cases = 0
            for case in cycle.get("cases", {}).values():
                normalized = normalize_status(case.get("result", case.get("test_case_status", "")))
                if normalized == "passed":
                    passed_cases += 1
            rows.append(
                [
                    str(folder_id),
                    str(folder_name),
                    cycle_key,
                    cycle_name,
                    str(total_cases),
                    str(passed_cases),
                ]
            )
    return rows


def _write_text_if_changed(path: str, text: str) -> bool:
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as f:
            if f.read() == text:
                return False
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return True


def _write_text_always(path: str, text: str) -> None:
    """Write ``text`` to ``path``, creating parent dirs; always overwrites."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def write_cycle_progress_csv(path: str, rows: list[list[str]]) -> bool:
    header = [
        "folder_id",
        "folder_name",
        "cycle_key",
        "cycle_name",
        "total_cases",
        "passed_cases",
    ]
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return _write_text_if_changed(path, buffer.getvalue())


def _weekly_cycle_display_label(cycle: dict[str, Any]) -> str:
    cycle_name = str(cycle.get("cycle_name") or "").strip()
    cycle_key = str(cycle.get("cycle_key") or "").strip()
    return cycle_name or cycle_key or str(cycle.get("cycle_id") or "Unnamed cycle")


def _weekly_cycle_sort_key_from_cycle(cycle: dict[str, Any]) -> tuple[int, int | str, int | str, str, str]:
    cycle_name = str(cycle.get("cycle_name") or "").strip()
    summary_row = {
        "cycle_index": _extract_cycle_index(cycle),
        "cycle_title": cycle_name,
        "cycle_key": str(cycle.get("cycle_key") or "").strip(),
    }
    return _summary_sort_key(summary_row)


def _default_weekday_labels() -> list[str]:
    return ["База", "Вт", "Ср", "Чт", "Пт"]


def _test_day_from_folder_day(folder_day: date) -> date:
    # Folder name encodes the day a branch was fixed; the actual test run
    # happens on the next business day. Branches fixed on Fri/Sat/Sun are
    # all tested on the following Monday.
    weekday = folder_day.weekday()
    if weekday <= 3:  # Mon..Thu -> next day
        return folder_day + timedelta(days=1)
    return folder_day + timedelta(days=(7 - weekday))  # Fri/Sat/Sun -> next Mon


def _release_week_start(day: date) -> date:
    # ISO week start (Monday). Caller is expected to pass a "test day"
    # (see _test_day_from_folder_day) so a build fixed on Fri groups with
    # the following Mon..Sun week instead of staying with the previous one.
    return day - timedelta(days=day.weekday())


def _release_week_end(start: date) -> date:
    return start + timedelta(days=7)


def _parse_report_day_from_folder_name(folder_name: str) -> date | None:
    raw_name = str(folder_name or "").strip()
    if not raw_name:
        return None
    dotted_matches = re.findall(r"\b(\d{2}\.\d{2}\.\d{4})\b", raw_name)
    if dotted_matches:
        # New folder naming: the report day is the right-most dd.mm.yyyy date.
        try:
            return datetime.strptime(dotted_matches[-1], "%d.%m.%Y").date()
        except ValueError:
            pass
    for pattern in ("%Y.%m.%d", "%Y-%m-%d", "%Y_%m_%d"):
        try:
            return datetime.strptime(raw_name, pattern).date()
        except ValueError:
            continue
    return None


def _parse_weekly_column_label_from_folder_name(folder_name: str) -> str | None:
    raw_name = str(folder_name or "").strip()
    if not raw_name:
        return None
    match = re.match(r"^(nightly-dev-\d{4}[._-]\d{2}[._-]\d{2})\b", raw_name, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1)


def _normalize_display_date(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    try:
        return parse_datetime(value).date().isoformat()
    except ValueError:
        return value


def _resolve_display_date(actual_start_date: str, fallback_date: str) -> str:
    actual = _normalize_display_date(actual_start_date)
    if actual:
        return actual
    return _normalize_display_date(fallback_date)


def _parse_display_date(value: str) -> date | None:
    normalized = _normalize_display_date(value)
    if not normalized:
        return None
    try:
        return date.fromisoformat(normalized)
    except ValueError:
        return None


def _resolve_case_display_date(case: dict[str, Any]) -> str:
    return _resolve_display_date(
        str(case.get("actual_start_date") or ""),
        str(case.get("execution_date", case.get("cycle_updated_on", ""))),
    )


def _resolve_daily_title_date(cycles: dict[str, Any]) -> str:
    date_counter: Counter[str] = Counter()
    for cycle in cycles.values():
        if not isinstance(cycle, dict):
            continue
        for case in cycle.get("cases", {}).values():
            if not isinstance(case, dict):
                continue
            display_date = _resolve_case_display_date(case)
            if display_date:
                date_counter[display_date] += 1
    if not date_counter:
        return ""
    top_count = max(date_counter.values())
    candidates = sorted(value for value, count in date_counter.items() if count == top_count)
    return candidates[0]


def _resolve_folder_dominant_actual_date(cycles: dict[str, Any]) -> date | None:
    date_counter: Counter[date] = Counter()
    for cycle in cycles.values():
        if not isinstance(cycle, dict):
            continue
        for case in cycle.get("cases", {}).values():
            if not isinstance(case, dict):
                continue
            display_day = _parse_display_date(_resolve_case_display_date(case))
            if display_day is None:
                continue
            date_counter[display_day] += 1
    if not date_counter:
        return None
    top_count = max(date_counter.values())
    candidates = sorted(day for day, count in date_counter.items() if count == top_count)
    return candidates[0]


def _resolve_daily_title_day(cycles: dict[str, Any]) -> date | None:
    title_date = _resolve_daily_title_date(cycles)
    if not title_date:
        return None
    return _parse_display_date(title_date)


def _daily_document_title(folder_name: str) -> str:
    return f"[{folder_name}] Отчёт теста ML planner'а на полигонах"


def _normalize_weekly_cycle_label(label: str) -> tuple[str, bool]:
    raw = _prettify_group_title(str(label or "").strip())
    if not raw:
        return "", False
    normalized = re.sub(r"\s*\((?:cloned|клонированный)\)\s*$", "", raw, flags=re.IGNORECASE).strip()
    if not normalized:
        normalized = raw
    is_cloned = normalized != raw
    return normalized, is_cloned


def _resolve_folder_report_day(folder_name: str, cycles: dict[str, Any]) -> date | None:
    parsed_from_folder_name = _parse_report_day_from_folder_name(folder_name)
    if parsed_from_folder_name is not None:
        return parsed_from_folder_name
    return _resolve_folder_dominant_actual_date(cycles)


def _filter_report_data_by_resolved_folder_day(
    report_data: dict[tuple[str, str], dict[str, Any]],
    from_date: date,
    to_date: date,
    extra_report_days: set[date] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    extra = extra_report_days or set()
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for key, payload in report_data.items():
        _folder_id, folder_name = key
        cycles = payload.get("cycles", {})
        if not isinstance(cycles, dict):
            continue
        rd = _resolve_folder_report_day(folder_name, cycles)
        if rd is None:
            out[key] = payload
        elif rd in extra or (from_date <= rd <= to_date):
            out[key] = payload
    return out


def _filter_tree_folders_by_report_day(
    folders: list[FolderNode],
    from_date: date,
    to_date: date,
    extra_report_days: set[date] | None = None,
) -> list[FolderNode]:
    extra = extra_report_days or set()
    out: list[FolderNode] = []
    for folder in folders:
        folder_day = _parse_report_day_from_folder_name(folder.folder_name)
        if folder_day is None:
            # Keep unknown names to avoid accidental data loss.
            out.append(folder)
            continue
        if folder_day in extra:
            out.append(folder)
            continue
        if from_date <= folder_day <= to_date:
            out.append(folder)
    return out


def _drv_branch_token_to_folder_day(token: str) -> date | None:
    """If DRV/Jira branch string is exactly ``YYYY.MM.DD``, return that calendar day."""
    t = str(token or "").strip()
    m = re.fullmatch(r"(\d{4})\.(\d{2})\.(\d{2})", t)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _zephyr_regenerate_last_n_days_from_environment() -> int:
    """``ZEPHYR_REGENERATE_LAST_N_DAYS`` from process env, else last value in repo ``.env``."""
    raw = (os.getenv("ZEPHYR_REGENERATE_LAST_N_DAYS") or "").strip()
    if raw.isdigit():
        n = int(raw)
        return n if n > 0 else 0
    file_val = str(_get_repo_dotenv_parsed().get("ZEPHYR_REGENERATE_LAST_N_DAYS") or "").strip()
    if file_val.isdigit():
        n = int(file_val)
        return n if n > 0 else 0
    return 0


def _drv_cap_extra_folder_days(
    days: set[date],
    from_d: date,
    to_d: date,
) -> set[date]:
    """Keep at most N extra folder-days closest to the rolling window (both sides).

    ``ZEPHYR_DRV_EXTRA_FOLDER_DAYS_MAX`` (default 48, use 0 for unlimited) avoids
    fetching dozens of legacy folders when Jira descriptions contain many dates.
    """
    cap = _parse_int_env("ZEPHYR_DRV_EXTRA_FOLDER_DAYS_MAX", 48, 0, 366)
    if cap <= 0 or len(days) <= cap:
        return set(days)
    scored: list[tuple[int, date]] = []
    for d in days:
        if d < from_d:
            scored.append((from_d.toordinal() - d.toordinal(), d))
        elif d > to_d:
            scored.append((d.toordinal() - to_d.toordinal(), d))
    scored.sort(key=lambda x: (x[0], x[1]))
    return {t[1] for t in scored[:cap]}


def _drv_calendar_days_parse_from_text(text: str) -> set[date]:
    """Collect calendar days from build/branch text (ISO-ish and dd.mm.yyyy).

    Supports ``YYYY.MM.DD``, ``YYYY-MM-DD``, ``YYYY_MM_DD``, and optional one-digit
    month/day (e.g. ``2026.5.7``), plus ``dd.mm.yyyy`` tokens.
    """
    out: set[date] = set()
    s = str(text or "")
    for m in re.finditer(r"\b(\d{4})[._-](\d{1,2})[._-](\d{1,2})\b", s):
        try:
            out.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
        except ValueError:
            continue
    for m in re.finditer(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b", s):
        try:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            out.add(date(y, mo, d))
        except ValueError:
            continue
    return out


def _drv_extra_folder_days_outside_rolling_window(
    from_d: date | None,
    to_d: date | None,
    *,
    jira_base: str,
    jira_headers: dict[str, str] | None,
    issues: list[dict[str, Any]] | None = None,
) -> set[date]:
    """Report-day folder names ``YYYY.MM.DD`` from DRV that fall outside the rolling window.

    Those folders are still fetched so the weekly matrix can show the real column
    for ``Лучшая ветка`` (name + stats both from DRV-named build, not from unrelated columns).
    """
    out: set[date] = set()
    if from_d is None or to_d is None or not jira_base or not jira_headers:
        return out
    if not _parse_bool_env(os.getenv("ZEPHYR_AUTOFLEET_ABTEST_ENABLED", "true")):
        return out
    eff_issues = (
        issues
        if issues is not None
        else fetch_autofleet_abtest_candidates(base_url=jira_base, auth_headers=jira_headers)
    )
    for item in _build_best_branch_schedule_from_jira(eff_issues):
        for fd in _drv_calendar_days_parse_from_text(str(item.get("branch") or "")):
            if fd < from_d or fd > to_d:
                out.add(fd)
    for issue in eff_issues:
        if not isinstance(issue, dict):
            continue
        fields = issue.get("fields") or {}
        if not isinstance(fields, dict):
            fields = {}
        # Only summary branch token + point-A build line — not full ADF description
        # (descriptions often contain many unrelated dates and would pull in every legacy folder).
        branch_snip = _extract_branch_from_summary(fields.get("summary")) or ""
        point_a = extract_build_from_description_point_a(fields.get("description")) or ""
        blob = "\n".join(p for p in (branch_snip, point_a) if p)
        for fd in _drv_calendar_days_parse_from_text(blob):
            if fd < from_d or fd > to_d:
                out.add(fd)
    build = str(
        fetch_autofleet_abtest_build_name(
            base_url=jira_base,
            auth_headers=jira_headers,
            issues=eff_issues,
        )
        or ""
    ).strip()
    for fd in _drv_calendar_days_parse_from_text(build):
        if fd < from_d or fd > to_d:
            out.add(fd)
    if not eff_issues:
        print(
            "DRV: warning: 0 Jira issues from AB/DRV JQL — "
            "check ZEPHYR_JIRA_BASE_URL and auth (ZEPHYR_JIRA_API_TOKEN if Jira rejects the Zephyr token)."
        )
    elif not out:
        print(
            "DRV: no calendar days outside the rolling window parsed from "
            "schedule / summary+point-A / latest build — extra folder fetch may be empty; "
            "best-branch column still uses name from Jira when schedule resolves."
        )
    else:
        print(
            f"DRV: {len(out)} calendar day(s) outside {from_d}..{to_d} before cap "
            f"({len(eff_issues)} Jira issue(s))."
        )
    capped = _drv_cap_extra_folder_days(out, from_d, to_d)
    if len(capped) < len(out):
        print(
            "DRV: capped extra folder day(s) outside rolling window "
            f"from {len(out)} to {len(capped)} "
            f"(ZEPHYR_DRV_EXTRA_FOLDER_DAYS_MAX; use 0 for no cap)."
        )
    return capped


_DEFECT_KEY_PATTERN = re.compile(r"\b[A-Z][A-Z0-9]+-\d+\b")
_FAILED_STATUS_TOKENS = {
    "fail",
    "failed",
    "не пройден",
    "не пройдено",
    "не пройдён",
    "blocked",
    "заблокирован",
    "заблокирована",
    "заблокировано",
}
_PASSED_STATUS_TOKENS = {"pass", "passed", "пройден", "пройдено", "пройдена", "ok"}


def _is_failed_execution_status(status: str | None) -> bool:
    text = str(status or "").strip().lower()
    return bool(text) and text in _FAILED_STATUS_TOKENS


def _extract_defect_keys_from_cycles(cycles: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    if not isinstance(cycles, dict):
        return keys
    for cycle in cycles.values():
        if not isinstance(cycle, dict):
            continue
        for case in cycle.get("cases", {}).values():
            if not isinstance(case, dict):
                continue
            tasks_raw = str(case.get("tasks") or "")
            if not tasks_raw:
                continue
            keys.extend(_DEFECT_KEY_PATTERN.findall(tasks_raw))
    return keys


def _iter_cases_in_cycles(cycles: dict[str, Any]):
    if not isinstance(cycles, dict):
        return
    for cycle in cycles.values():
        if not isinstance(cycle, dict):
            continue
        for case in cycle.get("cases", {}).values():
            if isinstance(case, dict):
                yield case


def _empty_defect_analytics() -> dict[str, Any]:
    return {
        "keys_ordered": [],
        "matrix": {},
        "totals_by_build": {},
        "cases_with_bug_by_build": {},
        "cases_without_bug_by_build": {},
        "failed_cases_with_bug_by_build": {},
        "failed_cases_without_bug_by_build": {},
        "bug_total_cases": {},
        "bug_builds_count": {},
        "hot_bugs": [],
    }


def _coalesce_weekly_defect_analytics(
    defect_analytics: dict[str, Any] | None,
    defect_keys: list[str] | None,
) -> dict[str, Any] | None:
    """Prefer ``defect_analytics`` keys; if empty but ``defect_keys`` has Jira keys, use a minimal analytics shell.

    ``_weekly_cycle_matrix_data`` can expose keys in ``defect_keys_ordered`` while
    ``keys_ordered`` in analytics is empty in edge cases; without this, the report
    falls back to the legacy bullet list and skips the full bugs table.
    """
    analytics = defect_analytics
    raw_keys = list((analytics or {}).get("keys_ordered") or [])
    if raw_keys:
        return analytics
    keys_from_list = [str(k).strip() for k in (defect_keys or []) if str(k).strip()]
    if not keys_from_list:
        return analytics
    merged = _empty_defect_analytics()
    merged["keys_ordered"] = keys_from_list
    return merged


def _compute_weekly_defect_analytics(
    cycles_by_day: dict[date, list[dict[str, Any]]],
    ordered_days: list[date],
    column_labels: list[str],
) -> dict[str, Any]:
    """Aggregate per-bug analytics across the week.

    Returns a dict consumable by the renderers. Buckets:
      keys_ordered, matrix, totals_by_build,
      cases_with_bug_by_build / cases_without_bug_by_build,
      failed_cases_with_bug_by_build / failed_cases_without_bug_by_build,
      bug_total_cases, bug_builds_count, hot_bugs.
    """
    matrix: dict[str, dict[str, int]] = {}
    cases_with_bug_by_build: dict[str, int] = {label: 0 for label in column_labels}
    cases_without_bug_by_build: dict[str, int] = {label: 0 for label in column_labels}
    failed_with_bug_by_build: dict[str, int] = {label: 0 for label in column_labels}
    failed_without_bug_by_build: dict[str, int] = {label: 0 for label in column_labels}
    bug_total_cases: dict[str, int] = {}
    bug_appears_in_label: dict[str, set[str]] = {}
    keys_first_seen_order: list[str] = []

    label_by_day = dict(zip(ordered_days, column_labels))
    for day, build_label in label_by_day.items():
        for cycles_dict in cycles_by_day.get(day, []):
            for case in _iter_cases_in_cycles(cycles_dict):
                tasks_raw = str(case.get("tasks") or "")
                bug_keys = _DEFECT_KEY_PATTERN.findall(tasks_raw) if tasks_raw else []
                seen_per_case: set[str] = set()
                is_failed = _is_failed_execution_status(case.get("executionStatus"))
                if bug_keys:
                    cases_with_bug_by_build[build_label] = (
                        cases_with_bug_by_build.get(build_label, 0) + 1
                    )
                    if is_failed:
                        failed_with_bug_by_build[build_label] = (
                            failed_with_bug_by_build.get(build_label, 0) + 1
                        )
                else:
                    cases_without_bug_by_build[build_label] = (
                        cases_without_bug_by_build.get(build_label, 0) + 1
                    )
                    if is_failed:
                        failed_without_bug_by_build[build_label] = (
                            failed_without_bug_by_build.get(build_label, 0) + 1
                        )
                for raw_key in bug_keys:
                    key = raw_key.strip()
                    if not key or key in seen_per_case:
                        continue
                    seen_per_case.add(key)
                    if key not in matrix:
                        matrix[key] = {label: 0 for label in column_labels}
                        keys_first_seen_order.append(key)
                        bug_total_cases[key] = 0
                        bug_appears_in_label[key] = set()
                    matrix[key][build_label] = matrix[key].get(build_label, 0) + 1
                    bug_total_cases[key] += 1
                    bug_appears_in_label[key].add(build_label)

    bug_builds_count = {key: len(labels) for key, labels in bug_appears_in_label.items()}
    totals_by_build = {
        label: sum(1 for key in matrix if matrix[key].get(label, 0) > 0)
        for label in column_labels
    }

    label_index = {label: idx for idx, label in enumerate(column_labels)}
    hot_bugs: list[str] = []
    for key in keys_first_seen_order:
        present_indexes = sorted(
            label_index[label]
            for label in bug_appears_in_label.get(key, set())
            if label in label_index
        )
        if len(present_indexes) < 2:
            continue
        for a, b in zip(present_indexes, present_indexes[1:]):
            if b - a == 1:
                hot_bugs.append(key)
                break

    keys_ordered = sorted(
        keys_first_seen_order,
        key=lambda k: (
            -bug_total_cases.get(k, 0),
            -bug_builds_count.get(k, 0),
            k,
        ),
    )

    return {
        "keys_ordered": keys_ordered,
        "matrix": matrix,
        "totals_by_build": totals_by_build,
        "cases_with_bug_by_build": cases_with_bug_by_build,
        "cases_without_bug_by_build": cases_without_bug_by_build,
        "failed_cases_with_bug_by_build": failed_with_bug_by_build,
        "failed_cases_without_bug_by_build": failed_without_bug_by_build,
        "bug_total_cases": bug_total_cases,
        "bug_builds_count": bug_builds_count,
        "hot_bugs": hot_bugs,
    }


def _extract_cycle_key_objects(cycles: dict[str, Any]) -> list[dict[str, str]]:
    """Return [{id: cycle_key, name?: cycle_name}] for every Zephyr cycle.

    Mirrors what daily reports embed as <div id="zephyr-cycle-keys-json">.
    The Confluence publisher uses these to build a per-build Zephyr Reporting
    macro (TEST_RESULTS_SUMMARY_BY_STATUS) instead of the inline SVG pie.
    """
    out: list[dict[str, str]] = []
    if not isinstance(cycles, dict):
        return out
    seen_keys: set[str] = set()
    for cycle in cycles.values():
        if not isinstance(cycle, dict):
            continue
        ck = str(cycle.get("cycle_key") or "").strip()
        if not ck or ck in seen_keys:
            continue
        seen_keys.add(ck)
        cn = str(cycle.get("cycle_name") or "").strip()
        if cn:
            out.append({"id": ck, "name": cn})
        else:
            out.append({"id": ck})
    return out


def _weekly_aggregate_progress_map(
    progress_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Same aggregation as weekly matrix rows: one entry per normalized cycle label."""
    aggregated: dict[str, dict[str, Any]] = {}
    for progress_row in progress_rows:
        label = _build_summary_cycle_label(progress_row)
        normalized_label, is_cloned = _normalize_weekly_cycle_label(label)
        if not normalized_label:
            continue
        cycle_sort_key = _summary_sort_key(progress_row)
        total_cases = int(progress_row.get("total_cases", 0))
        passed_cases = int(progress_row.get("passed_cases", 0))
        all_not_executed = bool(progress_row.get("all_not_executed", False))
        existing = aggregated.get(normalized_label)
        if existing is None:
            aggregated[normalized_label] = {
                "sort_key": cycle_sort_key,
                "total_cases": total_cases,
                "passed_cases": passed_cases,
                "all_not_executed": all_not_executed,
                "all_blocked": bool(progress_row.get("all_blocked", False)),
                "is_cloned": is_cloned,
            }
            continue
        if existing["is_cloned"] and not is_cloned:
            aggregated[normalized_label] = {
                "sort_key": cycle_sort_key,
                "total_cases": total_cases,
                "passed_cases": passed_cases,
                "all_not_executed": all_not_executed,
                "all_blocked": bool(progress_row.get("all_blocked", False)),
                "is_cloned": False,
            }
            continue
        if (not existing["is_cloned"]) and is_cloned:
            continue
        if cycle_sort_key < existing["sort_key"]:
            existing["sort_key"] = cycle_sort_key
        existing["total_cases"] = max(existing["total_cases"], total_cases)
        existing["passed_cases"] = max(existing["passed_cases"], passed_cases)
        existing["all_not_executed"] = existing["all_not_executed"] and all_not_executed
        existing["all_blocked"] = existing["all_blocked"] and bool(
            progress_row.get("all_blocked", False)
        )
    return aggregated


def _weekly_cycle_matrix_data(
    report_data: dict[tuple[str, str], dict[str, Any]]
) -> tuple[
    date | None,
    list[str],
    list[list[str]],
    list[list[bool]],
    list[list[bool]],
    dict[str, dict[str, int]],
    list[str],
    dict[str, list[dict[str, str]]],
    dict[str, Any],
]:
    weekly_groups: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for (folder_id, folder_name), payload in report_data.items():
        cycles = payload.get("cycles", {})
        if not isinstance(cycles, dict):
            continue
        report_day = _resolve_folder_report_day(folder_name, cycles)
        if report_day is None:
            report_day = _resolve_daily_title_day(cycles)
        if report_day is None:
            continue
        progress_rows = _build_cycle_progress_rows(cycles)
        if not progress_rows:
            continue
        test_day = _test_day_from_folder_day(report_day)
        week_start = _release_week_start(test_day)
        weekly_groups[week_start].append(
            {
                "folder_id": str(folder_id),
                "report_day": report_day,
                "test_day": test_day,
                "column_label": _parse_weekly_column_label_from_folder_name(folder_name),
                "progress_rows": progress_rows,
                "cycles": cycles,
            }
        )

    if not weekly_groups:
        return None, [], [], [], [], {}, [], {}, _empty_defect_analytics()

    # Export the latest available week (matches README behavior).
    target_week_start = max(weekly_groups.keys())
    week_daily_summaries = sorted(
        weekly_groups[target_week_start],
        key=lambda item: (item["report_day"], item["folder_id"]),
    )
    progress_rows_by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    column_labels_by_day: dict[date, list[str]] = defaultdict(list)
    cycles_by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for summary in week_daily_summaries:
        progress_rows_by_day[summary["report_day"]].extend(summary["progress_rows"])
        column_label = str(summary.get("column_label") or "").strip()
        if column_label:
            column_labels_by_day[summary["report_day"]].append(column_label)
        day_cycles = summary.get("cycles")
        if isinstance(day_cycles, dict):
            cycles_by_day[summary["report_day"]].append(day_cycles)
    ordered_days = sorted(progress_rows_by_day.keys())
    column_labels: list[str] = []
    for day in ordered_days:
        day_labels = column_labels_by_day.get(day, [])
        if day_labels:
            column_labels.append(Counter(day_labels).most_common(1)[0][0])
        else:
            column_labels.append(f"nightly-dev-{day.strftime('%Y.%m.%d')}")

    # Per-column (per-build) case status counts, aggregated defect Jira keys
    # and per-build Zephyr cycle key objects (for the Confluence publisher).
    column_status_counts: dict[str, dict[str, int]] = {}
    cycle_keys_by_label: dict[str, list[dict[str, str]]] = {}
    defect_keys_seen: set[str] = set()
    defect_keys_ordered: list[str] = []
    for day_date, day_label in zip(ordered_days, column_labels):
        merged_counts: dict[str, int] = defaultdict(int)
        seen_cycle_keys: set[str] = set()
        cycle_objs: list[dict[str, str]] = []
        for cycles_dict in cycles_by_day.get(day_date, []):
            day_counts = _daily_aggregate_case_status_counts(cycles_dict)
            for status_key, value in day_counts.items():
                merged_counts[status_key] += int(value)
            for defect_key in _extract_defect_keys_from_cycles(cycles_dict):
                if defect_key in defect_keys_seen:
                    continue
                defect_keys_seen.add(defect_key)
                defect_keys_ordered.append(defect_key)
            for entry in _extract_cycle_key_objects(cycles_dict):
                cid = str(entry.get("id") or "").strip()
                if not cid or cid in seen_cycle_keys:
                    continue
                seen_cycle_keys.add(cid)
                cycle_objs.append(entry)
        column_status_counts[day_label] = dict(merged_counts)
        cycle_keys_by_label[day_label] = cycle_objs

    defect_analytics = _compute_weekly_defect_analytics(
        cycles_by_day, ordered_days, column_labels
    )

    day_maps: dict[date, dict[str, dict[str, Any]]] = {}
    joined_passed_by_label: dict[str, dict[str, int]] = {}
    joined_totals_by_label: dict[str, dict[str, int]] = {}
    joined_all_not_executed_by_label: dict[str, dict[str, bool]] = {}
    joined_all_blocked_by_label: dict[str, dict[str, bool]] = {}
    for day_date, day_label in zip(ordered_days, column_labels):
        day_map = _weekly_aggregate_progress_map(progress_rows_by_day[day_date])
        day_maps[day_date] = day_map
        joined_passed_by_label[day_label] = {
            cycle_label: int(day_payload["passed_cases"])
            for cycle_label, day_payload in day_map.items()
        }
        joined_totals_by_label[day_label] = {
            cycle_label: int(day_payload["total_cases"])
            for cycle_label, day_payload in day_map.items()
        }
        joined_all_not_executed_by_label[day_label] = {
            cycle_label: bool(day_payload.get("all_not_executed", False))
            for cycle_label, day_payload in day_map.items()
        }
        joined_all_blocked_by_label[day_label] = {
            cycle_label: bool(day_payload.get("all_blocked", False))
            for cycle_label, day_payload in day_map.items()
        }

    all_cycle_labels: set[str] = set()
    for day_map in day_maps.values():
        all_cycle_labels.update(day_map.keys())
    if not all_cycle_labels:
        return (
            target_week_start,
            column_labels,
            [],
            [],
            [],
            column_status_counts,
            defect_keys_ordered,
            cycle_keys_by_label,
            defect_analytics,
        )

    sort_key_by_cycle: dict[str, tuple[Any, ...]] = {}
    for cycle_label in all_cycle_labels:
        sort_candidates: list[tuple[Any, ...]] = []
        for day_map in day_maps.values():
            payload = day_map.get(cycle_label)
            if payload is None:
                continue
            sort_candidates.append(payload["sort_key"])
        if sort_candidates:
            sort_key_by_cycle[cycle_label] = min(sort_candidates)

    rows: list[list[str]] = []
    cell_all_not_executed: list[list[bool]] = []
    cell_all_blocked: list[list[bool]] = []
    for cycle_label in sorted(
        all_cycle_labels,
        key=lambda label: (sort_key_by_cycle.get(label, (9_999_999, label, "", "", "")), label.lower()),
    ):
        total_cases = 0
        for totals_map in joined_totals_by_label.values():
            total_cases = max(total_cases, int(totals_map.get(cycle_label, 0)))
        row = [cycle_label, str(total_cases)]
        ne_flags: list[bool] = []
        blocked_flags: list[bool] = []
        for day_label in column_labels:
            row.append(str(joined_passed_by_label.get(day_label, {}).get(cycle_label, 0)))
            ne_flags.append(bool(joined_all_not_executed_by_label.get(day_label, {}).get(cycle_label, False)))
            blocked_flags.append(bool(joined_all_blocked_by_label.get(day_label, {}).get(cycle_label, False)))
        rows.append(row)
        cell_all_not_executed.append(ne_flags)
        cell_all_blocked.append(blocked_flags)

    # Insert group subtotal row after each scenario group.
    grouped_rows: list[list[str]] = []
    grouped_ne_flags: list[list[bool]] = []
    grouped_blocked_flags: list[list[bool]] = []

    index = 0
    while index < len(rows):
        group_start = index
        row = rows[index]
        group_id = _summary_scenario_group(
            {"cycle_index": _extract_cycle_index({"cycle_name": row[0]}), "cycle_title": row[0], "cycle_key": ""}
        ) or "Прочее"
        group_labels: list[str] = []
        group_total_cases = 0
        group_day_sums = [0] * len(column_labels)
        while index < len(rows):
            current = rows[index]
            current_group_id = _summary_scenario_group(
                {
                    "cycle_index": _extract_cycle_index({"cycle_name": current[0]}),
                    "cycle_title": current[0],
                    "cycle_key": "",
                }
            ) or "Прочее"
            if current_group_id != group_id:
                break
            group_labels.append(str(current[0]))
            group_total_cases += int(current[1]) if len(current) > 1 else 0
            for day_idx in range(len(column_labels)):
                group_day_sums[day_idx] += int(current[2 + day_idx]) if 2 + day_idx < len(current) else 0
            index += 1

        for copy_idx in range(group_start, index):
            grouped_rows.append(rows[copy_idx])
            grouped_ne_flags.append(cell_all_not_executed[copy_idx])
            grouped_blocked_flags.append(cell_all_blocked[copy_idx])

        group_title = _summary_group_title_from_labels(group_labels, fallback_group=group_id)
        subtotal_row = [f"Итого: {group_title}", str(group_total_cases)]
        subtotal_row.extend(str(value) for value in group_day_sums)
        grouped_rows.append(subtotal_row)
        grouped_ne_flags.append([False] * len(column_labels))
        grouped_blocked_flags.append([False] * len(column_labels))

    return (
        target_week_start,
        column_labels,
        grouped_rows,
        grouped_ne_flags,
        grouped_blocked_flags,
        column_status_counts,
        defect_keys_ordered,
        cycle_keys_by_label,
        defect_analytics,
    )


def _weekly_cycle_matrix_rows(report_data: dict[tuple[str, str], dict[str, Any]]) -> list[list[str]]:
    _, _, rows, _, _, _, _, _, _ = _weekly_cycle_matrix_data(report_data)
    return rows


def _split_report_data_by_week(
    report_data: dict[tuple[str, str], dict[str, Any]]
) -> dict[date, dict[tuple[str, str], dict[str, Any]]]:
    grouped: dict[date, dict[tuple[str, str], dict[str, Any]]] = defaultdict(dict)
    for key, payload in report_data.items():
        folder_id, folder_name = key
        cycles = payload.get("cycles", {})
        if not isinstance(cycles, dict):
            continue
        report_day = _resolve_folder_report_day(folder_name, cycles)
        if report_day is None:
            report_day = _resolve_daily_title_day(cycles)
        if report_day is None:
            continue
        progress_rows = _build_cycle_progress_rows(cycles)
        if not progress_rows:
            continue
        test_day = _test_day_from_folder_day(report_day)
        week_start = _release_week_start(test_day)
        grouped[week_start][(folder_id, folder_name)] = payload
    return grouped


def _weekly_cycle_matrix_data_all(
    report_data: dict[tuple[str, str], dict[str, Any]]
) -> list[
    tuple[
        date | None,
        list[str],
        list[list[str]],
        list[list[bool]],
        list[list[bool]],
        dict[str, dict[str, int]],
        list[str],
        dict[str, list[dict[str, str]]],
        dict[str, Any],
    ]
]:
    by_week = _split_report_data_by_week(report_data)
    matrices: list[
        tuple[
            date | None,
            list[str],
            list[list[str]],
            list[list[bool]],
            list[list[bool]],
            dict[str, dict[str, int]],
            list[str],
            dict[str, list[dict[str, str]]],
            dict[str, Any],
        ]
    ] = []
    for week_start in sorted(by_week.keys()):
        matrices.append(_weekly_cycle_matrix_data(by_week[week_start]))
    return matrices


def _weekly_output_path_for_week(base_path: str, week_start: date | None) -> str:
    if week_start is None:
        return base_path
    root, ext = os.path.splitext(base_path)
    return f"{root}_{week_start.isoformat()}{ext}"


def write_weekly_cycle_matrix_csv(path: str, weekday_labels: list[str], rows: list[list[str]]) -> bool:
    header = [
        "Тестовый цикл",
        "Всего кейсов",
    ]
    header.extend(label for label in weekday_labels)
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return _write_text_if_changed(path, buffer.getvalue())


def _weekly_matrix_title_text(week_start: date | None, weekday_labels: list[str]) -> str:
    # Title must stay stable for the whole ISO week, otherwise the
    # Confluence publisher creates a new page every time a new folder lands
    # mid-week (it matches existing pages by exact title). We always show
    # Mon..Sun of the ISO week derived from `week_start`, regardless of how
    # many builds we have data for so far.
    if isinstance(week_start, date):
        title_start = week_start
        title_end = week_start + timedelta(days=6)
        week_no = int(title_start.isocalendar()[1])
        return (
            f"Weekly W_{week_no:02d}. {title_start.strftime('%d.%m.%Y')} - "
            f"{title_end.strftime('%d.%m.%Y')}"
        )

    # Fallback: no week_start available (shouldn't normally happen). Try to
    # derive a date from the first column label so the title is at least
    # informative.
    for label in weekday_labels or []:
        match = re.search(r"\b(\d{4}\.\d{2}\.\d{2})\b", str(label))
        if not match:
            continue
        try:
            folder_day = datetime.strptime(match.group(1), "%Y.%m.%d").date()
        except ValueError:
            continue
        test_day = _test_day_from_folder_day(folder_day)
        iso_monday = _release_week_start(test_day)
        title_start = iso_monday
        title_end = iso_monday + timedelta(days=6)
        week_no = int(title_start.isocalendar()[1])
        return (
            f"Weekly W_{week_no:02d}. {title_start.strftime('%d.%m.%Y')} - "
            f"{title_end.strftime('%d.%m.%Y')}"
        )
    return "Weekly W_??. N/A"


def _weekly_builds_html_list(column_labels: list[str]) -> str:
    items = [
        f"  <li>{html.escape(str(label))}</li>"
        for label in column_labels
        if str(label).strip()
    ]
    if not items:
        return ""
    return "<ul>\n" + "\n".join(items) + "\n</ul>"


def _weekly_builds_wiki_list(column_labels: list[str]) -> str:
    items = [
        f"* {_wiki_escape(str(label))}"
        for label in column_labels
        if str(label).strip()
    ]
    return "\n".join(items)


def _weekly_best_branch_column_title(branch_name: str) -> str:
    return f"Лучшая ветка: {str(branch_name or '').strip()}"


def _jira_issue_effective_datetime(issue: dict[str, Any]) -> datetime | None:
    fields = issue.get("fields") or {}
    if not isinstance(fields, dict):
        return None
    created_dt = _coerce_utc_naive(_to_datetime(fields.get("created")))
    summary_dt = _coerce_utc_naive(parse_date_from_summary(fields.get("summary")))
    min_dt = datetime.min
    effective_dt = max(created_dt or min_dt, summary_dt or min_dt)
    if effective_dt == min_dt:
        return None
    return effective_dt


_SUMMARY_BRANCH_RE = re.compile(
    r"\b(?:nightly-dev-\d{4}[._-]\d{2}[._-]\d{2}|f/[A-Za-z0-9._/-]+)\b",
    flags=re.IGNORECASE,
)


def _extract_branch_from_summary(summary: str | None) -> str:
    text = str(summary or "").strip()
    if not text:
        return ""
    match = _SUMMARY_BRANCH_RE.search(text)
    if not match:
        return ""
    return match.group(0).strip()


def _build_best_branch_schedule_from_jira(
    issues: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build branch change schedule activated from the next week.

    Rule: when branch in Jira summary changes, this branch is used starting
    from the following week.
    """
    prepared: list[tuple[datetime, datetime, dict[str, Any], str]] = []
    min_dt = datetime.min
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        fields = issue.get("fields") or {}
        if not isinstance(fields, dict):
            fields = {}
        branch = _extract_branch_from_summary(fields.get("summary"))
        if not branch:
            branch = extract_build_from_description_point_a(fields.get("description"))
        if not branch:
            continue
        effective_dt = _jira_issue_effective_datetime(issue)
        if effective_dt is None:
            continue
        created_dt = _coerce_utc_naive(_to_datetime(fields.get("created"))) or min_dt
        prepared.append((effective_dt, created_dt, issue, branch))
    prepared.sort(key=lambda item: (item[0], item[1], str(item[2].get("key") or "")))

    schedule: list[dict[str, Any]] = []
    current_branch = ""
    for effective_dt, _created_dt, issue, branch in prepared:
        if branch == current_branch:
            continue
        current_branch = branch
        effective_week = _release_week_start(effective_dt.date())
        activate_week = effective_week + timedelta(days=7)
        schedule.append(
            {
                "branch": branch,
                "issue_key": str(issue.get("key") or "").strip(),
                "effective_date": effective_dt.date(),
                "activate_week_start": activate_week,
            }
        )
    return schedule


def _resolve_branch_for_report_week(
    schedule: list[dict[str, Any]],
    report_week_start: date | None,
) -> dict[str, Any] | None:
    if report_week_start is None:
        return None
    chosen: dict[str, Any] | None = None
    for item in schedule:
        activate_week = item.get("activate_week_start")
        if not isinstance(activate_week, date):
            continue
        if activate_week <= report_week_start:
            chosen = item
        else:
            break
    return chosen


def _weekly_best_branch_name_for_report_week(
    schedule: list[dict[str, Any]],
    report_week_start: date | None,
    *,
    base_url: str,
    auth_headers: dict[str, str],
    abtest_issues: list[dict[str, Any]] | None = None,
) -> str:
    """Branch for 'Лучшая ветка': schedule from Jira AB/DRV tickets, else same Jira fetch as always.

    When rolling date filter leaves only the current ISO week, the schedule may
    have no ``activate_week <= week`` yet; then use ``fetch_autofleet_abtest_build_name``
    (same DRV/JQL parsing as without a short window).
    """
    item = _resolve_branch_for_report_week(schedule, report_week_start)
    name = str((item or {}).get("branch") or "").strip()
    if name:
        return name
    if not _parse_bool_env(os.getenv("ZEPHYR_AUTOFLEET_ABTEST_ENABLED", "true")):
        return ""
    bu = (base_url or "").strip().rstrip("/")
    if not bu or not auth_headers:
        return ""
    return str(
        fetch_autofleet_abtest_build_name(
            base_url=bu, auth_headers=auth_headers, issues=abtest_issues
        )
        or ""
    ).strip()


def _weekly_column_index_matching_branch(
    labels: list[str], branch_name: str
) -> int | None:
    """Pick matrix column index for ``branch_name`` using the same headers as in the report.

    Order: exact header match; same calendar day as ``YYYY.MM.DD`` in branch; branch
    substring of header; header substring of branch (for long Jira tokens).
    """
    raw = str(branch_name or "").strip()
    if not raw:
        return None
    low = raw.lower()
    items: list[tuple[int, str]] = [
        (i, str(lab or "").strip()) for i, lab in enumerate(labels) if str(lab or "").strip()
    ]
    if not items:
        return None
    for i, lab in items:
        if lab.lower() == low:
            return i
    branch_dates = _drv_calendar_days_parse_from_text(raw)
    if branch_dates:
        for i, lab in items:
            if branch_dates & _drv_calendar_days_parse_from_text(lab):
                return i
    branch_day: date | None = None
    bm = re.search(r"\b(\d{4})\.(\d{2})\.(\d{2})\b", raw)
    if bm:
        try:
            branch_day = date(int(bm.group(1)), int(bm.group(2)), int(bm.group(3)))
        except ValueError:
            branch_day = None
    if branch_day is not None and bm is not None:
        token = bm.group(0)
        for i, lab in items:
            if token in lab:
                return i
            for ym in re.finditer(r"\b(\d{4})\.(\d{1,2})\.(\d{1,2})\b", lab):
                try:
                    ld = date(int(ym.group(1)), int(ym.group(2)), int(ym.group(3)))
                except ValueError:
                    continue
                if ld == branch_day:
                    return i
    if len(low) >= 8:
        for i, lab in items:
            if low in lab.lower():
                return i
    for i, lab in items:
        llab = lab.lower()
        if len(llab) >= 8 and llab in low:
            return i
    return None


def _weekly_collect_daily_summaries_from_report_data(
    report_data: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    """One entry per folder payload with progress rows (all ISO weeks, all folders)."""
    out: list[dict[str, Any]] = []
    for (folder_id, folder_name), payload in report_data.items():
        cycles = payload.get("cycles", {})
        if not isinstance(cycles, dict):
            continue
        report_day = _resolve_folder_report_day(folder_name, cycles)
        if report_day is None:
            report_day = _resolve_daily_title_day(cycles)
        if report_day is None:
            continue
        progress_rows = _build_cycle_progress_rows(cycles)
        if not progress_rows:
            continue
        col_raw = _parse_weekly_column_label_from_folder_name(folder_name) or ""
        col = str(col_raw).strip()
        if not col:
            col = f"nightly-dev-{report_day.strftime('%Y.%m.%d')}"
        out.append(
            {
                "folder_id": str(folder_id),
                "folder_name": str(folder_name),
                "report_day": report_day,
                "column_label": str(col).strip(),
                "progress_rows": progress_rows,
                "cycles": cycles,
            }
        )
    return out


def _summary_matches_branch(summary: dict[str, Any], branch_name: str) -> bool:
    raw = str(branch_name or "").strip()
    if not raw:
        return False
    col = str(summary.get("column_label") or "").strip()
    if col and _weekly_column_index_matching_branch([col], raw) is not None:
        return True
    rd = summary.get("report_day")
    if isinstance(rd, date):
        # Same fallback as weekly matrix column headers when folder name has no nightly-dev prefix.
        synthetic = f"nightly-dev-{rd.strftime('%Y.%m.%d')}"
        if _weekly_column_index_matching_branch([synthetic], raw) is not None:
            return True
        if rd in _drv_calendar_days_parse_from_text(raw):
            return True
    fname = str(summary.get("folder_name") or "").strip()
    parsed = (_parse_weekly_column_label_from_folder_name(fname) or "").strip()
    if parsed and _weekly_column_index_matching_branch([parsed], raw) is not None:
        return True
    if fname and _weekly_column_index_matching_branch([fname], raw) is not None:
        return True
    return False


def _weekly_day_map_passed_cases(
    day_map: dict[str, dict[str, Any]], row_label: str
) -> int:
    pl = day_map.get(row_label)
    if pl is not None:
        return int(pl.get("passed_cases", 0))
    rl = str(row_label or "").strip().lower()
    for key, value in day_map.items():
        if str(key or "").strip().lower() == rl:
            return int(value.get("passed_cases", 0))
    return 0


def _weekly_best_branch_context_from_all_daily(
    report_data: dict[tuple[str, str], dict[str, Any]],
    branch_name: str,
    matrix_entry: tuple[
        date | None,
        list[str],
        list[list[str]],
        list[list[bool]],
        list[list[bool]],
        dict[str, dict[str, int]],
        list[str],
        dict[str, list[dict[str, str]]],
        dict[str, Any],
    ],
    *,
    report_week_start: date | None,
) -> dict[str, Any]:
    """Pie + per-scenario column for «лучшая ветка» from **daily** payloads across all loaded folders.

    If the build is absent from this ISO week's matrix columns, data still comes from the
    latest matching daily report (any week in ``report_data``). If there is no matching
    folder at all, aggregates are empty and scenario cells are zero for this week's rows.
    """
    branch_name = str(branch_name or "").strip()
    summaries = _weekly_collect_daily_summaries_from_report_data(report_data)
    matches = [s for s in summaries if _summary_matches_branch(s, branch_name)]
    merged_progress: list[dict[str, Any]] = []
    merged_cycle_dicts: list[dict[str, Any]] = []
    if matches:
        best_day = max(s["report_day"] for s in matches)
        for s in matches:
            if s["report_day"] != best_day:
                continue
            merged_progress.extend(s["progress_rows"])
            cyc = s.get("cycles")
            if isinstance(cyc, dict):
                merged_cycle_dicts.append(cyc)
    if not matches and branch_name and report_week_start is not None:
        print(
            f"Week {report_week_start}: DRV branch {branch_name!r} — "
            "no matching daily folder in loaded report_data; "
            "«лучшая ветка» column shows empty/zero aggregates."
        )

    day_map = _weekly_aggregate_progress_map(merged_progress)
    _wk, _labels, rows, _ne, _blk, _cnt, _defk, _ckbl, _an = matrix_entry
    scenario_passed_by_row: dict[str, int] = {}
    for row in rows:
        if not row:
            continue
        rk = str(row[0] or "").strip()
        if not rk or rk.startswith("Итого:"):
            continue
        scenario_passed_by_row[rk] = _weekly_day_map_passed_cases(day_map, rk)

    merged_status: dict[str, int] = defaultdict(int)
    cycle_keys: list[dict[str, str]] = []
    seen_cycle_keys: set[str] = set()
    for cycles_dict in merged_cycle_dicts:
        for status_key, value in (_daily_aggregate_case_status_counts(cycles_dict) or {}).items():
            merged_status[status_key] += int(value)
        for entry in _extract_cycle_key_objects(cycles_dict):
            cid = str(entry.get("id") or "").strip()
            if not cid or cid in seen_cycle_keys:
                continue
            seen_cycle_keys.add(cid)
            cycle_keys.append(entry)

    return {
        "title": _weekly_best_branch_column_title(branch_name),
        "name": branch_name,
        "overall_counts": dict(merged_status),
        "scenario_passed_by_row": scenario_passed_by_row,
        "cycle_keys": cycle_keys,
    }


def _weekly_best_branch_column_context_for_week(
    matrix_entry: tuple[
        date | None,
        list[str],
        list[list[str]],
        list[list[bool]],
        list[list[bool]],
        dict[str, dict[str, int]],
        list[str],
        dict[str, list[dict[str, str]]],
        dict[str, Any],
    ],
    *,
    report_data: dict[tuple[str, str], dict[str, Any]],
    best_branch_name: str,
    report_week_start: date | None,
) -> dict[str, Any] | None:
    if report_week_start is None:
        return None
    branch_name = str(best_branch_name or "").strip()
    if not branch_name:
        return None
    return _weekly_best_branch_context_from_all_daily(
        report_data,
        branch_name,
        matrix_entry,
        report_week_start=report_week_start,
    )


def _defect_summary_text(entry: dict[str, str] | None) -> str:
    return str((entry or {}).get("summary") or "").strip()


def _defect_summary_html(entry: dict[str, str] | None) -> str:
    text = _defect_summary_text(entry)
    return html.escape(text) if text else "—"


def _defect_summary_wiki(entry: dict[str, str] | None) -> str:
    text = _defect_summary_text(entry)
    return _wiki_escape(text) if text else "—"


def _weekly_defects_html_block(defect_keys: list[str]) -> str:
    if not defect_keys:
        return "<p><em>В процессе написания</em></p>"
    items = [
        f"  <li>{_weekly_jira_key_span_html(key)}</li>" for key in defect_keys
    ]
    return "<ul class='weekly-defects-list'>\n" + "\n".join(items) + "\n</ul>"


def _weekly_defects_wiki_block(defect_keys: list[str]) -> str:
    if not defect_keys:
        return "_В процессе написания_"
    return "\n".join(f"* {_weekly_jira_key_wiki(key)}" for key in defect_keys)


_DEFECT_TOP_LIMIT = 10


def _weekly_defect_extended_analytics_enabled() -> bool:
    """When false, weekly defect section omits summary, top-N, bug×build matrix, and hot bugs."""
    name = "ZEPHYR_WEEKLY_DEFECT_EXTENDED_ANALYTICS"
    parsed = _get_repo_dotenv_parsed()
    if name in parsed:
        return _parse_bool_env(parsed[name])
    return _parse_bool_env(os.getenv(name, "true"))


def _norm_jira_token(text: str | None) -> str:
    return str(text or "").strip().lower()


def _weekly_jira_key_span_html(issue_key: str) -> str:
    """Issue key cell: monospace link + data attribute for Confluence Jira macro."""
    key = str(issue_key or "").strip()
    if not key:
        return "—"
    url = _jira_issue_url(key)
    return (
        f'<span class="weekly-jira-key" data-jira-key="{html.escape(key)}">'
        f'<a class="weekly-jira-key-link" href="{html.escape(url)}" '
        f'target="_blank" rel="noopener">{html.escape(key)}</a></span>'
    )


def _jira_status_lozenge_html(status: str | None) -> str:
    t = _norm_jira_token(status)
    bg, fg, bd = "#dfe1e6", "#42526e", "#dfe1e6"
    if any(x in t for x in ("done", "closed", "resolved", "готов", "закрыт", "выполнен", "решён", "решен")):
        bg, fg, bd = "#e3fcef", "#006644", "#b3d4c7"
    elif any(x in t for x in ("progress", "development", "в работе", "review")):
        bg, fg, bd = "#deebff", "#0747a6", "#b3d4ff"
    elif any(x in t for x in ("block", "blocked", "блок")):
        bg, fg, bd = "#ffebe6", "#bf2600", "#ffccc7"
    elif any(x in t for x in ("hold", "wait", "to do", "backlog", "отлож", "ожид", "новый", "open")):
        bg, fg, bd = "#f4f5f7", "#42526e", "#dfe1e6"
    esc = html.escape(status or "—")
    return (
        f'<span class="jira-lozenge jira-lozenge-status" style="display:inline-block;max-width:100%;'
        f"padding:0 6px;border-radius:3px;font-size:11px;font-weight:600;line-height:1.82;"
        f"text-transform:uppercase;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
        f'background-color:{bg};color:{fg};border:1px solid {bd};" title="{esc}">{esc}</span>'
    )


def _jira_priority_lozenge_html(priority: str | None) -> str:
    t = _norm_jira_token(priority)
    bg, fg = "#dfe1e6", "#42526e"
    if any(x in t for x in ("highest", "blocker", "critical", "наивыс", "блокирующ")):
        bg, fg = "#de350b", "#ffffff"
    elif "high" in t or "высок" in t:
        bg, fg = "#ff8b00", "#ffffff"
    elif "medium" in t or "средн" in t or "normal" in t:
        bg, fg = "#ffab00", "#172b4d"
    elif "low" in t or "низк" in t or "lowest" in t:
        bg, fg = "#0065ff", "#ffffff"
    esc = html.escape(priority or "—")
    return (
        f'<span class="jira-lozenge jira-lozenge-priority" style="display:inline-block;max-width:100%;'
        f"padding:0 6px;border-radius:3px;font-size:11px;font-weight:600;line-height:1.82;"
        f"white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
        f'background-color:{bg};color:{fg};" title="{esc}">{esc}</span>'
    )


_WEEKLY_JIRA_KEY_SPAN_RE = re.compile(
    r'<span\s+class="weekly-jira-key"\s+data-jira-key="([A-Z][A-Z0-9]+-\d+)"[^>]*>.*?</span>',
    re.IGNORECASE | re.DOTALL,
)


def _jira_issue_confluence_storage_macro(issue_key: str) -> str:
    """Confluence storage: native Jira Issues macro (renders like Jira, not plain text)."""
    key = html.escape(issue_key.strip())
    server_id = (os.getenv("ZEPHYR_CONFLUENCE_JIRA_SERVER_ID") or "").strip()
    server_param = ""
    if server_id:
        server_param = (
            f'<ac:parameter ac:name="serverId">{html.escape(server_id)}</ac:parameter>'
        )
    return (
        '<ac:structured-macro ac:name="jira" ac:schema-version="1">'
        f'<ac:parameter ac:name="key">{key}</ac:parameter>{server_param}'
        "</ac:structured-macro>"
    )


def _replace_weekly_jira_key_spans_with_confluence_macro(body_html: str) -> str:
    if (
        not body_html
        or "weekly-jira-key" not in body_html
        or not _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_WEEKLY_JIRA_MACRO", "true"))
    ):
        return body_html

    def _repl(match: re.Match[str]) -> str:
        return _jira_issue_confluence_storage_macro(match.group(1))

    return _WEEKLY_JIRA_KEY_SPAN_RE.sub(_repl, body_html)


_WEEKLY_DEFECTS_HEADER_RE = re.compile(
    r"<h3\b[^>]*>\s*<strong>\s*Завед[её]нные дефекты\s*</strong>\s*</h3>\s*",
    flags=re.IGNORECASE,
)
_WEEKLY_DEFECTS_JIRA_OPEN_RE = re.compile(
    r"<div\b[^>]*\bclass=(?:'|\")[^'\"]*\bweekly-defects-jira\b[^'\"]*(?:'|\")[^>]*>",
    flags=re.IGNORECASE,
)
_WEEKLY_EXCERPT_MACRO_RE = re.compile(
    r"<ac:structured-macro\b[^>]*\bac:name=['\"]excerpt['\"][^>]*>\s*"
    r"<ac:rich-text-body>(?P<body>.*?)</ac:rich-text-body>\s*</ac:structured-macro>",
    flags=re.IGNORECASE | re.DOTALL,
)


def _unwrap_weekly_excerpt_macro(body_html: str) -> str:
    """Drop a single weekly excerpt wrapper so the span can be rebuilt."""
    match = _WEEKLY_EXCERPT_MACRO_RE.search(body_html)
    if not match:
        return body_html
    return body_html[: match.start()] + match.group("body") + body_html[match.end() :]


def _find_weekly_excerpt_block_span(body_html: str) -> tuple[int, int] | None:
    """Return [start, end) for the weekly Confluence excerpt (выборка).

    Span covers Weekly title, overall/scenario score blocks, and «Заведённые дефекты».
    """
    start_match = re.search(r"<h1\b[^>]*>\s*Weekly\b", body_html, flags=re.IGNORECASE)
    if not start_match:
        return None
    start = start_match.start()

    scenario_match = re.search(
        r"<h3\b[^>]*>\s*<strong>\s*Score по сценариям\s*</strong>\s*</h3>\s*"
        r"<table\b[^>]*>.*?</table>",
        body_html[start:],
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not scenario_match:
        return None
    end = start + scenario_match.end()

    tail = body_html[end:]
    if not _WEEKLY_DEFECTS_HEADER_RE.search(tail):
        return start, end

    # Defects are the last weekly section (before </body>); include all trailing content.
    return start, len(body_html)


def _wrap_html_span_with_excerpt_macro(
    body_html: str, block_start: int, block_end: int
) -> str:
    block_html = body_html[block_start:block_end]
    wrapped = (
        '<ac:structured-macro ac:name="excerpt" ac:schema-version="1">'
        f"<ac:rich-text-body>{block_html}</ac:rich-text-body>"
        "</ac:structured-macro>"
    )
    return body_html[:block_start] + wrapped + body_html[block_end:]


def _wrap_weekly_scenario_block_with_excerpt_macro(body_html: str) -> str:
    """Wrap weekly excerpt block (выборка): Weekly heading through «Заведённые дефекты»."""
    if (
        not body_html
        or "Score по сценариям" not in body_html
        or "<table" not in body_html
        or not _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_WEEKLY_EXCERPT", "true"))
    ):
        return body_html
    body_html = _unwrap_weekly_excerpt_macro(body_html)
    span = _find_weekly_excerpt_block_span(body_html)
    if span is None:
        return body_html
    block_start, block_end = span
    return _wrap_html_span_with_excerpt_macro(body_html, block_start, block_end)


_DAILY_REPORT_PREAMBLE_OPEN_RE = re.compile(
    r"<div\b[^>]*\bclass=(?:'|\")[^'\"]*\breport-preamble\b",
    flags=re.IGNORECASE,
)


def _daily_has_scenarios_section(body_html: str) -> bool:
    return bool(
        re.search(
            r"<h2\b[^>]*\bid\s*=\s*['\"]scenarios['\"]",
            body_html,
            flags=re.IGNORECASE,
        )
    )


def _find_daily_excerpt_block_span(body_html: str) -> tuple[int, int] | None:
    """Return [start, end) for the daily Confluence excerpt (выборка).

    Span covers preamble (sections 1–2) through conclusion (section 4); TOC stays outside.
    """
    if not body_html or not _daily_has_scenarios_section(body_html):
        return None
    start_match = _DAILY_REPORT_PREAMBLE_OPEN_RE.search(body_html)
    if not start_match:
        start_match = re.search(
            r"<h2\b[^>]*\bid\s*=\s*['\"]sec-object['\"][^>]*>",
            body_html,
            flags=re.IGNORECASE,
        )
    if not start_match:
        start_match = re.search(
            r"<h2\b[^>]*\bid\s*=\s*['\"]scenarios['\"][^>]*>",
            body_html,
            flags=re.IGNORECASE,
        )
    if not start_match:
        return None
    start = start_match.start()
    body_close = body_html.lower().rfind("</body>")
    end = body_close if body_close > start else len(body_html)
    return start, end


def _wrap_daily_report_with_excerpt_macro(body_html: str) -> str:
    """Wrap daily excerpt block (выборка): preamble through conclusion; TOC outside."""
    if not body_html or not _parse_bool_env(
        os.getenv("ZEPHYR_CONFLUENCE_DAILY_EXCERPT", "true")
    ):
        return body_html
    if not _daily_has_scenarios_section(body_html):
        return body_html
    body_html = _unwrap_weekly_excerpt_macro(body_html)
    span = _find_daily_excerpt_block_span(body_html)
    if span is None:
        return body_html
    block_start, block_end = span
    return _wrap_html_span_with_excerpt_macro(body_html, block_start, block_end)


def _replace_legacy_weekly_table_macros_with_excerpt(body_html: str) -> str:
    if not body_html:
        return body_html
    body_html = body_html.replace('ac:name="table-filter"', 'ac:name="excerpt"')
    body_html = body_html.replace("ac:name='table-filter'", "ac:name='excerpt'")
    body_html = body_html.replace('ac:name="table-excerpt"', 'ac:name="excerpt"')
    body_html = body_html.replace("ac:name='table-excerpt'", "ac:name='excerpt'")
    return body_html


def _weekly_jira_key_wiki(issue_key: str) -> str:
    """Confluence wiki: embedded Jira issue (falls back to link if macro unsupported)."""
    key = str(issue_key or "").strip()
    if not key:
        return " "
    if _parse_bool_env(os.getenv("ZEPHYR_WEEKLY_WIKI_PLAIN_JIRA_LINKS")):
        return f"[{_wiki_escape(key)}|{_jira_issue_url(key)}]"
    return f"{{jira:key={_wiki_escape(key)}}}"


def _filter_keys_by_issuetype(
    keys: list[str],
    defect_meta: dict[str, dict[str, str]] | None,
) -> list[str]:
    """Optionally restrict to issuetype = ZEPHYR_DEFECT_TYPE_FILTER (default Bug).

    If Jira metadata is missing for a key, the key is kept (graceful).
    """
    raw_filter = os.getenv("ZEPHYR_DEFECT_TYPE_FILTER", "Bug")
    allowed = {
        token.strip().lower()
        for token in str(raw_filter or "").split(",")
        if token.strip()
    }
    if not allowed:
        return keys
    meta = defect_meta or {}
    out: list[str] = []
    for key in keys:
        entry = meta.get(key)
        if not entry:
            out.append(key)
            continue
        issuetype = (entry.get("issuetype") or "").strip().lower()
        if not issuetype or issuetype in allowed:
            out.append(key)
    return out


def _weekly_defect_analytics_html(
    defect_analytics: dict[str, Any] | None,
    defect_meta: dict[str, dict[str, str]] | None,
    column_labels: list[str],
) -> str:
    analytics = defect_analytics or {}
    keys_ordered = list(analytics.get("keys_ordered") or [])
    keys_ordered = _filter_keys_by_issuetype(keys_ordered, defect_meta)
    if not keys_ordered:
        return "<p><em>В процессе написания</em></p>"

    matrix = analytics.get("matrix") or {}
    bug_total_cases = analytics.get("bug_total_cases") or {}
    bug_builds_count = analytics.get("bug_builds_count") or {}
    totals_by_build = analytics.get("totals_by_build") or {}
    cases_with = analytics.get("cases_with_bug_by_build") or {}
    cases_without = analytics.get("cases_without_bug_by_build") or {}
    failed_with = analytics.get("failed_cases_with_bug_by_build") or {}
    failed_without = analytics.get("failed_cases_without_bug_by_build") or {}
    hot_bugs = [k for k in (analytics.get("hot_bugs") or []) if k in keys_ordered]
    meta = defect_meta or {}
    extended = _weekly_defect_extended_analytics_enabled()

    parts: list[str] = []
    parts.append('<div class="weekly-defects-jira">')

    if extended:
        # 1. Сводка
        parts.append("<h4>Сводка</h4>")
        parts.append("<table class='weekly-defects-table weekly-defects-summary'>")
        header_cells = ["<th>Метрика</th>"] + [
            f"<th>{html.escape(label)}</th>" for label in column_labels
        ]
        parts.append("<thead><tr>" + "".join(header_cells) + "</tr></thead>")
        parts.append("<tbody>")

        def _row(label: str, getter) -> str:
            cells = [f"<td>{html.escape(label)}</td>"]
            for col in column_labels:
                cells.append(f"<td>{int(getter(col) or 0)}</td>")
            return "<tr>" + "".join(cells) + "</tr>"

        parts.append(_row("Уникальных багов", lambda c: totals_by_build.get(c, 0)))
        parts.append(_row("Кейсов с багом", lambda c: cases_with.get(c, 0)))
        parts.append(_row("Кейсов без бага", lambda c: cases_without.get(c, 0)))
        parts.append(_row("Fail-кейсов с багом", lambda c: failed_with.get(c, 0)))
        parts.append(_row("Fail-кейсов без бага", lambda c: failed_without.get(c, 0)))
        parts.append("</tbody></table>")

        # 2. Топ-N
        parts.append(f"<h4>Топ багов (до {_DEFECT_TOP_LIMIT})</h4>")
        top_keys = keys_ordered[:_DEFECT_TOP_LIMIT]
        parts.append("<table class='weekly-defects-table weekly-defects-top'>")
        parts.append(
            "<thead><tr>"
            "<th>Ключ</th><th>Summary</th><th>Priority</th><th>Status</th>"
            "<th>Кейсов</th><th>Билдов</th>"
            "</tr></thead><tbody>"
        )
        for key in top_keys:
            entry = meta.get(key, {}) or {}
            parts.append(
                "<tr>"
                f"<td class='weekly-jira-key-cell'>{_weekly_jira_key_span_html(key)}</td>"
                f"<td>{_defect_summary_html(entry)}</td>"
                f"<td>{_jira_priority_lozenge_html(entry.get('priority', ''))}</td>"
                f"<td>{_jira_status_lozenge_html(entry.get('status', ''))}</td>"
                f"<td style='text-align:center;'>{int(bug_total_cases.get(key, 0))}</td>"
                f"<td style='text-align:center;'>{int(bug_builds_count.get(key, 0))}</td>"
                "</tr>"
            )
        parts.append("</tbody></table>")

        # 3. Матрица «баг x билд»
        parts.append("<h4>Матрица «баг × билд»</h4>")
        parts.append("<table class='weekly-defects-table weekly-defects-matrix'>")
        # <colgroup>: «Ключ» ~12%, «Priority» ~12%, build-колонки делят остаток поровну.
        n_builds_matrix = len(column_labels)
        if n_builds_matrix > 0:
            key_pct = 10.0
            summary_pct = 20.0
            priority_pct = 10.0
            builds_total_pct = 100.0 - key_pct - summary_pct - priority_pct
            build_pct = builds_total_pct / n_builds_matrix
            col_tags = [
                f"<col style='width:{key_pct:.2f}%'>",
                f"<col style='width:{summary_pct:.2f}%'>",
                f"<col style='width:{priority_pct:.2f}%'>",
            ]
            col_tags.extend(
                f"<col style='width:{build_pct:.2f}%'>" for _ in range(n_builds_matrix)
            )
            parts.append("<colgroup>" + "".join(col_tags) + "</colgroup>")
        matrix_header = (
            "<thead><tr><th>Ключ</th><th>Summary</th><th>Priority</th>"
            + "".join(f"<th>{html.escape(label)}</th>" for label in column_labels)
            + "</tr></thead><tbody>"
        )
        parts.append(matrix_header)
        for key in keys_ordered:
            entry = meta.get(key, {}) or {}
            cells = [
                f"<td class='weekly-jira-key-cell'>{_weekly_jira_key_span_html(key)}</td>",
                f"<td>{_defect_summary_html(entry)}</td>",
                f"<td>{_jira_priority_lozenge_html(entry.get('priority', ''))}</td>",
            ]
            row_map = matrix.get(key, {}) or {}
            for col in column_labels:
                value = int(row_map.get(col, 0))
                cells.append(
                    f"<td class='matrix-num'>{value if value > 0 else '—'}</td>"
                )
            parts.append("<tr>" + "".join(cells) + "</tr>")
        parts.append("</tbody></table>")

        # 4. Hot bugs
        if hot_bugs:
            links = ", ".join(_weekly_jira_key_span_html(key) for key in hot_bugs)
            parts.append(
                "<p class='weekly-defects-hot'>"
                "<strong>Горячие баги (в подряд идущих билдах):</strong> "
                f"{links}"
                "</p>"
            )

    # Full list (always when keys non-empty)
    parts.append("<h4>Все баги</h4>")
    parts.append("<table class='weekly-defects-table weekly-defects-all'>")
    parts.append(
        "<thead><tr>"
        "<th>Ключ</th><th>Summary</th><th>Priority</th><th>Status</th>"
        "<th>Кейсов</th><th>Билдов</th>"
        "</tr></thead><tbody>"
    )
    for key in keys_ordered:
        entry = meta.get(key, {}) or {}
        parts.append(
            "<tr>"
            f"<td class='weekly-jira-key-cell'>{_weekly_jira_key_span_html(key)}</td>"
            f"<td>{_defect_summary_html(entry)}</td>"
            f"<td>{_jira_priority_lozenge_html(entry.get('priority', ''))}</td>"
            f"<td>{_jira_status_lozenge_html(entry.get('status', ''))}</td>"
            f"<td style='text-align:center;'>{int(bug_total_cases.get(key, 0))}</td>"
            f"<td style='text-align:center;'>{int(bug_builds_count.get(key, 0))}</td>"
            "</tr>"
        )
    parts.append("</tbody></table>")

    parts.append("</div>")
    return "\n".join(parts)


def _weekly_defect_analytics_wiki(
    defect_analytics: dict[str, Any] | None,
    defect_meta: dict[str, dict[str, str]] | None,
    column_labels: list[str],
) -> str:
    analytics = defect_analytics or {}
    keys_ordered = list(analytics.get("keys_ordered") or [])
    keys_ordered = _filter_keys_by_issuetype(keys_ordered, defect_meta)
    if not keys_ordered:
        return "_В процессе написания_"

    matrix = analytics.get("matrix") or {}
    bug_total_cases = analytics.get("bug_total_cases") or {}
    bug_builds_count = analytics.get("bug_builds_count") or {}
    totals_by_build = analytics.get("totals_by_build") or {}
    cases_with = analytics.get("cases_with_bug_by_build") or {}
    cases_without = analytics.get("cases_without_bug_by_build") or {}
    failed_with = analytics.get("failed_cases_with_bug_by_build") or {}
    failed_without = analytics.get("failed_cases_without_bug_by_build") or {}
    hot_bugs = [k for k in (analytics.get("hot_bugs") or []) if k in keys_ordered]
    meta = defect_meta or {}
    extended = _weekly_defect_extended_analytics_enabled()

    parts: list[str] = []

    if extended:
        # 1. Сводка
        parts.append("h4. Сводка")
        header_cells = ["Метрика"] + list(column_labels)
        parts.append("|| " + " || ".join(_wiki_escape(c) for c in header_cells) + " ||")

        def _row(label: str, source: dict[str, int]) -> str:
            cells = [label] + [str(int(source.get(c, 0))) for c in column_labels]
            return "| " + " | ".join(_wiki_escape(c) for c in cells) + " |"

        parts.append(_row("Уникальных багов", totals_by_build))
        parts.append(_row("Кейсов с багом", cases_with))
        parts.append(_row("Кейсов без бага", cases_without))
        parts.append(_row("Fail-кейсов с багом", failed_with))
        parts.append(_row("Fail-кейсов без бага", failed_without))
        parts.append("")

        # 2. Топ
        parts.append(f"h4. Топ багов (до {_DEFECT_TOP_LIMIT})")
        parts.append(
            "|| Ключ || Summary || Priority || Status || Кейсов || Билдов ||"
        )
        for key in keys_ordered[:_DEFECT_TOP_LIMIT]:
            entry = meta.get(key, {}) or {}
            cells = [
                _weekly_jira_key_wiki(key),
                _defect_summary_wiki(entry),
                _wiki_escape(entry.get("priority", "")) or " ",
                _wiki_escape(entry.get("status", "")) or " ",
                str(int(bug_total_cases.get(key, 0))),
                str(int(bug_builds_count.get(key, 0))),
            ]
            parts.append("| " + " | ".join(cells) + " |")
        parts.append("")

        # 3. Матрица
        parts.append("h4. Матрица «баг × билд»")
        matrix_header = ["Ключ", "Summary", "Priority"] + list(column_labels)
        parts.append("|| " + " || ".join(_wiki_escape(c) for c in matrix_header) + " ||")
        for key in keys_ordered:
            entry = meta.get(key, {}) or {}
            row_map = matrix.get(key, {}) or {}
            cells = [
                _weekly_jira_key_wiki(key),
                _defect_summary_wiki(entry),
                _wiki_escape(entry.get("priority", "")) or " ",
            ]
            for col in column_labels:
                value = int(row_map.get(col, 0))
                cells.append(f"*{value}*" if value > 0 else "—")
            parts.append("| " + " | ".join(cells) + " |")

        # 4. Hot bugs
        if hot_bugs:
            parts.append("")
            links = ", ".join(_weekly_jira_key_wiki(key) for key in hot_bugs)
            parts.append(
                "{warning:title=Горячие баги}\n"
                f"В подряд идущих билдах: {links}\n"
                "{warning}"
            )

    if parts:
        parts.append("")
    parts.append("h4. Все баги")
    parts.append("|| Ключ || Summary || Priority || Status || Кейсов || Билдов ||")
    for key in keys_ordered:
        entry = meta.get(key, {}) or {}
        cells = [
            _weekly_jira_key_wiki(key),
            _defect_summary_wiki(entry),
            _wiki_escape(entry.get("priority", "")) or " ",
            _wiki_escape(entry.get("status", "")) or " ",
            str(int(bug_total_cases.get(key, 0))),
            str(int(bug_builds_count.get(key, 0))),
        ]
        parts.append("| " + " | ".join(cells) + " |")

    return "\n".join(parts)


# Confluence page title must differ from ZEPHYR_CONFLUENCE_BUGS_PARENT_TITLE («Баги» folder).
BUGS_ROLLUP_CONFLUENCE_TITLE = "Сводка багов"
BUGS_ROLLUP_DISPLAY_TITLE = "Баги"
BUGS_ROLLUP_SECTION_LAST_WEEKS = "Баги за последние 2 недели"
BUGS_ROLLUP_SECTION_ALL = "Все заведённые баги"

_WEEKLY_HTML_DEFECT_STYLES = (
    "body{font-family:Arial,sans-serif;margin:24px;}"
    "h1{margin-bottom:8px;}h2{margin-top:20px;margin-bottom:8px;font-weight:700;}"
    "h3{margin-top:16px;margin-bottom:8px;font-weight:700;}"
    "h4{margin-top:14px;margin-bottom:6px;font-weight:600;}"
    "table{border-collapse:collapse;width:100%;margin-bottom:16px;table-layout:fixed;}"
    "th,td{border:1px solid #d6d6d6;padding:6px 8px;text-align:left;vertical-align:top;"
    "overflow-wrap:anywhere;word-wrap:break-word;}"
    "th{background:#f0f2f5;font-weight:600;}"
    ".weekly-defects-table{font-size:14px;}"
    ".weekly-defects-table th{background:#f4f5f7;color:#42526e;font-weight:600;}"
    ".weekly-defects-matrix{table-layout:fixed;width:100%;}"
    ".weekly-defects-matrix td,.weekly-defects-matrix th{padding:4px 6px;}"
    ".weekly-defects-matrix .matrix-num{text-align:center;font-weight:700;}"
    ".weekly-defects-all{table-layout:fixed;width:100%;}"
    ".weekly-jira-key-cell{white-space:nowrap;vertical-align:middle;width:1%;}"
    ".weekly-jira-key-link{font-family:ui-monospace,Consolas,monospace;font-size:13px;"
    "font-weight:600;color:#0052cc;text-decoration:none;background:#e9f2ff;"
    "padding:2px 6px;border-radius:3px;border:1px solid #b3d4ff;}"
    ".weekly-jira-key-link:hover{text-decoration:underline;}"
    ".weekly-defects-hot .weekly-jira-key-link{margin-right:4px;}"
    ".jira-lozenge{vertical-align:middle;}"
    ".bugs-rollup-subtitle{color:#5e6c84;margin:0 0 16px;}"
)


def _bugs_rollup_last_weeks_count() -> int:
    raw = (os.getenv("ZEPHYR_BUGS_ROLLUP_LAST_WEEKS") or "2").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 2


def _defect_rollup_from_report_data(
    report_data: dict[tuple[str, str], dict[str, Any]],
    *,
    last_n_weeks: int | None,
) -> tuple[list[str], dict[str, Any], list[date]]:
    """Aggregate defect analytics across one or more ISO weeks (weekly report format)."""
    weekly_groups: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for (folder_id, folder_name), payload in report_data.items():
        cycles = payload.get("cycles", {})
        if not isinstance(cycles, dict):
            continue
        report_day = _resolve_folder_report_day(folder_name, cycles)
        if report_day is None:
            report_day = _resolve_daily_title_day(cycles)
        if report_day is None:
            continue
        progress_rows = _build_cycle_progress_rows(cycles)
        if not progress_rows:
            continue
        test_day = _test_day_from_folder_day(report_day)
        week_start = _release_week_start(test_day)
        weekly_groups[week_start].append(
            {
                "folder_id": str(folder_id),
                "report_day": report_day,
                "test_day": test_day,
                "column_label": _parse_weekly_column_label_from_folder_name(folder_name),
                "cycles": cycles,
            }
        )
    if not weekly_groups:
        return [], _empty_defect_analytics(), []

    week_keys = sorted(weekly_groups.keys())
    if last_n_weeks is not None:
        week_keys = week_keys[-last_n_weeks:]

    cycles_by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    column_labels_by_day: dict[date, list[str]] = defaultdict(list)
    for week_start in week_keys:
        week_daily_summaries = sorted(
            weekly_groups[week_start],
            key=lambda item: (item["report_day"], item["folder_id"]),
        )
        for summary in week_daily_summaries:
            day_cycles = summary.get("cycles")
            if isinstance(day_cycles, dict):
                cycles_by_day[summary["report_day"]].append(day_cycles)
            column_label = str(summary.get("column_label") or "").strip()
            if column_label:
                column_labels_by_day[summary["report_day"]].append(column_label)

    ordered_days = sorted(cycles_by_day.keys())
    column_labels: list[str] = []
    for day in ordered_days:
        day_labels = column_labels_by_day.get(day, [])
        if day_labels:
            column_labels.append(Counter(day_labels).most_common(1)[0][0])
        else:
            column_labels.append(f"nightly-dev-{day.strftime('%Y.%m.%d')}")

    analytics = _compute_weekly_defect_analytics(
        cycles_by_day, ordered_days, column_labels
    )
    return column_labels, analytics, week_keys


def _bugs_rollup_section_subtitle(
    week_keys: list[date], column_labels: list[str]
) -> str:
    parts: list[str] = []
    if week_keys:
        parts.append(
            f"Недели: {week_keys[0].isoformat()}"
            + (f" — {week_keys[-1].isoformat()}" if len(week_keys) > 1 else "")
        )
    if column_labels:
        parts.append(f"Билдов: {len(column_labels)}")
    return " · ".join(parts)


def _bugs_rollup_html_section(
    section_title: str,
    *,
    defect_analytics: dict[str, Any],
    defect_meta: dict[str, dict[str, str]] | None,
    column_labels: list[str],
    week_keys: list[date],
) -> list[str]:
    blocks = [f"<h2><strong>{html.escape(section_title)}</strong></h2>"]
    subtitle = _bugs_rollup_section_subtitle(week_keys, column_labels)
    if subtitle:
        blocks.append(f"<p class='bugs-rollup-subtitle'>{html.escape(subtitle)}</p>")
    keys = list((defect_analytics or {}).get("keys_ordered") or [])
    if keys:
        blocks.append(
            _weekly_defect_analytics_html(defect_analytics, defect_meta, column_labels)
        )
    else:
        blocks.append("<p><em>Баги не найдены в данных Zephyr за выбранный период.</em></p>")
    return blocks


def render_bugs_rollup_html_report(
    *,
    last_weeks_analytics: dict[str, Any],
    all_analytics: dict[str, Any],
    defect_meta: dict[str, dict[str, str]] | None,
    last_weeks_labels: list[str],
    all_labels: list[str],
    last_weeks_keys: list[date],
    all_week_keys: list[date],
) -> str:
    sections = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        f"<title>{html.escape(BUGS_ROLLUP_CONFLUENCE_TITLE)}</title>",
        f"<style>{_WEEKLY_HTML_DEFECT_STYLES}</style>",
        "</head><body>",
        f"<h1>{html.escape(BUGS_ROLLUP_DISPLAY_TITLE)}</h1>",
    ]
    sections.extend(
        _bugs_rollup_html_section(
            BUGS_ROLLUP_SECTION_LAST_WEEKS,
            defect_analytics=last_weeks_analytics,
            defect_meta=defect_meta,
            column_labels=last_weeks_labels,
            week_keys=last_weeks_keys,
        )
    )
    sections.extend(
        _bugs_rollup_html_section(
            BUGS_ROLLUP_SECTION_ALL,
            defect_analytics=all_analytics,
            defect_meta=defect_meta,
            column_labels=all_labels,
            week_keys=all_week_keys,
        )
    )
    sections.append("</body></html>")
    return "\n".join(sections)


def _bugs_rollup_wiki_section(
    section_title: str,
    *,
    defect_analytics: dict[str, Any],
    defect_meta: dict[str, dict[str, str]] | None,
    column_labels: list[str],
    week_keys: list[date],
) -> list[str]:
    lines = [f"h2. {section_title}", ""]
    subtitle = _bugs_rollup_section_subtitle(week_keys, column_labels)
    if subtitle:
        lines.append(f"_{subtitle}_")
    lines.append("")
    keys = list((defect_analytics or {}).get("keys_ordered") or [])
    if keys:
        lines.append(
            _weekly_defect_analytics_wiki(defect_analytics, defect_meta, column_labels)
        )
    else:
        lines.append("_Баги не найдены в данных Zephyr за выбранный период._")
    lines.append("")
    return lines


def render_bugs_rollup_wiki_report(
    *,
    last_weeks_analytics: dict[str, Any],
    all_analytics: dict[str, Any],
    defect_meta: dict[str, dict[str, str]] | None,
    last_weeks_labels: list[str],
    all_labels: list[str],
    last_weeks_keys: list[date],
    all_week_keys: list[date],
) -> str:
    lines = [f"h1. {BUGS_ROLLUP_DISPLAY_TITLE}", ""]
    lines.extend(
        _bugs_rollup_wiki_section(
            BUGS_ROLLUP_SECTION_LAST_WEEKS,
            defect_analytics=last_weeks_analytics,
            defect_meta=defect_meta,
            column_labels=last_weeks_labels,
            week_keys=last_weeks_keys,
        )
    )
    lines.extend(
        _bugs_rollup_wiki_section(
            BUGS_ROLLUP_SECTION_ALL,
            defect_analytics=all_analytics,
            defect_meta=defect_meta,
            column_labels=all_labels,
            week_keys=all_week_keys,
        )
    )
    return "\n".join(lines).rstrip() + "\n"


def write_bugs_rollup_reports(
    output_dir: str,
    report_data: dict[tuple[str, str], dict[str, Any]],
    formats: set[str],
    *,
    defect_meta: dict[str, dict[str, str]] | None,
    last_weeks: int | None = None,
) -> list[str]:
    """Write a single bugs index page (last N weeks + all time) in weekly defect format."""
    os.makedirs(output_dir, exist_ok=True)
    n_weeks = last_weeks if last_weeks is not None else _bugs_rollup_last_weeks_count()
    last_labels, last_analytics, last_week_keys = _defect_rollup_from_report_data(
        report_data, last_n_weeks=n_weeks
    )
    all_labels, all_analytics, all_week_keys = _defect_rollup_from_report_data(
        report_data, last_n_weeks=None
    )
    written: list[str] = []
    if "html" in formats:
        html_path = os.path.join(output_dir, "bugs_index.html")
        body = render_bugs_rollup_html_report(
            last_weeks_analytics=last_analytics,
            all_analytics=all_analytics,
            defect_meta=defect_meta,
            last_weeks_labels=last_labels,
            all_labels=all_labels,
            last_weeks_keys=last_week_keys,
            all_week_keys=all_week_keys,
        )
        _write_text_always(html_path, body)
        written.append(html_path)
    if "wiki" in formats:
        wiki_path = os.path.join(output_dir, "bugs_index.confluence.txt")
        body = render_bugs_rollup_wiki_report(
            last_weeks_analytics=last_analytics,
            all_analytics=all_analytics,
            defect_meta=defect_meta,
            last_weeks_labels=last_labels,
            all_labels=all_labels,
            last_weeks_keys=last_week_keys,
            all_week_keys=all_week_keys,
        )
        _write_text_always(wiki_path, body)
        written.append(wiki_path)
    return written


def _list_bugs_rollup_html_paths(output_dir: str) -> list[str]:
    if not os.path.isdir(output_dir):
        return []
    path = os.path.join(output_dir, "bugs_index.html")
    return [path] if os.path.isfile(path) else []


def render_weekly_html_report(
    week_start: date | None,
    weekday_labels: list[str],
    rows: list[list[str]],
    cell_all_not_executed: list[list[bool]] | None = None,
    cell_all_blocked: list[list[bool]] | None = None,
    *,
    column_status_counts: dict[str, dict[str, int]] | None = None,
    defect_keys: list[str] | None = None,
    cycle_keys_by_label: dict[str, list[dict[str, Any]]] | None = None,
    defect_analytics: dict[str, Any] | None = None,
    defect_meta: dict[str, dict[str, str]] | None = None,
    template_dir: str | None = None,
    folder_id_resolve: str | None = None,
    folder_id_mapping: str = "",
    folder_name_mapping: str = "",
    best_branch_column: dict[str, Any] | None = None,
) -> str:
    labels = list(weekday_labels)
    best_column = best_branch_column or {}
    best_title = str(best_column.get("title") or "").strip()
    has_best_column = bool(best_title)
    data_labels = ([best_title] if has_best_column else []) + labels
    best_scenario_map = (
        best_column.get("scenario_passed_by_row")
        if isinstance(best_column.get("scenario_passed_by_row"), dict)
        else {}
    )
    best_overall_counts = (
        best_column.get("overall_counts")
        if isinstance(best_column.get("overall_counts"), dict)
        else {}
    )
    best_cycle_keys = best_column.get("cycle_keys")
    if not isinstance(best_cycle_keys, list):
        best_cycle_keys = []
    title_text = _weekly_matrix_title_text(week_start, weekday_labels)
    header_cells = ["<th>Тестовый цикл</th>", "<th>Всего кейсов</th>"]
    header_cells.extend(f"<th>{html.escape(label)}</th>" for label in data_labels)
    builds_html = _weekly_builds_html_list(labels)
    builds_wiki = _weekly_builds_wiki_list(labels)
    preamble = _format_readable_html_preamble(
        template_dir,
        "weekly",
        folder_id_resolve,
        folder_id_mapping,
        folder_name_mapping,
        week_start,
        week_builds_html=builds_html,
        week_builds_wiki=builds_wiki,
    )
    sections = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        f"<title>{html.escape(title_text)}</title>",
        (
            "<style>"
            "body{font-family:Arial,sans-serif;margin:24px;}"
            "h1{margin-bottom:8px;}"
            "h2{margin-top:24px;margin-bottom:8px;font-weight:700;}"
            "h3{margin-top:20px;margin-bottom:8px;font-weight:700;}"
            "h4{margin-top:14px;margin-bottom:6px;font-weight:600;}"
            "table{border-collapse:collapse;width:100%;margin-bottom:16px;table-layout:fixed;}"
            "th,td{border:1px solid #d6d6d6;padding:6px 8px;text-align:left;vertical-align:top;overflow-wrap:anywhere;word-wrap:break-word;}"
            "th{background:#f0f2f5;font-weight:600;}.passed-count-cell{font-weight:400;text-align:center;}"
            ".total-cases-cell{text-align:center;}"
            ".group-total-row td{font-weight:700;background:#ffffff;color:#1f2328;}"
            ".group-total-row td:not(:first-child){text-align:center;}"
            ".scenario-sep td{border:none;height:8px;padding:0;background:transparent;}"
            ".report-preamble{margin:12px 0 20px;}"
            ".daily-pie-wrap{display:flex;flex-wrap:wrap;align-items:flex-start;gap:16px;margin:12px 0;}"
            ".pie-legend{display:flex;flex-wrap:wrap;gap:12px;align-items:center;font-size:13px;max-width:520px;}"
            ".pie-swatch{display:inline-block;width:12px;height:12px;border-radius:2px;margin-right:6px;vertical-align:middle;}"
            ".pie-empty{color:#666;margin:8px 0;}"
            ".weekly-overall-grid{display:flex;flex-wrap:wrap;gap:24px;align-items:flex-start;margin:12px 0 16px;}"
            ".weekly-overall-cell{flex:1 1 240px;min-width:240px;max-width:360px;}"
            ".weekly-overall-cell h4{margin:0 0 6px;}"
            ".weekly-defects-list{margin:8px 0 16px;padding-left:24px;}"
            ".weekly-defects-table{font-size:14px;}"
            ".weekly-defects-table th{background:#f4f5f7;color:#42526e;font-weight:600;}"
            ".weekly-defects-matrix{table-layout:fixed;width:100%;}"
            ".weekly-defects-matrix td,.weekly-defects-matrix th{padding:4px 6px;}"
            ".weekly-defects-matrix .matrix-num{text-align:center;font-weight:700;}"
            ".weekly-defects-all{table-layout:fixed;width:100%;}"
            ".weekly-jira-key-cell{white-space:nowrap;vertical-align:middle;width:1%;}"
            ".weekly-jira-key-link{font-family:ui-monospace,Consolas,monospace;font-size:13px;"
            "font-weight:600;color:#0052cc;text-decoration:none;background:#e9f2ff;"
            "padding:2px 6px;border-radius:3px;border:1px solid #b3d4ff;}"
            ".weekly-jira-key-link:hover{text-decoration:underline;}"
            ".weekly-defects-hot .weekly-jira-key-link{margin-right:4px;}"
            ".jira-lozenge{vertical-align:middle;}"
            "</style>"
        ),
        "</head><body>",
        f"<h1>{html.escape(title_text)}</h1>",
    ]
    if preamble:
        sections.append(preamble)
    sections.append("<h2 id='scenarios'><strong>3. Результаты тестирования</strong></h2>")

    sections.append("<h3 id='overall-score'><strong>Общий score</strong></h3>")
    counts_by_label = column_status_counts or {}
    cycle_keys_map = cycle_keys_by_label or {}
    if data_labels:
        sections.append("<div class='weekly-overall-grid'>")
        if has_best_column:
            cycle_keys_attr = ""
            if best_cycle_keys:
                cycle_keys_attr = (
                    " data-zephyr-cycle-keys=\""
                    + html.escape(
                        json.dumps(best_cycle_keys, ensure_ascii=True),
                        quote=True,
                    )
                    + "\""
                )
            sections.append(
                f"<div class='weekly-overall-cell'{cycle_keys_attr}>"
                f"<h4>{html.escape(best_title)}</h4>"
                f"{_daily_status_pie_svg(best_overall_counts)}"
                "</div>"
            )
        for label in labels:
            counts = counts_by_label.get(label, {}) or {}
            cycle_keys = cycle_keys_map.get(label) or []
            cycle_keys_attr = ""
            if cycle_keys:
                cycle_keys_attr = (
                    " data-zephyr-cycle-keys=\""
                    + html.escape(
                        json.dumps(cycle_keys, ensure_ascii=True),
                        quote=True,
                    )
                    + "\""
                )
            sections.append(
                f"<div class='weekly-overall-cell'{cycle_keys_attr}>"
                f"<h4>{html.escape(str(label))}</h4>"
                f"{_daily_status_pie_svg(counts)}"
                "</div>"
            )
        sections.append("</div>")
    else:
        sections.append("<p class='pie-empty'>Нет данных по статусам</p>")

    sections.append("<h3 id='scenario-score'><strong>Score по сценариям</strong></h3>")
    # Compact build columns by ~30%: redistribute the saved width to
    # "Тестовый цикл" (long titles) and "Всего кейсов".
    n_builds = len(data_labels)
    if n_builds > 0:
        equal_pct = 100.0 / (n_builds + 2)
        build_pct = equal_pct * 0.7
        remain_pct = 100.0 - build_pct * n_builds
        cycle_pct = remain_pct * 0.78
        total_pct = remain_pct - cycle_pct
        col_tags = [
            f"<col style='width:{cycle_pct:.2f}%'>",
            f"<col style='width:{total_pct:.2f}%'>",
        ]
        col_tags.extend(
            f"<col style='width:{build_pct:.2f}%'>" for _ in range(n_builds)
        )
        colgroup_html = "<colgroup>" + "".join(col_tags) + "</colgroup>"
    else:
        colgroup_html = ""
    sections.extend(
        [
            "<table>",
            colgroup_html,
            "<thead><tr>" + "".join(header_cells) + "</tr></thead><tbody>",
        ]
    )
    for row_idx, row in enumerate(rows):
        if str(row[0]).startswith("Итого:"):
            total_cells = [
                f"<td style='font-weight:700;background:#ffffff;color:#1f2328;'>{html.escape(row[0])}</td>",
                (
                    "<td class='total-cases-cell' "
                    "style='font-weight:700;background:#ffffff;color:#1f2328;text-align:center;'>"
                    f"{html.escape(row[1])}</td>"
                ),
            ]
            data_values: list[str] = []
            if has_best_column:
                data_values.append(str(best_scenario_map.get(str(row[0] or "").strip(), 0)))
            data_values.extend(
                str(row[2 + idx] if 2 + idx < len(row) else "0") for idx in range(len(labels))
            )
            total_cells.extend(
                (
                    "<td style='font-weight:700;background:#ffffff;color:#1f2328;text-align:center;'>"
                    f"{html.escape(value)}</td>"
                )
                for value in data_values
            )
            sections.append(
                (
                    "<tr class='group-total-row'>"
                    + "".join(total_cells)
                    + "</tr>"
                )
            )
            continue
        passed_cells: list[str] = []
        ne_row = (
            cell_all_not_executed[row_idx]
            if cell_all_not_executed and row_idx < len(cell_all_not_executed)
            else []
        )
        blocked_row = (
            cell_all_blocked[row_idx]
            if cell_all_blocked and row_idx < len(cell_all_blocked)
            else []
        )
        for idx in range(len(data_labels)):
            if has_best_column and idx == 0:
                passed_value = int(best_scenario_map.get(str(row[0] or "").strip(), 0))
                all_ne = False
                all_blocked = False
            else:
                regular_idx = idx - (1 if has_best_column else 0)
                passed_value = int(row[2 + regular_idx]) if 2 + regular_idx < len(row) else 0
                all_ne = bool(ne_row[regular_idx]) if regular_idx < len(ne_row) else False
                all_blocked = (
                    bool(blocked_row[regular_idx]) if regular_idx < len(blocked_row) else False
                )
            passed_cells.append(
                "<td class='passed-count-cell' "
                f"style='background:{_passed_count_color(passed_value, all_not_executed=all_ne, all_blocked=all_blocked)};"
                f"color:{_passed_count_text_color(passed_value, all_not_executed=all_ne, all_blocked=all_blocked)};"
                "text-align:center;'>"
                f"{passed_value}</td>"
            )
        sections.append(
            (
                "<tr>"
                f"<td>{html.escape(row[0])}</td>"
                f"<td class='total-cases-cell' style='text-align:center;'>{html.escape(row[1])}</td>"
                + "".join(passed_cells)
                + "</tr>"
            )
        )
    # Grand total across detail rows (skip "Итого: <group>" subtotals to
    # avoid double-counting).
    if rows:
        detail_rows = [r for r in rows if not str(r[0]).startswith("Итого:")]
        total_cases_grand = sum(int(r[1]) for r in detail_rows if len(r) > 1)
        passed_grand: list[int] = []
        if has_best_column:
            passed_grand.append(
                sum(int(best_scenario_map.get(str(r[0] or "").strip(), 0)) for r in detail_rows)
            )
        passed_grand.extend(
            sum(int(r[2 + idx]) for r in detail_rows if 2 + idx < len(r))
            for idx in range(len(labels))
        )
        grand_cells = [
            "<td style='font-weight:700;background:#eef1f5;color:#1f2328;'>Итого</td>",
            (
                "<td class='total-cases-cell' "
                "style='font-weight:700;background:#eef1f5;color:#1f2328;text-align:center;'>"
                f"{total_cases_grand}</td>"
            ),
        ]
        grand_cells.extend(
            (
                "<td style='font-weight:700;background:#eef1f5;color:#1f2328;text-align:center;'>"
                f"{value}</td>"
            )
            for value in passed_grand
        )
        sections.append(
            "<tr class='grand-total-row'>" + "".join(grand_cells) + "</tr>"
        )
    sections.append("</tbody></table>")

    sections.append("<h3 id='defects'><strong>Заведённые дефекты</strong></h3>")
    merged_analytics = _coalesce_weekly_defect_analytics(defect_analytics, defect_keys)
    if merged_analytics and (merged_analytics.get("keys_ordered") or []):
        sections.append(
            _weekly_defect_analytics_html(merged_analytics, defect_meta, labels)
        )
    else:
        sections.append(_weekly_defects_html_block(defect_keys or []))

    sections.append("</body></html>")
    return "\n".join(sections)


def render_weekly_wiki_report(
    week_start: date | None,
    weekday_labels: list[str],
    rows: list[list[str]],
    *,
    column_status_counts: dict[str, dict[str, int]] | None = None,
    defect_keys: list[str] | None = None,
    defect_analytics: dict[str, Any] | None = None,
    defect_meta: dict[str, dict[str, str]] | None = None,
    template_dir: str | None = None,
    folder_id_resolve: str | None = None,
    folder_id_mapping: str = "",
    folder_name_mapping: str = "",
    best_branch_column: dict[str, Any] | None = None,
) -> str:
    title_text = _weekly_matrix_title_text(week_start, weekday_labels)
    labels = list(weekday_labels)
    best_column = best_branch_column or {}
    best_title = str(best_column.get("title") or "").strip()
    has_best_column = bool(best_title)
    data_labels = ([best_title] if has_best_column else []) + labels
    best_scenario_map = (
        best_column.get("scenario_passed_by_row")
        if isinstance(best_column.get("scenario_passed_by_row"), dict)
        else {}
    )
    best_overall_counts = (
        best_column.get("overall_counts")
        if isinstance(best_column.get("overall_counts"), dict)
        else {}
    )
    builds_html = _weekly_builds_html_list(labels)
    builds_wiki = _weekly_builds_wiki_list(labels)
    lines = [f"h1. {_wiki_escape(title_text)}", ""]
    wiki_pre = _format_readable_wiki_preamble(
        template_dir,
        "weekly",
        folder_id_resolve,
        folder_id_mapping,
        folder_name_mapping,
        week_start,
        week_builds_html=builds_html,
        week_builds_wiki=builds_wiki,
    )
    if wiki_pre:
        lines.extend(wiki_pre.splitlines())
        lines.append("")

    lines.append("{anchor:scenarios}")
    lines.append("h2. *3. Результаты тестирования*")
    lines.append("")

    lines.append("{anchor:overall_score}")
    lines.append("h3. *Общий score*")
    lines.append("")
    counts_by_label = column_status_counts or {}
    if data_labels:
        # One {chart} macro per build, stacked vertically — same approach as
        # the daily wiki report. Side-by-side layout requires PNG attachments
        # and is handled by the Confluence publisher (HTML -> storage).
        if has_best_column:
            lines.append(f"h4. {_wiki_escape(best_title)}")
            best_chart = _daily_status_chart_wiki_block(best_overall_counts)
            if best_chart:
                lines.extend(best_chart.splitlines())
            else:
                lines.append("_Нет данных по статусам_")
            lines.append("")
        for label in labels:
            counts = counts_by_label.get(label, {}) or {}
            lines.append(f"h4. {_wiki_escape(str(label))}")
            chart_block = _daily_status_chart_wiki_block(counts)
            if chart_block:
                lines.extend(chart_block.splitlines())
            else:
                lines.append("_Нет данных по статусам_")
            lines.append("")
    else:
        lines.append("_Нет данных по статусам_")
        lines.append("")

    lines.append("{anchor:scenario_score}")
    lines.append("h3. *Score по сценариям*")
    lines.append("")
    header_cells = ["Тестовый цикл", "Всего кейсов"]
    header_cells.extend(f"{_wiki_escape(label)}" for label in data_labels)
    lines.append("|| " + " || ".join(header_cells) + " ||")
    for row in rows:
        if str(row[0]).startswith("Итого:"):
            row_values = [f"*{row[0]}*", f"*{row[1]}*"]
            if has_best_column:
                row_values.append(f"*{best_scenario_map.get(str(row[0] or '').strip(), 0)}*")
            for idx in range(len(labels)):
                row_values.append(f"*{row[2 + idx] if 2 + idx < len(row) else '0'}*")
            lines.append("| " + " | ".join(_wiki_escape(str(value)) for value in row_values) + " |")
            continue
        row_values = [row[0], row[1]]
        if has_best_column:
            row_values.append(str(best_scenario_map.get(str(row[0] or "").strip(), 0)))
        for idx in range(len(labels)):
            row_values.append(row[2 + idx] if 2 + idx < len(row) else "0")
        lines.append(
            "| "
            + " | ".join(_wiki_escape(str(value)) for value in row_values)
            + " |"
        )
    if rows:
        detail_rows = [r for r in rows if not str(r[0]).startswith("Итого:")]
        total_cases_grand = sum(int(r[1]) for r in detail_rows if len(r) > 1)
        passed_grand: list[int] = []
        if has_best_column:
            passed_grand.append(
                sum(int(best_scenario_map.get(str(r[0] or "").strip(), 0)) for r in detail_rows)
            )
        passed_grand.extend(
            sum(int(r[2 + idx]) for r in detail_rows if 2 + idx < len(r))
            for idx in range(len(labels))
        )
        grand_values = [f"*Итого*", f"*{total_cases_grand}*"]
        for value in passed_grand:
            grand_values.append(f"*{value}*")
        lines.append(
            "| " + " | ".join(_wiki_escape(str(v)) for v in grand_values) + " |"
        )
    lines.append("")

    lines.append("{anchor:defects}")
    lines.append("h3. *Заведённые дефекты*")
    lines.append("")
    merged_analytics = _coalesce_weekly_defect_analytics(defect_analytics, defect_keys)
    if merged_analytics and (merged_analytics.get("keys_ordered") or []):
        defects_block = _weekly_defect_analytics_wiki(
            merged_analytics, defect_meta, labels
        )
    else:
        defects_block = _weekly_defects_wiki_block(defect_keys or [])
    if defects_block:
        lines.extend(defects_block.splitlines())
    lines.append("")
    return "\n".join(lines)


def write_weekly_readable_reports(
    output_dir: str,
    week_start: date | None,
    weekday_labels: list[str],
    rows: list[list[str]],
    formats: set[str],
    cell_all_not_executed: list[list[bool]] | None = None,
    cell_all_blocked: list[list[bool]] | None = None,
    *,
    column_status_counts: dict[str, dict[str, int]] | None = None,
    defect_keys: list[str] | None = None,
    cycle_keys_by_label: dict[str, list[dict[str, Any]]] | None = None,
    defect_analytics: dict[str, Any] | None = None,
    defect_meta: dict[str, dict[str, str]] | None = None,
    template_dir: str | None = None,
    folder_id_resolve: str | None = None,
    folder_id_mapping: str = "",
    folder_name_mapping: str = "",
    filename_suffix: str = "",
    best_branch_column: dict[str, Any] | None = None,
) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    week_label = week_start.isoformat() if week_start else "unknown_week"
    base_name = f"weekly_cycle_matrix_{week_label}{filename_suffix}"
    updated_paths: list[str] = []
    if "html" in formats:
        html_path = os.path.join(output_dir, f"{base_name}.html")
        html_body = render_weekly_html_report(
            week_start,
            weekday_labels,
            rows,
            cell_all_not_executed=cell_all_not_executed,
            cell_all_blocked=cell_all_blocked,
            column_status_counts=column_status_counts,
            defect_keys=defect_keys,
            cycle_keys_by_label=cycle_keys_by_label,
            defect_analytics=defect_analytics,
            defect_meta=defect_meta,
            template_dir=template_dir,
            folder_id_resolve=folder_id_resolve,
            folder_id_mapping=folder_id_mapping,
            folder_name_mapping=folder_name_mapping,
            best_branch_column=best_branch_column,
        )
        if _write_text_if_changed(html_path, html_body):
            updated_paths.append(html_path)
    if "wiki" in formats:
        wiki_path = os.path.join(output_dir, f"{base_name}.confluence.txt")
        wiki_body = render_weekly_wiki_report(
            week_start,
            weekday_labels,
            rows,
            column_status_counts=column_status_counts,
            defect_keys=defect_keys,
            defect_analytics=defect_analytics,
            defect_meta=defect_meta,
            template_dir=template_dir,
            folder_id_resolve=folder_id_resolve,
            folder_id_mapping=folder_id_mapping,
            folder_name_mapping=folder_name_mapping,
            best_branch_column=best_branch_column,
        )
        if _write_text_if_changed(wiki_path, wiki_body):
            updated_paths.append(wiki_path)
    return updated_paths


def _daily_sanitize_cycle_title(title: str) -> str:
    """Remove cloned markers from Zephyr cycle titles for daily readable output."""
    t = str(title or "").strip()
    if not t:
        return ""
    t = re.sub(r"\s*\((?:cloned|клонированный)\)\s*$", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"(?i)\s+\bcloned\s*$", "", t).strip()
    return t


def _daily_strip_cycle_title_suffix(title: str) -> str:
    return _daily_sanitize_cycle_title(title)


def _daily_progress_row_for_display(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["cycle_title"] = _daily_sanitize_cycle_title(str(out.get("cycle_title") or ""))
    return out


def _daily_display_cycle_index(cycle: dict[str, Any]) -> str | None:
    idx = _extract_cycle_index(cycle)
    if idx and re.match(r"^\d+\.\d+$", idx):
        return f"3.{idx}"
    return None


def _daily_cycle_anchor_id(display_index: str | None, cycle: dict[str, Any]) -> str:
    if display_index:
        return "cycle-" + display_index.replace(".", "-")
    slug = slugify(
        str(cycle.get("cycle_key") or cycle.get("cycle_name") or cycle.get("cycle_id") or "cycle")
    )
    return f"cycle-{slug}"


def _daily_cycle_heading_parts(cycle: dict[str, Any]) -> tuple[str, str]:
    display_idx = _daily_display_cycle_index(cycle)
    anchor = _daily_cycle_anchor_id(display_idx, cycle)
    heading = _daily_toc_child_label(cycle)
    return anchor, heading


def _daily_wiki_anchor_name(html_anchor_id: str) -> str:
    return html_anchor_id.replace("-", "_")


def _daily_cycle_to_label_row(cycle: dict[str, Any]) -> dict[str, Any]:
    cycle_title = str(cycle.get("cycle_name") or cycle.get("cycle_key") or cycle.get("cycle_id") or "")
    return {
        "cycle_title": _daily_sanitize_cycle_title(cycle_title),
        "cycle_index": _extract_cycle_index(cycle),
        "cycle_key": str(cycle.get("cycle_key") or ""),
    }


def _daily_toc_group_sort_key(gid: str) -> tuple[int, int | str]:
    if gid == "_other":
        return (2, 0)
    if gid.isdigit():
        return (0, int(gid))
    return (1, gid.lower())


def _daily_toc_groups_from_sorted_cycles(
    sorted_cycles: list[dict[str, Any]],
) -> list[tuple[str, str, list[dict[str, Any]]]]:
    """
    Group cycles by major index (first digit of X.Y). Returns
    (group_id, group_heading_plain, cycles) sorted like conclusion groups.
    """
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    order: list[str] = []
    for cycle in sorted_cycles:
        idx = _extract_cycle_index(cycle)
        m = re.match(r"^(\d+)\.(\d+)$", idx or "")
        gid = m.group(1) if m else "_other"
        if gid not in groups:
            order.append(gid)
            groups[gid] = []
        groups[gid].append(cycle)

    result: list[tuple[str, str, list[dict[str, Any]]]] = []
    for gid in sorted(order, key=_daily_toc_group_sort_key):
        group_cycles = groups[gid]
        rows = [_daily_cycle_to_label_row(c) for c in group_cycles]
        labels = [_build_summary_cycle_label(r) for r in rows]
        title = _summary_group_title_from_labels(labels, fallback_group=gid)
        if gid == "_other":
            heading = title
        else:
            heading = f"3.{gid} {title}"
        result.append((gid, heading, group_cycles))
    return result


def _daily_toc_child_label(cycle: dict[str, Any]) -> str:
    idx = _extract_cycle_index(cycle)
    raw_title = cycle.get("cycle_name") or cycle.get("cycle_key") or cycle.get("cycle_id") or ""
    stripped = _daily_sanitize_cycle_title(str(raw_title))
    stripped = re.sub(r"(?i)^test\s+cycle:\s*", "", stripped).strip()
    tail = stripped
    if idx and re.match(r"^\d+\.\d+$", idx):
        tail = re.sub(rf"^\s*{re.escape(idx)}\s+", "", tail).strip()
        tail = re.sub(r"^\s*\d+\.\d+\s+", "", tail).strip()
    if idx and re.match(r"^\d+\.\d+$", idx):
        return f"{idx} {tail}".strip() if tail else idx
    return tail or stripped or "Unnamed cycle"


def _daily_render_html_toc(sorted_cycles: list[dict[str, Any]]) -> str:
    groups = _daily_toc_groups_from_sorted_cycles(sorted_cycles)
    scenarios_anchor = "#scenarios"
    parts: list[str] = [
        "<nav class='report-toc'><p class='report-toc-title'><strong>Оглавление</strong></p><ul>",
        "<li><a href='#sec-object'><strong>1. Объект тестирования</strong></a></li>",
        "<li><a href='#sec-environment'><strong>2. Условия окружения</strong></a></li>",
        f"<li><a href='{html.escape(scenarios_anchor, quote=True)}'><strong>3. Результаты тестирования</strong></a></li>",
    ]
    for _gid, heading, group_cycles in groups:
        first = group_cycles[0]
        anchor, _ = _daily_cycle_heading_parts(first)
        parts.append(
            "<li>"
            f"<a href='#{html.escape(anchor, quote=True)}'>"
            f"&nbsp;&nbsp;&nbsp;&nbsp;{html.escape(heading)}</a>"
            "</li>"
        )
    parts.append("<li><a href='#conclusion'><strong>4. Заключение</strong></a></li>")
    parts.append("</ul></nav>")
    return "".join(parts)


def _daily_aggregate_case_status_counts(cycles: dict[str, Any]) -> dict[str, int]:
    counts: defaultdict[str, int] = defaultdict(int)
    for cycle in cycles.values():
        for case in cycle.get("cases", {}).values():
            counts[normalize_status(case.get("result", case.get("test_case_status", "")))] += 1
    return dict(counts)


def _daily_aggregate_case_status_counts_for_cycle(cycle: dict[str, Any]) -> dict[str, int]:
    return _daily_aggregate_case_status_counts({"_": cycle})


def _daily_global_passed_total(cycles: dict[str, Any]) -> tuple[int, int]:
    total_y = 0
    total_x = 0
    for cycle in cycles.values():
        for case in cycle.get("cases", {}).values():
            total_y += 1
            if normalize_status(case.get("result", case.get("test_case_status", ""))) == "passed":
                total_x += 1
    return total_x, total_y


def _daily_scenario_group_lines(progress_rows: list[dict[str, Any]]) -> list[str]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in progress_rows:
        g = _summary_scenario_group(row)
        if not g:
            g = "_other"
        groups[g].append(row)

    def gid_sort(gid: str) -> tuple[int, int | str]:
        if gid == "_other":
            return (2, 0)
        if gid.isdigit():
            return (0, int(gid))
        return (1, gid.lower())

    lines: list[str] = []
    for gid in sorted(groups.keys(), key=gid_sort):
        group_rows = groups[gid]
        labels = [_build_summary_cycle_label(r) for r in group_rows]
        title = _summary_group_title_from_labels(labels, fallback_group=gid)
        passed_sum = sum(int(r["passed_cases"]) for r in group_rows)
        total_sum = sum(int(r["total_cases"]) for r in group_rows)
        lines.append(f"{title} - выполняется {passed_sum}/{total_sum}")
    return lines


def _daily_status_pie_svg(
    counts: dict[str, int],
    *,
    size: int = 220,
    extra_wrap_class: str = "",
) -> str:
    order: list[tuple[str, str, str]] = [
        ("passed", "#33c24d", "Пройден"),
        ("failed", "#e53935", "Не пройден"),
        ("not_executed", "#c9c9c2", "Не выполнен"),
        ("blocked", "#4a90e2", "Заблокирован"),
        ("other", "#adb5bd", "Прочее"),
    ]
    total = sum(counts.get(k, 0) for k, _, _ in order)
    if total <= 0:
        return "<p class='pie-empty'>Нет данных по статусам</p>"
    cx = cy = size / 2
    r = size / 2 - 12
    angle = -math.pi / 2
    paths: list[str] = []
    for key, color, _lbl in order:
        val = counts.get(key, 0)
        if val <= 0:
            continue
        slice_angle = 2 * math.pi * val / total
        x0 = cx + r * math.cos(angle)
        y0 = cy + r * math.sin(angle)
        x1 = cx + r * math.cos(angle + slice_angle)
        y1 = cy + r * math.sin(angle + slice_angle)
        large_arc = 1 if slice_angle > math.pi else 0
        paths.append(
            f"<path d='M {cx:.2f} {cy:.2f} L {x0:.2f} {y0:.2f} A {r:.2f} {r:.2f} 0 {large_arc} 1 "
            f"{x1:.2f} {y1:.2f} Z' fill='{html.escape(color)}' stroke='#ffffff' stroke-width='1'/>"
        )
        angle += slice_angle
    legend_items: list[str] = []
    for key, color, label in order:
        val = counts.get(key, 0)
        if val <= 0:
            continue
        pct = 100.0 * val / total
        legend_items.append(
            "<span class='pie-legend-item'><span class='pie-swatch' "
            f"style='background:{html.escape(color)}'></span>"
            f"{html.escape(label)}: {val} ({pct:.1f}%)</span>"
        )
    legend = "<div class='pie-legend'>" + " ".join(legend_items) + "</div>"
    svg_inner = "\n".join(paths)
    wrap_class = "daily-pie-wrap"
    ext = extra_wrap_class.strip()
    if ext:
        wrap_class = f"{wrap_class} {ext}"
    return (
        f"<div class='{wrap_class}'><svg xmlns='http://www.w3.org/2000/svg' "
        f"width='{size}' height='{size}' viewBox='0 0 {size} {size}' role='img' "
        f"aria-label='Распределение статусов по кейсам'>{svg_inner}</svg>{legend}</div>"
    )


def _daily_status_summary_lines(counts: dict[str, int]) -> list[str]:
    ordered: list[tuple[str, str]] = [
        ("passed", "Пройден"),
        ("failed", "Не пройден"),
        ("not_executed", "Не выполнен"),
        ("blocked", "Заблокирован"),
        ("other", "Прочее"),
    ]
    total = sum(int(counts.get(key, 0)) for key, _ in ordered)
    if total <= 0:
        return ["Статусы: нет данных"]
    lines: list[str] = []
    for key, label in ordered:
        value = int(counts.get(key, 0))
        if value <= 0:
            continue
        pct = (value / total) * 100.0
        lines.append(f"{label}: {value} ({pct:.1f}%)")
    return lines


def _daily_status_chart_wiki_block(counts: dict[str, int]) -> str:
    """Confluence wiki: table + native Chart macro (pie)."""
    ordered: list[tuple[str, str]] = [
        ("passed", "Пройден"),
        ("failed", "Не пройден"),
        ("not_executed", "Не выполнен"),
        ("blocked", "Заблокирован"),
        ("other", "Прочее"),
    ]
    row_lines: list[str] = []
    for key, label in ordered:
        value = int(counts.get(key, 0))
        if value <= 0:
            continue
        row_lines.append(f"| {_wiki_escape(label)} | {value} |")
    if not row_lines:
        return ""
    title = _wiki_escape("Распределение статусов (по кейсам)")
    header = "|| Категория || Количество ||"
    table_block = "\n".join([header] + row_lines)
    return (
        f"{{chart:type=pie|title={title}|width=420|height=320}}\n"
        f"{table_block}\n"
        f"{{chart}}"
    )


def _daily_zephyr_normalize_cycle_value_objects(
    cycle_objects: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for obj in cycle_objects:
        oid = str(obj.get("id") or "").strip()
        if not oid:
            continue
        name = str(obj.get("name") or "").strip()
        if name:
            normalized.append({"id": oid, "name": name})
        else:
            normalized.append({"id": oid})
    return normalized


def _daily_zephyr_build_conditions_info(
    cycle_value_objects: list[dict[str, Any]],
    *,
    project_id: int,
    project_key: str,
    project_display_name: str,
) -> dict[str, Any]:
    """conditionsInfo JSON for Zephyr Reporting TEST_RESULTS_SUMMARY_BY_STATUS (storage)."""
    proj_entry = {
        "id": project_key,
        "name": project_display_name,
        "projectId": project_id,
    }

    def _exec_between(alias: str = "testResult") -> dict[str, Any]:
        return {
            "alias": alias,
            "field": "executionDate",
            "comparisonOperator": "between",
        }

    def _last_result(alias: str) -> dict[str, Any]:
        return {
            "alias": alias,
            "field": "onlyLastTestResult",
            "comparisonOperator": "IS",
            "value": {"id": True},
        }

    return {
        "filterByOption": "FILTER_BY_TEST_RUN",
        "projectCondition": {
            "alias": "testResult",
            "field": "projectId",
            "comparisonOperator": "IN",
            "value": [proj_entry],
        },
        "noneCondition": {
            "testResultExecutionDateCondition": _exec_between(),
            "lastTestResultCondition": _last_result("testCase"),
        },
        "testRunCondition": {
            "testRunKeyCondition": {
                "alias": "testRun",
                "field": "key",
                "comparisonOperator": "IN",
                "value": cycle_value_objects,
            },
            "testResultExecutionDateCondition": _exec_between(),
            "lastTestResultCondition": _last_result("testRun"),
        },
        "testPlanCondition": {
            "testPlanKeyCondition": {
                "alias": "testPlan",
                "field": "key",
                "comparisonOperator": "IN",
            },
            "testResultExecutionDateCondition": _exec_between(),
            "lastTestResultCondition": _last_result("testPlan"),
        },
        "iterationCondition": {
            "testRunIterationCondition": {
                "alias": "testRun",
                "field": "iterationName",
                "comparisonOperator": "IN",
            },
            "testResultExecutionDateCondition": _exec_between(),
            "lastTestResultCondition": _last_result("testRun"),
        },
        "versionCondition": {
            "testRunVersionCondition": {
                "alias": "testRun",
                "field": "versionId",
                "comparisonOperator": "IN",
            },
            "testResultExecutionDateCondition": _exec_between(),
            "lastTestResultCondition": _last_result("testRun"),
        },
        "folderCondition": {
            "testCaseFolderCondition": {
                "alias": "testCase",
                "field": "folderName",
                "options": {},
                "comparisonOperator": "IN",
            },
            "testResultExecutionDateCondition": _exec_between(),
            "lastTestResultCondition": _last_result("testCase"),
        },
        "issueCondition": {
            "favoriteFilterCondition": {
                "alias": "issue",
                "queryLanguage": "JQL",
                "field": "favoriteFilter",
            },
            "lastTestResultCondition": _last_result("testCase"),
        },
        "epicCondition": {
            "favoriteFilterCondition": {
                "alias": "epic",
                "queryLanguage": "JQL",
                "field": "favoriteFilter",
            },
            "lastTestResultCondition": _last_result("testCase"),
        },
        "customCondition": {
            "testCaseConditions": [],
            "testRunConditions": [],
            "testPlanConditions": [],
            "testResultConditions": [],
            "lastTestResultCondition": _last_result("testRun"),
        },
        "traceabilityCustomTreeCondition": {
            "lastTestResultCondition": _last_result("testCase"),
        },
    }


def _daily_zephyr_test_results_summary_storage_macro(
    cycle_objects: list[dict[str, Any]],
) -> str:
    """Confluence storage XML: Zephyr Reporting macro replacing Chart on publish."""
    normalized = _daily_zephyr_normalize_cycle_value_objects(cycle_objects)
    if not normalized:
        return ""

    app_id = (
        os.getenv("ZEPHYR_CONFLUENCE_ZEPHYR_APP_ID")
        or "7eb1ea68-9ea9-315e-abbf-042a60da5b3b"
    ).strip()
    project_id_raw = (os.getenv("ZEPHYR_PROJECT_ID") or "10904").strip()
    try:
        project_id = int(project_id_raw)
    except ValueError:
        project_id = 10904
    project_key = (os.getenv("ZEPHYR_JIRA_PROJECT_KEY") or "QA").strip()
    project_name = (os.getenv("ZEPHYR_JIRA_PROJECT_DISPLAY_NAME") or "T&V").strip()

    settings = {
        "displayUnit": "COUNT",
        "traceabilityReportOption": "COVERAGE_TEST_CASES",
        "traceabilityTreeOption": "COVERAGE_TEST_CASES",
        "traceabilityCustomTreeDisplayOption": "CONDENSED",
        "traceabilityMatrixOption": "COVERAGE_TEST_CASES",
        "period": "MONTH",
        "scorecardOption": "EXECUTION_RESULTS",
    }
    conditions = _daily_zephyr_build_conditions_info(
        normalized,
        project_id=project_id,
        project_key=project_key,
        project_display_name=project_name,
    )

    settings_json = json.dumps(settings, ensure_ascii=False, separators=(",", ":"))
    conditions_json = json.dumps(conditions, ensure_ascii=False, separators=(",", ":"))
    extra_json = "{}"

    return (
        '<ac:structured-macro ac:name="TEST_RESULTS_SUMMARY_BY_STATUS" '
        'ac:schema-version="1">'
        f'<ac:parameter ac:name="settings">{html.escape(settings_json, quote=True)}</ac:parameter>'
        f'<ac:parameter ac:name="appId">{html.escape(app_id, quote=True)}</ac:parameter>'
        f'<ac:parameter ac:name="conditionsInfo">{html.escape(conditions_json, quote=True)}</ac:parameter>'
        f'<ac:parameter ac:name="extraParams">{html.escape(extra_json, quote=True)}</ac:parameter>'
        '<ac:parameter ac:name="reportKey">TEST_RESULTS_SUMMARY_BY_STATUS</ac:parameter>'
        "</ac:structured-macro>"
    )


def _daily_status_chart_storage_macro(counts: dict[str, int]) -> str:
    """Confluence storage XML: Chart macro (pie) + data table for REST publish."""
    ordered: list[tuple[str, str, str]] = [
        ("passed", "Пройден", "#33c24d"),
        ("failed", "Не пройден", "#e53935"),
        ("not_executed", "Не выполнен", "#c9c9c2"),
        ("blocked", "Заблокирован", "#4a90e2"),
        ("other", "Прочее", "#adb5bd"),
    ]
    row_cells: list[str] = []
    colors: list[str] = []
    for key, label, color in ordered:
        value = int(counts.get(key, 0))
        if value <= 0:
            continue
        colors.append(color)
        row_cells.append(
            "<tr>"
            f"<td>{html.escape(label)}</td>"
            f"<td>{value}</td>"
            "</tr>"
        )
    if not row_cells:
        return ""
    title = html.escape("Распределение статусов (по кейсам)")
    header_row = (
        "<tr><th>Категория</th><th>Количество</th></tr>"
    )
    table = (
        "<table><tbody>"
        + header_row
        + "".join(row_cells)
        + "</tbody></table>"
    )
    colors_csv = html.escape(",".join(colors))
    return (
        '<ac:structured-macro ac:name="chart">'
        '<ac:parameter ac:name="type">pie</ac:parameter>'
        f'<ac:parameter ac:name="title">{title}</ac:parameter>'
        f'<ac:parameter ac:name="colors">{colors_csv}</ac:parameter>'
        '<ac:parameter ac:name="dataOrientation">vertical</ac:parameter>'
        '<ac:parameter ac:name="legend">true</ac:parameter>'
        '<ac:parameter ac:name="pieSectionLabel"></ac:parameter>'
        f"<ac:rich-text-body>{table}</ac:rich-text-body>"
        "</ac:structured-macro>"
    )


def _daily_status_palette() -> list[tuple[str, tuple[int, int, int]]]:
    return [
        ("passed", (51, 194, 77)),
        ("failed", (229, 57, 53)),
        ("not_executed", (201, 201, 194)),
        ("blocked", (74, 144, 226)),
        ("other", (173, 181, 189)),
    ]


def _write_daily_pie_png(path: str, counts: dict[str, int], *, size: int = 240) -> bool:
    ordered = _daily_status_palette()
    total = sum(counts.get(key, 0) for key, _ in ordered)
    if total <= 0:
        return False

    cx = cy = size // 2
    radius = (size // 2) - 8
    inner_radius = 0
    img = bytearray([255] * (size * size * 3))

    slices: list[tuple[float, float, tuple[int, int, int]]] = []
    angle = -math.pi / 2
    for key, color in ordered:
        value = counts.get(key, 0)
        if value <= 0:
            continue
        portion = 2 * math.pi * value / total
        slices.append((angle, angle + portion, color))
        angle += portion

    for y in range(size):
        dy = y - cy
        for x in range(size):
            dx = x - cx
            dist2 = dx * dx + dy * dy
            if dist2 > radius * radius or dist2 < inner_radius * inner_radius:
                continue
            a = math.atan2(dy, dx)
            # Normalize to [start, start+2pi) window
            if a < -math.pi / 2:
                a += 2 * math.pi
            for start, end, color in slices:
                adj_end = end
                if adj_end < start:
                    adj_end += 2 * math.pi
                aa = a
                if aa < start:
                    aa += 2 * math.pi
                if start <= aa <= adj_end:
                    idx = (y * size + x) * 3
                    img[idx : idx + 3] = bytes(color)
                    break

    # White separators for cleaner slices.
    for y in range(size):
        dy = y - cy
        for x in range(size):
            dx = x - cx
            d = math.sqrt(dx * dx + dy * dy)
            if abs(d - radius) <= 0.7:
                idx = (y * size + x) * 3
                img[idx : idx + 3] = b"\xff\xff\xff"

    rows = bytearray()
    stride = size * 3
    for y in range(size):
        rows.append(0)  # no filter
        start = y * stride
        rows.extend(img[start : start + stride])

    def _chunk(tag: bytes, payload: bytes) -> bytes:
        return (
            len(payload).to_bytes(4, "big")
            + tag
            + payload
            + zlib.crc32(tag + payload).to_bytes(4, "big")
        )

    signature = b"\x89PNG\r\n\x1a\n"
    ihdr = _chunk(
        b"IHDR",
        size.to_bytes(4, "big")
        + size.to_bytes(4, "big")
        + b"\x08\x02\x00\x00\x00",
    )
    idat = _chunk(b"IDAT", zlib.compress(bytes(rows), level=9))
    iend = _chunk(b"IEND", b"")
    payload = signature + ihdr + idat + iend
    if os.path.exists(path):
        try:
            with open(path, "rb") as current:
                if current.read() == payload:
                    return False
        except OSError:
            pass
    with open(path, "wb") as target:
        target.write(payload)
    return True


def _load_daily_confluence_execution_macro(template_dir: str | None) -> str:
    env_val = (os.getenv("ZEPHYR_CONFLUENCE_TEST_EXEC_MACRO") or "").strip()
    if env_val:
        return env_val
    candidates: list[str] = []
    if template_dir:
        candidates.append(
            os.path.join(template_dir, "daily", "wiki", "confluence_execution_macro.txt")
        )
    here = os.path.dirname(os.path.abspath(__file__))
    candidates.append(
        os.path.join(here, "report_templates", "readable", "daily", "wiki", "confluence_execution_macro.txt")
    )
    for path in candidates:
        if path and os.path.isfile(path):
            try:
                with open(path, encoding="utf-8") as f:
                    text = f.read().strip()
            except OSError:
                continue
            if text:
                return text
    return ""


def _daily_cycle_keys_from_cycles(cycles: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for cycle in sorted(cycles.values(), key=_cycle_sort_key):
        cycle_key = str(cycle.get("cycle_key") or "").strip()
        if not cycle_key or cycle_key in seen:
            continue
        seen.add(cycle_key)
        keys.append(cycle_key)
    return keys


def _render_daily_confluence_execution_macro(
    template_dir: str | None, folder_name: str, cycles: dict[str, Any]
) -> str:
    """
    Render optional Zephyr macro text with cycle placeholders.

    Supported placeholders in env/template macro text:
    - {CYCLE_KEYS_CSV}  -> QA-1,QA-2,QA-3
    - {CYCLE_KEYS_PIPE} -> QA-1|QA-2|QA-3
    - {CYCLE_KEYS_JSON} -> ["QA-1","QA-2","QA-3"]
    - {CYCLE_KEYS_OBJECTS_JSON} -> [{"id":"QA-1"},{"id":"QA-2"}]
    - {FOLDER_NAME}     -> raw folder name from current report
    """
    macro = _load_daily_confluence_execution_macro(template_dir)
    if not macro:
        return ""
    cycle_keys = _daily_cycle_keys_from_cycles(cycles)
    cycle_key_objects_json = json.dumps(
        [{"id": key} for key in cycle_keys], ensure_ascii=False, separators=(",", ":")
    )
    repl = {
        "{CYCLE_KEYS_CSV}": ",".join(cycle_keys),
        "{CYCLE_KEYS_PIPE}": "|".join(cycle_keys),
        "{CYCLE_KEYS_JSON}": json.dumps(cycle_keys, ensure_ascii=False),
        "{CYCLE_KEYS_OBJECTS_JSON}": cycle_key_objects_json,
        "{FOLDER_NAME}": folder_name,
    }
    out = macro
    for token, value in repl.items():
        out = out.replace(token, value)
    return out


def render_daily_html_report(
    folder_name: str,
    cycles: dict[str, Any],
    *,
    folder_id: str,
    template_dir: str | None = None,
) -> str:
    doc_title = _daily_document_title(folder_name)
    preamble = _format_readable_html_preamble(
        template_dir,
        "daily",
        folder_id,
        folder_id,
        folder_name,
        None,
    )
    sorted_cycles = sorted(cycles.values(), key=_cycle_sort_key)
    status_counts = _daily_aggregate_case_status_counts(cycles)
    toc_html = _daily_render_html_toc(sorted_cycles)
    sections = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        f"<title>{html.escape(doc_title)}</title>",
        (
            "<style>"
            "body{font-family:Arial,sans-serif;margin:24px;}"
            "h1{margin-bottom:8px;}h2{margin-top:24px;margin-bottom:8px;font-weight:700;}"
            "table{border-collapse:collapse;width:100%;margin-bottom:16px;table-layout:fixed;}"
            "th,td{border:1px solid #d6d6d6;padding:6px 8px;text-align:left;vertical-align:top;overflow-wrap:anywhere;word-wrap:break-word;}"
            "th{background:#f0f2f5;font-weight:600;}"
            ".report-preamble{margin:12px 0 20px;}"
            ".report-toc{border:1px solid #e1e4e8;padding:12px 16px;margin:16px 0;background:#fafbfc;border-radius:6px;}"
            ".report-toc ul{margin:8px 0;padding-left:20px;}"
            ".report-toc a{text-decoration:none;color:#0969da;}"
            ".report-toc a:hover{text-decoration:underline;}"
            ".report-toc-title{margin:0 0 8px;font-size:1rem;}"
            ".report-toc>li{margin:6px 0;}"
            ".report-toc a.report-toc-main-link{font-weight:700;}"
            ".report-toc a.report-toc-group-link{font-weight:600;color:#0969da;}"
            ".report-toc a.report-toc-sub-link{display:inline-block;margin-left:20px;}"
            ".status-badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:12px;font-weight:700;line-height:1.4;}"
            ".st-pass{background:#33c24d;color:#ffffff;}"
            ".st-fail{background:#e53935;color:#ffffff;}"
            ".st-not-executed{background:#c9c9c2;color:#2f2f2f;}"
            ".st-in-progress{background:#f0ad4e;color:#2f2f2f;}"
            ".st-blocked{background:#4a90e2;color:#ffffff;}"
            ".st-cant-test{background:#9c27ff;color:#ffffff;}"
            ".st-not-tested-pi{background:#8d7cc3;color:#ffffff;}"
            ".st-danger{background:#4f6078;color:#ffffff;}"
            ".st-cant-reproduce{background:#f08f78;color:#2f2f2f;}"
            ".st-false-positive{background:#ecd96b;color:#2f2f2f;}"
            ".st-unknown{background:#f4f5f7;color:#172b4d;}"
            ".daily-pie-wrap{display:flex;flex-wrap:wrap;align-items:flex-start;gap:16px;margin:12px 0;}"
            ".pie-legend{display:flex;flex-wrap:wrap;gap:12px;align-items:center;font-size:13px;max-width:520px;}"
            ".pie-swatch{display:inline-block;width:12px;height:12px;border-radius:2px;margin-right:6px;vertical-align:middle;}"
            ".pie-empty{color:#666;margin:8px 0;}"
            ".daily-conclusion-score{font-size:1.05rem;font-weight:700;margin:8px 0 12px;}"
            ".daily-scenario-line{font-weight:700;margin:4px 0;}"
            ".daily-tab{margin:0 0 8px;white-space:pre;}"
            ".scenario-result-cell{vertical-align:middle;text-align:center;}"
            ".scenario-result-cell .daily-pie-wrap{justify-content:center;margin:0 auto;}"
            ".scenario-result-cell .pie-legend{font-size:11px;gap:6px;max-width:280px;}"
            ".daily-jira-key{display:inline-block;font-family:ui-monospace,Consolas,monospace;"
            "font-size:12px;font-weight:600;color:#0052cc;text-decoration:none;background:#e9f2ff;"
            "padding:1px 6px;margin:1px 2px 1px 0;border-radius:3px;border:1px solid #b3d4ff;}"
            ".daily-jira-key:hover{text-decoration:underline;}"
            "</style>"
        ),
        "</head><body>",
        toc_html,
    ]
    sections.append("<p class='daily-tab'>\t</p>")
    if preamble:
        sections.append(preamble)
    sections.append("<h2 id='scenarios'><strong>3. Результаты тестирования</strong></h2>")
    sections.append("<p class='daily-tab'>\t</p>")
    sections.append(
        _daily_status_pie_svg(
            status_counts, extra_wrap_class="daily-pie-strip-publish"
        )
    )
    for cycle in sorted_cycles:
        anchor, heading_plain = _daily_cycle_heading_parts(cycle)
        heading_display = re.sub(r"^\s*\d+\.\d+\s+", "", heading_plain).strip() or heading_plain
        sections.append(
            f"<h2 id='{html.escape(anchor, quote=True)}'><strong>{html.escape(heading_display)}</strong></h2>"
        )
        sections.append(
            "<table>"
            "<colgroup>"
            "<col style='width:17%'>"
            "<col style='width:22%'>"
            "<col style='width:9%'>"
            "<col style='width:8%'>"
            "<col style='width:14%'>"
            "<col style='width:10%'>"
            "<col style='width:12%'>"
            "</colgroup>"
            "<thead><tr>"
            "<th>Название</th><th>Критерий валидации</th><th>Тестовый прогон</th>"
            "<th>Статус</th><th>Результат</th><th>Комментарий</th><th>Задачи</th>"
            "</tr></thead><tbody>"
        )
        sorted_cases, criterion_spans = _prepare_cycle_cases_with_groups(cycle)
        cycle_key_value = str(cycle.get("cycle_key") or "")
        cycle_cell_html = html.escape(cycle_key_value)
        if cycle_key_value:
            cycle_url = _jira_cycle_url(cycle_key_value)
            cycle_cell_html = (
                f"<a href='{html.escape(cycle_url, quote=True)}' target='_blank' rel='noopener'>"
                f"{html.escape(cycle_key_value)}</a>"
            )
        cycle_counts = _daily_aggregate_case_status_counts_for_cycle(cycle)
        n_rows = len(sorted_cases)
        if n_rows == 0:
            marker = (
                "<span class='scenario-result-macro-marker' "
                f"data-cycle-key='{html.escape(cycle_key_value, quote=True)}' "
                f"data-cycle-name='{html.escape(str(cycle.get('cycle_name') or ''), quote=True)}'></span>"
            )
            sections.append(
                "<tr>"
                "<td colspan='7'>Нет кейсов в этом цикле</td>"
                "<td class='scenario-result-cell'>"
                f"{marker}"
                f"{_daily_status_pie_svg(cycle_counts, size=168)}"
                "</td>"
                "</tr>"
            )
        else:
            for idx, case in enumerate(sorted_cases):
                result_value = case.get("result", case.get("test_case_status", ""))
                criterion_cell = ""
                if criterion_spans[idx] > 0:
                    criterion_text = case.get("_criterion_display", "")
                    criterion_cell = (
                        f"<td rowspan='{criterion_spans[idx]}'>"
                        f"{_html_comment_cell(criterion_text)}</td>"
                    )
                result_col = ""
                if idx == 0:
                    marker = (
                        "<span class='scenario-result-macro-marker' "
                        f"data-cycle-key='{html.escape(cycle_key_value, quote=True)}' "
                        f"data-cycle-name='{html.escape(str(cycle.get('cycle_name') or ''), quote=True)}'></span>"
                    )
                    result_col = (
                        "<td "
                        f"rowspan='{n_rows}' class='scenario-result-cell'>"
                        f"{marker}"
                        f"{_daily_status_pie_svg(cycle_counts, size=168)}"
                        "</td>"
                    )
                sections.append(
                    "<tr>"
                    f"<td>{html.escape(case['test_case_name'])}</td>"
                    f"{criterion_cell}"
                    f"<td>{cycle_cell_html}</td>"
                    f"<td>{_status_badge_html(result_value)}</td>"
                    f"{result_col}"
                    f"<td>{_html_comment_cell(case.get('comment', ''))}</td>"
                    f"<td>{_html_tasks_cell(case.get('tasks', ''))}</td>"
                    "</tr>"
                )
        sections.append("</tbody></table>")
    progress_rows_raw = sorted(_build_cycle_progress_rows(cycles), key=_summary_sort_key)
    progress_rows = [_daily_progress_row_for_display(r) for r in progress_rows_raw]
    sections.append("<h2 id='summary'><strong>Сводка по тестовым циклам</strong></h2>")
    sections.append(
        "<table>"
        "<thead><tr>"
        "<th>Тестовый цикл</th><th>Всего кейсов</th><th>Пройдено кейсов</th>"
        "</tr></thead><tbody>"
    )
    previous_group = ""
    for row in progress_rows:
        current_group = _summary_scenario_group(row)
        if previous_group and current_group and current_group != previous_group:
            sections.append("<tr class='scenario-sep'><td colspan='3'></td></tr>")
        cycle_label = _build_summary_cycle_label(row)
        passed_cases = int(row["passed_cases"])
        all_ne = bool(row.get("all_not_executed", False))
        all_blocked = bool(row.get("all_blocked", False))
        bg_color = _passed_count_color(
            passed_cases, all_not_executed=all_ne, all_blocked=all_blocked
        )
        text_color = _passed_count_text_color(
            passed_cases, all_not_executed=all_ne, all_blocked=all_blocked
        )
        sections.append(
            "<tr>"
            f"<td>{html.escape(cycle_label)}</td>"
            f"<td>{row['total_cases']}</td>"
            f"<td class='passed-count-cell' style='background:{bg_color};color:{text_color};'>{passed_cases}</td>"
            "</tr>"
        )
        previous_group = current_group or previous_group
    sections.append("</tbody></table>")
    total_x, total_y = _daily_global_passed_total(cycles)
    status_summary_lines = _daily_status_summary_lines(status_counts)
    scenario_lines = _daily_scenario_group_lines(progress_rows)
    sections.append("<h2 id='conclusion'><strong>4. Заключение</strong></h2>")
    sections.append(
        "<p class='daily-conclusion-score'>"
        f"Итоговый score nightly-dev-{html.escape(folder_name)}, {total_x}/{total_y}</p>"
    )
    _counts_json = html.escape(
        json.dumps(status_counts, ensure_ascii=True, sort_keys=True)
    )
    sections.append(
        f'<div id="zephyr-status-counts-json" style="display:none">{_counts_json}</div>'
    )
    cycle_objs_for_macro: list[dict[str, Any]] = []
    for cycle in sorted_cycles:
        ck = str(cycle.get("cycle_key") or "").strip()
        if not ck:
            continue
        cn = str(cycle.get("cycle_name") or "").strip()
        if cn:
            cycle_objs_for_macro.append({"id": ck, "name": cn})
        else:
            cycle_objs_for_macro.append({"id": ck})
    _cycle_keys_json = html.escape(
        json.dumps(cycle_objs_for_macro, ensure_ascii=True)
    )
    sections.append(
        f'<div id="zephyr-cycle-keys-json" style="display:none">{_cycle_keys_json}</div>'
    )
    for line in status_summary_lines:
        sections.append(f"<p class='daily-scenario-line'>{html.escape(line)}</p>")
    for line in scenario_lines:
        sections.append(f"<p class='daily-scenario-line'>{html.escape(line)}</p>")
    sections.append("</body></html>")
    return "\n".join(sections)


def render_daily_wiki_report(
    folder_name: str,
    cycles: dict[str, Any],
    *,
    folder_id: str,
    template_dir: str | None = None,
) -> str:
    sorted_cycles = sorted(cycles.values(), key=_cycle_sort_key)
    status_counts = _daily_aggregate_case_status_counts(cycles)
    grouped_cycles = _daily_toc_groups_from_sorted_cycles(sorted_cycles)
    toc_lines = ["h3. Оглавление"]
    toc_lines.append(f"* [{_wiki_escape('1. Объект тестирования')}|#sec_object]")
    toc_lines.append(f"* [{_wiki_escape('2. Условия окружения')}|#sec_environment]")
    toc_lines.append(f"* [{_wiki_escape('3. Результаты тестирования')}|#scenarios]")
    for _gid, heading, group_cycles in grouped_cycles:
        first = group_cycles[0]
        anchor, _ = _daily_cycle_heading_parts(first)
        w_anchor = _daily_wiki_anchor_name(anchor)
        toc_lines.append(f"** [{_wiki_escape(heading)}|#{w_anchor}]")
    toc_lines.append(f"* [{_wiki_escape('4. Заключение')}|#conclusion]")
    lines: list[str] = []
    lines.extend(toc_lines)
    lines.append("")
    lines.append("\t")
    wiki_pre = _format_readable_wiki_preamble(
        template_dir,
        "daily",
        folder_id,
        folder_id,
        folder_name,
        None,
    )
    if wiki_pre:
        lines.extend(wiki_pre.splitlines())
        lines.append("")
    lines.append("{anchor:scenarios}")
    lines.append("h2. *3. Результаты тестирования*")
    lines.append("")
    chart_section = _daily_status_chart_wiki_block(status_counts)
    if chart_section:
        lines.append("\t")
        lines.extend(chart_section.splitlines())
        lines.append("")
    for cycle in sorted_cycles:
        anchor, heading_plain = _daily_cycle_heading_parts(cycle)
        heading_display = re.sub(r"^\s*\d+\.\d+\s+", "", heading_plain).strip() or heading_plain
        w_anchor = _daily_wiki_anchor_name(anchor)
        lines.append(f"{{anchor:{w_anchor}}}")
        lines.append(f"h2. *{_wiki_escape(heading_display)}*")
        cycle_key_value = str(cycle.get("cycle_key") or "")
        cycle_cell_wiki = _wiki_escape(cycle_key_value)
        if cycle_key_value:
            cycle_url = _jira_cycle_url(cycle_key_value)
            cycle_cell_wiki = f"[{cycle_key_value}|{cycle_url}]"
        lines.append(
            "|| Название || Критерий валидации || Тестовый прогон || Статус || Результат || Комментарий || Задачи ||"
        )
        sorted_cases, criterion_spans = _prepare_cycle_cases_with_groups(cycle)
        for idx, case in enumerate(sorted_cases):
            res = case.get("result", case.get("test_case_status", ""))
            cmt = _wiki_text_with_links(case.get("comment", ""))
            tasks = _wiki_tasks_cell(case.get("tasks", ""))
            criterion_cell_wiki = ""
            if criterion_spans[idx] > 0:
                criterion_cell_wiki = _wiki_escape(case.get("_criterion_display", ""))
            lines.append(
                "| "
                + " | ".join(
                    [
                        _wiki_escape(case["test_case_name"]),
                        criterion_cell_wiki,
                        cycle_cell_wiki,
                        _wiki_status_markup(res),
                        "",
                        cmt,
                        tasks,
                    ]
                )
                + " |"
            )
        cycle_chart_wiki = _daily_status_chart_wiki_block(
            _daily_aggregate_case_status_counts_for_cycle(cycle)
        )
        if cycle_chart_wiki:
            lines.append("")
            lines.extend(cycle_chart_wiki.splitlines())
        lines.append("")
    progress_rows_raw = sorted(_build_cycle_progress_rows(cycles), key=_summary_sort_key)
    progress_rows = [_daily_progress_row_for_display(r) for r in progress_rows_raw]
    lines.append("{anchor:summary}")
    lines.append("h2. Сводка по тестовым циклам")
    lines.append("|| Тестовый цикл || Всего кейсов || Пройдено кейсов ||")
    previous_group = ""
    for row in progress_rows:
        current_group = _summary_scenario_group(row)
        if previous_group and current_group and current_group != previous_group:
            lines.append("")
        cycle_label = _build_summary_cycle_label(row)
        passed_cases = int(row["passed_cases"])
        lines.append(
            "| "
            + " | ".join(
                [
                    _wiki_escape(cycle_label),
                    str(row["total_cases"]),
                    str(passed_cases),
                ]
            )
            + " |"
        )
        previous_group = current_group or previous_group
    lines.append("")
    total_x, total_y = _daily_global_passed_total(cycles)
    status_summary_lines = _daily_status_summary_lines(status_counts)
    scenario_lines = _daily_scenario_group_lines(progress_rows)
    macro_body = _render_daily_confluence_execution_macro(template_dir, folder_name, cycles)
    lines.append("{anchor:conclusion}")
    lines.append("h2. 4. Заключение")
    lines.append("")
    lines.append(
        f"*Итоговый score nightly-dev-{_wiki_escape(folder_name)}, {total_x}/{total_y}*"
    )
    if macro_body:
        lines.append("")
        lines.append(macro_body)
    if status_summary_lines:
        lines.append("")
        for line in status_summary_lines:
            lines.append(f"*{_wiki_escape(line)}*")
    lines.append("")
    for line in scenario_lines:
        lines.append(_wiki_escape(line))
    return "\n".join(lines)


def write_daily_readable_reports(
    output_dir: str,
    report_data: dict[tuple[str, str], dict[str, Any]],
    formats: set[str],
    *,
    template_dir: str | None = None,
) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    written_paths: list[str] = []
    for (folder_id, folder_name), payload in sorted(report_data.items(), key=lambda item: item[0][1]):
        cycles = payload["cycles"]
        fid = str(folder_id)
        base_name = _build_daily_report_base_name(fid, folder_name, cycles)
        if "html" in formats:
            html_path = os.path.join(output_dir, f"{base_name}.html")
            body = render_daily_html_report(
                folder_name, cycles, folder_id=fid, template_dir=template_dir
            )
            if _write_text_if_changed(html_path, body):
                written_paths.append(html_path)
        if "wiki" in formats:
            wiki_path = os.path.join(output_dir, f"{base_name}.confluence.txt")
            chart_name = f"{base_name}_conclusion_pie.png"
            chart_path = os.path.join(output_dir, chart_name)
            chart_written = _write_daily_pie_png(
                chart_path, _daily_aggregate_case_status_counts(cycles)
            )
            body = render_daily_wiki_report(
                folder_name,
                cycles,
                folder_id=fid,
                template_dir=template_dir,
            )
            if _write_text_if_changed(wiki_path, body):
                written_paths.append(wiki_path)
            if chart_written:
                written_paths.append(chart_path)
    return written_paths


def write_build_log_reports(
    output_dir: str,
    report_data: dict[tuple[str, str], dict[str, Any]],
    formats: set[str],
    *,
    jira_base_url: str,
    jira_auth_headers: dict[str, str] | None,
) -> list[str]:
    """One HTML/wiki file per Jira issue: reproduction lines per build, newest build first.

    Files are always rewritten on each run (even when content is unchanged) so timestamps
    refresh and Confluence weekly publish receives every HTML path.
    """
    pages = _gather_jira_issue_build_log_pages(report_data)
    if not pages:
        return []
    summaries = _fetch_jira_issue_summaries(
        list(pages.keys()),
        base_url=jira_base_url,
        auth_headers=jira_auth_headers,
    )
    os.makedirs(output_dir, exist_ok=True)
    written_paths: list[str] = []
    for issue_key in sorted(pages.keys()):
        blocks = pages[issue_key]
        summary = summaries.get(issue_key, "")
        safe_name = re.sub(r"[^\w.-]+", "_", issue_key.strip()) or "issue"
        stem = f"{safe_name}_build_log"
        if "html" in formats:
            html_path = os.path.join(output_dir, f"{stem}.html")
            body = render_jira_issue_build_log_html(issue_key, summary, blocks)
            if body:
                _write_text_always(html_path, body)
                written_paths.append(html_path)
        if "wiki" in formats:
            wiki_path = os.path.join(output_dir, f"{stem}.confluence.txt")
            body = render_jira_issue_build_log_wiki(issue_key, summary, blocks)
            if body:
                _write_text_always(wiki_path, body)
                written_paths.append(wiki_path)
    return written_paths


def _list_build_log_html_publish_paths(output_dir: str) -> list[str]:
    """All per-issue build log HTML files on disk (for Confluence bugs publish)."""
    if not os.path.isdir(output_dir):
        return []
    return sorted(
        os.path.join(output_dir, name)
        for name in os.listdir(output_dir)
        if name.endswith("_build_log.html")
    )


def _list_daily_readable_html_paths(output_dir: str) -> list[str]:
    if not os.path.isdir(output_dir):
        return []
    return sorted(
        os.path.join(output_dir, name)
        for name in os.listdir(output_dir)
        if name.endswith(".html") and not name.startswith("weekly_cycle_matrix")
    )


def _list_weekly_readable_html_paths(output_dir: str) -> list[str]:
    if not os.path.isdir(output_dir):
        return []
    return sorted(
        os.path.join(output_dir, name)
        for name in os.listdir(output_dir)
        if name.endswith(".html") and name.startswith("weekly_cycle_matrix")
    )


def _merge_confluence_publish_paths(
    expected_paths: list[str], output_dir: str, *, list_on_disk
) -> list[str]:
    on_disk = list_on_disk(output_dir)
    merged = sorted({p for p in expected_paths if p} | {p for p in on_disk if p})
    if on_disk and not expected_paths:
        print(
            f"Confluence publish: using {len(on_disk)} HTML file(s) already on disk "
            f"under {output_dir}."
        )
    return merged


def _expected_daily_readable_html_paths(
    output_dir: str,
    report_data: dict[tuple[str, str], dict[str, Any]],
) -> list[str]:
    paths: list[str] = []
    for (folder_id, folder_name), _payload in sorted(
        report_data.items(), key=lambda item: item[0][1]
    ):
        cycles = _payload.get("cycles", {})
        base_name = _build_daily_report_base_name(str(folder_id), folder_name, cycles)
        paths.append(os.path.join(output_dir, f"{base_name}.html"))
    return paths


def _expected_weekly_readable_html_paths(
    output_dir: str,
    report_data: dict[tuple[str, str], dict[str, Any]],
    *,
    per_folder: bool,
) -> list[str]:
    paths: list[str] = []
    for week_start, *_rest in _weekly_cycle_matrix_data_all(report_data):
        week_label = week_start.isoformat() if week_start else "unknown_week"
        base_name = f"weekly_cycle_matrix_{week_label}"
        paths.append(os.path.join(output_dir, f"{base_name}.html"))
    if per_folder:
        for folder_key, payload in report_data.items():
            folder_id_pf, _folder_name_pf = folder_key
            suffix = f"_folder_{folder_id_pf}"
            single_folder_data = {folder_key: payload}
            for week_start, *_rest in _weekly_cycle_matrix_data_all(
                single_folder_data
            ):
                week_label = week_start.isoformat() if week_start else "unknown_week"
                base_name = f"weekly_cycle_matrix_{week_label}{suffix}"
                paths.append(os.path.join(output_dir, f"{base_name}.html"))
    return paths


def print_readable_report(weekly: dict[date, Counter[str]]) -> None:
    if not weekly:
        print("No executions found for selected filters.")
        return

    total_counter: Counter[str] = Counter()
    for week in sorted(weekly.keys()):
        counter = weekly[week]
        total_counter.update(counter)
        total = week_total(counter)
        passed = pass_count_for_week(counter)
        pass_rate = (passed / total * 100.0) if total else 0.0
        print("-" * 60)
        print(f"Week: {week.isoformat()} | Total: {total}")
        for status_label, count in sorted(counter.items(), key=lambda item: (-item[1], item[0].lower())):
            print(f"  {status_label}: {count}")
        print(f"  pass_rate (normalized): {pass_rate:.2f}%")

    grand_total = week_total(total_counter)
    print("-" * 60)
    print("Totals across all weeks:")
    for status_label, count in sorted(
        total_counter.items(), key=lambda item: (-item[1], item[0].lower())
    ):
        print(f"  {status_label}: {count}")
    print(f"Grand total: {grand_total}")


def _apply_regenerate_last_n_days(
    args: argparse.Namespace, n: int, effective_rolling_days: int
) -> int:
    if args.from_date or args.to_date:
        raise ValueError(
            "Rolling regenerate mode cannot be used together with "
            "--from-date/--to-date"
        )
    if n < 1:
        raise ValueError("--regenerate-last-n-days requires N >= 1")
    today = date.today()
    args.from_date = (today - timedelta(days=n - 1)).isoformat()
    args.to_date = today.isoformat()
    if effective_rolling_days <= 0:
        effective_rolling_days = n
    print(
        f"Date window override: last {n} days "
        f"({args.from_date} .. {args.to_date}), "
        f"rolling-days={effective_rolling_days}"
    )
    return effective_rolling_days


def list_folders_as_json(
    base_url: str,
    headers: dict[str, str],
    args: argparse.Namespace,
    tree_name_pattern: re.Pattern[str] | None,
    tree_root_path_pattern: re.Pattern[str] | None,
    tree_source_query: dict[str, Any],
    tree_source_body: dict[str, Any],
    root_folder_ids: list[str],
) -> str:
    """Discover the folder tree and return a JSON string (array of folder dicts)."""
    selected: list[FolderNode] = []
    if args.tree_source_endpoint:
        nodes, _, _ = discover_folders_custom_tree_source(
            base_url=base_url,
            endpoint=args.tree_source_endpoint,
            headers=headers,
            method=args.tree_source_method,
            query_params=tree_source_query,
            body=tree_source_body,
        )
    elif args.tree_autoprobe:
        nodes, _, _ = probe_tree_endpoints(
            base_url=base_url,
            headers=headers,
            project_id=args.project_id,
        )
    else:
        nodes, _, _ = discover_folders_tree_fallback(
            base_url=base_url,
            folder_search_endpoint=args.folder_search_endpoint,
            foldertree_endpoint=args.foldertree_endpoint,
            headers=headers,
            project_id=args.project_id,
        )
    selected = select_tree_target_folders(
        nodes=nodes,
        root_folder_ids=root_folder_ids,
        leaf_only=args.tree_leaf_only,
        name_pattern=tree_name_pattern,
        root_path_pattern=tree_root_path_pattern,
    )
    result = [
        {
            "id": f.folder_id,
            "name": f.folder_name,
            "parent_id": f.parent_id,
            "full_path": f.full_path,
            "is_leaf": f.is_leaf,
        }
        for f in selected
    ]
    return json.dumps(result, ensure_ascii=False, indent=2)


def post_test_result(
    base_url: str,
    headers: dict[str, str],
    test_run_id: str,
    item_id: str,
    status_id: str,
    comment: str | None = None,
    execution_date: str | None = None,
) -> dict[str, Any] | None:
    """POST a new test result for a test-run item.

    Corresponds to: POST rest/tests/1.0/testrun/{test_run_id}/testresults
    Body fields that the Navio/Zephyr API accepts::

        {
            "testRunItemId": <item_id>,
            "testResultStatusId": <status_id>,
            "comment": "...",          # optional
            "executionDate": "...",    # optional ISO-8601
        }

    Returns the parsed response body or None when the server returns no body.
    """
    endpoint = f"rest/tests/1.0/testrun/{test_run_id}/testresults"
    body: dict[str, Any] = {
        "testRunItemId": item_id,
        "testResultStatusId": status_id,
    }
    if comment is not None:
        body["comment"] = comment
    if execution_date is not None:
        body["executionDate"] = execution_date
    return request_json(base_url, endpoint, headers, method="POST", body=body)


def put_test_result(
    base_url: str,
    headers: dict[str, str],
    test_run_id: str,
    result_id: str,
    status_id: str,
    comment: str | None = None,
    execution_date: str | None = None,
) -> dict[str, Any] | None:
    """PUT (update) an existing test result by its id.

    Corresponds to: PUT rest/tests/1.0/testrun/{test_run_id}/testresults/{result_id}
    Returns the parsed response body or None when the server returns no body.
    """
    endpoint = f"rest/tests/1.0/testrun/{test_run_id}/testresults/{result_id}"
    body: dict[str, Any] = {"testResultStatusId": status_id}
    if comment is not None:
        body["comment"] = comment
    if execution_date is not None:
        body["executionDate"] = execution_date
    return request_json(base_url, endpoint, headers, method="PUT", body=body)


def put_test_step_result(
    base_url: str,
    headers: dict[str, str],
    test_run_id: str,
    result_id: str,
    step_result_id: str,
    status_id: str,
    comment: str | None = None,
) -> dict[str, Any] | None:
    """PUT (update) a single script-step result within a test result.

    Corresponds to:
    PUT rest/tests/1.0/testrun/{test_run_id}/testresults/{result_id}/testscriptresults/{step_result_id}
    Returns the parsed response body or None when the server returns no body.
    """
    endpoint = (
        f"rest/tests/1.0/testrun/{test_run_id}/testresults/{result_id}"
        f"/testscriptresults/{step_result_id}"
    )
    body: dict[str, Any] = {"testResultStatusId": status_id}
    if comment is not None:
        body["comment"] = comment
    return request_json(base_url, endpoint, headers, method="PUT", body=body)


def _emit_startup_heartbeat() -> None:
    print(
        f"zephyr_weekly_report starting pid={os.getpid()} "
        f"at {datetime.now().isoformat(timespec='seconds')}",
        flush=True,
    )


def main() -> int:
    _load_repo_dotenv_if_absent()
    args = parse_args()
    _emit_startup_heartbeat()

    lock_acquired = False
    try:
        if args.run_lock_file:
            lock_acquired = _try_acquire_run_lock(args.run_lock_file)
            if not lock_acquired:
                print(
                    f"Another run is already active; lock file is held: {args.run_lock_file}",
                    file=sys.stderr,
                )
                return 0

        loop_interval_minutes = _resolve_loop_interval_minutes(args)
        if loop_interval_minutes is not None:
            return _run_loop(args, loop_interval_minutes)

        return run_once(args)
    finally:
        if lock_acquired:
            _release_run_lock()


def run_once(args: argparse.Namespace) -> int:
    try:
        effective_rolling_days = args.rolling_days
        if args.regenerate_last_7_days and args.regenerate_last_n_days > 0:
            raise ValueError(
                "Use either --regenerate-last-7-days or --regenerate-last-n-days, not both"
            )
        if args.regenerate_last_n_days < 0:
            raise ValueError("--regenerate-last-n-days must be >= 0")

        regen_n = 0
        if args.regenerate_last_7_days:
            regen_n = 7
        elif args.regenerate_last_n_days > 0:
            regen_n = args.regenerate_last_n_days
        elif not args.from_date and not args.to_date:
            regen_n = _zephyr_regenerate_last_n_days_from_environment()

        if regen_n > 0:
            effective_rolling_days = _apply_regenerate_last_n_days(
                args, regen_n, effective_rolling_days
            )
        else:
            if not args.from_date:
                fd = (os.getenv("ZEPHYR_FROM_DATE") or "").strip()
                if fd:
                    args.from_date = fd
            if not args.to_date:
                td = (os.getenv("ZEPHYR_TO_DATE") or "").strip()
                if td:
                    args.to_date = td
        token = args.token or os.getenv("ZEPHYR_API_TOKEN")
        if not args.base_url:
            raise ValueError(
                "Missing Zephyr base URL. Pass --base-url or set ZEPHYR_BASE_URL "
                "in the environment or .env file."
            )
        if not token:
            raise ValueError(
                "Missing API token. Pass --token or set ZEPHYR_API_TOKEN environment variable."
            )
        rolling_days = max(0, int(args.rolling_days))
        if rolling_days > 0:
            to_date = date.today()
            from_date = to_date - timedelta(days=rolling_days - 1)
            print(
                "Rolling window: "
                f"rolling_days={rolling_days} from_date={from_date.isoformat()} "
                f"to_date={to_date.isoformat()} (overrides --from-date / --to-date)",
                file=sys.stderr,
            )
        else:
            from_date = parse_date(args.from_date)
            to_date = parse_date(args.to_date)
            if from_date and to_date and from_date > to_date:
                raise ValueError("--from-date must be less or equal to --to-date")

        headers = build_headers(args.token_header, args.token_prefix, token)
        extra_params = parse_extra_params(args.extra_param)
        root_folder_ids = parse_root_folder_ids(args.root_folder_id)
        allowed_root_folder_ids = set(parse_root_folder_ids(args.allowed_root_folder_id))
        date_fields = args.date_field or DEFAULT_DATE_FIELDS
        status_fields = args.status_field or DEFAULT_STATUS_FIELDS
        folder_name_pattern = re.compile(args.folder_name_regex) if args.folder_name_regex else None
        folder_path_pattern = re.compile(args.folder_path_regex) if args.folder_path_regex else None
        tree_name_pattern = re.compile(args.tree_name_regex) if args.tree_name_regex else None
        tree_root_path_pattern = (
            re.compile(args.tree_root_path_regex) if args.tree_root_path_regex else None
        )
        tree_source_query = _parse_json_object_arg(
            args.tree_source_query_json, "--tree-source-query-json"
        )
        tree_source_body = _parse_json_object_arg(
            args.tree_source_body_json, "--tree-source-body-json"
        )
        created_or_existing_folder = ensure_folder_created_or_existing(
            args=args,
            headers=headers,
            tree_source_query=tree_source_query,
            tree_source_body=tree_source_body,
        )
        if args.create_folder_use_as_root and created_or_existing_folder:
            root_folder_ids = [created_or_existing_folder.folder_id]
            allowed_root_folder_ids = {created_or_existing_folder.folder_id}
            print(
                "Using created/existing folder as root filter: "
                f"{created_or_existing_folder.folder_id}"
            )
        elif args.create_folder_use_as_root and args.create_folder_dry_run:
            print(
                "create-folder-use-as-root requested with dry-run; "
                "root filters were not overridden because folder id is unknown."
            )

        if args.discover_folders:
            folder_rows: list[tuple[FolderNode, dict[date, Counter[str]]]] = []
            cycles_cases_rows: list[list[str]] = []
            case_steps_rows: list[list[str]] = []
            report_data: dict[tuple[str, str], dict[str, Any]] | None = None
            daily_readable_paths: list[str] = []
            build_log_report_paths: list[str] = []
            build_log_html_publish_paths: list[str] = []
            bugs_rollup_html_publish_paths: list[str] = []
            daily_html_publish_paths: list[str] = []
            weekly_html_publish_paths: list[str] = []
            readable_template_dir = args.readable_template_dir or os.getenv(
                "ZEPHYR_READABLE_TEMPLATE_DIR"
            )
            if readable_template_dir:
                readable_template_dir = os.path.expanduser(readable_template_dir.strip()) or None
            confluence_cfg = _load_confluence_publish_config()
            publish_confluence_daily = bool(confluence_cfg and confluence_cfg.publish_daily)
            publish_confluence_weekly = bool(confluence_cfg and confluence_cfg.publish_weekly)
            publish_confluence_bugs = bool(confluence_cfg and confluence_cfg.publish_bugs)
            confluence_publish_roots: ConfluencePublishRoots | None = None
            confluence_week_parents: ConfluenceWeekParentCache | None = None
            need_cycles_cases_data = (
                args.export_cycles_cases
                or args.export_daily_readable
                or args.export_build_log_report
            )
            collect_case_steps = (
                args.export_case_steps
                or args.export_daily_readable
                or args.export_build_log_report
            )
            timings = TimingRecorder()
            total_executions = 0
            total_skipped = Counter()
            errors: list[str] = []
            if collect_case_steps:
                with timed_step(timings, "fetch test result status names"):
                    status_names = fetch_test_result_status_names(
                        args.base_url, headers, args.project_id
                    )
            else:
                status_names = {}
            abtest_issues: list[dict[str, Any]] | None = None
            drv_extra_report_days: set[date] = set()
            if from_date is not None and to_date is not None:
                with timed_step(timings, "fetch DRV/Jira metadata"):
                    jira_rb_drv = _resolve_weekly_jira_metadata_base(args.base_url)
                    jira_rh_drv = _jira_bug_metadata_auth_headers(headers) or headers
                    abtest_issues = fetch_autofleet_abtest_candidates(
                        base_url=jira_rb_drv,
                        auth_headers=jira_rh_drv,
                    )
                    drv_extra_report_days = _drv_extra_folder_days_outside_rolling_window(
                        from_date,
                        to_date,
                        jira_base=jira_rb_drv,
                        jira_headers=jira_rh_drv,
                        issues=abtest_issues,
                    )
                print(
                    "DRV: summary — "
                    f"Jira issues={len(abtest_issues)}, "
                    f"extra folder day(s) for tree/report_data={len(drv_extra_report_days)}."
                )
                if drv_extra_report_days:
                    print(
                        "DRV: keeping report_data / tree folder day(s) outside "
                        f"{from_date}..{to_date}: "
                        f"{', '.join(sorted(str(d) for d in drv_extra_report_days))}"
                    )

            if args.discovery_mode == "executions" or args.discover_from_executions:
                if not args.project_id:
                    raise ValueError("--discover-from-executions requires --project-id")
                project_query = sanitize_tql_query(
                    fill_template(
                    args.project_query,
                    "project_id",
                    args.project_id,
                    "--project-query",
                    )
                )
                scan_params = dict(extra_params)
                scan_params["query"] = project_query
                with timed_step(timings, "execution discovery fetch", project_query):
                    executions = fetch_executions(
                        base_url=args.base_url,
                        endpoint=args.endpoint,
                        headers=headers,
                        extra_params=scan_params,
                        page_size=args.page_size,
                    )
                if args.debug_folder_fields:
                    print_folder_field_debug(executions)
                folder_ids_from_executions = set()
                for item in executions:
                    folder_id, _ = _extract_folder_info(item)
                    if folder_id:
                        folder_ids_from_executions.add(folder_id)
                (
                    resolved_folder_names,
                    resolved_folder_paths,
                    name_resolution_stats,
                ) = ({}, {}, Counter())
                with timed_step(
                    timings,
                    "resolve folder names",
                    f"{len(folder_ids_from_executions)} folder id(s)",
                ):
                    (
                        resolved_folder_names,
                        resolved_folder_paths,
                        name_resolution_stats,
                    ) = resolve_folder_names_by_id(
                        folder_ids=folder_ids_from_executions,
                        endpoint_templates=args.folder_name_endpoint_template,
                        base_url=args.base_url,
                        headers=headers,
                    )
                if args.debug_folder_fields:
                    print_resolved_folder_names(resolved_folder_names)
                    print_resolved_folder_paths(resolved_folder_paths)
                with timed_step(timings, "aggregate execution discovery folders"):
                    folder_rows, filter_stats = aggregate_by_folder_from_executions(
                        items=executions,
                        date_fields=date_fields,
                        status_fields=status_fields,
                        from_date=from_date,
                        to_date=to_date,
                        root_folder_ids=root_folder_ids,
                        allowed_root_folder_ids=allowed_root_folder_ids or None,
                        folder_name_pattern=folder_name_pattern,
                        resolved_folder_names=resolved_folder_names,
                        folder_path_pattern=folder_path_pattern,
                        resolved_folder_paths=resolved_folder_paths,
                    )
                total_executions = len(executions)
                if need_cycles_cases_data:
                    grouped_cycles: dict[str, list[dict[str, Any]]] = defaultdict(list)
                    grouped_folder_names: dict[str, str] = {}
                    for item in executions:
                        folder_id, folder_name = _extract_folder_info(item)
                        if not folder_id:
                            continue
                        grouped_cycles[folder_id].append(item)
                        grouped_folder_names.setdefault(
                            folder_id, folder_name or f"folder_{folder_id}"
                        )
                    for folder_id, folder_cycles in grouped_cycles.items():
                        folder = FolderNode(
                            folder_id=folder_id,
                            folder_name=(
                                resolved_folder_names.get(folder_id)
                                or grouped_folder_names.get(folder_id, f"folder_{folder_id}")
                            ),
                            parent_id=None,
                        )
                        cycles_cases_rows.extend(
                            build_cycle_case_rows(
                                folder=folder,
                                cycles=folder_cycles,
                                testcase_endpoint_templates=args.testcase_endpoint_template,
                                base_url=args.base_url,
                                headers=headers,
                                synthetic_cycle_ids=args.synthetic_cycle_ids,
                            )
                        )
                        if collect_case_steps:
                            case_steps_rows.extend(
                                build_case_step_rows(
                                    folder=folder,
                                    cycles=folder_cycles,
                                    base_url=args.base_url,
                                    headers=headers,
                                    status_names=status_names,
                                    synthetic_cycle_ids=args.synthetic_cycle_ids,
                                    detail_workers=args.detail_workers,
                                )
                            )
                if filter_stats:
                    print(f"Execution discovery filter stats: {dict(filter_stats)}")
                if name_resolution_stats:
                    print(f"Folder name resolution stats: {dict(name_resolution_stats)}")
            else:
                tree_started_at = time.perf_counter()
                discovery_errors: list[str] = []
                folders: list[FolderNode] = []
                source = "none"
                if args.tree_source_endpoint:
                    try:
                        folders, source = discover_folders_custom_tree_source(
                            base_url=args.base_url,
                            headers=headers,
                            endpoint=args.tree_source_endpoint,
                            method=args.tree_source_method,
                            query_params=tree_source_query,
                            body=tree_source_body,
                        )
                    except Exception as exc:  # pylint: disable=broad-except
                        discovery_errors.append(str(exc))
                if not folders:
                    folders, source, fallback_errors = discover_folders_tree_fallback(
                        base_url=args.base_url,
                        headers=headers,
                        project_id=args.project_id,
                        folder_search_endpoint=args.folder_search_endpoint,
                        foldertree_endpoint=args.foldertree_endpoint,
                    )
                    discovery_errors.extend(fallback_errors)
                if not folders and args.tree_autoprobe:
                    folders, source, probe_attempts = probe_tree_endpoints(
                        base_url=args.base_url,
                        headers=headers,
                        project_id=args.project_id,
                    )
                    if probe_attempts:
                        print("Tree autoprobe attempts:")
                        for attempt in probe_attempts[:20]:
                            print(f"- {attempt}")
                if not folders:
                    joined = "\n".join(f"- {err}" for err in discovery_errors)
                    raise RuntimeError(
                        "Folder discovery returned no folders in tree mode.\n"
                        f"Tried source fallback. Errors:\n{joined}"
                    )
                print(f"Tree discovery source: {source}")
                selected_folders = select_tree_target_folders(
                    nodes=folders,
                    root_folder_ids=root_folder_ids,
                    leaf_only=args.tree_leaf_only,
                    name_pattern=tree_name_pattern,
                    root_path_pattern=tree_root_path_pattern,
                )
                if from_date is not None and to_date is not None:
                    selected_before_day_filter = len(selected_folders)
                    selected_folders = _filter_tree_folders_by_report_day(
                        selected_folders,
                        from_date,
                        to_date,
                        drv_extra_report_days if drv_extra_report_days else None,
                    )
                    if len(selected_folders) != selected_before_day_filter:
                        print(
                            "Tree day-window filter "
                            f"({from_date} .. {to_date}"
                            + (
                                f", +{len(drv_extra_report_days)} DRV day(s)"
                                if drv_extra_report_days
                                else ""
                            )
                            + "): "
                            f"{len(selected_folders)} folder(s) "
                            f"(before filter: {selected_before_day_filter})"
                        )
                print(
                    f"Tree folders discovered: {len(folders)}; "
                    f"selected after filters: {len(selected_folders)} folder(s)"
                )
                timings.record(
                    "tree discovery",
                    time.perf_counter() - tree_started_at,
                    f"source={source}, selected={len(selected_folders)}",
                )

                n_selected = len(selected_folders)
                if n_selected:
                    roadmap_parts: list[str] = [
                        f"fetching executions for {n_selected} folder(s)",
                    ]
                    if need_cycles_cases_data:
                        roadmap_parts.append("building cycles/cases detail rows")
                    if collect_case_steps:
                        roadmap_parts.append("building case-step rows")
                    if args.export_cycles_cases:
                        roadmap_parts.append("writing cycles/cases CSV")
                    if args.export_case_steps:
                        roadmap_parts.append("writing case steps CSV")
                    if args.export_daily_readable:
                        roadmap_parts.append("building daily readable reports")
                    if publish_confluence_daily:
                        roadmap_parts.append("Confluence daily publish")
                    if publish_confluence_weekly:
                        roadmap_parts.append("Confluence weekly publish")
                    if publish_confluence_bugs:
                        roadmap_parts.append("Confluence bugs publish")
                    print("Next: " + ", then ".join(roadmap_parts) + ".")

                folder_worker_count = _bounded_worker_count(args.folder_workers, n_selected)
                detail_worker_count = _bounded_worker_count(args.detail_workers)
                if n_selected:
                    print(
                        "Workers: "
                        f"folder_workers={folder_worker_count}, "
                        f"detail_workers={detail_worker_count}"
                    )

                def _process_tree_folder(idx: int, folder: FolderNode) -> dict[str, Any]:
                    folder_started_at = time.perf_counter()
                    per_folder_params = dict(extra_params)
                    per_folder_params["query"] = sanitize_tql_query(
                        fill_template(
                            args.query_template,
                            "folder_id",
                            folder.folder_id,
                            "--query-template",
                        )
                    )
                    print(
                        f"[{idx}/{n_selected}] Folder {folder.folder_id} ({folder.folder_name}): "
                        "fetching executions..."
                    )
                    executions = fetch_executions(
                        base_url=args.base_url,
                        endpoint=args.endpoint,
                        headers=headers,
                        extra_params=per_folder_params,
                        page_size=args.page_size,
                    )
                    weekly, skipped = aggregate_weekly(
                        items=executions,
                        date_fields=date_fields,
                        status_fields=status_fields,
                        from_date=from_date,
                        to_date=to_date,
                    )
                    folder_cycles_cases_rows: list[list[str]] = []
                    folder_case_steps_rows: list[list[str]] = []
                    if need_cycles_cases_data:
                        folder_cycles_cases_rows = build_cycle_case_rows(
                            folder=folder,
                            cycles=executions,
                            testcase_endpoint_templates=args.testcase_endpoint_template,
                            base_url=args.base_url,
                            headers=headers,
                            synthetic_cycle_ids=args.synthetic_cycle_ids,
                        )
                    if collect_case_steps:
                        folder_case_steps_rows = build_case_step_rows(
                            folder=folder,
                            cycles=executions,
                            base_url=args.base_url,
                            headers=headers,
                            status_names=status_names,
                            synthetic_cycle_ids=args.synthetic_cycle_ids,
                            detail_workers=detail_worker_count,
                        )
                    duration = time.perf_counter() - folder_started_at
                    print(
                        f"[{idx}/{n_selected}] Folder {folder.folder_id} ({folder.folder_name}): "
                        f"done ({len(executions)} execution(s), {duration:.2f}s)"
                    )
                    return {
                        "idx": idx,
                        "folder": folder,
                        "weekly": weekly,
                        "skipped": skipped,
                        "executions_count": len(executions),
                        "cycles_cases_rows": folder_cycles_cases_rows,
                        "case_steps_rows": folder_case_steps_rows,
                        "duration": duration,
                    }

                folder_results: list[dict[str, Any]] = []
                if n_selected:
                    with timed_step(
                        timings,
                        "process tree folders",
                        f"folders={n_selected}, workers={folder_worker_count}",
                    ):
                        if folder_worker_count == 1:
                            for idx, folder in enumerate(selected_folders, start=1):
                                try:
                                    folder_results.append(_process_tree_folder(idx, folder))
                                except Exception as exc:  # pylint: disable=broad-except
                                    message = (
                                        f"Folder {folder.folder_id} ({folder.folder_name}) failed: {exc}"
                                    )
                                    if args.continue_on_folder_error:
                                        errors.append(message)
                                        print(f"[{idx}/{n_selected}] FAILED (continuing): {message}")
                                        continue
                                    raise RuntimeError(message) from exc
                        else:
                            with concurrent.futures.ThreadPoolExecutor(
                                max_workers=folder_worker_count
                            ) as executor:
                                future_to_context = {
                                    executor.submit(_process_tree_folder, idx, folder): (idx, folder)
                                    for idx, folder in enumerate(selected_folders, start=1)
                                }
                                for future in concurrent.futures.as_completed(future_to_context):
                                    idx, folder = future_to_context[future]
                                    try:
                                        folder_results.append(future.result())
                                    except Exception as exc:  # pylint: disable=broad-except
                                        message = (
                                            f"Folder {folder.folder_id} ({folder.folder_name}) failed: {exc}"
                                        )
                                        if args.continue_on_folder_error:
                                            errors.append(message)
                                            print(f"[{idx}/{n_selected}] FAILED (continuing): {message}")
                                            continue
                                        raise RuntimeError(message) from exc

                for result in sorted(folder_results, key=lambda item: int(item["idx"])):
                    folder_rows.append((result["folder"], result["weekly"]))
                    total_executions += int(result["executions_count"])
                    total_skipped.update(result["skipped"])
                    cycles_cases_rows.extend(result["cycles_cases_rows"])
                    case_steps_rows.extend(result["case_steps_rows"])
                    timings.record(
                        "process folder",
                        float(result["duration"]),
                        (
                            f"{result['folder'].folder_id} "
                            f"{result['folder'].folder_name}"
                        )
                    )

            write_csv_started_at = time.perf_counter()
            print("Writing per-folder weekly CSVs...")
            os.makedirs(args.per_folder_dir, exist_ok=True)
            for folder, weekly in folder_rows:
                file_name = f"{slugify(folder.folder_name)}_{folder.folder_id}.csv"
                path = os.path.join(args.per_folder_dir, file_name)
                write_csv(path, weekly)

            write_folder_summary_csv(args.output, folder_rows)
            if args.export_cycles_cases:
                write_cycles_cases_csv(args.cycles_cases_output, cycles_cases_rows)
            if args.export_case_steps:
                write_case_steps_csv(args.case_steps_output, case_steps_rows)
            timings.record("write CSV outputs", time.perf_counter() - write_csv_started_at)
            needs_report_data = bool(
                args.cycle_progress_output
                or args.weekly_cycle_matrix_output
                or args.export_weekly_readable
            )
            # One aggregation for readable/matrix paths, then rolling filter **before** writing
            # daily/weekly artifacts so HTML/wiki/Confluence targets match the same date window.
            if args.export_daily_readable or needs_report_data or args.export_build_log_report:
                with timed_step(timings, 'aggregate readable report data'):
                    if case_steps_rows:
                        report_data = aggregate_readable_daily_reports_from_steps(
                            case_steps_rows, cycles_cases_rows
                        )
                    else:
                        report_data = aggregate_readable_daily_reports_legacy(cycles_cases_rows)
            if (
                effective_rolling_days > 0
                and from_date is not None
                and to_date is not None
                and report_data is not None
            ):
                report_data = _filter_report_data_by_resolved_folder_day(
                    report_data,
                    from_date,
                    to_date,
                    extra_report_days=drv_extra_report_days if drv_extra_report_days else None,
                )
            if args.export_daily_readable:
                selected_formats = set(args.daily_readable_format or ["html", "wiki"])
                if not report_data:
                    print(
                        "Skipping daily readable reports: no folder payloads "
                        "(empty cycles/case steps or outside rolling window)."
                    )
                else:
                    fmt_join = ", ".join(sorted(selected_formats))
                    print(
                        f"Building daily readable reports for {len(report_data)} folder payload(s) "
                        f"(formats: {fmt_join})..."
                    )
                    with timed_step(
                        timings,
                        "write daily readable reports",
                        f"payloads={len(report_data)}, formats={fmt_join}",
                    ):
                        daily_readable_paths = write_daily_readable_reports(
                            output_dir=args.daily_readable_dir,
                            report_data=report_data,
                            formats=selected_formats,
                            template_dir=readable_template_dir,
                        )
                    if "html" in selected_formats:
                        daily_html_publish_paths = _expected_daily_readable_html_paths(
                            args.daily_readable_dir, report_data
                        )
            if args.export_build_log_report:
                if not report_data:
                    print(
                        "Skipping build log reports: no folder payloads "
                        "(empty cycles/case steps or outside rolling window)."
                    )
                else:
                    bl_formats = set(args.build_log_report_format or _env_csv_values("ZEPHYR_BUILD_LOG_REPORT_FORMATS", ["html", "wiki"]))
                    print(
                        f"Building per-issue build log reports from {len(report_data)} "
                        f"folder payload(s) (formats: {', '.join(sorted(bl_formats))})..."
                    )
                    jira_meta_base = _resolve_weekly_jira_metadata_base(args.base_url)
                    jira_meta_headers = _jira_bug_metadata_auth_headers(headers) or headers
                    build_log_report_paths = write_build_log_reports(
                        output_dir=args.build_log_report_dir,
                        report_data=report_data,
                        formats=bl_formats,
                        jira_base_url=jira_meta_base,
                        jira_auth_headers=jira_meta_headers,
                    )
                    if "html" in bl_formats:
                        regenerated_html = {
                            p for p in build_log_report_paths if p.endswith(".html")
                        }
                        build_log_html_publish_paths = _list_build_log_html_publish_paths(
                            args.build_log_report_dir
                        )
                        stale_on_disk = [
                            p for p in build_log_html_publish_paths if p not in regenerated_html
                        ]
                        if stale_on_disk:
                            print(
                                "Build log Confluence publish: "
                                f"{len(regenerated_html)} page(s) from current Zephyr data, "
                                f"{len(stale_on_disk)} additional HTML file(s) on disk "
                                "(no logviewer+Jira link in this run; republishing previous content)."
                            )
            bugs_rollup_report_paths: list[str] = []
            if report_data and (
                args.export_build_log_report
                or publish_confluence_bugs
                or args.export_weekly_readable
            ):
                bugs_rollup_dir = (
                    os.getenv("ZEPHYR_BUGS_ROLLUP_DIR", "reports/bugs_rollup").strip()
                    or "reports/bugs_rollup"
                )
                rollup_formats = set(
                    args.build_log_report_format
                    or _env_csv_values("ZEPHYR_BUILD_LOG_REPORT_FORMATS", ["html", "wiki"])
                )
                rollup_keys: list[str] = []
                seen_rollup_keys: set[str] = set()
                for last_n in (
                    _bugs_rollup_last_weeks_count(),
                    None,
                ):
                    _labels, analytics, _weeks = _defect_rollup_from_report_data(
                        report_data, last_n_weeks=last_n
                    )
                    for key in analytics.get("keys_ordered") or []:
                        k = str(key).strip()
                        if k and k not in seen_rollup_keys:
                            seen_rollup_keys.add(k)
                            rollup_keys.append(k)
                jira_meta_base = _resolve_weekly_jira_metadata_base(args.base_url)
                jira_meta_headers = _jira_bug_metadata_auth_headers(headers) or headers
                rollup_defect_meta = _fetch_jira_bug_metadata(
                    rollup_keys,
                    base_url=jira_meta_base,
                    auth_headers=jira_meta_headers,
                )
                with timed_step(timings, "write bugs rollup index reports"):
                    bugs_rollup_report_paths = write_bugs_rollup_reports(
                        output_dir=bugs_rollup_dir,
                        report_data=report_data,
                        formats=rollup_formats,
                        defect_meta=rollup_defect_meta,
                    )
                bugs_rollup_html_publish_paths = [
                    p for p in bugs_rollup_report_paths if p.endswith(".html")
                ]
            cycle_progress_csv_updated: bool | None = None
            weekly_matrix_csv_updates: list[tuple[str, bool]] = []
            report_data_for_matrix = report_data or {}
            if args.cycle_progress_output:
                cycle_progress_rows = _cycle_progress_csv_rows(report_data_for_matrix)
                cycle_progress_csv_updated = write_cycle_progress_csv(
                    args.cycle_progress_output, cycle_progress_rows
                )
            weekly_cycle_week_start: date | None = None
            weekly_cycle_weekday_labels = _default_weekday_labels()
            weekly_cycle_rows: list[list[str]] = []
            weekly_cycle_cell_all_ne: list[list[bool]] = []
            weekly_cycle_cell_all_blocked: list[list[bool]] = []
            weekly_cycle_matrices: list[
                tuple[
                    date | None,
                    list[str],
                    list[list[str]],
                    list[list[bool]],
                    list[list[bool]],
                    dict[str, dict[str, int]],
                    list[str],
                    dict[str, list[dict[str, str]]],
                    dict[str, Any],
                ]
            ] = []
            weekly_matrix_started_at = time.perf_counter()
            if args.weekly_cycle_matrix_output:
                weekly_cycle_matrices = _weekly_cycle_matrix_data_all(report_data_for_matrix)
                if weekly_cycle_matrices:
                    (
                        weekly_cycle_week_start,
                        weekly_cycle_weekday_labels,
                        weekly_cycle_rows,
                        weekly_cycle_cell_all_ne,
                        weekly_cycle_cell_all_blocked,
                        _weekly_cycle_status_counts,
                        _weekly_cycle_defect_keys,
                        _weekly_cycle_keys_by_label,
                        _weekly_cycle_defect_analytics,
                    ) = weekly_cycle_matrices[-1]
                    # Keep the legacy output path as the latest week for compatibility.
                    latest_updated = write_weekly_cycle_matrix_csv(
                        args.weekly_cycle_matrix_output,
                        weekly_cycle_weekday_labels,
                        weekly_cycle_rows,
                    )
                    weekly_matrix_csv_updates.append((args.weekly_cycle_matrix_output, latest_updated))
                for week_start, weekday_labels, rows, _cell_flags, _blocked_flags, _wcsc, _wdk, _wck, _wda in weekly_cycle_matrices:
                    week_path = _weekly_output_path_for_week(args.weekly_cycle_matrix_output, week_start)
                    week_updated = write_weekly_cycle_matrix_csv(week_path, weekday_labels, rows)
                    weekly_matrix_csv_updates.append((week_path, week_updated))
                timings.record(
                    "write weekly cycle matrix",
                    time.perf_counter() - weekly_matrix_started_at,
                    f"matrices={len(weekly_cycle_matrices)}",
                )
            weekly_readable_paths: list[str] = []
            if args.export_weekly_readable:
                weekly_readable_started_at = time.perf_counter()
                if not weekly_cycle_matrices:
                    weekly_cycle_matrices = _weekly_cycle_matrix_data_all(report_data_for_matrix)
                if weekly_cycle_matrices:
                    (
                        weekly_cycle_week_start,
                        weekly_cycle_weekday_labels,
                        weekly_cycle_rows,
                        weekly_cycle_cell_all_ne,
                        weekly_cycle_cell_all_blocked,
                        _weekly_cycle_status_counts,
                        _weekly_cycle_defect_keys,
                        _weekly_cycle_keys_by_label,
                        _weekly_cycle_defect_analytics,
                    ) = weekly_cycle_matrices[-1]
                selected_weekly_formats = set(args.weekly_readable_format or ["html", "wiki"])
                jira_weekly_rest_base = _resolve_weekly_jira_metadata_base(args.base_url)
                jira_weekly_rest_headers = _jira_bug_metadata_auth_headers(headers) or headers
                weekly_abtest_issues = (
                    abtest_issues
                    if abtest_issues is not None
                    else fetch_autofleet_abtest_candidates(
                        base_url=jira_weekly_rest_base,
                        auth_headers=jira_weekly_rest_headers,
                    )
                )
                weekly_branch_schedule = _build_best_branch_schedule_from_jira(
                    weekly_abtest_issues
                )
                # Collect all unique defect keys across all weeks and per-folder
                # variants, then fetch metadata from Jira once.
                all_defect_keys: list[str] = []
                seen_defect_keys: set[str] = set()
                for matrix_entry in weekly_cycle_matrices:
                    analytics = matrix_entry[8] or {}
                    for key in analytics.get("keys_ordered", []) or []:
                        if key not in seen_defect_keys:
                            seen_defect_keys.add(key)
                            all_defect_keys.append(key)
                    for key in matrix_entry[7] or []:
                        k = str(key).strip()
                        if k and k not in seen_defect_keys:
                            seen_defect_keys.add(k)
                            all_defect_keys.append(k)
                defect_meta = _fetch_jira_bug_metadata(
                    all_defect_keys,
                    base_url=jira_weekly_rest_base,
                    auth_headers=jira_weekly_rest_headers,
                )
                for (
                    week_start,
                    weekday_labels,
                    rows,
                    cell_all_ne,
                    cell_all_blocked,
                    column_status_counts,
                    defect_keys,
                    cycle_keys_by_label,
                    defect_analytics,
                ) in weekly_cycle_matrices:
                    best_name = _weekly_best_branch_name_for_report_week(
                        weekly_branch_schedule,
                        week_start,
                        base_url=jira_weekly_rest_base,
                        auth_headers=jira_weekly_rest_headers,
                        abtest_issues=weekly_abtest_issues,
                    )
                    best_branch_column = _weekly_best_branch_column_context_for_week(
                        (
                            week_start,
                            weekday_labels,
                            rows,
                            cell_all_ne,
                            cell_all_blocked,
                            column_status_counts,
                            defect_keys,
                            cycle_keys_by_label,
                            defect_analytics,
                        ),
                        report_data=report_data_for_matrix,
                        best_branch_name=best_name,
                        report_week_start=week_start,
                    )
                    weekly_readable_paths.extend(
                        write_weekly_readable_reports(
                            output_dir=args.weekly_readable_dir,
                            week_start=week_start,
                            weekday_labels=weekday_labels,
                            rows=rows,
                            formats=selected_weekly_formats,
                            cell_all_not_executed=cell_all_ne,
                            cell_all_blocked=cell_all_blocked,
                            column_status_counts=column_status_counts,
                            defect_keys=defect_keys,
                            cycle_keys_by_label=cycle_keys_by_label,
                            defect_analytics=defect_analytics,
                            defect_meta=defect_meta,
                            template_dir=readable_template_dir,
                            best_branch_column=best_branch_column,
                        )
                    )
                if args.weekly_readable_per_folder:
                    for folder_key, payload in report_data_for_matrix.items():
                        folder_id_pf, folder_name_pf = folder_key
                        single_folder_data = {folder_key: payload}
                        per_folder_matrices = _weekly_cycle_matrix_data_all(single_folder_data)
                        suffix = f"_folder_{folder_id_pf}"
                        for (
                            week_start_pf,
                            weekday_labels_pf,
                            rows_pf,
                            cell_all_ne_pf,
                            cell_all_blocked_pf,
                            column_status_counts_pf,
                            defect_keys_pf,
                            cycle_keys_by_label_pf,
                            defect_analytics_pf,
                        ) in per_folder_matrices:
                            best_name_pf = _weekly_best_branch_name_for_report_week(
                                weekly_branch_schedule,
                                week_start_pf,
                                base_url=jira_weekly_rest_base,
                                auth_headers=jira_weekly_rest_headers,
                                abtest_issues=weekly_abtest_issues,
                            )
                            best_branch_column_pf = _weekly_best_branch_column_context_for_week(
                                (
                                    week_start_pf,
                                    weekday_labels_pf,
                                    rows_pf,
                                    cell_all_ne_pf,
                                    cell_all_blocked_pf,
                                    column_status_counts_pf,
                                    defect_keys_pf,
                                    cycle_keys_by_label_pf,
                                    defect_analytics_pf,
                                ),
                                report_data=single_folder_data,
                                best_branch_name=best_name_pf,
                                report_week_start=week_start_pf,
                            )
                            weekly_readable_paths.extend(
                                write_weekly_readable_reports(
                                    output_dir=args.weekly_readable_dir,
                                    week_start=week_start_pf,
                                    weekday_labels=weekday_labels_pf,
                                    rows=rows_pf,
                                    formats=selected_weekly_formats,
                                    cell_all_not_executed=cell_all_ne_pf,
                                    cell_all_blocked=cell_all_blocked_pf,
                                    column_status_counts=column_status_counts_pf,
                                    defect_keys=defect_keys_pf,
                                    cycle_keys_by_label=cycle_keys_by_label_pf,
                                    defect_analytics=defect_analytics_pf,
                                    defect_meta=defect_meta,
                                    template_dir=readable_template_dir,
                                    folder_id_resolve=str(folder_id_pf),
                                    folder_id_mapping=str(folder_id_pf),
                                    folder_name_mapping=str(folder_name_pf),
                                    filename_suffix=suffix,
                                    best_branch_column=best_branch_column_pf,
                                )
                            )
                if "html" in selected_weekly_formats:
                    weekly_html_publish_paths = _expected_weekly_readable_html_paths(
                        args.weekly_readable_dir,
                        report_data_for_matrix,
                        per_folder=args.weekly_readable_per_folder,
                    )
                timings.record(
                    "write weekly readable reports",
                    time.perf_counter() - weekly_readable_started_at,
                    f"files={len(weekly_readable_paths)}",
                )
            if confluence_cfg and (
                publish_confluence_daily
                or publish_confluence_weekly
                or publish_confluence_bugs
            ):
                with timed_step(timings, "resolve Confluence publish roots"):
                    confluence_publish_roots = resolve_confluence_publish_roots(
                        confluence_cfg
                    )
                    confluence_week_parents = ConfluenceWeekParentCache(
                        confluence_cfg, confluence_publish_roots.root_parent
                    )
                print("Confluence publish layout:")
                print(
                    f"- Root parent: {confluence_publish_roots.root_parent} "
                    f"(week folders: {confluence_cfg.week_folder_title_template})"
                )
                print(
                    f"- Bugs folder: {confluence_publish_roots.bugs_parent} "
                    f"({confluence_cfg.bugs_parent_title})"
                )
            if confluence_cfg and publish_confluence_daily and args.export_daily_readable:
                daily_html_publish_paths = _merge_confluence_publish_paths(
                    daily_html_publish_paths,
                    args.daily_readable_dir,
                    list_on_disk=_list_daily_readable_html_paths,
                )
            if confluence_cfg and publish_confluence_weekly and args.export_weekly_readable:
                weekly_html_publish_paths = _merge_confluence_publish_paths(
                    weekly_html_publish_paths,
                    args.weekly_readable_dir,
                    list_on_disk=_list_weekly_readable_html_paths,
                )
            if confluence_cfg and publish_confluence_bugs and args.export_build_log_report:
                build_log_html_publish_paths = _merge_confluence_publish_paths(
                    build_log_html_publish_paths,
                    args.build_log_report_dir,
                    list_on_disk=_list_build_log_html_publish_paths,
                )
            if confluence_cfg and publish_confluence_bugs:
                bugs_rollup_dir = (
                    os.getenv("ZEPHYR_BUGS_ROLLUP_DIR", "reports/bugs_rollup").strip()
                    or "reports/bugs_rollup"
                )
                bugs_rollup_html_publish_paths = _merge_confluence_publish_paths(
                    bugs_rollup_html_publish_paths,
                    bugs_rollup_dir,
                    list_on_disk=_list_bugs_rollup_html_paths,
                )
            if confluence_cfg and publish_confluence_daily:
                if not args.export_daily_readable:
                    print(
                        "Confluence daily publish skipped: --export-daily-readable not enabled."
                    )
                elif not daily_html_publish_paths:
                    daily_fmt = set(args.daily_readable_format or ["html", "wiki"])
                    if "html" not in daily_fmt:
                        print(
                            "Confluence daily publish skipped: daily readable format does not "
                            "include html."
                        )
                    else:
                        print(
                            "Confluence daily publish skipped: no daily HTML files "
                            f"under {args.daily_readable_dir}."
                        )
                else:
                    existing_daily_html = [p for p in daily_html_publish_paths if os.path.isfile(p)]
                    missing_daily = [p for p in daily_html_publish_paths if not os.path.isfile(p)]
                    if missing_daily:
                        print(
                            "Confluence publish warning: "
                            f"{len(missing_daily)} expected daily HTML file(s) missing on disk."
                        )
                    if existing_daily_html:
                        if not confluence_week_parents:
                            print(
                                "Confluence daily publish skipped: week folder cache not "
                                "initialized (set ZEPHYR_CONFLUENCE_PARENT_PAGE_ID)."
                            )
                        else:
                            with timed_step(
                                timings,
                                "publish daily reports to Confluence",
                                f"files={len(existing_daily_html)}",
                            ):
                                outcomes = publish_reports_to_confluence_by_week(
                                    existing_daily_html,
                                    confluence_cfg,
                                    week_parents=confluence_week_parents,
                                    fallback_parent=(
                                        confluence_publish_roots.root_parent
                                        if confluence_publish_roots
                                        else None
                                    ),
                                )
                            print("Confluence daily publish (under Week wNN):")
                            for line in outcomes:
                                print(f"- {line}")
                    else:
                        print(
                            "Confluence daily publish skipped: no HTML files found on disk at "
                            "expected paths."
                        )
            if confluence_cfg and publish_confluence_weekly:
                if not args.export_weekly_readable:
                    print(
                        "Confluence weekly publish skipped: --export-weekly-readable not enabled."
                    )
                elif not weekly_html_publish_paths:
                    weekly_fmt = set(args.weekly_readable_format or ["html", "wiki"])
                    if "html" not in weekly_fmt:
                        print(
                            "Confluence weekly publish skipped: weekly readable format does "
                            "not include html."
                        )
                    else:
                        print(
                            "Confluence weekly publish skipped: no weekly HTML pages for "
                            "report_data."
                        )
                else:
                    existing_weekly_html = [
                        p for p in weekly_html_publish_paths if os.path.isfile(p)
                    ]
                    missing_weekly = [
                        p for p in weekly_html_publish_paths if not os.path.isfile(p)
                    ]
                    if missing_weekly:
                        print(
                            "Confluence publish warning: "
                            f"{len(missing_weekly)} expected weekly HTML file(s) missing on disk."
                        )
                    if existing_weekly_html:
                        if not confluence_week_parents:
                            print(
                                "Confluence weekly publish skipped: week folder cache not "
                                "initialized (set ZEPHYR_CONFLUENCE_PARENT_PAGE_ID)."
                            )
                        else:
                            with timed_step(
                                timings,
                                "publish weekly reports to Confluence",
                                f"files={len(existing_weekly_html)}",
                            ):
                                outcomes = publish_reports_to_confluence_by_week(
                                    existing_weekly_html,
                                    confluence_cfg,
                                    week_parents=confluence_week_parents,
                                    fallback_parent=(
                                        confluence_publish_roots.root_parent
                                        if confluence_publish_roots
                                        else None
                                    ),
                                )
                            print("Confluence weekly publish (under Week wNN):")
                            for line in outcomes:
                                print(f"- {line}")
                    else:
                        print(
                            "Confluence weekly publish skipped: no HTML files found on disk at "
                            "expected paths."
                        )
            if confluence_cfg and publish_confluence_bugs:
                build_log_fmt = set(
                    args.build_log_report_format
                    or _env_csv_values(
                        "ZEPHYR_BUILD_LOG_REPORT_FORMATS", ["html", "wiki"]
                    )
                )
                if "html" not in build_log_fmt:
                    print(
                        "Confluence bugs publish skipped: build log format does not "
                        "include html."
                    )
                else:
                    existing_bug_html = (
                        [p for p in build_log_html_publish_paths if os.path.isfile(p)]
                        if args.export_build_log_report
                        else []
                    )
                    missing_bugs = (
                        [p for p in build_log_html_publish_paths if not os.path.isfile(p)]
                        if args.export_build_log_report
                        else []
                    )
                    if missing_bugs:
                        print(
                            "Confluence publish warning: "
                            f"{len(missing_bugs)} expected build-log HTML file(s) missing on disk."
                        )
                    existing_rollup_html = [
                        p for p in bugs_rollup_html_publish_paths if os.path.isfile(p)
                    ]
                    publish_bug_html = existing_bug_html + existing_rollup_html
                    if publish_bug_html:
                        with timed_step(
                            timings,
                            "publish bug reports to Confluence",
                            f"files={len(publish_bug_html)}",
                        ):
                            outcomes = publish_reports_to_confluence(
                                publish_bug_html,
                                confluence_cfg,
                                parent_page_id=(
                                    confluence_publish_roots.bugs_parent
                                    if confluence_publish_roots
                                    else None
                                ),
                            )
                        print("Confluence bugs publish:")
                        for line in outcomes:
                            print(f"- {line}")
                    else:
                        print(
                            "Confluence bugs publish skipped: no build-log or bugs rollup "
                            "HTML files found on disk."
                        )
            print(f"Saved summary CSV: {args.output}")
            print(f"Saved per-folder CSV directory: {args.per_folder_dir}")
            if args.export_cycles_cases:
                print(f"Saved cycles/cases CSV: {args.cycles_cases_output}")
            if args.export_case_steps:
                print(f"Saved case steps CSV: {args.case_steps_output}")
            if args.export_daily_readable:
                print(f"Saved daily readable reports: {args.daily_readable_dir}")
                print(f"Daily readable files updated: {len(daily_readable_paths)}")
            if args.export_build_log_report:
                print(f"Saved build log reports: {args.build_log_report_dir}")
                print(f"Build log report files written: {len(build_log_report_paths)}")
            if bugs_rollup_report_paths:
                bugs_rollup_dir = (
                    os.getenv("ZEPHYR_BUGS_ROLLUP_DIR", "reports/bugs_rollup").strip()
                    or "reports/bugs_rollup"
                )
                print(f"Saved bugs rollup index reports: {bugs_rollup_dir}")
                print(f"Bugs rollup files written: {len(bugs_rollup_report_paths)}")
            if args.cycle_progress_output:
                status = (
                    "content changed"
                    if cycle_progress_csv_updated
                    else "unchanged (skipped write)"
                )
                print(f"Cycle progress CSV: {args.cycle_progress_output} ({status})")
            if args.weekly_cycle_matrix_output:
                if not weekly_matrix_csv_updates:
                    print(f"Weekly cycle matrix CSV: {args.weekly_cycle_matrix_output} (no data)")
                else:
                    changed_count = sum(1 for _path, updated in weekly_matrix_csv_updates if updated)
                    print(
                        "Weekly cycle matrix CSV files: "
                        f"{len(weekly_matrix_csv_updates)} total, {changed_count} updated"
                    )
                    for out_path, updated in weekly_matrix_csv_updates:
                        status = "content changed" if updated else "unchanged (skipped write)"
                        print(f"- {out_path} ({status})")
            if args.export_weekly_readable:
                print(f"Saved weekly readable reports: {args.weekly_readable_dir}")
                print(f"Weekly readable files updated: {len(weekly_readable_paths)}")
            print(f"Processed folders: {len(folder_rows)}")
            print(f"Fetched executions: {total_executions}")
            if total_skipped:
                print(f"Skipped records: {dict(total_skipped)}")
            if errors:
                print("Folder errors:")
                for item in errors:
                    print(f"- {item}")
            timings.summarize()
            return 0

        executions = fetch_executions(
            base_url=args.base_url,
            endpoint=args.endpoint,
            headers=headers,
            extra_params=extra_params,
            page_size=args.page_size,
        )

        weekly, skipped = aggregate_weekly(
            items=executions,
            date_fields=date_fields,
            status_fields=status_fields,
            from_date=from_date,
            to_date=to_date,
        )

        write_csv(args.output, weekly)
        print_readable_report(weekly)
        print(f"\nSaved CSV: {args.output}")
        print(f"Fetched executions: {len(executions)}")
        if skipped:
            print(f"Skipped records: {dict(skipped)}")
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        return 1


class _TeeIO:
    """Write to multiple text streams (console + log file)."""

    def __init__(self, *streams: TextIO) -> None:
        self._streams = streams

    def write(self, data: str) -> int:
        for stream in self._streams:
            stream.write(data)
            stream.flush()
        return len(data)

    def flush(self) -> None:
        for stream in self._streams:
            stream.flush()

    def isatty(self) -> bool:
        return bool(getattr(self._streams[0], "isatty", lambda: False)())


def _log_to_file_enabled() -> bool:
    raw = os.getenv("ZEPHYR_LOG_TO_FILE")
    if raw is None or not str(raw).strip():
        return True
    return _parse_bool_env(raw)


def _log_retention_days() -> int:
    raw = (os.getenv("ZEPHYR_LOG_RETENTION_DAYS") or "").strip()
    if not raw.isdigit():
        return 7
    return int(raw)


def _prune_old_zephyr_logs(log_dir: Path, retention_days: int) -> None:
    if retention_days <= 0 or not log_dir.is_dir():
        return
    cutoff = datetime.now().timestamp() - retention_days * 86400
    for path in log_dir.glob("zephyr_*.log"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except OSError:
            pass


def _restore_stdio_and_close_log(
    orig_stdout: TextIO, orig_stderr: TextIO, log_f: TextIO
) -> None:
    sys.stdout = orig_stdout
    sys.stderr = orig_stderr
    try:
        log_f.flush()
        log_f.close()
    except OSError:
        pass


def _maybe_setup_run_log_file() -> None:
    if not _log_to_file_enabled():
        return
    script_dir = Path(__file__).resolve().parent
    dir_raw = (os.getenv("ZEPHYR_LOG_DIR") or "logs").strip() or "logs"
    log_dir = Path(dir_raw) if Path(dir_raw).is_absolute() else (script_dir / dir_raw)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return
    _prune_old_zephyr_logs(log_dir, _log_retention_days())
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = log_dir / f"zephyr_{stamp}.log"
    orig_out, orig_err = sys.__stdout__, sys.__stderr__
    try:
        log_f = log_path.open(
            "w",
            encoding="utf-8",
            errors="replace",
            newline="",
            buffering=1,
        )
    except OSError:
        return
    for stream in (orig_out, orig_err):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(line_buffering=True)
            except (OSError, ValueError):
                pass
    sys.stdout = _TeeIO(orig_out, log_f)
    sys.stderr = _TeeIO(orig_err, log_f)
    atexit.register(_restore_stdio_and_close_log, orig_out, orig_err, log_f)


if __name__ == "__main__":
    _maybe_setup_run_log_file()
    raise SystemExit(main())

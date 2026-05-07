#!/usr/bin/env python3
"""Generate a weekly Zephyr test execution summary.

The script fetches paginated execution data from a Zephyr API endpoint,
aggregates executions by ISO week (Monday start) using raw API statuses,
and computes normalized pass rate for reporting.
"""

from __future__ import annotations

import argparse
import base64
import io
import csv
import html
import json
import math
import mimetypes
import os
import re
import sys
import uuid
import zlib
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Callable


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
    title_prefix: str = ""
    api_prefix: str = "rest/api"
    auth_scheme: str = "basic"
    update_existing: bool = False
    publish_daily: bool = False
    publish_weekly: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Zephyr executions and build weekly pass/fail report."
    )
    parser.add_argument(
        "--base-url",
        required=True,
        help="Zephyr base URL, e.g. https://api.zephyrscale.smartbear.com",
    )
    parser.add_argument(
        "--endpoint",
        default="/v2/testexecutions",
        help="API endpoint path (default: /v2/testexecutions)",
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
        "--rolling-days",
        type=int,
        default=0,
        help=(
            "When >0 with --from-date and --to-date, filter aggregated readable "
            "report_data by resolved folder day."
        ),
    )
    parser.add_argument(
        "--regenerate-last-7-days",
        action="store_true",
        help=(
            "Optional convenience mode: set --from-date/--to-date to the last 7 days "
            "(today inclusive) and apply --rolling-days=7 when not provided."
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
    return parser.parse_args()


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
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HTTP {exc.code} while requesting '{url}' [{method.upper()}]. Response: {body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Network error while requesting '{url}': {exc}") from exc


def _parse_bool_env(value: str | None) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


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
    update_existing = _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_UPDATE_EXISTING"))
    if not (publish_daily or publish_weekly):
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
        title_prefix=title_prefix,
        api_prefix=api_prefix,
        auth_scheme=auth_scheme,
        update_existing=update_existing,
        publish_daily=publish_daily,
        publish_weekly=publish_weekly,
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
        params={"spaceKey": cfg.space_key, "title": title, "expand": "version"},
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


def _confluence_upsert_storage_page(
    cfg: ConfluencePublishConfig,
    title: str,
    storage_html: str,
    *,
    legacy_title: str | None = None,
) -> tuple[str, str]:
    existing = _confluence_find_page_by_title(cfg, title)
    if (
        existing is None
        and cfg.update_existing
        and legacy_title
        and legacy_title != title
    ):
        existing = _confluence_find_page_by_title(cfg, legacy_title)
    if existing:
        page_id, current_version = existing
        body = {
            "id": page_id,
            "type": "page",
            "title": title,
            "space": {"key": cfg.space_key},
            "version": {"number": current_version + 1},
            "body": {"storage": {"value": storage_html, "representation": "storage"}},
        }
        _confluence_request_json(cfg, f"{cfg.api_prefix}/content/{page_id}", method="PUT", body=body)
        return page_id, "updated"
    create_body: dict[str, Any] = {
        "type": "page",
        "title": title,
        "space": {"key": cfg.space_key},
        "body": {"storage": {"value": storage_html, "representation": "storage"}},
    }
    if cfg.parent_page_id:
        create_body["ancestors"] = [{"id": cfg.parent_page_id}]
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
        attrs = (match.group("before") or "") + (match.group("after") or "")
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
    html_paths: list[str], cfg: ConfluencePublishConfig
) -> list[str]:
    outcomes: list[str] = []
    for html_path in html_paths:
        with open(html_path, encoding="utf-8") as source:
            raw_html = source.read()
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


def fetch_testrun_items(
    base_url: str, headers: dict[str, str], test_run_id: str
) -> list[dict[str, Any]]:
    endpoint = f"rest/tests/1.0/testrun/{test_run_id}/testrunitems"
    params = {"fields": "id,index,issueCount,$lastTestResult"}
    payload = request_json(base_url, endpoint, headers, params=params, method="GET")
    if isinstance(payload, dict):
        items = payload.get("testRunItems")
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
    return []


def fetch_test_results_for_item(
    base_url: str,
    headers: dict[str, str],
    test_run_id: str,
    item_id: str,
) -> list[dict[str, Any]]:
    endpoint = f"rest/tests/1.0/testrun/{test_run_id}/testresults"
    params = {
        "fields": (
            "id,testResultStatusId,executionDate,comment,"
            "traceLinks,"
            "testScriptResults(id,testResultStatusId,executionDate,comment,index,description,expectedResult,testData,traceLinks)"
        ),
        "itemId": item_id,
    }
    payload = request_json(base_url, endpoint, headers, params=params, method="GET")
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        extracted = extract_items(payload)
        if extracted:
            return extracted
    return []


def _collect_task_links(raw_links: Any) -> list[str]:
    links: list[str] = []
    if not isinstance(raw_links, list):
        return links
    for entry in raw_links:
        if isinstance(entry, str) and entry.strip():
            links.append(entry.strip())
            continue
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("key") or "").strip()
        url = str(entry.get("url") or entry.get("href") or "").strip()
        text = key or url
        if text:
            links.append(text)
    return links


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
) -> list[list[str]]:
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
            if isinstance(last_result, dict):
                nested_case = last_result.get("testCase")
                if isinstance(nested_case, dict):
                    case_id = str(nested_case.get("id") or "")
                    case_key = str(nested_case.get("key") or "")
                    case_name = str(nested_case.get("name") or "")
                    case_objective = str(nested_case.get("objective") or "")

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
                test_result_links = _collect_task_links(test_result.get("traceLinks"))
                script_results = test_result.get("testScriptResults")

                if isinstance(script_results, list) and script_results:
                    for step in script_results:
                        if not isinstance(step, dict):
                            continue
                        step_status_id = str(step.get("testResultStatusId") or "")
                        step_status = status_names.get(step_status_id, step_status_id)
                        step_links = _collect_task_links(step.get("traceLinks"))
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
                        ]
                    )
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


def aggregate_readable_daily_reports_from_steps(
    case_steps_rows: list[list[str]],
    cycles_cases_rows: list[list[str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Group folder -> test cycle (test_run_id) -> real test cases with result and comment from steps.
    Result priority: step_status_name, then test_result_status_name, then cycles CSV fallback.
    """
    fallback_status = build_cycle_run_fallback_status(cycles_cases_rows)
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

        if not folder_id or not folder_name or not test_run_id or not test_case_key:
            continue

        mkey = (folder_id, folder_name, test_run_id, test_case_key)
        if mkey not in case_merge:
            case_merge[mkey] = {
                "test_case_name": test_case_name or "",
                "cycle_key": cycle_key_disp or "",
                "cycle_name": cycle_name_disp or "",
                "cycle_objective": cycle_objective or "",
                "step_status_name": step_status_name or "",
                "test_result_status_name": test_result_status_name or "",
                "step_comment": step_comment.strip() if step_comment else "",
                "step_execution_date": step_execution_date or "",
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
            if step_comment.strip() and not m["step_comment"]:
                m["step_comment"] = step_comment.strip()
            if step_execution_date and step_execution_date > (m["step_execution_date"] or ""):
                m["step_execution_date"] = step_execution_date
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
        cycle_bucket["cases"][test_case_key] = {
            "test_case_key": test_case_key,
            "test_case_name": m["test_case_name"],
            "result": result,
            "execution_date": m["step_execution_date"],
            "comment": m["step_comment"],
            "objective": m["objective"],
            "tasks": m["task_links"],
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
            "execution_date": cycle_updated_on,
            "comment": "",
            "objective": "",
            "tasks": "",
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
) -> dict[str, str]:
    week_label = week_start.isoformat() if week_start else "N/A"
    return {
        "folder_id": escape(str(folder_id)),
        "folder_name": escape(str(folder_name)),
        "folder_name_slug": escape(slugify(folder_name)),
        "week_start": escape(week_label),
        "week_label": escape(week_label),
    }


def _format_readable_html_preamble(
    template_dir: str | None,
    kind: str,
    folder_id_resolve: str | None,
    folder_id_mapping: str,
    folder_name: str,
    week_start: date | None,
) -> str:
    raw = _resolve_readable_template(template_dir, kind, "html", folder_id_resolve)
    if not raw or not raw.strip():
        return ""
    mapping = _readable_template_mapping(
        folder_id_mapping, folder_name, week_start, html.escape
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
) -> str:
    raw = _resolve_readable_template(template_dir, kind, "wiki", folder_id_resolve)
    if not raw or not raw.strip():
        return ""
    mapping = _readable_template_mapping(
        folder_id_mapping, folder_name, week_start, _wiki_escape
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


def _jira_cycle_url(cycle_key: str) -> str:
    base_url = os.getenv("ZEPHYR_BASE_URL", "https://jira.navio.auto").rstrip("/")
    return f"{base_url}/secure/Tests.jspa#/testCycle/{urllib.parse.quote(cycle_key)}"


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


def _release_week_start(day: date) -> date:
    offset = (day.weekday() - 3) % 7
    return day - timedelta(days=offset)


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
) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for key, payload in report_data.items():
        _folder_id, folder_name = key
        cycles = payload.get("cycles", {})
        if not isinstance(cycles, dict):
            continue
        rd = _resolve_folder_report_day(folder_name, cycles)
        if rd is None:
            out[key] = payload
        elif from_date <= rd <= to_date:
            out[key] = payload
    return out


def _filter_tree_folders_by_report_day(
    folders: list[FolderNode], from_date: date, to_date: date
) -> list[FolderNode]:
    out: list[FolderNode] = []
    for folder in folders:
        folder_day = _parse_report_day_from_folder_name(folder.folder_name)
        if folder_day is None:
            # Keep unknown names to avoid accidental data loss.
            out.append(folder)
            continue
        if from_date <= folder_day <= to_date:
            out.append(folder)
    return out


def _weekly_cycle_matrix_data(
    report_data: dict[tuple[str, str], dict[str, Any]]
) -> tuple[date | None, list[str], list[list[str]], list[list[bool]], list[list[bool]]]:
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
        week_start = _release_week_start(report_day)
        weekly_groups[week_start].append(
            {
                "folder_id": str(folder_id),
                "report_day": report_day,
                "column_label": _parse_weekly_column_label_from_folder_name(folder_name),
                "progress_rows": progress_rows,
            }
        )

    if not weekly_groups:
        return None, [], [], [], []

    def _aggregate_progress_map(
        progress_rows: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
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
            # Prefer non-cloned row; use cloned only as fallback when base is absent.
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

    # Export the latest available week (matches README behavior).
    target_week_start = max(weekly_groups.keys())
    week_daily_summaries = sorted(
        weekly_groups[target_week_start],
        key=lambda item: (item["report_day"], item["folder_id"]),
    )
    progress_rows_by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    column_labels_by_day: dict[date, list[str]] = defaultdict(list)
    for summary in week_daily_summaries:
        progress_rows_by_day[summary["report_day"]].extend(summary["progress_rows"])
        column_label = str(summary.get("column_label") or "").strip()
        if column_label:
            column_labels_by_day[summary["report_day"]].append(column_label)
    ordered_days = sorted(progress_rows_by_day.keys())
    column_labels: list[str] = []
    for day in ordered_days:
        day_labels = column_labels_by_day.get(day, [])
        if day_labels:
            column_labels.append(Counter(day_labels).most_common(1)[0][0])
        else:
            column_labels.append(f"nightly-dev-{day.strftime('%Y.%m.%d')}")

    day_maps: dict[date, dict[str, dict[str, Any]]] = {}
    joined_passed_by_label: dict[str, dict[str, int]] = {}
    joined_totals_by_label: dict[str, dict[str, int]] = {}
    joined_all_not_executed_by_label: dict[str, dict[str, bool]] = {}
    joined_all_blocked_by_label: dict[str, dict[str, bool]] = {}
    for day_date, day_label in zip(ordered_days, column_labels):
        day_map = _aggregate_progress_map(progress_rows_by_day[day_date])
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
        return target_week_start, column_labels, [], [], []

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

    # Insert group subtotal row before each scenario group.
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

        group_title = _summary_group_title_from_labels(group_labels, fallback_group=group_id)
        subtotal_row = [f"Итого: {group_title}", str(group_total_cases)]
        subtotal_row.extend(str(value) for value in group_day_sums)
        grouped_rows.append(subtotal_row)
        grouped_ne_flags.append([False] * len(column_labels))
        grouped_blocked_flags.append([False] * len(column_labels))

        for copy_idx in range(group_start, index):
            grouped_rows.append(rows[copy_idx])
            grouped_ne_flags.append(cell_all_not_executed[copy_idx])
            grouped_blocked_flags.append(cell_all_blocked[copy_idx])

    return target_week_start, column_labels, grouped_rows, grouped_ne_flags, grouped_blocked_flags


def _weekly_cycle_matrix_rows(report_data: dict[tuple[str, str], dict[str, Any]]) -> list[list[str]]:
    _, _, rows, _, _ = _weekly_cycle_matrix_data(report_data)
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
        week_start = _release_week_start(report_day)
        grouped[week_start][(folder_id, folder_name)] = payload
    return grouped


def _weekly_cycle_matrix_data_all(
    report_data: dict[tuple[str, str], dict[str, Any]]
) -> list[tuple[date | None, list[str], list[list[str]], list[list[bool]], list[list[bool]]]]:
    by_week = _split_report_data_by_week(report_data)
    matrices: list[tuple[date | None, list[str], list[list[str]], list[list[bool]], list[list[bool]]]] = []
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
    week_label = week_start.isoformat() if week_start else "N/A"
    labels = list(weekday_labels)
    present_days: list[date] = []
    for label in labels:
        match = re.search(r"\b(\d{4}\.\d{2}\.\d{2})\b", str(label))
        if not match:
            continue
        try:
            present_days.append(datetime.strptime(match.group(1), "%Y.%m.%d").date())
        except ValueError:
            continue
    if present_days:
        range_start = min(present_days)
        range_end = max(present_days)
        week_no = int(range_start.isocalendar()[1])
        title_start = range_start + timedelta(days=1)
        title_end = range_end + timedelta(days=1)
        return (
            f"Weekly W_{week_no:02d}. {title_start.strftime('%d.%m.%Y')} - "
            f"{title_end.strftime('%d.%m.%Y')}"
        )
    return f"Weekly W_??. {week_label}"


def render_weekly_html_report(
    week_start: date | None,
    weekday_labels: list[str],
    rows: list[list[str]],
    cell_all_not_executed: list[list[bool]] | None = None,
    cell_all_blocked: list[list[bool]] | None = None,
    *,
    template_dir: str | None = None,
    folder_id_resolve: str | None = None,
    folder_id_mapping: str = "",
    folder_name_mapping: str = "",
) -> str:
    labels = list(weekday_labels)
    title_text = _weekly_matrix_title_text(week_start, weekday_labels)
    header_cells = ["<th>Тестовый цикл</th>", "<th>Всего кейсов</th>"]
    header_cells.extend(f"<th>{html.escape(label)}</th>" for label in labels)
    preamble = _format_readable_html_preamble(
        template_dir,
        "weekly",
        folder_id_resolve,
        folder_id_mapping,
        folder_name_mapping,
        week_start,
    )
    sections = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        f"<title>{html.escape(title_text)}</title>",
        (
            "<style>"
            "body{font-family:Arial,sans-serif;margin:24px;}"
            "h1{margin-bottom:8px;}table{border-collapse:collapse;width:100%;margin-bottom:16px;table-layout:fixed;}"
            "th,td{border:1px solid #d6d6d6;padding:6px 8px;text-align:left;vertical-align:top;overflow-wrap:anywhere;word-wrap:break-word;}"
            "th{background:#f0f2f5;font-weight:600;}.passed-count-cell{font-weight:400;text-align:center;}"
            ".total-cases-cell{text-align:center;}"
            ".group-total-row td{font-weight:700;background:#ffffff;color:#1f2328;}"
            ".group-total-row td:not(:first-child){text-align:center;}"
            ".scenario-sep td{border:none;height:8px;padding:0;background:transparent;}"
            ".report-preamble{margin:12px 0 20px;}"
            "</style>"
        ),
        "</head><body>",
        f"<h1>{html.escape(title_text)}</h1>",
    ]
    if preamble:
        sections.append(preamble)
    sections.extend(
        [
            "<table>",
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
            total_cells.extend(
                (
                    "<td style='font-weight:700;background:#ffffff;color:#1f2328;text-align:center;'>"
                    f"{html.escape(row[2 + idx] if 2 + idx < len(row) else '0')}</td>"
                )
                for idx in range(len(labels))
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
        for idx in range(len(labels)):
            passed_value = int(row[2 + idx]) if 2 + idx < len(row) else 0
            all_ne = bool(ne_row[idx]) if idx < len(ne_row) else False
            all_blocked = bool(blocked_row[idx]) if idx < len(blocked_row) else False
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
    sections.extend(["</tbody></table>", "</body></html>"])
    return "\n".join(sections)


def render_weekly_wiki_report(
    week_start: date | None,
    weekday_labels: list[str],
    rows: list[list[str]],
    *,
    template_dir: str | None = None,
    folder_id_resolve: str | None = None,
    folder_id_mapping: str = "",
    folder_name_mapping: str = "",
) -> str:
    title_text = _weekly_matrix_title_text(week_start, weekday_labels)
    labels = list(weekday_labels)
    lines = [f"h1. {_wiki_escape(title_text)}", ""]
    wiki_pre = _format_readable_wiki_preamble(
        template_dir,
        "weekly",
        folder_id_resolve,
        folder_id_mapping,
        folder_name_mapping,
        week_start,
    )
    if wiki_pre:
        lines.extend(wiki_pre.splitlines())
        lines.append("")
    header_cells = ["Тестовый цикл", "Всего кейсов"]
    header_cells.extend(f"{_wiki_escape(label)}" for label in labels)
    lines.append("|| " + " || ".join(header_cells) + " ||")
    for row in rows:
        if str(row[0]).startswith("Итого:"):
            row_values = [f"*{row[0]}*", f"*{row[1]}*"]
            for idx in range(len(labels)):
                row_values.append(f"*{row[2 + idx] if 2 + idx < len(row) else '0'}*")
            lines.append("| " + " | ".join(_wiki_escape(str(value)) for value in row_values) + " |")
            continue
        row_values = [row[0], row[1]]
        for idx in range(len(labels)):
            row_values.append(row[2 + idx] if 2 + idx < len(row) else "0")
        lines.append(
            "| "
            + " | ".join(_wiki_escape(str(value)) for value in row_values)
            + " |"
        )
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
    template_dir: str | None = None,
    folder_id_resolve: str | None = None,
    folder_id_mapping: str = "",
    folder_name_mapping: str = "",
    filename_suffix: str = "",
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
            template_dir=template_dir,
            folder_id_resolve=folder_id_resolve,
            folder_id_mapping=folder_id_mapping,
            folder_name_mapping=folder_name_mapping,
        )
        if _write_text_if_changed(html_path, html_body):
            updated_paths.append(html_path)
    if "wiki" in formats:
        wiki_path = os.path.join(output_dir, f"{base_name}.confluence.txt")
        wiki_body = render_weekly_wiki_report(
            week_start,
            weekday_labels,
            rows,
            template_dir=template_dir,
            folder_id_resolve=folder_id_resolve,
            folder_id_mapping=folder_id_mapping,
            folder_name_mapping=folder_name_mapping,
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
            "<col style='width:8%'>"
            "<col style='width:8%'>"
            "<col style='width:10%'>"
            "<col style='width:14%'>"
            "</colgroup>"
            "<thead><tr>"
            "<th>Название</th><th>Критерий валидации</th><th>Тестовый прогон</th>"
            "<th>Статус</th><th>Дата</th><th>Комментарий</th><th>Задачи</th><th>Результат</th>"
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
                display_date = _render_report_date(case)
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
                    f"<td>{html.escape(display_date)}</td>"
                    f"<td>{_html_comment_cell(case.get('comment', ''))}</td>"
                    f"<td>{_html_comment_cell(case.get('tasks', ''))}</td>"
                    f"{result_col}"
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
            "|| Название || Критерий валидации || Тестовый прогон || Статус || Дата || Комментарий || Задачи ||"
        )
        sorted_cases, criterion_spans = _prepare_cycle_cases_with_groups(cycle)
        for idx, case in enumerate(sorted_cases):
            res = case.get("result", case.get("test_case_status", ""))
            exd = _render_report_date(case)
            cmt = _wiki_text_with_links(case.get("comment", ""))
            tasks = _wiki_text_with_links(case.get("tasks", ""))
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
                        _wiki_escape(exd),
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
        base_name = f"{slugify(folder_name)}_{folder_id}"
        cycles = payload["cycles"]
        fid = str(folder_id)
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


def _expected_daily_readable_html_paths(
    output_dir: str,
    report_data: dict[tuple[str, str], dict[str, Any]],
) -> list[str]:
    paths: list[str] = []
    for (folder_id, folder_name), _payload in sorted(
        report_data.items(), key=lambda item: item[0][1]
    ):
        base_name = f"{slugify(folder_name)}_{folder_id}"
        paths.append(os.path.join(output_dir, f"{base_name}.html"))
    return paths


def _expected_weekly_readable_html_paths(
    output_dir: str,
    report_data: dict[tuple[str, str], dict[str, Any]],
    *,
    per_folder: bool,
) -> list[str]:
    paths: list[str] = []
    for week_start, _wl, _rows, _ne, _blk in _weekly_cycle_matrix_data_all(report_data):
        week_label = week_start.isoformat() if week_start else "unknown_week"
        base_name = f"weekly_cycle_matrix_{week_label}"
        paths.append(os.path.join(output_dir, f"{base_name}.html"))
    if per_folder:
        for folder_key, payload in report_data.items():
            folder_id_pf, _folder_name_pf = folder_key
            suffix = f"_folder_{folder_id_pf}"
            single_folder_data = {folder_key: payload}
            for week_start, _wl, _rows, _ne, _blk in _weekly_cycle_matrix_data_all(
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


def main() -> int:
    args = parse_args()
    try:
        effective_rolling_days = args.rolling_days
        if args.regenerate_last_7_days:
            if args.from_date or args.to_date:
                raise ValueError(
                    "--regenerate-last-7-days cannot be used together with "
                    "--from-date/--to-date"
                )
            today = date.today()
            from_override = today - timedelta(days=6)
            to_override = today
            args.from_date = from_override.isoformat()
            args.to_date = to_override.isoformat()
            if effective_rolling_days <= 0:
                effective_rolling_days = 7
            print(
                "Date window override: last 7 days "
                f"({args.from_date} .. {args.to_date}), "
                f"rolling-days={effective_rolling_days}"
            )
        token = args.token or os.getenv("ZEPHYR_API_TOKEN")
        if not token:
            raise ValueError(
                "Missing API token. Pass --token or set ZEPHYR_API_TOKEN environment variable."
            )
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
            need_cycles_cases_data = args.export_cycles_cases or args.export_daily_readable
            collect_case_steps = args.export_case_steps or args.export_daily_readable
            total_executions = 0
            total_skipped = Counter()
            errors: list[str] = []
            status_names = (
                fetch_test_result_status_names(args.base_url, headers, args.project_id)
                if collect_case_steps
                else {}
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
                ) = resolve_folder_names_by_id(
                    folder_ids=folder_ids_from_executions,
                    endpoint_templates=args.folder_name_endpoint_template,
                    base_url=args.base_url,
                    headers=headers,
                )
                if args.debug_folder_fields:
                    print_resolved_folder_names(resolved_folder_names)
                    print_resolved_folder_paths(resolved_folder_paths)
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
                                )
                            )
                if filter_stats:
                    print(f"Execution discovery filter stats: {dict(filter_stats)}")
                if name_resolution_stats:
                    print(f"Folder name resolution stats: {dict(name_resolution_stats)}")
            else:
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
                        selected_folders, from_date, to_date
                    )
                    if len(selected_folders) != selected_before_day_filter:
                        print(
                            "Tree selected folders after day window filter "
                            f"({from_date} .. {to_date}): {len(selected_folders)}"
                        )
                print(
                    f"Tree folders discovered: {len(folders)}; selected after filters: {len(selected_folders)}"
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
                    print("Next: " + ", then ".join(roadmap_parts) + ".")

                for idx, folder in enumerate(selected_folders, start=1):
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
                    try:
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
                        folder_rows.append((folder, weekly))
                        total_executions += len(executions)
                        total_skipped.update(skipped)
                        if need_cycles_cases_data:
                            cycles_cases_rows.extend(
                                build_cycle_case_rows(
                                    folder=folder,
                                    cycles=executions,
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
                                    cycles=executions,
                                    base_url=args.base_url,
                                    headers=headers,
                                    status_names=status_names,
                                    synthetic_cycle_ids=args.synthetic_cycle_ids,
                                )
                            )
                        print(
                            f"[{idx}/{n_selected}] Folder {folder.folder_id} ({folder.folder_name}): "
                            f"done ({len(executions)} execution(s))"
                        )
                    except Exception as exc:  # pylint: disable=broad-except
                        message = f"Folder {folder.folder_id} ({folder.folder_name}) failed: {exc}"
                        if args.continue_on_folder_error:
                            errors.append(message)
                            print(f"[{idx}/{n_selected}] FAILED (continuing): {message}")
                            continue
                        raise RuntimeError(message) from exc

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
            if args.export_daily_readable:
                selected_formats = set(args.daily_readable_format or ["html", "wiki"])
                if case_steps_rows:
                    report_data = aggregate_readable_daily_reports_from_steps(
                        case_steps_rows, cycles_cases_rows
                    )
                else:
                    report_data = aggregate_readable_daily_reports_legacy(cycles_cases_rows)
                fmt_join = ", ".join(sorted(selected_formats))
                print(
                    f"Building daily readable reports for {len(report_data)} folder payload(s) "
                    f"(formats: {fmt_join})..."
                )
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
            needs_report_data = bool(
                args.cycle_progress_output
                or args.weekly_cycle_matrix_output
                or args.export_weekly_readable
            )
            if needs_report_data:
                if report_data is None:
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
                    report_data, from_date, to_date
                )
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
                tuple[date | None, list[str], list[list[str]], list[list[bool]], list[list[bool]]]
            ] = []
            if args.weekly_cycle_matrix_output:
                weekly_cycle_matrices = _weekly_cycle_matrix_data_all(report_data_for_matrix)
                if weekly_cycle_matrices:
                    (
                        weekly_cycle_week_start,
                        weekly_cycle_weekday_labels,
                        weekly_cycle_rows,
                        weekly_cycle_cell_all_ne,
                        weekly_cycle_cell_all_blocked,
                    ) = weekly_cycle_matrices[-1]
                    # Keep the legacy output path as the latest week for compatibility.
                    latest_updated = write_weekly_cycle_matrix_csv(
                        args.weekly_cycle_matrix_output,
                        weekly_cycle_weekday_labels,
                        weekly_cycle_rows,
                    )
                    weekly_matrix_csv_updates.append((args.weekly_cycle_matrix_output, latest_updated))
                for week_start, weekday_labels, rows, _cell_flags, _blocked_flags in weekly_cycle_matrices:
                    week_path = _weekly_output_path_for_week(args.weekly_cycle_matrix_output, week_start)
                    week_updated = write_weekly_cycle_matrix_csv(week_path, weekday_labels, rows)
                    weekly_matrix_csv_updates.append((week_path, week_updated))
            weekly_readable_paths: list[str] = []
            if args.export_weekly_readable:
                if not weekly_cycle_matrices:
                    weekly_cycle_matrices = _weekly_cycle_matrix_data_all(report_data_for_matrix)
                if weekly_cycle_matrices:
                    (
                        weekly_cycle_week_start,
                        weekly_cycle_weekday_labels,
                        weekly_cycle_rows,
                        weekly_cycle_cell_all_ne,
                        weekly_cycle_cell_all_blocked,
                    ) = weekly_cycle_matrices[-1]
                selected_weekly_formats = set(args.weekly_readable_format or ["html", "wiki"])
                for week_start, weekday_labels, rows, cell_all_ne, cell_all_blocked in weekly_cycle_matrices:
                    weekly_readable_paths.extend(
                        write_weekly_readable_reports(
                            output_dir=args.weekly_readable_dir,
                            week_start=week_start,
                            weekday_labels=weekday_labels,
                            rows=rows,
                            formats=selected_weekly_formats,
                            cell_all_not_executed=cell_all_ne,
                            cell_all_blocked=cell_all_blocked,
                            template_dir=readable_template_dir,
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
                        ) in per_folder_matrices:
                            weekly_readable_paths.extend(
                                write_weekly_readable_reports(
                                    output_dir=args.weekly_readable_dir,
                                    week_start=week_start_pf,
                                    weekday_labels=weekday_labels_pf,
                                    rows=rows_pf,
                                    formats=selected_weekly_formats,
                                    cell_all_not_executed=cell_all_ne_pf,
                                    cell_all_blocked=cell_all_blocked_pf,
                                    template_dir=readable_template_dir,
                                    folder_id_resolve=str(folder_id_pf),
                                    folder_id_mapping=str(folder_id_pf),
                                    folder_name_mapping=str(folder_name_pf),
                                    filename_suffix=suffix,
                                )
                            )
                if "html" in selected_weekly_formats:
                    weekly_html_publish_paths = _expected_weekly_readable_html_paths(
                        args.weekly_readable_dir,
                        report_data_for_matrix,
                        per_folder=args.weekly_readable_per_folder,
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
                            "Confluence daily publish skipped: no folder payloads (empty report_data)."
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
                        outcomes = publish_reports_to_confluence(
                            existing_daily_html, confluence_cfg
                        )
                        print("Confluence daily publish:")
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
                            "Confluence weekly publish skipped: weekly readable format does not "
                            "include html."
                        )
                    else:
                        print(
                            "Confluence weekly publish skipped: no weekly matrix data for "
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
                        outcomes = publish_reports_to_confluence(
                            existing_weekly_html, confluence_cfg
                        )
                        print("Confluence weekly publish:")
                        for line in outcomes:
                            print(f"- {line}")
                    else:
                        print(
                            "Confluence weekly publish skipped: no HTML files found on disk at "
                            "expected paths."
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


if __name__ == "__main__":
    raise SystemExit(main())

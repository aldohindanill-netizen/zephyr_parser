#!/usr/bin/env python3
"""Generate a weekly Zephyr test execution summary.

The script fetches paginated execution data from a Zephyr API endpoint,
normalizes statuses, aggregates by ISO week (Monday start), and exports CSV.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import sys
import ssl
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any


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
class WeeklyStat:
    total: int = 0
    passed: int = 0
    failed: int = 0
    blocked: int = 0
    not_executed: int = 0
    other: int = 0

    def to_row(self, week_start: date) -> list[str]:
        pass_rate = (self.passed / self.total * 100.0) if self.total else 0.0
        return [
            week_start.isoformat(),
            str(self.total),
            str(self.passed),
            str(self.failed),
            str(self.blocked),
            str(self.not_executed),
            str(self.other),
            f"{pass_rate:.2f}",
        ]


@dataclass
class FolderNode:
    folder_id: str
    folder_name: str
    parent_id: str | None
    full_path: str = ""
    is_leaf: bool = False


@dataclass
class ConfluenceConfig:
    base_url: str
    space_key: str
    parent_page_id: str
    username: str
    api_token: str
    auth_mode: str = "auto"
    verify_ssl: bool = True
    dry_run: bool = False
    update_existing: bool = False


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
        default="reports/cycle_progress.csv",
        help="CSV path for cycle progress summary (total and passed cases per cycle).",
    )
    parser.add_argument(
        "--weekly-cycle-matrix-output",
        default="reports/weekly_cycle_matrix.csv",
        help=(
            "CSV path for weekly cycle matrix "
            "(cycle, total, passed by weekday Monday-Friday)."
        ),
    )
    parser.add_argument(
        "--export-weekly-readable",
        action="store_true",
        help="Export weekly cycle matrix in readable html/wiki formats.",
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
        "--publish-confluence-daily",
        action="store_true",
        help="Publish daily HTML reports to Confluence.",
    )
    parser.add_argument(
        "--publish-confluence-weekly",
        action="store_true",
        help="Publish weekly HTML reports to Confluence.",
    )
    parser.add_argument(
        "--confluence-base-url",
        default=None,
        help="Confluence base URL, e.g. https://your-domain.atlassian.net/wiki",
    )
    parser.add_argument(
        "--confluence-space-key",
        default=None,
        help="Confluence space key for published pages.",
    )
    parser.add_argument(
        "--confluence-parent-page-id",
        default=None,
        help="Confluence parent page id for created report pages.",
    )
    parser.add_argument(
        "--confluence-username",
        default=None,
        help="Confluence username (Cloud: Atlassian email).",
    )
    parser.add_argument(
        "--confluence-api-token",
        default=None,
        help="Confluence API token / password (prefer environment variable).",
    )
    parser.add_argument(
        "--confluence-auth-mode",
        default=None,
        choices=("auto", "basic", "bearer"),
        help="Confluence auth mode: auto, basic, or bearer (default: auto).",
    )
    parser.add_argument(
        "--confluence-verify-ssl",
        default=None,
        choices=("true", "false"),
        help="Verify SSL certificates for Confluence requests (default: true).",
    )
    parser.add_argument(
        "--confluence-dry-run",
        action="store_true",
        help="Print Confluence actions but do not send API requests.",
    )
    parser.add_argument(
        "--confluence-update-existing",
        action="store_true",
        help="Update existing Confluence pages with the same title instead of skipping.",
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


def request_json_absolute_url(
    url: str,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    verify_ssl: bool = True,
) -> Any:
    query = urllib.parse.urlencode(params or {}, doseq=True)
    full_url = url
    if query and method.upper() == "GET":
        sep = "&" if "?" in full_url else "?"
        full_url = f"{full_url}{sep}{query}"
    request_headers = dict(headers)
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        full_url, headers=request_headers, method=method.upper(), data=payload
    )
    ssl_context = None if verify_ssl else ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(request, timeout=30, context=ssl_context) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HTTP {exc.code} while requesting '{full_url}' [{method.upper()}]. Response: {response_body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Network error while requesting '{full_url}': {exc}") from exc


def _parse_bool_value(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return default


def _normalize_confluence_base_url(value: str) -> str:
    return value.strip().rstrip("/")


def _confluence_content_api_candidates(base_url: str) -> list[str]:
    parsed = urllib.parse.urlparse(base_url)
    path = (parsed.path or "").rstrip("/").lower()
    if path.endswith("/wiki"):
        return [f"{base_url}/rest/api/content"]
    return [f"{base_url}/wiki/rest/api/content", f"{base_url}/rest/api/content"]


def _build_confluence_auth_headers(username: str, api_token: str) -> dict[str, str]:
    user_pass = f"{username}:{api_token}".encode("utf-8")
    # Confluence expects standard Basic auth base64; urllib has no direct helper here.
    # Use Python stdlib base64 while keeping dependencies minimal.
    import base64

    return {
        "Authorization": f"Basic {base64.b64encode(user_pass).decode('ascii')}",
        "Accept": "application/json",
    }


def _build_confluence_auth_headers_candidates(cfg: ConfluenceConfig) -> list[dict[str, str]]:
    bearer_headers = {
        "Authorization": f"Bearer {cfg.api_token}",
        "Accept": "application/json",
    }
    if cfg.auth_mode == "bearer":
        return [bearer_headers]
    if cfg.auth_mode == "basic":
        return [_build_confluence_auth_headers(cfg.username, cfg.api_token)]
    candidates: list[dict[str, str]] = [bearer_headers]
    if cfg.username:
        candidates.append(_build_confluence_auth_headers(cfg.username, cfg.api_token))
    return candidates


def _extract_html_body_for_confluence(raw_html: str) -> str:
    style_blocks = re.findall(r"<style[^>]*>.*?</style>", raw_html, flags=re.IGNORECASE | re.DOTALL)
    body_match = re.search(r"<body[^>]*>(.*)</body>", raw_html, flags=re.IGNORECASE | re.DOTALL)
    body_content = body_match.group(1).strip() if body_match else raw_html.strip()
    if style_blocks:
        return "\n".join(style_blocks + [body_content])
    return body_content


def _confluence_title_from_report_path(path: str) -> str:
    file_name = os.path.basename(path)
    stem = file_name.rsplit(".", 1)[0]
    if stem.startswith("weekly_cycle_matrix_"):
        suffix = stem.replace("weekly_cycle_matrix_", "", 1)
        return f"Weekly cycle matrix: {suffix}"
    if stem.startswith("nightly-dev-"):
        suffix = stem.replace("nightly-dev-", "", 1)
        return f"Daily report: {suffix}"
    return stem.replace("_", " ")


def _confluence_find_page(
    content_url: str,
    cfg: ConfluenceConfig,
    headers: dict[str, str],
    title: str,
) -> tuple[str, int] | None:
    payload = request_json_absolute_url(
        content_url,
        headers=headers,
        params={"spaceKey": cfg.space_key, "title": title, "expand": "version"},
        method="GET",
        verify_ssl=cfg.verify_ssl,
    )
    if not isinstance(payload, dict):
        return None
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        return None
    first = results[0]
    if not isinstance(first, dict):
        return None
    page_id = str(first.get("id") or "").strip()
    version = first.get("version")
    version_number = 0
    if isinstance(version, dict):
        raw_number = version.get("number")
        if isinstance(raw_number, int):
            version_number = raw_number
    if not page_id:
        return None
    return page_id, version_number


def publish_html_report_to_confluence(path: str, cfg: ConfluenceConfig) -> str:
    with open(path, "r", encoding="utf-8") as report_file:
        report_html = report_file.read()
    title = _confluence_title_from_report_path(path)
    storage_html = _extract_html_body_for_confluence(report_html)
    if cfg.dry_run:
        action = "upsert" if cfg.update_existing else "create"
        return f"DRY-RUN {action} page '{title}' from {path}"
    candidate_urls = _confluence_content_api_candidates(cfg.base_url)
    auth_headers_candidates = _build_confluence_auth_headers_candidates(cfg)
    last_error: Exception | None = None
    for headers in auth_headers_candidates:
        for content_url in candidate_urls:
            try:
                existing = _confluence_find_page(content_url, cfg, headers, title)
                if existing:
                    if not cfg.update_existing:
                        return f"SKIP already exists '{title}'"
                    page_id, current_version = existing
                    update_payload = {
                        "id": page_id,
                        "type": "page",
                        "title": title,
                        "version": {"number": max(current_version, 0) + 1},
                        "body": {
                            "storage": {"value": storage_html, "representation": "storage"}
                        },
                    }
                    request_json_absolute_url(
                        f"{content_url.rstrip('/')}/{page_id}",
                        headers=headers,
                        method="PUT",
                        body=update_payload,
                        verify_ssl=cfg.verify_ssl,
                    )
                    return f"UPDATED '{title}'"
                payload = {
                    "type": "page",
                    "title": title,
                    "space": {"key": cfg.space_key},
                    "ancestors": [{"id": cfg.parent_page_id}],
                    "body": {
                        "storage": {"value": storage_html, "representation": "storage"}
                    },
                }
                request_json_absolute_url(
                    content_url,
                    headers=headers,
                    method="POST",
                    body=payload,
                    verify_ssl=cfg.verify_ssl,
                )
                return f"CREATED '{title}'"
            except Exception as exc:  # pylint: disable=broad-except
                last_error = exc
                continue
    if last_error is not None:
        raise RuntimeError(f"Confluence publish failed for '{path}': {last_error}") from last_error
    raise RuntimeError(f"Confluence publish failed for '{path}'")


def load_confluence_config(args: argparse.Namespace) -> ConfluenceConfig:
    base_url = args.confluence_base_url or os.getenv("CONFLUENCE_BASE_URL", "")
    space_key = args.confluence_space_key or os.getenv("CONFLUENCE_SPACE_KEY", "")
    parent_page_id = args.confluence_parent_page_id or os.getenv("CONFLUENCE_PARENT_PAGE_ID", "")
    username = args.confluence_username or os.getenv("CONFLUENCE_USERNAME", "")
    api_token = args.confluence_api_token or os.getenv("CONFLUENCE_API_TOKEN", "")
    auth_mode = (
        (args.confluence_auth_mode or os.getenv("CONFLUENCE_AUTH_MODE", "auto"))
        .strip()
        .lower()
    )
    verify_ssl_raw = args.confluence_verify_ssl or os.getenv("CONFLUENCE_VERIFY_SSL")
    verify_ssl = _parse_bool_value(verify_ssl_raw, default=True)
    dry_run_env = _parse_bool_value(os.getenv("CONFLUENCE_DRY_RUN"), default=False)
    dry_run = bool(args.confluence_dry_run) or dry_run_env
    update_existing_env = _parse_bool_value(os.getenv("CONFLUENCE_UPDATE_EXISTING"), default=False)
    update_existing = bool(args.confluence_update_existing) or update_existing_env
    if not base_url:
        raise ValueError("Missing Confluence base URL. Set --confluence-base-url or CONFLUENCE_BASE_URL.")
    if not space_key:
        raise ValueError("Missing Confluence space key. Set --confluence-space-key or CONFLUENCE_SPACE_KEY.")
    if not parent_page_id:
        raise ValueError(
            "Missing Confluence parent page id. Set --confluence-parent-page-id or CONFLUENCE_PARENT_PAGE_ID."
        )
    if not api_token:
        raise ValueError("Missing Confluence API token. Set --confluence-api-token or CONFLUENCE_API_TOKEN.")
    if auth_mode not in {"auto", "basic", "bearer"}:
        raise ValueError("Invalid Confluence auth mode. Use auto, basic, or bearer.")
    if auth_mode == "basic" and not username:
        raise ValueError(
            "Missing Confluence username for basic auth. Set --confluence-username or CONFLUENCE_USERNAME."
        )
    return ConfluenceConfig(
        base_url=_normalize_confluence_base_url(base_url),
        space_key=space_key.strip(),
        parent_page_id=parent_page_id.strip(),
        username=username.strip(),
        api_token=api_token.strip(),
        auth_mode=auth_mode,
        verify_ssl=verify_ssl,
        dry_run=dry_run,
        update_existing=update_existing,
    )


def publish_reports_to_confluence(paths: list[str], cfg: ConfluenceConfig) -> list[str]:
    outcomes: list[str] = []
    for path in sorted(paths):
        outcomes.append(publish_html_report_to_confluence(path, cfg))
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
        for key in ("values", "results", "items", "content", "folders", "children"):
            value = payload.get(key)
            if isinstance(value, list):
                for entry in value:
                    collected.extend(_collect_folder_nodes(entry))
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
) -> tuple[list[tuple[FolderNode, dict[date, WeeklyStat]]], Counter]:
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

    rows: list[tuple[FolderNode, dict[date, WeeklyStat]]] = []
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


def _build_daily_report_title(folder_name: str, cycles: dict[str, Any]) -> str:
    report_date = _resolve_daily_title_date(cycles)
    if report_date:
        return f"nightly-dev-{folder_name} ({report_date})"
    return f"nightly-dev-{folder_name} (unknown-date)"


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
                    case_iteration_key = str(
                        nested_case.get("iterationKey")
                        or nested_case.get("iterationId")
                        or get_by_path(nested_case, "iteration.key")
                        or get_by_path(nested_case, "iteration.id")
                        or ""
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
    return rows


def week_start(d: date) -> date:
    return d.fromordinal(d.toordinal() - d.weekday())


def aggregate_weekly(
    items: list[dict[str, Any]],
    date_fields: list[str],
    status_fields: list[str],
    from_date: date | None,
    to_date: date | None,
) -> tuple[dict[date, WeeklyStat], Counter]:
    per_week: dict[date, WeeklyStat] = defaultdict(WeeklyStat)
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
        status = normalize_status(raw_status)

        bucket = per_week[week_start(execution_day)]
        bucket.total += 1
        if status == "passed":
            bucket.passed += 1
        elif status == "failed":
            bucket.failed += 1
        elif status == "blocked":
            bucket.blocked += 1
        elif status == "not_executed":
            bucket.not_executed += 1
        else:
            bucket.other += 1

    return per_week, skipped


def write_csv(path: str, weekly: dict[date, WeeklyStat]) -> None:
    header = [
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
        for week in sorted(weekly.keys()):
            writer.writerow(weekly[week].to_row(week))


def write_folder_summary_csv(
    path: str, folder_rows: list[tuple[FolderNode, dict[date, WeeklyStat]]]
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
                stat = weekly[week]
                row = stat.to_row(week)
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
            case_merge[mkey] = {
                "test_case_name": test_case_name or "",
                "cycle_key": cycle_key_disp or "",
                "cycle_name": cycle_name_disp or "",
                "cycle_objective": cycle_objective or "",
                "step_status_name": step_status_name or "",
                "test_result_status_name": test_result_status_name or "",
                "step_comment": step_comment.strip() if step_comment else "",
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
            if step_comment.strip() and not m["step_comment"]:
                m["step_comment"] = step_comment.strip()
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

        step_status = str(m["step_status_name"] or "").strip()
        test_result_status = str(m["test_result_status_name"] or "").strip()
        fallback = str(fallback_status.get((folder_id, test_run_id), "") or "").strip()

        # Prefer the final test result status over step status. This avoids
        # false "Not Executed" when steps are stale but case result is final.
        if test_result_status:
            result = test_result_status
        elif step_status:
            result = step_status
        else:
            result = fallback
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


def _status_badge_html(status: str) -> str:
    normalized = (status or "").strip().lower()
    status_to_class = {
        "not executed": "st-not-executed",
        "not_executed": "st-not-executed",
        "untested": "st-not-executed",
        "in progress": "st-in-progress",
        "in_progress": "st-in-progress",
        "pass": "st-pass",
        "passed": "st-pass",
        "done": "st-pass",
        "fail": "st-fail",
        "failed": "st-fail",
        "blocked": "st-blocked",
        "can't test": "st-cant-test",
        "cant test": "st-cant-test",
        "not tested in this pi": "st-not-tested-pi",
        "danger": "st-danger",
        "can't reproduce": "st-cant-reproduce",
        "cant reproduce": "st-cant-reproduce",
        "false positive": "st-false-positive",
    }
    cls = status_to_class.get(normalized, "st-unknown")
    class_to_inline_style = {
        "st-pass": "background:#33c24d;color:#ffffff;",
        "st-fail": "background:#e53935;color:#ffffff;",
        "st-not-executed": "background:#c9c9c2;color:#2f2f2f;",
        "st-in-progress": "background:#f0ad4e;color:#2f2f2f;",
        "st-blocked": "background:#4a90e2;color:#ffffff;",
        "st-cant-test": "background:#9c27ff;color:#ffffff;",
        "st-not-tested-pi": "background:#8d7cc3;color:#ffffff;",
        "st-danger": "background:#4f6078;color:#ffffff;",
        "st-cant-reproduce": "background:#f08f78;color:#2f2f2f;",
        "st-false-positive": "background:#ecd96b;color:#2f2f2f;",
        "st-unknown": "background:#f4f5f7;color:#172b4d;",
    }
    inline_style = class_to_inline_style.get(cls, class_to_inline_style["st-unknown"])
    return (
        "<span "
        f"class='status-badge {cls}' "
        "style='display:inline-block;padding:2px 8px;border-radius:10px;"
        "font-size:12px;font-weight:700;line-height:1.4;"
        f"{inline_style}'>{html.escape(status or '')}</span>"
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
        for case in cycle.get("cases", {}).values():
            normalized = normalize_status(case.get("result", case.get("test_case_status", "")))
            if normalized == "passed":
                passed_cases += 1
        rows.append(
            {
                "cycle_title": str(cycle_title),
                "cycle_index": cycle_index,
                "cycle_key": cycle_key,
                "total_cases": total_cases,
                "passed_cases": passed_cases,
            }
        )
    return rows


def _passed_count_color(passed_cases: int) -> str:
    if passed_cases <= 0:
        return "#ff9074"
    if passed_cases == 1:
        return "#ffc402"
    if passed_cases == 2:
        return "#37b37e"
    return "#01875b"


def _passed_count_text_color(passed_cases: int) -> str:
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


def write_cycle_progress_csv(path: str, rows: list[list[str]]) -> None:
    header = [
        "folder_id",
        "folder_name",
        "cycle_key",
        "cycle_name",
        "total_cases",
        "passed_cases",
    ]
    output_dir = os.path.dirname(path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


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


def _parse_report_day_from_folder_name(folder_name: str) -> date | None:
    raw_name = str(folder_name or "").strip()
    if not raw_name:
        return None
    for pattern in ("%Y.%m.%d", "%Y-%m-%d", "%Y_%m_%d"):
        try:
            return datetime.strptime(raw_name, pattern).date()
        except ValueError:
            continue
    return None


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
    dominant_from_daily_data = _resolve_folder_dominant_actual_date(cycles)
    if dominant_from_daily_data is not None:
        return dominant_from_daily_data
    return _parse_report_day_from_folder_name(folder_name)


def _weekly_cycle_matrix_data(
    report_data: dict[tuple[str, str], dict[str, Any]]
) -> tuple[date | None, list[str], list[list[str]]]:
    weekly_groups: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for (folder_id, folder_name), payload in report_data.items():
        cycles = payload.get("cycles", {})
        if not isinstance(cycles, dict):
            continue
        report_day = _resolve_daily_title_day(cycles)
        if report_day is None or report_day.weekday() > 4:
            continue
        progress_rows = _build_cycle_progress_rows(cycles)
        if not progress_rows:
            continue
        week_start = report_day - timedelta(days=report_day.weekday())
        weekly_groups[week_start].append(
            {
                "folder_id": str(folder_id),
                "report_day": report_day,
                "progress_rows": progress_rows,
            }
        )

    if not weekly_groups:
        return None, [], []

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
            existing = aggregated.get(normalized_label)
            if existing is None:
                aggregated[normalized_label] = {
                    "sort_key": cycle_sort_key,
                    "total_cases": total_cases,
                    "passed_cases": passed_cases,
                    "is_cloned": is_cloned,
                }
                continue
            # Prefer non-cloned row; use cloned only as fallback when base is absent.
            if existing["is_cloned"] and not is_cloned:
                aggregated[normalized_label] = {
                    "sort_key": cycle_sort_key,
                    "total_cases": total_cases,
                    "passed_cases": passed_cases,
                    "is_cloned": False,
                }
                continue
            if (not existing["is_cloned"]) and is_cloned:
                continue
            if cycle_sort_key < existing["sort_key"]:
                existing["sort_key"] = cycle_sort_key
            existing["total_cases"] = max(existing["total_cases"], total_cases)
            existing["passed_cases"] = max(existing["passed_cases"], passed_cases)
        return aggregated

    target_week_start = max(
        weekly_groups.keys(),
        key=lambda week_start: (len(weekly_groups[week_start]), week_start),
    )
    week_daily_summaries = sorted(
        weekly_groups[target_week_start],
        key=lambda item: (item["report_day"], item["folder_id"]),
    )
    progress_rows_by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for summary in week_daily_summaries:
        progress_rows_by_day[summary["report_day"]].extend(summary["progress_rows"])
    ordered_days = sorted(progress_rows_by_day.keys())
    column_labels = [f"nightly-dev-{day.strftime('%Y.%m.%d')}" for day in ordered_days]

    day_maps: dict[date, dict[str, dict[str, Any]]] = {}
    joined_passed_by_label: dict[str, dict[str, int]] = {}
    joined_totals_by_label: dict[str, dict[str, int]] = {}
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

    all_cycle_labels: set[str] = set()
    for day_map in day_maps.values():
        all_cycle_labels.update(day_map.keys())
    if not all_cycle_labels:
        return target_week_start, column_labels, []

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
    for cycle_label in sorted(
        all_cycle_labels,
        key=lambda label: (sort_key_by_cycle.get(label, (9_999_999, label, "", "", "")), label.lower()),
    ):
        total_cases = 0
        for totals_map in joined_totals_by_label.values():
            total_cases = max(total_cases, int(totals_map.get(cycle_label, 0)))
        row = [cycle_label, str(total_cases)]
        for day_label in column_labels:
            row.append(str(joined_passed_by_label.get(day_label, {}).get(cycle_label, 0)))
        rows.append(row)
    return target_week_start, column_labels, rows


def _weekly_cycle_matrix_rows(report_data: dict[tuple[str, str], dict[str, Any]]) -> list[list[str]]:
    _, _, rows = _weekly_cycle_matrix_data(report_data)
    return rows


def write_weekly_cycle_matrix_csv(path: str, weekday_labels: list[str], rows: list[list[str]]) -> None:
    header = [
        "Тестовый цикл",
        "Всего кейсов",
    ]
    header.extend(label for label in weekday_labels)
    output_dir = os.path.dirname(path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def render_weekly_html_report(week_start: date | None, weekday_labels: list[str], rows: list[list[str]]) -> str:
    week_label = week_start.isoformat() if week_start else "N/A"
    labels = list(weekday_labels)
    col_count = 2 + len(labels)
    header_cells = ["<th>Тестовый цикл</th>", "<th>Всего кейсов</th>"]
    header_cells.extend(f"<th>{html.escape(label)}</th>" for label in labels)
    sections = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        f"<title>Weekly cycle matrix: {html.escape(week_label)}</title>",
        (
            "<style>"
            "body{font-family:Arial,sans-serif;margin:24px;}"
            "h1{margin-bottom:8px;}table{border-collapse:collapse;width:100%;margin-bottom:16px;table-layout:fixed;}"
            "th,td{border:1px solid #d6d6d6;padding:6px 8px;text-align:left;vertical-align:top;overflow-wrap:anywhere;word-wrap:break-word;}"
            "th{background:#f0f2f5;font-weight:600;}.passed-count-cell{font-weight:700;text-align:center;}"
            ".scenario-sep td{border:none;height:8px;padding:0;background:transparent;}"
            "</style>"
        ),
        "</head><body>",
        f"<h1>Weekly cycle matrix: {html.escape(week_label)}</h1>",
        "<table>",
        "<thead><tr>" + "".join(header_cells) + "</tr></thead><tbody>",
    ]
    previous_group = ""
    for row in rows:
        summary_row = {
            "cycle_index": _extract_cycle_index({"cycle_name": row[0]}),
            "cycle_title": row[0],
            "cycle_key": "",
        }
        current_group = _summary_scenario_group(summary_row)
        if previous_group and current_group and current_group != previous_group:
            sections.append(f"<tr class='scenario-sep'><td colspan='{col_count}'></td></tr>")
        passed_cells: list[str] = []
        for idx in range(len(labels)):
            passed_value = int(row[2 + idx]) if 2 + idx < len(row) else 0
            passed_cells.append(
                "<td class='passed-count-cell' "
                f"style='background:{_passed_count_color(passed_value)};color:{_passed_count_text_color(passed_value)};'>"
                f"{passed_value}</td>"
            )
        sections.append(
            (
                "<tr>"
                f"<td>{html.escape(row[0])}</td>"
                f"<td>{html.escape(row[1])}</td>"
                + "".join(passed_cells)
                + "</tr>"
            )
        )
        previous_group = current_group or previous_group
    sections.extend(["</tbody></table>", "</body></html>"])
    return "\n".join(sections)


def render_weekly_wiki_report(
    week_start: date | None,
    weekday_labels: list[str],
    rows: list[list[str]],
) -> str:
    week_label = week_start.isoformat() if week_start else "N/A"
    labels = list(weekday_labels)
    col_count = 2 + len(labels)
    lines = [f"h1. Weekly cycle matrix: {_wiki_escape(week_label)}", ""]
    header_cells = ["Тестовый цикл", "Всего кейсов"]
    header_cells.extend(f"{_wiki_escape(label)}" for label in labels)
    lines.append("|| " + " || ".join(header_cells) + " ||")
    previous_group = ""
    for row in rows:
        summary_row = {
            "cycle_index": _extract_cycle_index({"cycle_name": row[0]}),
            "cycle_title": row[0],
            "cycle_key": "",
        }
        current_group = _summary_scenario_group(summary_row)
        if previous_group and current_group and current_group != previous_group:
            lines.append("| " + " | ".join("" for _ in range(col_count)) + " |")
        row_values = [row[0], row[1]]
        for idx in range(len(labels)):
            row_values.append(row[2 + idx] if 2 + idx < len(row) else "0")
        lines.append(
            "| "
            + " | ".join(_wiki_escape(str(value)) for value in row_values)
            + " |"
        )
        previous_group = current_group or previous_group
    lines.append("")
    return "\n".join(lines)


def write_weekly_readable_reports(
    output_dir: str,
    week_start: date | None,
    weekday_labels: list[str],
    rows: list[list[str]],
    formats: set[str],
) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    week_label = week_start.isoformat() if week_start else "unknown_week"
    base_name = f"weekly_cycle_matrix_{week_label}"
    written_paths: list[str] = []
    if "html" in formats:
        html_path = os.path.join(output_dir, f"{base_name}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(render_weekly_html_report(week_start, weekday_labels, rows))
        written_paths.append(html_path)
    if "wiki" in formats:
        wiki_path = os.path.join(output_dir, f"{base_name}.confluence.txt")
        with open(wiki_path, "w", encoding="utf-8") as f:
            f.write(render_weekly_wiki_report(week_start, weekday_labels, rows))
        written_paths.append(wiki_path)
    return written_paths


def render_daily_html_report(folder_name: str, cycles: dict[str, Any]) -> str:
    report_title = _build_daily_report_title(folder_name, cycles)
    sections = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        f"<title>Daily report: {html.escape(report_title)}</title>",
        (
            "<style>"
            "body{font-family:Arial,sans-serif;margin:24px;}"
            "h1{margin-bottom:8px;}h2{margin-top:24px;margin-bottom:8px;}"
            "table{border-collapse:collapse;width:100%;margin-bottom:16px;table-layout:fixed;}"
            "th,td{border:1px solid #d6d6d6;padding:6px 8px;text-align:left;vertical-align:top;overflow-wrap:anywhere;word-wrap:break-word;}"
            "th{background:#f0f2f5;font-weight:600;}"
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
            ".passed-count-cell{font-weight:700;text-align:center;}"
            ".scenario-sep td{border:none;height:8px;padding:0;background:transparent;}"
            "</style>"
        ),
        "</head><body>",
        f"<h1>Daily report: {html.escape(report_title)}</h1>",
    ]
    cycle_groups = _group_cycles_by_prefix(cycles)
    for group in cycle_groups:
        group_title = str(group.get("group_title") or "Тестовые циклы")
        sections.append(f"<h2>{html.escape(group_title)}</h2>")
        sections.append(
            "<table>"
            "<colgroup>"
            "<col style='width:20%' />"
            "<col style='width:30%' />"
            "<col style='width:10%' />"
            "<col style='width:10%' />"
            "<col style='width:10%' />"
            "<col style='width:15%' />"
            "<col style='width:5%' />"
            "</colgroup>"
            "<thead><tr>"
            "<th>Название</th><th>Критерий валидации</th><th>Тестовый прогон</th>"
            "<th>Статус</th><th>Дата</th><th>Комментарий</th><th>Задачи</th>"
            "</tr></thead><tbody>"
        )
        for cycle in group["cycles"]:
            sorted_cases, criterion_spans = _prepare_cycle_cases_with_groups(cycle)
            cycle_key_value = str(cycle.get("cycle_key") or "")
            cycle_cell_html = html.escape(cycle_key_value)
            if cycle_key_value:
                cycle_url = _jira_cycle_url(cycle_key_value)
                cycle_cell_html = (
                    f"<a href='{html.escape(cycle_url, quote=True)}' target='_blank' rel='noopener'>"
                    f"{html.escape(cycle_key_value)}</a>"
                )
            for idx, case in enumerate(sorted_cases):
                result_value = case.get("result", case.get("test_case_status", ""))
                display_date = _resolve_case_display_date(case)
                criterion_cell = ""
                if criterion_spans[idx] > 0:
                    criterion_text = case.get("_criterion_display", "")
                    criterion_cell = (
                        f"<td rowspan='{criterion_spans[idx]}'>{_html_comment_cell(criterion_text)}</td>"
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
                    "</tr>"
                )
        sections.append("</tbody></table>")

    progress_rows = sorted(_build_cycle_progress_rows(cycles), key=_summary_sort_key)
    sections.append("<h2>Сводка по тестовым циклам</h2>")
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
        bg_color = _passed_count_color(passed_cases)
        text_color = _passed_count_text_color(passed_cases)
        sections.append(
            "<tr>"
            f"<td>{html.escape(cycle_label)}</td>"
            f"<td>{row['total_cases']}</td>"
            f"<td class='passed-count-cell' style='background:{bg_color};color:{text_color};'>{passed_cases}</td>"
            "</tr>"
        )
        previous_group = current_group or previous_group
    sections.append("</tbody></table>")
    sections.append("</body></html>")
    return "\n".join(sections)


def render_daily_wiki_report(folder_name: str, cycles: dict[str, Any]) -> str:
    report_title = _build_daily_report_title(folder_name, cycles)
    lines = [f"h1. Daily report: {_wiki_escape(report_title)}", ""]
    cycle_groups = _group_cycles_by_prefix(cycles)
    for group in cycle_groups:
        group_title = str(group.get("group_title") or "Тестовые циклы")
        lines.append(f"h2. {_wiki_escape(group_title)}")
        lines.append(
            "|| Название || Критерий валидации || Тестовый прогон || Статус || Дата || Комментарий || Задачи ||"
        )
        for cycle in group["cycles"]:
            cycle_key_value = str(cycle.get("cycle_key") or "")
            cycle_cell_wiki = _wiki_escape(cycle_key_value)
            if cycle_key_value:
                cycle_url = _jira_cycle_url(cycle_key_value)
                cycle_cell_wiki = f"[{cycle_key_value}|{cycle_url}]"
            sorted_cases, criterion_spans = _prepare_cycle_cases_with_groups(cycle)
            for idx, case in enumerate(sorted_cases):
                res = case.get("result", case.get("test_case_status", ""))
                exd = _resolve_case_display_date(case)
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
                            _wiki_escape(res),
                            _wiki_escape(exd),
                            cmt,
                            tasks,
                        ]
                    )
                    + " |"
                )
        lines.append("")

    progress_rows = sorted(_build_cycle_progress_rows(cycles), key=_summary_sort_key)
    lines.append("h2. Сводка по тестовым циклам")
    lines.append("|| Тестовый цикл || Всего кейсов || Пройдено кейсов ||")
    previous_group = ""
    for row in progress_rows:
        current_group = _summary_scenario_group(row)
        if previous_group and current_group and current_group != previous_group:
            lines.append("|  |  |  |")
        cycle_label = _build_summary_cycle_label(row)
        lines.append(
            "| "
            + " | ".join(
                [
                    _wiki_escape(cycle_label),
                    str(row["total_cases"]),
                    str(row["passed_cases"]),
                ]
            )
            + " |"
        )
        previous_group = current_group or previous_group
    lines.append("")
    return "\n".join(lines)


def write_daily_readable_reports(
    output_dir: str,
    report_data: dict[tuple[str, str], dict[str, Any]],
    formats: set[str],
) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    written_paths: list[str] = []
    for (folder_id, folder_name), payload in sorted(report_data.items(), key=lambda item: item[0][1]):
        cycles = payload["cycles"]
        base_name = _build_daily_report_base_name(str(folder_id), str(folder_name), cycles)
        if "html" in formats:
            html_path = os.path.join(output_dir, f"{base_name}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(render_daily_html_report(folder_name, cycles))
            written_paths.append(html_path)
        if "wiki" in formats:
            wiki_path = os.path.join(output_dir, f"{base_name}.confluence.txt")
            with open(wiki_path, "w", encoding="utf-8") as f:
                f.write(render_daily_wiki_report(folder_name, cycles))
            written_paths.append(wiki_path)
    return written_paths


def print_table(weekly: dict[date, WeeklyStat]) -> None:
    if not weekly:
        print("No executions found for selected filters.")
        return

    columns = [
        "week_start",
        "total",
        "passed",
        "failed",
        "blocked",
        "not_exec",
        "other",
        "pass_rate%",
    ]
    rows = []
    for week in sorted(weekly.keys()):
        stat = weekly[week]
        pass_rate = (stat.passed / stat.total * 100.0) if stat.total else 0.0
        rows.append(
            [
                week.isoformat(),
                str(stat.total),
                str(stat.passed),
                str(stat.failed),
                str(stat.blocked),
                str(stat.not_executed),
                str(stat.other),
                f"{pass_rate:.2f}",
            ]
        )

    widths = [len(c) for c in columns]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))

    fmt = " | ".join("{:<" + str(w) + "}" for w in widths)
    separator = "-+-".join("-" * w for w in widths)
    print(fmt.format(*columns))
    print(separator)
    for row in rows:
        print(fmt.format(*row))


def main() -> int:
    args = parse_args()
    try:
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
        publish_confluence_daily = args.publish_confluence_daily or _parse_bool_value(
            os.getenv("CONFLUENCE_PUBLISH_DAILY"), default=False
        )
        publish_confluence_weekly = args.publish_confluence_weekly or _parse_bool_value(
            os.getenv("CONFLUENCE_PUBLISH_WEEKLY"), default=False
        )
        confluence_cfg: ConfluenceConfig | None = None
        if publish_confluence_daily or publish_confluence_weekly:
            confluence_cfg = load_confluence_config(args)
            print(
                "Confluence mode: "
                f"daily={publish_confluence_daily} "
                f"weekly={publish_confluence_weekly} "
                f"dry_run={confluence_cfg.dry_run} "
                f"auth_mode={confluence_cfg.auth_mode} "
                f"update_existing={confluence_cfg.update_existing}"
            )

        if args.discover_folders:
            folder_rows: list[tuple[FolderNode, dict[date, WeeklyStat]]] = []
            cycles_cases_rows: list[list[str]] = []
            case_steps_rows: list[list[str]] = []
            need_cycles_cases_data = bool(
                args.export_cycles_cases
                or args.export_daily_readable
                or args.cycle_progress_output
                or args.weekly_cycle_matrix_output
                or args.export_weekly_readable
            )
            collect_case_steps = bool(
                args.export_case_steps
                or args.export_daily_readable
                or args.weekly_cycle_matrix_output
                or args.export_weekly_readable
            )
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
                print(
                    f"Tree folders discovered: {len(folders)}; selected after filters: {len(selected_folders)}"
                )

                for folder in selected_folders:
                    per_folder_params = dict(extra_params)
                    per_folder_params["query"] = sanitize_tql_query(
                        fill_template(
                            args.query_template,
                            "folder_id",
                            folder.folder_id,
                            "--query-template",
                        )
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
                    except Exception as exc:  # pylint: disable=broad-except
                        message = f"Folder {folder.folder_id} ({folder.folder_name}) failed: {exc}"
                        if args.continue_on_folder_error:
                            errors.append(message)
                            continue
                        raise RuntimeError(message) from exc

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
            report_data: dict[tuple[str, str], dict[str, Any]] | None = None
            daily_readable_paths: list[str] = []
            if args.export_daily_readable:
                selected_formats = set(args.daily_readable_format or ["html", "wiki"])
                if case_steps_rows:
                    report_data = aggregate_readable_daily_reports_from_steps(
                        case_steps_rows, cycles_cases_rows
                    )
                else:
                    report_data = aggregate_readable_daily_reports_legacy(cycles_cases_rows)
                daily_readable_paths = write_daily_readable_reports(
                    output_dir=args.daily_readable_dir,
                    report_data=report_data,
                    formats=selected_formats,
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
            if args.cycle_progress_output:
                cycle_progress_rows = _cycle_progress_csv_rows(report_data)
                write_cycle_progress_csv(args.cycle_progress_output, cycle_progress_rows)
            weekly_cycle_week_start: date | None = None
            weekly_cycle_weekday_labels = _default_weekday_labels()
            weekly_cycle_rows: list[list[str]] = []
            if args.weekly_cycle_matrix_output:
                (
                    weekly_cycle_week_start,
                    weekly_cycle_weekday_labels,
                    weekly_cycle_rows,
                ) = _weekly_cycle_matrix_data(report_data)
                write_weekly_cycle_matrix_csv(
                    args.weekly_cycle_matrix_output,
                    weekly_cycle_weekday_labels,
                    weekly_cycle_rows,
                )
            weekly_readable_paths: list[str] = []
            if args.export_weekly_readable:
                if not weekly_cycle_rows:
                    (
                        weekly_cycle_week_start,
                        weekly_cycle_weekday_labels,
                        weekly_cycle_rows,
                    ) = _weekly_cycle_matrix_data(report_data)
                selected_weekly_formats = set(args.weekly_readable_format or ["html", "wiki"])
                weekly_readable_paths = write_weekly_readable_reports(
                    output_dir=args.weekly_readable_dir,
                    week_start=weekly_cycle_week_start,
                    weekday_labels=weekly_cycle_weekday_labels,
                    rows=weekly_cycle_rows,
                    formats=selected_weekly_formats,
                )
            if confluence_cfg and publish_confluence_daily:
                daily_html_paths = [p for p in daily_readable_paths if p.endswith(".html")]
                if daily_html_paths:
                    outcomes = publish_reports_to_confluence(daily_html_paths, confluence_cfg)
                    print("Confluence daily publish:")
                    for line in outcomes:
                        print(f"- {line}")
                else:
                    print("Confluence daily publish skipped: no daily HTML files were generated.")
            if confluence_cfg and publish_confluence_weekly:
                weekly_html_paths = [p for p in weekly_readable_paths if p.endswith(".html")]
                if weekly_html_paths:
                    outcomes = publish_reports_to_confluence(weekly_html_paths, confluence_cfg)
                    print("Confluence weekly publish:")
                    for line in outcomes:
                        print(f"- {line}")
                else:
                    print("Confluence weekly publish skipped: no weekly HTML files were generated.")
            print(f"Saved summary CSV: {args.output}")
            print(f"Saved per-folder CSV directory: {args.per_folder_dir}")
            if args.export_cycles_cases:
                print(f"Saved cycles/cases CSV: {args.cycles_cases_output}")
            if args.export_case_steps:
                print(f"Saved case steps CSV: {args.case_steps_output}")
            if args.export_daily_readable:
                print(f"Saved daily readable reports: {args.daily_readable_dir}")
                print(f"Readable files written: {len(daily_readable_paths)}")
            if args.cycle_progress_output:
                print(f"Saved cycle progress CSV: {args.cycle_progress_output}")
            if args.weekly_cycle_matrix_output:
                print(f"Saved weekly cycle matrix CSV: {args.weekly_cycle_matrix_output}")
            if args.export_weekly_readable:
                print(f"Saved weekly readable reports: {args.weekly_readable_dir}")
                print(f"Weekly readable files written: {len(weekly_readable_paths)}")
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
        print_table(weekly)
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

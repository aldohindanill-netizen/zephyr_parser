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
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
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

        result = (
            m["step_status_name"]
            or m["test_result_status_name"]
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
    return f"<span class='status-badge {cls}'>{html.escape(status or '')}</span>"


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


def render_daily_html_report(folder_name: str, cycles: dict[str, Any]) -> str:
    sections = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        f"<title>Daily report: {html.escape(folder_name)}</title>",
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
            "</style>"
        ),
        "</head><body>",
        f"<h1>Daily report: {html.escape(folder_name)}</h1>",
    ]
    for cycle in sorted(cycles.values(), key=lambda item: (item["cycle_key"], item["cycle_name"])):
        cycle_title = cycle["cycle_name"] or cycle["cycle_key"] or cycle["cycle_id"] or "Unnamed cycle"
        cycle_cell_html = _render_cycle_info_html(cycle)
        sections.append(f"<h2>Test cycle: {html.escape(cycle_title)}</h2>")
        sections.append(
            "<table>"
            "<colgroup>"
            "<col style='width:20%'>"
            "<col style='width:30%'>"
            "<col style='width:10%'>"
            "<col style='width:10%'>"
            "<col style='width:10%'>"
            "<col style='width:15%'>"
            "<col style='width:5%'>"
            "</colgroup>"
            "<thead><tr>"
            "<th>Название</th><th>Критерий валидации</th><th>Тестовый прогон</th>"
            "<th>Статус</th><th>Дата</th><th>Комментарий</th><th>Задачи</th>"
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
        for idx, case in enumerate(sorted_cases):
            result_value = case.get("result", case.get("test_case_status", ""))
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
                f"<td>{html.escape(case.get('execution_date', case.get('cycle_updated_on', '')))}</td>"
                f"<td>{_html_comment_cell(case.get('comment', ''))}</td>"
                f"<td>{_html_comment_cell(case.get('tasks', ''))}</td>"
                "</tr>"
            )
        sections.append("</tbody></table>")
    sections.append("</body></html>")
    return "\n".join(sections)


def render_daily_wiki_report(folder_name: str, cycles: dict[str, Any]) -> str:
    lines = [f"h1. Daily report: {_wiki_escape(folder_name)}", ""]
    for cycle in sorted(cycles.values(), key=lambda item: (item["cycle_key"], item["cycle_name"])):
        cycle_title = cycle["cycle_name"] or cycle["cycle_key"] or cycle["cycle_id"] or "Unnamed cycle"
        cycle_key_value = str(cycle.get("cycle_key") or "")
        cycle_cell_wiki = _wiki_escape(cycle_key_value)
        if cycle_key_value:
            cycle_url = _jira_cycle_url(cycle_key_value)
            cycle_cell_wiki = f"[{cycle_key_value}|{cycle_url}]"
        lines.append(f"h2. Test cycle: {_wiki_escape(cycle_title)}")
        lines.append(
            "|| Название || Критерий валидации || Тестовый прогон || Статус || Дата || Комментарий || Задачи ||"
        )
        sorted_cases, criterion_spans = _prepare_cycle_cases_with_groups(cycle)
        for idx, case in enumerate(sorted_cases):
            res = case.get("result", case.get("test_case_status", ""))
            exd = case.get("execution_date", case.get("cycle_updated_on", ""))
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
    return "\n".join(lines)


def write_daily_readable_reports(
    output_dir: str,
    report_data: dict[tuple[str, str], dict[str, Any]],
    formats: set[str],
) -> int:
    os.makedirs(output_dir, exist_ok=True)
    written = 0
    for (folder_id, folder_name), payload in sorted(report_data.items(), key=lambda item: item[0][1]):
        base_name = f"{slugify(folder_name)}_{folder_id}"
        cycles = payload["cycles"]
        if "html" in formats:
            html_path = os.path.join(output_dir, f"{base_name}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(render_daily_html_report(folder_name, cycles))
            written += 1
        if "wiki" in formats:
            wiki_path = os.path.join(output_dir, f"{base_name}.confluence.txt")
            with open(wiki_path, "w", encoding="utf-8") as f:
                f.write(render_daily_wiki_report(folder_name, cycles))
            written += 1
    return written


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

        if args.discover_folders:
            folder_rows: list[tuple[FolderNode, dict[date, WeeklyStat]]] = []
            cycles_cases_rows: list[list[str]] = []
            case_steps_rows: list[list[str]] = []
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
            if args.export_daily_readable:
                selected_formats = set(args.daily_readable_format or ["html", "wiki"])
                if case_steps_rows:
                    report_data = aggregate_readable_daily_reports_from_steps(
                        case_steps_rows, cycles_cases_rows
                    )
                else:
                    report_data = aggregate_readable_daily_reports_legacy(cycles_cases_rows)
                written_reports = write_daily_readable_reports(
                    output_dir=args.daily_readable_dir,
                    report_data=report_data,
                    formats=selected_formats,
                )
            print(f"Saved summary CSV: {args.output}")
            print(f"Saved per-folder CSV directory: {args.per_folder_dir}")
            if args.export_cycles_cases:
                print(f"Saved cycles/cases CSV: {args.cycles_cases_output}")
            if args.export_case_steps:
                print(f"Saved case steps CSV: {args.case_steps_output}")
            if args.export_daily_readable:
                print(f"Saved daily readable reports: {args.daily_readable_dir}")
                print(f"Readable files written: {written_reports}")
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

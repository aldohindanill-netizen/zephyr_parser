#!/usr/bin/env python3
"""Generate a weekly Zephyr test execution summary.

The script fetches paginated execution data from a Zephyr API endpoint,
normalizes statuses, aggregates by ISO week (Monday start), and exports CSV.
"""

from __future__ import annotations

import argparse
import csv
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
            total_executions = 0
            total_skipped = Counter()
            errors: list[str] = []

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
            print(f"Saved summary CSV: {args.output}")
            print(f"Saved per-folder CSV directory: {args.per_folder_dir}")
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

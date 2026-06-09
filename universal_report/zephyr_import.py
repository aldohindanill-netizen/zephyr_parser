"""Fetch Zephyr folders and cycle data for universal report section 3."""

from __future__ import annotations

import argparse
import os
import re
from datetime import date
from typing import Any

from zephyr_weekly_report import (
    FolderNode,
    _collect_folder_nodes,
    _env_int,
    _extract_folder_info,
    _parse_json_object_arg,
    _read_cycle_field,
    aggregate_readable_daily_reports_from_steps,
    build_case_step_rows,
    build_cycle_case_rows,
    build_headers,
    discover_folders_custom_tree_source,
    discover_folders_tree_fallback,
    fetch_executions,
    fetch_test_result_status_names,
    fill_template,
    parse_date,
    parse_extra_params,
    parse_root_folder_ids,
    probe_tree_endpoints,
    request_json,
    sanitize_tql_query,
    select_tree_target_folders,
)


def _extra_params_from_env() -> list[str]:
    items: list[str] = []
    mapping = {
        "fields": "ZEPHYR_FIELDS",
        "maxResults": "ZEPHYR_MAX_RESULTS",
        "startAt": "ZEPHYR_START_AT",
        "archived": "ZEPHYR_ARCHIVED",
    }
    for key, env_name in mapping.items():
        value = (os.getenv(env_name) or "").strip()
        if value:
            items.append(f"{key}={value}")
    return items


def build_pipeline_args() -> argparse.Namespace:
    return argparse.Namespace(
        base_url=(os.getenv("ZEPHYR_BASE_URL") or "").strip(),
        endpoint=(os.getenv("ZEPHYR_ENDPOINT") or "rest/tests/1.0/testrun/search").strip(),
        token=None,
        token_header="Authorization",
        token_prefix="Bearer",
        extra_param=_extra_params_from_env(),
        page_size=100,
        project_id=(os.getenv("ZEPHYR_PROJECT_ID") or "").strip() or None,
        query_template=(
            os.getenv("ZEPHYR_QUERY_TEMPLATE")
            or "testRun.folderTreeId IN ({folder_id})"
        ).strip(),
        root_folder_id=parse_root_folder_ids(
            [(os.getenv("ZEPHYR_ROOT_FOLDER_IDS") or "").strip()]
            if (os.getenv("ZEPHYR_ROOT_FOLDER_IDS") or "").strip()
            else []
        ),
        folder_search_endpoint=(
            os.getenv("ZEPHYR_FOLDER_SEARCH_ENDPOINT") or "rest/tests/1.0/folder/search"
        ).strip(),
        foldertree_endpoint=(
            os.getenv("ZEPHYR_FOLDERTREE_ENDPOINT") or ""
        ).strip(),
        tree_leaf_only=(os.getenv("ZEPHYR_TREE_LEAF_ONLY") or "true").strip().lower()
        in {"1", "true", "yes", "on"},
        tree_name_regex=(os.getenv("ZEPHYR_TREE_NAME_REGEX") or "").strip() or None,
        tree_root_path_regex=(os.getenv("ZEPHYR_TREE_ROOT_PATH_REGEX") or "").strip() or None,
        tree_autoprobe=(os.getenv("ZEPHYR_TREE_AUTOPROBE") or "").strip().lower()
        in {"1", "true", "yes", "on"},
        tree_source_endpoint=(os.getenv("ZEPHYR_TREE_SOURCE_ENDPOINT") or "").strip() or None,
        tree_source_method=(os.getenv("ZEPHYR_TREE_SOURCE_METHOD") or "GET").strip(),
        tree_source_query_json=(os.getenv("ZEPHYR_TREE_SOURCE_QUERY_JSON") or "").strip() or None,
        tree_source_body_json=(os.getenv("ZEPHYR_TREE_SOURCE_BODY_JSON") or "").strip() or None,
        synthetic_cycle_ids=(os.getenv("ZEPHYR_SYNTHETIC_CYCLE_IDS") or "true").strip().lower()
        in {"1", "true", "yes", "on"},
        detail_workers=_env_int("ZEPHYR_DETAIL_WORKERS", 4),
        testcase_endpoint_template=[
            t.strip()
            for t in (os.getenv("ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE") or "").split(",")
            if t.strip()
        ]
        or ["rest/tests/1.0/testrun/{cycle_id}/testcase/search"],
    )


def _pipeline_headers(args: argparse.Namespace) -> dict[str, str]:
    token = args.token or (os.getenv("ZEPHYR_API_TOKEN") or "").strip()
    if not args.base_url:
        raise ValueError("ZEPHYR_BASE_URL is not configured")
    if not token:
        raise ValueError("ZEPHYR_API_TOKEN is not configured")
    return build_headers(args.token_header, args.token_prefix, token)


def _foldertree_endpoint_candidates(args: argparse.Namespace) -> list[str]:
    seen: set[str] = set()
    candidates: list[str] = []

    def add(endpoint: str) -> None:
        cleaned = (endpoint or "").strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            candidates.append(cleaned)

    add(args.foldertree_endpoint)
    project_id = (args.project_id or "").strip()
    if project_id:
        add(f"rest/tests/1.0/project/{project_id}/foldertree/testrun")
        add(f"rest/tests/1.0/project/{project_id}/foldertree")
    add((os.getenv("ZEPHYR_FOLDER_ENDPOINT") or "").strip())
    add("rest/tests/1.0/foldertree")
    return candidates


def _discover_foldertree_nodes(
    args: argparse.Namespace, headers: dict[str, str]
) -> tuple[list[FolderNode], str]:
    best_nodes: list[FolderNode] = []
    best_source = ""
    params: dict[str, str] = {}
    if args.project_id:
        params["projectId"] = args.project_id

    for endpoint in _foldertree_endpoint_candidates(args):
        try:
            payload = request_json(
                base_url=args.base_url,
                endpoint=endpoint,
                headers=headers,
                params=params,
                method="GET",
            )
        except Exception:  # noqa: BLE001
            continue
        nodes = _collect_folder_nodes(payload)
        if len(nodes) > len(best_nodes):
            best_nodes = nodes
            best_source = f"GET {endpoint}"
    return best_nodes, best_source


def _discover_raw_folder_nodes(
    args: argparse.Namespace,
    headers: dict[str, str],
    *,
    full_tree: bool = False,
) -> list[FolderNode]:
    if full_tree:
        nodes, _ = _discover_foldertree_nodes(args, headers)
        if nodes:
            return nodes
        raise RuntimeError(
            "Не удалось загрузить полное дерево папок Zephyr. "
            "Проверьте ZEPHYR_FOLDERTREE_ENDPOINT, ZEPHYR_PROJECT_ID и ZEPHYR_API_TOKEN."
        )

    tree_source_query = _parse_json_object_arg(args.tree_source_query_json, "tree-source-query")
    tree_source_body = _parse_json_object_arg(args.tree_source_body_json, "tree-source-body")
    if args.tree_source_endpoint:
        nodes, _, _ = discover_folders_custom_tree_source(
            base_url=args.base_url,
            endpoint=args.tree_source_endpoint,
            headers=headers,
            method=args.tree_source_method,
            query_params=tree_source_query,
            body=tree_source_body,
        )
    elif args.tree_autoprobe:
        nodes, _, _ = probe_tree_endpoints(
            base_url=args.base_url,
            headers=headers,
            project_id=args.project_id,
        )
    else:
        nodes, _, _ = discover_folders_tree_fallback(
            base_url=args.base_url,
            folder_search_endpoint=args.folder_search_endpoint,
            foldertree_endpoint=args.foldertree_endpoint,
            headers=headers,
            project_id=args.project_id,
        )
    return nodes


def _discover_folder_nodes(
    args: argparse.Namespace,
    headers: dict[str, str],
    *,
    scope: str = "all",
) -> list[FolderNode]:
    nodes = _discover_raw_folder_nodes(args, headers, full_tree=(scope == "all"))
    if scope == "all":
        return select_tree_target_folders(
            nodes=nodes,
            root_folder_ids=[],
            leaf_only=False,
            name_pattern=None,
            root_path_pattern=None,
        )
    root_folder_ids = list(args.root_folder_id or [])
    tree_name_pattern = (
        re.compile(args.tree_name_regex) if args.tree_name_regex else None
    )
    tree_root_path_pattern = (
        re.compile(args.tree_root_path_regex) if args.tree_root_path_regex else None
    )
    return select_tree_target_folders(
        nodes=nodes,
        root_folder_ids=root_folder_ids,
        leaf_only=args.tree_leaf_only,
        name_pattern=tree_name_pattern,
        root_path_pattern=tree_root_path_pattern,
    )


def _folder_to_dict(folder: FolderNode) -> dict[str, Any]:
    return {
        "id": folder.folder_id,
        "name": folder.folder_name,
        "parent_id": folder.parent_id or "",
        "full_path": folder.full_path,
        "is_leaf": bool(folder.is_leaf),
    }


def _filter_cycles_by_date(
    cycles: dict[str, Any],
    parsed_from: date | None,
    parsed_to: date | None,
) -> dict[str, Any]:
    if parsed_from is None and parsed_to is None:
        return cycles
    filtered: dict[str, Any] = {}
    for cycle_id, cycle in cycles.items():
        cases: dict[str, Any] = {}
        for case_key, case in (cycle.get("cases") or {}).items():
            exec_date_raw = str(case.get("execution_date") or case.get("actual_start_date") or "")
            exec_date = parse_date(exec_date_raw[:10] if exec_date_raw else None)
            if parsed_from and exec_date and exec_date < parsed_from:
                continue
            if parsed_to and exec_date and exec_date > parsed_to:
                continue
            cases[case_key] = case
        if cases:
            cycle_copy = dict(cycle)
            cycle_copy["cases"] = cases
            filtered[cycle_id] = cycle_copy
    return filtered


def _parse_date_bounds(
    from_date: str | None,
    to_date: str | None,
) -> tuple[date | None, date | None]:
    return (
        parse_date(from_date) if from_date else None,
        parse_date(to_date) if to_date else None,
    )


def _fetch_executions_for_folder(
    args: argparse.Namespace,
    headers: dict[str, str],
    folder_id: str,
) -> list[dict[str, Any]]:
    extra_params = parse_extra_params(args.extra_param)
    per_folder_params = dict(extra_params)
    per_folder_params["query"] = sanitize_tql_query(
        fill_template(
            args.query_template,
            "folder_id",
            str(folder_id),
            "--query-template",
        )
    )
    return fetch_executions(
        base_url=args.base_url,
        endpoint=args.endpoint,
        headers=headers,
        extra_params=per_folder_params,
        page_size=args.page_size,
    )


def _aggregate_cycles(
    folder: FolderNode,
    executions: list[dict[str, Any]],
    args: argparse.Namespace,
    headers: dict[str, str],
    *,
    parsed_from: date | None = None,
    parsed_to: date | None = None,
) -> dict[str, Any]:
    if not executions:
        return {}
    status_names = fetch_test_result_status_names(
        args.base_url, headers, args.project_id
    )
    case_steps_rows = build_case_step_rows(
        folder=folder,
        cycles=executions,
        base_url=args.base_url,
        headers=headers,
        status_names=status_names,
        synthetic_cycle_ids=args.synthetic_cycle_ids,
        detail_workers=max(1, int(args.detail_workers)),
    )
    cycles_cases_rows = build_cycle_case_rows(
        folder=folder,
        cycles=executions,
        testcase_endpoint_templates=args.testcase_endpoint_template,
        base_url=args.base_url,
        headers=headers,
        synthetic_cycle_ids=args.synthetic_cycle_ids,
    )
    reports = aggregate_readable_daily_reports_from_steps(
        case_steps_rows, cycles_cases_rows
    )
    report_key = (folder.folder_id, folder.folder_name)
    cycles: dict[str, Any] = {}
    if report_key in reports:
        cycles = reports[report_key]["cycles"]
    else:
        for key, payload in reports.items():
            if key[0] == folder.folder_id:
                cycles = payload["cycles"]
                break
    return _filter_cycles_by_date(cycles, parsed_from, parsed_to)


def _cycle_execution_summary(cycle: dict[str, Any]) -> dict[str, str]:
    test_run_id = _read_cycle_field(cycle, ["id", "testRunId"], "")
    cycle_key = _read_cycle_field(cycle, ["iteration.key", "testRunKey", "key"], "")
    cycle_name = _read_cycle_field(cycle, ["iteration.name", "testRunName", "name"], "")
    status = _read_cycle_field(cycle, ["status.name", "status", "result"], "")
    return {
        "test_run_id": test_run_id,
        "cycle_key": cycle_key,
        "cycle_name": cycle_name,
        "status": status,
    }


def _build_cycle_key_query(project_id: str, cycle_key: str) -> str:
    key = cycle_key.strip()
    if not key:
        raise ValueError("cycle_key is required")
    if not project_id:
        raise ValueError("ZEPHYR_PROJECT_ID is not configured")
    if key.isdigit():
        return sanitize_tql_query(
            f"testRun.projectId IN ({project_id}) AND testRun.id = {key}"
        )
    escaped = key.replace("\\", "\\\\").replace('"', '\\"')
    return sanitize_tql_query(
        f'testRun.projectId IN ({project_id}) AND testRun.key = "{escaped}"'
    )


def list_zephyr_folders(*, scope: str = "all") -> list[dict[str, Any]]:
    return list_zephyr_folders_with_meta(scope=scope)["folders"]


def list_zephyr_folders_with_meta(*, scope: str = "all") -> dict[str, Any]:
    normalized_scope = (scope or "all").strip().lower()
    if normalized_scope not in {"all", "pipeline"}:
        normalized_scope = "all"
    args = build_pipeline_args()
    headers = _pipeline_headers(args)
    source = ""
    if normalized_scope == "all":
        folder_nodes, source = _discover_foldertree_nodes(args, headers)
        if not folder_nodes:
            raise RuntimeError(
                "Не удалось загрузить полное дерево папок Zephyr. "
                "Проверьте ZEPHYR_FOLDERTREE_ENDPOINT, ZEPHYR_PROJECT_ID и ZEPHYR_API_TOKEN."
            )
        folders = select_tree_target_folders(
            nodes=folder_nodes,
            root_folder_ids=[],
            leaf_only=False,
            name_pattern=None,
            root_path_pattern=None,
        )
    else:
        folders = _discover_folder_nodes(args, headers, scope="pipeline")
    folder_dicts = [_folder_to_dict(folder) for folder in folders]
    root_count = sum(1 for folder in folder_dicts if not folder.get("parent_id"))
    return {
        "folders": folder_dicts,
        "scope": normalized_scope,
        "folder_count": len(folder_dicts),
        "root_count": root_count,
        "source": source,
    }


def list_cycles_in_folder(
    folder_id: str,
    folder_name: str,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[dict[str, str]]:
    args = build_pipeline_args()
    headers = _pipeline_headers(args)
    folder = FolderNode(
        folder_id=str(folder_id),
        folder_name=str(folder_name or folder_id),
        parent_id=None,
    )
    executions = _fetch_executions_for_folder(args, headers, folder.folder_id)
    parsed_from, parsed_to = _parse_date_bounds(from_date, to_date)
    summaries: list[dict[str, str]] = []
    for cycle in executions:
        summary = _cycle_execution_summary(cycle)
        if not summary["test_run_id"] and not summary["cycle_key"]:
            continue
        if parsed_from or parsed_to:
            updated_raw = _read_cycle_field(
                cycle,
                ["updatedOn", "updatedDate", "actualStartDate", "plannedStartDate"],
                "",
            )
            exec_date = parse_date(updated_raw[:10] if updated_raw else None)
            if parsed_from and exec_date and exec_date < parsed_from:
                continue
            if parsed_to and exec_date and exec_date > parsed_to:
                continue
        summaries.append(summary)
    return _dedupe_cycle_summaries(summaries)


def fetch_cycles_for_folder(
    folder_id: str,
    folder_name: str,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any]:
    args = build_pipeline_args()
    headers = _pipeline_headers(args)
    folder = FolderNode(
        folder_id=str(folder_id),
        folder_name=str(folder_name or folder_id),
        parent_id=None,
    )
    executions = _fetch_executions_for_folder(args, headers, folder.folder_id)
    parsed_from, parsed_to = _parse_date_bounds(from_date, to_date)
    return _aggregate_cycles(
        folder,
        executions,
        args,
        headers,
        parsed_from=parsed_from,
        parsed_to=parsed_to,
    )


def fetch_cycles_for_test_run(
    cycle_key: str,
    *,
    test_run_id: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any]:
    args = build_pipeline_args()
    headers = _pipeline_headers(args)
    extra_params = parse_extra_params(args.extra_param)
    per_params = dict(extra_params)
    if test_run_id:
        if not args.project_id:
            raise ValueError("ZEPHYR_PROJECT_ID is not configured")
        per_params["query"] = sanitize_tql_query(
            f"testRun.projectId IN ({args.project_id}) AND testRun.id = {test_run_id}"
        )
    else:
        per_params["query"] = _build_cycle_key_query(str(args.project_id or ""), cycle_key)
    executions = fetch_executions(
        base_url=args.base_url,
        endpoint=args.endpoint,
        headers=headers,
        extra_params=per_params,
        page_size=args.page_size,
    )
    if not executions:
        label = test_run_id or cycle_key
        raise ValueError(f"Тест-цикл не найден: {label}")
    execution = executions[0]
    folder_id, folder_name = _extract_folder_info(execution)
    if not folder_id:
        folder_id = "unknown"
        folder_name = folder_name or "unknown"
    folder = FolderNode(
        folder_id=str(folder_id),
        folder_name=str(folder_name or folder_id),
        parent_id=None,
    )
    parsed_from, parsed_to = _parse_date_bounds(from_date, to_date)
    return _aggregate_cycles(
        folder,
        [execution],
        args,
        headers,
        parsed_from=parsed_from,
        parsed_to=parsed_to,
    )


def fetch_cycles_for_selected_runs(
    selected: list[dict[str, str]],
    *,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for item in selected:
        test_run_id = str(item.get("test_run_id") or "").strip()
        cycle_key = str(item.get("cycle_key") or "").strip()
        if not test_run_id and not cycle_key:
            continue
        chunk = fetch_cycles_for_test_run(
            cycle_key or test_run_id,
            test_run_id=test_run_id or None,
            from_date=from_date,
            to_date=to_date,
        )
        merged.update(chunk)
    if not merged:
        raise ValueError("Не удалось загрузить выбранные тест-циклы")
    return merged


def fetch_cycles_by_import_mode(
    *,
    import_mode: str,
    folder_id: str = "",
    folder_name: str = "",
    cycle_key: str = "",
    test_run_id: str = "",
    selected_cycles: list[dict[str, str]] | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any]:
    mode = (import_mode or "folder").strip().lower()
    if mode == "cycles":
        items = list(selected_cycles or [])
        if not items:
            raise ValueError("selected_cycles is required for cycles import")
        return fetch_cycles_for_selected_runs(items, from_date=from_date, to_date=to_date)
    if mode == "cycle":
        if test_run_id:
            return fetch_cycles_for_test_run(
                cycle_key or test_run_id,
                test_run_id=test_run_id,
                from_date=from_date,
                to_date=to_date,
            )
        if not cycle_key.strip():
            raise ValueError("cycle_key or test_run_id is required for cycle import")
        return fetch_cycles_for_test_run(
            cycle_key.strip(),
            from_date=from_date,
            to_date=to_date,
        )
    if not folder_id:
        raise ValueError("folder_id is required for folder import")
    return fetch_cycles_for_folder(
        folder_id,
        folder_name,
        from_date=from_date,
        to_date=to_date,
    )


def _natural_sort_key(value: str) -> list[Any]:
    parts = re.split(r"(\d+)", value.lower())
    result: list[Any] = []
    for part in parts:
        if not part:
            continue
        result.append(int(part) if part.isdigit() else part)
    return result


def _sort_cycle_summaries(summaries: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(
        summaries,
        key=lambda cycle: _natural_sort_key(
            cycle.get("cycle_key") or cycle.get("cycle_name") or cycle.get("test_run_id") or ""
        ),
    )


def _dedupe_cycle_summaries(summaries: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    unique: list[dict[str, str]] = []
    for summary in summaries:
        key = summary.get("test_run_id") or summary.get("cycle_key") or ""
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(summary)
    return _sort_cycle_summaries(unique)

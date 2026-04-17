"""Thin async-friendly wrappers around zephyr_weekly_report for bot use.

All Zephyr API calls are blocking (urllib). They are run in a thread-pool
executor via asyncio.to_thread so the bot event loop is never blocked.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any

import zephyr_weekly_report as zwr
import config


# ---------------------------------------------------------------------------
# Shared headers (built once per process)
# ---------------------------------------------------------------------------

def _headers() -> dict[str, str]:
    return zwr.build_headers("Authorization", "Bearer", config.ZEPHYR_API_TOKEN)


# ---------------------------------------------------------------------------
# Data types returned to handlers
# ---------------------------------------------------------------------------

@dataclass
class Folder:
    id: str
    name: str
    full_path: str


@dataclass
class TestRun:
    id: str
    name: str
    status: str
    updated_on: str


@dataclass
class RunItem:
    item_id: str
    case_id: str
    case_key: str
    case_name: str


@dataclass
class StatusOption:
    id: str
    name: str


# ---------------------------------------------------------------------------
# Folders
# ---------------------------------------------------------------------------

async def list_folders() -> list[Folder]:
    """Discover the folder tree and return leaf folders matching configured filters."""

    def _sync() -> list[Folder]:
        nodes, _, _ = zwr.discover_folders_tree_fallback(
            base_url=config.ZEPHYR_BASE_URL,
            folder_search_endpoint=config.ZEPHYR_FOLDER_SEARCH_ENDPOINT,
            foldertree_endpoint=config.ZEPHYR_FOLDERTREE_ENDPOINT,
            headers=_headers(),
            project_id=config.ZEPHYR_PROJECT_ID or None,
        )
        name_pattern = re.compile(config.ZEPHYR_TREE_NAME_REGEX) if config.ZEPHYR_TREE_NAME_REGEX else None
        selected = zwr.select_tree_target_folders(
            nodes=nodes,
            root_folder_ids=config.ZEPHYR_ROOT_FOLDER_IDS,
            leaf_only=True,
            name_pattern=name_pattern,
            root_path_pattern=None,
        )
        return [
            Folder(id=f.folder_id, name=f.folder_name, full_path=f.full_path)
            for f in selected
        ]

    return await asyncio.to_thread(_sync)


# ---------------------------------------------------------------------------
# Test runs inside a folder
# ---------------------------------------------------------------------------

async def list_runs(folder_id: str) -> list[TestRun]:
    """Fetch all test runs inside a folder."""

    def _sync() -> list[TestRun]:
        query = config.ZEPHYR_QUERY_TEMPLATE.replace("{folder_id}", folder_id)
        if config.ZEPHYR_PROJECT_ID:
            query = query.replace("{project_id}", config.ZEPHYR_PROJECT_ID)
        query = zwr.sanitize_tql_query(query)
        executions = zwr.fetch_executions(
            base_url=config.ZEPHYR_BASE_URL,
            endpoint=config.ZEPHYR_ENDPOINT,
            headers=_headers(),
            extra_params={"query": query, "maxResults": "40"},
            page_size=40,
        )
        runs: list[TestRun] = []
        for item in executions:
            run_id = str(item.get("id") or "")
            if not run_id:
                continue
            runs.append(TestRun(
                id=run_id,
                name=zwr._read_cycle_field(item, ["name", "testRunName"], run_id),
                status=zwr._read_cycle_field(item, ["status.name", "status", "result"], ""),
                updated_on=zwr._read_cycle_field(item, ["updatedOn", "executedOn"], ""),
            ))
        return runs

    return await asyncio.to_thread(_sync)


# ---------------------------------------------------------------------------
# Test cases (items) inside a run
# ---------------------------------------------------------------------------

async def list_run_items(test_run_id: str) -> list[RunItem]:
    """Fetch test-run items (one per test case) for a given test run."""

    def _sync() -> list[RunItem]:
        raw_items = zwr.fetch_testrun_items(
            base_url=config.ZEPHYR_BASE_URL,
            headers=_headers(),
            test_run_id=test_run_id,
        )
        result: list[RunItem] = []
        for item in raw_items:
            item_id = str(item.get("id") or "")
            if not item_id:
                continue
            last = item.get("$lastTestResult") or {}
            case = last.get("testCase") or {}
            result.append(RunItem(
                item_id=item_id,
                case_id=str(case.get("id") or ""),
                case_key=str(case.get("key") or ""),
                case_name=str(case.get("name") or f"item_{item_id}"),
            ))
        return result

    return await asyncio.to_thread(_sync)


# ---------------------------------------------------------------------------
# Status options
# ---------------------------------------------------------------------------

async def list_statuses() -> list[StatusOption]:
    """Fetch the project-level test result status dictionary."""

    def _sync() -> list[StatusOption]:
        names = zwr.fetch_test_result_status_names(
            base_url=config.ZEPHYR_BASE_URL,
            headers=_headers(),
            project_id=config.ZEPHYR_PROJECT_ID or None,
        )
        return [StatusOption(id=sid, name=sname) for sid, sname in names.items()]

    return await asyncio.to_thread(_sync)


# ---------------------------------------------------------------------------
# Upload a result
# ---------------------------------------------------------------------------

async def upload_result(
    test_run_id: str,
    item_id: str,
    status_id: str,
    comment: str | None = None,
) -> dict[str, Any] | None:
    """POST a new test result for a test-run item."""

    def _sync() -> dict[str, Any] | None:
        return zwr.post_test_result(
            base_url=config.ZEPHYR_BASE_URL,
            headers=_headers(),
            test_run_id=test_run_id,
            item_id=item_id,
            status_id=status_id,
            comment=comment,
        )

    return await asyncio.to_thread(_sync)

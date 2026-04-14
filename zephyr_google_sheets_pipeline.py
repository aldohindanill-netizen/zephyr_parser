#!/usr/bin/env python3
"""Generate and sync Zephyr daily run data with Google Sheets.

This script extends existing Zephyr parsing logic:
- generate-sheet: builds/updates Google Sheet rows from Zephyr folder runs
- sync-sheet: reads Pass/Fail + comment from sheet and writes back to Zephyr
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import zephyr_weekly_report as zr


RUN_HEADERS = [
    "scenario",
    "cycle_key",
    "cycle_name",
    "test_case_name",
    "test_case_key",
    "test_run_id",
    "test_run_item_id",
    "test_result_id",
    "Pass",
    "Fail",
    "Comment",
    "zephyr_status",
    "sync_status",
    "synced_at",
    "folder_id",
    "folder_name",
]

DEFAULT_UPDATE_ENDPOINT_TEMPLATE = "rest/tests/1.0/testresult/{test_result_id}"


@dataclass
class RunRow:
    scenario: str
    cycle_key: str
    cycle_name: str
    test_case_name: str
    test_case_key: str
    test_run_id: str
    test_run_item_id: str
    test_result_id: str
    pass_value: bool
    fail_value: bool
    comment: str
    zephyr_status: str
    sync_status: str
    synced_at: str
    folder_id: str
    folder_name: str

    def to_values(self) -> list[Any]:
        return [
            self.scenario,
            self.cycle_key,
            self.cycle_name,
            self.test_case_name,
            self.test_case_key,
            self.test_run_id,
            self.test_run_item_id,
            self.test_result_id,
            self.pass_value,
            self.fail_value,
            self.comment,
            self.zephyr_status,
            self.sync_status,
            self.synced_at,
            self.folder_id,
            self.folder_name,
        ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Google Sheet from Zephyr and sync Pass/Fail back."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--base-url", required=True, help="Zephyr base URL")
        p.add_argument(
            "--token",
            default=None,
            help="Zephyr API token (optional; otherwise ZEPHYR_API_TOKEN env)",
        )
        p.add_argument("--token-header", default="Authorization")
        p.add_argument("--token-prefix", default="Bearer")
        p.add_argument(
            "--project-id",
            required=True,
            help="Zephyr project id used for status resolution and discovery",
        )
        p.add_argument(
            "--folder-search-endpoint",
            default="rest/tests/1.0/folder/search",
        )
        p.add_argument(
            "--foldertree-endpoint",
            default="rest/tests/1.0/foldertree",
        )
        p.add_argument(
            "--tree-source-endpoint",
            default=None,
            help="Optional instance-specific tree endpoint",
        )
        p.add_argument(
            "--tree-source-method",
            choices=("GET", "POST"),
            default="GET",
        )
        p.add_argument("--tree-source-query-json", default=None)
        p.add_argument("--tree-source-body-json", default=None)
        p.add_argument(
            "--endpoint",
            default="rest/tests/1.0/testrun/search",
            help="Test run search endpoint",
        )
        p.add_argument(
            "--query-template",
            default="testRun.projectId IN ({project_id}) AND testRun.folderTreeId IN ({folder_id}) ORDER BY testRun.name ASC",
        )
        p.add_argument(
            "--extra-param",
            action="append",
            default=[],
            help="Additional query params key=value",
        )
        p.add_argument("--page-size", type=int, default=100)
        p.add_argument(
            "--testcase-endpoint-template",
            action="append",
            default=["rest/tests/1.0/testrun/{cycle_id}/testcase/search"],
        )
        p.add_argument(
            "--synthetic-cycle-ids",
            action="store_true",
            help="Use synthetic cycle ids when not present",
        )
        p.add_argument(
            "--branch-name",
            required=True,
            help="Branch/folder name in Zephyr (used to select target folder)",
        )
        p.add_argument(
            "--folder-parent-id",
            default=None,
            help="Optional parent folder id to disambiguate folder name",
        )
        p.add_argument(
            "--folder-path-regex",
            default=None,
            help="Optional regex to filter target folder full path",
        )
        p.add_argument(
            "--from-date",
            default=None,
            help="Optional inclusive date filter (YYYY-MM-DD)",
        )
        p.add_argument(
            "--to-date",
            default=None,
            help="Optional inclusive date filter (YYYY-MM-DD)",
        )
        p.add_argument(
            "--create-folder-first",
            action="store_true",
            help="Ensure Zephyr folder exists before generate step.",
        )
        p.add_argument(
            "--create-folder-name",
            default=None,
            help="Explicit folder name for create-first flow.",
        )
        p.add_argument(
            "--create-folder-name-template",
            default=None,
            help="strftime template for create-first flow, e.g. %%Y.%%m.%%d",
        )
        p.add_argument(
            "--create-folder-parent-id",
            default=None,
            help="Optional parent folder id for create-first flow.",
        )
        p.add_argument(
            "--create-folder-endpoint",
            default="/rest/tests/1.0/folder",
            help="Folder create endpoint for create-first flow.",
        )
        p.add_argument(
            "--create-folder-name-field",
            default="name",
            help="Request body field for folder name.",
        )
        p.add_argument(
            "--create-folder-project-id-field",
            default="projectId",
            help="Request body field for project id.",
        )
        p.add_argument(
            "--create-folder-parent-id-field",
            default="parentId",
            help="Request body field for parent id.",
        )
        p.add_argument(
            "--create-folder-body-json",
            default=None,
            help="Extra JSON merged into folder create body.",
        )
        p.add_argument(
            "--create-folder-dry-run",
            action="store_true",
            help="Print create payload without POST request.",
        )

    def add_google(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--google-service-account-file",
            default=None,
            help="Path to Google service-account JSON. Or env GOOGLE_SERVICE_ACCOUNT_FILE",
        )
        p.add_argument(
            "--spreadsheet-id",
            default=None,
            help="Existing spreadsheet id (optional for generate-sheet)",
        )
        p.add_argument(
            "--spreadsheet-title",
            default="Zephyr Daily Execution Sheet",
            help="Used when spreadsheet is created",
        )
        p.add_argument("--config-sheet", default="Config")
        p.add_argument("--run-sheet", default="Run")

    g = sub.add_parser("generate-sheet", help="Generate/update sheet rows from Zephyr")
    add_common(g)
    add_google(g)
    g.add_argument(
        "--fields",
        default=(
            "id,key,name,folderId,iterationId,projectVersionId,environmentId,userKeys,"
            "environmentIds,plannedStartDate,plannedEndDate,executionTime,estimatedTime,"
            "testResultStatuses,testCaseCount,issueCount,status(id,name,i18nKey,color),"
            "customFieldValues,createdOn,createdBy,updatedOn,updatedBy,owner,objective"
        ),
    )
    g.add_argument(
        "--status-pass-name",
        default="Pass",
        help="Zephyr status display name for pass",
    )
    g.add_argument(
        "--status-fail-name",
        default="Fail",
        help="Zephyr status display name for fail",
    )

    s = sub.add_parser("sync-sheet", help="Sync checked rows from sheet to Zephyr")
    add_common(s)
    add_google(s)
    s.add_argument(
        "--update-endpoint-template",
        default=DEFAULT_UPDATE_ENDPOINT_TEMPLATE,
        help="Zephyr endpoint template for status/comment update",
    )
    s.add_argument(
        "--update-method",
        choices=("PUT", "POST", "PATCH"),
        default="PUT",
    )
    s.add_argument(
        "--update-status-id-field",
        default="testResultStatusId",
    )
    s.add_argument(
        "--update-comment-field",
        default="comment",
    )
    s.add_argument(
        "--update-extra-body-json",
        default=None,
        help="JSON object merged into update request body",
    )
    s.add_argument("--status-pass-name", default="Pass")
    s.add_argument("--status-fail-name", default="Fail")
    s.add_argument(
        "--writeback",
        action="store_true",
        help="Write sync results back to sheet (default true).",
    )
    s.add_argument(
        "--no-writeback",
        action="store_true",
        help="Do not write sync status columns to sheet.",
    )
    return parser.parse_args()


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


def _resolve_token(args: argparse.Namespace) -> str:
    token = args.token or os.getenv("ZEPHYR_API_TOKEN")
    if not token:
        raise ValueError("Missing token. Pass --token or set ZEPHYR_API_TOKEN")
    return token


def _resolve_google_credentials(args: argparse.Namespace) -> str:
    path = args.google_service_account_file or os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    if not path:
        raise ValueError(
            "Missing Google credentials path. Pass --google-service-account-file "
            "or set GOOGLE_SERVICE_ACCOUNT_FILE."
        )
    if not os.path.exists(path):
        raise ValueError(f"Google credentials file does not exist: {path}")
    return path


def _build_google_services(credentials_file: str) -> tuple[Any, Any]:
    try:
        from google.oauth2 import service_account  # type: ignore
        from googleapiclient.discovery import build  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Google API libraries are required. Install: "
            "pip install google-api-python-client google-auth"
        ) from exc

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file, scopes=scopes
    )
    sheets = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    drive = build("drive", "v3", credentials=credentials, cache_discovery=False)
    return sheets, drive


def _ensure_spreadsheet(
    sheets: Any,
    spreadsheet_id: str | None,
    spreadsheet_title: str,
) -> tuple[str, str]:
    if spreadsheet_id:
        meta = (
            sheets.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="spreadsheetId,spreadsheetUrl")
            .execute()
        )
        return str(meta["spreadsheetId"]), str(meta.get("spreadsheetUrl", ""))

    created = (
        sheets.spreadsheets()
        .create(body={"properties": {"title": spreadsheet_title}})
        .execute()
    )
    return str(created["spreadsheetId"]), str(created.get("spreadsheetUrl", ""))


def _ensure_sheet_tabs(
    sheets: Any, spreadsheet_id: str, tab_names: list[str]
) -> dict[str, int]:
    meta = (
        sheets.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )
    existing = {
        s["properties"]["title"]: int(s["properties"]["sheetId"])
        for s in meta.get("sheets", [])
    }
    requests: list[dict[str, Any]] = []
    for tab in tab_names:
        if tab not in existing:
            requests.append({"addSheet": {"properties": {"title": tab}}})
    if requests:
        batch = (
            sheets.spreadsheets()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            )
            .execute()
        )
        replies = batch.get("replies", [])
        for reply in replies:
            add = reply.get("addSheet", {})
            props = add.get("properties", {})
            title = props.get("title")
            sid = props.get("sheetId")
            if title and sid is not None:
                existing[str(title)] = int(sid)
    return existing


def _write_config_sheet(
    sheets: Any,
    spreadsheet_id: str,
    config_sheet: str,
    config_values: list[tuple[str, str]],
) -> None:
    values = [["key", "value"]] + [[k, v] for k, v in config_values]
    (
        sheets.spreadsheets()
        .values()
        .clear(
            spreadsheetId=spreadsheet_id,
            range=f"{config_sheet}!A:Z",
            body={},
        )
        .execute()
    )
    (
        sheets.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{config_sheet}!A1",
            valueInputOption="RAW",
            body={"values": values},
        )
        .execute()
    )


def _write_run_sheet(
    sheets: Any,
    spreadsheet_id: str,
    run_sheet: str,
    run_sheet_id: int,
    rows: list[RunRow],
) -> None:
    values = [RUN_HEADERS] + [row.to_values() for row in rows]
    (
        sheets.spreadsheets()
        .values()
        .clear(
            spreadsheetId=spreadsheet_id,
            range=f"{run_sheet}!A:Z",
            body={},
        )
        .execute()
    )
    (
        sheets.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{run_sheet}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        )
        .execute()
    )
    last_row = max(2, len(rows) + 1)
    requests: list[dict[str, Any]] = [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": run_sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "setDataValidation": {
                "range": {
                    "sheetId": run_sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": last_row,
                    "startColumnIndex": 8,
                    "endColumnIndex": 9,
                },
                "rule": {
                    "condition": {"type": "BOOLEAN"},
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
        {
            "setDataValidation": {
                "range": {
                    "sheetId": run_sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": last_row,
                    "startColumnIndex": 9,
                    "endColumnIndex": 10,
                },
                "rule": {
                    "condition": {"type": "BOOLEAN"},
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
    ]
    (
        sheets.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
        .execute()
    )


def _resolve_target_folder(
    args: argparse.Namespace,
    headers: dict[str, str],
    tree_source_query: dict[str, Any],
    tree_source_body: dict[str, Any],
) -> zr.FolderNode:
    nodes: list[zr.FolderNode] = []
    if args.tree_source_endpoint:
        try:
            nodes, _ = zr.discover_folders_custom_tree_source(
                base_url=args.base_url,
                headers=headers,
                endpoint=args.tree_source_endpoint,
                method=args.tree_source_method,
                query_params=tree_source_query,
                body=tree_source_body,
            )
        except Exception:
            nodes = []

    if not nodes:
        nodes, _, _ = zr.discover_folders_tree_fallback(
            base_url=args.base_url,
            headers=headers,
            project_id=args.project_id,
            folder_search_endpoint=args.folder_search_endpoint,
            foldertree_endpoint=args.foldertree_endpoint,
        )
    if not nodes:
        raise RuntimeError("Cannot discover folders from Zephyr tree endpoints.")

    name = args.branch_name.strip()
    parent = args.folder_parent_id.strip() if args.folder_parent_id else None
    path_pattern = (
        re.compile(args.folder_path_regex) if args.folder_path_regex else None
    )
    candidates: list[zr.FolderNode] = []
    for node in nodes:
        if node.folder_name.strip() != name:
            continue
        if parent and (node.parent_id or "").strip() != parent:
            continue
        if path_pattern and not path_pattern.search(node.full_path or ""):
            continue
        candidates.append(node)

    if not candidates:
        raise RuntimeError(
            f"Target folder '{name}' not found. "
            "Set --folder-parent-id/--folder-path-regex to disambiguate."
        )
    return sorted(candidates, key=lambda x: x.folder_id)[0]


def _build_rows_from_folder(
    args: argparse.Namespace,
    headers: dict[str, str],
    folder: zr.FolderNode,
) -> list[RunRow]:
    from_date = zr.parse_date(args.from_date)
    to_date = zr.parse_date(args.to_date)
    if from_date and to_date and from_date > to_date:
        raise ValueError("--from-date must be <= --to-date")

    extra_params = zr.parse_extra_params(args.extra_param)
    if args.command == "generate-sheet" and args.fields:
        extra_params.setdefault("fields", args.fields)
    extra_params["query"] = zr.sanitize_tql_query(
        zr.fill_template(
            zr.fill_template(args.query_template, "project_id", args.project_id, "--query-template"),
            "folder_id",
            folder.folder_id,
            "--query-template",
        )
    )
    cycles = zr.fetch_executions(
        base_url=args.base_url,
        endpoint=args.endpoint,
        headers=headers,
        extra_params=extra_params,
        page_size=args.page_size,
    )
    if not cycles:
        return []

    status_names = zr.fetch_test_result_status_names(args.base_url, headers, args.project_id)
    case_steps_rows = zr.build_case_step_rows(
        folder=folder,
        cycles=cycles,
        base_url=args.base_url,
        headers=headers,
        status_names=status_names,
        synthetic_cycle_ids=args.synthetic_cycle_ids,
    )
    cycles_cases_rows = zr.build_cycle_case_rows(
        folder=folder,
        cycles=cycles,
        testcase_endpoint_templates=args.testcase_endpoint_template,
        base_url=args.base_url,
        headers=headers,
        synthetic_cycle_ids=args.synthetic_cycle_ids,
    )
    report = zr.aggregate_readable_daily_reports_from_steps(case_steps_rows, cycles_cases_rows)
    payload = report.get((folder.folder_id, folder.folder_name))
    if not payload:
        return []

    result_lookup: dict[tuple[str, str], tuple[str, str]] = {}
    for row in case_steps_rows:
        if len(row) < 11:
            continue
        test_run_id = str(row[5]).strip() if row[5] else ""
        test_case_key = str(row[7]).strip() if row[7] else ""
        test_run_item_id = str(row[9]).strip() if row[9] else ""
        test_result_id = str(row[10]).strip() if row[10] else ""
        if not test_run_id or not test_case_key or not test_result_id:
            continue
        result_lookup.setdefault((test_run_id, test_case_key), (test_run_item_id, test_result_id))

    rows: list[RunRow] = []
    cycles_map = payload.get("cycles", {})
    for cycle in sorted(cycles_map.values(), key=lambda item: (item["cycle_key"], item["cycle_name"])):
        sorted_cases, _ = zr._prepare_cycle_cases_with_groups(cycle)  # pylint: disable=protected-access
        for case in sorted_cases:
            date_str = case.get("execution_date", "") or ""
            if from_date or to_date:
                include = True
                if date_str:
                    try:
                        d = zr.parse_datetime(date_str).date()
                        if from_date and d < from_date:
                            include = False
                        if to_date and d > to_date:
                            include = False
                    except ValueError:
                        include = True
                if not include:
                    continue

            test_case_key = str(case.get("test_case_key", "") or "")
            test_run_id = str(cycle.get("cycle_id", "") or "")
            test_run_item_id, test_result_id = result_lookup.get(
                (test_run_id, test_case_key), ("", "")
            )
            rows.append(
                RunRow(
                    scenario=str(case.get("_criterion_display", "") or ""),
                    cycle_key=str(cycle.get("cycle_key", "") or ""),
                    cycle_name=str(cycle.get("cycle_name", "") or ""),
                    test_case_name=str(case.get("test_case_name", "") or ""),
                    test_case_key=test_case_key,
                    test_run_id=test_run_id,
                    test_run_item_id=test_run_item_id,
                    test_result_id=test_result_id,
                    pass_value=False,
                    fail_value=False,
                    comment=str(case.get("comment", "") or ""),
                    zephyr_status=str(case.get("result", "") or ""),
                    sync_status="",
                    synced_at="",
                    folder_id=folder.folder_id,
                    folder_name=folder.folder_name,
                )
            )
    return rows


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_status_name_to_id(
    base_url: str,
    headers: dict[str, str],
    project_id: str,
) -> dict[str, str]:
    by_id = zr.fetch_test_result_status_names(base_url, headers, project_id)
    by_name: dict[str, str] = {}
    for sid, name in by_id.items():
        normalized = (name or "").strip().lower()
        if normalized:
            by_name[normalized] = sid
    return by_name


def _resolve_pass_fail_status_ids(
    base_url: str,
    headers: dict[str, str],
    project_id: str,
    pass_name: str,
    fail_name: str,
) -> tuple[str, str]:
    status_ids = _build_status_name_to_id(base_url, headers, project_id)
    pass_id = status_ids.get(pass_name.strip().lower())
    fail_id = status_ids.get(fail_name.strip().lower())
    if not pass_id or not fail_id:
        raise RuntimeError(
            f"Cannot resolve status ids for '{pass_name}'/'{fail_name}'."
        )
    return pass_id, fail_id


def _sync_sheet_rows(args: argparse.Namespace, sheets: Any, headers: dict[str, str]) -> None:
    values_resp = (
        sheets.spreadsheets()
        .values()
        .get(
            spreadsheetId=args.spreadsheet_id,
            range=f"{args.run_sheet}!A2:P",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )
    rows: list[list[Any]] = values_resp.get("values", [])
    if not rows:
        print("No rows in Run sheet.")
        return

    pass_id, fail_id = _resolve_pass_fail_status_ids(
        base_url=args.base_url,
        headers=headers,
        project_id=args.project_id,
        pass_name=args.status_pass_name,
        fail_name=args.status_fail_name,
    )

    extra_body = _parse_json_object_arg(args.update_extra_body_json, "--update-extra-body-json")
    updates: list[dict[str, Any]] = []
    updated = 0
    skipped = 0
    errors = 0

    for idx, row in enumerate(rows, start=2):
        padded = list(row) + [""] * (16 - len(row))
        test_result_id = str(padded[7] or "").strip()
        pass_flag = _to_bool(padded[8])
        fail_flag = _to_bool(padded[9])
        comment = str(padded[10] or "")

        if pass_flag and fail_flag:
            errors += 1
            updates.append(
                {
                    "range": f"{args.run_sheet}!L{idx}:N{idx}",
                    "values": [[str(padded[11] or ""), "ERROR: both Pass and Fail are checked", _now_iso()]],
                }
            )
            continue
        if not pass_flag and not fail_flag:
            skipped += 1
            continue
        if not test_result_id:
            errors += 1
            updates.append(
                {
                    "range": f"{args.run_sheet}!L{idx}:N{idx}",
                    "values": [[str(padded[11] or ""), "ERROR: missing test_result_id", _now_iso()]],
                }
            )
            continue

        status_id = pass_id if pass_flag else fail_id
        status_name = args.status_pass_name if pass_flag else args.status_fail_name
        endpoint = args.update_endpoint_template.replace(
            "{test_result_id}", quote(test_result_id, safe="")
        )
        body = dict(extra_body)
        body[args.update_status_id_field] = int(status_id) if str(status_id).isdigit() else status_id
        body[args.update_comment_field] = comment
        try:
            zr.request_json(
                base_url=args.base_url,
                endpoint=endpoint,
                headers=headers,
                method=args.update_method,
                body=body,
            )
            updated += 1
            updates.append(
                {
                    "range": f"{args.run_sheet}!L{idx}:N{idx}",
                    "values": [[status_name, "OK", _now_iso()]],
                }
            )
        except Exception as exc:  # pylint: disable=broad-except
            errors += 1
            updates.append(
                {
                    "range": f"{args.run_sheet}!L{idx}:N{idx}",
                    "values": [[str(padded[11] or ""), f"ERROR: {exc}", _now_iso()]],
                }
            )

    do_writeback = args.writeback or not args.no_writeback
    if updates and do_writeback:
        (
            sheets.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=args.spreadsheet_id,
                body={"valueInputOption": "RAW", "data": updates},
            )
            .execute()
        )

    print(f"Sync finished. Updated: {updated}, skipped: {skipped}, errors: {errors}")


def generate_sheet(args: argparse.Namespace) -> int:
    token = _resolve_token(args)
    google_credentials = _resolve_google_credentials(args)
    headers = zr.build_headers(args.token_header, args.token_prefix, token)
    tree_source_query = _parse_json_object_arg(args.tree_source_query_json, "--tree-source-query-json")
    tree_source_body = _parse_json_object_arg(args.tree_source_body_json, "--tree-source-body-json")
    created_or_existing = zr.ensure_folder_created_or_existing(
        args=args,
        headers=headers,
        tree_source_query=tree_source_query,
        tree_source_body=tree_source_body,
    )
    folder = created_or_existing or _resolve_target_folder(args, headers, tree_source_query, tree_source_body)
    rows = _build_rows_from_folder(args, headers, folder)
    pass_id, fail_id = _resolve_pass_fail_status_ids(
        base_url=args.base_url,
        headers=headers,
        project_id=args.project_id,
        pass_name=args.status_pass_name,
        fail_name=args.status_fail_name,
    )
    sheets, _ = _build_google_services(google_credentials)
    spreadsheet_id, spreadsheet_url = _ensure_spreadsheet(
        sheets=sheets,
        spreadsheet_id=args.spreadsheet_id,
        spreadsheet_title=args.spreadsheet_title,
    )
    tabs = _ensure_sheet_tabs(sheets, spreadsheet_id, [args.config_sheet, args.run_sheet])
    _write_config_sheet(
        sheets=sheets,
        spreadsheet_id=spreadsheet_id,
        config_sheet=args.config_sheet,
        config_values=[
            ("branch_name", args.branch_name),
            ("project_id", args.project_id),
            ("folder_id", folder.folder_id),
            ("folder_name", folder.folder_name),
            ("generated_at_utc", _now_iso()),
            ("pass_status_name", args.status_pass_name),
            ("fail_status_name", args.status_fail_name),
            ("pass_status_id", pass_id),
            ("fail_status_id", fail_id),
            ("update_endpoint_template", DEFAULT_UPDATE_ENDPOINT_TEMPLATE),
            ("update_status_id_field", "testResultStatusId"),
            ("update_comment_field", "comment"),
        ],
    )
    _write_run_sheet(
        sheets=sheets,
        spreadsheet_id=spreadsheet_id,
        run_sheet=args.run_sheet,
        run_sheet_id=tabs[args.run_sheet],
        rows=rows,
    )
    print(f"Spreadsheet ID: {spreadsheet_id}")
    if spreadsheet_url:
        print(f"Spreadsheet URL: {spreadsheet_url}")
    print(f"Rows written: {len(rows)}")
    return 0


def sync_sheet(args: argparse.Namespace) -> int:
    token = _resolve_token(args)
    google_credentials = _resolve_google_credentials(args)
    if not args.spreadsheet_id:
        raise ValueError("--spreadsheet-id is required for sync-sheet")
    headers = zr.build_headers(args.token_header, args.token_prefix, token)
    sheets, _ = _build_google_services(google_credentials)
    _sync_sheet_rows(args, sheets, headers)
    return 0


def main() -> int:
    args = parse_args()
    try:
        if args.command == "generate-sheet":
            return generate_sheet(args)
        if args.command == "sync-sheet":
            return sync_sheet(args)
        raise ValueError(f"Unsupported command: {args.command}")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

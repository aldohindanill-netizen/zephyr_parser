#!/usr/bin/env python3
"""Рекурсивно удалить дочерние страницы Confluence под родителем (родитель сохраняется).

По умолчанию dry-run; --execute — реальный DELETE (страницы в корзине Confluence).
Использует ZEPHYR_CONFLUENCE_* из env/.env (флаги publish не обязательны).
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from repo_env import load_repo_env_for_scripts  # noqa: E402
from zephyr_weekly_report import (  # noqa: E402
    ConfluencePublishConfig,
    _confluence_request_json,
)


def _load_confluence_connection_config() -> ConfluencePublishConfig:
    base_url = (os.getenv("ZEPHYR_CONFLUENCE_BASE_URL") or "").strip().rstrip("/")
    user = (os.getenv("ZEPHYR_CONFLUENCE_USER") or "").strip()
    api_token = (os.getenv("ZEPHYR_CONFLUENCE_API_TOKEN") or "").strip()
    space_key = (os.getenv("ZEPHYR_CONFLUENCE_SPACE_KEY") or "").strip()
    api_prefix = (os.getenv("ZEPHYR_CONFLUENCE_API_PREFIX") or "rest/api").strip().strip("/")
    auth_scheme = (os.getenv("ZEPHYR_CONFLUENCE_AUTH_SCHEME") or "basic").strip().lower()
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
            "Missing Confluence env vars: " + ", ".join(missing)
        )
    return ConfluencePublishConfig(
        base_url=base_url,
        user=user,
        api_token=api_token,
        space_key=space_key,
        api_prefix=api_prefix,
        auth_scheme=auth_scheme,
    )


def _list_child_pages(
    cfg: ConfluencePublishConfig, parent_page_id: str
) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    start = 0
    limit = 50
    while True:
        payload = _confluence_request_json(
            cfg,
            f"{cfg.api_prefix}/content/{parent_page_id}/child/page",
            params={"limit": str(limit), "start": str(start)},
        )
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            break
        for item in results:
            if not isinstance(item, dict):
                continue
            page_id = str(item.get("id") or "").strip()
            title = str(item.get("title") or "").strip()
            if page_id:
                out.append((page_id, title))
        size = int(payload.get("size") or 0) if isinstance(payload, dict) else 0
        if size < limit:
            break
        start += limit
    return out


def _delete_page(cfg: ConfluencePublishConfig, page_id: str) -> None:
    _confluence_request_json(
        cfg,
        f"{cfg.api_prefix}/content/{page_id}",
        method="DELETE",
    )


def _delete_subtree(
    cfg: ConfluencePublishConfig,
    page_id: str,
    title: str,
    *,
    dry_run: bool,
    verbose: bool,
    log: Callable[[str], None],
    counter: list[int],
) -> None:
    for child_id, child_title in _list_child_pages(cfg, page_id):
        _delete_subtree(
            cfg,
            child_id,
            child_title,
            dry_run=dry_run,
            verbose=verbose,
            log=log,
            counter=counter,
        )
    counter[0] += 1
    if dry_run or verbose:
        action = "would delete" if dry_run else "deleting"
        log(f"  {action}: {page_id} — {title}")
    elif not dry_run:
        log(f"  deleting: {page_id} — {title}")
    if not dry_run:
        _delete_page(cfg, page_id)


def _resolve_parent_page_id(cli_parent: str | None) -> str:
    parent = (cli_parent or os.getenv("ZEPHYR_CONFLUENCE_PARENT_PAGE_ID") or "").strip()
    if not parent:
        raise ValueError(
            "Parent page id required: set ZEPHYR_CONFLUENCE_PARENT_PAGE_ID "
            "or pass --parent-page-id."
        )
    return parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete all Confluence pages under a parent (recursive). "
            "The parent page itself is not deleted. Dry-run unless --execute."
        )
    )
    parser.add_argument(
        "--parent-page-id",
        default=None,
        help="Confluence page id (default: ZEPHYR_CONFLUENCE_PARENT_PAGE_ID)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete pages (default: dry-run only)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print each page in deletion order",
    )
    parser.add_argument(
        "--use-local-env",
        action="store_true",
        help="Load .env.local overrides (sandbox Confluence parent id)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dry_run = not args.execute

    try:
        load_repo_env_for_scripts(use_local_env=args.use_local_env)
        cfg = _load_confluence_connection_config()
        parent_page_id = _resolve_parent_page_id(args.parent_page_id)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    def log(msg: str) -> None:
        print(msg)

    direct_children = _list_child_pages(cfg, parent_page_id)
    if not direct_children:
        log(f"No child pages under parent {parent_page_id}.")
        return 0

    mode = "DRY-RUN" if dry_run else "EXECUTE"
    log(f"[{mode}] Parent page {parent_page_id} — {len(direct_children)} top-level child(ren)")
    if dry_run:
        log("Pass --execute to delete (pages go to Confluence trash).")

    counter: list[int] = [0]
    for page_id, title in direct_children:
        if args.verbose:
            log(f"Subtree under: {page_id} — {title}")
        _delete_subtree(
            cfg,
            page_id,
            title,
            dry_run=dry_run,
            verbose=args.verbose,
            log=log,
            counter=counter,
        )

    if dry_run:
        log(f"would_delete={counter[0]}")
    else:
        log(f"deleted={counter[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

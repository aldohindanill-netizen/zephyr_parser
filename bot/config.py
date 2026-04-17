"""Centralised configuration loaded from environment variables."""

from __future__ import annotations

import os


def _require(key: str) -> str:
    value = os.environ.get(key, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {key!r} is not set")
    return value


def _optional(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------
BOT_TOKEN: str = _require("BOT_TOKEN")

# ---------------------------------------------------------------------------
# Zephyr API
# ---------------------------------------------------------------------------
ZEPHYR_API_TOKEN: str = _require("ZEPHYR_API_TOKEN")
ZEPHYR_BASE_URL: str = _require("ZEPHYR_BASE_URL")
ZEPHYR_PROJECT_ID: str = _optional("ZEPHYR_PROJECT_ID", "")
ZEPHYR_ENDPOINT: str = _optional("ZEPHYR_ENDPOINT", "rest/tests/1.0/testrun/search")
ZEPHYR_FOLDERTREE_ENDPOINT: str = _optional(
    "ZEPHYR_FOLDERTREE_ENDPOINT",
    f"rest/tests/1.0/project/{_optional('ZEPHYR_PROJECT_ID', '')}/foldertree/testrun",
)
ZEPHYR_FOLDER_SEARCH_ENDPOINT: str = _optional(
    "ZEPHYR_FOLDER_SEARCH_ENDPOINT", "rest/tests/1.0/folder/search"
)
ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE: str = _optional(
    "ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE",
    "rest/tests/1.0/testrun/{cycle_id}/testcase/search",
)
ZEPHYR_ROOT_FOLDER_IDS: list[str] = [
    rid.strip()
    for rid in _optional("ZEPHYR_ROOT_FOLDER_IDS", "").split(",")
    if rid.strip()
]
ZEPHYR_TREE_NAME_REGEX: str = _optional("ZEPHYR_TREE_NAME_REGEX", "")
ZEPHYR_QUERY_TEMPLATE: str = _optional(
    "ZEPHYR_QUERY_TEMPLATE",
    "testRun.projectId IN ({project_id}) AND testRun.folderTreeId IN ({folder_id}) ORDER BY testRun.name ASC",
)

# ---------------------------------------------------------------------------
# Redis (session storage)
# ---------------------------------------------------------------------------
REDIS_HOST: str = _optional("REDIS_HOST", "localhost")
REDIS_PORT: int = int(_optional("REDIS_PORT", "6379"))
REDIS_PASSWORD: str | None = os.environ.get("REDIS_PASSWORD") or None
REDIS_DB: int = int(_optional("REDIS_DB", "0"))
REDIS_SESSION_TTL: int = int(_optional("REDIS_SESSION_TTL", "1800"))

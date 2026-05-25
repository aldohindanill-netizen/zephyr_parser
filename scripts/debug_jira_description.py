#!/usr/bin/env python3
"""Debug Jira issue description parsing (one key)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

import argparse

from repo_env import load_repo_env_for_scripts  # noqa: E402

from bug_duplicate_detection import parse_jira_description_fields  # noqa: E402
from zephyr_weekly_report import (  # noqa: E402
    _description_to_text,
    _jira_bug_metadata_auth_headers,
    build_headers,
    request_json,
)

_argp = argparse.ArgumentParser(description=__doc__)
_argp.add_argument("issue_key", nargs="?", default="CSD-47279")
_argp.add_argument(
    "--use-local-env",
    action="store_true",
    help="Load .env.local overrides",
)
_cli = _argp.parse_args()
load_repo_env_for_scripts(use_local_env=_cli.use_local_env)

key = _cli.issue_key.strip()
base = (os.getenv("ZEPHYR_JIRA_BASE_URL") or os.getenv("ZEPHYR_BASE_URL") or "").rstrip("/")
token = (os.getenv("ZEPHYR_JIRA_API_TOKEN") or os.getenv("ZEPHYR_API_TOKEN") or "").strip()
headers = _jira_bug_metadata_auth_headers(
    build_headers("Authorization", "Bearer", token)
) or build_headers("Authorization", "Bearer", token)

for prefix in ("/rest/api/2/issue/", "/rest/api/3/issue/"):
    try:
        payload = request_json(
            base,
            f"{prefix}{key}",
            headers,
            params={"fields": "description"},
        )
    except Exception as exc:
        print(f"{prefix} ERROR: {exc}")
        continue
    desc = (payload.get("fields") or {}).get("description")
    print(f"=== {key} via {prefix} ===")
    print("description type:", type(desc).__name__)
    if isinstance(desc, dict):
        print("top keys:", list(desc.keys())[:10])
    flat = _description_to_text(desc)
    print("flat lines:", len(flat.splitlines()))
    print("--- flat (first 1200 chars) ---")
    print(flat[:1200])
    print("--- parsed ---")
    print(json.dumps(parse_jira_description_fields(desc), ensure_ascii=False, indent=2))
    break

#!/usr/bin/env python3
"""Re-fetch Jira description fields and refresh duplicate_candidates.json."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

import argparse

from repo_env import load_repo_env_for_scripts  # noqa: E402

from bug_duplicate_detection import (  # noqa: E402
    bugs_duplicate_publish_min_confidence,
    find_duplicate_candidates,
    load_duplicate_overrides,
    load_embedding_cache,
    resolve_paths_for_rollup_dir,
    write_duplicate_candidates_debug,
)
from zephyr_weekly_report import (  # noqa: E402
    _fetch_jira_bug_metadata,
    _jira_bug_metadata_auth_headers,
    _resolve_weekly_jira_metadata_base,
    build_headers,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--use-local-env",
        action="store_true",
        help="Load .env.local overrides (reports_local paths)",
    )
    args = parser.parse_args()
    load_repo_env_for_scripts(use_local_env=args.use_local_env)

    rollup_dir = (
        os.getenv("ZEPHYR_BUGS_ROLLUP_DIR", "reports/bugs_rollup").strip()
        or "reports/bugs_rollup"
    )
    keys_path = Path(rollup_dir) / "duplicate_rollup_keys.json"
    if not keys_path.is_file():
        print(f"Missing {keys_path}; run zephyr first.")
        return 1

    with keys_path.open(encoding="utf-8") as fh:
        stored = json.load(fh)
    keys = [str(k).strip() for k in (stored.get("keys") or []) if str(k).strip()]
    if not keys:
        print("No keys in duplicate_rollup_keys.json")
        return 1

    base_url = _resolve_weekly_jira_metadata_base(
        os.getenv("ZEPHYR_BASE_URL", "https://jira.navio.auto")
    )
    token = (os.getenv("ZEPHYR_JIRA_API_TOKEN") or os.getenv("ZEPHYR_API_TOKEN") or "").strip()
    headers = _jira_bug_metadata_auth_headers(
        build_headers("Authorization", "Bearer", token)
    ) or build_headers("Authorization", "Bearer", token)

    print(f"Fetching Jira metadata for {len(keys)} keys...")
    meta = _fetch_jira_bug_metadata(keys, base_url=base_url, auth_headers=headers)

    filled_exp = sum(1 for k in keys if (meta.get(k, {}).get("expected_result") or "").strip())
    filled_act = sum(1 for k in keys if (meta.get(k, {}).get("actual_result") or "").strip())
    print(f"Parsed expected_result: {filled_exp}/{len(keys)}")
    print(f"Parsed actual_result: {filled_act}/{len(keys)}")

    keys_path.write_text(
        json.dumps(
            {
                "keys": keys,
                "summaries": {
                    k: str((meta.get(k) or {}).get("summary") or "") for k in keys
                },
                "expected_results": {
                    k: str((meta.get(k) or {}).get("expected_result") or "") for k in keys
                },
                "actual_results": {
                    k: str((meta.get(k) or {}).get("actual_result") or "") for k in keys
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    cache_path, overrides_path = resolve_paths_for_rollup_dir(rollup_dir)
    duplicates = find_duplicate_candidates(
        keys,
        meta,
        embedding_cache=load_embedding_cache(cache_path),
        overrides=load_duplicate_overrides(overrides_path),
    )
    write_duplicate_candidates_debug(
        str(Path(rollup_dir) / "duplicate_candidates.json"),
        duplicates,
    )

    matches = sum(1 for v in duplicates.values() if v is not None)
    high_matches = sum(1 for v in duplicates.values() if v is not None and v.confidence == "high")
    medium_matches = sum(
        1 for v in duplicates.values() if v is not None and v.confidence == "medium"
    )
    min_conf = bugs_duplicate_publish_min_confidence()
    print(
        f"Duplicate candidates: {matches}/{len(keys)} "
        f"(high={high_matches}, medium={medium_matches}, publish_min={min_conf})"
    )
    for k in keys:
        c = duplicates.get(k)
        if c:
            extra = ""
            if c.expected_sim is not None:
                extra += f" exp={c.expected_sim:.2f}"
            if c.actual_sim is not None:
                extra += f" act={c.actual_sim:.2f}"
            print(
                f"  {k} -> {c.other_key} "
                f"({c.method}, {c.score:.3f}, {c.confidence}{extra})"
            )

    print("\nRe-run run_zephyr.ps1 to refresh bugs_index.html duplicate column.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

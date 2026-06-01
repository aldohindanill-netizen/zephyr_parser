#!/usr/bin/env python3
"""Publish existing HTML under reports_local/ to Confluence sandbox (no Zephyr fetch)."""

from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from repo_env import load_repo_env_for_scripts  # noqa: E402

from zephyr_weekly_report import (  # noqa: E402
    ConfluenceWeekParentCache,
    publish_reports_to_confluence,
    publish_reports_to_confluence_by_week,
    resolve_confluence_publish_roots,
    _load_confluence_publish_config,
    _parse_bool_env,
)


def _publish_paths_one_by_one(
    label: str,
    paths: list[str],
    publish_fn,
) -> int:
    """Publish each HTML separately so one Confluence 5xx does not abort the batch."""
    exit_code = 0
    print(f"{label} ({len(paths)} file(s)):")
    for path in paths:
        try:
            outcomes = publish_fn(path)
            for line in outcomes:
                print(f"- {line}")
        except Exception as exc:  # noqa: BLE001
            print(f"- failed: {os.path.basename(path)}: {exc}", file=sys.stderr)
            exit_code = 1
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--use-local-env",
        action="store_true",
        default=True,
        help="Load .env.local (default: true)",
    )
    parser.add_argument("--daily", action="store_true", help="Publish daily_readable HTML")
    parser.add_argument("--weekly", action="store_true", help="Publish weekly_readable HTML")
    parser.add_argument("--analytics", action="store_true", help="Publish weekly_analytics.html")
    parser.add_argument("--bugs", action="store_true", help="Publish build_log + bugs rollup HTML")
    parser.add_argument("--all", action="store_true", help="Publish all enabled by env flags")
    args = parser.parse_args()

    load_repo_env_for_scripts(use_local_env=args.use_local_env)
    cfg = _load_confluence_publish_config()
    if not cfg:
        print("Confluence not configured in .env", file=sys.stderr)
        return 1

    roots = resolve_confluence_publish_roots(cfg)
    week_parents = ConfluenceWeekParentCache(cfg, roots.root_parent)
    print(f"Root parent: {roots.root_parent}; bugs parent: {roots.bugs_parent}")

    do_daily = args.daily or (
        args.all and _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_PUBLISH_DAILY"))
    )
    do_weekly = args.weekly or (
        args.all and _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY"))
    )
    do_analytics = args.analytics or (
        args.all and _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY_ANALYTICS"))
    )
    do_bugs = args.bugs or (
        args.all and _parse_bool_env(os.getenv("ZEPHYR_CONFLUENCE_PUBLISH_BUGS"))
    )
    if not any((do_daily, do_weekly, do_analytics, do_bugs)):
        do_daily = do_weekly = do_analytics = do_bugs = True

    exit_code = 0
    daily_dir = os.getenv("ZEPHYR_DAILY_READABLE_DIR", "reports_local/daily_readable")
    weekly_dir = os.getenv("ZEPHYR_WEEKLY_READABLE_DIR", "reports_local/weekly_readable")
    analytics_dir = os.getenv("ZEPHYR_WEEKLY_ANALYTICS_DIR", "reports_local/weekly_analytics")
    build_dir = os.getenv("ZEPHYR_BUILD_LOG_REPORT_DIR", "reports_local/build_log_reports")
    rollup_dir = os.getenv("ZEPHYR_BUGS_ROLLUP_DIR", "reports_local/bugs_rollup")

    if do_daily:
        daily_html = sorted(glob.glob(os.path.join(daily_dir, "*.html")))
        if daily_html:
            code = _publish_paths_one_by_one(
                "Confluence daily",
                daily_html,
                lambda path: publish_reports_to_confluence_by_week(
                    [path],
                    cfg,
                    week_parents=week_parents,
                    fallback_parent=roots.root_parent,
                ),
            )
            exit_code = max(exit_code, code)

    if do_weekly:
        weekly_html = sorted(glob.glob(os.path.join(weekly_dir, "weekly_cycle_matrix*.html")))
        if weekly_html:
            code = _publish_paths_one_by_one(
                "Confluence weekly",
                weekly_html,
                lambda path: publish_reports_to_confluence_by_week(
                    [path],
                    cfg,
                    week_parents=week_parents,
                    fallback_parent=roots.root_parent,
                ),
            )
            exit_code = max(exit_code, code)

    if do_analytics:
        analytics_html = os.path.join(analytics_dir, "weekly_analytics.html")
        if os.path.isfile(analytics_html):
            code = _publish_paths_one_by_one(
                "Confluence weekly analytics",
                [analytics_html],
                lambda path: publish_reports_to_confluence([path], cfg),
            )
            exit_code = max(exit_code, code)

    if do_bugs:
        bug_html = sorted(glob.glob(os.path.join(build_dir, "*_build_log.html")))
        rollup_index = os.path.join(rollup_dir, "bugs_index.html")
        if os.path.isfile(rollup_index):
            bug_html.append(rollup_index)
        if bug_html:
            code = _publish_paths_one_by_one(
                "Confluence bugs",
                bug_html,
                lambda path: publish_reports_to_confluence(
                    [path],
                    cfg,
                    parent_page_id=roots.bugs_parent,
                ),
            )
            exit_code = max(exit_code, code)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

"""Publish universal report HTML to Confluence using the daily pipeline."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from zephyr_weekly_report import (
    ConfluenceWeekParentCache,
    _load_confluence_connection_config,
    publish_reports_to_confluence_by_week,
    resolve_confluence_publish_roots,
)

from universal_report.builder import build_universal_report_base_name, write_universal_reports
from universal_report.schema import normalize_draft


def output_dir() -> Path:
    raw = (os.getenv("ZEPHYR_UNIVERSAL_READABLE_DIR") or "").strip()
    if raw:
        return Path(os.path.expanduser(raw))
    return Path(__file__).resolve().parent.parent / "reports_local" / "universal_readable"


def publish_universal_report(draft: dict[str, Any]) -> dict[str, Any]:
    draft = normalize_draft(draft)
    cfg = _load_confluence_connection_config()
    if cfg is None:
        raise ValueError(
            "Confluence is not configured. Set in .env / .env.secrets: "
            "ZEPHYR_CONFLUENCE_BASE_URL, ZEPHYR_CONFLUENCE_SPACE_KEY, "
            "ZEPHYR_CONFLUENCE_API_TOKEN"
            " (and ZEPHYR_CONFLUENCE_USER when ZEPHYR_CONFLUENCE_AUTH_SCHEME=basic). "
            "Also set ZEPHYR_CONFLUENCE_PARENT_PAGE_ID in .env.local for sandbox. "
            "Restart run_universal_report.cmd after editing env files."
        )
    out_dir = output_dir()
    paths = write_universal_reports(draft, str(out_dir), formats={"html"})
    html_paths = [p for p in paths if p.endswith(".html")]
    if not html_paths:
        base = build_universal_report_base_name(draft)
        html_paths = [str(out_dir / f"{base}.html")]
    roots = resolve_confluence_publish_roots(cfg)
    week_parents = ConfluenceWeekParentCache(cfg, roots.root_parent)
    outcomes = publish_reports_to_confluence_by_week(
        html_paths,
        cfg,
        week_parents=week_parents,
        fallback_parent=roots.root_parent,
    )
    return {
        "paths": html_paths,
        "outcomes": outcomes,
    }

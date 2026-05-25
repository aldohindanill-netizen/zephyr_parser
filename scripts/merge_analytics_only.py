#!/usr/bin/env python3
"""Merge analytics-only code from _analytics_source_report.py into main-based report."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TARGET = ROOT / "zephyr_weekly_report.py"
SOURCE = ROOT / "_analytics_source_report.py"


def extract_block(text: str, start_marker: str, end_marker: str) -> str:
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    return text[start:end]


def main() -> None:
    target = TARGET.read_text(encoding="utf-8")
    source = SOURCE.read_text(encoding="utf-8")

    # 1) Insert analytics data/render functions after _weekly_cycle_matrix_data_all
    insert_block = extract_block(
        source,
        "def _weekly_cycle_matrix_data_rolling(",
        "def _weekly_output_path_for_week(",
    )
    anchor = "    return matrices\n\n\ndef _weekly_output_path_for_week"
    if anchor not in target:
        raise SystemExit("anchor for matrix insert not found")
    target = target.replace(
        anchor,
        "    return matrices\n\n\n" + insert_block + "\ndef _weekly_output_path_for_week",
        1,
    )

    # 2) Insert shared analytics section renderers before render_weekly_html_report
    if "def _render_analytics_sections_html(" not in target:
        render_block = extract_block(
            source,
            "def _render_analytics_sections_html(",
            "def render_weekly_html_report(",
        )
        anchor2 = "def render_weekly_html_report("
        if anchor2 not in target:
            raise SystemExit("anchor for render insert not found")
        target = target.replace(anchor2, render_block + anchor2, 1)

    # 3) ConfluencePublishConfig
    target = target.replace(
        "    publish_weekly: bool = False\n\n\n@dataclass\nclass StepTiming",
        "    publish_weekly: bool = False\n    publish_weekly_analytics: bool = False\n\n\n@dataclass\nclass StepTiming",
        1,
    )

    # 4) _load_confluence_publish_config
    target = target.replace(
        "    publish_weekly = _parse_bool_env(os.getenv(\"ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY\"))\n    update_existing",
        "    publish_weekly = _parse_bool_env(os.getenv(\"ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY\"))\n    publish_weekly_analytics = _parse_bool_env(\n        os.getenv(\"ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY_ANALYTICS\")\n    )\n    update_existing",
        1,
    )
    target = target.replace(
        "    if not (publish_daily or publish_weekly):",
        "    if not (publish_daily or publish_weekly or publish_weekly_analytics):",
        1,
    )
    target = target.replace(
        "        publish_weekly=publish_weekly,\n    )",
        "        publish_weekly=publish_weekly,\n        publish_weekly_analytics=publish_weekly_analytics,\n    )",
        1,
    )

    # 5) publish_reports_to_confluence — analytics page (no audit)
    analytics_publish = '''        base_name = os.path.basename(html_path)
        is_weekly_analytics = base_name == "weekly_analytics.html"
        is_weekly = base_name.startswith("weekly_cycle_matrix")
        if is_weekly_analytics:
            body_html = _normalize_html_for_confluence_storage(raw_html)
            body_html = _inject_confluence_anchor_macros(body_html)
            body_html = _convert_fragment_links_to_confluence(body_html)
            body_html = _replace_weekly_overall_cells_with_zephyr_macro(body_html)
            body_html = _replace_weekly_jira_key_spans_with_confluence_macro(body_html)
            body_html = _replace_legacy_weekly_table_macros_with_excerpt(body_html)
            analytics_title = (
                os.getenv("ZEPHYR_CONFLUENCE_WEEKLY_ANALYTICS_TITLE") or "Zephyr Weekly Analytics"
            ).strip()
            page_id, action = _confluence_upsert_storage_page(
                cfg, analytics_title, body_html
            )
            outcomes.append(f"{action}: {analytics_title} (page id {page_id})")
            continue
        if is_weekly:'''
    target = target.replace(
        "        is_weekly = os.path.basename(html_path).startswith(\"weekly_cycle_matrix\")\n        if is_weekly:",
        analytics_publish,
        1,
    )

    # 6) parse_args — weekly analytics CLI
    cli_block = extract_block(
        source,
        '    parser.add_argument(\n        "--export-weekly-analytics"',
        '    parser.add_argument(\n        "--regenerate-last-7-days"',
    )
    if '--export-weekly-analytics"' not in target:
        target = target.replace(
            '    parser.add_argument(\n        "--regenerate-last-7-days"',
            cli_block + '    parser.add_argument(\n        "--regenerate-last-7-days"',
            1,
        )

    # 7) render_weekly_html_report — slim analytics
    html_scenarios = extract_block(
        source,
        '    sections.append("<h2 id=\'scenarios\'><strong>3. Результаты тестирования</strong></h2>")\n\n    if _weekly_report_include_analytics_enabled():',
        '    sections.append("</body></html>")\n    return "\\n".join(sections)\n\n\ndef render_weekly_wiki_report',
    )
    target = re.sub(
        r'    sections\.append\("<h2 id=\'scenarios\'>.*?</h2>"\)\n\n    sections\.append\("<h3 id=\'overall-score\'>.*?</body></html>"\)\n    return "\\n"\.join\(sections\)\n\n\ndef render_weekly_wiki_report',
        html_scenarios + "\n\ndef render_weekly_wiki_report",
        target,
        count=1,
        flags=re.DOTALL,
    )

    # 8) render_weekly_wiki_report — slim analytics
    wiki_scenarios = extract_block(
        source,
        '    lines.append("{anchor:scenarios}")\n    lines.append("h2. *3. Результаты тестирования*")\n    lines.append("")\n\n    if _weekly_report_include_analytics_enabled():',
        '    return "\\n".join(lines)\n\n\ndef write_weekly_readable_reports',
    )
    target = re.sub(
        r'    lines\.append\("\{anchor:scenarios\}"\).*?return "\\n"\.join\(lines\)\n\n\ndef write_weekly_readable_reports',
        wiki_scenarios + "\n\ndef write_weekly_readable_reports",
        target,
        count=1,
        flags=re.DOTALL,
    )

    # 9) run_once — needs_report_data
    target = target.replace(
        "                or args.export_weekly_readable\n            )",
        "                or args.export_weekly_readable\n                or _export_weekly_analytics_enabled(args)\n            )",
        1,
    )

    # 10) run_once — analytics export block (strip za.audit lines)
    analytics_run = extract_block(
        source,
        "            weekly_analytics_paths: list[str] = []\n            if _export_weekly_analytics_enabled(args):",
        "            if build_log_html_publish_paths and not args.export_weekly_readable:",
    )
    analytics_run = re.sub(r"\n\s*za\.audit_[^\n]+\n", "\n", analytics_run)
    if "weekly_analytics_paths" not in target:
        target = target.replace(
            "            if build_log_html_publish_paths and not args.export_weekly_readable:",
            analytics_run + "            if build_log_html_publish_paths and not args.export_weekly_readable:",
            1,
        )

    # 11) publish_confluence_weekly_analytics
    conf_analytics = '''            publish_confluence_weekly_analytics = bool(
                confluence_cfg and confluence_cfg.publish_weekly_analytics
            )
'''
    if "publish_confluence_weekly_analytics" not in target:
        target = target.replace(
            "            publish_confluence_weekly = bool(confluence_cfg and confluence_cfg.publish_weekly)\n",
            "            publish_confluence_weekly = bool(confluence_cfg and confluence_cfg.publish_weekly)\n"
            + conf_analytics,
            1,
        )

    conf_publish_block = extract_block(
        source,
        "            if confluence_cfg and publish_confluence_weekly_analytics:",
        "            print(f\"Saved summary CSV:",
    )
    conf_publish_block = re.sub(r"\n\s*za\.audit_[^\n]+\n", "\n", conf_publish_block)
    if "publish weekly analytics to Confluence" not in target:
        target = target.replace(
            '            print(f"Saved summary CSV:',
            conf_publish_block + '            print(f"Saved summary CSV:',
            1,
        )

    # 12) print analytics saved
    if "_export_weekly_analytics_enabled(args)" not in target.split("print(f\"Saved weekly")[-1][:500]:
        target = target.replace(
            '            if args.export_weekly_readable:\n                print(f"Saved weekly readable reports:',
            '            if args.export_weekly_readable:\n                print(f"Saved weekly readable reports:',
            1,
        )
    if "Saved weekly analytics reports" not in target:
        target = target.replace(
            '            if args.export_weekly_readable:\n                print(f"Saved weekly readable reports: {args.weekly_readable_dir}")\n                print(f"Weekly readable files updated: {len(weekly_readable_paths)}")\n            print(f"Processed folders:',
            '            if args.export_weekly_readable:\n                print(f"Saved weekly readable reports: {args.weekly_readable_dir}")\n                print(f"Weekly readable files updated: {len(weekly_readable_paths)}")\n            if _export_weekly_analytics_enabled(args):\n                print(f"Saved weekly analytics reports: {args.weekly_analytics_dir}")\n                print(f"Weekly analytics files updated: {len(weekly_analytics_paths)}")\n            print(f"Processed folders:',
            1,
        )

    TARGET.write_text(target, encoding="utf-8")
    print("merged analytics into", TARGET)


if __name__ == "__main__":
    main()

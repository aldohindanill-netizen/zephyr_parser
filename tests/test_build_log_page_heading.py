from __future__ import annotations

import unittest
from unittest import mock
from unittest.mock import patch

from zephyr_weekly_report import (
    ConfluencePublishConfig,
    _confluence_publish_title,
    _jira_issue_build_log_legacy_confluence_title,
    _jira_issue_build_log_page_heading,
    _jira_issue_build_log_summary_from_heading,
    render_jira_issue_build_log_html,
    render_jira_issue_build_log_wiki,
)


class BuildLogPageHeadingTests(unittest.TestCase):
    def test_heading_key_before_summary(self) -> None:
        self.assertEqual(
            _jira_issue_build_log_page_heading("CSD-42", "Пешеход спереди"),
            "CSD-42 — Пешеход спереди",
        )

    def test_heading_empty_summary_returns_key(self) -> None:
        self.assertEqual(_jira_issue_build_log_page_heading("CSD-42", ""), "CSD-42")
        self.assertEqual(_jira_issue_build_log_page_heading("CSD-42", "   "), "CSD-42")

    def test_heading_empty_key_returns_summary(self) -> None:
        self.assertEqual(_jira_issue_build_log_page_heading("", "Only summary"), "Only summary")

    def test_legacy_confluence_title_summary_first(self) -> None:
        self.assertEqual(
            _jira_issue_build_log_legacy_confluence_title("CSD-42", "Пешеход спереди"),
            "Пешеход спереди (CSD-42)",
        )
        self.assertIsNone(_jira_issue_build_log_legacy_confluence_title("CSD-42", ""))

    def test_summary_from_heading(self) -> None:
        self.assertEqual(
            _jira_issue_build_log_summary_from_heading(
                "CSD-42 — Пешеход спереди", "CSD-42"
            ),
            "Пешеход спереди",
        )
        self.assertEqual(
            _jira_issue_build_log_summary_from_heading("CSD-42", "CSD-42"),
            "",
        )

    def test_html_title_and_h1_use_key_first(self) -> None:
        body = render_jira_issue_build_log_html("CSD-42", "Пешеход спереди", [])
        self.assertIn("<title>CSD-42 — Пешеход спереди</title>", body)
        self.assertIn("<h1>CSD-42 — Пешеход спереди</h1>", body)
        self.assertNotIn("Пешеход спереди (CSD-42)", body)

    def test_html_empty_summary_key_only(self) -> None:
        body = render_jira_issue_build_log_html("CSD-42", "", [])
        self.assertIn("<title>CSD-42</title>", body)
        self.assertIn("<h1>CSD-42</h1>", body)

    def test_html_escapes_summary_in_title(self) -> None:
        body = render_jira_issue_build_log_html("CSD-42", "<script>x</script>", [])
        self.assertIn("<title>CSD-42 — &lt;script&gt;x&lt;/script&gt;</title>", body)
        self.assertNotIn("<script>", body)

    def test_wiki_heading_matches_html(self) -> None:
        wiki = render_jira_issue_build_log_wiki("CSD-42", "Пешеход спереди", [])
        self.assertEqual(wiki.splitlines()[0], "h1. CSD-42 — Пешеход спереди")

    def test_build_log_publish_prefers_old_style_legacy_title(self) -> None:
        cfg = ConfluencePublishConfig(
            base_url="https://wiki.example",
            space_key="QA",
            api_prefix="/wiki/rest/api",
            parent_page_id="1",
            user="u",
            api_token="t",
            title_prefix="",
        )
        html = render_jira_issue_build_log_html("CSD-42", "Пешеход спереди", [])
        with patch(
            "zephyr_weekly_report._confluence_resolve_publish_titles",
            return_value=("CSD-42 — Пешеход спереди", "CSD-42 build log"),
        ), patch(
            "zephyr_weekly_report._normalize_html_for_confluence_storage",
            return_value="<p/>",
        ), patch(
            "zephyr_weekly_report._inject_confluence_anchor_macros",
            side_effect=lambda x: x,
        ), patch(
            "zephyr_weekly_report._convert_fragment_links_to_confluence",
            side_effect=lambda x: x,
        ), patch(
            "zephyr_weekly_report._confluence_upsert_storage_page",
            return_value=("99", "updated"),
        ) as upsert, patch(
            "zephyr_weekly_report._maybe_audit_publish_confluence",
        ), patch("builtins.open", mock.mock_open(read_data=html)):
            from zephyr_weekly_report import _publish_single_html_to_confluence

            _publish_single_html_to_confluence(
                r"C:\reports\build_log_reports\CSD-42_build_log.html",
                cfg,
                parent_page_id="parent-1",
            )
        self.assertEqual(
            upsert.call_args.kwargs.get("legacy_title"),
            _confluence_publish_title("Пешеход спереди (CSD-42)", cfg),
        )

    def test_build_log_publish_legacy_title_respects_confluence_prefix(self) -> None:
        cfg = ConfluencePublishConfig(
            base_url="https://wiki.example",
            space_key="QA",
            api_prefix="/wiki/rest/api",
            parent_page_id="1",
            user="u",
            api_token="t",
            title_prefix="[DEV]",
        )
        html = render_jira_issue_build_log_html("CSD-42", "Пешеход спереди", [])
        prefixed_primary = _confluence_publish_title("CSD-42 — Пешеход спереди", cfg)
        with patch(
            "zephyr_weekly_report._confluence_resolve_publish_titles",
            return_value=(prefixed_primary, "CSD-42 build log"),
        ), patch(
            "zephyr_weekly_report._normalize_html_for_confluence_storage",
            return_value="<p/>",
        ), patch(
            "zephyr_weekly_report._inject_confluence_anchor_macros",
            side_effect=lambda x: x,
        ), patch(
            "zephyr_weekly_report._convert_fragment_links_to_confluence",
            side_effect=lambda x: x,
        ), patch(
            "zephyr_weekly_report._confluence_upsert_storage_page",
            return_value=("99", "updated"),
        ) as upsert, patch(
            "zephyr_weekly_report._maybe_audit_publish_confluence",
        ), patch("builtins.open", mock.mock_open(read_data=html)):
            from zephyr_weekly_report import _publish_single_html_to_confluence

            _publish_single_html_to_confluence(
                r"C:\reports\build_log_reports\CSD-42_build_log.html",
                cfg,
                parent_page_id="parent-1",
            )
        self.assertEqual(
            upsert.call_args.kwargs.get("legacy_title"),
            _confluence_publish_title("Пешеход спереди (CSD-42)", cfg),
        )
        self.assertEqual(upsert.call_args.args[1], prefixed_primary)


if __name__ == "__main__":
    unittest.main()

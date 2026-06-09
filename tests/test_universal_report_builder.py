"""Tests for universal report draft schema and builder."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from universal_report.builder import (
    _safe_http_url,
    build_universal_html,
    build_universal_report_base_name,
    build_universal_wiki,
    draft_cycles_to_render_dict,
    draft_to_preamble_html,
    write_universal_reports,
)
from universal_report.schema import new_draft, normalize_draft
from universal_report.zephyr_import import _filter_cycles_by_date


class UniversalReportBuilderTests(unittest.TestCase):
    def test_new_draft_has_defaults(self) -> None:
        draft = new_draft(build_name="test-build")
        self.assertEqual(draft["sections_1_2"]["build_name"], "test-build")
        self.assertEqual(draft["section_3_mode"], "manual")
        self.assertIn("Полигон ODE", draft["sections_1_2"]["infrastructure"])

    def test_build_html_contains_sections(self) -> None:
        draft = normalize_draft(new_draft(build_name="demo-2026-06-09"))
        draft["cycles"] = [
            {
                "cycle_id": "c1",
                "cycle_key": "TR-1",
                "cycle_name": "Scenario A",
                "cycle_objective": "",
                "cases": [
                    {
                        "test_case_key": "TC-1",
                        "test_case_name": "Test one",
                        "result": "Pass",
                        "objective": "Criterion A",
                        "comment": "ok",
                        "tasks": "PROJ-1",
                    }
                ],
            }
        ]
        html_body = build_universal_html(draft)
        self.assertIn("1. Объект тестирования", html_body)
        self.assertIn("2. Условия окружения", html_body)
        self.assertIn("3. Результаты тестирования", html_body)
        self.assertIn("4. Заключение", html_body)
        self.assertIn("demo-2026-06-09", html_body)
        self.assertIn("Test one", html_body)
        preamble = draft_to_preamble_html(draft)
        self.assertIn("demo-2026-06-09", preamble)

    def test_duplicate_cycle_ids_are_preserved(self) -> None:
        draft = normalize_draft(new_draft())
        draft["cycles"] = [
            {
                "cycle_id": "same",
                "cycle_key": "TR-1",
                "cycle_name": "A",
                "cases": [{"test_case_name": "Case A", "result": "Pass"}],
            },
            {
                "cycle_id": "same",
                "cycle_key": "TR-2",
                "cycle_name": "B",
                "cases": [{"test_case_name": "Case B", "result": "Fail"}],
            },
        ]
        cycles = draft_cycles_to_render_dict(draft)
        self.assertEqual(len(cycles), 2)
        names = {cycle["cycle_name"] for cycle in cycles.values()}
        self.assertEqual(names, {"A", "B"})

    def test_cycles_adapter(self) -> None:
        draft = normalize_draft(new_draft())
        draft["cycles"] = [
            {
                "cycle_id": "run-1",
                "cycle_key": "TR-9",
                "cycle_name": "Cycle",
                "cases": [{"test_case_name": "Case", "result": "Fail"}],
            }
        ]
        cycles = draft_cycles_to_render_dict(draft)
        self.assertIn("run-1", cycles)
        self.assertEqual(cycles["run-1"]["cases"]["Case"]["result"], "Fail")

    def test_base_name_has_date(self) -> None:
        draft = normalize_draft(new_draft(build_name="my build", report_date="2026-06-09"))
        name = build_universal_report_base_name(draft)
        self.assertTrue(name.startswith("universal-"))
        self.assertIn("2026-06-09", name)

    def test_preamble_emits_toc_anchor_ids(self) -> None:
        draft = normalize_draft(new_draft(build_name="anchor-test"))
        html_body = build_universal_html(draft)
        wiki_body = build_universal_wiki(draft)
        self.assertIn("id='sec-object'", html_body)
        self.assertIn("id='sec-environment'", html_body)
        self.assertIn("{anchor:sec_object}", wiki_body)
        self.assertIn("{anchor:sec_environment}", wiki_body)

    def test_unsafe_document_link_is_omitted(self) -> None:
        draft = normalize_draft(new_draft())
        draft["sections_1_2"]["document_links"] = [
            {"label": "Bad", "url": "javascript:alert(1)", "note": ""},
            {"label": "Good", "url": "https://example.com/doc", "note": ""},
        ]
        preamble = draft_to_preamble_html(draft)
        self.assertNotIn("javascript:", preamble)
        self.assertIn("https://example.com/doc", preamble)

    def test_safe_http_url(self) -> None:
        self.assertIsNone(_safe_http_url("javascript:alert(1)"))
        self.assertEqual(_safe_http_url("https://example.com/x"), "https://example.com/x")

    def test_normalize_draft_fills_meta(self) -> None:
        draft = normalize_draft({"meta": {"build_name": "only-build"}})
        self.assertEqual(draft["meta"]["folder_name"], "only-build")
        self.assertTrue(str(draft["meta"]["folder_id"]).startswith("universal-"))

    def test_malicious_report_date_is_sanitized_in_filename(self) -> None:
        draft = normalize_draft(
            {
                "meta": {
                    "build_name": "safe",
                    "report_date": "../../etc/passwd",
                }
            }
        )
        name = build_universal_report_base_name(draft)
        self.assertNotIn("..", name)
        self.assertNotIn("/", name)
        self.assertRegex(draft["meta"]["report_date"], r"^\d{4}-\d{2}-\d{2}$")

    def test_filter_cycles_by_date(self) -> None:
        cycles = {
            "c1": {
                "cases": {
                    "a": {"execution_date": "2026-06-01"},
                    "b": {"execution_date": "2026-06-15"},
                }
            }
        }
        from datetime import date

        filtered = _filter_cycles_by_date(cycles, date(2026, 6, 10), date(2026, 6, 20))
        self.assertEqual(list(filtered["c1"]["cases"].keys()), ["b"])

    def test_write_universal_reports_creates_html_and_wiki(self) -> None:
        draft = normalize_draft(new_draft(build_name="write-test", report_date="2026-06-09"))
        with tempfile.TemporaryDirectory() as tmp:
            paths = write_universal_reports(draft, tmp, formats={"html", "wiki"})
            html_paths = [p for p in paths if p.endswith(".html")]
            wiki_paths = [p for p in paths if p.endswith(".confluence.txt")]
            self.assertEqual(len(html_paths), 1)
            self.assertEqual(len(wiki_paths), 1)
            html_body = Path(html_paths[0]).read_text(encoding="utf-8")
            wiki_body = Path(wiki_paths[0]).read_text(encoding="utf-8")
            self.assertIn("write-test", html_body)
            self.assertIn("4. Заключение", wiki_body)


class UniversalConfluenceConfigTests(unittest.TestCase):
    def test_connection_config_ignores_publish_flags(self) -> None:
        from zephyr_weekly_report import (
            _load_confluence_connection_config,
            _load_confluence_publish_config,
        )

        env = {
            "ZEPHYR_CONFLUENCE_BASE_URL": "https://example.atlassian.net/wiki",
            "ZEPHYR_CONFLUENCE_SPACE_KEY": "QA",
            "ZEPHYR_CONFLUENCE_API_TOKEN": "token",
            "ZEPHYR_CONFLUENCE_AUTH_SCHEME": "bearer",
            "ZEPHYR_CONFLUENCE_PUBLISH_DAILY": "false",
            "ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY": "false",
            "ZEPHYR_CONFLUENCE_PUBLISH_WEEKLY_ANALYTICS": "false",
            "ZEPHYR_CONFLUENCE_PUBLISH_BUGS": "false",
        }
        with patch.dict(os.environ, env, clear=False):
            self.assertIsNotNone(_load_confluence_connection_config())
            self.assertIsNone(_load_confluence_publish_config())


if __name__ == "__main__":
    unittest.main()

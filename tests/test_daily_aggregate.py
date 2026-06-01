"""Tests for daily readable aggregation from case_steps rows."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from zephyr_weekly_report import (  # noqa: E402
    aggregate_readable_daily_reports_from_steps,
    aggregate_readable_daily_reports_legacy,
)


def _sample_case_step_row(
    *,
    test_case_key: str = "",
    test_case_id: str = "tc-1",
    step_comment: str = "step note",
    step_status: str = "Pass",
    test_result_status: str = "Fail",
    task_links: str = "CSD-100",
    step_index: str = "1",
) -> list[str]:
    row = [""] * 25
    row[0] = "fid"
    row[1] = "folder A"
    row[3] = "QA-CYC"
    row[4] = "Cycle 1"
    row[5] = "run-1"
    row[6] = test_case_id
    row[7] = test_case_key
    row[8] = "Case name"
    row[11] = step_index
    row[14] = step_comment
    row[16] = step_status
    row[19] = test_result_status
    row[22] = task_links
    return row


class DailyAggregateTests(unittest.TestCase):
    def test_aggregate_populates_status_comment_tasks(self) -> None:
        rows = [_sample_case_step_row(test_case_key="TC-1")]
        cycles_cases = [
            ["fid", "folder A", "run-1", "QA-CYC", "Cycle 1", "", "", "", "", "", "Blocked"],
        ]
        reports = aggregate_readable_daily_reports_from_steps(rows, cycles_cases)
        case = reports[("fid", "folder A")]["cycles"]["run-1"]["cases"]["TC-1"]
        self.assertEqual("Fail", case["result"])
        self.assertEqual("step note", case["comment"])
        self.assertIn("CSD-100", case["tasks"])

    def test_aggregate_accepts_test_case_id_without_key(self) -> None:
        rows = [_sample_case_step_row(test_case_key="", test_case_id="555")]
        reports = aggregate_readable_daily_reports_from_steps(rows, [])
        cases = reports[("fid", "folder A")]["cycles"]["run-1"]["cases"]
        self.assertIn("555", cases)
        self.assertEqual("step note", cases["555"]["comment"])

    def test_comment_uses_merged_log_parts_when_step_comment_empty(self) -> None:
        row = _sample_case_step_row(test_case_key="TC-2", step_comment="")
        row[14] = ""
        reports = aggregate_readable_daily_reports_from_steps([row], [])
        # Simulate second row merge adding comment via logs_comment_parts path
        row2 = _sample_case_step_row(test_case_key="TC-2", step_comment="from result")
        reports = aggregate_readable_daily_reports_from_steps([row, row2], [])
        case = reports[("fid", "folder A")]["cycles"]["run-1"]["cases"]["TC-2"]
        self.assertEqual("from result", case["comment"])

    def test_legacy_has_empty_comment_and_tasks(self) -> None:
        cycles_cases = [
            [
                "fid",
                "folder A",
                "run-1",
                "QA-CYC",
                "Cycle 1",
                "",
                "",
                "",
                "TC-1",
                "Case",
                "Pass",
            ],
        ]
        reports = aggregate_readable_daily_reports_legacy(cycles_cases)
        case = reports[("fid", "folder A")]["cycles"]["run-1"]["cases"]["TC-1"]
        self.assertEqual("Pass", case["result"])
        self.assertEqual("", case["comment"])
        self.assertEqual("", case["tasks"])


if __name__ == "__main__":
    unittest.main()

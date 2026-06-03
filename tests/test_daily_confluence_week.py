"""Tests for daily report Confluence week folder routing."""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from zephyr_weekly_report import (  # noqa: E402
    _build_daily_report_base_name,
    _confluence_week_start_from_publish_path,
    _release_week_start,
    _test_day_from_folder_day,
)


class DailyConfluenceWeekTests(unittest.TestCase):
    """Класс «DailyConfluenceWeekTests»."""
    def test_week_start_from_daily_html_matches_weekly_logic(self) -> None:
        """Вспомогательная функция: test week start from daily html matches weekly logic."""
        report_day = date(2026, 5, 16)  # Saturday folder label
        expected = _release_week_start(_test_day_from_folder_day(report_day))
        path = "reports/daily_readable/nightly-dev-2026-05-16_2026-05-16_10545.html"
        got = _confluence_week_start_from_publish_path(path)
        self.assertEqual(expected, got)

    def test_week_start_unknown_date_returns_none(self) -> None:
        """Вспомогательная функция: test week start unknown date returns none."""
        path = "reports/daily_readable/nightly-dev-folder_unknown-date_99.html"
        self.assertIsNone(_confluence_week_start_from_publish_path(path))

    def test_build_daily_report_base_name_uses_folder_report_day(self) -> None:
        """Вспомогательная функция: test build daily report base name uses folder report day."""
        folder_name = "2026.05.16"
        folder_id = "10545"
        cycles: dict = {}
        base = _build_daily_report_base_name(folder_id, folder_name, cycles)
        self.assertIn("_2026-05-16_", base)
        self.assertNotIn("unknown-date", base)

    def test_build_daily_report_base_name_iso_date_in_filename(self) -> None:
        """Вспомогательная функция: test build daily report base name iso date in filename."""
        folder_name = "2026.05.20"
        base = _build_daily_report_base_name("99", folder_name, {})
        self.assertRegex(base, r"_\d{4}-\d{2}-\d{2}_")


if __name__ == "__main__":
    unittest.main()

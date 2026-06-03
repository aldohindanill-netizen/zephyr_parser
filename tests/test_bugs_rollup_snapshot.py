from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from zephyr_weekly_report import (
    _append_jira_defect_keys,
    _bootstrap_snapshot_base_if_empty,
    _bootstrap_snapshot_from_disk,
    _compute_weekly_defect_analytics,
    _defect_scenario_names_list,
    _display_build_column_labels,
    _empty_defect_analytics,
    _issue_key_from_build_log_filename,
    _load_bugs_rollup_snapshot,
    _merge_defect_analytics,
    _merge_column_labels_ordered,
    _build_scenario_group_catalog,
    _defect_scenario_group_names_list,
    _parse_zephyr_issuelink_test_case_names,
    _refresh_bugs_rollup_all_time_snapshot,
    _save_bugs_rollup_snapshot,
    _sort_build_column_labels_chronologically,
    _bugs_rollup_last_weeks_count,
    _bugs_rollup_section_last_weeks_title,
)


class BugsRollupSnapshotTests(unittest.TestCase):
    def test_merge_defect_analytics_unions_keys_and_uses_max_per_cell(self) -> None:
        base = _empty_defect_analytics()
        base["keys_ordered"] = ["CSD-1"]
        base["matrix"] = {"CSD-1": {"build-a": 2}}
        base["bug_total_cases"] = {"CSD-1": 2}
        base["bug_builds_count"] = {"CSD-1": 1}

        incoming = _empty_defect_analytics()
        incoming["keys_ordered"] = ["CSD-2"]
        incoming["matrix"] = {
            "CSD-1": {"build-b": 3},
            "CSD-2": {"build-b": 1},
        }
        incoming["bug_total_cases"] = {"CSD-1": 3, "CSD-2": 1}
        incoming["bug_builds_count"] = {"CSD-1": 1, "CSD-2": 1}

        merged, labels = _merge_defect_analytics(
            base, incoming, ["build-a"], ["build-b"]
        )
        self.assertEqual(labels, ["build-a", "build-b"])
        self.assertEqual(set(merged["keys_ordered"]), {"CSD-1", "CSD-2"})
        self.assertEqual(merged["matrix"]["CSD-1"]["build-a"], 2)
        self.assertEqual(merged["matrix"]["CSD-1"]["build-b"], 3)
        self.assertEqual(merged["matrix"]["CSD-2"]["build-b"], 1)

    def test_empty_snapshot_plus_incoming_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = os.path.join(tmp, "bugs_rollup")
            os.makedirs(out_dir)
            os.environ["ZEPHYR_BUGS_ROLLUP_SNAPSHOT"] = os.path.join(
                out_dir, "defect_analytics_snapshot.json"
            )
            os.environ["ZEPHYR_BUGS_ROLLUP_SNAPSHOT_BOOTSTRAP_BUILD_LOGS"] = "false"
            try:
                incoming = _empty_defect_analytics()
                incoming["keys_ordered"] = ["CSD-9"]
                incoming["matrix"] = {"CSD-9": {"nightly": 1}}
                incoming["bug_total_cases"] = {"CSD-9": 1}
                incoming["bug_builds_count"] = {"CSD-9": 1}

                merged, labels, weeks = _refresh_bugs_rollup_all_time_snapshot(
                    out_dir,
                    incoming,
                    ["nightly"],
                    [date(2026, 5, 26)],
                )
                self.assertIn("CSD-9", merged["keys_ordered"])
                self.assertEqual(labels, ["nightly"])
                self.assertEqual(weeks, [date(2026, 5, 26)])

                stored = _load_bugs_rollup_snapshot(
                    os.environ["ZEPHYR_BUGS_ROLLUP_SNAPSHOT"]
                )
                self.assertIn("CSD-9", stored["analytics"]["keys_ordered"])
            finally:
                os.environ.pop("ZEPHYR_BUGS_ROLLUP_SNAPSHOT", None)
                os.environ.pop("ZEPHYR_BUGS_ROLLUP_SNAPSHOT_BOOTSTRAP_BUILD_LOGS", None)

    def test_bootstrap_from_build_log_filenames(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            build_dir = Path(tmp) / "build_log"
            build_dir.mkdir()
            (build_dir / "CSD-100_build_log.html").write_text("<html></html>", encoding="utf-8")
            (build_dir / "CSD-200_build_log.html").write_text("<html></html>", encoding="utf-8")
            rollup_dir = Path(tmp) / "rollup"
            rollup_dir.mkdir()
            keys_path = rollup_dir / "duplicate_rollup_keys.json"
            keys_path.write_text(
                json.dumps({"keys": ["CSD-300"]}),
                encoding="utf-8",
            )

            analytics, labels, weeks = _bootstrap_snapshot_from_disk(
                str(build_dir), str(rollup_dir)
            )
            self.assertEqual(
                set(analytics["keys_ordered"]),
                {"CSD-100", "CSD-200", "CSD-300"},
            )
            self.assertEqual(labels, [])
            self.assertEqual(weeks, [])

    def test_issue_key_from_build_log_filename(self) -> None:
        self.assertEqual(
            _issue_key_from_build_log_filename("NAVIO-42_build_log.html"),
            "NAVIO-42",
        )
        self.assertIsNone(_issue_key_from_build_log_filename("readme.html"))

    def test_append_jira_defect_keys_dedupes(self) -> None:
        keys: list[str] = []
        seen: set[str] = set()
        _append_jira_defect_keys(keys, seen, ["CSD-1", "CSD-1", "bad", "CSD-2"])
        self.assertEqual(keys, ["CSD-1", "CSD-2"])

    def test_bootstrap_snapshot_base_if_empty_noop_when_keys_present(self) -> None:
        base = _empty_defect_analytics()
        base["keys_ordered"] = ["CSD-1"]
        out_analytics, out_labels, out_weeks = _bootstrap_snapshot_base_if_empty(
            base, ["build-a"], [], "/tmp/unused"
        )
        self.assertEqual(out_analytics["keys_ordered"], ["CSD-1"])
        self.assertEqual(out_labels, ["build-a"])
        self.assertEqual(out_weeks, [])

    def test_merge_column_labels_sorted_after_backfill_style_merge(self) -> None:
        labels = _merge_column_labels_ordered(
            ["nightly-dev-2026.06.10"],
            ["nightly-dev-2026.06.01"],
        )
        self.assertEqual(
            labels,
            ["nightly-dev-2026.06.01", "nightly-dev-2026.06.10"],
        )

    def test_display_build_column_labels_keeps_most_recent(self) -> None:
        labels = _sort_build_column_labels_chronologically(
            [f"nightly-dev-2026.01.{day:02d}" for day in range(1, 6)]
        )
        trimmed = _display_build_column_labels(labels, 3)
        self.assertEqual(
            trimmed,
            [
                "nightly-dev-2026.01.03",
                "nightly-dev-2026.01.04",
                "nightly-dev-2026.01.05",
            ],
        )

    def test_refresh_snapshot_preserves_keys_from_prior_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = os.path.join(tmp, "bugs_rollup")
            os.makedirs(out_dir)
            os.environ["ZEPHYR_BUGS_ROLLUP_SNAPSHOT"] = os.path.join(
                out_dir, "defect_analytics_snapshot.json"
            )
            os.environ["ZEPHYR_BUGS_ROLLUP_SNAPSHOT_BOOTSTRAP_BUILD_LOGS"] = "false"
            try:
                first = _empty_defect_analytics()
                first["keys_ordered"] = ["CSD-OLD"]
                first["matrix"] = {"CSD-OLD": {"nightly-dev-2026.01.01": 2}}
                first["bug_total_cases"] = {"CSD-OLD": 2}
                first["bug_builds_count"] = {"CSD-OLD": 1}

                _refresh_bugs_rollup_all_time_snapshot(
                    out_dir,
                    first,
                    ["nightly-dev-2026.01.01"],
                    [date(2026, 1, 1)],
                )

                second = _empty_defect_analytics()
                second["keys_ordered"] = ["CSD-NEW"]
                second["matrix"] = {"CSD-NEW": {"nightly-dev-2026.06.01": 1}}
                second["bug_total_cases"] = {"CSD-NEW": 1}
                second["bug_builds_count"] = {"CSD-NEW": 1}

                merged, labels, _weeks = _refresh_bugs_rollup_all_time_snapshot(
                    out_dir,
                    second,
                    ["nightly-dev-2026.06.01"],
                    [date(2026, 6, 1)],
                )
                self.assertIn("CSD-OLD", merged["keys_ordered"])
                self.assertIn("CSD-NEW", merged["keys_ordered"])
                self.assertEqual(
                    merged["matrix"]["CSD-OLD"]["nightly-dev-2026.01.01"],
                    2,
                )
                self.assertEqual(labels[0], "nightly-dev-2026.01.01")
                self.assertEqual(labels[-1], "nightly-dev-2026.06.01")
            finally:
                os.environ.pop("ZEPHYR_BUGS_ROLLUP_SNAPSHOT", None)
                os.environ.pop("ZEPHYR_BUGS_ROLLUP_SNAPSHOT_BOOTSTRAP_BUILD_LOGS", None)

    def test_refresh_snapshot_max_merge_keeps_higher_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = os.path.join(tmp, "bugs_rollup")
            os.makedirs(out_dir)
            os.environ["ZEPHYR_BUGS_ROLLUP_SNAPSHOT"] = os.path.join(
                out_dir, "defect_analytics_snapshot.json"
            )
            os.environ["ZEPHYR_BUGS_ROLLUP_SNAPSHOT_BOOTSTRAP_BUILD_LOGS"] = "false"
            try:
                label = "nightly-dev-2026.03.01"
                high = _empty_defect_analytics()
                high["keys_ordered"] = ["CSD-1"]
                high["matrix"] = {"CSD-1": {label: 5}}
                high["bug_total_cases"] = {"CSD-1": 5}
                high["bug_builds_count"] = {"CSD-1": 1}
                _refresh_bugs_rollup_all_time_snapshot(
                    out_dir, high, [label], [date(2026, 3, 1)]
                )

                low = _empty_defect_analytics()
                low["keys_ordered"] = ["CSD-1"]
                low["matrix"] = {"CSD-1": {label: 2}}
                low["bug_total_cases"] = {"CSD-1": 2}
                low["bug_builds_count"] = {"CSD-1": 1}
                merged, _labels, _weeks = _refresh_bugs_rollup_all_time_snapshot(
                    out_dir, low, [label], [date(2026, 3, 1)]
                )
                self.assertEqual(merged["matrix"]["CSD-1"][label], 5)
                self.assertEqual(merged["bug_total_cases"]["CSD-1"], 5)
            finally:
                os.environ.pop("ZEPHYR_BUGS_ROLLUP_SNAPSHOT", None)
                os.environ.pop("ZEPHYR_BUGS_ROLLUP_SNAPSHOT_BOOTSTRAP_BUILD_LOGS", None)

    def test_compute_weekly_defect_analytics_collects_linked_scenarios(self) -> None:
        day = date(2026, 5, 26)
        cycles = {
            "run-1": {
                "cases": {
                    "QA-C1": {
                        "test_case_name": "Остановка перед конусами",
                        "tasks": "CSD-100",
                    }
                }
            }
        }
        analytics = _compute_weekly_defect_analytics(
            {day: [cycles]},
            [day],
            ["nightly-dev-2026.05.26"],
        )
        self.assertEqual(
            analytics["bug_linked_scenarios"]["CSD-100"],
            ["Остановка перед конусами"],
        )

    def test_merge_defect_analytics_unions_linked_scenarios(self) -> None:
        base = _empty_defect_analytics()
        base["keys_ordered"] = ["CSD-1"]
        base["bug_linked_scenarios"] = {"CSD-1": ["Сценарий A"]}

        incoming = _empty_defect_analytics()
        incoming["keys_ordered"] = ["CSD-1"]
        incoming["bug_linked_scenarios"] = {"CSD-1": ["Сценарий B", "сценарий a"]}

        merged, _labels = _merge_defect_analytics(base, incoming, [], [])
        self.assertEqual(
            merged["bug_linked_scenarios"]["CSD-1"],
            ["Сценарий A", "Сценарий B"],
        )

    def test_parse_zephyr_issuelink_extracts_test_case_names(self) -> None:
        payload = {
            "testCases": [
                {"testCase": {"key": "QA-T1", "name": "Объезд ремонтных работ"}},
                {"name": "Остановка перед конусами", "testCaseId": 99},
            ]
        }
        names = _parse_zephyr_issuelink_test_case_names(payload)
        self.assertEqual(
            names,
            ["Объезд ремонтных работ", "Остановка перед конусами"],
        )

    def test_defect_scenario_names_panel_then_zephyr_without_dup(self) -> None:
        meta = {
            "CSD-1": {"traceability_scenarios": "Сценарий A; Сценарий B"},
        }
        analytics = _empty_defect_analytics()
        analytics["bug_linked_scenarios"] = {
            "CSD-1": ["Сценарий B", "Сценарий из прогона"],
        }
        names = _defect_scenario_names_list("CSD-1", meta, analytics)
        self.assertEqual(
            names,
            ["Сценарий A", "Сценарий B", "Сценарий из прогона"],
        )

    def test_build_scenario_group_catalog(self) -> None:
        report_data = {
            ("f1", "folder"): {
                "cycles": {
                    "c1": {
                        "cycle_name": "1.1 Стационарный автомобиль без объезда",
                        "cycle_key": "QA-C1",
                    },
                    "c2": {
                        "cycle_name": "1.2 Стационарный автомобиль объезд слева",
                        "cycle_key": "QA-C2",
                    },
                    "c3": {
                        "cycle_name": "4.1 Пешеход из-под препятствия справа 3 секунды",
                        "cycle_key": "QA-C3",
                    },
                }
            }
        }
        titles, aliases = _build_scenario_group_catalog(report_data)
        self.assertIn("1", titles)
        self.assertIn("4", titles)
        self.assertNotEqual(titles["1"], titles["4"])
        self.assertEqual(
            aliases["1.2 стационарный автомобиль объезд слева".lower()],
            "1",
        )

    def test_defect_scenario_names_collapsed_to_groups(self) -> None:
        report_data = {
            ("f1", "folder"): {
                "cycles": {
                    "c1": {"cycle_name": "1.1 Alpha sub", "cycle_key": "k1"},
                    "c2": {"cycle_name": "1.2 Beta sub (cloned)", "cycle_key": "k2"},
                }
            }
        }
        catalog = _build_scenario_group_catalog(report_data)
        raw = ["1.2 Beta sub (cloned)", "1.1 Alpha sub"]
        grouped = _defect_scenario_group_names_list(raw, catalog)
        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped[0], catalog[0]["1"])

    def test_unmapped_traceability_name_preserved(self) -> None:
        catalog = _build_scenario_group_catalog({})
        names = _defect_scenario_group_names_list(
            ["Объезд ремонтных работ слева"],
            catalog,
        )
        self.assertEqual(names, ["Объезд ремонтных работ слева"])

    def test_defect_scenario_names_list_uses_grouping_with_catalog(self) -> None:
        report_data = {
            ("f1", "folder"): {
                "cycles": {
                    "c1": {"cycle_name": "7.1 Экстренное торможение", "cycle_key": "k1"},
                }
            }
        }
        catalog = _build_scenario_group_catalog(report_data)
        meta = {}
        analytics = _empty_defect_analytics()
        analytics["bug_linked_scenarios"] = {
            "CSD-1": ["7.1 Экстренное торможение (cloned)", "7.1 Экстренное торможение"],
        }
        names = _defect_scenario_names_list(
            "CSD-1",
            meta,
            analytics,
            scenario_group_catalog=catalog,
        )
        self.assertEqual(len(names), 1)
        self.assertEqual(names[0], catalog[0]["7"])


class BugsRollupLastWeeksTests(unittest.TestCase):
    def test_last_weeks_capped_by_regenerate_window(self) -> None:
        with patch(
            "zephyr_weekly_report._env_prefers_repo_dotenv", return_value="4"
        ), patch(
            "zephyr_weekly_report._zephyr_regenerate_last_n_days_from_environment",
            return_value=14,
        ):
            self.assertEqual(_bugs_rollup_last_weeks_count(), 2)

    def test_section_title_uses_capped_week_count(self) -> None:
        with patch(
            "zephyr_weekly_report._env_prefers_repo_dotenv", return_value="4"
        ), patch(
            "zephyr_weekly_report._zephyr_regenerate_last_n_days_from_environment",
            return_value=14,
        ):
            title = _bugs_rollup_section_last_weeks_title(
                [date(2026, 5, 19), date(2026, 5, 26), date(2026, 6, 2), date(2026, 6, 9)]
            )
            self.assertEqual(title, "Баги за последние 2 нед.")


if __name__ == "__main__":
    unittest.main()

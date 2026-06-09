"""Tests for universal report Zephyr import helpers."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from zephyr_weekly_report import FolderNode

from universal_report.zephyr_import import (
    _build_cycle_key_query,
    _dedupe_cycle_summaries,
    _discover_raw_folder_nodes,
    _filter_cycles_by_date,
    fetch_cycles_by_import_mode,
    list_zephyr_folders,
)


class UniversalZephyrImportTests(unittest.TestCase):
    def test_build_cycle_key_query_numeric(self) -> None:
        query = _build_cycle_key_query("10904", "12345")
        self.assertIn("testRun.id = 12345", query)

    def test_build_cycle_key_query_key(self) -> None:
        query = _build_cycle_key_query("10904", "TR-99")
        self.assertIn("testRun.key", query)
        self.assertIn("TR-99", query)

    @patch("universal_report.zephyr_import.select_tree_target_folders")
    @patch("universal_report.zephyr_import._discover_foldertree_nodes")
    @patch("universal_report.zephyr_import._pipeline_headers")
    @patch("universal_report.zephyr_import.build_pipeline_args")
    def test_list_zephyr_folders_scope_all(
        self,
        mock_args: MagicMock,
        mock_headers: MagicMock,
        mock_foldertree: MagicMock,
        mock_select: MagicMock,
    ) -> None:
        mock_args.return_value = MagicMock()
        mock_headers.return_value = {}
        mock_foldertree.return_value = (
            [FolderNode(folder_id="1", folder_name="A", parent_id=None, full_path="A", is_leaf=True)],
            "GET foldertree",
        )
        mock_select.return_value = [
            FolderNode(folder_id="1", folder_name="A", parent_id=None, full_path="A", is_leaf=True)
        ]
        folders = list_zephyr_folders(scope="all")
        self.assertEqual(len(folders), 1)
        self.assertEqual(folders[0]["id"], "1")
        self.assertIn("parent_id", folders[0])
        mock_foldertree.assert_called_once()
        mock_select.assert_called_once()
        call_kwargs = mock_select.call_args.kwargs
        self.assertEqual(call_kwargs["root_folder_ids"], [])
        self.assertFalse(call_kwargs["leaf_only"])
        self.assertIsNone(call_kwargs["name_pattern"])

    @patch("universal_report.zephyr_import.fetch_cycles_for_folder")
    def test_fetch_cycles_by_import_mode_folder(self, mock_folder: MagicMock) -> None:
        mock_folder.return_value = {"c1": {"cases": {}}}
        result = fetch_cycles_by_import_mode(
            import_mode="folder",
            folder_id="99",
            folder_name="test",
        )
        self.assertIn("c1", result)
        mock_folder.assert_called_once()

    @patch("universal_report.zephyr_import.fetch_cycles_for_selected_runs")
    def test_fetch_cycles_by_import_mode_cycles(self, mock_selected: MagicMock) -> None:
        mock_selected.return_value = {"c1": {"cases": {}}}
        result = fetch_cycles_by_import_mode(
            import_mode="cycles",
            selected_cycles=[{"test_run_id": "1", "cycle_key": "TR-1"}],
        )
        self.assertIn("c1", result)
        mock_selected.assert_called_once()

    @patch("universal_report.zephyr_import.fetch_cycles_for_test_run")
    def test_fetch_cycles_by_import_mode_cycle_key(self, mock_cycle: MagicMock) -> None:
        mock_cycle.return_value = {"run-1": {"cases": {}}}
        result = fetch_cycles_by_import_mode(
            import_mode="cycle",
            cycle_key="TR-1",
        )
        self.assertIn("run-1", result)
        mock_cycle.assert_called_once_with("TR-1", from_date=None, to_date=None)

    @patch("universal_report.zephyr_import._discover_foldertree_nodes")
    @patch("universal_report.zephyr_import.discover_folders_tree_fallback")
    def test_discover_raw_folder_nodes_full_tree_prefers_foldertree(
        self,
        mock_fallback: MagicMock,
        mock_foldertree: MagicMock,
    ) -> None:
        args = MagicMock()
        headers = {}
        full_nodes = [
            FolderNode(folder_id="1", folder_name="Root", parent_id=None, full_path="Root", is_leaf=False)
        ]
        mock_foldertree.return_value = (full_nodes, "GET foldertree")
        nodes = _discover_raw_folder_nodes(args, headers, full_tree=True)
        self.assertEqual(nodes, full_nodes)
        mock_fallback.assert_not_called()

    @patch("universal_report.zephyr_import._discover_foldertree_nodes")
    def test_discover_raw_folder_nodes_full_tree_raises_when_empty(
        self,
        mock_foldertree: MagicMock,
    ) -> None:
        mock_foldertree.return_value = ([], "")
        with self.assertRaises(RuntimeError):
            _discover_raw_folder_nodes(MagicMock(), {}, full_tree=True)

    def test_dedupe_cycle_summaries(self) -> None:
        summaries = [
            {"test_run_id": "1", "cycle_key": "A", "cycle_name": "First", "status": ""},
            {"test_run_id": "1", "cycle_key": "A", "cycle_name": "Dup", "status": ""},
            {"test_run_id": "2", "cycle_key": "B", "cycle_name": "Second", "status": ""},
        ]
        deduped = _dedupe_cycle_summaries(summaries)
        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped[0]["cycle_key"], "A")

    def test_filter_cycles_by_date(self) -> None:
        from datetime import date

        cycles = {
            "c1": {
                "cases": {
                    "a": {"execution_date": "2026-06-01"},
                    "b": {"execution_date": "2026-06-15"},
                }
            }
        }
        filtered = _filter_cycles_by_date(cycles, date(2026, 6, 10), date(2026, 6, 20))
        self.assertEqual(list(filtered["c1"]["cases"].keys()), ["b"])


if __name__ == "__main__":
    unittest.main()

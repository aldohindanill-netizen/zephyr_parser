"""Tests for zephyr_pipeline_health."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import zephyr_pipeline_health as ph

UTC = timezone.utc


class PipelineHealthTests(unittest.TestCase):
    def _write_audit(self, path: Path, records: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "\n".join(json.dumps(x) for x in records) + "\n",
            encoding="utf-8",
        )

    def test_writes_html_with_audit_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            audit_path = root / "reports" / "audit" / "audit.jsonl"
            self._write_audit(
                audit_path,
                [
                    {"timestamp_utc": "2026-06-01T10:00:00Z", "operation": "run_start", "result": "success"},
                    {
                        "timestamp_utc": "2026-06-01T10:05:00Z",
                        "operation": "run_finish",
                        "result": "success",
                        "exit_code": 0,
                    },
                ],
            )
            out = root / "reports" / "pipeline_health.html"
            with mock.patch.object(ph, "_REPO_ROOT", root):
                with mock.patch.dict(
                    os.environ,
                    {
                        "ZEPHYR_AUDIT_LOG": str(audit_path),
                        "ZEPHYR_PIPELINE_HEALTH_HTML": str(out),
                        "ZEPHYR_RUN_LOCK_FILE": str(root / "reports" / "nolock"),
                    },
                    clear=False,
                ):
                    path = ph.write_pipeline_health_html(exit_code=0)
            self.assertTrue(path.is_file())
            text = path.read_text(encoding="utf-8")
            self.assertIn("run_finish", text)
            self.assertIn("Основной пайплайн", text)
            self.assertIn("Nightly embeddings", text)
            self.assertIn("13:00", text)

    def test_lock_stale_minutes_from_env(self) -> None:
        with mock.patch.dict(os.environ, {"ZEPHYR_HEALTH_LOCK_STALE_MINUTES": "120"}, clear=False):
            self.assertEqual(ph.lock_stale_minutes(), 120.0)
        with mock.patch.dict(
            os.environ,
            {"ZEPHYR_HEALTH_LOCK_STALE_MINUTES": "", "ZEPHYR_RUN_TIMEOUT_MINUTES": "75"},
            clear=False,
        ):
            self.assertEqual(ph.lock_stale_minutes(), 75.0)

    def test_stale_lock_shows_critical_hint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lock = root / "reports" / "weekly.lock"
            lock.parent.mkdir(parents=True)
            lock.write_text("x", encoding="utf-8")
            old = datetime.now().timestamp() - 120 * 60
            os.utime(lock, (old, old))
            state, detail = ph._lock_status_for_path(  # noqa: SLF001
                lock, stale_minutes=90.0, label="Python"
            )
            self.assertEqual(state, "stale")
            self.assertIn("Критично", detail)
            self.assertIn("runbook", detail)

    def test_embeddings_block_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            audit_path = root / "reports" / "audit" / "audit.jsonl"
            ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._write_audit(
                audit_path,
                [
                    {
                        "timestamp_utc": ts,
                        "operation": "embeddings_finish",
                        "result": "success",
                        "exit_code": 0,
                    },
                ],
            )
            out = root / "reports" / "pipeline_health.html"
            with mock.patch.object(ph, "_REPO_ROOT", root):
                with mock.patch.dict(
                    os.environ,
                    {
                        "ZEPHYR_AUDIT_LOG": str(audit_path),
                        "ZEPHYR_PIPELINE_HEALTH_HTML": str(out),
                        "ZEPHYR_RUN_LOCK_FILE": str(root / "reports" / "nolock"),
                    },
                    clear=False,
                ):
                    ph.write_pipeline_health_html()
            text = out.read_text(encoding="utf-8")
            self.assertIn("embeddings_finish", text)
            self.assertIn("success", text)

    def test_embeddings_failure_is_red(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            audit_path = root / "reports" / "audit" / "audit.jsonl"
            self._write_audit(
                audit_path,
                [
                    {
                        "timestamp_utc": "2026-06-01T02:30:00Z",
                        "operation": "embeddings_finish",
                        "result": "failure",
                        "exit_code": 1,
                    },
                ],
            )
            out = root / "reports" / "pipeline_health.html"
            with mock.patch.object(ph, "_REPO_ROOT", root):
                with mock.patch.dict(
                    os.environ,
                    {
                        "ZEPHYR_AUDIT_LOG": str(audit_path),
                        "ZEPHYR_PIPELINE_HEALTH_HTML": str(out),
                        "ZEPHYR_RUN_LOCK_FILE": str(root / "reports" / "nolock"),
                    },
                    clear=False,
                ):
                    ph.write_pipeline_health_html()
            text = out.read_text(encoding="utf-8")
            self.assertIn('class="err">failure', text)

    def test_publish_confluence_warn_row(self) -> None:
        rec = {
            "timestamp_utc": "2026-06-01T11:00:00Z",
            "operation": "publish_confluence",
            "result": "failure",
            "title": "Weekly Analytics",
        }
        self.assertTrue(ph._is_warn_record(rec))  # noqa: SLF001
        self.assertFalse(ph._is_error_record(rec))  # noqa: SLF001
        self.assertEqual(ph._audit_row_class(rec), "warn")  # noqa: SLF001

    def test_embeddings_finish_error_row(self) -> None:
        rec = {
            "timestamp_utc": "2026-06-01T02:00:00Z",
            "operation": "embeddings_finish",
            "result": "failure",
            "exit_code": 1,
        }
        self.assertTrue(ph._is_error_record(rec))  # noqa: SLF001
        self.assertEqual(ph._audit_row_class(rec), "err")  # noqa: SLF001

    def test_integration_call_error_row(self) -> None:
        rec = {
            "timestamp_utc": "2026-06-01T12:00:00Z",
            "operation": "integration_call",
            "result": "error",
        }
        self.assertTrue(ph._is_error_record(rec))  # noqa: SLF001

    def test_run_start_newer_than_finish_clears_stale_finish(self) -> None:
        records = [
            {
                "timestamp_utc": "2026-06-02T12:00:00Z",
                "operation": "run_start",
                "result": "success",
            },
            {
                "timestamp_utc": "2026-06-02T11:00:00Z",
                "operation": "run_finish",
                "result": "success",
                "exit_code": 0,
            },
        ]
        parsed = ph._last_run_from_audit(records)  # noqa: SLF001
        self.assertIsNotNone(parsed)
        self.assertIsNone(parsed["finish"])
        self.assertEqual(parsed["start"]["timestamp_utc"], "2026-06-02T12:00:00Z")

    def test_embeddings_found_beyond_display_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            audit_path = root / "reports" / "audit" / "audit.jsonl"
            audit_path.parent.mkdir(parents=True)
            filler = [
                json.dumps(
                    {
                        "timestamp_utc": f"2026-06-01T{h:02d}:00:00Z",
                        "operation": "run_finish",
                        "result": "success",
                        "exit_code": 0,
                    }
                )
                for h in range(600)
            ]
            emb_line = json.dumps(
                {
                    "timestamp_utc": "2026-06-01T02:30:00Z",
                    "operation": "embeddings_finish",
                    "result": "success",
                    "exit_code": 0,
                }
            )
            audit_path.write_text("\n".join([emb_line] + filler) + "\n", encoding="utf-8")
            with mock.patch.object(ph, "_REPO_ROOT", root):
                with mock.patch.dict(
                    os.environ,
                    {
                        "ZEPHYR_AUDIT_LOG": str(audit_path),
                        "ZEPHYR_HEALTH_EMBEDDINGS_AUDIT_SCAN_LINES": "10000",
                    },
                    clear=False,
                ):
                    tail = ph._read_audit_tail(500)  # noqa: SLF001
                    self.assertIsNone(ph._last_embeddings_from_audit(tail))  # noqa: SLF001
                    scanned = ph._last_embeddings_from_audit_scan()  # noqa: SLF001
            self.assertIsNotNone(scanned)
            self.assertEqual(scanned["finish"]["operation"], "embeddings_finish")

    def test_embeddings_running_when_start_newer_than_finish(self) -> None:
        emb = {
            "start": {
                "timestamp_utc": "2026-06-02T02:00:00Z",
                "operation": "embeddings_start",
                "result": "success",
            },
            "finish": None,
        }
        records = [
            {
                "timestamp_utc": "2026-06-01T02:30:00Z",
                "operation": "embeddings_finish",
                "result": "success",
                "exit_code": 0,
            },
            emb["start"],
        ]
        parsed = ph._last_embeddings_from_audit(records)  # noqa: SLF001
        self.assertIsNotNone(parsed)
        self.assertIsNone(parsed["finish"])
        status, _detail, css = ph._embeddings_summary(parsed, None)  # noqa: SLF001
        self.assertEqual(status, "running")
        self.assertEqual(css, "warn")

    def test_embeddings_interrupted_hours_from_env(self) -> None:
        emb = {
            "start": {
                "timestamp_utc": (datetime.now(UTC) - timedelta(hours=6)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "operation": "embeddings_start",
            },
            "finish": None,
        }
        with mock.patch.dict(os.environ, {"ZEPHYR_HEALTH_EMBEDDINGS_INTERRUPTED_HOURS": "5"}, clear=False):
            interrupted_h = ph.embeddings_interrupted_hours()
            status, detail, css = ph._embeddings_summary(emb, None)  # noqa: SLF001
        self.assertEqual(interrupted_h, 5.0)
        self.assertEqual(status, "прервано")
        self.assertIn("5", detail)
        self.assertEqual(css, "err")

    def test_embeddings_overdue_after_36h(self) -> None:
        old_ts = (datetime.now(UTC) - timedelta(hours=40)).strftime("%Y-%m-%dT%H:%M:%SZ")
        emb = {
            "finish": {
                "timestamp_utc": old_ts,
                "operation": "embeddings_finish",
                "result": "success",
                "exit_code": 0,
            }
        }
        status, _detail, css = ph._embeddings_summary(emb, None)  # noqa: SLF001
        self.assertEqual(status, "просрочено")
        self.assertEqual(css, "err")


if __name__ == "__main__":
    unittest.main()

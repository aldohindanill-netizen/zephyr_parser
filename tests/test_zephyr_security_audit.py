"""Tests for zephyr_security and zephyr_audit."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

import zephyr_audit as za
import zephyr_security as zs


class EnforceTokenTests(unittest.TestCase):
    """Класс «EnforceTokenTests»."""
    def test_cli_token_blocked_when_enforced(self) -> None:
        """Вспомогательная функция: test cli token blocked when enforced."""
        with mock.patch.dict(os.environ, {"ZEPHYR_ENFORCE_ENV_TOKEN": "true"}, clear=False):
            with self.assertRaises(ValueError) as ctx:
                zs.enforce_token_from_env_only("secret-token")
            self.assertIn("disabled", str(ctx.exception).lower())

    def test_cli_token_allowed_when_not_enforced(self) -> None:
        """Вспомогательная функция: test cli token allowed when not enforced."""
        with mock.patch.dict(os.environ, {"ZEPHYR_ENFORCE_ENV_TOKEN": "false"}, clear=False):
            with self.assertWarns(UserWarning):
                zs.enforce_token_from_env_only("secret-token")


class LogviewerStrictTests(unittest.TestCase):
    """Класс «LogviewerStrictTests»."""
    def test_strict_rejects_non_matching_url(self) -> None:
        """Вспомогательная функция: test strict rejects non matching url."""
        default_url = "https://logviewer.df.sbauto.tech/logs/abc123"
        bad = "https://evil.example/logs/abc"
        with mock.patch.dict(
            os.environ,
            {"ZEPHYR_LOGVIEWER_STRICT": "true", "ZEPHYR_LOGVIEWER_URL_REGEX": ""},
            clear=False,
        ):
            out = zs.filter_logviewer_urls([default_url, bad])
        self.assertEqual(out, [default_url])

    def test_non_strict_keeps_finditer_matches_only_via_fullmatch(self) -> None:
        """Вспомогательная функция: test non strict keeps finditer matches only via fullmatch."""
        bad = "https://evil.example/logs/abc"
        with mock.patch.dict(
            os.environ,
            {"ZEPHYR_LOGVIEWER_STRICT": "false", "ZEPHYR_LOGVIEWER_URL_REGEX": ""},
            clear=False,
        ):
            out = zs.filter_logviewer_urls([bad])
        self.assertEqual(out, [])


class AuditLogTests(unittest.TestCase):
    """Класс «AuditLogTests»."""
    def test_audit_writes_jsonl_line(self) -> None:
        """Вспомогательная функция: test audit writes jsonl line."""
        za.reset_run_id()
        with tempfile.TemporaryDirectory() as tmp:
            log_path = os.path.join(tmp, "audit.jsonl")
            with mock.patch.dict(
                os.environ,
                {
                    "ZEPHYR_AUDIT_ENABLED": "true",
                    "ZEPHYR_AUDIT_LOG": log_path,
                    "ZEPHYR_AUDIT_REASON": "unit test",
                },
                clear=False,
            ):
                za.audit_run_start(mode="test")
                za.audit_export_file("/tmp/out.csv", kind="weekly_csv")
                za.audit_run_finish(0)
                za.audit_embeddings_start()
                za.audit_embeddings_finish(0)
            with open(log_path, encoding="utf-8") as fh:
                lines = fh.read().strip().splitlines()
        self.assertGreaterEqual(len(lines), 5)
        first = json.loads(lines[0])
        self.assertEqual(first["operation"], "run_start")
        self.assertEqual(first["reason"], "unit test")
        emb_start = json.loads(lines[-2])
        emb_finish = json.loads(lines[-1])
        self.assertEqual(emb_start["operation"], "embeddings_start")
        self.assertEqual(emb_finish["operation"], "embeddings_finish")
        self.assertEqual(emb_start["run_id"], first["run_id"])
        self.assertEqual(emb_finish["run_id"], first["run_id"])
        self.assertEqual(emb_finish["exit_code"], 0)

    def test_embeddings_audit_reuses_preset_run_id(self) -> None:
        """Вспомогательная функция: test embeddings audit reuses preset run id."""
        za.reset_run_id()
        preset = "00000000-0000-4000-8000-000000000001"
        with tempfile.TemporaryDirectory() as tmp:
            log_path = os.path.join(tmp, "audit.jsonl")
            with mock.patch.dict(
                os.environ,
                {
                    "ZEPHYR_AUDIT_ENABLED": "true",
                    "ZEPHYR_AUDIT_LOG": log_path,
                    "ZEPHYR_AUDIT_RUN_ID": preset,
                },
                clear=False,
            ):
                za.audit_embeddings_start()
                za.audit_embeddings_finish(0)
            with open(log_path, encoding="utf-8") as fh:
                records = [json.loads(line) for line in fh]
        self.assertEqual(records[0]["run_id"], preset)
        self.assertEqual(records[1]["run_id"], preset)


if __name__ == "__main__":
    unittest.main()

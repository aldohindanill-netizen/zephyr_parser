"""Characterization tests for repo_env (.env parsing shared by main script and tools)."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import repo_env as re


class RepoEnvTests(unittest.TestCase):
    def tearDown(self) -> None:
        re._dotenv_cache = None
        re._dotenv_secrets_cache = None
        re._dotenv_local_cache = None

    def test_parse_dotenv_strips_quotes_and_export_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".env"
            path.write_text(
                'export FOO="bar"\n# comment\nBAZ=1\n',
                encoding="utf-8",
            )
            parsed = re._parse_dotenv_file(path)
            self.assertEqual(parsed["FOO"], "bar")
            self.assertEqual(parsed["BAZ"], "1")
            self.assertNotIn("comment", parsed)

    def test_load_repo_env_does_not_override_existing_process_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".env").write_text("KEEP_ME=from_file\n", encoding="utf-8")
            with mock.patch.object(re, "_REPO_ROOT", root):
                re._dotenv_cache = None
                with mock.patch.dict(os.environ, {"KEEP_ME": "from_process"}, clear=False):
                    re.load_repo_env(overlay_local=False)
                    self.assertEqual(os.environ.get("KEEP_ME"), "from_process")

    def test_load_repo_env_overlay_local_wins(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".env").write_text("KEY=base\n", encoding="utf-8")
            (root / ".env.local").write_text("KEY=local\n", encoding="utf-8")
            with mock.patch.object(re, "_REPO_ROOT", root):
                re._dotenv_cache = None
                re._dotenv_local_cache = None
                env_copy = {k: v for k, v in os.environ.items() if k != "KEY"}
                with mock.patch.dict(os.environ, env_copy, clear=True):
                    re.load_repo_env(overlay_local=True)
                    self.assertEqual(os.environ.get("KEY"), "local")

    def test_load_repo_env_secrets_do_not_override_process_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".env").write_text("", encoding="utf-8")
            (root / ".env.secrets").write_text("KEEP_ME=from_secrets\n", encoding="utf-8")
            with mock.patch.object(re, "_REPO_ROOT", root):
                re._dotenv_cache = None
                re._dotenv_secrets_cache = None
                with mock.patch.dict(os.environ, {"KEEP_ME": "from_process"}, clear=False):
                    re.load_repo_env(overlay_local=False)
                    self.assertEqual(os.environ.get("KEEP_ME"), "from_process")

    def test_load_repo_env_secrets_override_env_file_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".env").write_text("SECRET_TOKEN=from_env\n", encoding="utf-8")
            (root / ".env.secrets").write_text("SECRET_TOKEN=from_secrets\n", encoding="utf-8")
            with mock.patch.object(re, "_REPO_ROOT", root):
                re._dotenv_cache = None
                re._dotenv_secrets_cache = None
                env_copy = {k: v for k, v in os.environ.items() if k != "SECRET_TOKEN"}
                with mock.patch.dict(os.environ, env_copy, clear=True):
                    re.load_repo_env(overlay_local=False)
                    self.assertEqual(os.environ.get("SECRET_TOKEN"), "from_secrets")

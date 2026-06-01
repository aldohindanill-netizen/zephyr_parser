"""Tests for Confluence publish title/lookup helpers."""

from __future__ import annotations

import os
import unittest
from unittest import mock

from zephyr_weekly_report import (
    ConfluencePublishConfig,
    _confluence_publish_title,
    _confluence_space_wide_title_lookup,
)


def _cfg(*, title_prefix: str = "") -> ConfluencePublishConfig:
    return ConfluencePublishConfig(
        base_url="https://wiki.example",
        space_key="QA",
        api_prefix="/wiki/rest/api",
        parent_page_id="1",
        user="u",
        api_token="t",
        title_prefix=title_prefix,
    )


class ConfluencePublishTitleTests(unittest.TestCase):
    def test_prefix_applied_to_html_style_title(self) -> None:
        cfg = _cfg(title_prefix="[LOCAL]")
        self.assertEqual(
            _confluence_publish_title("nightly dev report", cfg),
            "[LOCAL] nightly dev report",
        )

    def test_prefix_not_doubled(self) -> None:
        cfg = _cfg(title_prefix="[LOCAL]")
        self.assertEqual(
            _confluence_publish_title("[LOCAL] nightly dev report", cfg),
            "[LOCAL] nightly dev report",
        )

    def test_space_wide_lookup_off_when_prefix_set(self) -> None:
        cfg = _cfg(title_prefix="[LOCAL]")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ZEPHYR_CONFLUENCE_SPACE_WIDE_TITLE_LOOKUP", None)
            self.assertFalse(_confluence_space_wide_title_lookup(cfg))

    def test_space_wide_lookup_env_override(self) -> None:
        cfg = _cfg(title_prefix="[LOCAL]")
        with mock.patch.dict(
            os.environ,
            {"ZEPHYR_CONFLUENCE_SPACE_WIDE_TITLE_LOOKUP": "true"},
            clear=False,
        ):
            self.assertTrue(_confluence_space_wide_title_lookup(cfg))


if __name__ == "__main__":
    unittest.main()

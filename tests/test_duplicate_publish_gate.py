from __future__ import annotations

import os
import unittest
from unittest import mock

from bug_duplicate_detection import DuplicateCandidate
from zephyr_weekly_report import _duplicate_candidate_cell_html, _duplicate_candidate_cell_wiki


class DuplicatePublishGateTests(unittest.TestCase):
    """Класс «DuplicatePublishGateTests»."""
    def test_html_hides_medium_when_publish_min_high(self) -> None:
        """Вспомогательная функция: test html hides medium when publish min high."""
        cand = DuplicateCandidate("CSD-2", 0.9, "embedding_candidate", confidence="medium")
        with mock.patch.dict(
            os.environ, {"ZEPHYR_BUGS_DUPLICATE_PUBLISH_MIN_CONFIDENCE": "high"}, clear=False
        ):
            cell = _duplicate_candidate_cell_html("CSD-1", {"CSD-1": cand})
        self.assertIn("—", cell)

    def test_wiki_shows_medium_when_publish_min_medium(self) -> None:
        """Вспомогательная функция: test wiki shows medium when publish min medium."""
        cand = DuplicateCandidate("CSD-2", 0.9, "embedding_candidate", confidence="medium")
        with mock.patch.dict(
            os.environ, {"ZEPHYR_BUGS_DUPLICATE_PUBLISH_MIN_CONFIDENCE": "medium"}, clear=False
        ):
            cell = _duplicate_candidate_cell_wiki("CSD-1", {"CSD-1": cand})
        self.assertIn("возможно дубль", cell)


if __name__ == "__main__":
    unittest.main()

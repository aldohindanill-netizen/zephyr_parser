import unittest
from unittest import mock

import zephyr_weekly_report as z


class JiraSummaryFetchTests(unittest.TestCase):
    """Класс «JiraSummaryFetchTests»."""
    def test_falls_back_to_single_issue_for_missing_bulk_keys(self):
        """Вспомогательная функция: test falls back to single issue for missing bulk keys."""
        def fake_request_json(base_url, endpoint, headers, **kwargs):  # noqa: ANN001
            """Вспомогательная функция: fake request json."""
            if endpoint in ("/rest/api/2/search", "/rest/api/3/search"):
                return {
                    "issues": [
                        {"key": "CSD-1", "fields": {"summary": "first summary"}},
                    ]
                }
            if endpoint in ("/rest/api/2/issue/CSD-2", "/rest/api/3/issue/CSD-2"):
                return {"key": "CSD-2", "fields": {"summary": "second summary"}}
            raise RuntimeError(f"Unexpected endpoint: {endpoint}")

        with mock.patch.object(z, "request_json", side_effect=fake_request_json):
            result = z._fetch_jira_issue_summaries(  # noqa: SLF001
                ["CSD-1", "CSD-2"],
                base_url="https://jira.example",
                auth_headers={"Authorization": "Bearer token"},
            )

        self.assertEqual(result.get("CSD-1"), "first summary")
        self.assertEqual(result.get("CSD-2"), "second summary")


if __name__ == "__main__":
    unittest.main()

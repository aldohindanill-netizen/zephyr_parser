"""Unit tests for bug_duplicate_detection."""

from __future__ import annotations

import os
import tempfile
import unittest

from bug_duplicate_detection import (
    DuplicateCandidate,
    _normalize_summary_for_match,
    _tokenize_summary,
    build_results_embedding_text,
    find_duplicate_candidates,
    load_embedding_cache,
    parse_jira_description_fields,
    parse_traceability_scenario_names,
    results_hash,
    summary_hash,
    text_similarity,
    write_duplicate_candidates_debug,
)

SCREENSHOT_TABLE = """
| Branch / Commit hash | nightly-dev-2026.04.26 |
| Link to Logviewer | https://logviewer.example/logs |
| Preconditions | Мишень стоит по центру полосы движения ВАТС. В соседней полосе установлены конусы. |
| Actual result | ВАТС не останавливается и едет в конусы. |
| Expected result | При приближении к мишени/конусам, ВАТС останавливается, избегая столкновения. |
| Reproducibility | |
"""

JIRA_WIKI_TABLE = (
    "|*Branch /* {*}Commit hash{*}{*}:{*}|*nightly-dev-2026.04.15*|\n"
    "|*Preconditions:*|Мишень стоит по центру полосы. В соседней полосе конусы.|\n"
    "|*Actual result:*|ВАТС врезается в конусы|\n"
    "|*Expected result:*|При приближении ВАТС останавливается, избегая столкновения.|\n"
    "|*Reproducibility:*|1/3|"
)


class ParseDescriptionFieldsTests(unittest.TestCase):
    def test_parses_wiki_table_from_screenshot(self) -> None:
        fields = parse_jira_description_fields(SCREENSHOT_TABLE)
        self.assertIn("конусы", fields["actual_result"])
        self.assertIn("останавливается", fields["expected_result"])
        self.assertIn("Мишень", fields["preconditions"])

    def test_parses_colon_format(self) -> None:
        text = "Actual result: ВАТС едет в конусы\nExpected result: ВАТС останавливается"
        fields = parse_jira_description_fields(text)
        self.assertIn("конусы", fields["actual_result"])
        self.assertIn("останавливается", fields["expected_result"])

    def test_parses_label_on_next_line(self) -> None:
        text = "Actual result\nВАТС не останавливается\nExpected result\nВАТС останавливается"
        fields = parse_jira_description_fields(text)
        self.assertEqual(fields["actual_result"], "ВАТС не останавливается")
        self.assertEqual(fields["expected_result"], "ВАТС останавливается")

    def test_parses_jira_wiki_bold_table(self) -> None:
        fields = parse_jira_description_fields(JIRA_WIKI_TABLE)
        self.assertIn("конусы", fields["actual_result"].lower())
        self.assertIn("останавливается", fields["expected_result"].lower())
        self.assertIn("Мишень", fields["preconditions"])

    def test_parses_traceability_table_row(self) -> None:
        table = (
            "|*Traceability:*|"
            "[Сценарий A|https://jira.example/browse/QA-T1], "
            "[Сценарий B|https://jira.example/browse/QA-T2]|"
        )
        fields = parse_jira_description_fields(table)
        self.assertIn("Сценарий A", fields["traceability"])
        names = parse_traceability_scenario_names(fields["traceability"])
        self.assertEqual(names, ["Сценарий A", "Сценарий B"])

    def test_traceability_dedupes_case_insensitive(self) -> None:
        raw = "[Сценарий A|url1], сценарий a; Сценарий B"
        names = parse_traceability_scenario_names(raw)
        self.assertEqual(names, ["Сценарий A", "Сценарий B"])


class NormalizeSummaryTests(unittest.TestCase):
    def test_strips_vats_prefix(self) -> None:
        raw = "ВАТС не останавливается перед препятствием."
        norm = _normalize_summary_for_match(raw)
        self.assertFalse(norm.startswith("ватс"))
        self.assertIn("останавливается", norm)

    def test_lowercase_and_punctuation(self) -> None:
        norm = _normalize_summary_for_match("Hello, World!")
        self.assertEqual(norm, "hello world")


class TextSimilarityTests(unittest.TestCase):
    def test_pedestrian_braking_pair_high_score(self) -> None:
        a = "ВАТС не начинает торможение перед пешеходом спереди"
        b = "ВАТС перестает тормозить перед пешеходом спереди"
        score = text_similarity(a, b)
        self.assertGreaterEqual(score, 0.78)

    def test_unrelated_low_score(self) -> None:
        a = "Возникает МРМ2 без видимых причин на полигоне SAT"
        b = "ВАТС останавливается на перекрестке на зеленый сигнал светофора"
        score = text_similarity(a, b)
        self.assertLess(score, 0.78)


class FindDuplicateCandidatesTests(unittest.TestCase):
    def _meta(self, entries: list[tuple[str, dict[str, str]]]) -> dict[str, dict[str, str]]:
        return dict(entries)

    def test_similar_summary_but_different_results_not_duplicate(self) -> None:
        meta = self._meta(
            [
                (
                    "CSD-46908",
                    {
                        "summary": "ВАТС не начинает торможение перед пешеходом спереди",
                        "expected_result": "ВАТС тормозит перед пешеходом",
                        "actual_result": "ВАТС не начинает торможение",
                    },
                ),
                (
                    "CSD-46911",
                    {
                        "summary": "ВАТС перестает тормозить перед пешеходом спереди",
                        "expected_result": "ВАТС продолжает тормозить перед пешеходом",
                        "actual_result": "ВАТС перестает тормозить",
                    },
                ),
            ]
        )
        result = find_duplicate_candidates(
            ["CSD-46908", "CSD-46911"],
            meta,
            text_threshold=0.78,
            use_embeddings=False,
        )
        self.assertIsNone(result["CSD-46908"])
        self.assertIsNone(result["CSD-46911"])

    def test_same_expected_and_actual_are_duplicates(self) -> None:
        shared_expected = "При приближении к мишени, ВАТС останавливается"
        shared_actual = "ВАТС не останавливается и едет в конусы"
        meta = self._meta(
            [
                (
                    "CSD-A",
                    {
                        "summary": "Разный заголовок A",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
                (
                    "CSD-B",
                    {
                        "summary": "Совсем другой заголовок B",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
            ]
        )
        result = find_duplicate_candidates(
            ["CSD-A", "CSD-B"],
            meta,
            text_threshold=0.78,
            use_embeddings=False,
        )
        self.assertIsNotNone(result["CSD-A"])
        assert result["CSD-A"] is not None
        self.assertEqual(result["CSD-A"].other_key, "CSD-B")
        self.assertEqual(result["CSD-A"].method, "text_expected_actual")

    def test_fallback_summary_when_no_results(self) -> None:
        meta = self._meta(
            [
                ("CSD-46908", {"summary": "ВАТС не начинает торможение перед пешеходом спереди"}),
                ("CSD-46911", {"summary": "ВАТС перестает тормозить перед пешеходом спереди"}),
            ]
        )
        result = find_duplicate_candidates(
            ["CSD-46908", "CSD-46911"],
            meta,
            text_threshold=0.78,
            use_embeddings=False,
        )
        self.assertIsNotNone(result["CSD-46908"])
        assert result["CSD-46908"] is not None
        self.assertEqual(result["CSD-46908"].method, "text_summary")

    def test_no_match_below_threshold(self) -> None:
        meta = self._meta(
            [
                ("CSD-48117", {"summary": "Возникает МРМ2 без видимых причин"}),
                ("CSD-46929", {"summary": "ВАТС останавливается на перекрестке на зеленый сигнал"}),
            ]
        )
        result = find_duplicate_candidates(
            ["CSD-48117", "CSD-46929"],
            meta,
            text_threshold=0.78,
            use_embeddings=False,
        )
        self.assertIsNone(result["CSD-48117"])

    def test_split_override_blocks_match(self) -> None:
        meta = self._meta(
            [
                (
                    "CSD-46908",
                    {
                        "summary": "same",
                        "expected_result": "same exp",
                        "actual_result": "same act",
                    },
                ),
                (
                    "CSD-46911",
                    {
                        "summary": "same",
                        "expected_result": "same exp",
                        "actual_result": "same act",
                    },
                ),
            ]
        )
        overrides = {"split": [["CSD-46908", "CSD-46911"]]}
        result = find_duplicate_candidates(
            ["CSD-46908", "CSD-46911"],
            meta,
            overrides=overrides,
            use_embeddings=False,
        )
        self.assertIsNone(result["CSD-46908"])

    def test_merge_override_forces_link(self) -> None:
        meta = self._meta(
            [
                ("CSD-A", {"summary": "one"}),
                ("CSD-B", {"summary": "two"}),
            ]
        )
        overrides = {"merge": [["CSD-A", "CSD-B"]]}
        result = find_duplicate_candidates(
            ["CSD-A", "CSD-B"],
            meta,
            overrides=overrides,
            use_embeddings=False,
        )
        self.assertIsNotNone(result["CSD-A"])
        assert result["CSD-A"] is not None
        self.assertEqual(result["CSD-A"].method, "override")

    def test_embedding_cache_merge(self) -> None:
        meta = self._meta(
            [
                (
                    "CSD-X",
                    {
                        "summary": "ignored",
                        "expected_result": "полностью разное описание ошибки номер один",
                        "actual_result": "ещё один факт",
                    },
                ),
                (
                    "CSD-Y",
                    {
                        "summary": "ignored",
                        "expected_result": "абсолютно иной дефект без общих слов",
                        "actual_result": "другой факт",
                    },
                ),
            ]
        )
        cache = {
            "vectors": {
                "CSD-X": [1.0, 0.0, 0.0],
                "CSD-Y": [0.99, 0.01, 0.0],
            }
        }
        result = find_duplicate_candidates(
            ["CSD-X", "CSD-Y"],
            meta,
            embedding_cache=cache,
            text_threshold=0.99,
            embed_threshold=0.85,
            use_embeddings=True,
        )
        self.assertIsNotNone(result["CSD-X"])
        assert result["CSD-X"] is not None
        self.assertEqual(result["CSD-X"].method, "embedding_candidate")
        self.assertEqual(result["CSD-X"].confidence, "medium")

    def test_domain_gate_blocks_conflicting_embedding(self) -> None:
        meta = self._meta(
            [
                (
                    "CSD-A",
                    {
                        "summary": "ВАТС не тормозит перед пешеходом",
                        "expected_result": "ВАТС тормозит перед пешеходом",
                        "actual_result": "ВАТС едет в пешехода",
                    },
                ),
                (
                    "CSD-B",
                    {
                        "summary": "Самопроизвольное отключение АП валидатором локализации",
                        "expected_result": "АП не отключается",
                        "actual_result": "Локализация валится и АП выключается",
                    },
                ),
            ]
        )
        cache = {
            "vectors": {
                "CSD-A": [1.0, 0.0, 0.0],
                "CSD-B": [0.99, 0.01, 0.0],
            }
        }
        result = find_duplicate_candidates(
            ["CSD-A", "CSD-B"],
            meta,
            embedding_cache=cache,
            text_threshold=0.99,
            embed_threshold=0.85,
            use_embeddings=True,
        )
        self.assertIsNone(result["CSD-A"])

    def test_soft_expected_actual_rule_promotes_high(self) -> None:
        meta = self._meta(
            [
                (
                    "CSD-47279",
                    {
                        "summary": "ВАТС врезается в конусы при возможности их объезда",
                        "expected_result": "При приближении к ремонтным работам ВАТС перестраивается с безопасным интервалом",
                        "actual_result": "ВАТС врезается в конусы",
                    },
                ),
                (
                    "CSD-46923",
                    {
                        "summary": "ВАТС едет в конусы",
                        "expected_result": "При приближении к ремонтным работам ВАТС останавливается или объезжает конусы с интервалом",
                        "actual_result": "ВАТС едет в конусы",
                    },
                ),
            ]
        )
        result = find_duplicate_candidates(
            ["CSD-47279", "CSD-46923"],
            meta,
            text_threshold=0.78,
            use_embeddings=False,
        )
        self.assertIsNotNone(result["CSD-47279"])
        assert result["CSD-47279"] is not None
        self.assertEqual(result["CSD-47279"].method, "text_expected_actual_soft")
        self.assertEqual(result["CSD-47279"].confidence, "high")

    def test_scenario_conflict_blocks_despite_matching_results(self) -> None:
        shared_expected = "При приближении к мишени, ВАТС останавливается"
        shared_actual = "ВАТС не останавливается и едет в конусы"
        meta = self._meta(
            [
                (
                    "CSD-A",
                    {
                        "summary": "A",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
                (
                    "CSD-B",
                    {
                        "summary": "B",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
            ]
        )
        scenarios_by_key = {
            "CSD-A": ["1. Сценарий объезда"],
            "CSD-B": ["2. Сценарий пешехода"],
        }
        result = find_duplicate_candidates(
            ["CSD-A", "CSD-B"],
            meta,
            scenarios_by_key=scenarios_by_key,
            text_threshold=0.78,
            use_embeddings=False,
        )
        self.assertIsNone(result["CSD-A"])
        self.assertIsNone(result["CSD-B"])

    def test_scenario_match_allows_duplicate(self) -> None:
        shared_expected = "При приближении к мишени, ВАТС останавливается"
        shared_actual = "ВАТС не останавливается и едет в конусы"
        meta = self._meta(
            [
                (
                    "CSD-A",
                    {
                        "summary": "A",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
                (
                    "CSD-B",
                    {
                        "summary": "B",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
            ]
        )
        scenarios_by_key = {
            "CSD-A": ["1. Сценарий объезда", "2. Другой"],
            "CSD-B": ["1. Сценарий объезда"],
        }
        result = find_duplicate_candidates(
            ["CSD-A", "CSD-B"],
            meta,
            scenarios_by_key=scenarios_by_key,
            text_threshold=0.78,
            use_embeddings=False,
        )
        self.assertIsNotNone(result["CSD-A"])
        assert result["CSD-A"] is not None
        self.assertEqual(result["CSD-A"].other_key, "CSD-B")
        self.assertTrue(result["CSD-A"].scenario_match)

    def test_scenario_unknown_when_both_empty(self) -> None:
        shared_expected = "При приближении к мишени, ВАТС останавливается"
        shared_actual = "ВАТС не останавливается и едет в конусы"
        meta = self._meta(
            [
                (
                    "CSD-A",
                    {
                        "summary": "A",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
                (
                    "CSD-B",
                    {
                        "summary": "B",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
            ]
        )
        result = find_duplicate_candidates(
            ["CSD-A", "CSD-B"],
            meta,
            scenarios_by_key={"CSD-A": [], "CSD-B": []},
            text_threshold=0.78,
            use_embeddings=False,
        )
        self.assertIsNotNone(result["CSD-A"])

    def test_scenario_gate_disabled_allows_conflict(self) -> None:
        shared_expected = "При приближении к мишени, ВАТС останавливается"
        shared_actual = "ВАТС не останавливается и едет в конусы"
        meta = self._meta(
            [
                (
                    "CSD-A",
                    {
                        "summary": "A",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
                (
                    "CSD-B",
                    {
                        "summary": "B",
                        "expected_result": shared_expected,
                        "actual_result": shared_actual,
                    },
                ),
            ]
        )
        scenarios_by_key = {
            "CSD-A": ["1. Сценарий объезда"],
            "CSD-B": ["2. Сценарий пешехода"],
        }
        env_key = "ZEPHYR_BUGS_DUPLICATE_SCENARIO_GATE"
        prev = os.environ.get(env_key)
        try:
            os.environ[env_key] = "false"
            result = find_duplicate_candidates(
                ["CSD-A", "CSD-B"],
                meta,
                scenarios_by_key=scenarios_by_key,
                text_threshold=0.78,
                use_embeddings=False,
            )
        finally:
            if prev is None:
                os.environ.pop(env_key, None)
            else:
                os.environ[env_key] = prev
        self.assertIsNotNone(result["CSD-A"])


class CacheAndDebugTests(unittest.TestCase):
    def test_load_missing_cache(self) -> None:
        self.assertIsNone(load_embedding_cache("/nonexistent/path.json"))

    def test_write_debug_json_includes_sims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "duplicate_candidates.json")
            write_duplicate_candidates_debug(
                path,
                {
                    "CSD-1": DuplicateCandidate(
                        "CSD-2", 0.9, "text_expected_actual", expected_sim=0.95, actual_sim=0.9
                    ),
                },
            )
            with open(path, encoding="utf-8") as fh:
                content = fh.read()
            self.assertIn("expected_sim", content)
            self.assertIn("actual_sim", content)
            self.assertIn("confidence", content)


class ResultsEmbeddingTextTests(unittest.TestCase):
    def test_build_results_text(self) -> None:
        text = build_results_embedding_text("ожидаем", "факт")
        self.assertIn("EXPECTED:", text)
        self.assertIn("ACTUAL:", text)

    def test_results_hash_stable(self) -> None:
        h1 = results_hash("exp", "act")
        h2 = results_hash("exp", "act")
        self.assertEqual(h1, h2)
        self.assertNotEqual(h1, summary_hash("exp act"))


if __name__ == "__main__":
    unittest.main()

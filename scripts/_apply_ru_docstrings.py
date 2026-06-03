#!/usr/bin/env python3
"""Одноразовый скрипт: русские docstring в zephyr_weekly_report.py (не меняет логику)."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
TARGET = _REPO / "zephyr_weekly_report.py"
RU_MAP_PATH = _REPO / "_en_docstrings_ru_map.json"

SECTION_HEADERS: list[tuple[int, str]] = [
    (69, "# --- CLI, env, конфигурация ---"),
    (1270, "# --- Загрузка .env и Confluence (базовые хелперы) ---"),
    (3500, "# --- Даты билдов, nightly, папки ---"),
    (3820, "# --- Zephyr/Jira: ссылки, traceLinks, issue keys ---"),
    (4600, "# --- Daily readable: агрегация шагов и legacy ---"),
    (7000, "# --- Weekly matrix, defect analytics ---"),
    (9070, "# --- Bugs rollup и snapshot ---"),
    (10800, "# --- Weekly analytics, matrix delta ---"),
    (12000, "# --- Daily HTML/wiki, Chart и Zephyr macro ---"),
    (13160, "# --- Build-log отчёты по Jira issue ---"),
    (13360, "# --- Zephyr API write (test results) ---"),
    (14950, "# --- main, orchestration, логирование ---"),
]

_GUESS_PREFIX: list[tuple[str, str]] = [
    ("_env_int", "Прочитать целое из переменной окружения."),
    ("_env_bool", "Прочитать булево из переменной окружения."),
    ("_env_csv", "Прочитать список CSV из переменной окружения."),
    ("_load_", "Загрузить: "),
    ("_save_", "Сохранить: "),
    ("_fetch_", "Загрузить из API: "),
    ("_parse_", "Разобрать: "),
    ("_merge_", "Слить: "),
    ("_build_", "Построить: "),
    ("_render_", "Сформировать разметку: "),
    ("_write_", "Записать: "),
    ("_empty_", "Пустая структура: "),
    ("_default_", "По умолчанию: "),
    ("_resolve_", "Определить: "),
    ("_normalize_", "Нормализовать: "),
    ("_format_", "Форматировать: "),
    ("_validate_", "Проверить: "),
    ("_strip_", "Очистить: "),
    ("_extract_", "Извлечь: "),
    ("_compute_", "Вычислить: "),
    ("_collect_", "Собрать: "),
    ("_aggregate_", "Агрегировать: "),
    ("_bootstrap_", "Инициализация с диска: "),
    ("_refresh_", "Обновить: "),
    ("_prune_", "Удалить устаревшее: "),
    ("_bugs_rollup_", "Bugs rollup: "),
    ("_defect_", "Дефекты: "),
    ("_confluence_", "Confluence: "),
    ("_daily_", "Daily-отчёт: "),
    ("_weekly_", "Weekly-отчёт: "),
    ("_jira_", "Jira: "),
    ("_zephyr_", "Zephyr: "),
    ("render_", "Сформировать HTML/wiki: "),
    ("write_", "Записать отчёты: "),
    ("fetch_", "Загрузить из API: "),
    ("publish_", "Опубликовать в Confluence: "),
    ("build_", "Построить: "),
    ("parse_", "Разобрать: "),
    ("aggregate_", "Агрегировать: "),
    ("request_", "HTTP-запрос: "),
    ("list_", "Список: "),
    ("post_", "POST в Zephyr API: "),
    ("put_", "PUT в Zephyr API: "),
]


def _humanize(name: str) -> str:
    return name.lstrip("_").replace("_", " ") or name


def guess_docstring(name: str, *, is_class: bool) -> str:
    if is_class:
        return f"Класс «{name}»."
    for prefix, hint in _GUESS_PREFIX:
        if name.startswith(prefix):
            tail = _humanize(name[len(prefix) :])
            return f"{hint}{tail}." if tail else hint.rstrip(": ") + "."
    return f"Вспомогательная функция: {_humanize(name)}."


def load_ru_map() -> dict[str, str]:
    if RU_MAP_PATH.is_file():
        return json.loads(RU_MAP_PATH.read_text(encoding="utf-8"))
    return {}


def translate_en_doc(doc: str, ru_map: dict[str, str]) -> str:
    doc = doc.strip()
    if doc in ru_map:
        return ru_map[doc]
    if re.search(r"[а-яА-ЯёЁ]", doc):
        return doc
    # Краткий русский пересказ первой строки (без дословного EN)
    first = doc.split("\n", 1)[0].strip().rstrip(".")
    return f"См. реализацию: {first}."


def _docstring_expr(body: list[ast.stmt]) -> ast.Expr | None:
    if not body:
        return None
    first = body[0]
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
        if isinstance(first.value.value, str):
            return first
    return None


def _set_lines_docstring(
    lines: list[str],
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
    new_doc: str,
) -> None:
    if not node.body:
        return
    line_idx = node.body[0].lineno - 1
    indent = "    "
    m = re.match(r"^(\s*)", lines[line_idx])
    if m:
        indent = m.group(1)
    safe = new_doc.replace('"""', "'\"'\"'")
    if "\n" in new_doc:
        inner = "\n".join(indent + ln for ln in new_doc.split("\n"))
        block = f'{indent}"""\n{inner}\n{indent}"""\n'
    else:
        block = f'{indent}"""{safe}"""\n'

    existing = _docstring_expr(node.body)
    if existing is not None:
        start = existing.lineno - 1
        end = existing.end_lineno
        lines[start:end] = [block]
    else:
        ins = node.body[0].lineno - 1
        lines.insert(ins, block)


def _insert_section_headers(lines: list[str]) -> None:
    for line_no, header in sorted(SECTION_HEADERS, key=lambda x: -x[0]):
        idx = line_no - 1
        if 0 <= idx < len(lines) and header in lines[idx]:
            continue
        if 0 <= idx < len(lines) and lines[idx].strip().startswith("# ---"):
            continue
        lines.insert(idx, header + "\n")


def apply(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    source = source.replace(
        '"""Generate a weekly Zephyr test execution summary.\n\nThe script fetches paginated execution data from a Zephyr API endpoint,\naggregates executions by ISO week (Monday start) using raw API statuses,\nand computes normalized pass rate for reporting.\n"""',
        '"""Недельный отчёт по экзекьюшенам Zephyr (Jira-hosted API).\n\nСкачивает пагинированные execution, агрегирует по ISO-неделям (понедельник),\nсчитает pass rate, пишет CSV/HTML/wiki и опционально публикует в Confluence.\n"""',
        1,
    )
    ru_map = load_ru_map()
    tree = ast.parse(source)
    lines = source.splitlines(keepends=True)
    nodes: list[ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            nodes.append(node)
    nodes.sort(key=lambda n: n.lineno, reverse=True)

    for node in nodes:
        ds = ast.get_docstring(node)
        if ds and re.search(r"[а-яА-ЯёЁ]", ds[:500]) and "См. реализацию:" not in ds:
            continue
        if ds:
            new_doc = translate_en_doc(ds, ru_map)
        else:
            new_doc = guess_docstring(
                node.name,
                is_class=isinstance(node, ast.ClassDef),
            )
        _set_lines_docstring(lines, node, new_doc)

    if path.name == "zephyr_weekly_report.py":
        _insert_section_headers(lines)
    path.write_text("".join(lines), encoding="utf-8")


if __name__ == "__main__":
    import sys

    targets = [Path(p) for p in sys.argv[1:]] if len(sys.argv) > 1 else [TARGET]
    for t in targets:
        apply(t)
        print("Updated", t)

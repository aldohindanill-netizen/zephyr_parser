#!/usr/bin/env python3
"""Заменить «См. реализацию: …» на переводы из _ru_map_data или эвристику."""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "scripts"))
from _apply_ru_docstrings import _set_lines_docstring, guess_docstring  # noqa: E402
from _ru_map_data import EN_TO_RU  # noqa: E402

PREFIX = "См. реализацию: "


def fix(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    lines = source.splitlines(keepends=True)
    nodes = [
        n
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    nodes.sort(key=lambda n: n.lineno, reverse=True)
    for node in nodes:
        ds = ast.get_docstring(node)
        if not ds or PREFIX not in ds:
            continue
        en = ds.replace(PREFIX, "", 1).strip()
        if en in EN_TO_RU:
            new_doc = EN_TO_RU[en]
        elif en.rstrip(".") in EN_TO_RU:
            new_doc = EN_TO_RU[en.rstrip(".")]
        else:
            new_doc = guess_docstring(
                node.name, is_class=isinstance(node, ast.ClassDef)
            )
        _set_lines_docstring(lines, node, new_doc)
    path.write_text("".join(lines), encoding="utf-8")


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else _REPO / "zephyr_weekly_report.py"
    fix(target)
    print("Fixed", target)

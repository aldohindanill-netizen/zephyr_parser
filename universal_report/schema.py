"""Draft JSON schema and defaults for universal reports."""

from __future__ import annotations

import re
import uuid
from datetime import date
from typing import Any

_REPORT_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def sanitize_report_date(raw: str | None) -> str:
    value = str(raw or "").strip()
    if _REPORT_DATE_RE.fullmatch(value):
        try:
            date.fromisoformat(value)
            return value
        except ValueError:
            pass
    return date.today().isoformat()


DEFAULT_DOCUMENT_LINKS: list[dict[str, str]] = [
    {
        "label": "Правила ООН с интегрированными дополнениями",
        "url": "https://wiki.navio.auto/pages/viewpage.action?pageId=312332672",
        "note": (
            "157-01 — автоматизированные системы удержания в полосе движения; "
            "79-04 — системы рулевого управления"
        ),
    }
]

DEFAULT_INFRASTRUCTURE: list[str] = [
    "Полигон ODE",
    "Полигон АДУЛЯР",
]

DEFAULT_EQUIPMENT: list[str] = [
    "Мишень в виде задней полусферы автомобиля",
    "Мишень в виде задней полусферы автомобиля на подвижной системе",
    "Мишень в виде манекена-ребёнка на подвижной системе",
    "Конусы",
    "Светофор",
    "Знаки ремонтных работ",
]


def default_sections_1_2(build_name: str = "") -> dict[str, Any]:
    label = build_name or "my-build"
    return {
        "object_description_prefix": "Объект тестирования — сборки:",
        "build_name": label,
        "document_links": [dict(link) for link in DEFAULT_DOCUMENT_LINKS],
        "infrastructure": list(DEFAULT_INFRASTRUCTURE),
        "equipment": list(DEFAULT_EQUIPMENT),
        "speed_kmh": 40,
    }


def new_draft(
    *,
    title: str = "",
    report_date: str | None = None,
    build_name: str = "",
) -> dict[str, Any]:
    today = date.today().isoformat()
    resolved_date = sanitize_report_date(report_date) if report_date else today
    resolved_build = build_name or f"build-{resolved_date}"
    resolved_title = title or f"[{resolved_build}] Отчёт тестирования"
    draft_id = uuid.uuid4().hex[:12]
    return {
        "id": draft_id,
        "meta": {
            "title": resolved_title,
            "report_date": resolved_date,
            "build_name": resolved_build,
            "folder_name": resolved_build,
            "folder_id": f"universal-{draft_id}",
        },
        "sections_1_2": default_sections_1_2(resolved_build),
        "section_3_mode": "manual",
        "zephyr_source": {
            "folder_id": "",
            "folder_name": "",
            "from_date": resolved_date,
            "to_date": resolved_date,
        },
        "cycles": [],
        "updated_at": today,
        "created_at": today,
    }


def normalize_draft(raw: dict[str, Any]) -> dict[str, Any]:
    """Fill missing keys and coerce types for API payloads."""
    draft = dict(raw)
    if not draft.get("id"):
        draft["id"] = uuid.uuid4().hex[:12]
    meta = dict(draft.get("meta") or {})
    today = date.today().isoformat()
    meta["report_date"] = sanitize_report_date(meta.get("report_date") or today)
    if not str(meta.get("build_name") or "").strip():
        meta["build_name"] = str(meta.get("folder_name") or "").strip() or f"build-{meta['report_date']}"
    if not str(meta.get("folder_name") or "").strip():
        meta["folder_name"] = meta["build_name"]
    meta.setdefault("folder_id", f"universal-{draft['id']}")
    meta.setdefault("title", f"[{meta['build_name']}] Отчёт тестирования")
    draft["meta"] = meta
    sections = dict(draft.get("sections_1_2") or {})
    defaults = default_sections_1_2(meta["build_name"])
    for key, value in defaults.items():
        sections.setdefault(key, value)
    draft["sections_1_2"] = sections
    draft.setdefault("section_3_mode", "manual")
    zephyr_source = dict(draft.get("zephyr_source") or {})
    zephyr_source.setdefault("folder_id", "")
    zephyr_source.setdefault("folder_name", "")
    zephyr_source["from_date"] = sanitize_report_date(
        zephyr_source.get("from_date") or meta["report_date"]
    )
    zephyr_source["to_date"] = sanitize_report_date(
        zephyr_source.get("to_date") or meta["report_date"]
    )
    draft["zephyr_source"] = zephyr_source
    draft.setdefault("cycles", [])
    draft.setdefault("created_at", today)
    draft["updated_at"] = today
    return draft


def draft_summary(draft: dict[str, Any]) -> dict[str, str]:
    meta = draft.get("meta") or {}
    return {
        "id": str(draft.get("id") or ""),
        "title": str(meta.get("title") or ""),
        "report_date": str(meta.get("report_date") or ""),
        "build_name": str(meta.get("build_name") or ""),
        "updated_at": str(draft.get("updated_at") or ""),
        "section_3_mode": str(draft.get("section_3_mode") or "manual"),
        "cycle_count": str(len(draft.get("cycles") or [])),
    }

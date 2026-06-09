"""Build universal report HTML/wiki from draft JSON using daily render pipeline."""

from __future__ import annotations

import html
import os
import re
from typing import Any
from urllib.parse import urlparse

from zephyr_weekly_report import (
    _daily_aggregate_case_status_counts,
    _write_daily_pie_png,
    _write_text_if_changed,
    render_daily_html_report,
    render_daily_wiki_report,
    slugify,
)

from universal_report.schema import normalize_draft

PreparedDraft = tuple[dict[str, Any], dict[str, Any], dict[str, Any]]


def _prepare_draft(draft: dict[str, Any]) -> PreparedDraft:
    normalized = normalize_draft(draft)
    meta = normalized["meta"]
    cycles = draft_cycles_to_render_dict(normalized)
    return normalized, meta, cycles


def _wiki_escape(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "\\\\")


def _safe_http_url(raw_url: str) -> str | None:
    url = str(raw_url or "").strip()
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    return url


def draft_to_preamble_html(draft: dict[str, Any]) -> str:
    sections = draft.get("sections_1_2") or {}
    build_name = html.escape(str(sections.get("build_name") or ""))
    prefix = html.escape(str(sections.get("object_description_prefix") or "Объект тестирования — сборки:"))
    links_html: list[str] = []
    for link in sections.get("document_links") or []:
        label = html.escape(str(link.get("label") or ""))
        safe_url = _safe_http_url(str(link.get("url") or ""))
        if not label or not safe_url:
            continue
        url = html.escape(safe_url, quote=True)
        note = str(link.get("note") or "").strip()
        part = f'<a href="{url}" target="_blank" rel="noopener">{label}</a>'
        if note:
            part += f" ({html.escape(note)})"
        links_html.append(part)
    infra_items = "".join(
        f"<li>{html.escape(str(item))}</li>"
        for item in (sections.get("infrastructure") or [])
        if str(item).strip()
    )
    equip_items = "".join(
        f"<li>{html.escape(str(item))}</li>"
        for item in (sections.get("equipment") or [])
        if str(item).strip()
    )
    speed = sections.get("speed_kmh", 40)
    body = (
        "<h2 id='sec-object'><strong>1. Объект тестирования</strong></h2>"
        "<h3 id='sec-object-desc'><strong>1.1. Описание объекта тестирования</strong></h3>"
        f"<p>{prefix} <strong>{build_name}</strong>.</p>"
        "<p>Ссылка на используемые документы: "
        + ("; ".join(links_html) if links_html else "—")
        + "</p>"
        "<h2 id='sec-environment'><strong>2. Условия окружения для проведения испытаний</strong></h2>"
        "<h3 id='sec-infra'><strong>2.1. Инфраструктура</strong></h3>"
        f"<ul>{infra_items}</ul>"
        "<h3 id='sec-equipment'><strong>2.2. Оборудование</strong></h3>"
        "<p>Для испытаний необходимо следующее оборудование:</p>"
        f"<ul>{equip_items}</ul>"
        f"<p id='sec-speed'><strong>На начало всех тестов скорость ВАТС {html.escape(str(speed))} км/ч</strong></p>"
    )
    return f"<div class='report-preamble'>{body}</div>"


def draft_to_preamble_wiki(draft: dict[str, Any]) -> str:
    sections = draft.get("sections_1_2") or {}
    build_name = _wiki_escape(str(sections.get("build_name") or ""))
    prefix = _wiki_escape(str(sections.get("object_description_prefix") or "Объект тестирования — сборки:"))
    link_parts: list[str] = []
    for link in sections.get("document_links") or []:
        label = _wiki_escape(str(link.get("label") or ""))
        safe_url = _safe_http_url(str(link.get("url") or ""))
        note = str(link.get("note") or "").strip()
        if label and safe_url:
            part = f"[{label}|{safe_url}]"
            if note:
                part += f" ({_wiki_escape(note)})"
            link_parts.append(part)
    infra_lines = "\n".join(
        f"* {_wiki_escape(str(item))}"
        for item in (sections.get("infrastructure") or [])
        if str(item).strip()
    )
    equip_lines = "\n".join(
        f"* {_wiki_escape(str(item))}"
        for item in (sections.get("equipment") or [])
        if str(item).strip()
    )
    speed = sections.get("speed_kmh", 40)
    return "\n".join(
        [
            "{anchor:sec_object}",
            "h2. *1. Объект тестирования*",
            "h3. *1.1. Описание объекта тестирования*",
            f"{prefix} *{build_name}*.",
            "Ссылка на используемые документы: " + ("; ".join(link_parts) if link_parts else "—"),
            "{anchor:sec_environment}",
            "h2. *2. Условия окружения для проведения испытаний*",
            "h3. *2.1. Инфраструктура*",
            infra_lines,
            "h3. *2.2. Оборудование*",
            "Для испытаний необходимо следующее оборудование:",
            equip_lines,
            f"*На начало всех тестов скорость ВАТС {_wiki_escape(str(speed))} км/ч*",
        ]
    )


def _unique_key(base: str, used: set[str]) -> str:
    key = base or "item"
    if key not in used:
        used.add(key)
        return key
    suffix = 2
    while f"{key}-{suffix}" in used:
        suffix += 1
    unique = f"{key}-{suffix}"
    used.add(unique)
    return unique


def draft_cycles_to_render_dict(draft: dict[str, Any]) -> dict[str, Any]:
    cycles_out: dict[str, Any] = {}
    used_cycle_ids: set[str] = set()
    for idx, cycle in enumerate(draft.get("cycles") or []):
        cycle_id = _unique_key(
            str(cycle.get("cycle_id") or cycle.get("cycle_key") or f"cycle-{idx + 1}"),
            used_cycle_ids,
        )
        cases_out: dict[str, Any] = {}
        used_case_keys: set[str] = set()
        for case_idx, case in enumerate(cycle.get("cases") or []):
            case_key = _unique_key(
                str(
                    case.get("test_case_key")
                    or case.get("test_case_name")
                    or f"case-{case_idx + 1}"
                ),
                used_case_keys,
            )
            cases_out[case_key] = {
                "test_case_key": case_key,
                "test_case_name": str(case.get("test_case_name") or ""),
                "result": str(case.get("result") or ""),
                "execution_date": str(case.get("execution_date") or ""),
                "actual_start_date": str(case.get("actual_start_date") or ""),
                "case_iteration_key": str(case.get("case_iteration_key") or ""),
                "comment": str(case.get("comment") or ""),
                "objective": str(case.get("objective") or ""),
                "tasks": str(case.get("tasks") or ""),
                "logs_source_text": str(case.get("logs_source_text") or case.get("comment") or ""),
            }
        cycles_out[cycle_id] = {
            "cycle_id": cycle_id,
            "cycle_key": str(cycle.get("cycle_key") or ""),
            "cycle_name": str(cycle.get("cycle_name") or ""),
            "cycle_objective": str(cycle.get("cycle_objective") or ""),
            "cases": cases_out,
        }
    return cycles_out


def render_cycles_to_draft_cycles(cycles: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for cycle_id, cycle in cycles.items():
        cases: list[dict[str, Any]] = []
        for case_key, case in (cycle.get("cases") or {}).items():
            cases.append(
                {
                    "test_case_key": case.get("test_case_key") or case_key,
                    "test_case_name": case.get("test_case_name") or "",
                    "result": case.get("result") or "",
                    "execution_date": case.get("execution_date") or "",
                    "actual_start_date": case.get("actual_start_date") or "",
                    "case_iteration_key": case.get("case_iteration_key") or "",
                    "comment": case.get("comment") or "",
                    "objective": case.get("objective") or "",
                    "tasks": case.get("tasks") or "",
                    "logs_source_text": case.get("logs_source_text") or "",
                }
            )
        result.append(
            {
                "cycle_id": str(cycle.get("cycle_id") or cycle_id),
                "cycle_key": cycle.get("cycle_key") or "",
                "cycle_name": cycle.get("cycle_name") or "",
                "cycle_objective": cycle.get("cycle_objective") or "",
                "cases": cases,
            }
        )
    return result


def build_universal_report_base_name(draft: dict[str, Any]) -> str:
    draft = normalize_draft(draft)
    meta = draft["meta"]
    build_slug = slugify(str(meta.get("build_name") or "build"))
    report_date = re.sub(r"[^\d-]", "", str(meta.get("report_date") or "")) or "unknown-date"
    draft_id = str(draft.get("id") or "draft")
    safe_id = re.sub(r"[^\w.-]+", "_", draft_id) or "draft"
    return f"universal-{build_slug}_{report_date}_{safe_id}"


def build_universal_html(draft: dict[str, Any]) -> str:
    normalized, meta, cycles = _prepare_draft(draft)
    return render_daily_html_report(
        str(meta["folder_name"]),
        cycles,
        folder_id=str(meta["folder_id"]),
        preamble_html=draft_to_preamble_html(normalized),
        document_title=str(meta["title"]),
        conclusion_score_label=str(meta["build_name"]),
    )


def build_universal_wiki(draft: dict[str, Any]) -> str:
    normalized, meta, cycles = _prepare_draft(draft)
    return render_daily_wiki_report(
        str(meta["folder_name"]),
        cycles,
        folder_id=str(meta["folder_id"]),
        preamble_wiki=draft_to_preamble_wiki(normalized),
        conclusion_score_label=str(meta["build_name"]),
    )


def write_universal_reports(
    draft: dict[str, Any],
    output_dir: str,
    *,
    formats: set[str] | None = None,
) -> list[str]:
    normalized, meta, cycles = _prepare_draft(draft)
    formats = formats or {"html", "wiki"}
    os.makedirs(output_dir, exist_ok=True)
    base_name = build_universal_report_base_name(normalized)
    written: list[str] = []
    if "html" in formats:
        html_path = os.path.join(output_dir, f"{base_name}.html")
        body = render_daily_html_report(
            str(meta["folder_name"]),
            cycles,
            folder_id=str(meta["folder_id"]),
            preamble_html=draft_to_preamble_html(normalized),
            document_title=str(meta["title"]),
            conclusion_score_label=str(meta["build_name"]),
        )
        _write_text_if_changed(html_path, body)
        written.append(html_path)
    if "wiki" in formats:
        wiki_path = os.path.join(output_dir, f"{base_name}.confluence.txt")
        chart_path = os.path.join(output_dir, f"{base_name}_conclusion_pie.png")
        status_counts = _daily_aggregate_case_status_counts(cycles)
        if _write_daily_pie_png(chart_path, status_counts):
            written.append(chart_path)
        body = render_daily_wiki_report(
            str(meta["folder_name"]),
            cycles,
            folder_id=str(meta["folder_id"]),
            preamble_wiki=draft_to_preamble_wiki(normalized),
            conclusion_score_label=str(meta["build_name"]),
        )
        _write_text_if_changed(wiki_path, body)
        written.append(wiki_path)
    return written

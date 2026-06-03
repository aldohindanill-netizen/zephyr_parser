"""Дополняемый JSONL-аудит экспортов, публикаций в Confluence и жизненного цикла запуска.

События пишутся в `ZEPHYR_AUDIT_LOG` (по умолчанию reports/audit/audit.jsonl).
Управление: ZEPHYR_AUDIT_ENABLED, ZEPHYR_AUDIT_ACTOR, ZEPHYR_AUDIT_RETENTION_DAYS.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone

UTC = timezone.utc
from pathlib import Path
from typing import Any

_RUN_ID: str | None = None


def _parse_bool_env(value: str | None, default: bool = True) -> bool:
    """Разобрать булево из строки env (1/true/yes/on)."""
    if value is None or not str(value).strip():
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def audit_enabled() -> bool:
    """Включён ли аудит (ZEPHYR_AUDIT_ENABLED, по умолчанию true)."""
    return _parse_bool_env(os.getenv("ZEPHYR_AUDIT_ENABLED"), default=True)


def audit_reason() -> str:
    """Необязательная причина/контекст запуска (ZEPHYR_AUDIT_REASON)."""
    return (os.getenv("ZEPHYR_AUDIT_REASON") or "").strip()


def audit_actor() -> str:
    """Идентификатор субъекта: ZEPHYR_AUDIT_ACTOR, иначе USERNAME/USER, иначе zephyr-service."""
    explicit = (os.getenv("ZEPHYR_AUDIT_ACTOR") or "").strip()
    if explicit:
        return explicit
    for key in ("USERNAME", "USER"):
        val = (os.getenv(key) or "").strip()
        if val:
            return val
    return "zephyr-service"


def audit_log_path() -> Path:
    """Абсолютный путь к файлу аудита (относительные пути — от корня репозитория)."""
    raw = (os.getenv("ZEPHYR_AUDIT_LOG") or "reports/audit/audit.jsonl").strip()
    path = Path(raw)
    if not path.is_absolute():
        root = Path(__file__).resolve().parent
        path = root / path
    return path


def audit_retention_days() -> int:
    """Срок хранения *.jsonl в каталоге аудита (дней); 0 — не удалять по возрасту."""
    raw = (os.getenv("ZEPHYR_AUDIT_RETENTION_DAYS") or "186").strip()
    if raw.isdigit():
        return int(raw)
    return 186


def current_run_id() -> str:
    """UUID текущего запуска: из ZEPHYR_AUDIT_RUN_ID или сгенерированный при первом обращении."""
    global _RUN_ID
    if _RUN_ID is None:
        _RUN_ID = (os.getenv("ZEPHYR_AUDIT_RUN_ID") or "").strip() or str(uuid.uuid4())
    return _RUN_ID


def reset_run_id() -> None:
    """Сбросить run_id (вызывается в начале нового run_start)."""
    global _RUN_ID
    _RUN_ID = None


def prune_old_audit_logs() -> None:
    """Удалить устаревшие *.jsonl в каталоге аудита по mtime и ZEPHYR_AUDIT_RETENTION_DAYS."""
    days = audit_retention_days()
    if days <= 0:
        return
    log_dir = audit_log_path().parent
    if not log_dir.is_dir():
        return
    cutoff = datetime.now(UTC).timestamp() - days * 86400
    for path in log_dir.glob("*.jsonl"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except OSError:
            pass


def audit_event(
    operation: str,
    *,
    result: str = "success",
    **fields: Any,
) -> None:
    """Записать одну строку JSONL с операцией, run_id, actor и дополнительными полями."""
    if not audit_enabled():
        return
    record: dict[str, Any] = {
        "timestamp_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_id": current_run_id(),
        "actor": audit_actor(),
        "operation": operation,
        "result": result,
    }
    reason = audit_reason()
    if reason:
        record["reason"] = reason
    record.update(fields)
    path = audit_log_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as exc:
        print(f"audit log write failed: {exc}", file=sys.stderr)


def audit_run_start(**extra: Any) -> None:
    """Начало пайплайна: новый run_id, prune старых логов, событие run_start."""
    reset_run_id()
    prune_old_audit_logs()
    audit_event("run_start", **extra)


def audit_run_finish(exit_code: int, **extra: Any) -> None:
    """Завершение пайплайна: result success/failure по exit_code."""
    result = "success" if exit_code == 0 else "failure"
    audit_event("run_finish", result=result, exit_code=exit_code, **extra)


def audit_export_file(path: str, *, record_count: int | None = None, kind: str = "file") -> None:
    """Аудит записи артефакта на диск (CSV, HTML и т.д.)."""
    fields: dict[str, Any] = {"path": path, "data_scope": kind}
    if record_count is not None:
        fields["record_count"] = record_count
    audit_event("export_file", **fields)


def audit_publish_confluence(
    *,
    title: str,
    page_id: str,
    action: str,
    path: str,
    result: str = "success",
) -> None:
    """Аудит публикации или обновления страницы Confluence."""
    audit_event(
        "publish_confluence",
        result=result,
        title=title,
        page_id=page_id,
        action=action,
        path=path,
        data_scope="confluence_page",
    )


def audit_api_write(
    *,
    endpoint: str,
    method: str,
    result: str = "success",
    detail: str = "",
) -> None:
    """Аудит исходящего изменяющего HTTP-запроса к API."""
    fields: dict[str, Any] = {"endpoint": endpoint, "method": method}
    if detail:
        fields["detail"] = detail
    audit_event("api_write", result=result, **fields)


def audit_integration(
    *,
    system: str,
    operation: str,
    result: str = "success",
    detail: str = "",
) -> None:
    """Аудит вызова внешней интеграции (не обязательно HTTP write)."""
    fields: dict[str, Any] = {"system": system, "integration_operation": operation}
    if detail:
        fields["detail"] = detail
    audit_event("integration_call", result=result, **fields)


def audit_embeddings_start(**extra: Any) -> None:
    """Начало отдельного job вычисления embeddings для дубликатов багов."""
    audit_event("embeddings_start", **extra)


def audit_embeddings_finish(exit_code: int, **extra: Any) -> None:
    """Завершение job embeddings."""
    result = "success" if exit_code == 0 else "failure"
    audit_event("embeddings_finish", result=result, exit_code=exit_code, **extra)

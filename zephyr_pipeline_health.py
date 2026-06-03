"""HTML-дашборд pipeline health (`reports/pipeline_health.html`) для on-call QA.

Анализирует audit.jsonl, lock-файлы, логи zephyr/embeddings; пороги — через ZEPHYR_* env.
"""

from __future__ import annotations

import html
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

UTC = timezone.utc
_REPO_ROOT = Path(__file__).resolve().parent
_AUDIT_SCAN_LINES = 500
_DEFAULT_EMBEDDINGS_AUDIT_SCAN_LINES = 10_000
_DEFAULT_EMBEDDINGS_OVERDUE_HOURS = 36.0
_DEFAULT_EMBEDDINGS_INTERRUPTED_HOURS = 2.0
_STALE_LOCK_HINT = (
    "Критично: возможно зависший прогон. Откройте последний logs/zephyr_*.log и audit.jsonl; "
    "удаление lock — только по runbook после подтверждения."
)


def _pipeline_version() -> str:
    """Вспомогательная функция: pipeline version."""
    path = _REPO_ROOT / "PIPELINE_VERSION"
    try:
        return path.read_text(encoding="utf-8").strip() or "unknown"
    except OSError:
        return "unknown"


def _reports_dir() -> Path:
    """Вспомогательная функция: reports dir."""
    raw = (os.getenv("ZEPHYR_DAILY_READABLE_DIR") or "reports/daily_readable").strip()
    health_dir = (os.getenv("ZEPHYR_HEALTH_REPORT_DIR") or "").strip()
    if health_dir:
        path = Path(health_dir)
        if not path.is_absolute():
            path = _REPO_ROOT / path
        return path.parent if path.suffix else path
    readable = Path(raw)
    if not readable.is_absolute():
        readable = _REPO_ROOT / readable
    return readable.parent


def health_html_path() -> Path:
    """Вспомогательная функция: health html path."""
    explicit = (os.getenv("ZEPHYR_PIPELINE_HEALTH_HTML") or "").strip()
    if explicit:
        path = Path(explicit)
        if not path.is_absolute():
            path = _REPO_ROOT / path
        return path
    return _reports_dir() / "pipeline_health.html"


def run_lock_path() -> Path:
    """Вспомогательная функция: run lock path."""
    raw = (os.getenv("ZEPHYR_RUN_LOCK_FILE") or "reports/.zephyr_weekly_report.lock").strip()
    path = Path(raw)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    return path


def scheduled_lock_path() -> Path:
    """Вспомогательная функция: scheduled lock path."""
    return _REPO_ROOT / "reports" / ".zephyr_scheduled.lock"


def _env_float(name: str, default: float, *, minimum: float = 0.25) -> float:
    """Вспомогательная функция: env float."""
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


def embeddings_overdue_hours() -> float:
    """Вспомогательная функция: embeddings overdue hours."""
    return _env_float(
        "ZEPHYR_HEALTH_EMBEDDINGS_OVERDUE_HOURS",
        _DEFAULT_EMBEDDINGS_OVERDUE_HOURS,
        minimum=1.0,
    )


def embeddings_interrupted_hours() -> float:
    """Вспомогательная функция: embeddings interrupted hours."""
    return _env_float(
        "ZEPHYR_HEALTH_EMBEDDINGS_INTERRUPTED_HOURS",
        _DEFAULT_EMBEDDINGS_INTERRUPTED_HOURS,
        minimum=0.5,
    )


def embeddings_audit_scan_lines() -> int:
    """Вспомогательная функция: embeddings audit scan lines."""
    raw = (os.getenv("ZEPHYR_HEALTH_EMBEDDINGS_AUDIT_SCAN_LINES") or "").strip()
    if raw.isdigit():
        return max(500, int(raw))
    return _DEFAULT_EMBEDDINGS_AUDIT_SCAN_LINES


def lock_stale_minutes() -> float:
    """Вспомогательная функция: lock stale minutes."""
    raw = (os.getenv("ZEPHYR_HEALTH_LOCK_STALE_MINUTES") or "").strip()
    if raw:
        try:
            minutes = float(raw)
            if minutes >= 1:
                return minutes
        except ValueError:
            pass
    timeout_raw = (os.getenv("ZEPHYR_RUN_TIMEOUT_MINUTES") or "90").strip()
    try:
        minutes = float(timeout_raw)
        if minutes >= 1:
            return minutes
    except ValueError:
        pass
    return 90.0


def _log_dir() -> Path:
    """Вспомогательная функция: log dir."""
    raw = (os.getenv("ZEPHYR_LOG_DIR") or "logs").strip() or "logs"
    path = Path(raw)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    return path


def _reports_logs_dir() -> Path:
    """Вспомогательная функция: reports logs dir."""
    return _reports_dir() / "logs"


def _audit_log_path() -> Path:
    """Вспомогательная функция: audit log path."""
    raw = (os.getenv("ZEPHYR_AUDIT_LOG") or "reports/audit/audit.jsonl").strip()
    path = Path(raw)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    return path


def _read_audit_tail(limit: int = _AUDIT_SCAN_LINES) -> list[dict[str, Any]]:
    """Вспомогательная функция: read audit tail."""
    path = _audit_log_path()
    if not path.is_file():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    records: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    records.reverse()
    return records


def _parse_utc_timestamp(ts: str) -> datetime | None:
    """Разобрать: utc timestamp."""
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


def _pair_start_finish(
    start: dict[str, Any] | None,
    finish: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Вспомогательная функция: pair start finish."""
    if not start and not finish:
        return None
    if start and finish:
        start_ts = str(start.get("timestamp_utc") or "")
        finish_ts = str(finish.get("timestamp_utc") or "")
        if start_ts > finish_ts:
            return {"start": start, "finish": None}
    return {"start": start, "finish": finish}


def _last_run_from_audit(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Вспомогательная функция: last run from audit."""
    start: dict[str, Any] | None = None
    finish: dict[str, Any] | None = None
    for rec in records:
        op = rec.get("operation")
        if op == "run_start" and start is None:
            start = rec
        if op == "run_finish" and finish is None:
            finish = rec
    return _pair_start_finish(start, finish)


def _last_embeddings_from_audit_scan() -> dict[str, Any] | None:
    """Вспомогательная функция: last embeddings from audit scan."""
    ops = {"embeddings_start", "embeddings_finish"}
    max_lines = embeddings_audit_scan_lines()
    finish: dict[str, Any] | None = None
    start: dict[str, Any] | None = None
    path = _audit_log_path()
    if not path.is_file():
        return None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    scanned = 0
    for line in reversed(lines):
        scanned += 1
        if scanned > max_lines:
            break
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        op = rec.get("operation")
        if op == "embeddings_finish" and finish is None:
            finish = rec
        if op == "embeddings_start" and start is None:
            start = rec
        if finish and start:
            break
    return _pair_start_finish(start, finish)


def _last_embeddings_from_audit(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Вспомогательная функция: last embeddings from audit."""
    finish: dict[str, Any] | None = None
    start: dict[str, Any] | None = None
    for rec in records:
        op = rec.get("operation")
        if op == "embeddings_finish" and finish is None:
            finish = rec
        if op == "embeddings_start" and start is None:
            start = rec
    return _pair_start_finish(start, finish)


def _duration_seconds(start_ts: str, finish_ts: str) -> int | None:
    """Вспомогательная функция: duration seconds."""
    start = _parse_utc_timestamp(start_ts)
    finish = _parse_utc_timestamp(finish_ts)
    if start is None or finish is None:
        return None
    return max(0, int((finish - start).total_seconds()))


def _latest_zephyr_log() -> tuple[str, str] | None:
    """Вспомогательная функция: latest zephyr log."""
    log_dir = _log_dir()
    if not log_dir.is_dir():
        return None
    candidates = sorted(log_dir.glob("zephyr_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return None
    latest = candidates[0]
    mtime = datetime.fromtimestamp(latest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    return str(latest), mtime


def _latest_embeddings_log() -> tuple[str, str] | None:
    """Вспомогательная функция: latest embeddings log."""
    log_dir = _reports_logs_dir()
    if not log_dir.is_dir():
        return None
    candidates = sorted(
        log_dir.glob("embeddings_*.log"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not candidates:
        return None
    latest = candidates[0]
    mtime = datetime.fromtimestamp(latest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    return str(latest), mtime


def _lock_status_for_path(path: Path, *, stale_minutes: float, label: str) -> tuple[str, str]:
    """Вспомогательная функция: lock status for path."""
    if not path.exists():
        return "free", f"{label}: lock file absent"
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        age_min = (datetime.now() - mtime).total_seconds() / 60.0
        if age_min > stale_minutes:
            return (
                "stale",
                f"{label}: lock present {age_min:.0f} min (>{stale_minutes:.0f} min). {_STALE_LOCK_HINT}",
            )
        if age_min > 0.5:
            return "held", f"{label}: lock present {age_min:.0f} min"
        return "held", f"{label}: lock present"
    except OSError:
        return "unknown", f"{label}: lock file unreadable"


def _lock_status() -> tuple[str, str, str]:
    """Вспомогательная функция: lock status."""
    stale_min = lock_stale_minutes()
    py_state, py_detail = _lock_status_for_path(
        run_lock_path(), stale_minutes=stale_min, label="Python"
    )
    sched_state, sched_detail = _lock_status_for_path(
        scheduled_lock_path(), stale_minutes=stale_min, label="Scheduler"
    )
    severity = {"free": 0, "held": 1, "unknown": 2, "stale": 3}
    combined = py_state if severity.get(py_state, 0) >= severity.get(sched_state, 0) else sched_state
    detail = f"{py_detail}; {sched_detail}"
    lock_class = {"free": "ok", "held": "warn", "stale": "err", "unknown": "warn"}.get(
        combined, "warn"
    )
    return combined, detail, lock_class


def _hours_since_utc(ts: str) -> float | None:
    """Вспомогательная функция: hours since utc."""
    parsed = _parse_utc_timestamp(ts)
    if parsed is None:
        return None
    return (datetime.now(UTC) - parsed).total_seconds() / 3600.0


def _embeddings_summary(
    emb: dict[str, Any] | None,
    log_info: tuple[str, str] | None,
) -> tuple[str, str, str]:
    """См. реализацию: Return (status_label, detail_text, css_class)."""
    finish_rec = (emb or {}).get("finish") or {}
    start_rec = (emb or {}).get("start") or {}

    if finish_rec:
        status = str(finish_rec.get("result") or "unknown")
        code = finish_rec.get("exit_code")
        exit_s = str(code) if code is not None else "—"
        ts = str(finish_rec.get("timestamp_utc") or "—")
        detail = f"embeddings_finish UTC {ts}, exit {exit_s}"
        if status == "success":
            hours = _hours_since_utc(ts)
            overdue_h = embeddings_overdue_hours()
            if hours is not None and hours > overdue_h:
                return (
                    "просрочено",
                    f"{detail}; последний успех {overdue_h:g}+ ч назад",
                    "err",
                )
            return "success", detail, "ok"
        return "failure", detail, "err"

    if start_rec and not finish_rec:
        ts = str(start_rec.get("timestamp_utc") or "—")
        hours = _hours_since_utc(ts)
        detail = f"embeddings_start UTC {ts}, finish не найден"
        interrupted_h = embeddings_interrupted_hours()
        if hours is not None and hours > interrupted_h:
            return "прервано", f"{detail} (>{interrupted_h:g} ч без finish)", "err"
        return "running", detail, "warn"

    if log_info:
        detail = f"нет embeddings_finish в audit; лог {log_info[0]} (mtime {log_info[1]}, локальное время)"
        return "нет данных", detail, "warn"

    return "нет данных", "Nightly embeddings ещё не запускались (нет audit и лога)", "warn"


def _is_warn_record(rec: dict[str, Any]) -> bool:
    """Вспомогательная функция: is warn record."""
    if _is_error_record(rec):
        return False
    op = rec.get("operation")
    if op == "publish_confluence" and rec.get("result") not in (None, "success"):
        return True
    return False


def _is_error_record(rec: dict[str, Any]) -> bool:
    """Вспомогательная функция: is error record."""
    op = rec.get("operation")
    if op in ("run_finish", "embeddings_finish"):
        code = rec.get("exit_code")
        if code is not None and int(code) != 0:
            return True
    if op == "integration_call" and rec.get("result") == "error":
        return True
    if rec.get("result") not in (None, "success"):
        if op == "publish_confluence":
            return False
        return True
    return False


def _audit_row_class(rec: dict[str, Any]) -> str:
    """Вспомогательная функция: audit row class."""
    if _is_error_record(rec):
        return "err"
    if _is_warn_record(rec):
        return "warn"
    return ""


def write_pipeline_health_html(*, exit_code: int | None = None) -> Path:
    """См. реализацию: Write pipeline health HTML; returns output path."""
    out_path = health_html_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    audit_tail = _read_audit_tail()
    display_tail = audit_tail[:20]
    last_run = _last_run_from_audit(audit_tail)
    last_emb = _last_embeddings_from_audit_scan()
    lock_state, lock_detail, lock_class = _lock_status()
    log_info = _latest_zephyr_log()
    emb_log_info = _latest_embeddings_log()
    generated_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    version = _pipeline_version()

    run_status = "unknown"
    run_start_ts = "—"
    run_finish_ts = "—"
    run_exit = "—"
    duration_str = "—"

    if last_run:
        start_rec = last_run.get("start") or {}
        finish_rec = last_run.get("finish") or {}
        run_start_ts = str(start_rec.get("timestamp_utc") or "—")
        run_finish_ts = str(finish_rec.get("timestamp_utc") or "—")
        if finish_rec:
            code = finish_rec.get("exit_code")
            run_exit = str(code) if code is not None else "—"
            run_status = str(finish_rec.get("result") or "unknown")
            if run_start_ts != "—" and run_finish_ts != "—":
                secs = _duration_seconds(run_start_ts, run_finish_ts)
                if secs is not None:
                    duration_str = f"{secs // 60} min {secs % 60} s"
        elif start_rec:
            run_status = "running"
            run_exit = "—"

    if exit_code is not None:
        run_exit = str(exit_code)
        run_status = "success" if exit_code == 0 else "failure"

    status_class = "ok" if run_status == "success" else ("warn" if run_status == "running" else "err")

    emb_status, emb_detail, emb_class = _embeddings_summary(last_emb, emb_log_info)
    emb_log_path_html = "—"
    emb_log_mtime_html = "—"
    if emb_log_info:
        emb_log_path_html = html.escape(emb_log_info[0])
        emb_log_mtime_html = html.escape(emb_log_info[1])

    rows_html: list[str] = []
    for rec in display_tail:
        row_class = _audit_row_class(rec)
        op = html.escape(str(rec.get("operation", "")))
        result = html.escape(str(rec.get("result", "")))
        ts = html.escape(str(rec.get("timestamp_utc", "")))
        detail_parts = []
        for key in ("exit_code", "title", "path", "detail"):
            if key in rec and rec[key] not in (None, ""):
                detail_parts.append(f"{key}={rec[key]}")
        detail = html.escape(", ".join(detail_parts))
        rows_html.append(
            f'<tr class="{row_class}"><td>{ts}</td><td>{op}</td><td>{result}</td>'
            f'<td>{detail}</td></tr>'
        )

    log_path_html = "—"
    log_mtime_html = "—"
    if log_info:
        log_path_html = html.escape(log_info[0])
        log_mtime_html = html.escape(log_info[1])

    stale_min = lock_stale_minutes()

    body = f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <title>Zephyr pipeline health</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 1.5rem; max-width: 960px; }}
    h1 {{ font-size: 1.25rem; }}
    h2 {{ font-size: 1.05rem; margin-top: 1.5rem; }}
    .ok {{ color: #0a0; }} .warn {{ color: #a60; }} .err {{ color: #c00; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 0.9rem; }}
    th, td {{ border: 1px solid #ccc; padding: 0.35rem 0.5rem; text-align: left; }}
    th {{ background: #f4f4f4; }}
    tr.err td {{ background: #fff0f0; }}
    tr.warn td {{ background: #fff8e6; }}
    dl {{ display: grid; grid-template-columns: 12rem 1fr; gap: 0.25rem 1rem; }}
    dt {{ font-weight: 600; }}
    .hint {{ font-size: 0.85rem; color: #555; margin-top: 0.25rem; }}
  </style>
</head>
<body>
  <h1>Zephyr pipeline health</h1>
  <p>Для дежурного QA. Сгенерировано: {html.escape(generated_local)} (локальное время хоста).</p>

  <h2>Основной пайплайн (каждые 30 мин)</h2>
  <dl>
    <dt>Версия</dt><dd>{html.escape(version)}</dd>
    <dt>Последний прогон</dt><dd class="{status_class}">{html.escape(run_status)} (exit {html.escape(run_exit)})</dd>
    <dt>run_start (UTC)</dt><dd>{html.escape(run_start_ts)}</dd>
    <dt>run_finish (UTC)</dt><dd>{html.escape(run_finish_ts)}</dd>
    <dt>Длительность</dt><dd>{html.escape(duration_str)}</dd>
    <dt>Lock</dt><dd class="{lock_class}">{html.escape(lock_state)} — {html.escape(lock_detail)}</dd>
    <dt class="hint">Порог stale</dt><dd class="hint">{stale_min:g} мин (ZEPHYR_HEALTH_LOCK_STALE_MINUTES или ZEPHYR_RUN_TIMEOUT_MINUTES)</dd>
    <dt>Последний лог</dt><dd><code>{log_path_html}</code> (mtime {log_mtime_html}, локальное)</dd>
    <dt>Audit log</dt><dd><code>{html.escape(str(_audit_log_path()))}</code></dd>
  </dl>

  <h2>Nightly embeddings (13:00, локальное время хоста)</h2>
  <p class="hint">Время в audit — UTC; имя лога embeddings_YYYY-MM-DD — дата хоста.</p>
  <dl>
    <dt>Статус</dt><dd class="{emb_class}">{html.escape(emb_status)}</dd>
    <dt>Детали</dt><dd>{html.escape(emb_detail)}</dd>
    <dt>Последний лог</dt><dd><code>{emb_log_path_html}</code> (mtime {emb_log_mtime_html}, локальное)</dd>
  </dl>

  <h2>Последние события audit (20)</h2>
  <table>
    <thead><tr><th>UTC</th><th>operation</th><th>result</th><th>detail</th></tr></thead>
    <tbody>
      {''.join(rows_html) if rows_html else '<tr><td colspan="4">Нет записей audit</td></tr>'}
    </tbody>
  </table>
</body>
</html>
"""
    out_path.write_text(body, encoding="utf-8")
    return out_path


def main() -> int:
    """Вспомогательная функция: main."""
    import sys

    code: int | None = None
    if len(sys.argv) > 1:
        code = int(sys.argv[1])
    write_pipeline_health_html(exit_code=code)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

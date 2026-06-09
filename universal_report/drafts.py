"""CRUD for universal report drafts stored as JSON files."""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from universal_report.schema import draft_summary, new_draft, normalize_draft

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DRAFTS_DIR = _REPO_ROOT / "reports_local" / "universal_drafts"


def drafts_dir() -> Path:
    raw = (os.getenv("ZEPHYR_UNIVERSAL_DRAFTS_DIR") or "").strip()
    if raw:
        return Path(os.path.expanduser(raw))
    return _DEFAULT_DRAFTS_DIR


def _draft_path(draft_id: str) -> Path:
    safe = "".join(ch for ch in draft_id if ch.isalnum() or ch in "-_")
    if not safe:
        raise ValueError("Invalid draft id")
    return drafts_dir() / f"{safe}.json"


def list_drafts() -> list[dict[str, str]]:
    root = drafts_dir()
    if not root.is_dir():
        return []
    items: list[dict[str, str]] = []
    for path in sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            draft = json.loads(path.read_text(encoding="utf-8"))
            items.append(draft_summary(draft))
        except (OSError, json.JSONDecodeError):
            continue
    return items


def load_draft(draft_id: str) -> dict[str, Any]:
    path = _draft_path(draft_id)
    if not path.is_file():
        raise FileNotFoundError(f"Draft not found: {draft_id}")
    return normalize_draft(json.loads(path.read_text(encoding="utf-8")))


def save_draft(draft: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_draft(draft)
    normalized["updated_at"] = date.today().isoformat()
    root = drafts_dir()
    root.mkdir(parents=True, exist_ok=True)
    path = _draft_path(str(normalized["id"]))
    path.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return normalized


def create_draft(**kwargs: Any) -> dict[str, Any]:
    return save_draft(new_draft(**kwargs))


def delete_draft(draft_id: str) -> None:
    path = _draft_path(draft_id)
    if path.is_file():
        path.unlink()

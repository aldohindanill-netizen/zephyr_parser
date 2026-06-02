"""Load repo env layers shared by launchers and scripts."""

from __future__ import annotations

import os
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
_dotenv_cache: dict[str, str] | None = None
_dotenv_secrets_cache: dict[str, str] | None = None
_dotenv_local_cache: dict[str, str] | None = None


def _parse_dotenv_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError:
        return out
    for raw_line in text.splitlines():
        line = raw_line.strip().replace("\r", "")
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
            if not line:
                continue
        if "=" not in line:
            continue
        name, _, value = line.partition("=")
        name = name.strip()
        if not name:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        out[name] = value
    return out


def get_repo_dotenv_parsed() -> dict[str, str]:
    global _dotenv_cache
    if _dotenv_cache is None:
        _dotenv_cache = _parse_dotenv_file(_REPO_ROOT / ".env")
    return _dotenv_cache


def get_repo_dotenv_secrets_parsed() -> dict[str, str]:
    global _dotenv_secrets_cache
    if _dotenv_secrets_cache is None:
        _dotenv_secrets_cache = _parse_dotenv_file(_REPO_ROOT / ".env.secrets")
    return _dotenv_secrets_cache


def get_repo_dotenv_local_parsed() -> dict[str, str]:
    global _dotenv_local_cache
    if _dotenv_local_cache is None:
        _dotenv_local_cache = _parse_dotenv_file(_REPO_ROOT / ".env.local")
    return _dotenv_local_cache


def load_repo_env(*, overlay_local: bool = False) -> None:
    """Fill os.environ from .env layers with optional local overrides."""
    preexisting = set(os.environ.keys())
    for name, value in get_repo_dotenv_parsed().items():
        if name not in os.environ:
            os.environ[name] = value
    for name, value in get_repo_dotenv_secrets_parsed().items():
        if name not in preexisting:
            os.environ[name] = value
    if overlay_local:
        for name, value in get_repo_dotenv_local_parsed().items():
            os.environ[name] = value


def use_local_env_requested() -> bool:
    raw = (os.getenv("ZEPHYR_USE_LOCAL_ENV") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def load_repo_env_for_scripts(*, use_local_env: bool | None = None) -> None:
    overlay = use_local_env if use_local_env is not None else use_local_env_requested()
    load_repo_env(overlay_local=overlay)

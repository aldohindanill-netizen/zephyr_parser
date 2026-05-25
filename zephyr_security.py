"""Security helpers: TLS, log redaction, token policy, input validation."""

from __future__ import annotations

import json
import os
import re
import ssl
import sys
import urllib.request
import warnings
from typing import Any

_SENSITIVE_HEADER_NAMES = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "x-atlassian-token",
    }
)
_TOKEN_IN_TEXT_RE = re.compile(
    r"(Bearer\s+|Basic\s+)[A-Za-z0-9._~+/=-]{8,}",
    re.IGNORECASE,
)
_MAX_ERROR_BODY_LEN = 500


def _parse_bool_env(value: str | None, default: bool = False) -> bool:
    if value is None or not str(value).strip():
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def build_ssl_context() -> ssl.SSLContext:
    """TLS context for outbound HTTPS.

    By default uses OS/Python defaults (no ``minimum_version`` override) so
    corporate Jira/Zephyr endpoints keep working. Set ``ZEPHYR_SSL_MIN_VERSION``
    to ``1.2`` or ``1.3`` only when the server is known to support it.
    """
    ctx = ssl.create_default_context()
    raw = (os.getenv("ZEPHYR_SSL_MIN_VERSION") or "").strip().lower()
    if not raw or not hasattr(ssl, "TLSVersion"):
        return ctx
    version_map = {
        "1.2": ssl.TLSVersion.TLSv1_2,
        "1.3": ssl.TLSVersion.TLSv1_3,
        "tls1.2": ssl.TLSVersion.TLSv1_2,
        "tls1.3": ssl.TLSVersion.TLSv1_3,
        "tlsv1.2": ssl.TLSVersion.TLSv1_2,
        "tlsv1.3": ssl.TLSVersion.TLSv1_3,
    }
    min_ver = version_map.get(raw)
    if min_ver is None:
        print(
            f"Unknown ZEPHYR_SSL_MIN_VERSION={raw!r}; using default SSL context.",
            file=sys.stderr,
        )
        return ctx
    try:
        ctx.minimum_version = min_ver
    except (ValueError, AttributeError, ssl.SSLError) as exc:
        print(
            f"ZEPHYR_SSL_MIN_VERSION={raw!r} not applied: {exc}; using default.",
            file=sys.stderr,
        )
    return ctx


_SSL_CONTEXT: ssl.SSLContext | None = None


def ssl_context() -> ssl.SSLContext:
    global _SSL_CONTEXT
    if _SSL_CONTEXT is None:
        _SSL_CONTEXT = build_ssl_context()
    return _SSL_CONTEXT


def urlopen(
    request: urllib.request.Request,
    *,
    timeout: float = 30,
) -> Any:
    return urllib.request.urlopen(request, timeout=timeout, context=ssl_context())


def redact_headers(headers: dict[str, str] | None) -> dict[str, str]:
    if not headers:
        return {}
    out: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in _SENSITIVE_HEADER_NAMES:
            out[key] = "<redacted>"
        else:
            out[key] = value
    return out


def redact_text(text: str, *, max_len: int = _MAX_ERROR_BODY_LEN) -> str:
    if not text:
        return ""
    cleaned = _TOKEN_IN_TEXT_RE.sub(r"\1<redacted>", text)
    if len(cleaned) > max_len:
        return cleaned[:max_len] + "...(truncated)"
    return cleaned


def format_http_error(
    *,
    code: int,
    url: str,
    method: str,
    body: str,
) -> str:
    safe_url = redact_text(url, max_len=2000)
    safe_body = redact_text(body)
    return f"HTTP {code} while requesting '{safe_url}' [{method}]. Response: {safe_body}"


def enforce_token_from_env_only(cli_token: str | None) -> None:
    if not cli_token:
        return
    if _parse_bool_env(os.getenv("ZEPHYR_ENFORCE_ENV_TOKEN"), default=False):
        raise ValueError(
            "Passing --token is disabled (ZEPHYR_ENFORCE_ENV_TOKEN=true). "
            "Set ZEPHYR_API_TOKEN in .env or environment."
        )
    warnings.warn(
        "Passing --token via CLI may expose secrets in process listings; "
        "prefer ZEPHYR_API_TOKEN in .env.",
        UserWarning,
        stacklevel=3,
    )


def validate_json_object_env(raw: str | None, env_name: str) -> dict[str, Any]:
    if not raw or not str(raw).strip():
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{env_name} must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{env_name} must be a JSON object")
    return parsed


def logviewer_pattern() -> re.Pattern[str]:
    raw = (os.getenv("ZEPHYR_LOGVIEWER_URL_REGEX") or "").strip()
    if raw:
        try:
            return re.compile(raw, re.IGNORECASE)
        except re.error as exc:
            print(
                f"Invalid ZEPHYR_LOGVIEWER_URL_REGEX: {exc}; using default.",
                file=sys.stderr,
            )
    return re.compile(
        r"https://logviewer\.df\.sbauto\.tech/logs/[^\s)\"'<>]+",
        re.IGNORECASE,
    )


def is_allowed_logviewer_url(url: str) -> bool:
    return bool(logviewer_pattern().fullmatch(url.rstrip(".,;)>]")))


def filter_logviewer_urls(urls: list[str]) -> list[str]:
    strict = _parse_bool_env(os.getenv("ZEPHYR_LOGVIEWER_STRICT"), default=True)
    out: list[str] = []
    for url in urls:
        if is_allowed_logviewer_url(url):
            out.append(url)
        elif strict:
            print(
                f"logviewer URL rejected (does not match allowlist): {url[:120]}",
                file=sys.stderr,
            )
    return out

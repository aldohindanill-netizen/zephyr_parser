#!/usr/bin/env bash
# Unix launcher aligned with run_zephyr_weekly_report.ps1 (SmartBear Zephyr Scale Cloud v2).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

load_env_file() {
  local env_file="$1"
  local raw_line line trimmed name value
  while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
    line="${raw_line%$'\r'}"
    trimmed="${line#"${line%%[![:space:]]*}"}"
    trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
    [[ -z "$trimmed" || "${trimmed:0:1}" == "#" ]] && continue
    [[ "$trimmed" != *"="* ]] && continue
    name="${trimmed%%=*}"
    value="${trimmed#*=}"
    name="${name#"${name%%[![:space:]]*}"}"
    name="${name%"${name##*[![:space:]]}"}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "${#value}" -ge 2 && "$value" == \'*\' ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "${#value}" -ge 2 && "$value" == \"*\" ]]; then
      value="${value:1:${#value}-2}"
    fi
    export "${name}=${value}"
  done <"$env_file"
}

if [[ -f "$ENV_FILE" ]]; then
  load_env_file "$ENV_FILE"
fi

if [[ -z "${ZEPHYR_API_TOKEN:-}" ]]; then
  printf '%s\n' "Set ZEPHYR_API_TOKEN in .env or environment before running." >&2
  exit 1
fi

BASE_URL="${ZEPHYR_BASE_URL:-https://api.zephyrscale.smartbear.com}"
ENDPOINT="${ZEPHYR_ENDPOINT:-/v2/testexecutions}"
OUTPUT="${ZEPHYR_OUTPUT:-weekly_zephyr_report.csv}"
PAGE_SIZE="${ZEPHYR_PAGE_SIZE:-100}"
TOKEN_HEADER="${ZEPHYR_TOKEN_HEADER:-Authorization}"
TOKEN_PREFIX="${ZEPHYR_TOKEN_PREFIX:-Bearer}"

report_script="$SCRIPT_DIR/zephyr_weekly_report.py"

cmd=(
  "$report_script"
  --base-url "$BASE_URL"
  --endpoint "$ENDPOINT"
  --token "$ZEPHYR_API_TOKEN"
  --token-header "$TOKEN_HEADER"
  --token-prefix "$TOKEN_PREFIX"
  --page-size "$PAGE_SIZE"
  --output "$OUTPUT"
)

if [[ -n "${ZEPHYR_EXTRA_PARAMS:-}" ]]; then
  IFS=',' read -r -a extra_parts <<<"${ZEPHYR_EXTRA_PARAMS}"
  for param in "${extra_parts[@]}"; do
    p="${param#"${param%%[![:space:]]*}"}"
    p="${p%"${p##*[![:space:]]}"}"
    [[ -n "$p" ]] && cmd+=(--extra-param "$p")
  done
fi

if [[ -n "${ZEPHYR_DATE_FIELDS:-}" ]]; then
  IFS=',' read -r -a date_parts <<<"${ZEPHYR_DATE_FIELDS}"
  for field in "${date_parts[@]}"; do
    f="${field#"${field%%[![:space:]]*}"}"
    f="${f%"${f##*[![:space:]]}"}"
    [[ -n "$f" ]] && cmd+=(--date-field "$f")
  done
fi

if [[ -n "${ZEPHYR_STATUS_FIELDS:-}" ]]; then
  IFS=',' read -r -a status_parts <<<"${ZEPHYR_STATUS_FIELDS}"
  for field in "${status_parts[@]}"; do
    f="${field#"${field%%[![:space:]]*}"}"
    f="${f%"${f##*[![:space:]]}"}"
    [[ -n "$f" ]] && cmd+=(--status-field "$f")
  done
fi

if [[ "$#" -gt 0 ]]; then
  cmd+=("$@")
fi

pick_python() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    printf '%s\n' "$PYTHON_BIN"
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return
  fi
  if command -v python >/dev/null 2>&1; then
    command -v python
    return
  fi
  printf '%s\n' "Python not found. Install Python 3.10+ or set PYTHON_BIN." >&2
  exit 1
}

py="$(pick_python)"
exec "$py" "${cmd[@]}"

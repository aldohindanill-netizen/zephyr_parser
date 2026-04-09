#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

BASE_URL="${ZEPHYR_BASE_URL:-https://api.zephyrscale.smartbear.com}"
ENDPOINT="${ZEPHYR_ENDPOINT:-/v2/testexecutions}"
OUTPUT="${ZEPHYR_OUTPUT:-weekly_zephyr_report.csv}"
PAGE_SIZE="${ZEPHYR_PAGE_SIZE:-100}"
TOKEN_HEADER="${ZEPHYR_TOKEN_HEADER:-Authorization}"
TOKEN_PREFIX="${ZEPHYR_TOKEN_PREFIX:-Bearer}"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage:
  ./run_zephyr_weekly_report.sh [extra zephyr_weekly_report.py args]

Environment variables:
  ZEPHYR_TOKEN            (required) API token
  ZEPHYR_BASE_URL         default: https://api.zephyrscale.smartbear.com
  ZEPHYR_ENDPOINT         default: /v2/testexecutions
  ZEPHYR_OUTPUT           default: weekly_zephyr_report.csv
  ZEPHYR_PAGE_SIZE        default: 100
  ZEPHYR_TOKEN_HEADER     default: Authorization
  ZEPHYR_TOKEN_PREFIX     default: Bearer
  ZEPHYR_FROM_DATE        optional, YYYY-MM-DD
  ZEPHYR_TO_DATE          optional, YYYY-MM-DD
  ZEPHYR_EXTRA_PARAMS     optional, comma-separated key=value list
                           example: "projectKey=DEMO,testCycleKey=DEMO-R1"
  ZEPHYR_DATE_FIELDS      optional, comma-separated field paths
                           example: "executedOn,createdOn"
  ZEPHYR_STATUS_FIELDS    optional, comma-separated field paths
                           example: "status.name,testExecutionStatus.name"
EOF
  exec "$PYTHON_BIN" "$SCRIPT_DIR/zephyr_weekly_report.py" --help
fi

: "${ZEPHYR_TOKEN:?Set ZEPHYR_TOKEN environment variable before running}"

cmd=(
  "$PYTHON_BIN"
  "$SCRIPT_DIR/zephyr_weekly_report.py"
  --base-url "$BASE_URL"
  --endpoint "$ENDPOINT"
  --token "$ZEPHYR_TOKEN"
  --token-header "$TOKEN_HEADER"
  --token-prefix "$TOKEN_PREFIX"
  --page-size "$PAGE_SIZE"
  --output "$OUTPUT"
)

if [[ -n "${ZEPHYR_FROM_DATE:-}" ]]; then
  cmd+=(--from-date "$ZEPHYR_FROM_DATE")
fi

if [[ -n "${ZEPHYR_TO_DATE:-}" ]]; then
  cmd+=(--to-date "$ZEPHYR_TO_DATE")
fi

if [[ -n "${ZEPHYR_EXTRA_PARAMS:-}" ]]; then
  IFS=',' read -r -a extra_params <<<"$ZEPHYR_EXTRA_PARAMS"
  for param in "${extra_params[@]}"; do
    [[ -n "$param" ]] && cmd+=(--extra-param "$param")
  done
fi

if [[ -n "${ZEPHYR_DATE_FIELDS:-}" ]]; then
  IFS=',' read -r -a date_fields <<<"$ZEPHYR_DATE_FIELDS"
  for field in "${date_fields[@]}"; do
    [[ -n "$field" ]] && cmd+=(--date-field "$field")
  done
fi

if [[ -n "${ZEPHYR_STATUS_FIELDS:-}" ]]; then
  IFS=',' read -r -a status_fields <<<"$ZEPHYR_STATUS_FIELDS"
  for field in "${status_fields[@]}"; do
    [[ -n "$field" ]] && cmd+=(--status-field "$field")
  done
fi

cmd+=("$@")

exec "${cmd[@]}"

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
ENV_FILE="$SCRIPT_DIR/.env"

echo "[zephyr] Script dir: $SCRIPT_DIR"
if [[ -f "$ENV_FILE" ]]; then
  echo "[zephyr] Loading env from: $ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "[zephyr] No .env found at $ENV_FILE, using current environment"
fi

BASE_URL="${ZEPHYR_BASE_URL:-https://jira.navio.auto}"
ENDPOINT="${ZEPHYR_ENDPOINT:-/rest/tests/1.0/testrun/search}"
OUTPUT="${ZEPHYR_OUTPUT:-weekly_zephyr_report.csv}"

PROJECT_ID="${ZEPHYR_PROJECT_ID:-10904}"
FOLDER_TREE_ID="${ZEPHYR_FOLDER_TREE_ID:-9172}"
MAX_RESULTS="${ZEPHYR_MAX_RESULTS:-40}"
START_AT="${ZEPHYR_START_AT:-0}"
ARCHIVED="${ZEPHYR_ARCHIVED:-false}"

DATE_FIELD="${ZEPHYR_DATE_FIELD:-updatedOn}"
STATUS_FIELD="${ZEPHYR_STATUS_FIELD:-status.name}"

FIELDS="${ZEPHYR_FIELDS:-id,key,name,folderId,iterationId,projectVersionId,environmentId,userKeys,environmentIds,plannedStartDate,plannedEndDate,executionTime,estimatedTime,testResultStatuses,testCaseCount,issueCount,status(id,name,i18nKey,color),customFieldValues,createdOn,createdBy,updatedOn,updatedBy,owner}"
QUERY="${ZEPHYR_QUERY:-testRun.projectId IN (${PROJECT_ID}) AND testRun.folderTreeId IN (${FOLDER_TREE_ID}) ORDER BY testRun.name ASC}"

: "${ZEPHYR_API_TOKEN:?Set ZEPHYR_API_TOKEN before running}"
echo "[zephyr] Token is set via ZEPHYR_API_TOKEN"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "[zephyr] Error: Python binary '$PYTHON_BIN' was not found in PATH" >&2
  exit 1
fi
echo "[zephyr] Using Python: $(command -v "$PYTHON_BIN")"

cmd=(
  "$PYTHON_BIN"
  "$SCRIPT_DIR/zephyr_weekly_report.py"
  --base-url "$BASE_URL"
  --endpoint "$ENDPOINT"
  --extra-param "fields=$FIELDS"
  --extra-param "query=$QUERY"
  --extra-param "maxResults=$MAX_RESULTS"
  --extra-param "startAt=$START_AT"
  --extra-param "archived=$ARCHIVED"
  --date-field "$DATE_FIELD"
  --status-field "$STATUS_FIELD"
  --output "$OUTPUT"
)

cmd+=("$@")
echo "[zephyr] Running weekly report..."
echo "[zephyr] Output file: $OUTPUT"
exec "${cmd[@]}"

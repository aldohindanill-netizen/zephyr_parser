#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
ENV_FILE="$SCRIPT_DIR/.env"

strip_cr() {
  printf '%s' "${1//$'\r'/}"
}

normalize_bool() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

echo "[zephyr] Script dir: $SCRIPT_DIR"
if [[ -f "$ENV_FILE" ]]; then
  echo "[zephyr] Loading env from: $ENV_FILE"
  # Avoid sticky values from previous shell sessions: .env must be the source of truth.
  unset CONFLUENCE_PUBLISH_DAILY
  unset CONFLUENCE_PUBLISH_WEEKLY
  unset CONFLUENCE_BASE_URL
  unset CONFLUENCE_SPACE_KEY
  unset CONFLUENCE_PARENT_PAGE_ID
  unset CONFLUENCE_USERNAME
  unset CONFLUENCE_API_TOKEN
  unset CONFLUENCE_VERIFY_SSL
  unset CONFLUENCE_DRY_RUN
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "[zephyr] No .env found at $ENV_FILE, using current environment"
fi

BASE_URL="${ZEPHYR_BASE_URL:-https://jira.navio.auto}"
ENDPOINT="${ZEPHYR_ENDPOINT:-rest/tests/1.0/testrun/search}"
FOLDER_ENDPOINT="${ZEPHYR_FOLDER_ENDPOINT:-rest/tests/1.0/foldertree}"
FOLDER_SEARCH_ENDPOINT="${ZEPHYR_FOLDER_SEARCH_ENDPOINT:-rest/tests/1.0/folder/search}"
OUTPUT="${ZEPHYR_OUTPUT:-weekly_zephyr_report.csv}"
PER_FOLDER_DIR="${ZEPHYR_PER_FOLDER_DIR:-reports/by_folder}"

PROJECT_ID="${ZEPHYR_PROJECT_ID:-10904}"
if [[ -n "${ZEPHYR_FOLDERTREE_ENDPOINT:-}" ]]; then
  FOLDERTREE_ENDPOINT="${ZEPHYR_FOLDERTREE_ENDPOINT}"
else
  FOLDERTREE_ENDPOINT="rest/tests/1.0/project/${PROJECT_ID}/foldertree/testrun"
fi
ROOT_FOLDER_IDS="${ZEPHYR_ROOT_FOLDER_IDS:-}"
ALLOWED_ROOT_FOLDER_IDS="${ZEPHYR_ALLOWED_ROOT_FOLDER_IDS:-}"
FOLDER_NAME_REGEX="${ZEPHYR_FOLDER_NAME_REGEX:-}"
FOLDER_NAME_ENDPOINT_TEMPLATE="${ZEPHYR_FOLDER_NAME_ENDPOINT_TEMPLATE:-}"
FOLDER_PATH_REGEX="${ZEPHYR_FOLDER_PATH_REGEX:-}"
MAX_RESULTS="${ZEPHYR_MAX_RESULTS:-40}"
START_AT="${ZEPHYR_START_AT:-0}"
ARCHIVED="${ZEPHYR_ARCHIVED:-false}"

DATE_FIELD="${ZEPHYR_DATE_FIELD:-updatedOn}"
STATUS_FIELD="${ZEPHYR_STATUS_FIELD:-status.name}"

FIELDS="${ZEPHYR_FIELDS:-id,key,name,folderId,iterationId,projectVersionId,environmentId,userKeys,environmentIds,plannedStartDate,plannedEndDate,executionTime,estimatedTime,testResultStatuses,testCaseCount,issueCount,status(id,name,i18nKey,color),customFieldValues,createdOn,createdBy,updatedOn,updatedBy,owner}"
QUERY_TEMPLATE="${ZEPHYR_QUERY_TEMPLATE:-testRun.projectId IN (${PROJECT_ID}) AND testRun.folderTreeId IN ({folder_id}) ORDER BY testRun.name ASC}"
PROJECT_QUERY="${ZEPHYR_PROJECT_QUERY:-testRun.projectId IN (${PROJECT_ID}) ORDER BY testRun.name ASC}"
FROM_DATE="${ZEPHYR_FROM_DATE:-}"
TO_DATE="${ZEPHYR_TO_DATE:-}"
DEBUG_FOLDER_FIELDS="${ZEPHYR_DEBUG_FOLDER_FIELDS:-false}"
DISCOVERY_MODE="${ZEPHYR_DISCOVERY_MODE:-tree}"
TREE_LEAF_ONLY="${ZEPHYR_TREE_LEAF_ONLY:-true}"
TREE_NAME_REGEX="${ZEPHYR_TREE_NAME_REGEX:-}"
TREE_ROOT_PATH_REGEX="${ZEPHYR_TREE_ROOT_PATH_REGEX:-}"
TREE_AUTOPROBE="${ZEPHYR_TREE_AUTOPROBE:-true}"
TREE_SOURCE_ENDPOINT="${ZEPHYR_TREE_SOURCE_ENDPOINT:-}"
TREE_SOURCE_METHOD="${ZEPHYR_TREE_SOURCE_METHOD:-GET}"
TREE_SOURCE_QUERY_JSON="${ZEPHYR_TREE_SOURCE_QUERY_JSON:-}"
TREE_SOURCE_BODY_JSON="${ZEPHYR_TREE_SOURCE_BODY_JSON:-}"
EXPORT_CYCLES_CASES="${ZEPHYR_EXPORT_CYCLES_CASES:-false}"
CYCLES_CASES_OUTPUT="${ZEPHYR_CYCLES_CASES_OUTPUT:-reports/cycles_and_cases.csv}"
TESTCASE_ENDPOINT_TEMPLATE="${ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE:-}"
SYNTHETIC_CYCLE_IDS="${ZEPHYR_SYNTHETIC_CYCLE_IDS:-false}"
EXPORT_CASE_STEPS="${ZEPHYR_EXPORT_CASE_STEPS:-false}"
CASE_STEPS_OUTPUT="${ZEPHYR_CASE_STEPS_OUTPUT:-reports/case_steps.csv}"
EXPORT_DAILY_READABLE="${ZEPHYR_EXPORT_DAILY_READABLE:-false}"
DAILY_READABLE_DIR="${ZEPHYR_DAILY_READABLE_DIR:-reports/daily_readable}"
DAILY_READABLE_FORMATS="${ZEPHYR_DAILY_READABLE_FORMATS:-html,wiki}"
WEEKLY_CYCLE_MATRIX_OUTPUT="${ZEPHYR_WEEKLY_CYCLE_MATRIX_OUTPUT:-reports/weekly_cycle_matrix.csv}"
EXPORT_WEEKLY_READABLE="${ZEPHYR_EXPORT_WEEKLY_READABLE:-true}"
WEEKLY_READABLE_DIR="${ZEPHYR_WEEKLY_READABLE_DIR:-reports/weekly_readable}"
WEEKLY_READABLE_FORMATS="${ZEPHYR_WEEKLY_READABLE_FORMATS:-html,wiki}"
CONFLUENCE_PUBLISH_DAILY="${CONFLUENCE_PUBLISH_DAILY:-false}"
CONFLUENCE_PUBLISH_WEEKLY="${CONFLUENCE_PUBLISH_WEEKLY:-false}"
CONFLUENCE_BASE_URL="${CONFLUENCE_BASE_URL:-}"
CONFLUENCE_SPACE_KEY="${CONFLUENCE_SPACE_KEY:-}"
CONFLUENCE_PARENT_PAGE_ID="${CONFLUENCE_PARENT_PAGE_ID:-}"
CONFLUENCE_USERNAME="${CONFLUENCE_USERNAME:-}"
CONFLUENCE_API_TOKEN="${CONFLUENCE_API_TOKEN:-}"
CONFLUENCE_AUTH_MODE="${CONFLUENCE_AUTH_MODE:-auto}"
CONFLUENCE_VERIFY_SSL="${CONFLUENCE_VERIFY_SSL:-true}"
CONFLUENCE_DRY_RUN="${CONFLUENCE_DRY_RUN:-false}"

BASE_URL="$(strip_cr "$BASE_URL")"
ENDPOINT="$(strip_cr "$ENDPOINT")"
FOLDER_ENDPOINT="$(strip_cr "$FOLDER_ENDPOINT")"
FOLDER_SEARCH_ENDPOINT="$(strip_cr "$FOLDER_SEARCH_ENDPOINT")"
FOLDERTREE_ENDPOINT="$(strip_cr "$FOLDERTREE_ENDPOINT")"
OUTPUT="$(strip_cr "$OUTPUT")"
PER_FOLDER_DIR="$(strip_cr "$PER_FOLDER_DIR")"
PROJECT_ID="$(strip_cr "$PROJECT_ID")"
ROOT_FOLDER_IDS="$(strip_cr "$ROOT_FOLDER_IDS")"
ALLOWED_ROOT_FOLDER_IDS="$(strip_cr "$ALLOWED_ROOT_FOLDER_IDS")"
FOLDER_NAME_REGEX="$(strip_cr "$FOLDER_NAME_REGEX")"
FOLDER_NAME_ENDPOINT_TEMPLATE="$(strip_cr "$FOLDER_NAME_ENDPOINT_TEMPLATE")"
FOLDER_PATH_REGEX="$(strip_cr "$FOLDER_PATH_REGEX")"
MAX_RESULTS="$(strip_cr "$MAX_RESULTS")"
START_AT="$(strip_cr "$START_AT")"
ARCHIVED="$(strip_cr "$ARCHIVED")"
DATE_FIELD="$(strip_cr "$DATE_FIELD")"
STATUS_FIELD="$(strip_cr "$STATUS_FIELD")"
FIELDS="$(strip_cr "$FIELDS")"
QUERY_TEMPLATE="$(strip_cr "$QUERY_TEMPLATE")"
PROJECT_QUERY="$(strip_cr "$PROJECT_QUERY")"
FROM_DATE="$(strip_cr "$FROM_DATE")"
TO_DATE="$(strip_cr "$TO_DATE")"
DEBUG_FOLDER_FIELDS="$(strip_cr "$DEBUG_FOLDER_FIELDS")"
DISCOVERY_MODE="$(strip_cr "$DISCOVERY_MODE")"
TREE_LEAF_ONLY="$(strip_cr "$TREE_LEAF_ONLY")"
TREE_NAME_REGEX="$(strip_cr "$TREE_NAME_REGEX")"
TREE_ROOT_PATH_REGEX="$(strip_cr "$TREE_ROOT_PATH_REGEX")"
TREE_AUTOPROBE="$(strip_cr "$TREE_AUTOPROBE")"
TREE_SOURCE_ENDPOINT="$(strip_cr "$TREE_SOURCE_ENDPOINT")"
TREE_SOURCE_METHOD="$(strip_cr "$TREE_SOURCE_METHOD")"
TREE_SOURCE_QUERY_JSON="$(strip_cr "$TREE_SOURCE_QUERY_JSON")"
TREE_SOURCE_BODY_JSON="$(strip_cr "$TREE_SOURCE_BODY_JSON")"
EXPORT_CYCLES_CASES="$(strip_cr "$EXPORT_CYCLES_CASES")"
CYCLES_CASES_OUTPUT="$(strip_cr "$CYCLES_CASES_OUTPUT")"
TESTCASE_ENDPOINT_TEMPLATE="$(strip_cr "$TESTCASE_ENDPOINT_TEMPLATE")"
SYNTHETIC_CYCLE_IDS="$(strip_cr "$SYNTHETIC_CYCLE_IDS")"
EXPORT_CASE_STEPS="$(strip_cr "$EXPORT_CASE_STEPS")"
CASE_STEPS_OUTPUT="$(strip_cr "$CASE_STEPS_OUTPUT")"
EXPORT_DAILY_READABLE="$(strip_cr "$EXPORT_DAILY_READABLE")"
DAILY_READABLE_DIR="$(strip_cr "$DAILY_READABLE_DIR")"
DAILY_READABLE_FORMATS="$(strip_cr "$DAILY_READABLE_FORMATS")"
WEEKLY_CYCLE_MATRIX_OUTPUT="$(strip_cr "$WEEKLY_CYCLE_MATRIX_OUTPUT")"
EXPORT_WEEKLY_READABLE="$(strip_cr "$EXPORT_WEEKLY_READABLE")"
WEEKLY_READABLE_DIR="$(strip_cr "$WEEKLY_READABLE_DIR")"
WEEKLY_READABLE_FORMATS="$(strip_cr "$WEEKLY_READABLE_FORMATS")"
CONFLUENCE_PUBLISH_DAILY="$(normalize_bool "$(strip_cr "$CONFLUENCE_PUBLISH_DAILY")")"
CONFLUENCE_PUBLISH_WEEKLY="$(normalize_bool "$(strip_cr "$CONFLUENCE_PUBLISH_WEEKLY")")"
CONFLUENCE_BASE_URL="$(strip_cr "$CONFLUENCE_BASE_URL")"
CONFLUENCE_SPACE_KEY="$(strip_cr "$CONFLUENCE_SPACE_KEY")"
CONFLUENCE_PARENT_PAGE_ID="$(strip_cr "$CONFLUENCE_PARENT_PAGE_ID")"
CONFLUENCE_USERNAME="$(strip_cr "$CONFLUENCE_USERNAME")"
CONFLUENCE_API_TOKEN="$(strip_cr "$CONFLUENCE_API_TOKEN")"
CONFLUENCE_AUTH_MODE="$(normalize_bool "$(strip_cr "$CONFLUENCE_AUTH_MODE")")"
CONFLUENCE_VERIFY_SSL="$(normalize_bool "$(strip_cr "$CONFLUENCE_VERIFY_SSL")")"
CONFLUENCE_DRY_RUN="$(normalize_bool "$(strip_cr "$CONFLUENCE_DRY_RUN")")"

: "${ZEPHYR_API_TOKEN:?Set ZEPHYR_API_TOKEN before running}"
echo "[zephyr] Token is set via ZEPHYR_API_TOKEN"
echo "[zephyr] Confluence: daily=${CONFLUENCE_PUBLISH_DAILY} weekly=${CONFLUENCE_PUBLISH_WEEKLY} dry_run=${CONFLUENCE_DRY_RUN} auth_mode=${CONFLUENCE_AUTH_MODE}"

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
  --discover-folders
  --discovery-mode "$DISCOVERY_MODE"
  --folder-endpoint "$FOLDER_ENDPOINT"
  --folder-search-endpoint "$FOLDER_SEARCH_ENDPOINT"
  --foldertree-endpoint "$FOLDERTREE_ENDPOINT"
  --project-id "$PROJECT_ID"
  --query-template "$QUERY_TEMPLATE"
  --project-query "$PROJECT_QUERY"
  --extra-param "fields=$FIELDS"
  --extra-param "maxResults=$MAX_RESULTS"
  --extra-param "startAt=$START_AT"
  --extra-param "archived=$ARCHIVED"
  --date-field "$DATE_FIELD"
  --status-field "$STATUS_FIELD"
  --output "$OUTPUT"
  --per-folder-dir "$PER_FOLDER_DIR"
)

if [[ "$DISCOVERY_MODE" == "executions" ]]; then
  cmd+=(--discover-from-executions)
fi

if [[ "$TREE_LEAF_ONLY" == "true" ]]; then
  cmd+=(--tree-leaf-only)
fi

if [[ -n "$TREE_NAME_REGEX" ]]; then
  cmd+=(--tree-name-regex "$TREE_NAME_REGEX")
fi

if [[ -n "$TREE_ROOT_PATH_REGEX" ]]; then
  cmd+=(--tree-root-path-regex "$TREE_ROOT_PATH_REGEX")
fi

if [[ "$TREE_AUTOPROBE" == "true" ]]; then
  cmd+=(--tree-autoprobe)
fi

if [[ -n "$TREE_SOURCE_ENDPOINT" ]]; then
  cmd+=(--tree-source-endpoint "$TREE_SOURCE_ENDPOINT")
  cmd+=(--tree-source-method "$TREE_SOURCE_METHOD")
fi

if [[ -n "$TREE_SOURCE_QUERY_JSON" ]]; then
  cmd+=(--tree-source-query-json "$TREE_SOURCE_QUERY_JSON")
fi

if [[ -n "$TREE_SOURCE_BODY_JSON" ]]; then
  cmd+=(--tree-source-body-json "$TREE_SOURCE_BODY_JSON")
fi

if [[ "$EXPORT_CYCLES_CASES" == "true" ]]; then
  cmd+=(--export-cycles-cases)
  cmd+=(--cycles-cases-output "$CYCLES_CASES_OUTPUT")
fi

if [[ "$PYTHON_BIN" == "py" ]]; then
  cmd=("$PYTHON_BIN" -3 "${cmd[@]:1}")
fi

IFS=',' read -r -a root_ids <<<"$ROOT_FOLDER_IDS"
for root_id in "${root_ids[@]}"; do
  trimmed="${root_id//[[:space:]]/}"
  [[ -n "$trimmed" ]] && cmd+=(--root-folder-id "$trimmed")
done

if [[ -n "$ALLOWED_ROOT_FOLDER_IDS" ]]; then
  IFS=',' read -r -a allowed_ids <<<"$ALLOWED_ROOT_FOLDER_IDS"
  for folder_id in "${allowed_ids[@]}"; do
    trimmed="${folder_id//[[:space:]]/}"
    [[ -n "$trimmed" ]] && cmd+=(--allowed-root-folder-id "$trimmed")
  done
fi

if [[ -n "$FOLDER_NAME_REGEX" ]]; then
  cmd+=(--folder-name-regex "$FOLDER_NAME_REGEX")
fi

if [[ -n "$FOLDER_NAME_ENDPOINT_TEMPLATE" ]]; then
  cmd+=(--folder-name-endpoint-template "$FOLDER_NAME_ENDPOINT_TEMPLATE")
fi

if [[ -n "$TESTCASE_ENDPOINT_TEMPLATE" ]]; then
  cmd+=(--testcase-endpoint-template "$TESTCASE_ENDPOINT_TEMPLATE")
fi

if [[ "$SYNTHETIC_CYCLE_IDS" == "true" ]]; then
  cmd+=(--synthetic-cycle-ids)
fi

if [[ "$EXPORT_CASE_STEPS" == "true" ]]; then
  cmd+=(--export-case-steps)
  cmd+=(--case-steps-output "$CASE_STEPS_OUTPUT")
fi

if [[ "$EXPORT_DAILY_READABLE" == "true" ]]; then
  cmd+=(--export-daily-readable)
  cmd+=(--daily-readable-dir "$DAILY_READABLE_DIR")
  IFS=',' read -r -a readable_formats <<<"$DAILY_READABLE_FORMATS"
  for fmt in "${readable_formats[@]}"; do
    trimmed="${fmt//[[:space:]]/}"
    [[ -n "$trimmed" ]] && cmd+=(--daily-readable-format "$trimmed")
  done
fi

if [[ -n "$WEEKLY_CYCLE_MATRIX_OUTPUT" ]]; then
  cmd+=(--weekly-cycle-matrix-output "$WEEKLY_CYCLE_MATRIX_OUTPUT")
fi

if [[ "$EXPORT_WEEKLY_READABLE" == "true" ]]; then
  cmd+=(--export-weekly-readable)
  cmd+=(--weekly-readable-dir "$WEEKLY_READABLE_DIR")
  IFS=',' read -r -a weekly_formats <<<"$WEEKLY_READABLE_FORMATS"
  for fmt in "${weekly_formats[@]}"; do
    trimmed="${fmt//[[:space:]]/}"
    [[ -n "$trimmed" ]] && cmd+=(--weekly-readable-format "$trimmed")
  done
fi

if [[ "$CONFLUENCE_PUBLISH_DAILY" == "true" ]]; then
  cmd+=(--publish-confluence-daily)
fi

if [[ "$CONFLUENCE_PUBLISH_WEEKLY" == "true" ]]; then
  cmd+=(--publish-confluence-weekly)
fi

if [[ -n "$CONFLUENCE_BASE_URL" ]]; then
  cmd+=(--confluence-base-url "$CONFLUENCE_BASE_URL")
fi

if [[ -n "$CONFLUENCE_SPACE_KEY" ]]; then
  cmd+=(--confluence-space-key "$CONFLUENCE_SPACE_KEY")
fi

if [[ -n "$CONFLUENCE_PARENT_PAGE_ID" ]]; then
  cmd+=(--confluence-parent-page-id "$CONFLUENCE_PARENT_PAGE_ID")
fi

if [[ -n "$CONFLUENCE_USERNAME" ]]; then
  cmd+=(--confluence-username "$CONFLUENCE_USERNAME")
fi

if [[ -n "$CONFLUENCE_API_TOKEN" ]]; then
  cmd+=(--confluence-api-token "$CONFLUENCE_API_TOKEN")
fi

if [[ "$CONFLUENCE_AUTH_MODE" == "auto" || "$CONFLUENCE_AUTH_MODE" == "basic" || "$CONFLUENCE_AUTH_MODE" == "bearer" ]]; then
  cmd+=(--confluence-auth-mode "$CONFLUENCE_AUTH_MODE")
fi

if [[ "$CONFLUENCE_VERIFY_SSL" == "true" || "$CONFLUENCE_VERIFY_SSL" == "false" ]]; then
  cmd+=(--confluence-verify-ssl "$CONFLUENCE_VERIFY_SSL")
fi

if [[ "$CONFLUENCE_DRY_RUN" == "true" ]]; then
  cmd+=(--confluence-dry-run)
fi

if [[ -n "$FOLDER_PATH_REGEX" ]]; then
  cmd+=(--folder-path-regex "$FOLDER_PATH_REGEX")
fi

if [[ -n "$FROM_DATE" ]]; then
  cmd+=(--from-date "$FROM_DATE")
fi

if [[ -n "$TO_DATE" ]]; then
  cmd+=(--to-date "$TO_DATE")
fi

if [[ "$DEBUG_FOLDER_FIELDS" == "true" ]]; then
  cmd+=(--debug-folder-fields)
fi

cmd+=("$@")
echo "[zephyr] Running weekly report..."
echo "[zephyr] Output file: $OUTPUT"
exec "${cmd[@]}"

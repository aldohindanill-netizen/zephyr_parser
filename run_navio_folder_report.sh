#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
ENV_FILE="$SCRIPT_DIR/.env"

strip_cr() {
  printf '%s' "${1//$'\r'/}"
}

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
CREATE_FOLDER_FIRST="${ZEPHYR_CREATE_FOLDER_FIRST:-false}"
CREATE_FOLDER_NAME="${ZEPHYR_CREATE_FOLDER_NAME:-}"
CREATE_FOLDER_NAME_TEMPLATE="${ZEPHYR_CREATE_FOLDER_NAME_TEMPLATE:-}"
CREATE_FOLDER_PARENT_ID="${ZEPHYR_CREATE_FOLDER_PARENT_ID:-}"
CREATE_FOLDER_ENDPOINT="${ZEPHYR_CREATE_FOLDER_ENDPOINT:-rest/tests/1.0/folder}"
CREATE_FOLDER_NAME_FIELD="${ZEPHYR_CREATE_FOLDER_NAME_FIELD:-name}"
CREATE_FOLDER_PROJECT_ID_FIELD="${ZEPHYR_CREATE_FOLDER_PROJECT_ID_FIELD:-projectId}"
CREATE_FOLDER_PARENT_ID_FIELD="${ZEPHYR_CREATE_FOLDER_PARENT_ID_FIELD:-parentId}"
CREATE_FOLDER_BODY_JSON="${ZEPHYR_CREATE_FOLDER_BODY_JSON:-}"
CREATE_FOLDER_DRY_RUN="${ZEPHYR_CREATE_FOLDER_DRY_RUN:-false}"
CREATE_FOLDER_USE_AS_ROOT="${ZEPHYR_CREATE_FOLDER_USE_AS_ROOT:-false}"
EXPORT_CYCLES_CASES="${ZEPHYR_EXPORT_CYCLES_CASES:-false}"
CYCLES_CASES_OUTPUT="${ZEPHYR_CYCLES_CASES_OUTPUT:-reports/cycles_and_cases.csv}"
TESTCASE_ENDPOINT_TEMPLATE="${ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE:-}"
SYNTHETIC_CYCLE_IDS="${ZEPHYR_SYNTHETIC_CYCLE_IDS:-false}"
EXPORT_CASE_STEPS="${ZEPHYR_EXPORT_CASE_STEPS:-false}"
CASE_STEPS_OUTPUT="${ZEPHYR_CASE_STEPS_OUTPUT:-reports/case_steps.csv}"
EXPORT_DAILY_READABLE="${ZEPHYR_EXPORT_DAILY_READABLE:-false}"
DAILY_READABLE_DIR="${ZEPHYR_DAILY_READABLE_DIR:-reports/daily_readable}"
DAILY_READABLE_FORMATS="${ZEPHYR_DAILY_READABLE_FORMATS:-html,wiki}"

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
CREATE_FOLDER_FIRST="$(strip_cr "$CREATE_FOLDER_FIRST")"
CREATE_FOLDER_NAME="$(strip_cr "$CREATE_FOLDER_NAME")"
CREATE_FOLDER_NAME_TEMPLATE="$(strip_cr "$CREATE_FOLDER_NAME_TEMPLATE")"
CREATE_FOLDER_PARENT_ID="$(strip_cr "$CREATE_FOLDER_PARENT_ID")"
CREATE_FOLDER_ENDPOINT="$(strip_cr "$CREATE_FOLDER_ENDPOINT")"
CREATE_FOLDER_NAME_FIELD="$(strip_cr "$CREATE_FOLDER_NAME_FIELD")"
CREATE_FOLDER_PROJECT_ID_FIELD="$(strip_cr "$CREATE_FOLDER_PROJECT_ID_FIELD")"
CREATE_FOLDER_PARENT_ID_FIELD="$(strip_cr "$CREATE_FOLDER_PARENT_ID_FIELD")"
CREATE_FOLDER_BODY_JSON="$(strip_cr "$CREATE_FOLDER_BODY_JSON")"
CREATE_FOLDER_DRY_RUN="$(strip_cr "$CREATE_FOLDER_DRY_RUN")"
CREATE_FOLDER_USE_AS_ROOT="$(strip_cr "$CREATE_FOLDER_USE_AS_ROOT")"
EXPORT_CYCLES_CASES="$(strip_cr "$EXPORT_CYCLES_CASES")"
CYCLES_CASES_OUTPUT="$(strip_cr "$CYCLES_CASES_OUTPUT")"
TESTCASE_ENDPOINT_TEMPLATE="$(strip_cr "$TESTCASE_ENDPOINT_TEMPLATE")"
SYNTHETIC_CYCLE_IDS="$(strip_cr "$SYNTHETIC_CYCLE_IDS")"
EXPORT_CASE_STEPS="$(strip_cr "$EXPORT_CASE_STEPS")"
CASE_STEPS_OUTPUT="$(strip_cr "$CASE_STEPS_OUTPUT")"
EXPORT_DAILY_READABLE="$(strip_cr "$EXPORT_DAILY_READABLE")"
DAILY_READABLE_DIR="$(strip_cr "$DAILY_READABLE_DIR")"
DAILY_READABLE_FORMATS="$(strip_cr "$DAILY_READABLE_FORMATS")"

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

if [[ "$CREATE_FOLDER_FIRST" == "true" ]]; then
  cmd+=(--create-folder-first)
  cmd+=(--create-folder-endpoint "$CREATE_FOLDER_ENDPOINT")
  cmd+=(--create-folder-name-field "$CREATE_FOLDER_NAME_FIELD")
  cmd+=(--create-folder-project-id-field "$CREATE_FOLDER_PROJECT_ID_FIELD")
  cmd+=(--create-folder-parent-id-field "$CREATE_FOLDER_PARENT_ID_FIELD")
  if [[ -n "$CREATE_FOLDER_NAME" ]]; then
    cmd+=(--create-folder-name "$CREATE_FOLDER_NAME")
  fi
  if [[ -n "$CREATE_FOLDER_NAME_TEMPLATE" ]]; then
    cmd+=(--create-folder-name-template "$CREATE_FOLDER_NAME_TEMPLATE")
  fi
  if [[ -n "$CREATE_FOLDER_PARENT_ID" ]]; then
    cmd+=(--create-folder-parent-id "$CREATE_FOLDER_PARENT_ID")
  fi
  if [[ -n "$CREATE_FOLDER_BODY_JSON" ]]; then
    cmd+=(--create-folder-body-json "$CREATE_FOLDER_BODY_JSON")
  fi
  if [[ "$CREATE_FOLDER_DRY_RUN" == "true" ]]; then
    cmd+=(--create-folder-dry-run)
  fi
  if [[ "$CREATE_FOLDER_USE_AS_ROOT" == "true" ]]; then
    cmd+=(--create-folder-use-as-root)
  fi
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

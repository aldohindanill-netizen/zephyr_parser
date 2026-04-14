#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
ENV_FILE="$SCRIPT_DIR/.env"

strip_cr() {
  printf '%s' "${1//$'\r'/}"
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'EOF'
Usage:
  ./run_zephyr_google_pipeline.sh [generate|sync]

Modes:
  generate   Build/update Google Sheet from Zephyr folder data
  sync       Read Pass/Fail/Comment from existing sheet and write to Zephyr

Required env:
  ZEPHYR_API_TOKEN
  GOOGLE_SERVICE_ACCOUNT_FILE

Common env:
  ZEPHYR_BASE_URL
  ZEPHYR_PROJECT_ID
  ZEPHYR_BRANCH_NAME (or auto from current git branch)
  ZEPHYR_GSHEET_SPREADSHEET_ID (required for sync; optional for generate)

Examples:
  ./run_zephyr_google_pipeline.sh generate
  ./run_zephyr_google_pipeline.sh sync
EOF
  exit 0
fi

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${ZEPHYR_API_TOKEN:?Set ZEPHYR_API_TOKEN before running}"
: "${GOOGLE_SERVICE_ACCOUNT_FILE:?Set GOOGLE_SERVICE_ACCOUNT_FILE before running}"

BASE_URL="${ZEPHYR_BASE_URL:-https://jira.navio.auto}"
PROJECT_ID="${ZEPHYR_PROJECT_ID:-10904}"
BRANCH_NAME="${ZEPHYR_BRANCH_NAME:-}"
SPREADSHEET_ID="${GOOGLE_SPREADSHEET_ID:-}"
SPREADSHEET_TITLE="${GOOGLE_SPREADSHEET_TITLE:-Zephyr Daily Execution Sheet}"
CONFIG_SHEET="${GOOGLE_CONFIG_SHEET:-Config}"
RUN_SHEET="${GOOGLE_RUN_SHEET:-Run}"

FOLDER_SEARCH_ENDPOINT="${ZEPHYR_FOLDER_SEARCH_ENDPOINT:-rest/tests/1.0/folder/search}"
if [[ -n "${ZEPHYR_FOLDERTREE_ENDPOINT:-}" ]]; then
  FOLDERTREE_ENDPOINT="${ZEPHYR_FOLDERTREE_ENDPOINT}"
else
  FOLDERTREE_ENDPOINT="rest/tests/1.0/project/${PROJECT_ID}/foldertree/testrun"
fi

TREE_SOURCE_ENDPOINT="${ZEPHYR_TREE_SOURCE_ENDPOINT:-}"
TREE_SOURCE_METHOD="${ZEPHYR_TREE_SOURCE_METHOD:-GET}"
TREE_SOURCE_QUERY_JSON="${ZEPHYR_TREE_SOURCE_QUERY_JSON:-}"
TREE_SOURCE_BODY_JSON="${ZEPHYR_TREE_SOURCE_BODY_JSON:-}"

ENDPOINT="${ZEPHYR_ENDPOINT:-rest/tests/1.0/testrun/search}"
QUERY_TEMPLATE="${ZEPHYR_QUERY_TEMPLATE:-testRun.projectId IN (${PROJECT_ID}) AND testRun.folderTreeId IN ({folder_id}) ORDER BY testRun.name ASC}"
MAX_RESULTS="${ZEPHYR_MAX_RESULTS:-40}"
START_AT="${ZEPHYR_START_AT:-0}"
ARCHIVED="${ZEPHYR_ARCHIVED:-false}"
FIELDS="${ZEPHYR_FIELDS:-id,key,name,folderId,iterationId,projectVersionId,environmentId,userKeys,environmentIds,plannedStartDate,plannedEndDate,executionTime,estimatedTime,testResultStatuses,testCaseCount,issueCount,status(id,name,i18nKey,color),customFieldValues,createdOn,createdBy,updatedOn,updatedBy,owner,objective}"
TESTCASE_ENDPOINT_TEMPLATE="${ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE:-rest/tests/1.0/testrun/{cycle_id}/testcase/search}"
SYNTHETIC_CYCLE_IDS="${ZEPHYR_SYNTHETIC_CYCLE_IDS:-true}"
FROM_DATE="${ZEPHYR_FROM_DATE:-}"
TO_DATE="${ZEPHYR_TO_DATE:-}"
FOLDER_PARENT_ID="${ZEPHYR_FOLDER_PARENT_ID:-}"
FOLDER_PATH_REGEX="${ZEPHYR_FOLDER_PATH_REGEX:-}"
STATUS_PASS_NAME="${ZEPHYR_STATUS_PASS_NAME:-Pass}"
STATUS_FAIL_NAME="${ZEPHYR_STATUS_FAIL_NAME:-Fail}"

UPDATE_ENDPOINT_TEMPLATE="${ZEPHYR_UPDATE_ENDPOINT_TEMPLATE:-rest/tests/1.0/testresult/{test_result_id}}"
UPDATE_METHOD="${ZEPHYR_UPDATE_METHOD:-PUT}"
UPDATE_STATUS_ID_FIELD="${ZEPHYR_UPDATE_STATUS_ID_FIELD:-testResultStatusId}"
UPDATE_COMMENT_FIELD="${ZEPHYR_UPDATE_COMMENT_FIELD:-comment}"
UPDATE_EXTRA_BODY_JSON="${ZEPHYR_UPDATE_EXTRA_BODY_JSON:-}"

MODE="${1:-generate}"
if [[ "$MODE" != "generate" && "$MODE" != "sync" ]]; then
  echo "Usage: $0 [generate|sync]" >&2
  exit 1
fi

if [[ -z "$BRANCH_NAME" ]]; then
  # branch fallback: current git branch short name
  BRANCH_NAME="$(git -C "$SCRIPT_DIR" branch --show-current 2>/dev/null || true)"
fi
if [[ -z "$BRANCH_NAME" ]]; then
  echo "Set ZEPHYR_BRANCH_NAME in .env or run from git branch context." >&2
  exit 1
fi

BASE_URL="$(strip_cr "$BASE_URL")"
PROJECT_ID="$(strip_cr "$PROJECT_ID")"
BRANCH_NAME="$(strip_cr "$BRANCH_NAME")"
SPREADSHEET_ID="$(strip_cr "$SPREADSHEET_ID")"
SPREADSHEET_TITLE="$(strip_cr "$SPREADSHEET_TITLE")"
CONFIG_SHEET="$(strip_cr "$CONFIG_SHEET")"
RUN_SHEET="$(strip_cr "$RUN_SHEET")"
FOLDER_SEARCH_ENDPOINT="$(strip_cr "$FOLDER_SEARCH_ENDPOINT")"
FOLDERTREE_ENDPOINT="$(strip_cr "$FOLDERTREE_ENDPOINT")"
TREE_SOURCE_ENDPOINT="$(strip_cr "$TREE_SOURCE_ENDPOINT")"
TREE_SOURCE_METHOD="$(strip_cr "$TREE_SOURCE_METHOD")"
TREE_SOURCE_QUERY_JSON="$(strip_cr "$TREE_SOURCE_QUERY_JSON")"
TREE_SOURCE_BODY_JSON="$(strip_cr "$TREE_SOURCE_BODY_JSON")"
ENDPOINT="$(strip_cr "$ENDPOINT")"
QUERY_TEMPLATE="$(strip_cr "$QUERY_TEMPLATE")"
MAX_RESULTS="$(strip_cr "$MAX_RESULTS")"
START_AT="$(strip_cr "$START_AT")"
ARCHIVED="$(strip_cr "$ARCHIVED")"
FIELDS="$(strip_cr "$FIELDS")"
TESTCASE_ENDPOINT_TEMPLATE="$(strip_cr "$TESTCASE_ENDPOINT_TEMPLATE")"
SYNTHETIC_CYCLE_IDS="$(strip_cr "$SYNTHETIC_CYCLE_IDS")"
FROM_DATE="$(strip_cr "$FROM_DATE")"
TO_DATE="$(strip_cr "$TO_DATE")"
FOLDER_PARENT_ID="$(strip_cr "$FOLDER_PARENT_ID")"
FOLDER_PATH_REGEX="$(strip_cr "$FOLDER_PATH_REGEX")"
STATUS_PASS_NAME="$(strip_cr "$STATUS_PASS_NAME")"
STATUS_FAIL_NAME="$(strip_cr "$STATUS_FAIL_NAME")"
UPDATE_ENDPOINT_TEMPLATE="$(strip_cr "$UPDATE_ENDPOINT_TEMPLATE")"
UPDATE_METHOD="$(strip_cr "$UPDATE_METHOD")"
UPDATE_STATUS_ID_FIELD="$(strip_cr "$UPDATE_STATUS_ID_FIELD")"
UPDATE_COMMENT_FIELD="$(strip_cr "$UPDATE_COMMENT_FIELD")"
UPDATE_EXTRA_BODY_JSON="$(strip_cr "$UPDATE_EXTRA_BODY_JSON")"

cmd=(
  "$PYTHON_BIN"
  "$SCRIPT_DIR/zephyr_google_sheets_pipeline.py"
)

if [[ "$MODE" == "generate" ]]; then
  cmd+=(
    "generate-sheet"
    --base-url "$BASE_URL"
    --project-id "$PROJECT_ID"
    --branch-name "$BRANCH_NAME"
    --google-service-account-file "$GOOGLE_SERVICE_ACCOUNT_FILE"
    --spreadsheet-title "$SPREADSHEET_TITLE"
    --config-sheet "$CONFIG_SHEET"
    --run-sheet "$RUN_SHEET"
    --folder-search-endpoint "$FOLDER_SEARCH_ENDPOINT"
    --foldertree-endpoint "$FOLDERTREE_ENDPOINT"
    --endpoint "$ENDPOINT"
    --query-template "$QUERY_TEMPLATE"
    --extra-param "fields=$FIELDS"
    --extra-param "maxResults=$MAX_RESULTS"
    --extra-param "startAt=$START_AT"
    --extra-param "archived=$ARCHIVED"
    --testcase-endpoint-template "$TESTCASE_ENDPOINT_TEMPLATE"
    --status-pass-name "$STATUS_PASS_NAME"
    --status-fail-name "$STATUS_FAIL_NAME"
  )
  if [[ -n "$SPREADSHEET_ID" ]]; then
    cmd+=(--spreadsheet-id "$SPREADSHEET_ID")
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
  if [[ "$SYNTHETIC_CYCLE_IDS" == "true" ]]; then
    cmd+=(--synthetic-cycle-ids)
  fi
  if [[ -n "$FROM_DATE" ]]; then
    cmd+=(--from-date "$FROM_DATE")
  fi
  if [[ -n "$TO_DATE" ]]; then
    cmd+=(--to-date "$TO_DATE")
  fi
  if [[ -n "$FOLDER_PARENT_ID" ]]; then
    cmd+=(--folder-parent-id "$FOLDER_PARENT_ID")
  fi
  if [[ -n "$FOLDER_PATH_REGEX" ]]; then
    cmd+=(--folder-path-regex "$FOLDER_PATH_REGEX")
  fi
else
  if [[ -z "$SPREADSHEET_ID" ]]; then
    echo "Set GOOGLE_SPREADSHEET_ID for sync mode." >&2
    exit 1
  fi
  cmd+=(
    "sync-sheet"
    --base-url "$BASE_URL"
    --project-id "$PROJECT_ID"
    --branch-name "$BRANCH_NAME"
    --google-service-account-file "$GOOGLE_SERVICE_ACCOUNT_FILE"
    --spreadsheet-id "$SPREADSHEET_ID"
    --config-sheet "$CONFIG_SHEET"
    --run-sheet "$RUN_SHEET"
    --update-endpoint-template "$UPDATE_ENDPOINT_TEMPLATE"
    --update-method "$UPDATE_METHOD"
    --update-status-id-field "$UPDATE_STATUS_ID_FIELD"
    --update-comment-field "$UPDATE_COMMENT_FIELD"
    --status-pass-name "$STATUS_PASS_NAME"
    --status-fail-name "$STATUS_FAIL_NAME"
    --writeback
  )
  if [[ -n "$UPDATE_EXTRA_BODY_JSON" ]]; then
    cmd+=(--update-extra-body-json "$UPDATE_EXTRA_BODY_JSON")
  fi
fi

exec "${cmd[@]}"

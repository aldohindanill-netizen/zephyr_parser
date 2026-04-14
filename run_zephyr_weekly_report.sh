#!/usr/bin/env bash
# Linux/macOS launcher: mirrors run_zephyr_weekly_report.ps1 (same .env keys).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ./.env
  set +a
fi

if [[ -z "${ZEPHYR_API_TOKEN:-}" ]]; then
  echo "Set ZEPHYR_API_TOKEN in the environment or .env before running." >&2
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"

PROJECT_ID="${ZEPHYR_PROJECT_ID:-10904}"
BASE_URL="${ZEPHYR_BASE_URL:-https://jira.navio.auto}"
ENDPOINT="${ZEPHYR_ENDPOINT:-rest/tests/1.0/testrun/search}"
FOLDER_ENDPOINT="${ZEPHYR_FOLDER_ENDPOINT:-rest/tests/1.0/foldertree}"
FOLDER_SEARCH_ENDPOINT="${ZEPHYR_FOLDER_SEARCH_ENDPOINT:-rest/tests/1.0/folder/search}"
FOLDERTREE_ENDPOINT="${ZEPHYR_FOLDERTREE_ENDPOINT:-rest/tests/1.0/project/${PROJECT_ID}/foldertree/testrun}"
OUTPUT="${ZEPHYR_OUTPUT:-weekly_zephyr_report.csv}"
PER_FOLDER_DIR="${ZEPHYR_PER_FOLDER_DIR:-reports/by_folder}"
PAGE_SIZE="${ZEPHYR_PAGE_SIZE:-100}"
MAX_RESULTS="${ZEPHYR_MAX_RESULTS:-40}"
START_AT="${ZEPHYR_START_AT:-0}"
ARCHIVED="${ZEPHYR_ARCHIVED:-false}"
DATE_FIELD="${ZEPHYR_DATE_FIELD:-updatedOn}"
STATUS_FIELD="${ZEPHYR_STATUS_FIELD:-status.name}"
DISCOVERY_MODE="${ZEPHYR_DISCOVERY_MODE:-tree}"
ROOT_FOLDER_IDS="${ZEPHYR_ROOT_FOLDER_IDS:-}"
FOLDER_NAME_REGEX="${ZEPHYR_FOLDER_NAME_REGEX:-}"
FOLDER_PATH_REGEX="${ZEPHYR_FOLDER_PATH_REGEX:-}"
TREE_NAME_REGEX="${ZEPHYR_TREE_NAME_REGEX:-}"
TREE_ROOT_PATH_REGEX="${ZEPHYR_TREE_ROOT_PATH_REGEX:-}"
TREE_LEAF_ONLY="${ZEPHYR_TREE_LEAF_ONLY:-true}"
TREE_AUTOPROBE="${ZEPHYR_TREE_AUTOPROBE:-true}"
QUERY_TEMPLATE="${ZEPHYR_QUERY_TEMPLATE:-testRun.projectId IN (${PROJECT_ID}) AND testRun.folderTreeId IN ({folder_id}) ORDER BY testRun.name ASC}"
PROJECT_QUERY="${ZEPHYR_PROJECT_QUERY:-testRun.projectId IN (${PROJECT_ID}) ORDER BY testRun.name ASC}"
WEEKLY_CYCLE_MATRIX_OUTPUT="${ZEPHYR_WEEKLY_CYCLE_MATRIX_OUTPUT:-reports/weekly_cycle_matrix.csv}"
EXPORT_WEEKLY_READABLE="${ZEPHYR_EXPORT_WEEKLY_READABLE:-true}"
WEEKLY_READABLE_DIR="${ZEPHYR_WEEKLY_READABLE_DIR:-reports/weekly_readable}"
WEEKLY_READABLE_FORMATS="${ZEPHYR_WEEKLY_READABLE_FORMATS:-html,wiki}"
EXPORT_DAILY_READABLE="${ZEPHYR_EXPORT_DAILY_READABLE:-false}"
DAILY_READABLE_DIR="${ZEPHYR_DAILY_READABLE_DIR:-reports/daily_readable}"
DAILY_READABLE_FORMATS="${ZEPHYR_DAILY_READABLE_FORMATS:-html,wiki}"
CYCLE_PROGRESS_OUTPUT="${ZEPHYR_CYCLE_PROGRESS_OUTPUT:-reports/cycle_progress.csv}"
FIELDS="${ZEPHYR_FIELDS:-id,key,name,folderId,iterationId,projectVersionId,environmentId,userKeys,environmentIds,plannedStartDate,plannedEndDate,executionTime,estimatedTime,testResultStatuses,testCaseCount,issueCount,status(id,name,i18nKey,color),customFieldValues,createdOn,createdBy,updatedOn,updatedBy,owner}"
TOKEN_HEADER="${ZEPHYR_TOKEN_HEADER:-Authorization}"
TOKEN_PREFIX="${ZEPHYR_TOKEN_PREFIX:-Bearer}"
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
CONFLUENCE_UPDATE_EXISTING="${CONFLUENCE_UPDATE_EXISTING:-false}"

ARGS=(
  "${SCRIPT_DIR}/zephyr_weekly_report.py"
  "--base-url" "$BASE_URL"
  "--endpoint" "$ENDPOINT"
  "--discover-folders"
  "--discovery-mode" "$DISCOVERY_MODE"
  "--folder-endpoint" "$FOLDER_ENDPOINT"
  "--folder-search-endpoint" "$FOLDER_SEARCH_ENDPOINT"
  "--foldertree-endpoint" "$FOLDERTREE_ENDPOINT"
  "--project-id" "$PROJECT_ID"
  "--query-template" "$QUERY_TEMPLATE"
  "--project-query" "$PROJECT_QUERY"
  "--token" "$ZEPHYR_API_TOKEN"
  "--token-header" "$TOKEN_HEADER"
  "--token-prefix" "$TOKEN_PREFIX"
  "--page-size" "$PAGE_SIZE"
  "--output" "$OUTPUT"
  "--per-folder-dir" "$PER_FOLDER_DIR"
  "--extra-param" "fields=$FIELDS"
  "--extra-param" "maxResults=$MAX_RESULTS"
  "--extra-param" "startAt=$START_AT"
  "--extra-param" "archived=$ARCHIVED"
  "--date-field" "$DATE_FIELD"
  "--status-field" "$STATUS_FIELD"
  "--cycle-progress-output" "$CYCLE_PROGRESS_OUTPUT"
  "--weekly-cycle-matrix-output" "$WEEKLY_CYCLE_MATRIX_OUTPUT"
)

if [[ -n "${ZEPHYR_ROLLING_DAYS:-}" ]] && [[ "${ZEPHYR_ROLLING_DAYS}" =~ ^[0-9]+$ ]] && (( ZEPHYR_ROLLING_DAYS > 0 )); then
  ARGS+=(--rolling-days "$ZEPHYR_ROLLING_DAYS")
else
  if [[ -n "${ZEPHYR_FROM_DATE:-}" ]]; then
    ARGS+=(--from-date "$ZEPHYR_FROM_DATE")
  fi
  if [[ -n "${ZEPHYR_TO_DATE:-}" ]]; then
    ARGS+=(--to-date "$ZEPHYR_TO_DATE")
  fi
fi

if [[ "$TREE_LEAF_ONLY" == "true" ]]; then
  ARGS+=(--tree-leaf-only)
fi
if [[ "$TREE_AUTOPROBE" == "true" ]]; then
  ARGS+=(--tree-autoprobe)
fi
if [[ -n "$TREE_NAME_REGEX" ]]; then
  ARGS+=(--tree-name-regex "$TREE_NAME_REGEX")
fi
if [[ -n "$TREE_ROOT_PATH_REGEX" ]]; then
  ARGS+=(--tree-root-path-regex "$TREE_ROOT_PATH_REGEX")
fi
if [[ -n "$FOLDER_NAME_REGEX" ]]; then
  ARGS+=(--folder-name-regex "$FOLDER_NAME_REGEX")
fi
if [[ -n "$FOLDER_PATH_REGEX" ]]; then
  ARGS+=(--folder-path-regex "$FOLDER_PATH_REGEX")
fi

if [[ -n "$ROOT_FOLDER_IDS" ]]; then
  _ifs="$IFS"
  IFS=','
  # shellcheck disable=SC2206
  _parts=($ROOT_FOLDER_IDS)
  IFS="$_ifs"
  for rootId in "${_parts[@]}"; do
    rootId="${rootId#"${rootId%%[![:space:]]*}"}"
    rootId="${rootId%"${rootId##*[![:space:]]}"}"
    if [[ -n "$rootId" ]]; then
      ARGS+=(--root-folder-id "$rootId")
    fi
  done
fi

if [[ "$EXPORT_WEEKLY_READABLE" == "true" ]]; then
  ARGS+=(--export-weekly-readable --weekly-readable-dir "$WEEKLY_READABLE_DIR")
  _ifs="$IFS"
  IFS=','
  # shellcheck disable=SC2206
  _fmts=($WEEKLY_READABLE_FORMATS)
  IFS="$_ifs"
  for fmt in "${_fmts[@]}"; do
    fmt="${fmt#"${fmt%%[![:space:]]*}"}"
    fmt="${fmt%"${fmt##*[![:space:]]}"}"
    if [[ -n "$fmt" ]]; then
      ARGS+=(--weekly-readable-format "$fmt")
    fi
  done
fi

if [[ "$EXPORT_DAILY_READABLE" == "true" ]]; then
  ARGS+=(--export-daily-readable --daily-readable-dir "$DAILY_READABLE_DIR")
  _ifs="$IFS"
  IFS=','
  # shellcheck disable=SC2206
  _dfmts=($DAILY_READABLE_FORMATS)
  IFS="$_ifs"
  for fmt in "${_dfmts[@]}"; do
    fmt="${fmt#"${fmt%%[![:space:]]*}"}"
    fmt="${fmt%"${fmt##*[![:space:]]}"}"
    if [[ -n "$fmt" ]]; then
      ARGS+=(--daily-readable-format "$fmt")
    fi
  done
fi

if [[ "$CONFLUENCE_PUBLISH_DAILY" == "true" ]]; then
  ARGS+=(--publish-confluence-daily)
fi
if [[ "$CONFLUENCE_PUBLISH_WEEKLY" == "true" ]]; then
  ARGS+=(--publish-confluence-weekly)
fi
if [[ -n "$CONFLUENCE_BASE_URL" ]]; then
  ARGS+=(--confluence-base-url "$CONFLUENCE_BASE_URL")
fi
if [[ -n "$CONFLUENCE_SPACE_KEY" ]]; then
  ARGS+=(--confluence-space-key "$CONFLUENCE_SPACE_KEY")
fi
if [[ -n "$CONFLUENCE_PARENT_PAGE_ID" ]]; then
  ARGS+=(--confluence-parent-page-id "$CONFLUENCE_PARENT_PAGE_ID")
fi
if [[ -n "$CONFLUENCE_USERNAME" ]]; then
  ARGS+=(--confluence-username "$CONFLUENCE_USERNAME")
fi
if [[ -n "$CONFLUENCE_API_TOKEN" ]]; then
  ARGS+=(--confluence-api-token "$CONFLUENCE_API_TOKEN")
fi
if [[ "$CONFLUENCE_AUTH_MODE" =~ ^(auto|basic|bearer)$ ]]; then
  ARGS+=(--confluence-auth-mode "$CONFLUENCE_AUTH_MODE")
fi
if [[ "$CONFLUENCE_VERIFY_SSL" == "true" || "$CONFLUENCE_VERIFY_SSL" == "false" ]]; then
  ARGS+=(--confluence-verify-ssl "$CONFLUENCE_VERIFY_SSL")
fi
if [[ "$CONFLUENCE_DRY_RUN" == "true" ]]; then
  ARGS+=(--confluence-dry-run)
fi
if [[ "$CONFLUENCE_UPDATE_EXISTING" == "true" ]]; then
  ARGS+=(--confluence-update-existing)
fi

if [[ -n "${ZEPHYR_EXTRA_PARAMS:-}" ]]; then
  _ifs="$IFS"
  IFS=','
  # shellcheck disable=SC2206
  _extras=($ZEPHYR_EXTRA_PARAMS)
  IFS="$_ifs"
  for param in "${_extras[@]}"; do
    param="${param#"${param%%[![:space:]]*}"}"
    param="${param%"${param##*[![:space:]]}"}"
    if [[ -n "$param" ]]; then
      ARGS+=(--extra-param "$param")
    fi
  done
fi

if [[ -n "${ZEPHYR_LOOP_INTERVAL_MINUTES:-}" ]]; then
  ARGS+=(--loop-interval-minutes "$ZEPHYR_LOOP_INTERVAL_MINUTES")
fi
if [[ -n "${ZEPHYR_RUN_LOCK_FILE:-}" ]]; then
  ARGS+=(--run-lock-file "$ZEPHYR_RUN_LOCK_FILE")
fi

if [[ "$#" -gt 0 ]]; then
  ARGS+=("$@")
fi

exec "$PYTHON_BIN" "${ARGS[@]}"

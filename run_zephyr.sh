#!/usr/bin/env bash
# Unix launcher aligned with run_zephyr.ps1 (Jira / Zephyr tree workflow).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/PIPELINE_VERSION" ]]; then
  read -r pipeline_version < "$SCRIPT_DIR/PIPELINE_VERSION" || true
  pipeline_version="${pipeline_version//$'\r'/}"
  if [[ -n "${pipeline_version//[[:space:]]/}" ]]; then
    echo "Pipeline: $pipeline_version"
  fi
fi
ENV_FILE="$SCRIPT_DIR/.env"
ENV_EXAMPLE="$SCRIPT_DIR/.env.example"
ENV_SECRETS_FILE="$SCRIPT_DIR/.env.secrets"
ENV_SECRETS_EXAMPLE="$SCRIPT_DIR/.env.secrets.example"

env_enabled() {
  local raw="${1:-}"
  local default="${2:-false}"
  if [[ -z "${raw//[[:space:]]/}" ]]; then
    [[ "$default" == "true" ]]
    return
  fi
  raw=$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')
  case "$raw" in
    1 | true | yes | y | on) return 0 ;;
    *) return 1 ;;
  esac
}

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

if [[ ! -f "$ENV_FILE" ]]; then
  printf '%s\n' \
    "Missing configuration file: $ENV_FILE" \
    "" \
    "Copy the template and fill in secrets (at least ZEPHYR_API_TOKEN):" \
    "  cp \"$ENV_EXAMPLE\" \"$ENV_FILE\"" \
    >&2
  exit 1
fi

load_env_file "$ENV_FILE"
if [[ ! -f "$ENV_SECRETS_FILE" ]]; then
  printf '%s\n' \
    "Missing secrets file: $ENV_SECRETS_FILE" \
    "" \
    "Copy the template and fill in secret values:" \
    "  cp \"$ENV_SECRETS_EXAMPLE\" \"$ENV_SECRETS_FILE\"" \
    >&2
  exit 1
fi
load_env_file "$ENV_SECRETS_FILE"
export ZEPHYR_CONFLUENCE_AUTH_SCHEME="${ZEPHYR_CONFLUENCE_AUTH_SCHEME:-bearer}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"
export ZEPHYR_RUN_LOCK_FILE="${ZEPHYR_RUN_LOCK_FILE:-$SCRIPT_DIR/reports/.zephyr_weekly_report.lock}"

report_script="$SCRIPT_DIR/zephyr_weekly_report.py"
template_dir="$SCRIPT_DIR/report_templates/readable"

cmd=(
  "$report_script"
  --base-url "${ZEPHYR_BASE_URL:-}"
  --endpoint "${ZEPHYR_ENDPOINT:-}"
  --discover-folders
  --discovery-mode "${ZEPHYR_DISCOVERY_MODE:-tree}"
  --folder-endpoint "${ZEPHYR_FOLDER_ENDPOINT:-}"
  --folder-search-endpoint "${ZEPHYR_FOLDER_SEARCH_ENDPOINT:-}"
  --foldertree-endpoint "${ZEPHYR_FOLDERTREE_ENDPOINT:-}"
  --project-id "${ZEPHYR_PROJECT_ID:-}"
  --query-template "${ZEPHYR_QUERY_TEMPLATE:-}"
  --project-query "${ZEPHYR_PROJECT_QUERY:-}"
  --extra-param "fields=${ZEPHYR_FIELDS:-}"
  --extra-param "maxResults=${ZEPHYR_MAX_RESULTS:-}"
  --extra-param "startAt=${ZEPHYR_START_AT:-}"
  --extra-param "archived=${ZEPHYR_ARCHIVED:-}"
  --date-field "${ZEPHYR_DATE_FIELD:-}"
  --status-field "${ZEPHYR_STATUS_FIELD:-}"
  --output "${ZEPHYR_OUTPUT:-weekly_zephyr_report.csv}"
  --per-folder-dir "${ZEPHYR_PER_FOLDER_DIR:-reports/by_folder}"
  --root-folder-id "${ZEPHYR_ROOT_FOLDER_IDS:-}"
  --tree-leaf-only
  --tree-name-regex "${ZEPHYR_TREE_NAME_REGEX:-}"
  --folder-name-endpoint-template "${ZEPHYR_FOLDER_NAME_ENDPOINT_TEMPLATE:-}"
  --cycles-cases-output "${ZEPHYR_CYCLES_CASES_OUTPUT:-reports/cycles_and_cases.csv}"
  --testcase-endpoint-template "${ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE:-}"
  --case-steps-output "${ZEPHYR_CASE_STEPS_OUTPUT:-reports/case_steps.csv}"
  --daily-readable-dir "${ZEPHYR_DAILY_READABLE_DIR:-reports/daily_readable}"
  --readable-template-dir "$template_dir"
  --cycle-progress-output "${ZEPHYR_CYCLE_PROGRESS_OUTPUT:-reports/cycle_progress.csv}"
  --weekly-cycle-matrix-output "${ZEPHYR_WEEKLY_CYCLE_MATRIX_OUTPUT:-reports/weekly_cycle_matrix.csv}"
  --continue-on-folder-error
)

if [[ -n "${ZEPHYR_FOLDER_WORKERS:-}" ]]; then
  cmd+=(--folder-workers "${ZEPHYR_FOLDER_WORKERS}")
fi
if [[ -n "${ZEPHYR_DETAIL_WORKERS:-}" ]]; then
  cmd+=(--detail-workers "${ZEPHYR_DETAIL_WORKERS}")
fi

if env_enabled "${ZEPHYR_EXPORT_CYCLES_CASES:-}" true; then
  cmd+=(--export-cycles-cases)
fi
if env_enabled "${ZEPHYR_SYNTHETIC_CYCLE_IDS:-}" true; then
  cmd+=(--synthetic-cycle-ids)
fi
if env_enabled "${ZEPHYR_EXPORT_CASE_STEPS:-}" true; then
  cmd+=(--export-case-steps)
fi
if env_enabled "${ZEPHYR_EXPORT_DAILY_READABLE:-}" true; then
  cmd+=(--export-daily-readable)
  fmts_raw="${ZEPHYR_DAILY_READABLE_FORMATS:-html,wiki}"
  IFS=',' read -r -a fmt_parts <<<"$fmts_raw"
  for part in "${fmt_parts[@]}"; do
    f="${part#"${part%%[![:space:]]*}"}"
    f="${f%"${f##*[![:space:]]}"}"
    f=$(printf '%s' "$f" | tr '[:upper:]' '[:lower:]')
    case "$f" in
      html | wiki) cmd+=(--daily-readable-format "$f") ;;
    esac
  done
fi
if env_enabled "${ZEPHYR_EXPORT_WEEKLY_READABLE:-}" true; then
  cmd+=(--export-weekly-readable)
  if [[ -n "${ZEPHYR_WEEKLY_READABLE_DIR:-}" ]]; then
    cmd+=(--weekly-readable-dir "${ZEPHYR_WEEKLY_READABLE_DIR}")
  fi
  fmts_raw="${ZEPHYR_WEEKLY_READABLE_FORMATS:-html,wiki}"
  IFS=',' read -r -a fmt_parts <<<"$fmts_raw"
  for part in "${fmt_parts[@]}"; do
    f="${part#"${part%%[![:space:]]*}"}"
    f="${f%"${f##*[![:space:]]}"}"
    f=$(printf '%s' "$f" | tr '[:upper:]' '[:lower:]')
    case "$f" in
      html | wiki) cmd+=(--weekly-readable-format "$f") ;;
    esac
  done
fi

if [[ "${ZEPHYR_DISCOVERY_MODE:-tree}" == "executions" ]]; then
  cmd+=(--discover-from-executions)
fi
if [[ -n "${ZEPHYR_TREE_ROOT_PATH_REGEX:-}" ]]; then
  cmd+=(--tree-root-path-regex "${ZEPHYR_TREE_ROOT_PATH_REGEX}")
fi
if env_enabled "${ZEPHYR_TREE_AUTOPROBE:-}" false; then
  cmd+=(--tree-autoprobe)
fi
if [[ -n "${ZEPHYR_TREE_SOURCE_ENDPOINT:-}" ]]; then
  cmd+=(--tree-source-endpoint "${ZEPHYR_TREE_SOURCE_ENDPOINT}")
  cmd+=(--tree-source-method "${ZEPHYR_TREE_SOURCE_METHOD:-GET}")
  [[ -n "${ZEPHYR_TREE_SOURCE_QUERY_JSON:-}" ]] && cmd+=(--tree-source-query-json "${ZEPHYR_TREE_SOURCE_QUERY_JSON}")
  [[ -n "${ZEPHYR_TREE_SOURCE_BODY_JSON:-}" ]] && cmd+=(--tree-source-body-json "${ZEPHYR_TREE_SOURCE_BODY_JSON}")
fi
if env_enabled "${ZEPHYR_CREATE_FOLDER_FIRST:-}" false; then
  cmd+=(--create-folder-first)
  cmd+=(--create-folder-endpoint "${ZEPHYR_CREATE_FOLDER_ENDPOINT:-rest/tests/1.0/folder}")
  cmd+=(--create-folder-name-field "${ZEPHYR_CREATE_FOLDER_NAME_FIELD:-name}")
  cmd+=(--create-folder-project-id-field "${ZEPHYR_CREATE_FOLDER_PROJECT_ID_FIELD:-projectId}")
  cmd+=(--create-folder-parent-id-field "${ZEPHYR_CREATE_FOLDER_PARENT_ID_FIELD:-parentId}")
  [[ -n "${ZEPHYR_CREATE_FOLDER_NAME:-}" ]] && cmd+=(--create-folder-name "${ZEPHYR_CREATE_FOLDER_NAME}")
  [[ -n "${ZEPHYR_CREATE_FOLDER_NAME_TEMPLATE:-}" ]] && cmd+=(--create-folder-name-template "${ZEPHYR_CREATE_FOLDER_NAME_TEMPLATE}")
  [[ -n "${ZEPHYR_CREATE_FOLDER_PARENT_ID:-}" ]] && cmd+=(--create-folder-parent-id "${ZEPHYR_CREATE_FOLDER_PARENT_ID}")
  [[ -n "${ZEPHYR_CREATE_FOLDER_BODY_JSON:-}" ]] && cmd+=(--create-folder-body-json "${ZEPHYR_CREATE_FOLDER_BODY_JSON}")
  env_enabled "${ZEPHYR_CREATE_FOLDER_DRY_RUN:-}" false && cmd+=(--create-folder-dry-run)
  env_enabled "${ZEPHYR_CREATE_FOLDER_USE_AS_ROOT:-}" false && cmd+=(--create-folder-use-as-root)
fi
if [[ -n "${ZEPHYR_ALLOWED_ROOT_FOLDER_IDS:-}" ]]; then
  IFS=',' read -r -a allowed_ids <<<"${ZEPHYR_ALLOWED_ROOT_FOLDER_IDS}"
  for folder_id in "${allowed_ids[@]}"; do
    trimmed="${folder_id//[[:space:]]/}"
    [[ -n "$trimmed" ]] && cmd+=(--allowed-root-folder-id "$trimmed")
  done
fi
if [[ -n "${ZEPHYR_FOLDER_PATH_REGEX:-}" ]]; then
  cmd+=(--folder-path-regex "${ZEPHYR_FOLDER_PATH_REGEX}")
fi
if [[ -n "${ZEPHYR_FOLDER_NAME_REGEX:-}" ]]; then
  cmd+=(--folder-name-regex "${ZEPHYR_FOLDER_NAME_REGEX}")
fi
if env_enabled "${ZEPHYR_DEBUG_FOLDER_FIELDS:-}" false; then
  cmd+=(--debug-folder-fields)
fi
if [[ -n "${ZEPHYR_LOOP_INTERVAL_MINUTES:-}" ]]; then
  cmd+=(--loop-interval-minutes "${ZEPHYR_LOOP_INTERVAL_MINUTES}")
fi
if [[ -n "${ZEPHYR_RUN_LOCK_FILE:-}" ]]; then
  cmd+=(--run-lock-file "${ZEPHYR_RUN_LOCK_FILE}")
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
exec "$py" -u "${cmd[@]}"

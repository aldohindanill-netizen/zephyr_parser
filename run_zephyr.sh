#!/usr/bin/env bash
# Unix launcher aligned with run_zephyr.ps1 (Jira / Zephyr tree workflow).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
ENV_EXAMPLE="$SCRIPT_DIR/.env.example"

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
export ZEPHYR_CONFLUENCE_AUTH_SCHEME="${ZEPHYR_CONFLUENCE_AUTH_SCHEME:-bearer}"

report_script="$SCRIPT_DIR/zephyr_weekly_report.py"
template_dir="$SCRIPT_DIR/report_templates/readable"

cmd=(
  "$report_script"
  --base-url "${ZEPHYR_BASE_URL:-}"
  --endpoint "${ZEPHYR_ENDPOINT:-}"
  --discover-folders
  --discovery-mode "${ZEPHYR_DISCOVERY_MODE:-}"
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
  --output "${ZEPHYR_OUTPUT:-}"
  --per-folder-dir "${ZEPHYR_PER_FOLDER_DIR:-}"
  --root-folder-id "${ZEPHYR_ROOT_FOLDER_IDS:-}"
  --tree-leaf-only
  --tree-name-regex "${ZEPHYR_TREE_NAME_REGEX:-}"
  --folder-name-endpoint-template "${ZEPHYR_FOLDER_NAME_ENDPOINT_TEMPLATE:-}"
  --export-cycles-cases
  --cycles-cases-output "${ZEPHYR_CYCLES_CASES_OUTPUT:-}"
  --testcase-endpoint-template "${ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE:-}"
  --synthetic-cycle-ids
  --export-case-steps
  --case-steps-output "${ZEPHYR_CASE_STEPS_OUTPUT:-}"
  --export-daily-readable
  --daily-readable-dir "${ZEPHYR_DAILY_READABLE_DIR:-}"
  --daily-readable-format "html"
  --daily-readable-format "wiki"
  --readable-template-dir "$template_dir"
  --continue-on-folder-error
)

if [[ "${ZEPHYR_EXPORT_WEEKLY_READABLE:-}" == "true" ]]; then
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

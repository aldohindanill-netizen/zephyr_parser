"""Merge ZEPHYR_* from infra/.env.nocodb-n8n into zephyr_ingest_15m Build_Config and print output path."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV = ROOT / "infra" / ".env.nocodb-n8n"
WF = ROOT / "workflows" / "zephyr_ingest_15m.json"
OUT = ROOT / "infra" / ".zephyr_ingest_15m.runtime.json"


def parse_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        v = v.strip()
        if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')):
            v = v[1:-1]
        out[k.strip()] = v
    return out


def main() -> int:
    if not ENV.is_file():
        print("Missing", ENV, file=sys.stderr)
        return 1
    env = parse_env(ENV)
    token = env.get("ZEPHYR_API_TOKEN", "")
    if not token or token == "replace_with_real_token":
        print("ZEPHYR_API_TOKEN not set in infra/.env.nocodb-n8n", file=sys.stderr)
        return 1
    base = env.get("ZEPHYR_BASE_URL", "https://jira.navio.auto")
    project_id = env.get("ZEPHYR_PROJECT_ID", "")
    if not project_id:
        print("ZEPHYR_PROJECT_ID not set in infra/.env.nocodb-n8n", file=sys.stderr)
        return 1
    folder_search = env.get("ZEPHYR_FOLDER_SEARCH_ENDPOINT", "rest/tests/1.0/folder/search")
    foldertree = env.get("ZEPHYR_FOLDERTREE_ENDPOINT", "rest/tests/1.0/foldertree")
    leaf_only = env.get("ZEPHYR_FOLDER_LEAF_ONLY", "false").lower() in ("1", "true", "yes")
    root_ids = env.get("ZEPHYR_FOLDER_ROOT_IDS", "").strip()
    name_rx = env.get("ZEPHYR_FOLDER_NAME_REGEX", "").strip()
    path_rx = env.get("ZEPHYR_FOLDER_PATH_REGEX", "").strip()
    testrun_endpoint = env.get("ZEPHYR_ENDPOINT", "rest/tests/1.0/testrun/search").strip()
    query_template = env.get(
        "ZEPHYR_QUERY_TEMPLATE",
        "testRun.projectId IN ({project_id}) AND testRun.folderTreeId IN ({folder_id}) ORDER BY testRun.name ASC",
    ).strip()
    testresult_endpoint_tpl = env.get(
        "ZEPHYR_TESTRESULT_ENDPOINT_TEMPLATE",
        "rest/tests/1.0/testrun/{test_run_id}/testrunitems",
    ).strip()
    testresult_fields = env.get(
        "ZEPHYR_TESTRESULT_FIELDS",
        "id,index,$lastTestResult",
    ).strip()
    testrun_fields = env.get(
        "ZEPHYR_TESTRUN_FIELDS",
        "id,key,name,folderId,iterationId,projectVersionId,environmentId,userKeys,environmentIds,plannedStartDate,plannedEndDate,executionTime,estimatedTime,testResultStatuses,testCaseCount,issueCount,status(id,name,i18nKey,color),customFieldValues,createdOn,createdBy,updatedOn,updatedBy,owner",
    ).strip()
    max_results = env.get("ZEPHYR_MAX_RESULTS", "200").strip()
    start_at = env.get("ZEPHYR_START_AT", "0").strip()
    archived = env.get("ZEPHYR_ARCHIVED", "false").strip()

    obj = json.loads(WF.read_text(encoding="utf-8"))
    js = (
        "return [{json: {\n"
        f"  baseUrl: {json.dumps(base)},\n"
        f"  projectId: {json.dumps(project_id)},\n"
        f"  token: {json.dumps(token)},\n"
        f"  folderSearchEndpoint: {json.dumps(folder_search)},\n"
        f"  foldertreeEndpoint: {json.dumps(foldertree)},\n"
        f"  folderLeafOnly: {json.dumps(leaf_only)},\n"
        f"  folderRootIds: {json.dumps(root_ids)},\n"
        f"  folderNameRegex: {json.dumps(name_rx)},\n"
        f"  folderPathRegex: {json.dumps(path_rx)},\n"
        f"  testrunSearchEndpoint: {json.dumps(testrun_endpoint)},\n"
        f"  testrunFields: {json.dumps(testrun_fields)},\n"
        f"  testresultEndpointTemplate: {json.dumps(testresult_endpoint_tpl)},\n"
        f"  testresultFields: {json.dumps(testresult_fields)},\n"
        f"  queryTemplate: {json.dumps(query_template)},\n"
        f"  maxResults: {json.dumps(max_results)},\n"
        f"  startAt: {json.dumps(start_at)},\n"
        f"  archived: {json.dumps(archived)},\n"
        "}}];"
    )
    for n in obj.get("nodes", []):
        if n.get("name") == "Build_Config":
            n.setdefault("parameters", {})["jsCode"] = js
            break
    else:
        print("Build_Config node not found", file=sys.stderr)
        return 1
    OUT.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(OUT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

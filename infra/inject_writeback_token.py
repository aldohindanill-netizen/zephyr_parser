"""Merge ZEPHYR_* from infra/.env.nocodb-n8n into zephyr_writeback_15m Build_Config and print output path."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV = ROOT / "infra" / ".env.nocodb-n8n"
WF = ROOT / "workflows" / "zephyr_writeback_15m.json"
OUT = ROOT / "infra" / ".zephyr_writeback_15m.runtime.json"


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
    upd_tpl = env.get(
        "ZEPHYR_UPDATE_ENDPOINT_TEMPLATE",
        "rest/tests/1.0/testresult/{test_result_id}",
    )
    upd_method = env.get("ZEPHYR_UPDATE_METHOD", "PUT")
    status_field = env.get("ZEPHYR_UPDATE_STATUS_ID_FIELD", "testResultStatusId")
    comment_field = env.get("ZEPHYR_UPDATE_COMMENT_FIELD", "comment")

    obj = json.loads(WF.read_text(encoding="utf-8"))
    js = (
        "return [{json: {\n"
        f"  baseUrl: {json.dumps(base)},\n"
        f"  token: {json.dumps(token)},\n"
        f"  updateTemplate: {json.dumps(upd_tpl)},\n"
        f"  updateMethod: {json.dumps(upd_method)},\n"
        f"  statusField: {json.dumps(status_field)},\n"
        f"  commentField: {json.dumps(comment_field)},\n"
        "  maxAttempts: 5\n"
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

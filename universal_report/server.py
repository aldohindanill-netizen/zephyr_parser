"""Flask API and static UI for universal reports."""

from __future__ import annotations

import os
import traceback
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from repo_env import load_repo_env_for_scripts
from universal_report.builder import (
    build_universal_html,
    render_cycles_to_draft_cycles,
    write_universal_reports,
)
from universal_report.confluence_publish import output_dir, publish_universal_report
from universal_report.drafts import create_draft, delete_draft, list_drafts, load_draft, save_draft
from universal_report.schema import new_draft, normalize_draft
from universal_report.zephyr_import import (
    fetch_cycles_by_import_mode,
    list_cycles_in_folder,
    list_zephyr_folders_with_meta,
)

_WEB_DIR = Path(__file__).resolve().parent / "web"
_PACKAGE_DIR = Path(__file__).resolve().parent
_FOLDER_API_VERSION = "full-tree-v2"


def _server_host() -> str:
    return (os.getenv("ZEPHYR_UNIVERSAL_HOST") or "127.0.0.1").strip()


def _server_port() -> int:
    return int((os.getenv("ZEPHYR_UNIVERSAL_PORT") or "8765").strip())


def _api_error_response(exc: Exception) -> tuple[dict[str, str], int]:
    print(f"API error: {exc}", file=__import__("sys").stderr)
    if (os.getenv("ZEPHYR_UNIVERSAL_DEBUG") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return {"error": str(exc), "trace": traceback.format_exc()}, 500
    return {"error": str(exc)}, 500


def create_app() -> Flask:
    load_repo_env_for_scripts(use_local_env=True)
    app = Flask(__name__, static_folder=str(_WEB_DIR), static_url_path="/static")

    @app.get("/")
    def index() -> object:
        return send_from_directory(_WEB_DIR, "index.html")

    @app.get("/api/health")
    def api_health() -> object:
        return jsonify(
            {
                "ok": True,
                "folder_api_version": _FOLDER_API_VERSION,
                "package_dir": str(_PACKAGE_DIR),
            }
        )

    @app.get("/api/defaults")
    def api_defaults() -> object:
        return jsonify(new_draft())

    @app.get("/api/drafts")
    def api_list_drafts() -> object:
        return jsonify(list_drafts())

    @app.get("/api/drafts/<draft_id>")
    def api_get_draft(draft_id: str) -> object:
        try:
            return jsonify(load_draft(draft_id))
        except FileNotFoundError:
            return jsonify({"error": "Draft not found"}), 404

    @app.post("/api/drafts")
    def api_save_draft() -> object:
        payload = request.get_json(silent=True) or {}
        saved = save_draft(payload)
        return jsonify(saved)

    @app.post("/api/drafts/new")
    def api_new_draft() -> object:
        payload = request.get_json(silent=True) or {}
        created = create_draft(
            title=str(payload.get("title") or ""),
            report_date=str(payload.get("report_date") or "") or None,
            build_name=str(payload.get("build_name") or ""),
        )
        return jsonify(created)

    @app.delete("/api/drafts/<draft_id>")
    def api_delete_draft(draft_id: str) -> object:
        try:
            delete_draft(draft_id)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify({"ok": True})

    @app.post("/api/preview")
    def api_preview() -> object:
        draft = normalize_draft(request.get_json(silent=True) or {})
        html_body = build_universal_html(draft)
        return jsonify({"html": html_body})

    @app.post("/api/build")
    def api_build() -> object:
        draft = normalize_draft(request.get_json(silent=True) or {})
        out_dir = str(output_dir())
        paths = write_universal_reports(draft, out_dir, formats={"html", "wiki"})
        save_draft(draft)
        return jsonify({"paths": paths, "output_dir": out_dir})

    @app.after_request
    def _disable_static_cache(response: object) -> object:
        path = request.path or ""
        if path == "/" or path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
        return response

    @app.get("/api/zephyr/folders")
    def api_zephyr_folders() -> object:
        try:
            payload = list_zephyr_folders_with_meta(scope="all")
            payload["folder_api_version"] = _FOLDER_API_VERSION
            return jsonify(payload)
        except Exception as exc:  # noqa: BLE001
            body, status = _api_error_response(exc)
            return jsonify(body), status

    @app.get("/api/zephyr/folders/<folder_id>/cycles")
    def api_zephyr_folder_cycles(folder_id: str) -> object:
        folder_name = (request.args.get("folder_name") or "").strip()
        from_date = (request.args.get("from_date") or "").strip() or None
        to_date = (request.args.get("to_date") or "").strip() or None
        try:
            cycles = list_cycles_in_folder(
                folder_id,
                folder_name,
                from_date=from_date,
                to_date=to_date,
            )
            return jsonify({"cycles": cycles, "folder_id": folder_id})
        except Exception as exc:  # noqa: BLE001
            body, status = _api_error_response(exc)
            return jsonify(body), status

    @app.post("/api/zephyr/import")
    def api_zephyr_import() -> object:
        payload = request.get_json(silent=True) or {}
        import_mode = str(payload.get("import_mode") or "folder").strip().lower()
        folder_id = str(payload.get("folder_id") or "").strip()
        folder_name = str(payload.get("folder_name") or "").strip()
        cycle_key = str(payload.get("cycle_key") or "").strip()
        test_run_id = str(payload.get("test_run_id") or "").strip()
        selected_cycles_raw = payload.get("selected_cycles") or []
        selected_cycles = (
            [item for item in selected_cycles_raw if isinstance(item, dict)]
            if isinstance(selected_cycles_raw, list)
            else []
        )
        from_date = str(payload.get("from_date") or "").strip() or None
        to_date = str(payload.get("to_date") or "").strip() or None
        if import_mode == "folder" and not folder_id:
            return jsonify({"error": "folder_id is required"}), 400
        if import_mode == "cycle" and not cycle_key and not test_run_id:
            return jsonify({"error": "cycle_key or test_run_id is required"}), 400
        if import_mode == "cycles" and not selected_cycles:
            return jsonify({"error": "selected_cycles is required"}), 400
        try:
            cycles = fetch_cycles_by_import_mode(
                import_mode=import_mode,
                folder_id=folder_id,
                folder_name=folder_name,
                cycle_key=cycle_key,
                test_run_id=test_run_id,
                selected_cycles=selected_cycles,
                from_date=from_date,
                to_date=to_date,
            )
            draft_cycles = render_cycles_to_draft_cycles(cycles)
            return jsonify(
                {
                    "cycles": draft_cycles,
                    "cycle_count": len(draft_cycles),
                    "folder_id": folder_id,
                    "folder_name": folder_name,
                    "import_mode": import_mode,
                }
            )
        except Exception as exc:  # noqa: BLE001
            body, status = _api_error_response(exc)
            return jsonify(body), status

    @app.post("/api/publish")
    def api_publish() -> object:
        draft = normalize_draft(request.get_json(silent=True) or {})
        try:
            result = publish_universal_report(draft)
            save_draft(draft)
            return jsonify(result)
        except Exception as exc:  # noqa: BLE001
            body, status = _api_error_response(exc)
            return jsonify(body), status

    return app


def main() -> None:
    load_repo_env_for_scripts(use_local_env=True)
    app = create_app()
    host = _server_host()
    port = _server_port()
    print(f"Universal report UI: http://{host}:{port}")
    print(f"Folder API: {_FOLDER_API_VERSION} ({_PACKAGE_DIR})")
    app.run(host=host, port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()

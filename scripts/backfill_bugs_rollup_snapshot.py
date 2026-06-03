#!/usr/bin/env python3
"""Дозаполнение all-time snapshot bugs rollup из Zephyr или с диска.

Режимы:
  bootstrap (по умолчанию): слить ключи из build_log_reports и duplicate_rollup_keys.json
    в defect_analytics_snapshot.json без вызова Zephyr.
  full: запустить zephyr_weekly_report.py с широким окном ZEPHYR_FROM_DATE / ZEPHYR_TO_DATE.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from repo_env import load_repo_env_for_scripts  # noqa: E402

from zephyr_weekly_report import (  # noqa: E402
    _bootstrap_snapshot_from_disk,
    _bugs_rollup_snapshot_path,
    _default_build_log_report_dir,
    _empty_defect_analytics,
    _load_bugs_rollup_snapshot,
    _merge_defect_analytics,
    _recompute_defect_analytics_derived,
    _save_bugs_rollup_snapshot,
    _week_keys_from_snapshot_iso,
)


def _rollup_dir() -> str:
    """Каталог bugs rollup из ZEPHYR_BUGS_ROLLUP_DIR."""
    return (
        os.getenv("ZEPHYR_BUGS_ROLLUP_DIR", "reports/bugs_rollup").strip()
        or "reports/bugs_rollup"
    )


def run_bootstrap() -> int:
    """Обновить snapshot только данными с диска (bootstrap)."""
    rollup_dir = _rollup_dir()
    os.makedirs(rollup_dir, exist_ok=True)
    snapshot_path = _bugs_rollup_snapshot_path(rollup_dir)
    stored = _load_bugs_rollup_snapshot(snapshot_path)
    base_analytics = dict(stored.get("analytics") or _empty_defect_analytics())
    base_labels = list(stored.get("column_labels") or [])

    boot_analytics, boot_labels, _ = _bootstrap_snapshot_from_disk(
        _default_build_log_report_dir(), rollup_dir
    )
    merged, labels = _merge_defect_analytics(
        base_analytics, boot_analytics, base_labels, boot_labels
    )
    merged = _recompute_defect_analytics_derived(merged, labels)
    week_keys = _week_keys_from_snapshot_iso(list(stored.get("week_keys_iso") or []))

    _save_bugs_rollup_snapshot(snapshot_path, merged, labels, week_keys)
    print(
        f"Snapshot updated: {snapshot_path} "
        f"({len(merged.get('keys_ordered') or [])} bug key(s))"
    )
    return 0


def run_full_zephyr(extra_args: list[str]) -> int:
    """Полный прогон zephyr_weekly_report.py за заданный диапазон дат."""
    script = _REPO / "zephyr_weekly_report.py"
    env = os.environ.copy()
    env["ZEPHYR_REGENERATE_LAST_N_DAYS"] = "0"
    from_date = (env.get("ZEPHYR_FROM_DATE") or "").strip()
    to_date = (env.get("ZEPHYR_TO_DATE") or "").strip()
    if not from_date or not to_date:
        print(
            "full mode requires ZEPHYR_FROM_DATE and ZEPHYR_TO_DATE in .env",
            file=sys.stderr,
        )
        return 1

    cmd = [
        sys.executable,
        str(script),
        "--discover-folders",
        "--export-case-steps",
        "--export-build-log-report",
        "--from-date",
        from_date,
        "--to-date",
        to_date,
        *extra_args,
    ]
    print("Running:", " ".join(cmd))
    return subprocess.call(cmd, cwd=str(_REPO), env=env)


def main() -> int:
    """CLI: выбор режима bootstrap или full."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--use-local-env",
        action="store_true",
        help="Load .env.local overrides (reports_local paths)",
    )
    parser.add_argument(
        "--mode",
        choices=("bootstrap", "full"),
        default="bootstrap",
        help="bootstrap: keys from disk only; full: wide Zephyr run",
    )
    parser.add_argument(
        "zephyr_extra",
        nargs="*",
        help="Extra arguments forwarded to zephyr_weekly_report.py in full mode",
    )
    args = parser.parse_args()
    load_repo_env_for_scripts(use_local_env=args.use_local_env)

    if args.mode == "bootstrap":
        return run_bootstrap()
    return run_full_zephyr(list(args.zephyr_extra))


if __name__ == "__main__":
    raise SystemExit(main())

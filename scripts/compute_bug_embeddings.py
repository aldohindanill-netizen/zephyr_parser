#!/usr/bin/env python3
"""Сборка/обновление embedding-кэша для поиска дубликатов багов (опциональные зависимости).

Требуется: pip install sentence-transformers

Векторы строятся по Expected + Actual из таблицы описания, не по summary.

Пример:
  python scripts/compute_bug_embeddings.py --from-rollup-dir reports/bugs_rollup
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from bug_duplicate_detection import (  # noqa: E402
    build_results_embedding_text,
    load_embedding_cache,
    resolve_paths_for_rollup_dir,
    results_hash,
)
from repo_env import load_repo_env_for_scripts  # noqa: E402

DEFAULT_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"


def _load_rollup_keys_file(path: Path) -> tuple[list[str], dict[str, str], dict[str, str]]:
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    keys = [str(k).strip() for k in (data.get("keys") or []) if str(k).strip()]

    def _field_map(raw: object) -> dict[str, str]:
        if not isinstance(raw, dict):
            return {}
        return {str(k).strip(): str(v or "") for k, v in raw.items() if str(k).strip()}

    expected = _field_map(data.get("expected_results"))
    actual = _field_map(data.get("actual_results"))
    return keys, expected, actual


def _encode_texts(model: object, texts: list[str]) -> list[list[float]]:
    embeddings = model.encode(texts, normalize_embeddings=True)  # type: ignore[attr-defined]
    return [list(map(float, row)) for row in embeddings]


def update_cache(
    cache_path: str,
    keys: list[str],
    expected_by_key: dict[str, str],
    actual_by_key: dict[str, str],
    *,
    model_name: str = DEFAULT_MODEL,
) -> None:
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore[import-untyped]
    except ImportError as exc:
        raise SystemExit(
            "sentence-transformers is required: pip install sentence-transformers"
        ) from exc

    existing = load_embedding_cache(cache_path) or {}
    vectors: dict[str, list[float]] = dict(existing.get("vectors") or {})
    hashes: dict[str, str] = dict(existing.get("results_hash") or existing.get("summary_hash") or {})

    model = SentenceTransformer(model_name)
    pending_keys: list[str] = []
    pending_texts: list[str] = []
    for key in keys:
        expected = expected_by_key.get(key, "")
        actual = actual_by_key.get(key, "")
        embed_text = build_results_embedding_text(expected, actual)
        if not embed_text.strip():
            continue
        h = results_hash(expected, actual)
        if hashes.get(key) == h and key in vectors:
            continue
        pending_keys.append(key)
        pending_texts.append(embed_text)

    if pending_texts:
        encoded = _encode_texts(model, pending_texts)
        for key, vec in zip(pending_keys, encoded):
            vectors[key] = vec
            hashes[key] = results_hash(
                expected_by_key.get(key, ""),
                actual_by_key.get(key, ""),
            )

    payload = {
        "model": model_name,
        "vectors": vectors,
        "results_hash": hashes,
    }
    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    print(f"Updated embedding cache: {cache_path} ({len(vectors)} vectors)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute bug Expected+Actual embeddings cache"
    )
    parser.add_argument(
        "--from-rollup-dir",
        default="",
        help="Read duplicate_rollup_keys.json from this directory",
    )
    parser.add_argument(
        "--keys",
        default="",
        help="Comma-separated Jira keys (with --results-json or rollup keys file)",
    )
    parser.add_argument(
        "--results-json",
        default="",
        help='JSON {"expected_results": {...}, "actual_results": {...}}',
    )
    parser.add_argument(
        "--cache",
        default="",
        help="Output cache path (default: ZEPHYR_BUGS_DUPLICATE_CACHE or rollup dir)",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"sentence-transformers model (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--use-local-env",
        action="store_true",
        help="Load .env.local overrides (reports_local paths)",
    )
    args = parser.parse_args()
    load_repo_env_for_scripts(use_local_env=args.use_local_env)

    keys: list[str] = []
    expected_by_key: dict[str, str] = {}
    actual_by_key: dict[str, str] = {}

    rollup_dir = (args.from_rollup_dir or "").strip()
    if rollup_dir:
        keys_path = Path(rollup_dir) / "duplicate_rollup_keys.json"
        if not keys_path.is_file():
            raise SystemExit(f"Missing {keys_path}; run zephyr report first.")
        keys, expected_by_key, actual_by_key = _load_rollup_keys_file(keys_path)

    if args.keys.strip():
        for k in args.keys.split(","):
            ck = k.strip()
            if ck and ck not in keys:
                keys.append(ck)

    if args.results_json.strip():
        with open(args.results_json, encoding="utf-8") as fh:
            extra = json.load(fh)
        if isinstance(extra, dict):
            for k, v in (extra.get("expected_results") or {}).items():
                expected_by_key[str(k).strip()] = str(v or "")
            for k, v in (extra.get("actual_results") or {}).items():
                actual_by_key[str(k).strip()] = str(v or "")

    if not keys:
        raise SystemExit("No keys: use --from-rollup-dir or --keys")

    cache_path = (args.cache or "").strip()
    if not cache_path:
        base_dir = rollup_dir or os.getenv("ZEPHYR_BUGS_ROLLUP_DIR", "reports/bugs_rollup")
        cache_path, _ = resolve_paths_for_rollup_dir(base_dir)

    update_cache(
        cache_path,
        keys,
        expected_by_key,
        actual_by_key,
        model_name=args.model.strip() or DEFAULT_MODEL,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

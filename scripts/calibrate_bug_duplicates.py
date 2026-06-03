#!/usr/bin/env python3
"""Офлайн-калибровка порогов дубликатов по размеченным парам из duplicate_overrides."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from bug_duplicate_detection import find_duplicate_candidates, load_embedding_cache, resolve_paths_for_rollup_dir


@dataclass(frozen=True)
class Metrics:
    tp: int
    fp: int
    fn: int
    precision: float
    recall: float
    f1: float


def _safe_ratio(num: int, den: int) -> float:
    return (num / den) if den else 0.0


def _load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def _pair_key(a: str, b: str) -> tuple[str, str]:
    x, y = str(a).strip().upper(), str(b).strip().upper()
    return (x, y) if x <= y else (y, x)


def _predicted_pairs(result: dict[str, object]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for key, cand in result.items():
        if cand is None:
            continue
        other = str(getattr(cand, "other_key", "")).strip()
        if not other:
            continue
        pairs.add(_pair_key(key, other))
    return pairs


def _metrics(predicted: set[tuple[str, str]], positive: set[tuple[str, str]]) -> Metrics:
    tp = len(predicted & positive)
    fp = len(predicted - positive)
    fn = len(positive - predicted)
    precision = _safe_ratio(tp, tp + fp)
    recall = _safe_ratio(tp, tp + fn)
    f1 = _safe_ratio(2 * precision * recall, precision + recall) if precision and recall else 0.0
    return Metrics(tp=tp, fp=fp, fn=fn, precision=precision, recall=recall, f1=f1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rollup-dir",
        default="reports/bugs_rollup",
        help="Directory with duplicate_rollup_keys.json and duplicate_overrides.json",
    )
    parser.add_argument("--text-start", type=float, default=0.60)
    parser.add_argument("--text-end", type=float, default=0.90)
    parser.add_argument("--text-step", type=float, default=0.02)
    parser.add_argument("--embed-start", type=float, default=0.75)
    parser.add_argument("--embed-end", type=float, default=0.95)
    parser.add_argument("--embed-step", type=float, default=0.02)
    parser.add_argument("--target-precision", type=float, default=0.90)
    args = parser.parse_args()

    rollup_dir = Path(args.rollup_dir)
    keys_path = rollup_dir / "duplicate_rollup_keys.json"
    overrides_path = rollup_dir / "duplicate_overrides.json"
    if not keys_path.is_file():
        print(f"Missing {keys_path}")
        return 1
    if not overrides_path.is_file():
        print(f"Missing {overrides_path} (needs merge labels for calibration)")
        return 1

    rollup = _load_json(keys_path)
    overrides = _load_json(overrides_path)
    keys = [str(k).strip() for k in (rollup.get("keys") or []) if str(k).strip()]
    if not keys:
        print("No keys in duplicate_rollup_keys.json")
        return 1
    meta = {
        k: {
            "summary": str((rollup.get("summaries") or {}).get(k) or ""),
            "expected_result": str((rollup.get("expected_results") or {}).get(k) or ""),
            "actual_result": str((rollup.get("actual_results") or {}).get(k) or ""),
        }
        for k in keys
    }
    positive_pairs = {
        _pair_key(a, b)
        for a, b in (overrides.get("merge") or [])
        if str(a).strip() and str(b).strip()
    }
    if not positive_pairs:
        print("No merge pairs in duplicate_overrides.json; nothing to calibrate.")
        return 1

    print(f"Labeled positive pairs: {len(positive_pairs)}")
    cache_path, _ = resolve_paths_for_rollup_dir(str(rollup_dir))
    embedding_cache = load_embedding_cache(cache_path)
    if embedding_cache is None:
        print(f"Embedding cache not found at {cache_path}; calibration will run text-only.")
    print("text_thr embed_thr tp fp fn precision recall f1")

    best: tuple[float, float, Metrics] | None = None
    text_thr = args.text_start
    while text_thr <= args.text_end + 1e-9:
        embed_thr = args.embed_start
        while embed_thr <= args.embed_end + 1e-9:
            result = find_duplicate_candidates(
                keys,
                meta,
                embedding_cache=embedding_cache,
                overrides={"split": overrides.get("split") or []},
                text_threshold=text_thr,
                embed_threshold=embed_thr,
                use_embeddings=embedding_cache is not None,
            )
            m = _metrics(_predicted_pairs(result), positive_pairs)
            print(
                f"{text_thr:.2f} {embed_thr:.2f} {m.tp} {m.fp} {m.fn} "
                f"{m.precision:.3f} {m.recall:.3f} {m.f1:.3f}"
            )
            if m.precision >= args.target_precision:
                if best is None or m.f1 > best[2].f1:
                    best = (text_thr, embed_thr, m)
            embed_thr += args.embed_step
        text_thr += args.text_step

    if best is None:
        print(
            f"\nNo config reached target precision >= {args.target_precision:.2f}. "
            "Try lowering target or adding labels."
        )
        return 0

    t, e, m = best
    print(
        f"\nRecommended (precision-first): "
        f"TEXT={t:.2f} EMBED={e:.2f} "
        f"(precision={m.precision:.3f}, recall={m.recall:.3f}, f1={m.f1:.3f})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

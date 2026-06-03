#!/usr/bin/env python3
"""Быстрая проверка калибровки дубликатов (Expected/Actual + fallback по summary)."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from bug_duplicate_detection import find_duplicate_candidates, text_similarity

KNOWN = [
    (
        "CSD-46908",
        "ВАТС не начинает торможение перед пешеходом спереди",
        "ВАТС тормозит перед пешеходом",
        "ВАТС не начинает торможение",
    ),
    (
        "CSD-46911",
        "ВАТС перестает тормозить перед пешеходом спереди",
        "ВАТС продолжает тормозить перед пешеходом",
        "ВАТС перестает тормозить",
    ),
    (
        "CSD-46927",
        "ВАТС не останавливается перед стоп линией на красный сигнал светофора",
        "ВАТС останавливается перед стоп-линией",
        "ВАТС проезжает стоп-линию",
    ),
    (
        "CSD-47455",
        "Остановка за стоп-линией на красный сигнал светофора",
        "ВАТС останавливается перед стоп-линией",
        "Остановка за стоп-линией",
    ),
]


def main() -> int:
    meta = {
        k: {
            "summary": s,
            "expected_result": e,
            "actual_result": a,
        }
        for k, s, e, a in KNOWN
    }
    keys = [k for k, *_ in KNOWN]
    print("Pair scores (summary / expected / actual / min):")
    for i, (ka, sa, ea, aa) in enumerate(KNOWN):
        for kb, sb, eb, ab in KNOWN[i + 1 :]:
            s_sim = text_similarity(sa, sb)
            e_sim = text_similarity(ea, eb)
            a_sim = text_similarity(aa, ab)
            pair_min = min(e_sim, a_sim)
            print(
                f"  {ka} <-> {kb}: summary={s_sim:.3f} expected={e_sim:.3f} "
                f"actual={a_sim:.3f} min={pair_min:.3f}"
            )

    result = find_duplicate_candidates(keys, meta, use_embeddings=False, text_threshold=0.78)
    print("\nCandidates (threshold 0.78, results-first):")
    for key in keys:
        cand = result.get(key)
        if cand:
            extra = ""
            if cand.expected_sim is not None:
                extra += f" exp={cand.expected_sim:.3f}"
            if cand.actual_sim is not None:
                extra += f" act={cand.actual_sim:.3f}"
            print(f"  {key} -> {cand.other_key} ({cand.method}, {cand.score:.3f}{extra})")
        else:
            print(f"  {key} -> —")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

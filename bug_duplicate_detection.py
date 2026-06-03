"""Detect possible duplicate Jira bugs for bugs rollup reports (stdlib + optional embedding cache)."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

_RU_STOPWORDS = frozenset(
    {
        "а",
        "и",
        "в",
        "во",
        "на",
        "не",
        "ни",
        "но",
        "о",
        "об",
        "от",
        "по",
        "при",
        "с",
        "со",
        "у",
        "за",
        "из",
        "к",
        "до",
        "для",
        "или",
        "без",
        "под",
        "над",
        "перед",
        "после",
        "the",
        "a",
        "an",
    }
)

_VATS_PREFIX_RE = re.compile(
    r"^(?:ватс|vats)\s+",
    re.IGNORECASE,
)
_TOKEN_RE = re.compile(r"\w+", re.UNICODE)

_WIKI_TABLE_ROW_RE = re.compile(
    r"^\s*\|{1,2}\s*(?P<label>[^|]+?)\s*\|{1,2}(?P<value>.+?)\|{0,2}\s*$",
    re.UNICODE,
)
_JIRA_WIKI_LINK_RE = re.compile(r"\[([^\]|]+)\|[^\]]+\]|\[([^\]]+)\]")
_LABEL_VALUE_COLON_RE = re.compile(
    r"^\s*(?P<label>.+?)\s*:\s*(?P<value>.+?)\s*$",
    re.UNICODE,
)

_FIELD_LABEL_PATTERNS: dict[str, tuple[str, ...]] = {
    "expected_result": (
        r"^expected\s*result\s*$",
        r"^ожидаемый\s*результат\s*$",
        r"^ожидаем(?:ый|ое)?\s*result\s*$",
    ),
    "actual_result": (
        r"^actual\s*result\s*$",
        r"^фактический\s*результат\s*$",
        r"^фактическ(?:ий|ое)?\s*result\s*$",
    ),
    "preconditions": (
        r"^preconditions?\s*$",
        r"^предусловия\s*$",
        r"^precondition\s*$",
    ),
    "traceability": (
        r"^traceability\s*$",
        r"^трассируемость\s*$",
        r"^трассировка\s*$",
        r"^tests?\s*id\b.*$",
        r"^test\s*case\s*id\b.*$",
        r"^linked\s*tests?\b.*$",
        r"^привязанн(?:ые|ый)\s*тест(?:ы|(?:\s*кейс)?)?\b.*$",
    ),
}

_TRACEABILITY_SPLIT_RE = re.compile(r"[,;\n]+", re.UNICODE)


@dataclass(frozen=True)
class DuplicateCandidate:
    other_key: str
    score: float
    method: str  # text_expected_actual | text_summary | embedding_candidate | override
    confidence: str = "high"  # high | medium | low
    expected_sim: float | None = None
    actual_sim: float | None = None
    embed_sim: float | None = None
    domain_match: bool | None = None
    domain_conflict: bool | None = None
    domain_tags: tuple[str, ...] = ()
    scenario_match: bool | None = None
    scenario_conflict: bool | None = None
    scenario_tags: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()


def _parse_bool_env(raw: str | None, default: bool = False) -> bool:
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _parse_float_env(raw: str | None, default: float) -> float:
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(str(raw).strip())
    except ValueError:
        return default


def bugs_duplicate_detect_enabled() -> bool:
    return _parse_bool_env(os.getenv("ZEPHYR_BUGS_DUPLICATE_DETECT"), default=True)


def bugs_duplicate_text_threshold() -> float:
    return _parse_float_env(os.getenv("ZEPHYR_BUGS_DUPLICATE_TEXT_THRESHOLD"), 0.78)


def bugs_duplicate_text_soft_expected_threshold() -> float:
    return _parse_float_env(os.getenv("ZEPHYR_BUGS_DUPLICATE_TEXT_SOFT_EXPECTED_THRESHOLD"), 0.60)


def bugs_duplicate_text_soft_actual_threshold() -> float:
    return _parse_float_env(os.getenv("ZEPHYR_BUGS_DUPLICATE_TEXT_SOFT_ACTUAL_THRESHOLD"), 0.74)


def bugs_duplicate_embed_threshold() -> float:
    return _parse_float_env(os.getenv("ZEPHYR_BUGS_DUPLICATE_EMBED_THRESHOLD"), 0.85)


def bugs_duplicate_embeddings_enabled() -> bool:
    return _parse_bool_env(os.getenv("ZEPHYR_BUGS_DUPLICATE_EMBEDDINGS"), default=False)


def bugs_duplicate_publish_min_confidence() -> str:
    raw = (os.getenv("ZEPHYR_BUGS_DUPLICATE_PUBLISH_MIN_CONFIDENCE") or "high").strip().lower()
    return raw if raw in {"high", "medium", "low"} else "high"


def bugs_duplicate_summary_text_allows_high() -> bool:
    return _parse_bool_env(os.getenv("ZEPHYR_BUGS_DUPLICATE_SUMMARY_HIGH"), default=False)


def bugs_duplicate_domain_gate_enabled() -> bool:
    return _parse_bool_env(os.getenv("ZEPHYR_BUGS_DUPLICATE_DOMAIN_GATE"), default=True)


def bugs_duplicate_scenario_gate_enabled() -> bool:
    return _parse_bool_env(os.getenv("ZEPHYR_BUGS_DUPLICATE_SCENARIO_GATE"), default=True)


_DOMAIN_PATTERNS: dict[str, tuple[str, ...]] = {
    "cones": (r"\bконус", r"\bcone"),
    "pedestrian": (r"\bпешеход", r"\bpedestrian"),
    "traffic_light": (r"светофор", r"traffic[_\s-]?light", r"стоп[ -]?лини"),
    "localization": (r"локализац", r"\blocalization\b"),
    "mrm": (r"\bмрм", r"\bmrm\b"),
    "obstacle_avoidance": (r"объез", r"препятств", r"\bobstacle\b", r"\bavoid"),
}


def _default_cache_path(rollup_dir: str) -> str:
    explicit = (os.getenv("ZEPHYR_BUGS_DUPLICATE_CACHE") or "").strip()
    if explicit:
        return explicit
    return os.path.join(rollup_dir, "duplicate_embeddings_cache.json")


def _default_overrides_path(rollup_dir: str) -> str:
    explicit = (os.getenv("ZEPHYR_BUGS_DUPLICATE_OVERRIDES") or "").strip()
    if explicit:
        return explicit
    return os.path.join(rollup_dir, "duplicate_overrides.json")


def _extract_text_nodes(value: Any) -> list[str]:
    out: list[str] = []
    if isinstance(value, str):
        text = value.strip()
        if text:
            out.append(text)
        return out
    if isinstance(value, dict):
        text_value = value.get("text")
        if isinstance(text_value, str):
            text = text_value.strip()
            if text:
                out.append(text)
        for nested in value.values():
            out.extend(_extract_text_nodes(nested))
        return out
    if isinstance(value, list):
        for item in value:
            out.extend(_extract_text_nodes(item))
    return out


def _description_to_text(description: Any) -> str:
    if isinstance(description, str):
        return description
    if description is None:
        return ""
    parts = _extract_text_nodes(description)
    if parts:
        return "\n".join(parts)
    try:
        return json.dumps(description, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        return str(description)


def _strip_jira_wiki_markup(text: str) -> str:
    """Remove Jira wiki markers (*bold*, {*}, [links]) from table cell text."""
    s = str(text or "")
    s = _JIRA_WIKI_LINK_RE.sub(
        lambda m: (m.group(1) or m.group(2) or "").strip(),
        s,
    )
    s = re.sub(r"\{\*\}", "", s)
    s = re.sub(r"\*+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalize_field_label(label: str) -> str:
    cleaned = _strip_jira_wiki_markup(label)
    return re.sub(r"\s+", " ", cleaned.strip().lower()).strip(" :")


def _canonical_field_for_label(label: str) -> str | None:
    norm = _normalize_field_label(label)
    if not norm:
        return None
    for canonical, patterns in _FIELD_LABEL_PATTERNS.items():
        for pattern in patterns:
            if re.match(pattern, norm, re.IGNORECASE | re.UNICODE):
                return canonical
    return None


def _strip_wiki_cell(value: str) -> str:
    text = _strip_jira_wiki_markup(str(value or "").strip())
    text = re.sub(r"^\|+\s*", "", text)
    text = re.sub(r"\s*\|+$", "", text)
    return text.strip()


def parse_traceability_scenario_names(text: str) -> list[str]:
    """Extract unique scenario names from a Jira Traceability cell (wiki links + plain text)."""
    raw = str(text or "").strip()
    if not raw:
        return []

    names: list[str] = []
    seen_lower: set[str] = set()

    def _add(candidate: str) -> None:
        clean = _strip_wiki_cell(candidate)
        if not clean:
            return
        key = clean.lower()
        if key in seen_lower:
            return
        seen_lower.add(key)
        names.append(clean)

    for match in _JIRA_WIKI_LINK_RE.finditer(raw):
        link_text = (match.group(1) or match.group(2) or "").strip()
        if link_text:
            _add(link_text)

    remainder = _JIRA_WIKI_LINK_RE.sub("", raw)
    remainder = re.sub(r"\*+", "", remainder)
    for part in _TRACEABILITY_SPLIT_RE.split(remainder):
        _add(part)

    return names


def parse_jira_description_fields(description: Any) -> dict[str, str]:
    """Extract Expected/Actual, Preconditions, Traceability from Jira description table."""
    text = _description_to_text(description).replace("\r\n", "\n").replace("\r", "\n")
    out: dict[str, str] = {
        "expected_result": "",
        "actual_result": "",
        "preconditions": "",
        "traceability": "",
    }
    if not text.strip():
        return out

    lines = text.split("\n")
    pending_label: str | None = None
    open_canonical: str | None = None

    def _append(canonical: str, value: str) -> None:
        clean = _strip_wiki_cell(value)
        if not canonical or not clean:
            return
        existing = (out.get(canonical) or "").strip()
        if existing:
            out[canonical] = f"{existing} {clean}"
        else:
            out[canonical] = clean

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            pending_label = None
            open_canonical = None
            continue

        if line.startswith("|") and "|" in line[1:]:
            table_match = _WIKI_TABLE_ROW_RE.match(line)
            if table_match:
                label = table_match.group("label") or ""
                value = table_match.group("value") or ""
                canonical = _canonical_field_for_label(label)
                if canonical:
                    _append(canonical, value)
                    open_canonical = canonical
                else:
                    open_canonical = None
                pending_label = None
                continue

        if open_canonical and not line.startswith("|"):
            _append(open_canonical, line)
            continue

        colon_match = _LABEL_VALUE_COLON_RE.match(line)
        if colon_match:
            canonical = _canonical_field_for_label(colon_match.group("label") or "")
            if canonical:
                _append(canonical, colon_match.group("value") or "")
            pending_label = None
            open_canonical = None
            continue

        canonical = _canonical_field_for_label(line)
        if canonical:
            pending_label = canonical
            open_canonical = None
            continue

        if pending_label:
            _append(pending_label, line)
            pending_label = None

    return out


def summary_hash(text: str) -> str:
    normalized = _normalize_summary_for_match(text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def results_hash(expected: str, actual: str) -> str:
    payload = build_results_embedding_text(expected, actual)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_results_embedding_text(expected: str, actual: str) -> str:
    parts: list[str] = []
    exp = str(expected or "").strip()
    act = str(actual or "").strip()
    if exp:
        parts.append(f"EXPECTED: {exp}")
    if act:
        parts.append(f"ACTUAL: {act}")
    return "\n".join(parts)


def _normalize_summary_for_match(text: str) -> str:
    s = str(text or "").strip().lower()
    s = _VATS_PREFIX_RE.sub("", s)
    s = re.sub(r"[^\w\s]+", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokenize_summary(text: str) -> set[str]:
    normalized = _normalize_summary_for_match(text)
    if not normalized:
        return set()
    tokens: set[str] = set()
    for token in _TOKEN_RE.findall(normalized):
        if len(token) < 2:
            continue
        if token in _RU_STOPWORDS:
            continue
        tokens.add(token)
    return tokens


def text_similarity(text_a: str, text_b: str) -> float:
    """Jaccard on tokens, max with SequenceMatcher ratio on normalized strings."""
    tokens_a = _tokenize_summary(text_a)
    tokens_b = _tokenize_summary(text_b)
    jaccard = 0.0
    if tokens_a or tokens_b:
        union = tokens_a | tokens_b
        if union:
            jaccard = len(tokens_a & tokens_b) / len(union)
    norm_a = _normalize_summary_for_match(text_a)
    norm_b = _normalize_summary_for_match(text_b)
    ratio = SequenceMatcher(None, norm_a, norm_b).ratio() if norm_a and norm_b else 0.0
    return max(jaccard, ratio)


def _extract_domain_tags(*parts: str) -> set[str]:
    text = " ".join(str(p or "") for p in parts).lower()
    tags: set[str] = set()
    for tag, patterns in _DOMAIN_PATTERNS.items():
        if any(re.search(p, text, re.IGNORECASE | re.UNICODE) for p in patterns):
            tags.add(tag)
    return tags


def _confidence_rank(level: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get(str(level or "").lower(), 0)


def _candidate_rank(cand: DuplicateCandidate) -> tuple[int, float]:
    return (_confidence_rank(cand.confidence), cand.score)


def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    if len(vec_a) != len(vec_b) or not vec_a:
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def _pair_key(a: str, b: str) -> frozenset[str]:
    return frozenset((a.upper(), b.upper()))


def _load_overrides_from_dict(data: dict[str, Any] | None) -> tuple[set[frozenset[str]], dict[str, str]]:
    """Return (split_pairs, merge_best_other_by_key)."""
    split_pairs: set[frozenset[str]] = set()
    merge_map: dict[str, str] = {}
    if not data:
        return split_pairs, merge_map
    for pair in data.get("split") or []:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        a, b = str(pair[0]).strip(), str(pair[1]).strip()
        if a and b:
            split_pairs.add(_pair_key(a, b))
    for pair in data.get("merge") or []:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        a, b = str(pair[0]).strip(), str(pair[1]).strip()
        if not a or not b:
            continue
        merge_map[a] = b
        merge_map[b] = a
    return split_pairs, merge_map


def load_duplicate_overrides(path: str) -> dict[str, Any] | None:
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def load_embedding_cache(path: str) -> dict[str, Any] | None:
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def embedding_similarity(
    key_a: str,
    key_b: str,
    cache: dict[str, Any] | None,
) -> float | None:
    if not cache:
        return None
    vectors = cache.get("vectors")
    if not isinstance(vectors, dict):
        return None
    vec_a = vectors.get(key_a) or vectors.get(key_a.upper())
    vec_b = vectors.get(key_b) or vectors.get(key_b.upper())
    if not isinstance(vec_a, list) or not isinstance(vec_b, list):
        return None
    try:
        fa = [float(x) for x in vec_a]
        fb = [float(x) for x in vec_b]
    except (TypeError, ValueError):
        return None
    return _cosine_similarity(fa, fb)


def _meta_entry(key: str, meta: dict[str, dict[str, str]]) -> dict[str, str]:
    return meta.get(key) or meta.get(key.upper()) or {}


def _summary_for_key(key: str, meta: dict[str, dict[str, str]]) -> str:
    return str(_meta_entry(key, meta).get("summary") or "").strip()


def _expected_for_key(key: str, meta: dict[str, dict[str, str]]) -> str:
    return str(_meta_entry(key, meta).get("expected_result") or "").strip()


def _actual_for_key(key: str, meta: dict[str, dict[str, str]]) -> str:
    return str(_meta_entry(key, meta).get("actual_result") or "").strip()


def _scenarios_set_for_key(
    key: str,
    scenarios_by_key: dict[str, list[str]] | None,
) -> frozenset[str]:
    if not scenarios_by_key:
        return frozenset()
    names = scenarios_by_key.get(key) or scenarios_by_key.get(key.upper()) or []
    out: set[str] = set()
    for name in names:
        clean = str(name or "").strip().lower()
        if clean:
            out.add(clean)
    return frozenset(out)


def _scenario_gate_status(
    scenarios_a: frozenset[str],
    scenarios_b: frozenset[str],
) -> tuple[bool, bool, bool]:
    """Return (match, conflict, unknown) for scenario gate."""
    shared = scenarios_a & scenarios_b
    scenario_match = bool(shared)
    scenario_conflict = bool(scenarios_a and scenarios_b and not shared)
    scenario_unknown = not scenarios_a and not scenarios_b
    return scenario_match, scenario_conflict, scenario_unknown


def _pair_text_scores(
    key_a: str,
    key_b: str,
    meta: dict[str, dict[str, str]],
) -> tuple[float, str, float | None, float | None]:
    """Return (pair_score, method, expected_sim, actual_sim)."""
    exp_a = _expected_for_key(key_a, meta)
    exp_b = _expected_for_key(key_b, meta)
    act_a = _actual_for_key(key_a, meta)
    act_b = _actual_for_key(key_b, meta)

    expected_sim: float | None = None
    actual_sim: float | None = None
    if exp_a and exp_b:
        expected_sim = text_similarity(exp_a, exp_b)
    if act_a and act_b:
        actual_sim = text_similarity(act_a, act_b)

    if expected_sim is not None and actual_sim is not None:
        return min(expected_sim, actual_sim), "text_expected_actual", expected_sim, actual_sim

    partial: list[float] = []
    if expected_sim is not None:
        partial.append(expected_sim)
    if actual_sim is not None:
        partial.append(actual_sim)
    if partial:
        score = min(partial)
        return score, "text_partial_results", expected_sim, actual_sim

    summary_a = _summary_for_key(key_a, meta)
    summary_b = _summary_for_key(key_b, meta)
    if summary_a and summary_b:
        return text_similarity(summary_a, summary_b), "text_summary", None, None

    return 0.0, "text_summary", expected_sim, actual_sim


def _has_results_for_embedding(key: str, meta: dict[str, dict[str, str]]) -> bool:
    return bool(_expected_for_key(key, meta) or _actual_for_key(key, meta))


def _is_split(key_a: str, key_b: str, split_pairs: set[frozenset[str]]) -> bool:
    return _pair_key(key_a, key_b) in split_pairs


def find_duplicate_candidates(
    keys: list[str],
    meta: dict[str, dict[str, str]],
    *,
    embedding_cache: dict[str, Any] | None = None,
    overrides: dict[str, Any] | None = None,
    scenarios_by_key: dict[str, list[str]] | None = None,
    text_threshold: float | None = None,
    embed_threshold: float | None = None,
    use_embeddings: bool | None = None,
) -> dict[str, DuplicateCandidate | None]:
    """For each key, return the best duplicate candidate or None."""
    if not bugs_duplicate_detect_enabled():
        return {k: None for k in keys}

    text_thr = text_threshold if text_threshold is not None else bugs_duplicate_text_threshold()
    soft_exp_thr = bugs_duplicate_text_soft_expected_threshold()
    soft_act_thr = bugs_duplicate_text_soft_actual_threshold()
    embed_thr = embed_threshold if embed_threshold is not None else bugs_duplicate_embed_threshold()
    embeddings_on = (
        use_embeddings
        if use_embeddings is not None
        else bugs_duplicate_embeddings_enabled()
    )

    allow_summary_high = bugs_duplicate_summary_text_allows_high()
    domain_gate = bugs_duplicate_domain_gate_enabled()
    scenario_gate = bugs_duplicate_scenario_gate_enabled()
    clean_keys = [str(k).strip() for k in keys if str(k).strip()]
    split_pairs, merge_map = _load_overrides_from_dict(overrides)

    result: dict[str, DuplicateCandidate | None] = {k: None for k in clean_keys}

    for key in clean_keys:
        if key in merge_map:
            other = merge_map[key]
            if other and other != key and other in clean_keys:
                if not _is_split(key, other, split_pairs):
                    result[key] = DuplicateCandidate(
                        other,
                        1.0,
                        "override",
                        confidence="high",
                        reasons=("override_merge",),
                    )

    for key_a in clean_keys:
        if result.get(key_a) is not None:
            continue
        best: DuplicateCandidate | None = None
        for key_b in clean_keys:
            if key_a == key_b:
                continue
            if _is_split(key_a, key_b, split_pairs):
                continue

            scenarios_a = _scenarios_set_for_key(key_a, scenarios_by_key)
            scenarios_b = _scenarios_set_for_key(key_b, scenarios_by_key)
            scenario_match, scenario_conflict, _scenario_unknown = _scenario_gate_status(
                scenarios_a, scenarios_b
            )
            shared_scenarios = scenarios_a & scenarios_b
            if scenario_gate and scenario_conflict:
                continue

            text_score, text_method, exp_sim, act_sim = _pair_text_scores(key_a, key_b, meta)
            embed_score: float | None = None
            if embeddings_on and embedding_cache:
                if _has_results_for_embedding(key_a, meta) and _has_results_for_embedding(
                    key_b, meta
                ):
                    embed_score = embedding_similarity(key_a, key_b, embedding_cache)
            tags_a = _extract_domain_tags(
                _summary_for_key(key_a, meta),
                _expected_for_key(key_a, meta),
                _actual_for_key(key_a, meta),
            )
            tags_b = _extract_domain_tags(
                _summary_for_key(key_b, meta),
                _expected_for_key(key_b, meta),
                _actual_for_key(key_b, meta),
            )
            shared_tags = tags_a & tags_b
            domain_match = bool(shared_tags)
            domain_conflict = bool(tags_a and tags_b and not shared_tags)
            domain_unknown = not tags_a and not tags_b
            if domain_gate and domain_conflict:
                continue

            reasons: list[str] = []
            cand: DuplicateCandidate | None = None
            if text_score >= text_thr:
                if text_method == "text_expected_actual":
                    conf = "high"
                elif text_method == "text_summary" and allow_summary_high:
                    conf = "high"
                else:
                    conf = "medium"
                reasons.append(f"text_pass:{text_method}")
                cand = DuplicateCandidate(
                    key_b,
                    text_score,
                    text_method,
                    confidence=conf,
                    expected_sim=exp_sim,
                    actual_sim=act_sim,
                    embed_sim=embed_score,
                    domain_match=domain_match,
                    domain_conflict=domain_conflict,
                    domain_tags=tuple(sorted(shared_tags)),
                    scenario_match=scenario_match or None,
                    scenario_conflict=scenario_conflict or None,
                    scenario_tags=tuple(sorted(shared_scenarios)),
                    reasons=tuple(reasons),
                )
            elif (
                text_method == "text_expected_actual"
                and exp_sim is not None
                and act_sim is not None
                and domain_match
                and exp_sim >= soft_exp_thr
                and act_sim >= soft_act_thr
            ):
                reasons.extend(("text_soft_pass", "domain_match"))
                cand = DuplicateCandidate(
                    key_b,
                    min(exp_sim, act_sim),
                    "text_expected_actual_soft",
                    confidence="high",
                    expected_sim=exp_sim,
                    actual_sim=act_sim,
                    embed_sim=embed_score,
                    domain_match=domain_match,
                    domain_conflict=domain_conflict,
                    domain_tags=tuple(sorted(shared_tags)),
                    scenario_match=scenario_match or None,
                    scenario_conflict=scenario_conflict or None,
                    scenario_tags=tuple(sorted(shared_scenarios)),
                    reasons=tuple(reasons),
                )
            elif (
                embed_score is not None
                and embed_score >= embed_thr
                and (domain_match or domain_unknown)
            ):
                reasons.extend(("embedding_pass", "candidate_only"))
                cand = DuplicateCandidate(
                    key_b,
                    embed_score,
                    "embedding_candidate",
                    confidence="medium",
                    expected_sim=exp_sim,
                    actual_sim=act_sim,
                    embed_sim=embed_score,
                    domain_match=domain_match,
                    domain_conflict=domain_conflict,
                    domain_tags=tuple(sorted(shared_tags)),
                    scenario_match=scenario_match or None,
                    scenario_conflict=scenario_conflict or None,
                    scenario_tags=tuple(sorted(shared_scenarios)),
                    reasons=tuple(reasons),
                )

            if cand is None:
                continue
            if best is None or _candidate_rank(cand) > _candidate_rank(best):
                best = cand

        result[key_a] = best

    return result


def write_duplicate_candidates_debug(
    path: str,
    candidates: dict[str, DuplicateCandidate | None],
) -> None:
    payload: dict[str, Any] = {}
    for key, cand in candidates.items():
        if cand is None:
            payload[key] = None
        else:
            entry: dict[str, Any] = {
                "other_key": cand.other_key,
                "score": round(cand.score, 4),
                "method": cand.method,
                "confidence": cand.confidence,
            }
            if cand.expected_sim is not None:
                entry["expected_sim"] = round(cand.expected_sim, 4)
            if cand.actual_sim is not None:
                entry["actual_sim"] = round(cand.actual_sim, 4)
            if cand.embed_sim is not None:
                entry["embed_sim"] = round(cand.embed_sim, 4)
            if cand.domain_match is not None:
                entry["domain_match"] = cand.domain_match
            if cand.domain_conflict is not None:
                entry["domain_conflict"] = cand.domain_conflict
            if cand.domain_tags:
                entry["domain_tags"] = list(cand.domain_tags)
            if cand.scenario_match is not None:
                entry["scenario_match"] = cand.scenario_match
            if cand.scenario_conflict is not None:
                entry["scenario_conflict"] = cand.scenario_conflict
            if cand.scenario_tags:
                entry["scenario_tags"] = list(cand.scenario_tags)
            if cand.reasons:
                entry["reasons"] = list(cand.reasons)
            payload[key] = entry
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def resolve_paths_for_rollup_dir(rollup_dir: str) -> tuple[str, str]:
    return _default_cache_path(rollup_dir), _default_overrides_path(rollup_dir)

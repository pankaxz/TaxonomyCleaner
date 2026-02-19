from __future__ import annotations

from collections import defaultdict
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

from ...shared.models import SimilarityEdge
from ...shared.models import StageResult
from ...shared.utilities import build_inverted_index
from ...shared.utilities import cosine_similarity_sparse
from ...shared.utilities import ngram_vector
from ...shared.utilities import normalize_term

HIGH_THRESHOLD = 0.93
POSSIBLE_THRESHOLD = 0.85


def run_stage1_similarity(
    canonicals: List[str],
    canonical_rows: Optional[List[Dict[str, object]]] = None,
    embedding_client=None,
    embedding_model: Optional[str] = None,
    embedding_batch_size: int = 64,
) -> StageResult:
    result = StageResult()

    if not canonicals:
        result.payload["similarity_edges"] = []
        result.payload["alias_canonical_advisories"] = []
        result.payload["thresholds"] = {
            "high": HIGH_THRESHOLD,
            "possible": POSSIBLE_THRESHOLD,
        }
        result.payload["execution"] = {
            "input_canonicals": 0,
            "embedding_mode": "none",
            "dense_embeddings_used": False,
            "embedding_batch_size": int(embedding_batch_size),
            "alias_advisories": 0,
        }
        return result

    sparse_vectors: Dict[str, Dict[str, float]] = {}
    for term in canonicals:
        sparse_vectors[term] = ngram_vector(term)

    dense_vectors: Dict[str, List[float]] = {}
    if embedding_client is not None:
        vectors = embedding_client.embed_texts(
            canonicals,
            model=embedding_model,
            batch_size=embedding_batch_size,
        )

        for index, term in enumerate(canonicals):
            dense_vectors[term] = vectors[index]

    inverted_index = build_inverted_index(canonicals)
    candidate_pairs = _build_candidate_pairs(inverted_index)

    edges: List[SimilarityEdge] = []
    for left, right in sorted(candidate_pairs):
        if dense_vectors:
            score = _cosine_similarity_dense(dense_vectors[left], dense_vectors[right])
        else:
            score = cosine_similarity_sparse(sparse_vectors[left], sparse_vectors[right])

        if score < POSSIBLE_THRESHOLD:
            continue

        band = "possible_conflict"
        if score > HIGH_THRESHOLD:
            band = "high_collision"

        edge = SimilarityEdge(
            left=left,
            right=right,
            score=round(score, 6),
            band=band,
        )
        edges.append(edge)

    alias_canonical_advisories = _build_alias_canonical_advisories(
        canonicals=canonicals,
        canonical_rows=canonical_rows,
        canonical_sparse_vectors=sparse_vectors,
        canonical_dense_vectors=dense_vectors,
        canonical_inverted_index=inverted_index,
        embedding_client=embedding_client,
        embedding_model=embedding_model,
        embedding_batch_size=embedding_batch_size,
    )

    serialized_edges: List[Dict[str, object]] = []
    for edge in edges:
        serialized_edges.append(edge.to_dict())

    result.payload["similarity_edges"] = serialized_edges
    result.payload["alias_canonical_advisories"] = alias_canonical_advisories
    result.payload["thresholds"] = {
        "high": HIGH_THRESHOLD,
        "possible": POSSIBLE_THRESHOLD,
    }
    result.payload["execution"] = {
        "input_canonicals": len(canonicals),
        "embedding_mode": "http_dense" if embedding_client is not None else "heuristic_sparse",
        "dense_embeddings_used": bool(dense_vectors),
        "embedding_batch_size": int(embedding_batch_size),
        "alias_advisories": len(alias_canonical_advisories),
    }
    return result


def _build_alias_canonical_advisories(
    canonicals: List[str],
    canonical_rows: Optional[List[Dict[str, object]]],
    canonical_sparse_vectors: Dict[str, Dict[str, float]],
    canonical_dense_vectors: Dict[str, List[float]],
    canonical_inverted_index: Dict[str, Set[str]],
    embedding_client,
    embedding_model: Optional[str],
    embedding_batch_size: int,
) -> List[Dict[str, object]]:
    if not canonical_rows:
        return []

    alias_rows = _extract_alias_rows(canonical_rows)
    if not alias_rows:
        return []

    alias_sparse_vectors: Dict[str, Dict[str, float]] = {}
    alias_terms: List[str] = []
    for alias in sorted(alias_rows):
        alias_sparse_vectors[alias] = ngram_vector(alias)
        alias_terms.append(alias)

    alias_dense_vectors: Dict[str, List[float]] = {}
    if embedding_client is not None and alias_terms:
        vectors = embedding_client.embed_texts(
            alias_terms,
            model=embedding_model,
            batch_size=embedding_batch_size,
        )
        for index, alias in enumerate(alias_terms):
            alias_dense_vectors[alias] = vectors[index]

    advisories: List[Dict[str, object]] = []
    seen_keys: Set[Tuple[str, str, str, str]] = set()

    sorted_keys = sorted(alias_rows)
    for alias_text in sorted_keys:
        references = alias_rows.get(alias_text, [])
        if not references:
            continue

        alias_vector_sparse = alias_sparse_vectors.get(alias_text, {})
        alias_vector_dense = alias_dense_vectors.get(alias_text)

        candidate_canonicals = _find_candidate_canonicals_for_alias(
            alias_sparse_vector=alias_vector_sparse,
            canonical_inverted_index=canonical_inverted_index,
        )
        if not candidate_canonicals:
            continue

        for group, source_canonical in sorted(references):
            source_normalized = normalize_term(source_canonical)

            for target_canonical in sorted(candidate_canonicals):
                target_normalized = normalize_term(target_canonical)
                if target_normalized == source_normalized:
                    continue

                score = 0.0
                if alias_vector_dense is not None and target_canonical in canonical_dense_vectors:
                    score = _cosine_similarity_dense(alias_vector_dense, canonical_dense_vectors[target_canonical])
                else:
                    target_sparse = canonical_sparse_vectors.get(target_canonical, {})
                    score = cosine_similarity_sparse(alias_vector_sparse, target_sparse)

                if score < POSSIBLE_THRESHOLD:
                    continue

                band = "possible_conflict"
                if score > HIGH_THRESHOLD:
                    band = "high_collision"

                key = (group, source_canonical, alias_text, target_canonical)
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                advisories.append(
                    {
                        "group": group,
                        "source_canonical": source_canonical,
                        "alias": alias_text,
                        "target_canonical": target_canonical,
                        "score": round(score, 6),
                        "band": band,
                    }
                )

    advisories.sort(
        key=lambda row: (
            str(row["group"]),
            str(row["source_canonical"]),
            str(row["alias"]),
            str(row["target_canonical"]),
        )
    )
    return advisories


def _extract_alias_rows(
    canonical_rows: List[Dict[str, object]],
) -> Dict[str, List[Tuple[str, str]]]:
    alias_rows: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    seen_rows: Set[Tuple[str, str, str]] = set()

    for row in canonical_rows:
        if not isinstance(row, dict):
            continue

        group = str(row.get("group", "")).strip()
        canonical = str(row.get("canonical", "")).strip()
        aliases_value = row.get("aliases", [])
        if not canonical:
            continue
        if not isinstance(aliases_value, list):
            continue

        for alias in aliases_value:
            alias_text = str(alias).strip()
            if not alias_text:
                continue

            unique_key = (group, canonical, alias_text)
            if unique_key in seen_rows:
                continue
            seen_rows.add(unique_key)
            alias_rows[alias_text].append((group, canonical))

    return alias_rows


def _find_candidate_canonicals_for_alias(
    alias_sparse_vector: Dict[str, float],
    canonical_inverted_index: Dict[str, Set[str]],
) -> Set[str]:
    candidate_canonicals: Set[str] = set()

    for gram in alias_sparse_vector.keys():
        postings = canonical_inverted_index.get(gram, set())
        for canonical in postings:
            candidate_canonicals.add(canonical)

    return candidate_canonicals


def _build_candidate_pairs(inverted_index: Dict[str, Set[str]]) -> Set[Tuple[str, str]]:
    candidate_pairs: Set[Tuple[str, str]] = set()

    for postings in inverted_index.values():
        ordered_terms = sorted(postings)
        total = len(ordered_terms)

        left_index = 0
        while left_index < total:
            right_index = left_index + 1
            while right_index < total:
                left = ordered_terms[left_index]
                right = ordered_terms[right_index]
                candidate_pairs.add((left, right))
                right_index += 1
            left_index += 1

    return candidate_pairs


def _cosine_similarity_dense(left: List[float], right: List[float]) -> float:
    if not left:
        return 0.0
    if not right:
        return 0.0
    if len(left) != len(right):
        return 0.0

    dot_product = 0.0
    left_norm = 0.0
    right_norm = 0.0

    for left_value, right_value in zip(left, right):
        dot_product += left_value * right_value
        left_norm += left_value * left_value
        right_norm += right_value * right_value

    if left_norm == 0.0:
        return 0.0
    if right_norm == 0.0:
        return 0.0

    similarity = dot_product / ((left_norm ** 0.5) * (right_norm ** 0.5))
    return similarity

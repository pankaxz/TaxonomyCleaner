from __future__ import annotations

from collections import Counter
from collections import defaultdict
from collections import deque
from typing import Dict
from typing import List
from typing import Set

from ...shared.findings import create_finding
from ...shared.models import StageResult


def run_stage5_graph_validation(
    similarity_edges: List[Dict[str, object]],
    conflict_clusters: List[Dict[str, object]],
    classification_decisions: List[Dict[str, object]],
) -> StageResult:
    result = StageResult()

    adjacency: Dict[str, Dict[str, float]] = defaultdict(dict)
    nodes: Set[str] = set()

    for edge in similarity_edges:
        left = str(edge["left"])
        right = str(edge["right"])
        score = float(edge["score"])

        nodes.add(left)
        nodes.add(right)

        adjacency[left][right] = max(score, adjacency[left].get(right, 0.0))
        adjacency[right][left] = max(score, adjacency[right].get(left, 0.0))

    if not nodes:
        result.payload["graph_findings"] = []
        result.payload["graph_components"] = {}
        return result

    graph_components = _connected_components(adjacency)
    graph_cluster_map = _build_graph_cluster_map(graph_components)
    embedding_cluster_map = _build_embedding_cluster_map(conflict_clusters)
    abstraction_map = _build_abstraction_map(classification_decisions)

    _apply_over_generic_rule(result, nodes, adjacency)
    _apply_phantom_rule(result, nodes, adjacency)
    _apply_cluster_disagreement_rule(
        result=result,
        embedding_cluster_map=embedding_cluster_map,
        graph_cluster_map=graph_cluster_map,
        abstraction_map=abstraction_map,
    )

    findings_payload: List[Dict[str, object]] = []
    for finding in result.findings:
        findings_payload.append(finding.to_dict())
    result.payload["graph_findings"] = findings_payload

    components_payload: Dict[str, List[str]] = {}
    component_index = 1
    for component in graph_components:
        key = f"graph-{component_index:04d}"
        components_payload[key] = sorted(component)
        component_index += 1
    result.payload["graph_components"] = components_payload

    return result


def _build_graph_cluster_map(graph_components: List[Set[str]]) -> Dict[str, str]:
    cluster_map: Dict[str, str] = {}

    cluster_index = 1
    for component in graph_components:
        cluster_id = f"graph-{cluster_index:04d}"
        for term in component:
            cluster_map[term] = cluster_id
        cluster_index += 1

    return cluster_map


def _build_embedding_cluster_map(conflict_clusters: List[Dict[str, object]]) -> Dict[str, str]:
    cluster_map: Dict[str, str] = {}

    for cluster in conflict_clusters:
        cluster_id = str(cluster["cluster_id"])
        terms = cluster.get("terms", [])
        for term in terms:
            cluster_map[str(term)] = cluster_id

    return cluster_map


def _build_abstraction_map(classification_decisions: List[Dict[str, object]]) -> Dict[str, str]:
    abstraction_map: Dict[str, str] = {}

    for row in classification_decisions:
        canonical = str(row["canonical"])
        classification = row.get("classification", {})

        abstraction = ""
        if isinstance(classification, dict):
            abstraction = str(classification.get("abstraction_level", ""))

        abstraction_map[canonical] = abstraction

    return abstraction_map


def _apply_over_generic_rule(
    result: StageResult,
    nodes: Set[str],
    adjacency: Dict[str, Dict[str, float]],
) -> None:
    for term in sorted(nodes):
        degree = len(adjacency[term])

        ratio = 0.0
        if len(nodes) > 1:
            ratio = degree / (len(nodes) - 1)

        if ratio <= 0.70:
            continue

        result.add_finding(
            create_finding(
                rule_id="L5-001",
                blocking=True,
                location=f"node:{term}",
                observed_value=f"degree_ratio={ratio:.4f}",
                normalized_value="",
                proposed_action="manual_review",
                reason="Over-generic node candidate; degree ratio exceeds 0.70.",
            )
        )


def _apply_phantom_rule(
    result: StageResult,
    nodes: Set[str],
    adjacency: Dict[str, Dict[str, float]],
) -> None:
    for term in sorted(nodes):
        degree = len(adjacency[term])
        if degree <= 0:
            continue

        if degree > 1:
            continue

        total_weight = 0.0
        for score in adjacency[term].values():
            total_weight += score

        max_weight = 0.0
        for score in adjacency[term].values():
            if score > max_weight:
                max_weight = score

        pair_lock_ratio = 0.0
        if total_weight:
            pair_lock_ratio = max_weight / total_weight

        if pair_lock_ratio < 0.90:
            continue

        result.add_finding(
            create_finding(
                rule_id="L5-002",
                blocking=False,
                location=f"node:{term}",
                observed_value=f"pair_lock_ratio={pair_lock_ratio:.4f}",
                normalized_value="",
                proposed_action="manual_review",
                reason="Phantom node candidate; isolated and pair-locked.",
            )
        )


def _apply_cluster_disagreement_rule(
    result: StageResult,
    embedding_cluster_map: Dict[str, str],
    graph_cluster_map: Dict[str, str],
    abstraction_map: Dict[str, str],
) -> None:
    embedding_to_terms: Dict[str, List[str]] = defaultdict(list)
    for term, embedding_cluster in embedding_cluster_map.items():
        embedding_to_terms[embedding_cluster].append(term)

    for embedding_cluster, terms in sorted(embedding_to_terms.items()):
        if len(terms) < 3:
            continue

        abstraction_counts = Counter()
        for term in terms:
            abstraction = abstraction_map.get(term, "")
            if abstraction:
                abstraction_counts[abstraction] += 1

        if not abstraction_counts:
            continue

        majority_abstraction = abstraction_counts.most_common(1)[0][0]

        for term in sorted(terms):
            abstraction = abstraction_map.get(term, "")
            graph_cluster = graph_cluster_map.get(term, "")

            if not abstraction:
                continue
            if abstraction == majority_abstraction:
                continue
            if graph_cluster == embedding_cluster:
                continue

            observed = (
                f"embedding_cluster={embedding_cluster};"
                f"graph_cluster={graph_cluster};"
                f"abstraction={abstraction}"
            )
            result.add_finding(
                create_finding(
                    rule_id="L5-003",
                    blocking=False,
                    location=f"node:{term}",
                    observed_value=observed,
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Embedding, graph, and abstraction signals disagree.",
                )
            )


def _connected_components(adjacency: Dict[str, Dict[str, float]]) -> List[Set[str]]:
    seen: Set[str] = set()
    components: List[Set[str]] = []

    for node in sorted(adjacency):
        if node in seen:
            continue

        queue = deque([node])
        seen.add(node)
        component: Set[str] = set()

        while queue:
            current = queue.popleft()
            component.add(current)

            for neighbor in sorted(adjacency[current]):
                if neighbor in seen:
                    continue

                seen.add(neighbor)
                queue.append(neighbor)

        components.append(component)

    return components

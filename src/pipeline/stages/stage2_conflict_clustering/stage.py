from __future__ import annotations

from collections import defaultdict
from collections import deque
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple

from ...shared.models import ConflictCluster
from ...shared.models import StageResult


def run_stage2_clusters(
    similarity_edges: List[Dict[str, object]],
    max_cluster_size: int = 10,
) -> StageResult:
    result = StageResult()

    adjacency: Dict[str, Set[str]] = defaultdict(set)
    weighted_neighbors: Dict[str, List[Tuple[str, float]]] = defaultdict(list)

    for edge in similarity_edges:
        left = str(edge["left"])
        right = str(edge["right"])
        score = float(edge["score"])

        adjacency[left].add(right)
        adjacency[right].add(left)

        weighted_neighbors[left].append((right, score))
        weighted_neighbors[right].append((left, score))

    for term in weighted_neighbors:
        weighted_neighbors[term].sort(
            key=lambda item: (
                -item[1],
                item[0],
            )
        )

    components = _connected_components(adjacency)

    clusters: List[ConflictCluster] = []
    cluster_index = 1

    for component in components:
        component_size = len(component)
        if component_size <= 1:
            continue

        if component_size <= max_cluster_size:
            cluster = ConflictCluster(
                cluster_id=f"cluster-{cluster_index:04d}",
                terms=sorted(component),
            )
            clusters.append(cluster)
            cluster_index += 1
            continue

        subclusters = _split_component(
            component,
            weighted_neighbors,
            max_cluster_size,
        )
        for terms in subclusters:
            if len(terms) <= 1:
                continue

            cluster = ConflictCluster(
                cluster_id=f"cluster-{cluster_index:04d}",
                terms=sorted(terms),
            )
            clusters.append(cluster)
            cluster_index += 1

    serialized_clusters: List[Dict[str, object]] = []
    for cluster in clusters:
        serialized_clusters.append(cluster.to_dict())

    result.payload["conflict_clusters"] = serialized_clusters
    return result


def _connected_components(adjacency: Dict[str, Set[str]]) -> List[Set[str]]:
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


def _split_component(
    component: Set[str],
    weighted_neighbors: Dict[str, List[Tuple[str, float]]],
    max_cluster_size: int,
) -> List[Set[str]]:
    remaining: Set[str] = set(component)
    subclusters: List[Set[str]] = []

    while remaining:
        seed = max(
            remaining,
            key=lambda term: (
                _remaining_degree(term, remaining, weighted_neighbors),
                term,
            ),
        )

        cluster: Set[str] = {seed}
        remaining.remove(seed)

        for neighbor, _score in weighted_neighbors.get(seed, []):
            if len(cluster) >= max_cluster_size:
                break

            if neighbor not in remaining:
                continue

            cluster.add(neighbor)
            remaining.remove(neighbor)

        if len(cluster) < max_cluster_size and remaining:
            fill_candidates = sorted(remaining)
            for candidate in fill_candidates:
                if len(cluster) >= max_cluster_size:
                    break

                if not _is_linked_to_cluster(candidate, cluster, weighted_neighbors):
                    continue

                cluster.add(candidate)
                remaining.remove(candidate)

        subclusters.append(cluster)

    return subclusters


def _remaining_degree(
    term: str,
    remaining: Set[str],
    weighted_neighbors: Dict[str, List[Tuple[str, float]]],
) -> int:
    degree = 0
    for neighbor, _score in weighted_neighbors.get(term, []):
        if neighbor in remaining:
            degree += 1
    return degree


def _is_linked_to_cluster(
    candidate: str,
    cluster: Set[str],
    weighted_neighbors: Dict[str, List[Tuple[str, float]]],
) -> bool:
    for term in cluster:
        for neighbor, _score in weighted_neighbors.get(term, []):
            if neighbor == candidate:
                return True
    return False

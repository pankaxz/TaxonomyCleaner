"""Agent: Embedding-based Semantic Deduplicator

Uses a local embedding model (nomic-embed-text via llama-server) to catch
semantic duplicates that fuzzy string matching misses. Computes cosine
similarity between novel skill names and all taxonomy canonicals + aliases.

Examples it catches:
  - "node" vs "Node.js"           (short name)
  - "K8s" vs "Kubernetes"         (abbreviation)
  - "Postgres" vs "PostgreSQL"    (variant)
  - "GCP" vs "Google Cloud"       (acronym)

Runs after --assign-groups, before --semantic-dedup (LLM pass).
Fast pre-filter: one HTTP call per batch of embeddings, no LLM reasoning.

Input:  group_assignments.json (existing section) + taxonomy
Output: data/agents/sbert_dedup.json — suggested alias mappings with scores
"""

import json
import logging
import math
import urllib.request
from pathlib import Path
from typing import Any

from config import cfg
from discovery.taxonomy import TaxonomyReader

logger = logging.getLogger(__name__)


def _get_embeddings(texts: list[str], base_url: str) -> list[list[float]]:
    """Get embeddings from the local llama-server embedding endpoint.

    Sends a batch of texts and returns a list of embedding vectors.
    """
    payload = json.dumps({"input": texts}).encode()

    req = urllib.request.Request(
        f"{base_url}/v1/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read())

    # Sort by index to ensure correct ordering
    data = sorted(body["data"], key=lambda x: x["index"])
    return [item["embedding"] for item in data]


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _build_taxonomy_entries(taxonomy: dict) -> list[dict]:
    """Build a flat list of all taxonomy skill names (canonical + aliases).

    Each entry has: name, canonical, group.
    """
    entries = []
    for group, skills in taxonomy.items():
        for canonical, aliases in skills.items():
            entries.append({
                "name": canonical,
                "canonical": canonical,
                "group": group,
            })
            for alias in aliases:
                entries.append({
                    "name": alias,
                    "canonical": canonical,
                    "group": group,
                })
    return entries


class SbertDedup:
    """Detects semantic duplicates using embedding cosine similarity."""

    @classmethod
    def run(cls, *, threshold: float = 0.85) -> dict:
        """Run embedding-based dedup on skills from group_assignments.json.

        For each novel skill assigned to an existing group, computes cosine
        similarity against all taxonomy skill names. If any similarity exceeds
        the threshold, suggests it as an alias.

        Args:
            threshold: Cosine similarity threshold for alias suggestion.

        Returns:
            The full output report dict.
        """
        embed_url = cfg.get("embedding.base_url", "http://127.0.0.1:8090")
        taxonomy = TaxonomyReader._load()
        assignments = cls._load_group_assignments()

        if not assignments:
            logger.warning("sbert_dedup: no assignments to check")
            return {"meta": {}, "aliases": [], "novel": []}

        # Build taxonomy embedding index
        tax_entries = _build_taxonomy_entries(taxonomy)
        tax_names = [e["name"] for e in tax_entries]

        logger.info(
            f"sbert_dedup: embedding {len(tax_names)} taxonomy skills..."
        )

        # Embed taxonomy in batches (embedding server may have limits)
        batch_size = cfg.get("embedding.batch_size", 100)
        tax_embeddings: list[list[float]] = []
        for i in range(0, len(tax_names), batch_size):
            batch = tax_names[i : i + batch_size]
            tax_embeddings.extend(_get_embeddings(batch, embed_url))
            if (i + batch_size) % 500 == 0 or i + batch_size >= len(tax_names):
                logger.info(
                    f"sbert_dedup: embedded {min(i + batch_size, len(tax_names))}/{len(tax_names)} taxonomy skills"
                )

        logger.info(
            f"sbert_dedup: checking {len(assignments)} novel skills "
            f"against {len(tax_names)} taxonomy entries (threshold={threshold})"
        )

        # Embed novel skills
        novel_names = [a["skill_name"] for a in assignments]
        novel_embeddings = _get_embeddings(novel_names, embed_url)

        # Compare each novel skill against all taxonomy entries
        aliases = []
        novel_list = []

        for idx, assignment in enumerate(assignments):
            skill_name = assignment["skill_name"]
            skill_emb = novel_embeddings[idx]
            assigned_group = assignment["assigned_group"]

            # Find best match
            best_score = 0.0
            best_entry = None
            for j, tax_emb in enumerate(tax_embeddings):
                score = _cosine_similarity(skill_emb, tax_emb)
                if score > best_score:
                    best_score = score
                    best_entry = tax_entries[j]

            # Also find best match within the same group (more relevant)
            best_group_score = 0.0
            best_group_entry = None
            for j, tax_emb in enumerate(tax_embeddings):
                if tax_entries[j]["group"] == assigned_group:
                    score = _cosine_similarity(skill_emb, tax_emb)
                    if score > best_group_score:
                        best_group_score = score
                        best_group_entry = tax_entries[j]

            result = {
                "skill_name": skill_name,
                "assigned_group": assigned_group,
                "best_match": best_entry["canonical"] if best_entry else None,
                "best_match_name": best_entry["name"] if best_entry else None,
                "best_match_group": best_entry["group"] if best_entry else None,
                "best_score": round(best_score, 4),
                "best_in_group_match": (
                    best_group_entry["canonical"] if best_group_entry else None
                ),
                "best_in_group_name": (
                    best_group_entry["name"] if best_group_entry else None
                ),
                "best_in_group_score": round(best_group_score, 4),
            }

            if best_score >= threshold:
                result["is_alias"] = True
                result["alias_of"] = best_entry["canonical"]
                aliases.append(result)
            elif best_group_score >= threshold:
                result["is_alias"] = True
                result["alias_of"] = best_group_entry["canonical"]
                aliases.append(result)
            else:
                result["is_alias"] = False
                novel_list.append(result)

        report = {
            "meta": {
                "total_checked": len(assignments),
                "aliases_found": len(aliases),
                "confirmed_novel": len(novel_list),
                "threshold": threshold,
                "embedding_model": cfg.get(
                    "embedding.model", "nomic-embed-text-v1.5"
                ),
            },
            "aliases": aliases,
            "novel": novel_list,
        }

        # Write output
        out_dir = Path(
            cfg.get_abs_path("agents.output_dir") or "data/agents"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "sbert_dedup.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(
            f"sbert_dedup: done — {len(aliases)} aliases found, "
            f"{len(novel_list)} confirmed novel"
        )
        logger.info(f"sbert_dedup: output → {out_path}")
        return report

    @staticmethod
    def _load_group_assignments() -> list[dict]:
        """Load skills assigned to existing groups from group_assignments.json."""
        agents_dir = cfg.get_abs_path("agents.output_dir") or "data/agents"
        path = Path(agents_dir) / "group_assignments.json"
        if not path.exists():
            logger.error(
                "sbert_dedup: group_assignments.json not found — "
                "run --assign-groups first"
            )
            return []

        with open(path, "r", encoding="utf-8") as f:
            report = json.load(f)

        return report.get("existing", [])
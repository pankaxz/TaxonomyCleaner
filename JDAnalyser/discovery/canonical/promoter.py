"""Promotion manager — generate review files and apply approved promotions."""

import json
import logging
from pathlib import Path
from typing import Any, Dict

from config import cfg
from discovery.canonical.taxonomy import TaxonomyReader

logger = logging.getLogger(__name__)


def _save_json(data: Any, path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"saved {p}")
    return p


class PromotionManager:
    """Generate review files and apply approved promotions.

    Produces a final approved output in canonical-like structure
    without mutating the source canonical_data.json.

    Workflow:
        1. generate_review() → review_candidates.json
        2. Human edits (approve / reject / alias_of:X)
        3. apply_review() → updates queue status + writes approved output
    """

    @staticmethod
    def _load_queue() -> Dict[str, dict]:
        queue_path = cfg.get_abs_path("discovery.queue_path")
        if queue_path and Path(queue_path).exists():
            with open(queue_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    @staticmethod
    def _save_queue(queue: Dict[str, dict]) -> None:
        queue_path = cfg.get_abs_path("discovery.queue_path")
        _save_json(queue, queue_path)

    @staticmethod
    def _approved_output_path() -> str:
        configured = cfg.get_abs_path("discovery.approved_output")
        if configured:
            return configured

        queue_path = Path(
            cfg.get_abs_path("discovery.queue_path")
            or "data/discovery/technical_skills/discovery_queue.json"
        )
        return str(queue_path.parent / "approved_canonical_output.json")

    @staticmethod
    def _load_semantic_dedup() -> Dict[str, dict]:
        """Load semantic dedup alias suggestions keyed by lowered skill name.

        Returns empty dict if the file doesn't exist (--semantic-dedup not run).
        """
        agents_dir = cfg.get_abs_path("agents.output_dir") or "data/agents"
        path = Path(agents_dir) / "semantic_dedup.json"
        if not path.exists():
            return {}

        with open(path, "r", encoding="utf-8") as f:
            report = json.load(f)

        aliases: Dict[str, dict] = {}
        for entry in report.get("aliases", []):
            aliases[entry["skill_name"].lower()] = entry
        return aliases

    @staticmethod
    def _load_group_assignments() -> Dict[str, dict]:
        """Load LLM group assignments keyed by lowered skill name.

        Returns empty dict if the file doesn't exist (--assign-groups not run).
        """
        agents_dir = cfg.get_abs_path("agents.output_dir") or "data/agents"
        path = Path(agents_dir) / "group_assignments.json"
        if not path.exists():
            return {}

        with open(path, "r", encoding="utf-8") as f:
            report = json.load(f)

        assignments: Dict[str, dict] = {}
        # Index existing assignments
        for entry in report.get("existing", []):
            assignments[entry["skill_name"].lower()] = entry
        # Index new group suggestions (flatten from grouped structure)
        for group_entry in report.get("new_groups", []):
            suggested_group = group_entry["suggested_group"]
            for skill in group_entry.get("skills", []):
                skill["assigned_group"] = f"NEW:{suggested_group}"
                assignments[skill["skill_name"].lower()] = skill
        # Index rejected
        for entry in report.get("rejected", []):
            assignments[entry["skill_name"].lower()] = entry
        return assignments

    @classmethod
    def generate_review(cls) -> Path:
        """Write review_candidates.json with all ready_for_promotion entries.

        If --assign-groups was run first, each entry is enriched with the LLM's
        group assignment, reasoning, ontological_nature, abstraction_level, and
        confidence. The LLM's decision also influences the default action:
          - REJECT:* from LLM → action defaults to "reject"
          - alias_of match in taxonomy → "alias_of:<CanonicalName>"
          - otherwise → "approve"

        Sorted by seen_count descending.

        Returns:
            Path to the written review file.
        """
        queue = cls._load_queue()
        candidates = {
            key: entry
            for key, entry in queue.items()
            if entry.get("status") == "ready_for_promotion"
        }

        if not candidates:
            logger.info("discovery: no candidates ready for promotion")

        # Load LLM assignments if available
        llm_assignments = cls._load_group_assignments()
        if llm_assignments:
            logger.info(
                f"discovery: loaded {len(llm_assignments)} LLM group assignments"
            )

        # Load semantic dedup results if available
        semantic_aliases = cls._load_semantic_dedup()
        if semantic_aliases:
            logger.info(
                f"discovery: loaded {len(semantic_aliases)} semantic alias suggestions"
            )

        # Sort by frequency (highest first)
        sorted_candidates = dict(
            sorted(
                candidates.items(),
                key=lambda kv: kv[1].get("seen_count", 0),
                reverse=True,
            )
        )

        # Add action field + merge LLM assignments
        review: Dict[str, dict] = {}
        alias_map = TaxonomyReader.get_alias_map()
        canonical_case_map = {
            canonical.lower(): canonical for canonical in TaxonomyReader.get_all_canonicals()
        }
        group_names = TaxonomyReader.get_group_names()

        for key, entry in sorted_candidates.items():
            display_name = entry["display_name"]
            normalized = display_name.lower()

            # LLM assignment for this skill (if --assign-groups was run)
            llm = llm_assignments.get(normalized, {})
            llm_group = llm.get("assigned_group", "")

            # Determine suggested_group: prefer LLM assignment over scraper tag
            scraper_suggested = entry.get("suggested_groups", {})
            if llm_group and not llm_group.startswith("REJECT:"):
                # Strip "NEW:" prefix for the review file
                best_group = llm_group.removeprefix("NEW:")
            else:
                best_group = (
                    max(scraper_suggested, key=scraper_suggested.get, default="")
                    if scraper_suggested
                    else ""
                )

            # Check semantic dedup — LLM detected alias
            sem = semantic_aliases.get(normalized, {})
            sem_alias_of = sem.get("alias_of") if sem.get("is_alias") else None

            # Determine default action
            matched_canonical_lower = alias_map.get(normalized)
            matched_canonical = (
                canonical_case_map.get(matched_canonical_lower, display_name)
                if matched_canonical_lower
                else None
            )
            if matched_canonical:
                default_action = f"alias_of:{matched_canonical}"
            elif sem_alias_of:
                default_action = f"alias_of:{sem_alias_of}"
            elif llm_group.startswith("REJECT:"):
                default_action = "reject"
            elif normalized in group_names:
                default_action = "reject"
            else:
                default_action = "approve"

            review_entry = {
                "display_name": display_name,
                "seen_count": entry["seen_count"],
                "suggested_group": best_group,
                "all_suggested_groups": scraper_suggested,
                "sample_sources": entry.get("sample_sources", [])[:5],
                "action": default_action,
            }

            # Enrich with LLM fields if available
            if llm:
                review_entry["llm_reasoning"] = llm.get("reasoning", "")
                review_entry["llm_group"] = llm_group
                review_entry["ontological_nature"] = llm.get(
                    "ontological_nature", ""
                )
                review_entry["abstraction_level"] = llm.get(
                    "abstraction_level", ""
                )
                review_entry["confidence"] = llm.get("confidence", "")

            # Enrich with semantic dedup info if available
            if sem:
                review_entry["semantic_dedup"] = {
                    "is_alias": sem.get("is_alias", False),
                    "alias_of": sem.get("alias_of"),
                    "confidence": sem.get("confidence", ""),
                    "reasoning": sem.get("reasoning", ""),
                }

            review[key] = review_entry

        review_path = cfg.get_abs_path("discovery.review_output")
        _save_json(review, review_path)
        logger.info(
            f"discovery: generated review with {len(review)} candidates → {review_path}"
        )
        return Path(review_path)

    @classmethod
    def apply_review(cls) -> Dict[str, int]:
        """Read reviewed actions and write final approved output.

        Actions:
            "approve"           → include as new canonical under suggested_group
            "alias_of:Existing" → include as alias of an existing canonical skill
            "reject"            → mark rejected in queue, do not include

        Returns:
            Summary counts: {approved, aliased, rejected, skipped}
        """
        review_path = cfg.get_abs_path("discovery.review_output")
        if not review_path or not Path(review_path).exists():
            logger.error(f"discovery: review file not found at {review_path}")
            return {"approved": 0, "aliased": 0, "rejected": 0, "skipped": 0}

        with open(review_path, "r", encoding="utf-8") as f:
            review = json.load(f)

        # Load canonical data for validation/lookups only.
        canonical_path = cfg.get_abs_path("taxonomy.canonical_data")
        with open(canonical_path, "r", encoding="utf-8") as f:
            canonical: Dict[str, Dict[str, list]] = json.load(f)

        queue = cls._load_queue()
        approved_output: Dict[str, Dict[str, list]] = {}
        counts = {"approved": 0, "aliased": 0, "rejected": 0, "skipped": 0}

        for key, entry in review.items():
            action = entry.get("action", "approve").strip()
            display_name = entry.get("display_name", key)

            if action == "reject":
                if key in queue:
                    queue[key]["status"] = "rejected"
                counts["rejected"] += 1
                logger.info(f"  rejected: {display_name}")

            elif action.startswith("alias_of:"):
                # Add as alias of existing canonical
                target = action.split(":", 1)[1].strip()
                added = cls._add_alias(
                    approved_output,
                    display_name,
                    target,
                    canonical_lookup=canonical,
                )
                if added:
                    if key in queue:
                        queue[key]["status"] = "promoted"
                    counts["aliased"] += 1
                    logger.info(f"  aliased: {display_name} → {target}")
                else:
                    counts["skipped"] += 1
                    logger.warning(
                        f"  skipped alias: {display_name} → {target} (target not found)"
                    )

            elif action == "approve":
                group = entry.get("suggested_group", "")
                if not group:
                    counts["skipped"] += 1
                    logger.warning(f"  skipped: {display_name} (no suggested_group)")
                    continue
                cls._add_canonical(approved_output, display_name, group)
                if key in queue:
                    queue[key]["status"] = "promoted"
                counts["approved"] += 1
                logger.info(f"  approved: {display_name} → [{group}]")

            else:
                counts["skipped"] += 1
                logger.warning(f"  unknown action '{action}' for {display_name}")

        # Persist changes
        approved_path = cls._approved_output_path()
        _save_json(approved_output, approved_path)
        cls._save_queue(queue)

        # Taxonomy remains unchanged, but keep behavior explicit for callers.
        TaxonomyReader.invalidate()

        total = sum(counts.values())
        logger.info(
            f"discovery: applied review — "
            f"{counts['approved']} approved, {counts['aliased']} aliased, "
            f"{counts['rejected']} rejected, {counts['skipped']} skipped "
            f"({total} total)"
        )
        logger.info(f"discovery: approved output written to {approved_path}")
        return counts

    @staticmethod
    def _add_canonical(
        canonical: Dict[str, Dict[str, list]], skill_name: str, group: str
    ) -> None:
        """Add a new canonical skill to a group."""
        if group not in canonical:
            canonical[group] = {}
        canonical[group][skill_name] = []

    @staticmethod
    def _add_alias(
        approved_output: Dict[str, Dict[str, list]],
        alias: str,
        target_canonical: str,
        canonical_lookup: Dict[str, Dict[str, list]],
    ) -> bool:
        """Add alias for target canonical into approved_output.

        Target can resolve from:
          1) already collected approved_output entries in this run
          2) existing canonical_lookup (read-only source canonical data)
        Returns False if target canonical is not found.
        """
        target_lower = target_canonical.lower()
        for group, skills in approved_output.items():
            for canon, aliases in skills.items():
                if canon.lower() == target_lower:
                    # Avoid self-alias entries like "Databases" -> alias_of:Databases.
                    if alias.lower() == canon.lower():
                        return True
                    if alias not in aliases:
                        aliases.append(alias)
                    return True

        for group, skills in canonical_lookup.items():
            for canon, aliases in skills.items():
                if canon.lower() == target_lower:
                    if alias.lower() == canon.lower():
                        return True
                    approved_output.setdefault(group, {})
                    approved_output[group].setdefault(canon, [])
                    if alias not in approved_output[group][canon]:
                        approved_output[group][canon].append(alias)
                    return True
        return False

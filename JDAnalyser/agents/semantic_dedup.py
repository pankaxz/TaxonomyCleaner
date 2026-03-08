"""Agent: Semantic Deduplicator

Catches semantic equivalences that fuzzy string matching (SequenceMatcher) misses.
Examples the 4-tier dedup in dedup.py cannot catch:
  - "K8s" vs "Kubernetes"         (abbreviation)
  - "Postgres" vs "PostgreSQL"    (short name)
  - "React.js" vs "React"         (suffix variation)
  - "ML Ops" vs "MLOps"           (spacing)
  - "GCP" vs "Google Cloud"       (acronym)

Runs after --assign-groups. For each novel skill assigned to an existing group,
asks the LLM whether it's semantically equivalent to any canonical skill in
that group (or closely related groups).

Input:  group_assignments.json (existing section) + taxonomy
Output: data/agents/semantic_dedup.json — suggested alias mappings

Uses the local llama-server (OpenAI-compatible API) for classification.
"""

import json
import logging
import re
import urllib.request
from pathlib import Path
from typing import Any

from config import cfg
from discovery.taxonomy import TaxonomyReader

logger = logging.getLogger(__name__)


def _llm_chat(messages: list[dict], *, temperature: float | None = None) -> str:
    """Send a chat completion request to the local llama-server."""
    base_url = cfg.get("llm.base_url", "http://localhost:8080/v1")
    model = cfg.get("llm.model", "")
    temp = temperature if temperature is not None else cfg.get("llm.temperature", 0.1)
    max_tokens = cfg.get("llm.max_tokens", 8192)

    payload = json.dumps(
        {
            "model": model,
            "messages": messages,
            "temperature": temp,
            "max_tokens": max_tokens,
        }
    ).encode()

    req = urllib.request.Request(
        f"{base_url}/chat/completions",
        data=payload,
        headers={"Content-Type": "application/json"},
    )

    with urllib.request.urlopen(req, timeout=300) as resp:
        body = json.loads(resp.read())

    return body["choices"][0]["message"]["content"]


def _parse_llm_response(text: str) -> list[dict]:
    """Extract JSON object from LLM response.

    The model often wraps its answer in reasoning text, outputs Python-style
    booleans, or returns multiple JSON objects. This parser finds the JSON
    object containing "is_alias" and extracts it.
    """
    cleaned = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    cleaned = re.sub(r"```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"```", "", cleaned)
    cleaned = cleaned.strip()

    # Fix Python-style booleans/None → JSON true/false/null
    cleaned = re.sub(r'\bTrue\b', 'true', cleaned)
    cleaned = re.sub(r'\bFalse\b', 'false', cleaned)
    cleaned = re.sub(r'\bNone\b', 'null', cleaned)

    # Strategy: find every '{' and try to parse a JSON object starting there.
    # Return the first one that has an "is_alias" key.
    for i, ch in enumerate(cleaned):
        if ch == '{':
            # Try progressively larger substrings ending at each '}'
            for j in range(i + 1, len(cleaned)):
                if cleaned[j] == '}':
                    candidate = cleaned[i : j + 1]
                    try:
                        parsed = json.loads(candidate)
                        if isinstance(parsed, dict) and "is_alias" in parsed:
                            return [parsed]
                    except (json.JSONDecodeError, ValueError):
                        continue

    raise ValueError(f"Could not parse LLM response: {cleaned[:200]}")


def _build_group_context(taxonomy: dict, group_name: str) -> str:
    """Build a list of all canonicals + aliases for a specific group.

    This gives the LLM the full picture of what's already in the group
    so it can detect semantic equivalences.
    """
    skills = taxonomy.get(group_name, {})
    if not skills:
        return f"Group '{group_name}' has no skills yet."

    lines = []
    for canonical, aliases in skills.items():
        if aliases:
            alias_str = ", ".join(aliases)
            lines.append(f"- {canonical} (aliases: {alias_str})")
        else:
            lines.append(f"- {canonical}")
    return "\n".join(lines)


_SYSTEM_PROMPT = """\
You are a skill taxonomy deduplication expert. You will be given a candidate \
skill and a list of existing canonical skills (with their aliases) from the \
same taxonomy group.

Your job: determine if the candidate is semantically equivalent to any \
existing canonical skill. Common equivalence patterns:
- Abbreviations: "K8s" = "Kubernetes", "GCP" = "Google Cloud Platform"
- Short names: "Postgres" = "PostgreSQL", "Mongo" = "MongoDB"
- Suffix variations: "React.js" = "React", "Node.js" = "Node"
- Spacing/casing: "MLOps" = "ML Ops", "DevOps" = "Dev Ops"
- Product versions: "Python 3" = "Python", "ES6" = "ECMAScript"
- Vendor prefixes: "AWS Lambda" might alias "Lambda" if context is clear

Be CONSERVATIVE — only flag true semantic equivalences. Two skills in the \
same domain are NOT aliases just because they're related. \
"Docker" and "Kubernetes" are NOT aliases. "React" and "Angular" are NOT aliases.

Respond with a JSON object:
{"is_alias": true/false, "alias_of": "ExactCanonicalName or null", \
"confidence": "HIGH/MEDIUM/LOW", "reasoning": "one sentence why"}

No markdown fences, no extra text — just the raw JSON object."""


class SemanticDedup:
    """Detects semantic duplicates that string matching cannot catch."""

    @classmethod
    def run(cls) -> dict:
        """Run semantic dedup on skills from group_assignments.json.

        Compares each skill assigned to an existing group against all
        canonicals + aliases in that group. Uses checkpoint for resume.

        Returns:
            The full output report dict.
        """
        taxonomy = TaxonomyReader._load()
        assignments = cls._load_group_assignments()

        if not assignments:
            logger.warning("semantic_dedup: no assignments to check")
            return {"meta": {}, "aliases": [], "novel": []}

        logger.info(
            f"semantic_dedup: checking {len(assignments)} skills for semantic duplicates"
        )

        # Checkpoint setup
        out_dir = Path(
            cfg.get_abs_path("agents.output_dir") or "data/agents"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_path = out_dir / "semantic_dedup.checkpoint.json"

        # Load checkpoint if it exists
        completed: dict[str, dict] = {}
        if checkpoint_path.exists():
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                checkpoint_data = json.load(f)
            for entry in checkpoint_data.get("results", []):
                completed[entry["skill_name"].lower()] = entry
            logger.info(
                f"semantic_dedup: resuming — {len(completed)} already done, "
                f"{len(assignments) - len(completed)} remaining"
            )

        remaining = [
            a for a in assignments
            if a["skill_name"].lower() not in completed
        ]

        total = len(assignments)
        for i, skill_entry in enumerate(remaining, 1):
            skill_name = skill_entry["skill_name"]
            group = skill_entry["assigned_group"]
            done_so_far = len(completed)

            logger.info(
                f"semantic_dedup: [{done_so_far + 1}/{total}] {skill_name} ({group})"
            )

            group_context = _build_group_context(taxonomy, group)

            user_prompt = (
                f"CANDIDATE SKILL: {skill_name}\n\n"
                f"EXISTING SKILLS IN GROUP '{group}':\n{group_context}"
            )
            messages = [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ]

            try:
                raw_reply = _llm_chat(messages)
                parsed = _parse_llm_response(raw_reply)
                llm_result = parsed[0] if parsed else {}
                if not isinstance(llm_result, dict):
                    logger.warning(
                        f"semantic_dedup: unexpected response type for {skill_name}: {type(llm_result)}"
                    )
                    llm_result = {
                        "is_alias": False,
                        "alias_of": None,
                        "confidence": "",
                        "reasoning": f"Unexpected LLM response: {llm_result}",
                    }
            except Exception as e:
                logger.error(f"semantic_dedup: failed for {skill_name}: {e}")
                llm_result = {
                    "is_alias": False,
                    "alias_of": None,
                    "confidence": "",
                    "reasoning": f"LLM error: {e}",
                }

            result = {
                "skill_name": skill_name,
                "group": group,
                "is_alias": llm_result.get("is_alias", False),
                "alias_of": llm_result.get("alias_of"),
                "confidence": llm_result.get("confidence", ""),
                "reasoning": llm_result.get("reasoning", ""),
            }
            completed[skill_name.lower()] = result

            # Save checkpoint after every skill
            cls._save_checkpoint(checkpoint_path, completed)

        # Build final report
        all_results = list(completed.values())
        aliases = [r for r in all_results if r.get("is_alias")]
        novel = [r for r in all_results if not r.get("is_alias")]

        report = {
            "meta": {
                "total_checked": len(all_results),
                "aliases_found": len(aliases),
                "confirmed_novel": len(novel),
                "model": cfg.get("llm.model", ""),
            },
            "aliases": aliases,
            "novel": novel,
        }

        out_path = out_dir / "semantic_dedup.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        # Clear checkpoint on success
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            logger.info("semantic_dedup: checkpoint cleared")

        logger.info(
            f"semantic_dedup: done — {len(aliases)} aliases found, "
            f"{len(novel)} confirmed novel"
        )
        logger.info(f"semantic_dedup: output → {out_path}")
        return report

    @staticmethod
    def _load_group_assignments() -> list[dict]:
        """Load skills assigned to existing groups, excluding any already
        resolved as aliases by --sbert-dedup.
        """
        agents_dir = cfg.get_abs_path("agents.output_dir") or "data/agents"
        agents_path = Path(agents_dir)

        path = agents_path / "group_assignments.json"
        if not path.exists():
            logger.error(
                "semantic_dedup: group_assignments.json not found — "
                "run --assign-groups first"
            )
            return []

        with open(path, "r", encoding="utf-8") as f:
            report = json.load(f)

        existing = report.get("existing", [])

        # Filter out skills already flagged as aliases by sbert_dedup
        sbert_path = agents_path / "sbert_dedup.json"
        if sbert_path.exists():
            with open(sbert_path, "r", encoding="utf-8") as f:
                sbert_report = json.load(f)
            sbert_aliases = {
                entry["skill_name"].lower()
                for entry in sbert_report.get("aliases", [])
            }
            before = len(existing)
            existing = [
                e for e in existing
                if e["skill_name"].lower() not in sbert_aliases
            ]
            skipped = before - len(existing)
            if skipped:
                logger.info(
                    f"semantic_dedup: skipping {skipped} skills "
                    f"already resolved by sbert_dedup"
                )

        return existing

    @staticmethod
    def _save_checkpoint(path: Path, completed: dict[str, dict]) -> None:
        data = {"results": list(completed.values())}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
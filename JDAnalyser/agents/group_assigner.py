"""Agent: Group Assigner

Assigns the correct taxonomy group for novel skills using LLM reasoning.
The scraper's [Category] tags are inconsistent — e.g. Fastify gets tagged as
both "NodeJS Frameworks" and "Backend Systems" across different JDs.

Input:  novel skills from the discovery queue (ready_for_promotion or all)
Output: data/agents/group_assignments.json — each skill mapped to one group

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
    """Send a chat completion request to the local llama-server.

    Uses urllib (stdlib) — no external dependencies needed.
    Returns the assistant's reply text.
    """
    base_url = cfg.get("llm.base_url", "http://localhost:8080/v1")
    model = cfg.get("llm.model", "")
    temp = temperature if temperature is not None else cfg.get("llm.temperature", 0.1)
    max_tokens = cfg.get("llm.max_tokens", 4096)

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


def _build_taxonomy_context(taxonomy: dict) -> str:
    """Build a concise summary of all groups with sample skills for the prompt.

    Includes empty groups so the LLM knows they exist and can assign to them.
    """
    lines = []
    for group in sorted(taxonomy.keys()):
        skills = list(taxonomy[group].keys())
        if not skills:
            lines.append(f"- {group}: (no skills yet)")
            continue
        samples = ", ".join(skills[:6])
        suffix = f", ... ({len(skills)} total)" if len(skills) > 6 else ""
        lines.append(f"- {group}: {samples}{suffix}")
    return "\n".join(lines)


_SYSTEM_PROMPT = """\
You are a skill taxonomy classifier. You will be given a list of candidate \
skills and a taxonomy of existing groups with sample skills.

For each skill, follow these steps:

1. FIRST decide: is this a concrete technical skill, tool, framework, \
platform, or methodology? Or is it a soft skill, generic business term, \
or job responsibility?

2. If it is NOT a technical skill, set group to one of:
   - "REJECT:soft_skill" — interpersonal abilities (Communication, Leadership, Problem Solving)
   - "REJECT:generic_term" — too vague to be actionable (Emerging Technologies, Workflow)
   - "REJECT:business_domain" — industry/domain, not a skill (Finance, Logistics, Fintech)

3. If it IS a technical skill, pick the single best-matching group from \
the taxonomy. Prefer existing groups — even empty ones with "(no skills yet)". \
Only use "NEW:<SuggestedGroupName>" if no existing group is a reasonable fit.

4. For EVERY skill (including rejected ones), also classify:
   - "ontological_nature": one of "Software Artifact", "Concept", "Algorithm", \
"Protocol", "Standard / Specification", "Human Skill"
   - "abstraction_level": one of "Domain", "Method", "Concrete"
   - "confidence": one of "HIGH", "MEDIUM", "LOW"

Respond ONLY with a JSON array. Each element must have exactly these fields:
{"skill": "...", "group": "...", "reasoning": "one sentence why", \
"ontological_nature": "...", "abstraction_level": "...", "confidence": "..."}

No markdown fences, no extra text — just the raw JSON array."""


def _build_user_prompt(
    taxonomy_context: str, skill_batch: list[dict]
) -> str:
    """Build the user prompt for one batch of skills."""
    skill_lines = []
    for i, s in enumerate(skill_batch, 1):
        tags = s.get("suggested_groups", {})
        tag_hint = ""
        if tags:
            top_tags = sorted(tags, key=tags.get, reverse=True)[:3]
            tag_hint = f" (scraper suggested: {', '.join(top_tags)})"
        skill_lines.append(f"{i}. {s['display_name']}{tag_hint}")

    return (
        f"TAXONOMY GROUPS:\n{taxonomy_context}\n\n"
        f"SKILLS TO CLASSIFY:\n" + "\n".join(skill_lines)
    )


def _parse_llm_response(text: str) -> list[dict]:
    """Extract JSON array from LLM response, tolerating markdown fences,
    thinking tags, and truncated output.

    If the response is cut off mid-JSON (no closing ']'), we attempt to
    salvage all fully-formed objects by finding the last complete '}' and
    appending ']' to close the array.
    """
    # Strip <think>...</think> blocks (Qwen/DeepSeek reasoning traces)
    cleaned = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    # Strip markdown code fences
    cleaned = re.sub(r"```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"```", "", cleaned)
    cleaned = cleaned.strip()

    # Try to find a JSON array first
    start = cleaned.find("[")
    end = cleaned.rfind("]")

    if start != -1 and end != -1 and end > start:
        # Normal case — complete JSON array
        return json.loads(cleaned[start : end + 1])

    # Model may return a single object {...} instead of an array (batch_size=1)
    obj_start = cleaned.find("{")
    obj_end = cleaned.rfind("}")
    if obj_start != -1 and obj_end != -1 and obj_end > obj_start:
        parsed = json.loads(cleaned[obj_start : obj_end + 1])
        if isinstance(parsed, dict):
            return [parsed]

    # Truncated array — try to recover completed objects
    if start != -1:
        fragment = cleaned[start:]
        last_brace = fragment.rfind("}")
        if last_brace != -1:
            salvaged = fragment[: last_brace + 1] + "]"
            try:
                result = json.loads(salvaged)
                logger.warning(
                    f"group_assigner: recovered {len(result)} skills from truncated response"
                )
                return result
            except json.JSONDecodeError:
                pass

    raise ValueError(f"Could not parse LLM response: {cleaned[:200]}")


class GroupAssigner:
    """Assigns taxonomy groups to novel skills via LLM."""

    @classmethod
    def run(cls, *, source: str = "ready_for_promotion") -> dict:
        """Run group assignment with checkpoint/resume support.

        Saves progress after each batch to a checkpoint file. If interrupted,
        re-running picks up from the last completed batch instead of starting over.

        Args:
            source: Which queue entries to process.
                "ready_for_promotion" — only skills at promotion threshold
                "all" — all novel skills in the queue

        Returns:
            The full output report dict.
        """
        # Load inputs
        taxonomy = TaxonomyReader._load()
        taxonomy_context = _build_taxonomy_context(taxonomy)
        valid_groups = set(taxonomy.keys())

        queue = cls._load_queue()
        if source == "ready_for_promotion":
            skills = [
                v for v in queue.values()
                if v.get("status") == "ready_for_promotion"
            ]
        else:
            skills = list(queue.values())

        if not skills:
            logger.warning("group_assigner: no skills to classify")
            return {"meta": {}, "assignments": []}

        # Checkpoint setup — keyed by skill display_name for stable resume
        out_dir = Path(
            cfg.get_abs_path("agents.output_dir") or "data/agents"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_path = out_dir / "group_assignments.checkpoint.json"

        # Load checkpoint if it exists
        completed: dict[str, dict] = {}
        if checkpoint_path.exists():
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                checkpoint_data = json.load(f)
            for entry in checkpoint_data.get("assignments", []):
                completed[entry["skill_name"].lower()] = entry
            logger.info(
                f"group_assigner: resuming — {len(completed)} skills "
                f"already done, {len(skills) - len(completed)} remaining"
            )

        # Filter out already-completed skills
        remaining = [
            s for s in skills
            if s["display_name"].lower() not in completed
        ]

        if not remaining:
            logger.info("group_assigner: all skills already completed in checkpoint")
        else:
            logger.info(
                f"group_assigner: classifying {len(remaining)} skills "
                f"against {len(valid_groups)} groups"
            )

        # Process remaining skills in batches
        batch_size = cfg.get("llm.batch_size", 15)
        total_batches = (len(remaining) + batch_size - 1) // batch_size

        for batch_idx in range(0, len(remaining), batch_size):
            batch = remaining[batch_idx : batch_idx + batch_size]
            batch_num = batch_idx // batch_size + 1
            done_so_far = len(completed)
            logger.info(
                f"group_assigner: batch {batch_num}/{total_batches} "
                f"({len(batch)} skills) — {done_so_far}/{len(skills)} total done"
            )

            user_prompt = _build_user_prompt(taxonomy_context, batch)
            messages = [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ]

            try:
                raw_reply = _llm_chat(messages)
                parsed = _parse_llm_response(raw_reply)
            except Exception as e:
                logger.error(
                    f"group_assigner: batch {batch_num} failed: {e}"
                )
                for s in batch:
                    entry = {
                        "skill_name": s["display_name"],
                        "assigned_group": None,
                        "reasoning": f"LLM error: {e}",
                        "ontological_nature": "",
                        "abstraction_level": "",
                        "confidence": "",
                    }
                    completed[s["display_name"].lower()] = entry
                cls._save_checkpoint(checkpoint_path, completed)
                continue

            # Match LLM output back to the batch by skill name
            llm_map = {item["skill"].lower(): item for item in parsed}
            for s in batch:
                key = s["display_name"].lower()
                llm_item = llm_map.get(key)
                if llm_item:
                    entry = {
                        "skill_name": s["display_name"],
                        "assigned_group": llm_item["group"],
                        "reasoning": llm_item.get("reasoning", ""),
                        "ontological_nature": llm_item.get(
                            "ontological_nature", ""
                        ),
                        "abstraction_level": llm_item.get(
                            "abstraction_level", ""
                        ),
                        "confidence": llm_item.get("confidence", ""),
                        "scraper_suggested": s.get(
                            "suggested_groups", {}
                        ),
                    }
                else:
                    entry = {
                        "skill_name": s["display_name"],
                        "assigned_group": None,
                        "reasoning": "LLM did not return this skill",
                        "ontological_nature": "",
                        "abstraction_level": "",
                        "confidence": "",
                    }
                completed[key] = entry

            # Save checkpoint after every batch
            cls._save_checkpoint(checkpoint_path, completed)

        # Build final report from all completed assignments
        all_assignments = list(completed.values())

        # Split into sections
        existing_list = []
        new_list = []
        rejected_list = []
        failed_list = []

        for a in all_assignments:
            group = a["assigned_group"]
            if group is None:
                failed_list.append(a)
            elif group.startswith("REJECT:"):
                rejected_list.append(a)
            elif group.startswith("NEW:"):
                new_list.append(a)
            elif group in valid_groups:
                existing_list.append(a)
            else:
                # LLM returned a group name that doesn't exist — treat as new
                a["assigned_group"] = f"NEW:{group}"
                a["reasoning"] += " (group not found in taxonomy, converted to NEW)"
                new_list.append(a)

        # Consolidate duplicate NEW: suggestions — group by suggested name
        new_consolidated: dict[str, dict] = {}
        for a in new_list:
            new_name = a["assigned_group"].removeprefix("NEW:")
            key = new_name.lower().strip()
            if key not in new_consolidated:
                new_consolidated[key] = {
                    "suggested_group": new_name,
                    "skills": [],
                }
            new_consolidated[key]["skills"].append(
                {
                    "skill_name": a["skill_name"],
                    "reasoning": a["reasoning"],
                    "ontological_nature": a["ontological_nature"],
                    "abstraction_level": a["abstraction_level"],
                    "confidence": a["confidence"],
                }
            )

        report = {
            "meta": {
                "total_skills": len(skills),
                "assigned_to_existing_group": len(existing_list),
                "suggested_new_groups": len(new_consolidated),
                "suggested_new_skills": len(new_list),
                "rejected": len(rejected_list),
                "failed": len(failed_list),
                "model": cfg.get("llm.model", ""),
                "source": source,
            },
            "existing": existing_list,
            "new_groups": list(new_consolidated.values()),
            "rejected": rejected_list,
            "failed": failed_list,
        }

        # Write final output
        out_path = out_dir / "group_assignments.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        # Remove checkpoint — run completed successfully
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            logger.info("group_assigner: checkpoint cleared")

        logger.info(
            f"group_assigner: done — {len(existing_list)} existing, "
            f"{len(new_consolidated)} new groups ({len(new_list)} skills), "
            f"{len(rejected_list)} rejected, {len(failed_list)} failed"
        )
        logger.info(f"group_assigner: output → {out_path}")
        return report

    @staticmethod
    def _save_checkpoint(path: Path, completed: dict[str, dict]) -> None:
        """Write checkpoint file with all assignments completed so far."""
        data = {"assignments": list(completed.values())}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    @staticmethod
    def _load_queue() -> dict:
        queue_path = cfg.get_abs_path("discovery.queue_path")
        if queue_path and Path(queue_path).exists():
            with open(queue_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
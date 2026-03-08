"""Merge approved JDAnalyser candidates into canonical_data.json.

Follows the same patterns as resolve_duplicate_keywords.py:
  - load_store() for JSON loading
  - normalize_term() for case-insensitive comparison
  - write_store() for sorted output
  - write_report() for JSON merge report

Collision detection:
  If a candidate canonical already exists as an alias (or canonical) in the
  existing store, skip it and log to the merge report.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Add project root so we can import main.sort_canonical_data
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
sys.path.insert(0, PROJECT_ROOT)

from main import sort_canonical_data  # noqa: E402


# ---------------------------------------------------------------------------
# Utilities (same as resolve_duplicate_keywords.py)
# ---------------------------------------------------------------------------
def normalize_term(term: str) -> str:
    return " ".join(term.lower().split()).strip()


def load_store(path: str) -> Dict[str, Dict[str, List[str]]]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected dict, got {type(payload).__name__}")
    store: Dict[str, Dict[str, List[str]]] = {}
    for group, canonical_map in payload.items():
        if not isinstance(canonical_map, dict):
            continue
        group_map: Dict[str, List[str]] = {}
        for canonical, aliases in canonical_map.items():
            alias_list = [a for a in aliases if isinstance(a, str)] if isinstance(aliases, list) else []
            group_map[canonical] = alias_list
        store[group] = group_map
    return store


def write_store(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def write_report(path: str, report: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, sort_keys=True)
        f.write("\n")


# ---------------------------------------------------------------------------
# Reverse index: normalized alias/canonical → (group, canonical_spelling)
# ---------------------------------------------------------------------------
def build_reverse_index(
    store: Dict[str, Dict[str, List[str]]],
) -> Dict[str, List[Tuple[str, str]]]:
    """Map every normalized term (canonical or alias) → list of (group, canonical)."""
    index: Dict[str, List[Tuple[str, str]]] = {}
    for group, canonical_map in store.items():
        for canonical, aliases in canonical_map.items():
            key = normalize_term(canonical)
            index.setdefault(key, []).append((group, canonical))
            for alias in aliases:
                akey = normalize_term(alias)
                index.setdefault(akey, []).append((group, canonical))
    return index


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------
def merge(
    existing: Dict[str, Dict[str, List[str]]],
    candidates: Dict[str, Dict[str, List[str]]],
) -> Dict[str, object]:
    """Merge candidates into existing store (mutates existing). Returns report."""
    reverse = build_reverse_index(existing)

    new_groups: List[str] = []
    new_skills: List[Dict[str, str]] = []
    merged_aliases: List[Dict[str, object]] = []
    skipped_collisions: List[Dict[str, object]] = []

    for group, canonical_map in candidates.items():
        group_is_new = group not in existing

        for canonical, aliases in canonical_map.items():
            canon_key = normalize_term(canonical)

            # --- collision check: candidate canonical already in existing ---
            hits = reverse.get(canon_key, [])
            if hits:
                skipped_collisions.append({
                    "candidate_canonical": canonical,
                    "candidate_group": group,
                    "candidate_aliases": aliases,
                    "existing_occurrences": [
                        {"group": g, "canonical": c} for g, c in hits
                    ],
                })
                continue

            # --- also check each alias for collisions ---
            clean_aliases: List[str] = []
            for alias in aliases:
                alias_key = normalize_term(alias)
                alias_hits = reverse.get(alias_key, [])
                if alias_hits:
                    skipped_collisions.append({
                        "candidate_canonical": canonical,
                        "candidate_alias": alias,
                        "candidate_group": group,
                        "existing_occurrences": [
                            {"group": g, "canonical": c} for g, c in alias_hits
                        ],
                    })
                else:
                    clean_aliases.append(alias)

            # --- add to existing store ---
            if group not in existing:
                existing[group] = {}
                if group_is_new and group not in new_groups:
                    new_groups.append(group)
                    group_is_new = False  # only record once

            existing_group = existing[group]

            # Check if canonical already exists in this group (case-insensitive)
            existing_canon = None
            for ec in existing_group:
                if normalize_term(ec) == canon_key:
                    existing_canon = ec
                    break

            if existing_canon is not None:
                # Merge aliases into existing canonical entry
                existing_aliases = set(normalize_term(a) for a in existing_group[existing_canon])
                added = []
                for alias in clean_aliases:
                    if normalize_term(alias) not in existing_aliases:
                        existing_group[existing_canon].append(alias)
                        existing_aliases.add(normalize_term(alias))
                        added.append(alias)
                if added:
                    merged_aliases.append({
                        "group": group,
                        "canonical": existing_canon,
                        "added_aliases": added,
                    })
            else:
                # New skill in this group
                existing_group[canonical] = clean_aliases
                new_skills.append({"group": group, "canonical": canonical})

            # Update reverse index with newly added entries
            reverse.setdefault(canon_key, []).append((group, canonical))
            for alias in clean_aliases:
                reverse.setdefault(normalize_term(alias), []).append((group, canonical))

    return {
        "summary": {
            "new_groups": len(new_groups),
            "new_skills": len(new_skills),
            "merged_aliases": len(merged_aliases),
            "skipped_collisions": len(skipped_collisions),
        },
        "new_groups": new_groups,
        "new_skills": new_skills,
        "merged_aliases": merged_aliases,
        "skipped_collisions": skipped_collisions,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge approved candidates into canonical_data.json")
    p.add_argument(
        "--existing",
        default="Input/canonical_data.json",
        help="Path to existing canonical store.",
    )
    p.add_argument(
        "--candidates",
        default="Input/NewCandidates/approved_canonical_output.json",
        help="Path to approved candidate JSON.",
    )
    p.add_argument(
        "--output",
        default="Input/canonical_data_with_candidates.json",
        help="Path to write merged output.",
    )
    p.add_argument(
        "--report",
        default="Output/merge_report.json",
        help="Path to write merge report.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    existing = load_store(args.existing)
    candidates = load_store(args.candidates)

    report = merge(existing, candidates)

    # Sort the merged store
    sorted_store = sort_canonical_data(existing)

    write_store(args.output, sorted_store)
    write_report(args.report, report)

    s = report["summary"]
    print(f"Existing: {args.existing}")
    print(f"Candidates: {args.candidates}")
    print(f"Output: {args.output}")
    print(f"Report: {args.report}")
    print(f"New groups:        {s['new_groups']}")
    print(f"New skills:        {s['new_skills']}")
    print(f"Merged aliases:    {s['merged_aliases']}")
    print(f"Skipped collisions: {s['skipped_collisions']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

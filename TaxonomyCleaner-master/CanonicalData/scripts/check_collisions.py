"""Check new candidates against canonical_data.json and remove collisions.

Builds a reverse index of all normalized terms (canonicals + aliases) in the
existing store. Any candidate canonical or alias that collides is removed.
Produces a collision-free candidates file and a collision report.

Follows the same patterns as resolve_duplicate_keywords.py and merge_candidates.py.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
sys.path.insert(0, PROJECT_ROOT)

from main import sort_canonical_data  # noqa: E402


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


def write_json_file(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
        f.write("\n")


def build_reverse_index(
    store: Dict[str, Dict[str, List[str]]],
) -> Dict[str, List[Tuple[str, str, str]]]:
    """Map normalized term -> list of (group, canonical, kind)."""
    index: Dict[str, List[Tuple[str, str, str]]] = {}
    for group, canonical_map in store.items():
        for canonical, aliases in canonical_map.items():
            key = normalize_term(canonical)
            index.setdefault(key, []).append((group, canonical, "canonical"))
            for alias in aliases:
                akey = normalize_term(alias)
                index.setdefault(akey, []).append((group, canonical, "alias"))
    return index


def check_collisions(
    existing: Dict[str, Dict[str, List[str]]],
    candidates: Dict[str, Dict[str, List[str]]],
) -> Tuple[Dict[str, Dict[str, List[str]]], dict]:
    """Remove colliding entries from candidates. Returns (clean_candidates, report)."""
    index = build_reverse_index(existing)

    removed_canonicals: List[dict] = []
    removed_aliases: List[dict] = []
    clean: Dict[str, Dict[str, List[str]]] = {}

    for group, canonical_map in sorted(candidates.items()):
        for canonical, aliases in sorted(canonical_map.items()):
            canon_key = normalize_term(canonical)

            # Check if the candidate canonical collides with anything in existing
            hits = index.get(canon_key, [])
            if hits:
                removed_canonicals.append({
                    "candidate_canonical": canonical,
                    "candidate_group": group,
                    "candidate_aliases": aliases,
                    "reason": "canonical collides with existing term",
                    "existing_occurrences": [
                        {"group": g, "canonical": c, "kind": k}
                        for g, c, k in hits
                    ],
                })
                continue

            # Check each alias for collisions
            clean_aliases: List[str] = []
            for alias in aliases:
                alias_key = normalize_term(alias)
                alias_hits = index.get(alias_key, [])
                if alias_hits:
                    removed_aliases.append({
                        "candidate_canonical": canonical,
                        "candidate_group": group,
                        "candidate_alias": alias,
                        "reason": "alias collides with existing term",
                        "existing_occurrences": [
                            {"group": g, "canonical": c, "kind": k}
                            for g, c, k in alias_hits
                        ],
                    })
                else:
                    clean_aliases.append(alias)

            # Also check for internal duplicates within candidates themselves
            # (e.g. Power BI and PowerBI both in candidates)
            internal_hits = index.get(canon_key, [])
            if internal_hits:
                removed_canonicals.append({
                    "candidate_canonical": canonical,
                    "candidate_group": group,
                    "candidate_aliases": clean_aliases,
                    "reason": "canonical collides with earlier candidate",
                    "existing_occurrences": [
                        {"group": g, "canonical": c, "kind": k}
                        for g, c, k in internal_hits
                    ],
                })
                continue

            # Keep this candidate
            if group not in clean:
                clean[group] = {}
            clean[group][canonical] = clean_aliases

            # Add to index so later candidates can detect internal collisions
            index.setdefault(canon_key, []).append((group, canonical, "canonical"))
            for alias in clean_aliases:
                index.setdefault(normalize_term(alias), []).append((group, canonical, "alias"))

    # Count stats
    total_input_canonicals = sum(len(cm) for cm in candidates.values())
    total_clean_canonicals = sum(len(cm) for cm in clean.values())

    report = {
        "summary": {
            "input_canonicals": total_input_canonicals,
            "clean_canonicals": total_clean_canonicals,
            "removed_canonicals": len(removed_canonicals),
            "removed_aliases": len(removed_aliases),
        },
        "removed_canonicals": removed_canonicals,
        "removed_aliases": removed_aliases,
    }

    return clean, report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Check new candidates against canonical_data.json and remove collisions."
    )
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
        default="Input/NewCandidates/candidates_clean.json",
        help="Path to write collision-free candidates.",
    )
    p.add_argument(
        "--report",
        default="Output/collision_report.json",
        help="Path to write collision report.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    existing = load_store(args.existing)
    candidates = load_store(args.candidates)

    clean, report = check_collisions(existing, candidates)

    # Sort the clean candidates
    sorted_clean = sort_canonical_data(clean)

    write_json_file(args.output, sorted_clean)
    write_json_file(args.report, report)

    s = report["summary"]
    print(f"Existing store:    {args.existing}")
    print(f"Candidates input:  {args.candidates}")
    print(f"Clean output:      {args.output}")
    print(f"Report:            {args.report}")
    print(f"---")
    print(f"Input canonicals:    {s['input_canonicals']}")
    print(f"Clean canonicals:    {s['clean_canonicals']}")
    print(f"Removed canonicals:  {s['removed_canonicals']}")
    print(f"Removed aliases:     {s['removed_aliases']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

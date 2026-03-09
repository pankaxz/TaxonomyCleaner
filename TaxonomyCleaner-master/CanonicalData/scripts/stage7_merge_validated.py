"""Stage 7: Merge validated v2_preview entries into canonical_data.json.

Reads the v2_preview.json output from stage 6 and merges entries into the
canonical store, applying exclusion rules and collision detection.

Exclusion rules:
  - Entries with blocking findings (e.g. Sprint planning L4-001 timeout)
  - Entries with LOW confidence classification

Collision detection:
  - If a v2 canonical already exists in the store (as canonical or alias),
    it is logged as a collision and skipped (or aliases are merged).

Outputs:
  - Merged canonical_data.json (or custom --output path)
  - Overlap report showing already-existing entries
  - Merge report with new/skipped/merged counts

Usage:
  python scripts/stage7_merge_validated.py \\
      --v2-preview Output/candidate_validation/v2_preview.json \\
      --findings Output/candidate_validation/stage4_findings.json \\
      --existing Input/canonical_data.json \\
      --output Input/canonical_data_merged.json \\
      --report Output/candidate_validation/stage7_merge_report.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Set, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
sys.path.insert(0, PROJECT_ROOT)

from main import sort_canonical_data  # noqa: E402


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def normalize_term(term: str) -> str:
    return " ".join(term.lower().split()).strip()


def load_json(path: str) -> object:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, data: object, sort_keys: bool = False) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=sort_keys)
        f.write("\n")


def load_store(path: str) -> Dict[str, Dict[str, List[str]]]:
    payload = load_json(path)
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


# ---------------------------------------------------------------------------
# Build exclusion set from findings
# ---------------------------------------------------------------------------
def build_exclusion_set(
    findings_path: str,
    v2_entries: List[Dict[str, object]],
) -> Tuple[Set[str], List[Dict[str, str]]]:
    """Return (excluded_canonicals, exclusion_reasons)."""
    excluded: Set[str] = set()
    reasons: List[Dict[str, str]] = []

    # Blocking findings (L4-001 etc.)
    if findings_path and os.path.exists(findings_path):
        findings = load_json(findings_path)
        if isinstance(findings, list):
            for finding in findings:
                if not isinstance(finding, dict):
                    continue
                if finding.get("blocking", False):
                    location = str(finding.get("location", ""))
                    # location format: "canonical:Sprint planning"
                    if location.startswith("canonical:"):
                        term = location[len("canonical:"):]
                        excluded.add(term)
                        reasons.append({
                            "canonical": term,
                            "reason": f"blocking finding {finding.get('rule_id', '')}",
                            "detail": str(finding.get("reason", "")),
                        })

    # LOW confidence entries
    for entry in v2_entries:
        if entry.get("confidence") == "LOW":
            canon = entry.get("canonical", "")
            excluded.add(canon)
            reasons.append({
                "canonical": canon,
                "reason": "LOW confidence classification",
                "detail": f"status={entry.get('status', '')}",
            })

    return excluded, reasons


# ---------------------------------------------------------------------------
# Reverse index for collision detection
# ---------------------------------------------------------------------------
def build_reverse_index(
    store: Dict[str, Dict[str, List[str]]],
) -> Dict[str, List[Tuple[str, str]]]:
    """Map every normalized term (canonical or alias) -> list of (group, canonical)."""
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
# Overlap analysis
# ---------------------------------------------------------------------------
def find_overlaps(
    store: Dict[str, Dict[str, List[str]]],
    v2_entries: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    """Find v2 entries that already exist in the canonical store."""
    reverse = build_reverse_index(store)
    overlaps: List[Dict[str, object]] = []

    for entry in v2_entries:
        canon = entry.get("canonical", "")
        canon_key = normalize_term(canon)
        hits = reverse.get(canon_key, [])
        if hits:
            v2_aliases = entry.get("aliases", [])
            existing_aliases = []
            for g, c in hits:
                existing_aliases = store.get(g, {}).get(c, [])
            new_aliases = [a for a in v2_aliases if normalize_term(a) not in {normalize_term(ea) for ea in existing_aliases}]
            overlaps.append({
                "v2_canonical": canon,
                "v2_aliases": v2_aliases,
                "v2_tag": entry.get("tags", []),
                "v2_confidence": entry.get("confidence", ""),
                "existing_locations": [{"group": g, "canonical": c} for g, c in hits],
                "existing_aliases": existing_aliases,
                "new_aliases_to_merge": new_aliases,
            })

    return overlaps


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------
def merge_v2_into_store(
    store: Dict[str, Dict[str, List[str]]],
    v2_entries: List[Dict[str, object]],
    excluded: Set[str],
) -> Dict[str, object]:
    """Merge v2 entries into store (mutates store). Returns merge report."""
    reverse = build_reverse_index(store)

    new_groups: List[str] = []
    new_skills: List[Dict[str, str]] = []
    merged_aliases: List[Dict[str, object]] = []
    skipped_collisions: List[Dict[str, object]] = []
    skipped_excluded: List[str] = []

    for entry in v2_entries:
        canon = entry.get("canonical", "")
        aliases = entry.get("aliases", [])
        tags = entry.get("tags", [])
        group = tags[0] if tags else "Uncategorized"

        # Skip excluded entries
        if canon in excluded:
            skipped_excluded.append(canon)
            continue

        canon_key = normalize_term(canon)

        # Collision check: canonical already exists
        hits = reverse.get(canon_key, [])
        if hits:
            # Try to merge aliases into existing entry
            for existing_group, existing_canon in hits:
                existing_entry = store.get(existing_group, {})
                if existing_canon in existing_entry:
                    existing_alias_set = {normalize_term(a) for a in existing_entry[existing_canon]}
                    added = []
                    for alias in aliases:
                        if normalize_term(alias) not in existing_alias_set:
                            existing_entry[existing_canon].append(alias)
                            existing_alias_set.add(normalize_term(alias))
                            added.append(alias)
                    if added:
                        merged_aliases.append({
                            "group": existing_group,
                            "canonical": existing_canon,
                            "added_aliases": added,
                            "source_group": group,
                        })
                    else:
                        skipped_collisions.append({
                            "candidate_canonical": canon,
                            "candidate_group": group,
                            "existing_locations": [{"group": g, "canonical": c} for g, c in hits],
                            "reason": "already exists, no new aliases",
                        })
                    break
            # Update reverse index
            for alias in aliases:
                reverse.setdefault(normalize_term(alias), []).append((group, canon))
            continue

        # Check each alias for collisions
        clean_aliases: List[str] = []
        for alias in aliases:
            alias_key = normalize_term(alias)
            alias_hits = reverse.get(alias_key, [])
            if alias_hits:
                skipped_collisions.append({
                    "candidate_canonical": canon,
                    "candidate_alias": alias,
                    "candidate_group": group,
                    "existing_locations": [{"group": g, "canonical": c} for g, c in alias_hits],
                    "reason": "alias collides with existing entry",
                })
            else:
                clean_aliases.append(alias)

        # Add to store
        if group not in store:
            store[group] = {}
            new_groups.append(group)

        store[group][canon] = clean_aliases
        new_skills.append({"group": group, "canonical": canon})

        # Update reverse index
        reverse.setdefault(canon_key, []).append((group, canon))
        for alias in clean_aliases:
            reverse.setdefault(normalize_term(alias), []).append((group, canon))

    return {
        "summary": {
            "total_v2_entries": len(v2_entries),
            "excluded": len(skipped_excluded),
            "new_groups": len(new_groups),
            "new_skills": len(new_skills),
            "merged_aliases": len(merged_aliases),
            "skipped_collisions": len(skipped_collisions),
        },
        "excluded_entries": skipped_excluded,
        "new_groups": new_groups,
        "new_skills": new_skills,
        "merged_aliases": merged_aliases,
        "skipped_collisions": skipped_collisions,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage 7: Merge validated v2_preview into canonical_data.json"
    )
    p.add_argument(
        "--v2-preview",
        default="Output/candidate_validation/v2_preview.json",
        help="Path to v2_preview.json from stage 6.",
    )
    p.add_argument(
        "--findings",
        default="Output/candidate_validation/stage4_findings.json",
        help="Path to stage4_findings.json for blocking exclusions.",
    )
    p.add_argument(
        "--existing",
        default="Input/canonical_data.json",
        help="Path to existing canonical store.",
    )
    p.add_argument(
        "--output",
        default="Input/canonical_data_merged.json",
        help="Path to write merged output.",
    )
    p.add_argument(
        "--report",
        default="Output/candidate_validation/stage7_merge_report.json",
        help="Path to write merge report.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show overlap analysis and exclusions without merging.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    v2_entries = load_json(args.v2_preview)
    if not isinstance(v2_entries, list):
        print(f"ERROR: v2_preview is not a list: {type(v2_entries).__name__}")
        return 1

    store = load_store(args.existing)

    # Build exclusion set
    excluded, exclusion_reasons = build_exclusion_set(args.findings, v2_entries)

    # Show exclusions
    if excluded:
        print(f"\n--- Excluded entries ({len(excluded)}) ---")
        for reason in exclusion_reasons:
            print(f"  EXCLUDE: {reason['canonical']:40s} | {reason['reason']} | {reason['detail']}")

    # Find overlaps
    overlaps = find_overlaps(store, v2_entries)
    print(f"\n--- Overlap analysis ---")
    print(f"Total v2 entries:    {len(v2_entries)}")
    print(f"Already in store:    {len(overlaps)}")
    print(f"New entries:         {len(v2_entries) - len(overlaps) - len(excluded)}")
    print(f"Excluded:            {len(excluded)}")

    if overlaps:
        print(f"\n--- Overlapping entries ({len(overlaps)}) ---")
        for o in overlaps:
            locs = ", ".join(f"{loc['group']}/{loc['canonical']}" for loc in o["existing_locations"])
            new_note = f"  NEW ALIASES: {o['new_aliases_to_merge']}" if o["new_aliases_to_merge"] else ""
            print(f"  {o['v2_canonical']:40s} | exists at: {locs} | existing aliases: {o['existing_aliases']}{new_note}")

    if args.dry_run:
        print("\n--- Dry run: no files written ---")
        return 0

    # Merge
    report = merge_v2_into_store(store, v2_entries, excluded)

    # Sort and write
    sorted_store = sort_canonical_data(store)
    write_json(args.output, sorted_store)
    write_json(args.report, report, sort_keys=True)

    s = report["summary"]
    print(f"\n--- Merge results ---")
    print(f"Total v2 entries:    {s['total_v2_entries']}")
    print(f"Excluded:            {s['excluded']}")
    print(f"New groups:          {s['new_groups']}")
    print(f"New skills:          {s['new_skills']}")
    print(f"Merged aliases:      {s['merged_aliases']}")
    print(f"Skipped collisions:  {s['skipped_collisions']}")
    print(f"\nOutput: {args.output}")
    print(f"Report: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
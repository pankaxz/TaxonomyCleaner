from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Tuple


@dataclass
class Occurrence:
    kind: str  # canonical | alias
    group: str
    canonical: str
    alias_text: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve duplicate keywords by policy: "
            "remove duplicate aliases, and for duplicate canonicals keep one canonical."
        )
    )
    parser.add_argument(
        "--input",
        default="Input/canonical_data.final.json",
        help="Input canonical store path.",
    )
    parser.add_argument(
        "--output",
        default="Input/canonical_data.final.json",
        help="Output canonical store path. Can be same as input for in-place rewrite.",
    )
    parser.add_argument(
        "--report",
        default="Output/next/duplicate_resolution_report.json",
        help="Path to write resolution report JSON.",
    )
    return parser.parse_args()


def normalize_term(term: str) -> str:
    lowered = term.lower()
    collapsed = " ".join(lowered.split())
    return collapsed.strip()


def load_store(path: str) -> Dict[str, Dict[str, List[str]]]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError("Input JSON must be an object mapping groups to canonical maps.")

    store: Dict[str, Dict[str, List[str]]] = {}
    for group, canonical_map in payload.items():
        if not isinstance(group, str):
            continue
        if not isinstance(canonical_map, dict):
            continue

        group_map: Dict[str, List[str]] = {}
        for canonical, aliases in canonical_map.items():
            if not isinstance(canonical, str):
                continue

            alias_list: List[str] = []
            if isinstance(aliases, list):
                for alias in aliases:
                    if isinstance(alias, str):
                        alias_list.append(alias)

            group_map[canonical] = alias_list
        store[group] = group_map

    return store


def build_occurrence_index(
    store: Dict[str, Dict[str, List[str]]],
) -> Dict[str, List[Occurrence]]:
    index: Dict[str, List[Occurrence]] = {}

    for group, canonical_map in store.items():
        for canonical, aliases in canonical_map.items():
            canonical_key = normalize_term(canonical)
            if canonical_key not in index:
                index[canonical_key] = []
            index[canonical_key].append(
                Occurrence(
                    kind="canonical",
                    group=group,
                    canonical=canonical,
                    alias_text=None,
                )
            )

            for alias in aliases:
                alias_key = normalize_term(alias)
                if alias_key not in index:
                    index[alias_key] = []
                index[alias_key].append(
                    Occurrence(
                        kind="alias",
                        group=group,
                        canonical=canonical,
                        alias_text=alias,
                    )
                )

    return index


def select_canonical_to_keep(
    canonical_occurrences: List[Occurrence],
    store: Dict[str, Dict[str, List[str]]],
) -> Occurrence:
    ranked: List[Tuple[int, str, str, Occurrence]] = []

    for occurrence in canonical_occurrences:
        alias_count = 0
        group_map = store.get(occurrence.group, {})
        aliases = group_map.get(occurrence.canonical, [])
        if isinstance(aliases, list):
            alias_count = len(aliases)

        ranked.append(
            (
                -alias_count,  # more aliases first
                occurrence.group.lower(),
                occurrence.canonical.lower(),
                occurrence,
            )
        )

    ranked.sort(key=lambda item: (item[0], item[1], item[2]))
    winner = ranked[0][3]
    return winner


def apply_policy(
    store: Dict[str, Dict[str, List[str]]],
) -> Dict[str, object]:
    index = build_occurrence_index(store)

    removed_aliases: List[Dict[str, str]] = []
    removed_canonicals: List[Dict[str, object]] = []

    duplicate_terms = []
    for normalized_term, occurrences in index.items():
        if len(occurrences) <= 1:
            continue
        duplicate_terms.append((normalized_term, occurrences))

    duplicate_terms.sort(key=lambda item: item[0])

    for normalized_term, occurrences in duplicate_terms:
        alias_occurrences: List[Occurrence] = []
        canonical_occurrences: List[Occurrence] = []

        for occurrence in occurrences:
            if occurrence.kind == "alias":
                alias_occurrences.append(occurrence)
            elif occurrence.kind == "canonical":
                canonical_occurrences.append(occurrence)

        if alias_occurrences:
            for occurrence in alias_occurrences:
                group_map = store.get(occurrence.group)
                if group_map is None:
                    continue

                aliases = group_map.get(occurrence.canonical)
                if not isinstance(aliases, list):
                    continue

                kept_aliases: List[str] = []
                removed_here: List[str] = []
                for alias in aliases:
                    if normalize_term(alias) == normalized_term:
                        removed_here.append(alias)
                    else:
                        kept_aliases.append(alias)

                if removed_here:
                    group_map[occurrence.canonical] = kept_aliases
                    for alias_text in removed_here:
                        removed_aliases.append(
                            {
                                "normalized_term": normalized_term,
                                "group": occurrence.group,
                                "canonical": occurrence.canonical,
                                "alias": alias_text,
                            }
                        )

        if len(canonical_occurrences) > 1:
            winner = select_canonical_to_keep(canonical_occurrences, store)

            for occurrence in canonical_occurrences:
                if occurrence.group == winner.group and occurrence.canonical == winner.canonical:
                    continue

                group_map = store.get(occurrence.group)
                if group_map is None:
                    continue
                if occurrence.canonical not in group_map:
                    continue

                alias_count = len(group_map.get(occurrence.canonical, []))
                removed_canonicals.append(
                    {
                        "normalized_term": normalized_term,
                        "removed_group": occurrence.group,
                        "removed_canonical": occurrence.canonical,
                        "removed_alias_count": alias_count,
                        "kept_group": winner.group,
                        "kept_canonical": winner.canonical,
                    }
                )
                del group_map[occurrence.canonical]

    report: Dict[str, object] = {
        "summary": {
            "removed_aliases": len(removed_aliases),
            "removed_canonicals": len(removed_canonicals),
        },
        "removed_aliases": removed_aliases,
        "removed_canonicals": removed_canonicals,
    }
    return report


def write_store(path: str, store: Dict[str, Dict[str, List[str]]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(store, handle, indent=2, sort_keys=True)
        handle.write("\n")


def write_report(path: str, report: Dict[str, object]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> int:
    args = parse_args()
    store = load_store(args.input)
    report = apply_policy(store)
    write_store(args.output, store)
    write_report(args.report, report)

    summary = report.get("summary", {})
    removed_aliases = summary.get("removed_aliases", 0)
    removed_canonicals = summary.get("removed_canonicals", 0)

    print(f"Input: {args.input}")
    print(f"Output: {args.output}")
    print(f"Report: {args.report}")
    print(f"Removed aliases: {removed_aliases}")
    print(f"Removed canonicals: {removed_canonicals}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

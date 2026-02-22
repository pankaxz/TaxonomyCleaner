from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Tuple


DEFAULT_INPUT_PATH = "Input/canonical_data.final.json"


@dataclass
class DuplicateSummary:
    total_keywords: int
    unique_keywords: int
    duplicate_occurrences: int
    duplicate_unique_terms: int
    duplicate_terms: List[Tuple[str, int]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report keyword duplicates in canonical store (canonicals + aliases)."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_PATH,
        help="Path to canonical store JSON (default: Input/canonical_data.final.json).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Number of duplicate terms to print (default: 50).",
    )
    return parser.parse_args()


def load_store(path: str) -> Dict[str, Dict[str, List[str]]]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError("Input JSON must be an object mapping groups to canonical maps.")

    validated: Dict[str, Dict[str, List[str]]] = {}
    for group, canonical_map in payload.items():
        if not isinstance(group, str):
            continue
        if not isinstance(canonical_map, dict):
            continue

        group_bucket: Dict[str, List[str]] = {}
        for canonical, aliases in canonical_map.items():
            if not isinstance(canonical, str):
                continue

            alias_list: List[str] = []
            if isinstance(aliases, list):
                for alias in aliases:
                    if isinstance(alias, str):
                        alias_list.append(alias)

            group_bucket[canonical] = alias_list

        validated[group] = group_bucket

    return validated


def normalize_term(term: str) -> str:
    lowered = term.lower()
    collapsed = " ".join(lowered.split())
    return collapsed.strip()


def collect_keywords(store: Dict[str, Dict[str, List[str]]]) -> List[str]:
    keywords: List[str] = []

    for canonical_map in store.values():
        for canonical, aliases in canonical_map.items():
            keywords.append(canonical)
            for alias in aliases:
                keywords.append(alias)

    return keywords


def summarize_duplicates(keywords: List[str]) -> DuplicateSummary:
    counter = Counter(keywords)

    duplicate_terms: List[Tuple[str, int]] = []
    duplicate_occurrences = 0
    for term, count in counter.items():
        if count <= 1:
            continue
        duplicate_terms.append((term, count))
        duplicate_occurrences += count - 1

    duplicate_terms.sort(key=lambda item: (-item[1], item[0].lower(), item[0]))

    summary = DuplicateSummary(
        total_keywords=len(keywords),
        unique_keywords=len(counter),
        duplicate_occurrences=duplicate_occurrences,
        duplicate_unique_terms=len(duplicate_terms),
        duplicate_terms=duplicate_terms,
    )
    return summary


def print_summary(label: str, summary: DuplicateSummary, top: int) -> None:
    print(f"\n[{label}]")
    print(f"Total keywords: {summary.total_keywords}")
    print(f"Unique keywords: {summary.unique_keywords}")
    print(f"Duplicate occurrences: {summary.duplicate_occurrences}")
    print(f"Duplicate unique terms: {summary.duplicate_unique_terms}")

    if summary.duplicate_unique_terms == 0:
        print("No duplicate terms found.")
        return

    limit = max(0, int(top))
    if limit == 0:
        return

    print(f"Top {min(limit, summary.duplicate_unique_terms)} duplicate terms:")
    printed = 0
    for term, count in summary.duplicate_terms:
        print(f"- {term} ({count})")
        printed += 1
        if printed >= limit:
            break


def main() -> int:
    args = parse_args()
    store = load_store(args.input)

    exact_keywords = collect_keywords(store)
    exact_summary = summarize_duplicates(exact_keywords)

    normalized_keywords: List[str] = []
    for keyword in exact_keywords:
        normalized_keywords.append(normalize_term(keyword))
    normalized_summary = summarize_duplicates(normalized_keywords)

    print(f"Input file: {args.input}")
    print_summary("Exact Match", exact_summary, args.top)
    print_summary("Normalized Match", normalized_summary, args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
Stage 0 Rewrite Logic
=====================

This module encapsulates the core business logic for structural rewrites of canonical terms.
It is responsible for identifying "Atomicity Violations" (where a single canonical term actually represents multiple concepts)
and proposing deterministic, safe rewrites to split or clean them.

Key Concepts:
- Atomicity: A canonical term should represent exactly one concept. "Java/Kotlin" violates this.
- Determinism: Rewrites must be predictable and repeatable. We only rewrite if we are 100% sure of the intent.
- Safety: We do not split if the resulting terms look invalid (e.g., too short, still contain special chars).
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any
from typing import Dict
from typing import List
from typing import Set

from ...shared.utilities import contains_atomicity_violation
from ...shared.utilities import find_parenthetical_split
from ...shared.utilities import normalize_term

# Regex for splitting on " and ", case-insensitive.
# Note boundaries \b to avoid splitting "Command" into "Comm" and "".
AND_SPLIT_PATTERN = re.compile(r"\band\b", re.IGNORECASE)

# Reasons that prevent a proposed split from being considered "safe".
# If a split results in a term that still has these issues, we abort the split
# and force manual review instead.
SAFE_SPLIT_BLOCKING_REASONS = {
    "slash",
    "comma",
    "and",
    "parentheses",
}


@dataclass(frozen=True)
class AtomicityRewriteDecision:
    """
    Represents the decision made by the atomicity logic for a specific canonical term.

    Attributes:
        proposed_action (str): The ID of the action to take (e.g., "split_on_slash", "remove_parentheses").
        proposed_payload (Dict): Data needed to execute the action (e.g., the list of new terms).
        has_deterministic_rewrite (bool): True if the logic is confident enough to automate the change.
                                          False if human review is needed.
    """
    proposed_action: str
    proposed_payload: Dict[str, Any]
    has_deterministic_rewrite: bool


@dataclass(frozen=True)
class RewrittenCanonicalEntry:
    """
    Represents a single new entry (Canonical + Aliases) to be added to the taxonomy
    after a rewrite operation.

    A single original canonical might explode into multiple RewrittenCanonicalEntry objects
    (e.g., "A/B" -> Entry(A), Entry(B)).
    """
    canonical: str
    aliases: List[str]


def derive_atomicity_rewrite_decision(
    keyword: str,
    violations: List[str],
) -> AtomicityRewriteDecision:
    """
    Analyzes a canonical term and its violations to decide on a rewrite strategy.

    This function is the "Brain" of the chemical split. It looks at *why* a term failed validation
    and checks if there is a safe, deterministic way to fix it.

    Strategies (in order of precedence):
    1. Parentheses: "TensorFlow (TF)" -> Canonical: "TensorFlow", Alias: "TF".
    2. Slashes: "Java/Kotlin" -> Canonical: "Java", Canonical: "Kotlin".
    3. And: "C++ and C#" -> Canonical: "C++", Canonical: "C#".
    4. Commas: "React, Vue" -> Canonical: "React", Canonical: "Vue".

    Args:
        canonical (str): The raw canonical string.
        violations (List[str]): The list of rule ids/reasons why it failed initial validation.

    Returns:
        AtomicityRewriteDecision: The proposed fix (or a proposal to do nothing but flag it).
    """
    unique_violations: Set[str] = set(violations)

    # Strategy 1: Parentheses Extraction
    # Example: "Amazon Web Services (AWS)"
    # We strip the parentheses part, make the base term the new canonical, and add the content of parens as an alias.
    if "parentheses" in unique_violations:
        parenthetical_split = find_parenthetical_split(keyword)
        if parenthetical_split is not None:
            base_canonical = parenthetical_split[0]
            alias = parenthetical_split[1]
            payload = {
                "canonical": base_canonical,
                "add_alias": alias,
            }
            return AtomicityRewriteDecision(
                proposed_action="remove_parentheses",
                proposed_payload=payload,
                has_deterministic_rewrite=True,
            )

    # Strategy 2: Split on Slash "/"
    # Example: "OS/2" or "Java/Kotlin"
    # We try to split. If the parts look "safe" (valid terms), we approve the split.
    if "slash" in unique_violations:
        slash_candidates = _split_on_delimiter(keyword, "/")
        if _is_safe_split_candidate_list(slash_candidates):
            payload = {
                "split_candidates": slash_candidates,
                "split_delimiter": "/",
            }
            return AtomicityRewriteDecision(
                proposed_action="split_on_slash",
                proposed_payload=payload,
                has_deterministic_rewrite=True,
            )

    # Strategy 3: Split on "and"
    if "and" in unique_violations:
        and_candidates = _split_on_and(keyword)
        if _is_safe_split_candidate_list(and_candidates):
            payload = {
                "split_candidates": and_candidates,
                "split_delimiter": "and",
            }
            return AtomicityRewriteDecision(
                proposed_action="split_on_and",
                proposed_payload=payload,
                has_deterministic_rewrite=True,
            )

    # Strategy 4: Split on Comma
    if "comma" in unique_violations:
        comma_candidates = _split_on_delimiter(keyword, ",")
        if _is_safe_split_candidate_list(comma_candidates):
            payload = {
                "split_candidates": comma_candidates,
                "split_delimiter": ",",
            }
            return AtomicityRewriteDecision(
                proposed_action="split_on_comma",
                proposed_payload=payload,
                has_deterministic_rewrite=True,
            )

    # Fallback: We know it's broken, but we don't know how to fix it safely.
    # Flag for manual human review (L1-010).
    fallback_payload = {
        "violation_types": list(violations),
    }
    return AtomicityRewriteDecision(
        proposed_action="needs_exception",
        proposed_payload=fallback_payload,
        has_deterministic_rewrite=False,
    )


def build_rewritten_entries(
    original_canonical: str,
    original_aliases: List[str],
    rewrite_decision: AtomicityRewriteDecision | None,
) -> List[RewrittenCanonicalEntry]:
    """
    Executes the rewrite decision, transforming one canonical into 1..N new entries.

    This function also handles "Alias Attribution" - figuring out which of the original aliases
    should belong to which new canonical.

    Args:
        original_canonical (str): The starting canonical term.
        original_aliases (List[str]): The starting list of aliases.
        rewrite_decision (AtomicityRewriteDecision): The plan generated by `derive_atomicity_rewrite_decision`.

    Returns:
        List[RewrittenCanonicalEntry]: The list of new (Canonical, AliasList) objects.
    """
    # Case 0: No decision or no deterministic fix. Return as-is (pass-through).
    if rewrite_decision is None:
        return [
            RewrittenCanonicalEntry(
                canonical=original_canonical,
                aliases=_sanitize_aliases_for_canonical(original_aliases, original_canonical),
            )
        ]

    if not rewrite_decision.has_deterministic_rewrite:
        return [
            RewrittenCanonicalEntry(
                canonical=original_canonical,
                aliases=_sanitize_aliases_for_canonical(original_aliases, original_canonical),
            )
        ]

    # Case 1: Parentheses Removal
    # We keep all original aliases, plus add the extracted parenthetical content as a new alias.
    if rewrite_decision.proposed_action == "remove_parentheses":
        target_canonical = str(rewrite_decision.proposed_payload.get("canonical", "")).strip()
        parenthetical_alias = str(rewrite_decision.proposed_payload.get("add_alias", "")).strip()
        
        # Safety check: if extraction failed to produce a base term, abort.
        if not target_canonical:
            return [
                RewrittenCanonicalEntry(
                    canonical=original_canonical,
                    aliases=_sanitize_aliases_for_canonical(original_aliases, original_canonical),
                )
            ]

        aliases_with_parenthetical: List[str] = list(original_aliases)
        if parenthetical_alias:
            aliases_with_parenthetical.append(parenthetical_alias)

        return [
            RewrittenCanonicalEntry(
                canonical=target_canonical,
                aliases=_sanitize_aliases_for_canonical(aliases_with_parenthetical, target_canonical),
            )
        ]

    # Case 2: Splitting (Slash, And, Comma)
    # We split the canonical into parts.
    # The original aliases are ONLY assigned to the FIRST part.
    # Why? Because usually "A/B" means "A" and "B", and the aliases for "A/B" are often synonyms for the aggregate
    # or just "A". Assigning them to B is risky.
    # This is a conservative heuristic; we prefer losing aliases to assigning incorrect ones.
    if rewrite_decision.proposed_action in {"split_on_and", "split_on_slash", "split_on_comma"}:
        raw_candidates = rewrite_decision.proposed_payload.get("split_candidates", [])
        if not isinstance(raw_candidates, list) or not raw_candidates:
            return [
                RewrittenCanonicalEntry(
                    canonical=original_canonical,
                    aliases=_sanitize_aliases_for_canonical(original_aliases, original_canonical),
                )
            ]

        candidates: List[str] = []
        for candidate in raw_candidates:
            candidate_text = str(candidate).strip()
            if candidate_text:
                candidates.append(candidate_text)

        if len(candidates) < 2:
            return [
                RewrittenCanonicalEntry(
                    canonical=original_canonical,
                    aliases=_sanitize_aliases_for_canonical(original_aliases, original_canonical),
                )
            ]

        rewritten_entries: List[RewrittenCanonicalEntry] = []
        for index, candidate in enumerate(candidates):
            candidate_aliases: List[str] = []
            
            # Heuristic: Only inherit aliases for the first term in the split.
            # Example: "Winforms/WPF" with alias "Windows UI".
            # Split: "Winforms", "WPF".
            # "Windows UI" attaches to "Winforms". It's not perfect, but safer than attaching to both.
            if index == 0:
                candidate_aliases = _sanitize_aliases_for_canonical(original_aliases, candidate)
            
            rewritten_entries.append(
                RewrittenCanonicalEntry(
                    canonical=candidate,
                    aliases=candidate_aliases,
                )
            )

        return rewritten_entries

    # Default fallback
    return [
        RewrittenCanonicalEntry(
            canonical=original_canonical,
            aliases=_sanitize_aliases_for_canonical(original_aliases, original_canonical),
        )
    ]


def _split_on_delimiter(value: str, delimiter: str) -> List[str]:
    """Helper to split string by delimiter and strip whitespace."""
    raw_parts = value.split(delimiter)
    cleaned_parts: List[str] = []

    for raw_part in raw_parts:
        candidate = raw_part.strip()
        if not candidate:
            continue
        cleaned_parts.append(candidate)

    return cleaned_parts


def _split_on_and(value: str) -> List[str]:
    """Helper to split string by ' and ' (case-insensitive)."""
    raw_parts = AND_SPLIT_PATTERN.split(value)
    cleaned_parts: List[str] = []

    for raw_part in raw_parts:
        candidate = raw_part.strip()
        if not candidate:
            continue
        cleaned_parts.append(candidate)

    return cleaned_parts


def _is_safe_split_candidate_list(candidates: List[str]) -> bool:
    """
    Validates if a list of split candidates is 'safe' to apply.

    Safe means:
    1. At least 2 parts (otherwise why split?).
    2. No part is too short (< 2 chars).
    3. No part repeats (A/A).
    4. No part contains further atomicity violations (recursion is hard, so we just block it).
    """
    if len(candidates) < 2:
        return False

    seen_normalized: Set[str] = set()

    for candidate in candidates:
        if len(candidate.strip()) < 2:
            return False

        normalized_candidate = normalize_term(candidate)
        if normalized_candidate in seen_normalized:
            return False
        seen_normalized.add(normalized_candidate)

        # Recursive check: if the split result itself has a slash/comma/etc,
        # it's too complex for this stage. Abort and ask for manual review.
        candidate_violations = contains_atomicity_violation(candidate)
        for reason in candidate_violations:
            if reason in SAFE_SPLIT_BLOCKING_REASONS:
                return False

    return True


def _sanitize_aliases_for_canonical(aliases: List[str], canonical: str) -> List[str]:
    """
    Cleans up a list of aliases for a specific canonical.

    1. Removes aliases that match the canonical (redundant).
    2. Dedupes based on normalized values.
    3. Applies text cleaning (removing parens like '(canonical)').
    """
    canonical_normalized = _alias_equivalence_key(canonical)
    seen_aliases: Set[str] = set()
    atomic_aliases: List[str] = []

    for alias in aliases:
        expanded_aliases = _expand_alias_to_atomic_units(alias, canonical)
        for expanded_alias in expanded_aliases:
            normalized_alias = _alias_equivalence_key(expanded_alias)
            if not normalized_alias:
                continue
            if normalized_alias == canonical_normalized:
                continue
            if normalized_alias in seen_aliases:
                continue

            seen_aliases.add(normalized_alias)
            atomic_aliases.append(expanded_alias)

    sorted_aliases = sorted(
        atomic_aliases,
        key=lambda value: normalize_term(str(value)),
    )
    return sorted_aliases


def _canonicalize_alias_text(alias_text: str, canonical: str) -> str:
    """
    Aggressive text cleaner for aliases.

    - Replaces underscores/dashes with spaces.
    - Removes trailing references to the canonical itself (e.g. "TF (TensorFlow)" -> "TF").
    - Removes acronym definitions if they match the alias (e.g. "AWS (Amazon Web Services)" -> "Amazon Web Services").
    """
    cleaned = str(alias_text).strip()
    if not cleaned:
        return ""

    # Normalize separators so equivalent forms dedupe deterministically.
    cleaned = cleaned.replace("-", " ")
    cleaned = cleaned.replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    canonical_text = str(canonical).strip()
    if canonical_text:
        # Only strip trailing canonical tokens when canonical is acronym-like.
        # This avoids destructive rewrites like "docker dive" -> "docker".
        if _is_acronym_like(canonical_text):
            cleaned = _strip_redundant_acronym_suffix(cleaned, canonical_text)

    # Remove redundant acronym parenthetical if the parenthetical content is
    # exactly the acronym of the base phrase.
    parenthetical_split = find_parenthetical_split(cleaned)
    if parenthetical_split is not None:
        base_text, parenthetical_text = parenthetical_split
        base_acronym = _acronym(base_text)
        if base_acronym and normalize_term(parenthetical_text) == base_acronym:
            cleaned = base_text.strip()

    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _alias_equivalence_key(value: str) -> str:
    """Generates a key for loose string comparison (deduping)."""
    normalized = normalize_term(value)
    normalized = normalized.replace("-", " ")
    normalized = normalized.replace("_", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _acronym(value: str) -> str:
    """Extracts initials from a string to form an acronym (e.g. "Amazon Web Services" -> "aws")."""
    tokens = re.findall(r"[a-zA-Z0-9]+", str(value))
    if not tokens:
        return ""

    letters: List[str] = []
    for token in tokens:
        if not token:
            continue
        letters.append(token[0].lower())
    return "".join(letters)


def _is_acronym_like(value: str) -> bool:
    token = str(value).strip()
    if len(token) < 2:
        return False
    if len(token) > 12:
        return False

    alpha_chars: List[str] = []
    for char in token:
        if char.isalpha():
            alpha_chars.append(char)
            continue
        if char.isdigit():
            continue
        return False

    if len(alpha_chars) < 2:
        return False

    for char in alpha_chars:
        if not char.isupper():
            return False
    return True


def _strip_redundant_acronym_suffix(value: str, canonical_text: str) -> str:
    canonical_key = normalize_term(canonical_text)
    current = str(value).strip()
    if not current:
        return current

    parenthetical_pattern = re.compile(
        rf"\s*\(\s*{re.escape(canonical_text)}\s*\)\s*$",
        re.IGNORECASE,
    )
    trailing_token_pattern = re.compile(
        rf"\s+{re.escape(canonical_text)}\s*$",
        re.IGNORECASE,
    )

    while True:
        updated = current

        if parenthetical_pattern.search(updated):
            candidate = parenthetical_pattern.sub("", updated).strip()
            candidate_acronym = _acronym(candidate)
            if candidate and candidate_acronym == canonical_key:
                updated = candidate

        if updated == current and trailing_token_pattern.search(updated):
            candidate = trailing_token_pattern.sub("", updated).strip()
            candidate_acronym = _acronym(candidate)
            if candidate and candidate_acronym == canonical_key:
                updated = candidate

        if updated == current:
            break
        current = updated

    return current


def _extract_safe_alias_split_candidates(value: str) -> List[str]:
    violations = contains_atomicity_violation(value)
    candidates: List[str] = []

    if "slash" in violations:
        candidates = _split_on_delimiter(value, "/")
    elif "and" in violations:
        candidates = _split_on_and(value)
    elif "comma" in violations:
        candidates = _split_on_delimiter(value, ",")
    else:
        return []

    if not _is_safe_split_candidate_list(candidates):
        return []

    return candidates


def _expand_alias_to_atomic_units(alias: str, canonical: str) -> List[str]:
    """
    Recursively expands an alias until no safe split is possible.

    Example:
        "React/Angular" -> ["React", "Angular"]
    """
    seed = _canonicalize_alias_text(str(alias), canonical)
    if not seed:
        return []

    queue: List[str] = [seed]
    seen_terms: Set[str] = set()
    expanded: List[str] = []

    while queue:
        current = queue.pop(0).strip()
        if not current:
            continue

        normalized_current = _alias_equivalence_key(current)
        if normalized_current in seen_terms:
            continue
        seen_terms.add(normalized_current)

        split_candidates = _extract_safe_alias_split_candidates(current)
        if not split_candidates:
            expanded.append(current)
            continue

        for candidate in split_candidates:
            normalized_candidate = _canonicalize_alias_text(candidate, canonical)
            if not normalized_candidate:
                continue
            queue.append(normalized_candidate)

    return expanded

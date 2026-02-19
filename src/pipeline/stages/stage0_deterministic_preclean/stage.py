"""
Stage 0: Deterministic Pre-clean
================================

This is the entry point for the "Stage 0" cleaning pipeline.
Its goal is to perform "safe", deterministic cleanups on the raw taxonomy data BEFORE
any complex or probabilistic logic (like LLMs) touches it.

Responsibilities:
1.  Schema Validation: Ensure the JSON structure is exactly Group -> Canonical -> [Aliases].
2.  Alias Hygiene: Dedup, remove exact duplicates, remove "blocked" aliases (e.g. numpy != numba).
3.  Atomicity Enforcements: Split "merged" canonicals like "Java/Kotlin" into separate entries.
4.  Reporting: Generate a detailed ValidationReport of everything that was changed or flagged.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple

from ...shared.findings import create_finding
from ...shared.models import Finding, StageResult, ValidationReport
from ...shared.utilities import contains_atomicity_violation
from ...shared.utilities import contains_version_token
from ...shared.utilities import normalize_term
from .rewrite_logic import AtomicityRewriteDecision
from .rewrite_logic import RewrittenCanonicalEntry
from .rewrite_logic import build_rewritten_entries
from .rewrite_logic import derive_atomicity_rewrite_decision
from .rules import is_hard_blocked_alias


def validate_schema(store: Any) -> Tuple[List[Finding], bool]:
    """
    Validates that the input data strictly matches the expected taxonomy structure.
    
    Expected Structure:
    {
        "Group Name": {
            "Canonical Name": ["Alias 1", "Alias 2", ...]
        }
    }

    Args:
        store (Any): The loaded JSON data (types yet to be verified).

    Returns:
        Tuple[List[Finding], bool]: A list of schema errors found, and a boolean flag
                                    indicating if a fatal schema error exists (True = fatal).
    """
    findings: List[Finding] = []

    if not isinstance(store, dict):
        findings.append(
            create_finding(
                rule_id="L1-001",
                blocking=True,
                location="root",
                observed_value=str(type(store).__name__),
                normalized_value="",
                proposed_action="manual_review",
                reason="Root JSON must be an object mapping group -> canonical mapping.",
            )
        )
        return findings, True

    has_schema_error = False

    for group, group_value in store.items():
        group_location = f"group:{group}"
        if not isinstance(group, str):
            has_schema_error = True
            findings.append(
                create_finding(
                    rule_id="L1-001",
                    blocking=True,
                    location=group_location,
                    observed_value=repr(group),
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Group keys must be strings.",
                )
            )
            continue

        if not isinstance(group_value, dict):
            has_schema_error = True
            findings.append(
                create_finding(
                    rule_id="L1-001",
                    blocking=True,
                    location=group_location,
                    observed_value=str(type(group_value).__name__),
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Group value must be an object mapping canonical -> aliases.",
                )
            )
            continue

        for canonical, aliases in group_value.items():
            canonical_location = f"group:{group}.canonical:{canonical}"

            if not isinstance(canonical, str):
                has_schema_error = True
                findings.append(
                    create_finding(
                        rule_id="L1-001",
                        blocking=True,
                        location=canonical_location,
                        observed_value=repr(canonical),
                        normalized_value="",
                        proposed_action="manual_review",
                        reason="Canonical keys must be strings.",
                    )
                )
                continue

            if not isinstance(aliases, list):
                has_schema_error = True
                findings.append(
                    create_finding(
                        rule_id="L1-001",
                        blocking=True,
                        location=canonical_location,
                        observed_value=str(type(aliases).__name__),
                        normalized_value="",
                        proposed_action="manual_review",
                        reason="Aliases must be string arrays.",
                    )
                )
                continue

            for alias_index, alias in enumerate(aliases):
                alias_location = f"{canonical_location}.aliases[{alias_index}]"

                if not isinstance(alias, str):
                    has_schema_error = True
                    findings.append(
                        create_finding(
                            rule_id="L1-001",
                            blocking=True,
                            location=alias_location,
                            observed_value=repr(alias),
                            normalized_value="",
                            proposed_action="manual_review",
                            reason="Alias entries must be strings.",
                        )
                    )
                    continue

                #If alias is " ", " \n ", or "", strip() turns it into an empty string ("").
                if alias.strip():
                    continue

                has_schema_error = True
                findings.append(
                    create_finding(
                        rule_id="L1-001",
                        blocking=True,
                        location=alias_location,
                        observed_value=repr(alias),
                        normalized_value="",
                        proposed_action="remove_alias",
                        reason="Blank aliases are not allowed.",
                    )
                )

    return findings, has_schema_error


def run_stage0(
    store: Dict[str, Dict[str, List[str]]],
    source_path: str,
    source_hash: str,
    atomicity_exceptions: Set[str],
) -> StageResult:
    """
    Executes the main Stage 0 pipeline.

    Flow:
    1.  Validate Schema (abort if invalid).
    2.  Iterate through every Group/Canonical/Alias.
    3.  Clean Aliases (dedup, check blocklists).
    4.  Check Canonical Atomicity (split "A/B" -> "A", "B").
    5.  Build "Rewritten Store" (the new, clean taxonomy).
    6.  Re-validate the Rewritten Store to check for introduced collisions.
    7.  Compile findings and reports.

    Args:
        store: The raw input taxonomy.
        source_path: Metadata for the report.
        source_hash: Metadata for the report.
        atomicity_exceptions: A set of canonicals allowed to violate atomicity rules (whitelisted).

    Returns:
        StageResult: encapsulated result containing the cleaned data and validation reports.
    """
    result = StageResult()

    schema_findings, has_schema_error = validate_schema(store)
    for finding in schema_findings:
        result.add_finding(finding)

    if has_schema_error:
        validation_report = ValidationReport(
            source_path=source_path,
            source_hash=source_hash,
            summary=_summarize_findings(result.findings),
            findings=result.findings,
        )
        result.payload["validation_report"] = validation_report.to_dict()
        result.payload["cleaned_store"] = store
        result.payload["rewritten_store"] = store
        result.payload["rewrite_plan"] = []
        result.payload["rewritten_validation_report"] = _build_rewritten_store_validation_report(
            rewritten_store=store,
            source_path=source_path,
            source_hash=source_hash,
        )
        result.payload["original_canonical_rows"] = []
        result.payload["canonical_rows"] = []
        return result

    # Tracks where every Canonical is defined: "normalized_canonical" -> [(Group, OriginalCanonical), ...]
    # Used to detect if the same canonical (e.g. "Java") is defined in multiple groups (e.g. "Backend" and "Languages").
    canonical_locations: Dict[str, List[Tuple[str, str]]] = defaultdict(list)

    # Tracks where every Alias appears: "normalized_alias" -> [(Group, Canonical, OriginalAlias), ...]
    # Used to detect if the same alias (e.g. "react") is used for different concepts.
    # Basically an "Inverse Index" or "Reverse Lookup" for aliases.
    alias_locations: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)

    # Tracks which Canonicals an Alias points to: "normalized_alias" -> {"Canonical1", "Canonical2"}
    # Used to detect Ambiguity: If size > 1, the alias is ambiguous (points to different concepts).
    # e.g. "react" -> {"React", "Reaction"} would be an error.
    alias_targets: Dict[str, Set[str]] = defaultdict(set)
    group_names: Dict[str, str] = {}

    cleaned_store: Dict[str, Dict[str, List[str]]] = {}
    rewritten_store: Dict[str, Dict[str, List[str]]] = {}
    rewrite_plan: List[Dict[str, Any]] = []

    sorted_groups = sorted(store)

    # ---------------------------------------------------------
    # Main Processing Loop
    # Iterate through the hierarchy to build a Cleaned and Rewritten version.
    # ---------------------------------------------------------
    for group in sorted_groups:
        normalized_group = normalize_term(group)
        group_names[normalized_group] = group
        cleaned_store[group] = {}
        rewritten_store[group] = {}

        sorted_canonicals = sorted(store[group])

        for canonical in sorted_canonicals:
            aliases = store[group][canonical]
            normalized_canonical = normalize_term(canonical)

            if normalized_canonical == normalized_group:
                result.add_finding(
                    create_finding(
                        rule_id="L1-012",
                        blocking=False,
                        location=f"group:{group}.canonical:{canonical}",
                        observed_value=canonical,
                        normalized_value=normalized_canonical,
                        proposed_action="remove_canonical",
                        reason=(
                            "Canonical name matches group name and is removed "
                            "deterministically from rewritten store."
                        ),
                    )
                )

                rewrite_plan.append(
                    {
                        "group": group,
                        "source_canonical": canonical,
                        "rewrite_applied": True,
                        "proposed_action": "remove_canonical",
                        "target_canonicals": [],
                        "proposed_payload": {
                            "reason": "canonical_matches_group_name",
                        },
                        "removed_aliases": [],
                    }
                )
                continue

            canonical_locations[normalized_canonical].append((group, canonical))

            deduped_aliases: List[str] = []
            rewritten_aliases: List[str] = []
            removed_aliases_for_rewrite: List[str] = []
            alias_seen: Set[str] = set()
            removed_alias_seen: Set[str] = set()

            # -------------------------------------------------------------------------
            # ALIAS HYGIENE LOOP
            # Iterates through every alias to apply standard cleaning rules:
            # 1. Deduplication (case-insensitive).
            # 2. Hard Block checks (e.g. is this alias explicitly forbidden?).
            # 3. Version checks (is this alias actually a version number?).
            # 4. Preparation for rewriting (if valid, it goes into `rewritten_aliases`).
            # -------------------------------------------------------------------------
            for alias in aliases:
                normalized_alias = normalize_term(alias)
                alias_locations[normalized_alias].append((group, canonical, alias))
                alias_targets[normalized_alias].add(normalized_canonical)

                if normalized_alias in alias_seen:
                    result.add_finding(
                        create_finding(
                            rule_id="L1-009",
                            blocking=False,
                            location=f"group:{group}.canonical:{canonical}",
                            observed_value=alias,
                            normalized_value=normalized_alias,
                            proposed_action="dedupe",
                            reason="Duplicate alias inside the same canonical alias list.",
                        )
                    )
                    continue

                alias_seen.add(normalized_alias)
                deduped_aliases.append(alias)

                remove_alias_in_rewrite = False
                if is_hard_blocked_alias(normalized_canonical, normalized_alias):
                    remove_alias_in_rewrite = True
                    result.add_finding(
                        create_finding(
                            rule_id="L1-008",
                            blocking=True,
                            location=f"group:{group}.canonical:{canonical}",
                            observed_value=alias,
                            normalized_value=normalized_alias,
                            proposed_action="remove_alias",
                            reason="Alias is explicitly known to be non-interchangeable with canonical.",
                        )
                    )

                if contains_version_token(alias):
                    remove_alias_in_rewrite = True
                    result.add_finding(
                        create_finding(
                            rule_id="L1-007",
                            blocking=False,
                            location=f"group:{group}.canonical:{canonical}",
                            observed_value=alias,
                            normalized_value=normalized_alias,
                            proposed_action="remove_alias",
                            reason="Alias includes version-like token; versions are metadata, not aliases.",
                        )
                    )

                if remove_alias_in_rewrite:
                    if normalized_alias not in removed_alias_seen:
                        removed_alias_seen.add(normalized_alias)
                        removed_aliases_for_rewrite.append(alias)
                    continue

                rewritten_aliases.append(alias)

            cleaned_store[group][canonical] = deduped_aliases

            # -----------------------------------------------------
            # Atomicity Check
            # Does this canonical need to be split? (e.g. "Java/Kotlin")
            # -----------------------------------------------------
            violations = contains_atomicity_violation(canonical)
            is_excepted = normalized_canonical in atomicity_exceptions
            rewrite_decision: AtomicityRewriteDecision | None = None
            if violations and not is_excepted:
                rewrite_decision = derive_atomicity_rewrite_decision(
                    keyword=canonical,
                    violations=violations,
                )
                proposed_action = rewrite_decision.proposed_action
                proposed_payload = rewrite_decision.proposed_payload

                result.add_finding(
                    create_finding(
                        rule_id="L1-006",
                        blocking=True,
                        location=f"group:{group}.canonical:{canonical}",
                        observed_value=canonical,
                        normalized_value=normalized_canonical,
                        proposed_action=proposed_action,
                        reason="Canonical violates atomicity policy.",
                        proposed_payload=proposed_payload,
                    )
                )

                # 4. Handle Determinism
                # Even if we found a violation, we can only AUTO-FIX it if the rewrite
                # logic is 100% deterministic (e.g. splitting on slashes).
                # If it's ambiguous (e.g. "term1 and term2"), we flag it for manual review.
                if not rewrite_decision.has_deterministic_rewrite:
                    result.add_finding(
                        create_finding(
                            rule_id="L1-010",
                            blocking=False,
                            location=f"group:{group}.canonical:{canonical}",
                            observed_value=canonical,
                            normalized_value=normalized_canonical,
                            proposed_action="manual_review",
                            reason="No deterministic rewrite available; requires manual governance decision.",
                        )
                    )

            # ---------------------------------------------------------------------
            # WRITE TO STORE
            # Apply the rewrite decision (or just pass through if no decision).
            # This generates the actual list of `RewrittenCanonicalEntry` objects
            # (e.g., "Java/Kotlin" -> [Entry("Java"), Entry("Kotlin")]).
            # ---------------------------------------------------------------------
            rewritten_entries = build_rewritten_entries(
                original_canonical=canonical,
                original_aliases=rewritten_aliases,
                rewrite_decision=rewrite_decision,
            )
            
            # Upsert into the new clean store.
            # Handles merging: e.g. if we split "A/B" -> "A", and "A" already existed,
            # we merge their aliases.
            _upsert_rewritten_entries_for_group(
                rewritten_group_store=rewritten_store[group],
                rewritten_entries=rewritten_entries,
            )
            
            # Log the action for the "Rewrite Plan" artifact (audit trail).
            rewrite_plan.append(
                _build_rewrite_plan_entry(
                    group=group,
                    source_canonical=canonical,
                    rewrite_decision=rewrite_decision,
                    rewritten_entries=rewritten_entries,
                    removed_aliases=removed_aliases_for_rewrite,
                )
            )

    # ---------------------------------------------------------
    # Post-Rewrite Validation
    # We now have a "rewritten_store". We must cross-validate it to ensure
    # we didn't introduce new collisions (e.g. splitting "A/B" created an "A" that conflicts with existing "A").
    # ---------------------------------------------------------
    _apply_duplicate_canonical_findings(result, canonical_locations)
    _apply_alias_multiplicity_findings(result, alias_targets, alias_locations)
    _apply_alias_collision_findings(result, canonical_locations, alias_locations)
    _apply_group_collision_findings(result, group_names, canonical_locations)

    _auto_drop_aliases_matching_existing_canonicals(
        result=result,
        rewritten_store=rewritten_store,
    )
    suffix_redundancy_candidates = _collect_suffix_redundancy_candidates(rewritten_store)

    original_canonical_rows = _build_canonical_rows_from_store(cleaned_store)
    rewritten_canonical_rows = _build_canonical_rows_from_store(rewritten_store)

    result.payload["cleaned_store"] = cleaned_store
    result.payload["rewritten_store"] = rewritten_store
    result.payload["rewrite_plan"] = rewrite_plan
    result.payload["suffix_redundancy_candidates"] = suffix_redundancy_candidates
    result.payload["rewritten_validation_report"] = _build_rewritten_store_validation_report(
        rewritten_store=rewritten_store,
        source_path=source_path,
        source_hash=source_hash,
    )
    result.payload["original_canonical_rows"] = original_canonical_rows
    result.payload["canonical_rows"] = rewritten_canonical_rows
    result.payload["validation_report"] = ValidationReport(
        source_path=source_path,
        source_hash=source_hash,
        summary=_summarize_findings(result.findings),
        findings=sorted(
            result.findings,
            key=lambda finding: (
                finding.rule_id,
                finding.location,
                finding.observed_value,
            ),
        ),
    ).to_dict()

    return result


def _build_canonical_rows_from_store(store: Dict[str, Dict[str, List[str]]]) -> List[Dict[str, Any]]:
    """
    Flattens the hierarchical store into a list of row-like dictionaries.
    Useful for creating Pandas DataFrames or CSV exports later.

    Args:
        store: The taxonomy store {Group -> {Canonical -> [Aliases]}}

    Returns:
        List[Dict]: A list of objects with keys: group, canonical, canonical_normalized, aliases.
    """
    rows: List[Dict[str, Any]] = []

    sorted_groups = sorted(store)
    for group in sorted_groups:
        group_value = store.get(group, {})
        if not isinstance(group_value, dict):
            continue

        sorted_canonicals = sorted(group_value)
        for canonical in sorted_canonicals:
            aliases_value = group_value.get(canonical, [])
            aliases: List[str] = []
            if isinstance(aliases_value, list):
                for alias in aliases_value:
                    aliases.append(str(alias))

            rows.append(
                {
                    "group": str(group),
                    "canonical": str(canonical),
                    "canonical_normalized": normalize_term(str(canonical)),
                    "aliases": aliases,
                }
            )
    return rows


def _collect_suffix_redundancy_candidates(
    rewritten_store: Dict[str, Dict[str, List[str]]],
) -> List[Dict[str, str]]:
    """
    Audit-only detector:
    Find aliases that end with the normalized name of another canonical in the same group.

    This does not mutate the store. It produces advisory candidates for Stage 3/manual review.
    """
    candidates: List[Dict[str, str]] = []
    seen_keys: Set[Tuple[str, str, str, str]] = set()

    sorted_groups = sorted(rewritten_store)
    for group in sorted_groups:
        group_value = rewritten_store.get(group, {})
        if not isinstance(group_value, dict):
            continue

        canonical_rows: List[Tuple[str, str]] = []
        sorted_canonicals = sorted(group_value)
        for canonical in sorted_canonicals:
            canonical_text = str(canonical).strip()
            if not canonical_text:
                continue
            canonical_rows.append((canonical_text, normalize_term(canonical_text)))

        for canonical_text, canonical_normalized in canonical_rows:
            aliases_value = group_value.get(canonical_text, [])
            if not isinstance(aliases_value, list):
                continue

            for alias in aliases_value:
                alias_text = str(alias).strip()
                if not alias_text:
                    continue

                alias_normalized = normalize_term(alias_text)
                if not alias_normalized:
                    continue

                matched_canonical = ""
                matched_normalized = ""
                matched_length = -1

                for other_canonical, other_normalized in canonical_rows:
                    if other_normalized == canonical_normalized:
                        continue
                    if not other_normalized:
                        continue

                    is_suffix_match = False
                    if alias_normalized == other_normalized:
                        is_suffix_match = True
                    elif alias_normalized.endswith(" " + other_normalized):
                        is_suffix_match = True

                    if not is_suffix_match:
                        continue

                    other_length = len(other_normalized)
                    if other_length > matched_length:
                        matched_length = other_length
                        matched_canonical = other_canonical
                        matched_normalized = other_normalized

                if not matched_canonical:
                    continue

                unique_key = (group, canonical_text, alias_text, matched_canonical)
                if unique_key in seen_keys:
                    continue
                seen_keys.add(unique_key)

                candidates.append(
                    {
                        "group": group,
                        "canonical": canonical_text,
                        "alias": alias_text,
                        "matched_canonical": matched_canonical,
                        "matched_canonical_normalized": matched_normalized,
                    }
                )

    candidates.sort(
        key=lambda row: (
            row["group"],
            row["canonical"],
            row["alias"],
            row["matched_canonical"],
        )
    )
    return candidates


def _auto_drop_aliases_matching_existing_canonicals(
    result: StageResult,
    rewritten_store: Dict[str, Dict[str, List[str]]],
) -> None:
    """
    Deterministic auto-fix:
    Remove alias entries when alias text matches an existing canonical text
    exactly or after deterministic normalization.

    This intentionally avoids fuzzy or semantic matching.
    """
    canonical_strings: Set[str] = set()
    canonical_normalized_strings: Set[str] = set()

    sorted_groups = sorted(rewritten_store)
    for group in sorted_groups:
        group_value = rewritten_store.get(group, {})
        if not isinstance(group_value, dict):
            continue

        sorted_canonicals = sorted(group_value)
        for canonical in sorted_canonicals:
            canonical_text = str(canonical).strip()
            if not canonical_text:
                continue
            canonical_strings.add(canonical_text)
            canonical_normalized_strings.add(normalize_term(canonical_text))

    for group in sorted_groups:
        group_value = rewritten_store.get(group, {})
        if not isinstance(group_value, dict):
            continue

        sorted_canonicals = sorted(group_value)
        for canonical in sorted_canonicals:
            canonical_text = str(canonical).strip()
            canonical_normalized_text = normalize_term(canonical_text)
            aliases = group_value.get(canonical, [])
            if not isinstance(aliases, list):
                continue

            kept_aliases: List[str] = []
            for alias in aliases:
                alias_text = str(alias).strip()
                if not alias_text:
                    continue

                alias_normalized_text = normalize_term(alias_text)
                if alias_normalized_text == canonical_normalized_text:
                    continue

                alias_matches_canonical = False
                if alias_text in canonical_strings:
                    alias_matches_canonical = True
                elif alias_normalized_text in canonical_normalized_strings:
                    alias_matches_canonical = True

                if alias_matches_canonical:
                    result.add_finding(
                        create_finding(
                            rule_id="L1-011",
                            blocking=False,
                            location=f"group:{group}.canonical:{canonical}",
                            observed_value=alias_text,
                            normalized_value=alias_normalized_text,
                            proposed_action="remove_alias",
                            reason=(
                                "Alias matches an existing rewritten canonical string after normalization; "
                                "auto-removed deterministically."
                            ),
                        )
                    )
                    continue

                kept_aliases.append(str(alias))

            group_value[canonical] = kept_aliases


def _upsert_rewritten_entries_for_group(
    rewritten_group_store: Dict[str, List[str]],
    rewritten_entries: List[RewrittenCanonicalEntry],
) -> None:
    """
    Updates the rewritten store with new entries, handling merges if necessary.

    If a rewrite results in a canonical that ALREADY exists in the rewritten store,
    we merge the aliases rather than overwriting. This happens if e.g. "A/B" splits into "A" and "B",
    but "A" was also a standalone term elsewhere.

    Args:
        rewritten_group_store: The dictionary for the current group in the rewritten store.
        rewritten_entries: The list of new entries to add.
    """
    for entry in rewritten_entries:
        target_canonical = str(entry.canonical)
        target_aliases = [str(alias) for alias in entry.aliases]

        existing_aliases = rewritten_group_store.get(target_canonical)
        if existing_aliases is None:
            rewritten_group_store[target_canonical] = target_aliases
            continue

        merged_aliases = _merge_alias_lists(existing_aliases, target_aliases, target_canonical)
        rewritten_group_store[target_canonical] = merged_aliases


def _merge_alias_lists(
    left_aliases: List[str],
    right_aliases: List[str],
    canonical: str,
) -> List[str]:
    """
    Merges two lists of aliases, deduping them and removing any that match the canonical.

    Args:
        left_aliases: First list.
        right_aliases: Second list.
        canonical: The canonical term (to ensure it doesn't appear in aliases).

    Returns:
        List[str]: The clean, merged, sorted list of aliases.
    """
    canonical_normalized = normalize_term(canonical)
    seen: Set[str] = set()
    merged: List[str] = []

    for alias in [*left_aliases, *right_aliases]:
        alias_text = str(alias).strip()
        if not alias_text:
            continue
        alias_normalized = normalize_term(alias_text)
        if alias_normalized == canonical_normalized:
            continue
        if alias_normalized in seen:
            continue
        seen.add(alias_normalized)
        merged.append(alias_text)

    sorted_aliases = sorted(
        merged,
        key=lambda value: normalize_term(str(value)),
    )
    return sorted_aliases


def _build_rewrite_plan_entry(
    group: str,
    source_canonical: str,
    rewrite_decision: AtomicityRewriteDecision | None,
    rewritten_entries: List[RewrittenCanonicalEntry],
    removed_aliases: List[str],
) -> Dict[str, Any]:
    """
    Constructs a log entry describing what happened to a specific canonical.
    This is used for the "Rewrite Plan" artifact.

    Args:
        group: The group name.
        source_canonical: The original term.
        rewrite_decision: The decision object (what we planned to do).
        rewritten_entries: The actual result (what we did).
        removed_aliases: List of aliases dropped during hygiene.

    Returns:
        Dict: A structured log entry.
    """
    rewritten_canonicals: List[str] = []
    for entry in rewritten_entries:
        rewritten_canonicals.append(str(entry.canonical))

    if rewrite_decision is None:
        return {
            "group": group,
            "source_canonical": source_canonical,
            "rewrite_applied": False,
            "proposed_action": "none",
            "target_canonicals": rewritten_canonicals,
            "removed_aliases": removed_aliases,
        }

    return {
        "group": group,
        "source_canonical": source_canonical,
        "rewrite_applied": bool(rewrite_decision.has_deterministic_rewrite),
        "proposed_action": rewrite_decision.proposed_action,
        "target_canonicals": rewritten_canonicals,
        "proposed_payload": rewrite_decision.proposed_payload,
        "removed_aliases": removed_aliases,
    }


def _build_rewritten_store_validation_report(
    rewritten_store: Dict[str, Dict[str, List[str]]],
    source_path: str,
    source_hash: str,
) -> Dict[str, Any]:
    """
    Runs a full validation pass on the *output* of the rewrite process.
    This ensures that our rewrites didn't create invalid states (like collisions).

    Args:
        rewritten_store: The clean taxonomy.
        source_path: Metadata.
        source_hash: Metadata.

    Returns:
        Dict: A ValidationReport dictionary.
    """
    findings: List[Finding] = []

    schema_findings, has_schema_error = validate_schema(rewritten_store)
    for finding in schema_findings:
        findings.append(_rewrite_schema_finding(finding))

    if not has_schema_error:
        canonical_locations: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        alias_locations: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
        alias_targets: Dict[str, Set[str]] = defaultdict(set)
        group_names: Dict[str, str] = {}

        sorted_groups = sorted(rewritten_store)
        for group in sorted_groups:
            normalized_group = normalize_term(group)
            group_names[normalized_group] = group

            sorted_canonicals = sorted(rewritten_store[group])
            for canonical in sorted_canonicals:
                aliases = rewritten_store[group][canonical]
                normalized_canonical = normalize_term(canonical)
                canonical_locations[normalized_canonical].append((group, canonical))

                alias_seen: Set[str] = set()
                for alias in aliases:
                    normalized_alias = normalize_term(alias)
                    alias_locations[normalized_alias].append((group, canonical, alias))
                    alias_targets[normalized_alias].add(normalized_canonical)

                    if normalized_alias in alias_seen:
                        findings.append(
                            create_finding(
                                rule_id="L1R-009",
                                blocking=False,
                                location=f"group:{group}.canonical:{canonical}",
                                observed_value=alias,
                                normalized_value=normalized_alias,
                                proposed_action="dedupe",
                                reason="Duplicate alias inside rewritten canonical alias list.",
                            )
                        )
                        continue
                    alias_seen.add(normalized_alias)

                residual_violations = contains_atomicity_violation(canonical)
                if residual_violations:
                    findings.append(
                        create_finding(
                            rule_id="L1R-006",
                            blocking=False,
                            location=f"group:{group}.canonical:{canonical}",
                            observed_value=canonical,
                            normalized_value=normalized_canonical,
                            proposed_action="manual_review",
                            reason="Rewritten canonical still violates atomicity policy.",
                            proposed_payload={"violation_types": residual_violations},
                        )
                    )

        findings.extend(_collect_rewritten_duplicate_canonical_findings(canonical_locations))
        findings.extend(_collect_rewritten_alias_multiplicity_findings(alias_targets, alias_locations))
        findings.extend(_collect_rewritten_alias_collision_findings(canonical_locations, alias_locations))
        findings.extend(_collect_rewritten_group_collision_findings(group_names, canonical_locations))

    sorted_findings = sorted(
        findings,
        key=lambda finding: (
            finding.rule_id,
            finding.location,
            finding.observed_value,
        ),
    )
    report = ValidationReport(
        source_path=f"{source_path}#rewritten_store",
        source_hash=source_hash,
        summary=_summarize_findings(sorted_findings),
        findings=sorted_findings,
    )
    return report.to_dict()


def _rewrite_schema_finding(finding: Finding) -> Finding:
    """Helper to promote schema findings from the original store to the rewritten report."""
    rewritten_rule_id = "L1R-001"
    return create_finding(
        rule_id=rewritten_rule_id,
        blocking=bool(finding.blocking),
        location=finding.location,
        observed_value=finding.observed_value,
        normalized_value=finding.normalized_value,
        proposed_action=finding.proposed_action,
        reason=finding.reason,
        proposed_payload=dict(finding.proposed_payload),
    )


def _collect_rewritten_duplicate_canonical_findings(
    canonical_locations: Dict[str, List[Tuple[str, str]]],
) -> List[Finding]:
    """Checks if the rewritten store contains duplicates of the same canonical (case-insensitive)."""
    findings: List[Finding] = []
    for normalized_canonical, locations in sorted(canonical_locations.items()):
        if len(locations) <= 1:
            continue

        location_parts: List[str] = []
        for group, canonical in locations:
            location_parts.append(f"{group}:{canonical}")
        location = ", ".join(location_parts)

        findings.append(
            create_finding(
                rule_id="L1R-002",
                blocking=True,
                location=location,
                observed_value=normalized_canonical,
                normalized_value=normalized_canonical,
                proposed_action="dedupe",
                reason="Rewritten canonical appears multiple times after normalization.",
            )
        )
    return findings


def _collect_rewritten_alias_multiplicity_findings(
    alias_targets: Dict[str, Set[str]],
    alias_locations: Dict[str, List[Tuple[str, str, str]]],
) -> List[Finding]:
    """Checks if any alias in the rewritten store maps to >1 different canonicals."""
    findings: List[Finding] = []
    for normalized_alias, targets in sorted(alias_targets.items()):
        if len(targets) <= 1:
            continue

        target_list = sorted(targets)
        locations = alias_locations.get(normalized_alias, [])
        location_parts: List[str] = []
        for group, canonical, alias in locations[:10]:
            location_parts.append(f"{group}:{canonical}({alias})")
        location = ", ".join(location_parts)

        findings.append(
            create_finding(
                rule_id="L1R-004",
                blocking=True,
                location=location,
                observed_value=normalized_alias,
                normalized_value=normalized_alias,
                proposed_action="manual_review",
                reason="Rewritten alias maps to multiple canonical targets.",
                proposed_payload={"targets": target_list},
            )
        )
    return findings


def _collect_rewritten_alias_collision_findings(
    canonical_locations: Dict[str, List[Tuple[str, str]]],
    alias_locations: Dict[str, List[Tuple[str, str, str]]],
) -> List[Finding]:
    """Checks if any alias in the rewritten store collides with a canonical name."""
    findings: List[Finding] = []
    for normalized_alias, locations in sorted(alias_locations.items()):
        if normalized_alias not in canonical_locations:
            continue

        alias_location_parts: List[str] = []
        for group, canonical, alias in locations[:10]:
            alias_location_parts.append(f"{group}:{canonical}({alias})")

        canonical_location_parts: List[str] = []
        for group, canonical in canonical_locations[normalized_alias][:10]:
            canonical_location_parts.append(f"{group}:{canonical}")

        location = (
            "aliases["
            + ", ".join(alias_location_parts)
            + "] vs canonicals["
            + ", ".join(canonical_location_parts)
            + "]"
        )
        findings.append(
            create_finding(
                rule_id="L1R-003",
                blocking=True,
                location=location,
                observed_value=normalized_alias,
                normalized_value=normalized_alias,
                proposed_action="manual_review",
                reason="Rewritten alias collides with an existing rewritten canonical term.",
            )
        )
    return findings


def _collect_rewritten_group_collision_findings(
    group_names: Dict[str, str],
    canonical_locations: Dict[str, List[Tuple[str, str]]],
) -> List[Finding]:
    """Checks if any Group Name collides with a Canonical Name."""
    findings: List[Finding] = []
    for normalized_group, original_group in sorted(group_names.items()):
        if normalized_group not in canonical_locations:
            continue

        canonical_location_parts: List[str] = []
        for group, canonical in canonical_locations[normalized_group]:
            canonical_location_parts.append(f"{group}:{canonical}")

        location = f"group:{original_group} vs canonical:{', '.join(canonical_location_parts)}"
        findings.append(
            create_finding(
                rule_id="L1R-005",
                blocking=True,
                location=location,
                observed_value=original_group,
                normalized_value=normalized_group,
                proposed_action="manual_review",
                reason="Rewritten group name collides with rewritten canonical term.",
            )
        )
    return findings


def _apply_duplicate_canonical_findings(
    result: StageResult,
    canonical_locations: Dict[str, List[Tuple[str, str]]],
) -> None:
    """Validator: Adds findings for canonical duplication."""
    for normalized_canonical, locations in sorted(canonical_locations.items()):
        if len(locations) <= 1:
            continue

        pieces: List[str] = []
        for group, canonical in locations:
            pieces.append(f"{group}:{canonical}")
        location_str = ", ".join(pieces)

        result.add_finding(
            create_finding(
                rule_id="L1-002",
                blocking=True,
                location=location_str,
                observed_value=normalized_canonical,
                normalized_value=normalized_canonical,
                proposed_action="dedupe",
                reason="Canonical appears multiple times after normalization.",
            )
        )


def _apply_alias_multiplicity_findings(
    result: StageResult,
    alias_targets: Dict[str, Set[str]],
    alias_locations: Dict[str, List[Tuple[str, str, str]]],
) -> None:
    """Validator: Adds findings for alias multiplicity (alias -> multiple canonicals)."""
    for normalized_alias, targets in sorted(alias_targets.items()):
        if len(targets) <= 1:
            continue

        target_list = sorted(targets)
        locations = alias_locations.get(normalized_alias, [])

        location_parts: List[str] = []
        for group, canonical, alias in locations[:10]:
            location_parts.append(f"{group}:{canonical}({alias})")
        location_str = ", ".join(location_parts)

        result.add_finding(
            create_finding(
                rule_id="L1-004",
                blocking=True,
                location=location_str,
                observed_value=normalized_alias,
                normalized_value=normalized_alias,
                proposed_action="manual_review",
                reason="Alias maps to multiple canonical targets.",
                proposed_payload={"targets": target_list},
            )
        )


def _apply_alias_collision_findings(
    result: StageResult,
    canonical_locations: Dict[str, List[Tuple[str, str]]],
    alias_locations: Dict[str, List[Tuple[str, str, str]]],
) -> None:
    """Validator: Adds findings for alias collision (alias == some canonical)."""
    for normalized_alias, locations in sorted(alias_locations.items()):
        if normalized_alias not in canonical_locations:
            continue

        alias_location_parts: List[str] = []
        for group, canonical, alias in locations[:10]:
            alias_location_parts.append(f"{group}:{canonical}({alias})")
        alias_location_str = ", ".join(alias_location_parts)

        canonical_location_parts: List[str] = []
        for group, canonical in canonical_locations[normalized_alias][:10]:
            canonical_location_parts.append(f"{group}:{canonical}")
        canonical_location_str = ", ".join(canonical_location_parts)

        result.add_finding(
            create_finding(
                rule_id="L1-003",
                blocking=True,
                location=f"aliases[{alias_location_str}] vs canonicals[{canonical_location_str}]",
                observed_value=normalized_alias,
                normalized_value=normalized_alias,
                proposed_action="manual_review",
                reason="Alias collides with an existing canonical term.",
            )
        )


def _apply_group_collision_findings(
    result: StageResult,
    group_names: Dict[str, str],
    canonical_locations: Dict[str, List[Tuple[str, str]]],
) -> None:
    """Validator: Adds findings for group collision (group name == some canonical)."""
    for normalized_group, original_group in sorted(group_names.items()):
        if normalized_group not in canonical_locations:
            continue

        canonical_location_parts: List[str] = []
        for group, canonical in canonical_locations[normalized_group]:
            canonical_location_parts.append(f"{group}:{canonical}")
        canonical_location_str = ", ".join(canonical_location_parts)

        result.add_finding(
            create_finding(
                rule_id="L1-005",
                blocking=True,
                location=f"group:{original_group} vs canonical:{canonical_location_str}",
                observed_value=original_group,
                normalized_value=normalized_group,
                proposed_action="manual_review",
                reason="Group name collides with canonical term.",
            )
        )


def _summarize_findings(findings: List[Finding]) -> Dict[str, int]:
    """Counts errors, warnings, and blocking findings for the report summary."""
    errors = 0
    warnings = 0
    blocking = 0

    for finding in findings:
        if finding.severity == "error":
            errors += 1
        if finding.severity == "warning":
            warnings += 1
        if finding.blocking:
            blocking += 1

    summary = {
        "errors": errors,
        "warnings": warnings,
        "blocking": blocking,
    }
    return summary

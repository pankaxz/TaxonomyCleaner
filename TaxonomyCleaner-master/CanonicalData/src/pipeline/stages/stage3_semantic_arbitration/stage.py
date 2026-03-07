from __future__ import annotations

import os
from typing import Any
from typing import Dict
from typing import List
from typing import Set

from ...shared.findings import create_finding
from ...shared.models import ALLOWED_ARBITRATION_ACTIONS
from ...shared.models import ALLOWED_PRIMARY_CONFIDENCE
from ...shared.models import CONFIDENCE_HIGH
from ...shared.models import CONFIDENCE_LOW
from ...shared.models import GovernedArbitrationDecision
from ...shared.models import ReviewQueueEntry
from ...shared.models import StageResult
from ...shared.utilities import contains_atomicity_violation
from ...shared.utilities import explicit_split_tokens
from ...shared.utilities import load_json_file
from ...shared.utilities import normalize_term
from ...shared.utilities import write_json


def run_stage3_arbitration(
    conflict_clusters: List[Dict[str, object]],
    llm_client,
    known_canonicals: Set[str],
    alias_canonical_advisories: List[Dict[str, object]] | None = None,
    suffix_audit_candidates: List[Dict[str, object]] | None = None,
    checkpoint_every: int = 10,
    checkpoint_dir: str | None = None,
) -> StageResult:
    result = StageResult()
    governed_decisions: List[Dict[str, object]] = []
    review_queue: List[ReviewQueueEntry] = []

    total_clusters = len(conflict_clusters)
    processed_clusters = 0

    # 1. Check for existing checkpoint to resume
    start_index = 0
    if checkpoint_dir:
        meta_path = os.path.join(checkpoint_dir, "stage3_checkpoint_meta.json")
        if os.path.exists(meta_path):
            try:
                print(f">> Found checkpoint at {meta_path}. Attempting to resume...")
                meta = load_json_file(meta_path)
                processed_clusters = meta.get("processed_clusters", 0)
                
                # Load partial results
                decisions_path = os.path.join(checkpoint_dir, "stage3_arbitration_decisions.partial.json")
                if os.path.exists(decisions_path):
                    governed_decisions = load_json_file(decisions_path)
                
                queue_path = os.path.join(checkpoint_dir, "stage3_review_queue.partial.json")
                if os.path.exists(queue_path):
                    raw_queue = load_json_file(queue_path)
                    for item in raw_queue:
                        review_queue.append(ReviewQueueEntry(**item))

                findings_path = os.path.join(checkpoint_dir, "stage3_findings.partial.json")
                if os.path.exists(findings_path):
                    raw_findings = load_json_file(findings_path)
                    from ...shared.models import Finding  # Local import to avoid circular dependency if any
                    for item in raw_findings:
                        result.add_finding(Finding(**item))
                
                start_index = processed_clusters
                print(f">> Resuming from index {start_index} (Processed: {processed_clusters}/{total_clusters})")
            except Exception as exc:
                print(f">> Failed to resume from checkpoint: {exc}. Starting from scratch.")
                processed_clusters = 0
                start_index = 0
                governed_decisions = []
                review_queue = []
                result = StageResult()

    _append_suffix_audit_review_entries(
        review_queue=review_queue,
        suffix_audit_candidates=suffix_audit_candidates,
    )
    _append_alias_canonical_advisory_entries(
        review_queue=review_queue,
        alias_canonical_advisories=alias_canonical_advisories,
    )

    # 2. Process clusters (skipping already processed ones)
    for cluster in conflict_clusters[start_index:]:
        cluster_id = str(cluster["cluster_id"])
        
        print(f"\n--- Processing Cluster {cluster_id} ({processed_clusters + 1}/{total_clusters}) ---")

        terms: List[str] = []
        for term in cluster["terms"]:
            terms.append(str(term))
        
        print(f"Terms: {terms}")

        try:
            print(">> Calling LLM...")
            response = llm_client.arbitrate_cluster(cluster_id, terms)
        except Exception as exc:  # noqa: BLE001
            result.parse_error = True
            result.add_finding(
                create_finding(
                    rule_id="L3-001",
                    blocking=True,
                    location=f"cluster:{cluster_id}",
                    observed_value=str(exc),
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Arbitration model call failed.",
                )
            )
            continue

        if not isinstance(response, dict) or "decisions" not in response:
            result.parse_error = True
            result.add_finding(
                create_finding(
                    rule_id="L3-001",
                    blocking=True,
                    location=f"cluster:{cluster_id}",
                    observed_value=str(response),
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Arbitration response is not valid JSON object with decisions array.",
                )
            )
            continue

        decisions = response.get("decisions")
        if not isinstance(decisions, list):
            result.parse_error = True
            result.add_finding(
                create_finding(
                    rule_id="L3-001",
                    blocking=True,
                    location=f"cluster:{cluster_id}",
                    observed_value=str(response),
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Arbitration decisions must be an array.",
                )
            )
            continue
        
        print(f">> LLM Response Received (Decisions: {len(decisions)})")

        for row in decisions:
            decision = _validate_and_govern_decision(
                row=row,
                cluster_id=cluster_id,
                cluster_terms=terms,
                known_canonicals=known_canonicals,
                review_queue=review_queue,
                result=result,
            )
            governed_decisions.append(decision.to_dict())
            
            # Log decision details
            print(f"  [Term]: {decision.term}")
            print(f"  [Action]: {decision.effective_action} (Requested: {decision.requested_action})")
            print(f"  [Confidence]: {decision.confidence}")
            if decision.reasoning:
                print(f"  [Reasoning]: {decision.reasoning}")
            print("-" * 20)

        processed_clusters += 1
        checkpoint_enabled = checkpoint_every != 0
        if checkpoint_enabled and checkpoint_dir:
            print(f">> Saving checkpoint at {processed_clusters} processed clusters...")
            _write_stage3_checkpoint(
                checkpoint_dir=checkpoint_dir,
                processed_clusters=processed_clusters,
                total_clusters=total_clusters,
                governed_decisions=governed_decisions,
                review_queue=review_queue,
                findings=result.findings,
            )

    if checkpoint_every != 0 and checkpoint_dir:
        _write_stage3_checkpoint(
            checkpoint_dir=checkpoint_dir,
            processed_clusters=processed_clusters,
            total_clusters=total_clusters,
            governed_decisions=governed_decisions,
            review_queue=review_queue,
            findings=result.findings,
        )

    result.payload["governed_arbitration_decisions"] = governed_decisions

    serialized_review_queue: List[Dict[str, object]] = []
    for entry in review_queue:
        serialized_review_queue.append(entry.to_json_dict())
    result.payload["review_queue_entries"] = serialized_review_queue

    return result


def _append_suffix_audit_review_entries(
    review_queue: List[ReviewQueueEntry],
    suffix_audit_candidates: List[Dict[str, object]] | None,
) -> None:
    if not isinstance(suffix_audit_candidates, list):
        return

    existing_keys: Set[tuple[str, int, str, str, str]] = set()
    for entry in review_queue:
        existing_keys.add(
            (
                str(entry.term),
                int(entry.stage),
                str(entry.issue),
                str(entry.proposed_action),
                str(entry.confidence),
            )
        )

    sorted_candidates: List[Dict[str, object]] = []
    for row in suffix_audit_candidates:
        if isinstance(row, dict):
            sorted_candidates.append(row)

    sorted_candidates.sort(
        key=lambda row: (
            str(row.get("group", "")),
            str(row.get("canonical", "")),
            str(row.get("alias", "")),
            str(row.get("matched_canonical", "")),
        )
    )

    for row in sorted_candidates:
        group = str(row.get("group", "")).strip()
        canonical = str(row.get("canonical", "")).strip()
        alias = str(row.get("alias", "")).strip()
        matched_canonical = str(row.get("matched_canonical", "")).strip()

        if not alias:
            continue
        if not group:
            continue
        if not canonical:
            continue
        if not matched_canonical:
            continue

        issue = (
            "Stage0 suffix-overlap audit candidate: "
            f"alias '{alias}' under canonical '{canonical}' in group '{group}' "
            f"ends with canonical '{matched_canonical}'."
        )

        dedupe_key = (
            alias,
            3,
            issue,
            "KEEP_DISTINCT",
            CONFIDENCE_LOW,
        )
        if dedupe_key in existing_keys:
            continue
        existing_keys.add(dedupe_key)

        review_queue.append(
            ReviewQueueEntry(
                term=alias,
                stage=3,
                issue=issue,
                proposed_action="KEEP_DISTINCT",
                confidence=CONFIDENCE_LOW,
            )
        )


def _append_alias_canonical_advisory_entries(
    review_queue: List[ReviewQueueEntry],
    alias_canonical_advisories: List[Dict[str, object]] | None,
) -> None:
    if not isinstance(alias_canonical_advisories, list):
        return

    existing_keys: Set[tuple[str, int, str, str, str]] = set()
    for entry in review_queue:
        existing_keys.add(
            (
                str(entry.term),
                int(entry.stage),
                str(entry.issue),
                str(entry.proposed_action),
                str(entry.confidence),
            )
        )

    sorted_rows: List[Dict[str, object]] = []
    for row in alias_canonical_advisories:
        if isinstance(row, dict):
            sorted_rows.append(row)

    sorted_rows.sort(
        key=lambda row: (
            str(row.get("group", "")),
            str(row.get("source_canonical", "")),
            str(row.get("alias", "")),
            str(row.get("target_canonical", "")),
        )
    )

    for row in sorted_rows:
        group = str(row.get("group", "")).strip()
        source_canonical = str(row.get("source_canonical", "")).strip()
        alias = str(row.get("alias", "")).strip()
        target_canonical = str(row.get("target_canonical", "")).strip()
        score = row.get("score", 0.0)
        band = str(row.get("band", "")).strip()

        if not alias:
            continue
        if not group:
            continue
        if not source_canonical:
            continue
        if not target_canonical:
            continue

        issue = (
            "Stage1 alias->canonical advisory: "
            f"alias '{alias}' from canonical '{source_canonical}' in group '{group}' "
            f"is similar to canonical '{target_canonical}'"
        )
        if band:
            issue += f" ({band}"
            if isinstance(score, (int, float)):
                issue += f", score={float(score):.6f}"
            issue += ")"
        elif isinstance(score, (int, float)):
            issue += f" (score={float(score):.6f})"
        issue += "."

        dedupe_key = (
            alias,
            3,
            issue,
            "KEEP_DISTINCT",
            CONFIDENCE_LOW,
        )
        if dedupe_key in existing_keys:
            continue
        existing_keys.add(dedupe_key)

        review_queue.append(
            ReviewQueueEntry(
                term=alias,
                stage=3,
                issue=issue,
                proposed_action="KEEP_DISTINCT",
                confidence=CONFIDENCE_LOW,
            )
        )


def _validate_and_govern_decision(
    row: object,
    cluster_id: str,
    cluster_terms: List[str],
    known_canonicals: Set[str],
    review_queue: List[ReviewQueueEntry],
    result: StageResult,
) -> GovernedArbitrationDecision:
    if not isinstance(row, dict):
        result.parse_error = True
        result.add_finding(
            create_finding(
                rule_id="L3-001",
                blocking=True,
                location=f"cluster:{cluster_id}|term:<invalid_row>",
                observed_value=str(row),
                normalized_value="",
                proposed_action="manual_review",
                reason="Each arbitration decision must be a JSON object.",
                proposed_payload={
                    "cluster_id": cluster_id,
                    "cluster_terms": list(cluster_terms),
                },
            )
        )
        return GovernedArbitrationDecision(
            term="",
            requested_action="KEEP_DISTINCT",
            effective_action="KEEP_DISTINCT",
            target_canonical=None,
            split_candidates=None,
            confidence=CONFIDENCE_LOW,
            blocked=True,
            violations=["invalid_schema"],
        )

    term = str(row.get("term", ""))
    action = str(row.get("action", ""))
    target = row.get("target_canonical")
    split_candidates = row.get("split_candidates")
    confidence = str(row.get("confidence", ""))
    reasoning = row.get("reasoning", {})

    violations: List[str] = []
    blocked = False
    effective_action = action

    if term not in cluster_terms:
        violations.append("term_not_in_cluster")

    if action not in ALLOWED_ARBITRATION_ACTIONS:
        violations.append("invalid_action")

    if confidence not in ALLOWED_PRIMARY_CONFIDENCE:
        violations.append("invalid_confidence")

    if not isinstance(reasoning, dict):
        violations.append("invalid_reasoning")
        reasoning = {}

    if action in {"MERGE_AS_ALIAS", "MARK_AS_CONTEXTUAL"}:
        if target is None or not str(target).strip():
            violations.append("missing_target_canonical")

    if action == "MERGE_AS_ALIAS" and isinstance(reasoning, dict):
        ecosystem_reason = normalize_term(str(reasoning.get("ecosystem", "")))
        abstraction_reason = normalize_term(str(reasoning.get("abstraction_level", "")))

        if "different" in ecosystem_reason:
            violations.append("ecosystem_mismatch")

        if "different" in abstraction_reason:
            violations.append("abstraction_mismatch")

    if action == "SPLIT_INTO_MULTIPLE_CANONICALS":
        _validate_split_action(
            term=term,
            confidence=confidence,
            split_candidates=split_candidates,
            reasoning=reasoning,
            known_canonicals=known_canonicals,
            violations=violations,
        )

    if confidence == CONFIDENCE_LOW:
        review_queue.append(
            ReviewQueueEntry(
                term=term,
                stage=3,
                issue="LOW confidence arbitration",
                proposed_action=action if action else "KEEP_DISTINCT",
                confidence=CONFIDENCE_LOW,
            )
        )

        effective_action = "KEEP_DISTINCT"
        blocked = True
        violations.append("low_confidence_containment")

    if violations:
        blocked = True
        effective_action = "KEEP_DISTINCT"

        result.add_finding(
            create_finding(
                rule_id="L3-002",
                blocking=True,
                location=f"cluster:{cluster_id}|term:{term if term else '<missing>'}",
                observed_value=action,
                normalized_value=normalize_term(action),
                proposed_action="manual_review",
                reason="Arbitration decision failed deterministic governance validation.",
                proposed_payload={
                    "cluster_id": cluster_id,
                    "cluster_terms": list(cluster_terms),
                    "violations": sorted(set(violations)),
                    "requested_action": action,
                    "confidence": confidence,
                },
            )
        )

    safe_confidence = CONFIDENCE_LOW
    if confidence in ALLOWED_PRIMARY_CONFIDENCE:
        safe_confidence = confidence

    serialized_reasoning: Dict[str, str] = {}
    if isinstance(reasoning, dict):
        for key, value in reasoning.items():
            serialized_reasoning[str(key)] = str(value)

    governed = GovernedArbitrationDecision(
        term=term,
        requested_action=action,
        effective_action=effective_action,
        target_canonical=str(target) if target is not None else None,
        split_candidates=[str(item) for item in split_candidates] if isinstance(split_candidates, list) else None,
        confidence=safe_confidence,
        blocked=blocked,
        violations=sorted(set(violations)),
        reasoning=serialized_reasoning,
    )
    return governed


def _validate_split_action(
    term: str,
    confidence: str,
    split_candidates: object,
    reasoning: object,
    known_canonicals: Set[str],
    violations: List[str],
) -> None:
    if confidence != CONFIDENCE_HIGH:
        violations.append("split_requires_high_confidence")

    if not isinstance(split_candidates, list) or not split_candidates:
        violations.append("split_candidates_required")
    else:
        normalized_candidates: List[str] = []
        for item in split_candidates:
            normalized_candidates.append(normalize_term(str(item)))

        if len(normalized_candidates) != len(set(normalized_candidates)):
            violations.append("split_duplicate_targets")

        explicit_tokens = explicit_split_tokens(term)
        for candidate in normalized_candidates:
            if not candidate:
                violations.append("split_candidate_blank")
                continue

            if candidate not in explicit_tokens:
                violations.append("split_not_explicit_token_decomposition")

            if contains_atomicity_violation(candidate):
                violations.append("split_candidate_not_atomic")

        overlaps: List[str] = []
        for candidate in normalized_candidates:
            if candidate in known_canonicals:
                overlaps.append(candidate)

        if overlaps:
            violations.append("split_target_already_exists_requires_explicit_mapping")

    if not isinstance(reasoning, dict):
        return

    graph_safety_reason = normalize_term(str(reasoning.get("graph_safety", "")))
    abstraction_reason = normalize_term(str(reasoning.get("abstraction_level", "")))

    if "unsafe" in graph_safety_reason:
        violations.append("split_graph_impact_unsafe")

    if "different" in abstraction_reason:
        violations.append("split_abstraction_mismatch")


def _write_stage3_checkpoint(
    checkpoint_dir: str,
    processed_clusters: int,
    total_clusters: int,
    governed_decisions: List[Dict[str, Any]],
    review_queue: List[ReviewQueueEntry],
    findings,
) -> None:
    os.makedirs(checkpoint_dir, exist_ok=True)

    checkpoint_meta = {
        "processed_clusters": processed_clusters,
        "total_clusters": total_clusters,
        "decisions_count": len(governed_decisions),
        "review_queue_count": len(review_queue),
        "findings_count": len(findings),
    }

    write_json(os.path.join(checkpoint_dir, "stage3_checkpoint_meta.json"), checkpoint_meta)
    write_json(
        os.path.join(checkpoint_dir, "stage3_arbitration_decisions.partial.json"),
        governed_decisions,
    )

    review_rows: List[Dict[str, object]] = []
    for entry in review_queue:
        review_rows.append(entry.to_json_dict())
    write_json(
        os.path.join(checkpoint_dir, "stage3_review_queue.partial.json"),
        review_rows,
    )

    finding_rows: List[Dict[str, object]] = []
    for finding in findings:
        finding_rows.append(finding.to_dict())
    write_json(
        os.path.join(checkpoint_dir, "stage3_findings.partial.json"),
        finding_rows,
    )

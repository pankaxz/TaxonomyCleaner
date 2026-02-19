from __future__ import annotations

import os
from typing import Dict
from typing import List

from ...shared.findings import create_finding
from ...shared.utilities import load_json_file
from ...shared.utilities import write_json
from ...shared.models import ALLOWED_ABSTRACTION_LEVEL
from ...shared.models import ALLOWED_ONTOLOGICAL_NATURE
from ...shared.models import ALLOWED_PRIMARY_CONFIDENCE
from ...shared.models import CONFIDENCE_LOW
from ...shared.models import CanonicalRecordV2
from ...shared.models import ClassificationDecision
from ...shared.models import ReviewQueueEntry
from ...shared.models import StageResult


def run_stage4_classification(
    canonical_rows: List[Dict[str, object]],
    llm_client,
    checkpoint_every: int = 1,
    checkpoint_dir: str | None = None,
) -> StageResult:
    result = StageResult()
    decisions: List[Dict[str, object]] = []
    v2_records: List[Dict[str, object]] = []
    review_queue: List[ReviewQueueEntry] = []

    total_canonicals = len(canonical_rows)
    processed_canonicals = 0

    # 1. Check for existing checkpoint to resume
    start_index = 0
    if checkpoint_dir:
        meta_path = os.path.join(checkpoint_dir, "stage4_checkpoint_meta.json")
        if os.path.exists(meta_path):
            try:
                print(f">> Found checkpoint at {meta_path}. Attempting to resume...")
                meta = load_json_file(meta_path)
                processed_canonicals = meta.get("processed_canonicals", 0)
                
                # Load partial results
                decisions_path = os.path.join(checkpoint_dir, "stage4_classification_decisions.partial.json")
                if os.path.exists(decisions_path):
                    decisions = load_json_file(decisions_path)
                
                v2_path = os.path.join(checkpoint_dir, "stage4_v2_records.partial.json")
                if os.path.exists(v2_path):
                    v2_records = load_json_file(v2_path)
                
                queue_path = os.path.join(checkpoint_dir, "stage4_review_queue.partial.json")
                if os.path.exists(queue_path):
                    raw_queue = load_json_file(queue_path)
                    for item in raw_queue:
                        review_queue.append(ReviewQueueEntry(**item))

                findings_path = os.path.join(checkpoint_dir, "stage4_findings.partial.json")
                if os.path.exists(findings_path):
                    raw_findings = load_json_file(findings_path)
                    from ...shared.models import Finding  # Local import to avoid circular dependency if any
                    for item in raw_findings:
                        result.add_finding(Finding(**item))
                
                start_index = processed_canonicals
                print(f">> Resuming from index {start_index} (Processed: {processed_canonicals}/{total_canonicals})")
            except Exception as exc:
                print(f">> Failed to resume from checkpoint: {exc}. Starting from scratch.")
                processed_canonicals = 0
                start_index = 0
                decisions = []
                v2_records = []
                review_queue = []
                result = StageResult()


    def _process_single_canonical(row: Dict[str, object]) -> None:
        canonical = str(row["canonical"])
        aliases: List[str] = []
        for alias in row.get("aliases", []):
            aliases.append(str(alias))
        group = str(row["group"])

        print(f"\n--- Processing Canonical: {canonical} ({processed_canonicals + 1}/{total_canonicals}) ---")

        try:
            print(">> Calling LLM...")
            response = llm_client.classify_term(canonical)
        except Exception as exc:  # noqa: BLE001
            result.parse_error = True
            result.add_finding(
                create_finding(
                    rule_id="L4-001",
                    blocking=True,
                    location=f"canonical:{canonical}",
                    observed_value=str(exc),
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Classification model call failed.",
                )
            )
            return

        if not isinstance(response, dict):
            result.parse_error = True
            result.add_finding(
                create_finding(
                    rule_id="L4-001",
                    blocking=True,
                    location=f"canonical:{canonical}",
                    observed_value=str(response),
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Classification response is not a valid JSON object.",
                )
            )
            return

        special_type = response.get("type")
        if special_type == "COMPOSITE_STACK":
            _handle_composite_stack(canonical, aliases, review_queue, result, decisions)
            return

        if special_type == "CATEGORY":
            _handle_category(canonical, review_queue, result)
            return

        classification = response.get("classification")
        confidence = str(response.get("confidence", ""))
        status = str(response.get("status", "active"))
        is_contextual = bool(response.get("is_contextual", False))
        is_versioned = bool(response.get("is_versioned", False))
        is_marketing_language = bool(response.get("is_marketing_language", False))

        if not isinstance(classification, dict):
            result.parse_error = True
            result.add_finding(
                create_finding(
                    rule_id="L4-001",
                    blocking=True,
                    location=f"canonical:{canonical}",
                    observed_value=str(response),
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Classification payload must include classification object.",
                )
            )
            return

        parse_violations = _validate_classification_schema(classification, confidence)
        if parse_violations:
            result.parse_error = True
            result.add_finding(
                create_finding(
                    rule_id="L4-002",
                    blocking=True,
                    location=f"canonical:{canonical}",
                    observed_value=str(response),
                    normalized_value="",
                    proposed_action="manual_review",
                    reason="Classification decision failed schema validation.",
                    proposed_payload={"violations": parse_violations},
                )
            )
            return

        if confidence == CONFIDENCE_LOW:
            status = "under_review"
            review_queue.append(
                ReviewQueueEntry(
                    term=canonical,
                    stage=4,
                    issue="LOW confidence classification",
                    proposed_action="manual_review",
                    confidence=CONFIDENCE_LOW,
                )
            )
            result.add_finding(
                create_finding(
                    rule_id="L4-003",
                    blocking=False,
                    location=f"canonical:{canonical}",
                    observed_value=confidence,
                    normalized_value=confidence,
                    proposed_action="manual_review",
                    reason="LOW confidence classification is contained and queued for manual review.",
                )
            )
        elif status == "under_review":
            review_queue.append(
                ReviewQueueEntry(
                    term=canonical,
                    stage=4,
                    issue="Classification marked under_review",
                    proposed_action="manual_review",
                    confidence=confidence,
                )
            )

        decision = ClassificationDecision(
            canonical=canonical,
            aliases=aliases,
            classification={
                "ontological_nature": str(classification.get("ontological_nature")),
                "primary_type": classification.get("primary_type"),
                "functional_roles": [str(role) for role in classification.get("functional_roles", [])],
                "abstraction_level": str(classification.get("abstraction_level")),
            },
            status=status,
            confidence=confidence,
            is_contextual=is_contextual,
            is_versioned=is_versioned,
            is_marketing_language=is_marketing_language,
        )
        decisions.append(decision.to_dict())

        v2_record = CanonicalRecordV2(
            canonical=canonical,
            aliases=aliases,
            tags=[group],
            classification=decision.classification,
            status=decision.status,
            confidence=decision.confidence,
        )
        v2_records.append(v2_record.to_dict())
        
        print(f">> LLM Response Received")
        print(f"  [Primary Type]: {decision.classification.get('primary_type')}")
        print(f"  [Ontological Nature]: {decision.classification.get('ontological_nature')}")
        print(f"  [Abstraction Level]: {decision.classification.get('abstraction_level')}")
        print(f"  [Confidence]: {confidence}")
        print(f"  [Status]: {status}")
        print("-" * 20)

    for row in canonical_rows[start_index:]:
        _process_single_canonical(row)

        processed_canonicals += 1
        should_checkpoint = checkpoint_every > 0
        if should_checkpoint and checkpoint_dir:
            if processed_canonicals % checkpoint_every == 0:
                print(f">> Saving checkpoint at {processed_canonicals} processed canonicals...")
                _write_stage4_checkpoint(
                    checkpoint_dir=checkpoint_dir,
                    processed_canonicals=processed_canonicals,
                    total_canonicals=total_canonicals,
                    classification_decisions=decisions,
                    v2_records=v2_records,
                    review_queue=review_queue,
                    findings=result.findings,
                )

    sorted_decisions = sorted(decisions, key=lambda item: str(item["canonical"]).lower())
    sorted_records = sorted(v2_records, key=lambda item: str(item["canonical"]).lower())

    review_queue_rows: List[Dict[str, object]] = []
    for entry in review_queue:
        review_queue_rows.append(entry.to_json_dict())

    result.payload["classification_decisions"] = sorted_decisions
    result.payload["v2_records"] = sorted_records
    result.payload["review_queue_entries"] = review_queue_rows
    return result


def _validate_classification_schema(classification: Dict[str, object], confidence: str) -> List[str]:
    parse_violations: List[str] = []

    ontological_nature = str(classification.get("ontological_nature", ""))
    abstraction_level = str(classification.get("abstraction_level", ""))
    primary_type = classification.get("primary_type")
    functional_roles = classification.get("functional_roles", [])

    if ontological_nature not in ALLOWED_ONTOLOGICAL_NATURE:
        parse_violations.append("invalid_ontological_nature")

    if abstraction_level not in ALLOWED_ABSTRACTION_LEVEL:
        parse_violations.append("invalid_abstraction_level")

    if confidence not in ALLOWED_PRIMARY_CONFIDENCE:
        parse_violations.append("invalid_confidence")

    valid_functional_roles = isinstance(functional_roles, list)
    if valid_functional_roles:
        for role in functional_roles:
            if not isinstance(role, str):
                valid_functional_roles = False
                break
    if not valid_functional_roles:
        parse_violations.append("invalid_functional_roles")

    if primary_type is not None and not isinstance(primary_type, str):
        parse_violations.append("invalid_primary_type")

    return parse_violations


def _handle_composite_stack(
    canonical: str,
    aliases: List[str],
    review_queue: List[ReviewQueueEntry],
    result: StageResult,
    decisions: List[Dict[str, object]],
) -> None:
    review_queue.append(
        ReviewQueueEntry(
            term=canonical,
            stage=4,
            issue="Composite stack rejected as canonical",
            proposed_action="EXPAND_REQUIRED",
            confidence="LOW",
        )
    )

    result.add_finding(
        create_finding(
            rule_id="L4-004",
            blocking=False,
            location=f"canonical:{canonical}",
            observed_value="COMPOSITE_STACK",
            normalized_value="",
            proposed_action="manual_review",
            reason="Composite stack terms are rejected as canonical and require expansion.",
        )
    )

    decisions.append(
        {
            "canonical": canonical,
            "aliases": aliases,
            "classification": {
                "ontological_nature": "Concept",
                "primary_type": None,
                "functional_roles": [],
                "abstraction_level": "Domain",
            },
            "status": "under_review",
            "confidence": "LOW",
            "is_contextual": False,
            "is_versioned": False,
            "is_marketing_language": False,
        }
    )


def _handle_category(
    canonical: str,
    review_queue: List[ReviewQueueEntry],
    result: StageResult,
) -> None:
    review_queue.append(
        ReviewQueueEntry(
            term=canonical,
            stage=4,
            issue="Category rejected as canonical",
            proposed_action="REJECT_AS_CANONICAL",
            confidence="LOW",
        )
    )

    result.add_finding(
        create_finding(
            rule_id="L4-005",
            blocking=False,
            location=f"canonical:{canonical}",
            observed_value="CATEGORY",
            normalized_value="",
            proposed_action="manual_review",
            reason="Category terms are rejected as canonical; retain as tags/domain only.",
        )
    )


def _write_stage4_checkpoint(
    checkpoint_dir: str,
    processed_canonicals: int,
    total_canonicals: int,
    classification_decisions: List[Dict[str, object]],
    v2_records: List[Dict[str, object]],
    review_queue: List[ReviewQueueEntry],
    findings: List[object],  # List[Finding]
) -> None:
    meta = {
        "processed_canonicals": processed_canonicals,
        "total_canonicals": total_canonicals,
    }
    write_json(os.path.join(checkpoint_dir, "stage4_checkpoint_meta.json"), meta)

    write_json(
        os.path.join(checkpoint_dir, "stage4_classification_decisions.partial.json"),
        classification_decisions,
    )
    write_json(
        os.path.join(checkpoint_dir, "stage4_v2_records.partial.json"),
        v2_records,
    )

    queue_rows = [entry.to_json_dict() for entry in review_queue]
    write_json(
        os.path.join(checkpoint_dir, "stage4_review_queue.partial.json"),
        queue_rows,
    )

    finding_rows = [f.to_dict() for f in findings]
    write_json(
        os.path.join(checkpoint_dir, "stage4_findings.partial.json"),
        finding_rows,
    )

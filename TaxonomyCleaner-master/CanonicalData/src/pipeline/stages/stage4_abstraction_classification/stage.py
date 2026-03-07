from __future__ import annotations

import os
from typing import Any
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


DOMAIN_CANONICAL_OVERRIDES = {
    "computer vision",
    "machine learning",
    "deep learning",
    "natural language processing",
    "data science",
    "data engineering",
    "artificial intelligence",
}


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

        response = _normalize_classification_response(canonical, response)

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


def _normalize_classification_response(
    canonical: str,
    response: Dict[str, object],
) -> Dict[str, object]:
    normalized = dict(response)
    key_map = _build_casefold_key_map(response)

    normalized_type = _normalize_special_type(key_map)
    if normalized_type is not None:
        normalized["type"] = normalized_type
        return normalized

    classification_payload = _normalize_classification_payload(canonical, response, key_map)
    normalized["classification"] = classification_payload

    confidence_raw = _read_casefold_value(
        key_map,
        [
            "confidence",
            "confidence_level",
            "classification_confidence",
            "term_confidence",
        ],
    )
    normalized["confidence"] = _normalize_confidence(confidence_raw)

    status_raw = _read_casefold_value(key_map, ["status"])
    normalized["status"] = _normalize_status(status_raw, str(normalized["confidence"]))

    contextual_raw = _read_casefold_value(key_map, ["is_contextual", "contextual"])
    normalized["is_contextual"] = _normalize_boolean(contextual_raw, default=False)

    versioned_raw = _read_casefold_value(key_map, ["is_versioned", "versioned"])
    normalized["is_versioned"] = _normalize_boolean(versioned_raw, default=False)

    marketing_raw = _read_casefold_value(
        key_map,
        [
            "is_marketing_language",
            "marketing_language",
            "is_marketing",
        ],
    )
    normalized["is_marketing_language"] = _normalize_boolean(marketing_raw, default=False)

    return normalized


def _build_casefold_key_map(payload: Dict[str, object]) -> Dict[str, object]:
    mapped: Dict[str, object] = {}

    for key, value in payload.items():
        key_text = str(key).strip().lower()
        if not key_text:
            continue
        mapped[key_text] = value

    return mapped


def _read_casefold_value(
    key_map: Dict[str, object],
    candidates: List[str],
) -> object:
    for key in candidates:
        candidate_key = str(key).strip().lower()
        if candidate_key in key_map:
            return key_map[candidate_key]
    return None


def _normalize_special_type(key_map: Dict[str, object]) -> str | None:
    special_type_value = _read_casefold_value(
        key_map,
        [
            "type",
            "special_type",
        ],
    )
    if not isinstance(special_type_value, str):
        return None

    normalized_type = special_type_value.strip().upper()
    if normalized_type in {"COMPOSITE_STACK", "CATEGORY"}:
        return normalized_type
    return None


def _normalize_classification_payload(
    canonical: str,
    response: Dict[str, object],
    key_map: Dict[str, object],
) -> Dict[str, object]:
    raw_classification = _read_casefold_value(key_map, ["classification"])
    inferred_primary_type = _extract_primary_type_from_payload(raw_classification)

    if inferred_primary_type is None:
        inferred_primary_type = _extract_primary_type_from_payload(
            _read_casefold_value(
                key_map,
                [
                    "classification_type",
                    "classification",
                    "classification_name",
                    "classification_label",
                    "primary_classification",
                    "sub_classification",
                    "subclassification",
                    "technology_type",
                    "type",
                    "category",
                    "subcategory",
                    "main_category",
                ],
            )
        )

    if inferred_primary_type is None:
        inferred_primary_type = _read_first_text_from_nested_values(
            _read_casefold_value(key_map, ["classification"])
        )

    if inferred_primary_type is None:
        inferred_primary_type = canonical

    inferred_ontological = _extract_ontological_nature(raw_classification)
    if inferred_ontological is None:
        inferred_ontological = _infer_ontological_nature(inferred_primary_type, canonical)

    inferred_abstraction = _extract_abstraction_level(raw_classification)
    if inferred_abstraction is None:
        inferred_abstraction = _infer_abstraction_level(inferred_ontological, inferred_primary_type, canonical)

    inferred_ontological, inferred_abstraction, inferred_primary_type = _apply_domain_overrides(
        canonical=canonical,
        primary_type=inferred_primary_type,
        ontological_nature=inferred_ontological,
        abstraction_level=inferred_abstraction,
    )

    functional_roles = _extract_functional_roles(raw_classification)
    if not functional_roles:
        functional_roles = _extract_functional_roles_from_response(response)

    payload = {
        "ontological_nature": inferred_ontological,
        "primary_type": inferred_primary_type,
        "functional_roles": functional_roles,
        "abstraction_level": inferred_abstraction,
    }
    return payload


def _extract_primary_type_from_payload(value: object) -> str | None:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
        return None

    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                cleaned = item.strip()
                if cleaned:
                    return cleaned
            if isinstance(item, dict):
                nested = _extract_primary_type_from_payload(item)
                if nested is not None:
                    return nested
        return None

    if isinstance(value, dict):
        key_order = [
            "primary_type",
            "type",
            "classification_type",
            "technology_type",
            "subcategory",
            "subclassification",
            "sub_classification",
            "category",
            "main_category",
            "field",
            "name",
            "label",
        ]
        casefold_map = _build_casefold_key_map(value)
        for key in key_order:
            candidate = _read_casefold_value(casefold_map, [key])
            extracted = _extract_primary_type_from_payload(candidate)
            if extracted is not None:
                return extracted

        return _read_first_text_from_nested_values(value)

    return None


def _read_first_text_from_nested_values(value: object) -> str | None:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
        return None

    if isinstance(value, list):
        for item in value:
            nested = _read_first_text_from_nested_values(item)
            if nested is not None:
                return nested
        return None

    if isinstance(value, dict):
        preferred_keys = [
            "field",
            "category",
            "subcategory",
            "type",
            "name",
            "label",
            "definition",
        ]
        casefold_map = _build_casefold_key_map(value)
        for key in preferred_keys:
            nested = _read_first_text_from_nested_values(_read_casefold_value(casefold_map, [key]))
            if nested is not None:
                return nested

        for nested_value in value.values():
            nested = _read_first_text_from_nested_values(nested_value)
            if nested is not None:
                return nested
        return None

    return None


def _extract_ontological_nature(classification_value: object) -> str | None:
    if not isinstance(classification_value, dict):
        return None

    casefold_map = _build_casefold_key_map(classification_value)
    raw = _read_casefold_value(
        casefold_map,
        [
            "ontological_nature",
            "ontological nature",
        ],
    )
    if not isinstance(raw, str):
        return None

    normalized = _map_ontological_nature_from_text(raw)
    return normalized


def _extract_abstraction_level(classification_value: object) -> str | None:
    if not isinstance(classification_value, dict):
        return None

    casefold_map = _build_casefold_key_map(classification_value)
    raw = _read_casefold_value(
        casefold_map,
        [
            "abstraction_level",
            "abstraction level",
        ],
    )
    if not isinstance(raw, str):
        return None

    normalized = _map_abstraction_level_from_text(raw)
    return normalized


def _extract_functional_roles(classification_value: object) -> List[str]:
    if not isinstance(classification_value, dict):
        return []

    casefold_map = _build_casefold_key_map(classification_value)
    raw_roles = _read_casefold_value(casefold_map, ["functional_roles", "functional roles"])
    if not isinstance(raw_roles, list):
        return []

    roles: List[str] = []
    seen: set[str] = set()
    for role in raw_roles:
        if not isinstance(role, str):
            continue
        cleaned = role.strip()
        if not cleaned:
            continue
        dedupe_key = cleaned.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        roles.append(cleaned)

    return roles


def _extract_functional_roles_from_response(response: Dict[str, object]) -> List[str]:
    key_map = _build_casefold_key_map(response)
    role_sources = [
        "related_terms",
        "related_technologies",
        "use_cases",
        "examples",
    ]

    roles: List[str] = []
    seen: set[str] = set()
    for key in role_sources:
        raw = _read_casefold_value(key_map, [key])
        if not isinstance(raw, list):
            continue

        for item in raw:
            if not isinstance(item, str):
                continue
            cleaned = item.strip()
            if not cleaned:
                continue
            dedupe_key = cleaned.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            roles.append(cleaned)

            if len(roles) >= 8:
                return roles

    return roles


def _infer_ontological_nature(primary_type: str, canonical: str) -> str:
    text = f"{primary_type} {canonical}".lower()

    protocol_markers = [
        "protocol",
        "http",
        "https",
        "tcp",
        "udp",
        "grpc",
        "mqtt",
        "dns",
        "smtp",
        "imap",
        "ldap",
        "oauth",
        "openid",
    ]
    for marker in protocol_markers:
        if marker in text:
            return "Protocol"

    standard_markers = [
        "standard",
        "specification",
        "spec",
        "rfc",
        "iso",
        "ieee",
    ]
    for marker in standard_markers:
        if marker in text:
            return "Standard / Specification"

    algorithm_markers = [
        "algorithm",
        "search",
        "sort",
        "clustering",
        "regression",
        "classification",
        "optimization",
        "pathfinding",
        "gradient",
    ]
    for marker in algorithm_markers:
        if marker in text:
            return "Algorithm"

    skill_markers = [
        "leadership",
        "communication",
        "mentoring",
        "collaboration",
        "negotiation",
        "presentation",
        "stakeholder",
    ]
    for marker in skill_markers:
        if marker in text:
            return "Human Skill"

    concept_markers = [
        "concept",
        "methodology",
        "workflow",
        "process",
        "practice",
        "strategy",
        "governance",
        "analysis",
    ]
    for marker in concept_markers:
        if marker in text:
            return "Concept"

    return "Software Artifact"


def _infer_abstraction_level(
    ontological_nature: str,
    primary_type: str,
    canonical: str,
) -> str:
    if ontological_nature in {"Algorithm", "Standard / Specification", "Protocol"}:
        return "Method"

    if ontological_nature in {"Concept", "Human Skill"}:
        return "Domain"

    text = f"{primary_type} {canonical}".lower()
    concrete_markers = [
        "framework",
        "library",
        "sdk",
        "api",
        "tool",
        "platform",
        "service",
        "database",
        "runtime",
        "engine",
    ]
    for marker in concrete_markers:
        if marker in text:
            return "Concrete"

    method_markers = [
        "workflow",
        "method",
        "practice",
        "pattern",
    ]
    for marker in method_markers:
        if marker in text:
            return "Method"

    return "Concrete"


def _apply_domain_overrides(
    canonical: str,
    primary_type: str,
    ontological_nature: str,
    abstraction_level: str,
) -> tuple[str, str, str]:
    canonical_normalized = canonical.strip().lower()
    if canonical_normalized in DOMAIN_CANONICAL_OVERRIDES:
        return "Concept", "Domain", canonical

    primary_type_normalized = primary_type.strip().lower()
    if primary_type_normalized in {"technology", "general"}:
        if canonical_normalized.endswith("vision"):
            return "Concept", "Domain", canonical

    return ontological_nature, abstraction_level, primary_type


def _map_ontological_nature_from_text(value: str) -> str:
    cleaned = value.strip().lower()
    if cleaned == "software artifact":
        return "Software Artifact"
    if cleaned == "algorithm":
        return "Algorithm"
    if cleaned == "standard / specification":
        return "Standard / Specification"
    if cleaned == "protocol":
        return "Protocol"
    if cleaned == "concept":
        return "Concept"
    if cleaned == "human skill":
        return "Human Skill"

    if "standard" in cleaned or "specification" in cleaned:
        return "Standard / Specification"
    if "protocol" in cleaned:
        return "Protocol"
    if "algorithm" in cleaned:
        return "Algorithm"
    if "skill" in cleaned:
        return "Human Skill"
    if "concept" in cleaned or "category" in cleaned or "domain" in cleaned:
        return "Concept"
    return "Software Artifact"


def _map_abstraction_level_from_text(value: str) -> str:
    cleaned = value.strip().lower()
    if cleaned == "domain":
        return "Domain"
    if cleaned == "method":
        return "Method"
    if cleaned == "concrete":
        return "Concrete"

    if "domain" in cleaned or "category" in cleaned:
        return "Domain"
    if "method" in cleaned or "process" in cleaned:
        return "Method"
    return "Concrete"


def _normalize_confidence(value: object) -> str:
    if isinstance(value, str):
        cleaned = value.strip().upper()
        if cleaned in ALLOWED_PRIMARY_CONFIDENCE:
            return cleaned

        if "HIGH" in cleaned:
            return "HIGH"
        if "MEDIUM" in cleaned:
            return "MEDIUM"
        if "LOW" in cleaned:
            return "LOW"

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        score = float(value)
        if score >= 0.80:
            return "HIGH"
        if score >= 0.50:
            return "MEDIUM"
        return "LOW"

    return "MEDIUM"


def _normalize_status(value: object, confidence: str) -> str:
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned == "under_review":
            return "under_review"
        if cleaned == "active":
            return "active"

    if confidence == "LOW":
        return "under_review"
    return "active"


def _normalize_boolean(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)

    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned in {"true", "yes", "1", "y"}:
            return True
        if cleaned in {"false", "no", "0", "n"}:
            return False

    return default


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

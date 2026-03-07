from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"

CONFIDENCE_HIGH = "HIGH"
CONFIDENCE_MEDIUM = "MEDIUM"
CONFIDENCE_LOW = "LOW"

ALLOWED_ARBITRATION_ACTIONS = {
    "MERGE_AS_ALIAS",
    "KEEP_DISTINCT",
    "MARK_AS_CONTEXTUAL",
    "SPLIT_INTO_MULTIPLE_CANONICALS",
    "REMOVE_CANONICAL",
}

ALLOWED_PRIMARY_CONFIDENCE = {
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_LOW,
}

ALLOWED_ONTOLOGICAL_NATURE = {
    "Software Artifact",
    "Algorithm",
    "Standard / Specification",
    "Protocol",
    "Concept",
    "Human Skill",
}

ALLOWED_ABSTRACTION_LEVEL = {
    "Domain",
    "Method",
    "Concrete",
}


@dataclass
class Finding:
    rule_id: str
    severity: str
    blocking: bool
    location: str
    observed_value: str
    normalized_value: str
    proposed_action: str
    proposed_payload: Dict[str, Any]
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationReport:
    source_path: str
    source_hash: str
    summary: Dict[str, int]
    findings: List[Finding]

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        payload["source_path"] = self.source_path
        payload["source_hash"] = self.source_hash
        payload["summary"] = self.summary

        payload_findings: List[Dict[str, Any]] = []
        for finding in self.findings:
            payload_findings.append(finding.to_dict())
        payload["findings"] = payload_findings

        return payload


@dataclass
class SimilarityEdge:
    left: str
    right: str
    score: float
    band: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ConflictCluster:
    cluster_id: str
    terms: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ArbitrationDecision:
    term: str
    action: str
    target_canonical: Optional[str]
    split_candidates: Optional[List[str]]
    reasoning: Dict[str, str]
    confidence: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class GovernedArbitrationDecision:
    term: str
    requested_action: str
    effective_action: str
    target_canonical: Optional[str]
    split_candidates: Optional[List[str]]
    confidence: str
    blocked: bool
    violations: List[str] = field(default_factory=list)
    reasoning: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ClassificationDecision:
    canonical: str
    aliases: List[str]
    classification: Dict[str, Any]
    status: str
    confidence: str
    is_contextual: bool
    is_versioned: bool
    is_marketing_language: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ReviewQueueEntry:
    term: str
    stage: int
    issue: str
    proposed_action: str
    confidence: str

    def to_json_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CanonicalRecordV2:
    canonical: str
    aliases: List[str]
    tags: List[str]
    classification: Dict[str, Any]
    status: str
    confidence: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StageResult:
    findings: List[Finding] = field(default_factory=list)
    blocking_error: bool = False
    payload: Dict[str, Any] = field(default_factory=dict)
    parse_error: bool = False

    def add_finding(self, finding: Finding) -> None:
        self.findings.append(finding)
        if finding.blocking:
            self.blocking_error = True


@dataclass
class RunArtifacts:
    validation_report_json: str
    arbitration_decisions_json: str
    classification_decisions_json: str
    review_queue_jsonl: str
    proposed_changes_json: str
    proposed_changes_md: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

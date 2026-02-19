from __future__ import annotations

from typing import Dict
from typing import List


def build_proposed_changes(
    validation_report: Dict[str, object],
    arbitration_decisions: List[Dict[str, object]],
    classification_decisions: List[Dict[str, object]],
    graph_findings: List[Dict[str, object]],
    review_queue: List[Dict[str, object]],
) -> Dict[str, object]:
    blocking_findings = _collect_blocking_findings(validation_report)
    arbitration_review = _collect_arbitration_review(arbitration_decisions)
    classification_review = _collect_classification_review(classification_decisions)

    proposed = {
        "summary": {
            "blocking_findings": len(blocking_findings),
            "arbitration_review_items": len(arbitration_review),
            "classification_review_items": len(classification_review),
            "graph_findings": len(graph_findings),
            "review_queue_items": len(review_queue),
        },
        "blocking_findings": sorted(
            blocking_findings,
            key=lambda item: (
                str(item.get("rule_id", "")),
                str(item.get("location", "")),
            ),
        ),
        "arbitration_decisions": sorted(
            arbitration_review,
            key=lambda item: (
                str(item.get("term", "")),
                str(item.get("requested_action", "")),
            ),
        ),
        "classification_decisions": sorted(
            classification_review,
            key=lambda item: str(item.get("canonical", "")),
        ),
        "graph_findings": sorted(
            graph_findings,
            key=lambda item: (
                str(item.get("rule_id", "")),
                str(item.get("location", "")),
            ),
        ),
        "review_queue": sorted(
            review_queue,
            key=lambda item: (
                int(item.get("stage", 0)),
                str(item.get("term", "")),
            ),
        ),
    }
    return proposed


def _collect_blocking_findings(validation_report: Dict[str, object]) -> List[Dict[str, object]]:
    findings = validation_report.get("findings", [])
    if not isinstance(findings, list):
        return []

    blocking_findings: List[Dict[str, object]] = []
    for finding in findings:
        if not isinstance(finding, dict):
            continue

        if bool(finding.get("blocking", False)):
            blocking_findings.append(finding)

    return blocking_findings


def _collect_arbitration_review(arbitration_decisions: List[Dict[str, object]]) -> List[Dict[str, object]]:
    review_items: List[Dict[str, object]] = []

    for decision in arbitration_decisions:
        blocked = bool(decision.get("blocked", False))
        effective_action = decision.get("effective_action")
        requested_action = decision.get("requested_action")

        changed_by_governance = effective_action != requested_action
        if blocked or changed_by_governance:
            review_items.append(decision)

    return review_items


def _collect_classification_review(
    classification_decisions: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    review_items: List[Dict[str, object]] = []

    for decision in classification_decisions:
        status = decision.get("status")
        if status == "under_review":
            review_items.append(decision)

    return review_items


def render_markdown_diff(
    validation_report: Dict[str, object],
    proposed_changes: Dict[str, object],
) -> str:
    summary = proposed_changes.get("summary", {})
    lines: List[str] = []

    lines.append("# Governed Pipeline Diff Report")
    lines.append("")

    lines.append("## Source")
    lines.append("")
    lines.append(f"- Path: `{validation_report.get('source_path', '')}`")
    lines.append(f"- Hash: `{validation_report.get('source_hash', '')}`")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Blocking findings: {summary.get('blocking_findings', 0)}")
    lines.append(f"- Arbitration review items: {summary.get('arbitration_review_items', 0)}")
    lines.append(f"- Classification review items: {summary.get('classification_review_items', 0)}")
    lines.append(f"- Graph findings: {summary.get('graph_findings', 0)}")
    lines.append(f"- Review queue items: {summary.get('review_queue_items', 0)}")
    lines.append("")

    lines.extend(_rule_legend_lines())
    lines.extend(_section_lines("Blocking Findings", proposed_changes.get("blocking_findings", []), _format_blocking))
    lines.extend(
        _section_lines(
            "Arbitration Review",
            proposed_changes.get("arbitration_decisions", []),
            _format_arbitration,
        )
    )
    lines.extend(
        _section_lines(
            "Classification Under Review",
            proposed_changes.get("classification_decisions", []),
            _format_classification,
        )
    )
    lines.extend(_section_lines("Graph Findings", proposed_changes.get("graph_findings", []), _format_graph))
    lines.extend(_section_lines("Manual Review Queue", proposed_changes.get("review_queue", []), _format_review))

    lines.append("")
    lines.append("No source-of-truth file mutations were applied. All changes are advisory pending approval.")

    return "\n".join(lines)


def _rule_legend_lines() -> List[str]:
    lines: List[str] = []
    lines.append("## Rule Legend")
    lines.append("")
    lines.append("Rule IDs follow `L<layer>-<rule_number>`. Key families:")
    lines.append("- `L1-*` deterministic pre-clean integrity rules")
    lines.append("- `L3-*` arbitration schema and governance rules")
    lines.append("- `L4-*` classification schema and governance rules")
    lines.append("- `L5-*` graph validation rules")
    lines.append("")
    lines.append("Common codes:")
    lines.append("- `L1-001` schema/type integrity")
    lines.append("- `L1-002` canonical duplicate after normalization")
    lines.append("- `L1-003` alias collides with canonical")
    lines.append("- `L1-004` alias maps to multiple canonicals")
    lines.append("- `L1-005` group name collides with canonical")
    lines.append("- `L1-006` canonical atomicity violation")
    lines.append("- `L1-007` alias version-token warning")
    lines.append("- `L1-008` alias not safely interchangeable")
    lines.append("- `L1-009` duplicate alias in same canonical")
    lines.append("- `L1-010` no deterministic rewrite; manual review")
    lines.append("- `L3-001` invalid arbitration response schema")
    lines.append("- `L3-002` arbitration decision failed deterministic governance")
    lines.append("- `L4-001` invalid classification response envelope")
    lines.append("- `L4-002` classification decision failed schema validation")
    lines.append("- `L4-003` LOW-confidence classification containment warning")
    lines.append("- `L4-004` composite stack rejected as canonical")
    lines.append("- `L4-005` category rejected as canonical")
    lines.append("- `L5-001` over-generic node detection")
    lines.append("- `L5-002` phantom node detection")
    lines.append("- `L5-003` embedding/graph/classification disagreement")
    lines.append("")
    return lines


def _section_lines(title: str, rows: object, formatter) -> List[str]:
    lines: List[str] = []
    lines.append(f"## {title}")
    lines.append("")

    if not isinstance(rows, list) or not rows:
        lines.append("- None")
        lines.append("")
        return lines

    for row in rows:
        if not isinstance(row, dict):
            continue
        lines.append(formatter(row))

    lines.append("")
    return lines


def _format_blocking(item: Dict[str, object]) -> str:
    rule_id = str(item.get("rule_id", ""))
    location = str(item.get("location", ""))
    reason = str(item.get("reason", ""))
    return f"- `{rule_id}` {location} -> {reason}"


def _format_arbitration(item: Dict[str, object]) -> str:
    term = str(item.get("term", ""))
    requested_action = str(item.get("requested_action", ""))
    effective_action = str(item.get("effective_action", ""))

    violations_text = ""
    violations = item.get("violations", [])
    if isinstance(violations, list) and violations:
        joined = ", ".join(str(v) for v in violations)
        violations_text = f" (violations: {joined})"

    return (
        f"- `{term}` requested `{requested_action}`, "
        f"effective `{effective_action}`{violations_text}"
    )


def _format_classification(item: Dict[str, object]) -> str:
    canonical = str(item.get("canonical", ""))
    confidence = str(item.get("confidence", ""))
    return f"- `{canonical}` confidence `{confidence}`"


def _format_graph(item: Dict[str, object]) -> str:
    rule_id = str(item.get("rule_id", ""))
    location = str(item.get("location", ""))
    reason = str(item.get("reason", ""))
    return f"- `{rule_id}` {location} -> {reason}"


def _format_review(item: Dict[str, object]) -> str:
    stage = str(item.get("stage", ""))
    term = str(item.get("term", ""))
    issue = str(item.get("issue", ""))
    return f"- Stage {stage}: `{term}` -> {issue}"

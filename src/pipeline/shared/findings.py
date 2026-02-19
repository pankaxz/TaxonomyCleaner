from __future__ import annotations

from typing import Any
from typing import Dict

from .models import Finding


def create_finding(
    rule_id: str,
    blocking: bool,
    location: str,
    observed_value: str,
    normalized_value: str,
    proposed_action: str,
    reason: str,
    proposed_payload: Dict[str, Any] | None = None,
) -> Finding:
    severity = "warning"
    if blocking:
        severity = "error"

    payload: Dict[str, Any] = {}
    if proposed_payload is not None:
        payload = proposed_payload

    finding = Finding(
        rule_id=rule_id,
        severity=severity,
        blocking=blocking,
        location=location,
        observed_value=observed_value,
        normalized_value=normalized_value,
        proposed_action=proposed_action,
        proposed_payload=payload,
        reason=reason,
    )
    return finding

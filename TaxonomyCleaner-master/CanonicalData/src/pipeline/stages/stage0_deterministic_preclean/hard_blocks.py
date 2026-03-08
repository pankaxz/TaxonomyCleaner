"""Merge audit findings into the hard_block_alias_pairs.json file.

Reads new hard-block findings from artifacts/audit_results/hard_blocks_latest.json,
loads the existing pairs file, merges (deduplicates), and writes back as a single
valid JSON object.
"""
import os
import json
from typing import Dict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up 4 levels: stage0 -> stages -> pipeline -> src -> CanonicalData
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../../.."))
INPUT_FILE = os.path.join(PROJECT_ROOT, "artifacts/audit_results/hard_blocks_latest.json")
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "Input/hard_block_alias_pairs.json")


def load_existing_pairs(path: str) -> Dict[str, str]:
    """Load existing hard block pairs, returning empty dict if missing/corrupt."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (json.JSONDecodeError, OSError) as e:
        print(f"Warning: could not read existing pairs at {path}: {e}")
    return {}


def extract_pairs_from_findings(findings: list) -> Dict[str, str]:
    """Extract canonical→alias pairs from audit findings."""
    pairs: Dict[str, str] = {}
    for finding in findings:
        canonical = finding.get("canonical", "")
        alias = finding.get("alias", "")
        if canonical and alias:
            pairs[canonical] = alias
    return pairs


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file not found at {INPUT_FILE}")
        return

    # Load new findings
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    findings = data.get("findings", [])
    new_pairs = extract_pairs_from_findings(findings)

    if not new_pairs:
        print("No new hard block pairs found in audit results.")
        return

    # Load existing pairs
    existing = load_existing_pairs(OUTPUT_FILE)
    before_count = len(existing)

    # Merge: new pairs override existing on key collision
    existing.update(new_pairs)
    after_count = len(existing)

    # Write back as single valid JSON object
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)
        f.write("\n")

    added = after_count - before_count
    print(f"Merged {len(new_pairs)} new pairs into {OUTPUT_FILE}")
    print(f"  Before: {before_count} pairs")
    print(f"  After:  {after_count} pairs ({added} net new)")


if __name__ == "__main__":
    main()


import sys
import os

# Add the project root to sys.path
sys.path.append(os.getcwd())

from src.pipeline.stages.stage0_deterministic_preclean.stage import run_stage0
from src.pipeline.stages.stage0_deterministic_preclean.stage import validate_schema

def test_alias_atomicity():
    print("Testing Alias Atomicity Check...")
    
    # Setup mock data with a violation
    # "React/Angular" as an alias is an atomicity violator (slash)
    mock_store = {
        "WEb Ecosystems": {
            "UI Frameworks": ["React/Angular"]
        }
    }
    
    source_path = "test_source.json"
    source_hash = "abc12345"
    atomicity_exceptions = set()

    # Run stage 0
    try:
        result = run_stage0(
            store=mock_store,
            source_path=source_path,
            source_hash=source_hash,
            atomicity_exceptions=atomicity_exceptions
        )
    except Exception as e:
        print(f"FAILED: execution error: {e}")
        import traceback
        traceback.print_exc()
        return

    # Check findings
    findings = result.findings
    found_violation = False
    
    for f in findings:
        print(f"Finding: {f.rule_id} at {f.location} - {f.reason}")
        if f.rule_id == "L1-006" and f.observed_value == "React/Angular":
            found_violation = True
            print("  -> SUCCESS: Found expected atomicity violation for alias.")
            if f.proposed_action == "split_on_slash":
                 print("  -> SUCCESS: Proposed action is split_on_slash.")

    if not found_violation:
        print("FAILED: Did not find L1-006 violation for 'Java/Kotlin' alias.")
    else:
        print("TEST PASSED.")

if __name__ == "__main__":
    test_alias_atomicity()

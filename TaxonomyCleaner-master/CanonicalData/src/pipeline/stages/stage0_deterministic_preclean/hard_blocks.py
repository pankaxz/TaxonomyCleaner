# Paths relative to the script location (assuming src/agents/audit_hard_blocks_agent.py)
# We need to resolve these to absolute paths or relative to the project root.
import os
import json
from typing import Any, Dict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up 4 levels: stage0 -> stages -> pipeline -> src -> CanonicalData
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../../.."))
INPUT_FILE = os.path.join(PROJECT_ROOT, "artifacts/audit_results/hard_blocks_latest.json")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "Input/hard_block_alias_pairs.json")

def iterate_over_hard_block_file(store: Any, pair: Dict[str, str] = {}):
    for finding in store:
        pair.update({finding["canonical"]: finding["alias"]})

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file not found at {INPUT_FILE}")
        return

    try:
        with open(INPUT_FILE, 'r') as f:
            data = json.load(f)
            findings = data.get("findings", [])
            pair: Dict[str, str] = {}
            iterate_over_hard_block_file(findings, pair)

            with open(OUTPUT_DIR, "a", encoding="utf-8") as f:
                f.write(json.dumps(pair, ensure_ascii=False) + "\n")


    except Exception as e:
        print(f"Error processing hard blocks: {e}")

if __name__ == "__main__":
    import json
    main()


import argparse
import hashlib
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

from openai import OpenAI

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
DEFAULT_LLM_BASE_URL = "http://localhost:8002/"
DEFAULT_LLM_API_KEY = "sk-no-key-required"
DEFAULT_MODEL_NAME = "deepseek-r1-32b"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../.."))
INPUT_FILE = os.path.join(PROJECT_ROOT, "Input/canonical_data.json")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "artifacts/audit_results")

BRAIN_PROMPT_PATH = "/home/pankaj/.gemini/antigravity/brain/26ff4e9b-828c-4763-8bb0-a7f0aef3d70f/prompts/hard_block_detection_prompt.md"

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def hash_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def get_system_prompt() -> str:
    if os.path.exists(BRAIN_PROMPT_PATH):
        with open(BRAIN_PROMPT_PATH, "r", encoding="utf-8") as f:
            return f.read()
    else:
        print(f"Warning: Prompt file not found at {BRAIN_PROMPT_PATH}. Using fallback.")
        return "You are a helpful assistant."

def normalize_term(term: str) -> str:
    return term.lower().strip()


# -----------------------------------------------------------------------------
# Checkpoint
# -----------------------------------------------------------------------------
class Checkpoint:
    """Tracks which (group, canonical) pairs have been processed.

    Persists to a JSON file alongside the audit output. Validates that the
    input file hasn't changed since the checkpoint was created (via SHA-256).
    """

    def __init__(self, output_dir: str, input_path: str):
        self.path = os.path.join(output_dir, "checkpoint.json")
        self.input_path = input_path
        self.input_hash = hash_file(input_path)
        self.processed: Set[Tuple[str, str]] = set()
        self.findings: List[Dict[str, Any]] = []
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            data = load_json(self.path)
        except (json.JSONDecodeError, OSError):
            print("Warning: corrupt checkpoint file, starting fresh.")
            return

        saved_hash = data.get("input_hash", "")
        if saved_hash != self.input_hash:
            print(f"Input file changed (hash mismatch). Discarding old checkpoint.")
            # Also remove stale audit_log.jsonl so we don't mix results
            log_path = os.path.join(os.path.dirname(self.path), "audit_log.jsonl")
            if os.path.exists(log_path):
                os.remove(log_path)
            return

        for key in data.get("processed", []):
            parts = key.split("::", 1)
            if len(parts) == 2:
                self.processed.add((parts[0], parts[1]))

        self.findings = data.get("findings", [])
        print(f"Checkpoint loaded: {len(self.processed)} processed, {len(self.findings)} findings.")

    def save(self) -> None:
        data = {
            "input_path": self.input_path,
            "input_hash": self.input_hash,
            "updated_at": datetime.now().isoformat(),
            "processed": sorted(f"{g}::{c}" for g, c in self.processed),
            "findings": self.findings,
        }
        save_json(self.path, data)

    def is_done(self, group: str, canonical: str) -> bool:
        return (group, canonical) in self.processed

    def mark_done(self, group: str, canonical: str) -> None:
        self.processed.add((group, canonical))

    def add_findings(self, entries: List[Dict[str, Any]]) -> None:
        self.findings.extend(entries)


# -----------------------------------------------------------------------------
# Agent Class
# -----------------------------------------------------------------------------
class HardBlockAuditorAgent:
    def __init__(self, base_url: str, api_key: str, model: str, checkpoint: Checkpoint):
        self.client = OpenAI(base_url=base_url, api_key=api_key)
        self.model = model
        self.system_prompt = get_system_prompt().replace("{{INPUT_JSON}}", "")
        self.checkpoint = checkpoint
        self.output_dir = os.path.dirname(checkpoint.path)

    def audit_canonical(self, group: str, canonical: str, aliases: List[str]) -> None:
        if not aliases:
            self.checkpoint.mark_done(group, canonical)
            return

        user_payload = {"canonical": canonical, "aliases": aliases}
        user_message = json.dumps(user_payload, indent=2)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_message},
                ],
                temperature=0.0,
                max_tokens=1024,
            )
            content = response.choices[0].message.content
            self._process_response(group, canonical, content)

        except Exception as e:
            print(f"  Error processing {canonical}: {e}")
            return  # Don't mark as done so it retries on next run

        # Mark done and save checkpoint after successful processing
        self.checkpoint.mark_done(group, canonical)
        self.checkpoint.save()

    def _process_response(self, group: str, canonical: str, content: str) -> None:
        json_match = re.search(r'(\{.*\})', content, re.DOTALL)

        json_str = ""
        if json_match:
            json_str = json_match.group(1)
        else:
            clean_content = content.strip()
            if clean_content.startswith("```json"):
                clean_content = clean_content[7:]
            if clean_content.startswith("```"):
                clean_content = clean_content[3:]
            if clean_content.endswith("```"):
                clean_content = clean_content[:-3]
            json_str = clean_content.strip()

        try:
            data = json.loads(json_str)
            hard_blocks = data.get("hard_blocks", [])

            log_entry = {
                "group": group,
                "canonical": canonical,
                "findings": [],
                "timestamp": datetime.now().isoformat(),
            }

            if hard_blocks:
                print(f"  FOUND BLOCK for '{canonical}': {len(hard_blocks)} items")

                new_findings = []
                for block in hard_blocks:
                    finding = {
                        "alias": block.get("alias"),
                        "reason": block.get("reason"),
                        "confidence_score": block.get("confidence_score", 0.0),
                        "timestamp": datetime.now().isoformat(),
                    }
                    log_entry["findings"].append(finding)
                    new_findings.append({
                        "group": group,
                        "canonical": canonical,
                        **finding,
                    })

                self.checkpoint.add_findings(new_findings)

            # Append to incremental JSONL log
            log_path = os.path.join(self.output_dir, "audit_log.jsonl")
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

            # Update the latest report
            self._save_report()

        except json.JSONDecodeError:
            print(f"  Failed to parse JSON response for {canonical}: {content[:100]}...")

    def _save_report(self) -> None:
        latest_path = os.path.join(self.output_dir, "hard_blocks_latest.json")
        save_json(latest_path, {"findings": self.checkpoint.findings})


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Audit canonical entries for hard block pairs")
    parser.add_argument(
        "--input",
        default=INPUT_FILE,
        help="Path to canonical data JSON file to audit.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Discard existing checkpoint and start fresh.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    input_file = args.input

    print(f"Starting Hard Block Auditor...")
    print(f"Input: {input_file}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"Model: {DEFAULT_MODEL_NAME}")

    if not os.path.exists(input_file):
        print(f"Error: Input file not found at {input_file}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Handle --reset
    if args.reset:
        ckpt_path = os.path.join(OUTPUT_DIR, "checkpoint.json")
        log_path = os.path.join(OUTPUT_DIR, "audit_log.jsonl")
        for p in (ckpt_path, log_path):
            if os.path.exists(p):
                os.remove(p)
                print(f"Removed {p}")
        print("Checkpoint reset.")

    checkpoint = Checkpoint(OUTPUT_DIR, input_file)
    data = load_json(input_file)
    agent = HardBlockAuditorAgent(
        DEFAULT_LLM_BASE_URL, DEFAULT_LLM_API_KEY, DEFAULT_MODEL_NAME, checkpoint
    )

    # Count total work items (canonicals with aliases)
    work_items: List[Tuple[str, str, List[str]]] = []
    for group, group_data in data.items():
        if not isinstance(group_data, dict):
            continue
        for canonical, aliases in group_data.items():
            if not isinstance(aliases, list) or not aliases:
                continue
            work_items.append((group, canonical, aliases))

    total = len(work_items)
    already_done = sum(1 for g, c, _ in work_items if checkpoint.is_done(g, c))
    remaining = total - already_done

    print(f"Total canonicals with aliases: {total}")
    print(f"Already processed:             {already_done}")
    print(f"Remaining:                     {remaining}")

    if remaining == 0:
        print("Nothing to do — all entries already processed.")
        return

    processed_count = 0
    for group, canonical, aliases in work_items:
        if checkpoint.is_done(group, canonical):
            continue

        processed_count += 1
        print(f"[{processed_count}/{remaining}] Auditing: {canonical} ({len(aliases)} aliases)")
        agent.audit_canonical(group, canonical, aliases)

    print(f"\nDone. Total findings: {len(checkpoint.findings)}")

if __name__ == "__main__":
    main()

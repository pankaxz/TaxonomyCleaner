
import json
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Set
from openai import OpenAI
import argparse

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
DEFAULT_LLM_BASE_URL = "http://localhost:8080/v1"
DEFAULT_LLM_API_KEY = "sk-no-key-required"
DEFAULT_MODEL_NAME = "DeepSeek-R1-Distill-Qwen-32B-Q3_K_M.gguf"

# Paths relative to the script location (assuming src/agents/audit_hard_blocks_agent.py)
# We need to resolve these to absolute paths or relative to the project root.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../.."))
INPUT_FILE = os.path.join(PROJECT_ROOT, "Input/canonical_data.json")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "artifacts/audit_results")

# We need to point to the prompt file we created. 
# It was saved to /home/pankaj/.gemini/antigravity/brain/26ff4e9b-828c-4763-8bb0-a7f0aef3d70f/prompts/hard_block_detection_prompt.md
# But for the agent to be portable, ideally it should be in the repo.
# For now, I will use the absolute path from the artifact location, or I should copy it to the repo.
# Let's assume I should copy it to `artifacts/prompts/` in the repo if it's not there.
# I will use a hardcoded path for now based on where I know I saved it in brain, 
# but also try to look in the local project artifacts.

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

def get_system_prompt() -> str:
    if os.path.exists(BRAIN_PROMPT_PATH):
        with open(BRAIN_PROMPT_PATH, "r", encoding="utf-8") as f:
            return f.read()
    else:
        print(f"Warning: Prompt file not found at {BRAIN_PROMPT_PATH}. Using fallback.")
        return "You are a helpful assistant." # Fallback (should not happen in this flow)

def normalize_term(term: str) -> str:
    return term.lower().strip()

# -----------------------------------------------------------------------------
# Agent Class
# -----------------------------------------------------------------------------
class HardBlockAuditorAgent:
    def __init__(self, base_url: str, api_key: str, model: str):
        self.client = OpenAI(base_url=base_url, api_key=api_key)
        self.model = model
        self.system_prompt = get_system_prompt()
        self.findings: List[Dict[str, Any]] = []

    def audit_canonical(self, group: str, canonical: str, aliases: List[str]) -> None:
        if not aliases:
            return

        # Construct User Message
        user_payload = {
            "canonical": canonical,
            "aliases": aliases
        }
        user_message = json.dumps(user_payload, indent=2)

        # Prompt with template replacement if needed, 
        # but the prompt says "{{INPUT_JSON}}", so let's do that replacement manually
        # or just append it.
        # The prompt file instructions say: "Analyze the following Canonical and Alias list: {{INPUT_JSON}}"
        # So we should replace that placeholder.
        
        final_user_content = user_message
        final_system_prompt = self.system_prompt.replace("{{INPUT_JSON}}", "") # We put payload in user message usually, but let's follow the prompt style.
        # Actually, the prompt ends with "{{INPUT_JSON}}". It's better to put the JSON *in* the user message 
        # basically saying "Here is the input: ..."
        
        # Strategy: 
        # System Message: The definitions and rules.
        # User Message: The specific JSON input.
        
        # Let's strip the {{INPUT_JSON}} from the system prompt and append it to user message context if needed.
        # Or just use the system prompt as is (minus the placeholder) and send the JSON as user message.
        
        cleaned_system_prompt = self.system_prompt.replace("{{INPUT_JSON}}", "")

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": cleaned_system_prompt},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.0, # Deterministic
                max_tokens=1024,
            )

            content = response.choices[0].message.content
            self._process_response(group, canonical, content)

        except Exception as e:
            print(f"Error processing {canonical}: {e}")

    def _process_response(self, group: str, canonical: str, content: str) -> None:
        import re
        
        # Regex to find JSON object { ... }
        # Uses dotall flag to capture across newlines
        # Looks for the first opening brace and the last closing brace
        json_match = re.search(r'(\{.*\})', content, re.DOTALL)
        
        json_str = ""
        if json_match:
            json_str = json_match.group(1)
        else:
            # Fallback: try stripping markdown code blocks if regex failed 
            # (though regex should catch it if it's inside code blocks tools)
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
            
            if hard_blocks:
                print(f"FOUND BLOCK for '{canonical}': {len(hard_blocks)} items")
                
                # Prepare the findings for this canonical
                canonical_findings = {
                    "group": group,
                    "canonical": canonical,
                    "findings": []
                }
                
                for block in hard_blocks:
                    finding_entry = {
                        "alias": block.get("alias"),
                        "reason": block.get("reason"),
                        "confidence_score": block.get("confidence_score", 0.0), # Default to 0.0 if missing
                        "timestamp": datetime.now().isoformat()
                    }
                    canonical_findings["findings"].append(finding_entry)
                    
                    # Also keep in memory if we want a summary later
                    self.findings.append({
                        "group": group,
                        "canonical": canonical,
                        **finding_entry
                    })
                
                # Incremental Log: Append to JSONL file
                # File: artifacts/audit_results/audit_log.jsonl
                log_path = os.path.join(OUTPUT_DIR, "audit_log.jsonl")
                os.makedirs(os.path.dirname(log_path), exist_ok=True)
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(canonical_findings, ensure_ascii=False) + "\n")
            
            else:
                # Log clean checks too, so we know they were processed
                log_path = os.path.join(OUTPUT_DIR, "audit_log.jsonl")
                os.makedirs(os.path.dirname(log_path), exist_ok=True)
                clean_entry = {
                    "group": group,
                    "canonical": canonical,
                    "findings": [],
                    "timestamp": datetime.now().isoformat()
                }
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(clean_entry, ensure_ascii=False) + "\n")

            # Save the full report on EVERY iteration (overwrite the file)
            # This ensures we always have the latest state on disk.
            self.save_report()
                    
        except json.JSONDecodeError:
            print(f"Failed to parse JSON response for {canonical}: {content[:100]}...")

    def save_report(self):
        # Always use the same filename or timestamped one? 
        # For "continuous saving", a single "latest" file is better than creating thousands of files.
        # But let's also keep the final timestamped one logic if needed.
        
        # 1. Save "latest" file
        latest_path = os.path.join(OUTPUT_DIR, "hard_blocks_latest.json")
        save_json(latest_path, {"findings": self.findings})
        # print(f"Report updated at {latest_path}") # Too noisy for every iteration?

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def load_processed_canonicals() -> Set[str]:
    log_path = os.path.join(OUTPUT_DIR, "audit_log.jsonl")
    processed = set()
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if "canonical" in entry:
                        processed.add(entry["canonical"])
                except json.JSONDecodeError:
                    continue
    return processed

def main():
    print(f"Starting Hard Block Auditor...")
    print(f"Input: {INPUT_FILE}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"Model: {DEFAULT_MODEL_NAME}")

    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file not found at {INPUT_FILE}")
        return

    # Load Resume State
    processed_canonicals = load_processed_canonicals()
    print(f"Resuming... Already processed {len(processed_canonicals)} canonicals.")

    data = load_json(INPUT_FILE)
    agent = HardBlockAuditorAgent(DEFAULT_LLM_BASE_URL, DEFAULT_LLM_API_KEY, DEFAULT_MODEL_NAME)

    # Pre-populate agent findings from log if we want the final report to be complete?
    # Actually, the final report overwrites everything. 
    # If we resume, `agent.findings` is empty, so `save_report` will only save new findings.
    # To fix this, we should also reload existing findings from the log.
    
    # Reload existing findings
    log_path = os.path.join(OUTPUT_DIR, "audit_log.jsonl")
    if os.path.exists(log_path):
         with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    findings_list = entry.get("findings", [])
                    group = entry.get("group", "")
                    canonical = entry.get("canonical", "")
                    
                    for finding in findings_list:
                        agent.findings.append({
                            "group": group,
                            "canonical": canonical,
                            "alias": finding.get("alias"),
                            "reason": finding.get("reason"),
                            "confidence_score": finding.get("confidence_score"),
                            "timestamp": finding.get("timestamp")
                        })
                except:
                    continue
    
    print(f"Loaded {len(agent.findings)} existing findings from log.")

    total_canonicals = 0
    processed_count = 0
    
    for group, group_data in data.items():
        if not isinstance(group_data, dict):
            continue
            
        for canonical, aliases in group_data.items():
            total_canonicals += 1
            
            # Resume Check
            if canonical in processed_canonicals:
                continue

            # Simple optimization: If no aliases, skip
            if not aliases:
                continue
                
            print(f"[{processed_count+1}] Auditing: {canonical} ({len(aliases)} aliases)")
            agent.audit_canonical(group, canonical, aliases)
            processed_count += 1
            
            # Rate limiting / polite delay?
            # time.sleep(0.1) 

    agent.save_report()
    print("Done.")

if __name__ == "__main__":
    main()

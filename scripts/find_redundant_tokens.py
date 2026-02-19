
import json
import os
import sys

# Define path to input
INPUT_FILE = os.path.abspath("Output/stage0/stage0_rewritten_store.json")

def load_json_file(path):
    with open(path, 'r') as f:
        return json.load(f)

def normalize_term(term):
    return term.lower().strip()

def find_redundancies(data):
    count = 0
    # Data structure: { Group: { Canonical: [Alias, ...] } }
    
    for group, group_data in data.items():
        if not isinstance(group_data, dict):
            continue
            
            # Build set of all canonicals in this group for lookup
        group_canonicals = {normalize_term(c) for c in group_data.keys()}

        for canonical, aliases in group_data.items():
            if not isinstance(aliases, list):
                continue
                
            # We don't need to check against self (handled by exact match logic usually)
            # But the user might want to see those too if they exist.
            
            for alias in aliases:
                norm_alias = normalize_term(alias)
                
                # Check against ALL canonicals in the group
                for other_canonical in group_canonicals:
                     # Check 1: Trailing parenthetical
                     suffix_paren = f"({other_canonical})"
                     if norm_alias.endswith(suffix_paren):
                         print(f"[{group}] Canonical: '{canonical}' -> Alias: '{alias}' ends with ({other_canonical})")
                         count += 1
                         
                     # Check 2: Trailing token
                     suffix_token = f" {other_canonical}"
                     if norm_alias.endswith(suffix_token):
                         print(f"[{group}] Canonical: '{canonical}' -> Alias: '{alias}' ends with {other_canonical}")
                         count += 1
                    
    print(f"\nTotal redundancies found: {count}")

if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file not found at {INPUT_FILE}")
        # Try Input/canonical_data.json if stage0 output missing
        INPUT_FILE = os.path.abspath("Input/canonical_data.json")
        if not os.path.exists(INPUT_FILE):
             print(f"Error: Input file not found at {INPUT_FILE}")
             sys.exit(1)
        print(f"Falling back to {INPUT_FILE}")

    print(f"Scanning {INPUT_FILE}...")
    data = load_json_file(INPUT_FILE)
    find_redundancies(data)

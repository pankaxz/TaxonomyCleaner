import os
import ast
import json
import hashlib
import requests
from collections import defaultdict
from tqdm import tqdm

# =====================================================
# CONFIG
# =====================================================

REPO_PATH = "/mnt/workspace/Backups/TaxonomyCleaner-master/CanonicalData"
OUTPUT_DIR = "repo_analysis"
CHECKPOINT_DIR = f"{OUTPUT_DIR}/checkpoints"

LLM_ENDPOINT = "http://127.0.0.1:8080/v1/completions"
MODEL_NAME = "Qwen3.5-35B-A3B-Q5_K_S.gguf"

TEMPERATURE = 0.1
MAX_TOKENS = 400

MAX_PROMPT_CHARS = 12000
MAX_BATCH_CHUNKS = 6

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

PROGRESS_FILE = f"{CHECKPOINT_DIR}/progress.json"
CHUNK_CACHE_FILE = f"{CHECKPOINT_DIR}/chunk_cache.json"
FILE_CACHE_FILE = f"{CHECKPOINT_DIR}/file_cache.json"

SKIP_DIRS = {
    "__pycache__",
    ".git",
    ".venv",
    "repo_analysis",
    "tests",
    "artifacts",
    "build"
}

# =====================================================
# UTILITIES
# =====================================================

def load_json(path, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def hash_text(text):
    return hashlib.md5(text.encode()).hexdigest()


# =====================================================
# LLM CALL
# =====================================================

def ask_llm(prompt, retries=3):

    if len(prompt) > MAX_PROMPT_CHARS:
        prompt = prompt[:MAX_PROMPT_CHARS]

    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
        "n_ctx": 8192
    }

    for _ in range(retries):
        try:
            r = requests.post(LLM_ENDPOINT, json=payload, timeout=600)

            if r.status_code == 200:
                return r.json()["choices"][0]["text"].strip()

        except Exception as e:
            print("LLM error:", e)

    return ""


# =====================================================
# FILE COLLECTION
# =====================================================

def collect_python_files(repo):

    files = []

    for root, dirs, filenames in os.walk(repo):

        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]

        for f in filenames:
            if f.endswith(".py"):
                files.append(os.path.join(root, f))

    return sorted(files)


# =====================================================
# IMPORT GRAPH
# =====================================================

def build_import_graph(files):

    graph = defaultdict(set)

    for file in files:

        try:
            tree = ast.parse(open(file).read())
        except:
            continue

        for node in ast.walk(tree):

            if isinstance(node, ast.Import):
                for name in node.names:
                    graph[file].add(name.name)

            if isinstance(node, ast.ImportFrom):
                if node.module:
                    graph[file].add(node.module)

    return graph


# =====================================================
# CIRCULAR IMPORT DETECTION
# =====================================================

def find_cycles(graph):

    visited = set()
    stack = set()
    cycles = []

    def dfs(node):

        visited.add(node)
        stack.add(node)

        for neighbor in graph[node]:

            if neighbor not in graph:
                continue

            if neighbor not in visited:
                dfs(neighbor)

            elif neighbor in stack:
                cycles.append((node, neighbor))

        stack.remove(node)

    for node in graph:
        if node not in visited:
            dfs(node)

    return cycles


# =====================================================
# DEAD FUNCTION DETECTION
# =====================================================

def collect_function_usage(files):

    defined = set()
    called = set()

    for file in files:

        try:
            tree = ast.parse(open(file).read())
        except:
            continue

        for node in ast.walk(tree):

            if isinstance(node, ast.FunctionDef):
                defined.add(node.name)

            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    called.add(node.func.id)

    return defined, called


# =====================================================
# CODE CHUNK EXTRACTION
# =====================================================

def extract_chunks(file):

    try:
        source = open(file).read()
        tree = ast.parse(source)
        lines = source.splitlines()
    except:
        return []

    chunks = []

    for node in ast.walk(tree):

        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):

            start = node.lineno - 1
            end = node.end_lineno

            code = "\n".join(lines[start:end])

            chunks.append(code)

    return chunks


# =====================================================
# CHUNK SUMMARIZATION (BATCHED + CACHE)
# =====================================================

def summarize_chunks(chunks, chunk_cache):

    summaries = []

    batches = []
    current = []

    for chunk in chunks:

        key = hash_text(chunk)

        if key in chunk_cache:
            summaries.append(chunk_cache[key])
            continue

        current.append((key, chunk))

        if len(current) >= MAX_BATCH_CHUNKS:
            batches.append(current)
            current = []

    if current:
        batches.append(current)

    for batch in batches:

        codes = [c[1] for c in batch]

        joined = "\n\n---\n\n".join(codes)

        prompt = f"""
Summarize the following Python code blocks.

For each block include:

Purpose
Key functions/classes
Inputs
Outputs
Role in system

Code blocks:
{joined}
"""

        response = ask_llm(prompt)

        for key, _ in batch:
            chunk_cache[key] = response

        save_json(CHUNK_CACHE_FILE, chunk_cache)

        summaries.append(response)

    return summaries


# =====================================================
# FILE SUMMARY
# =====================================================

def summarize_file(file_path, summaries):

    text = "\n".join(summaries)

    prompt = f"""
These summaries describe parts of a Python file.

Produce a concise file-level description.

Include:

File purpose
Key components
Dependencies
Architectural role

Summaries:
{text}
"""

    return ask_llm(prompt)


# =====================================================
# ARCHITECTURE SUMMARY
# =====================================================

def summarize_architecture(file_summaries):

    text = "\n\n".join(file_summaries)

    prompt = f"""
You are analyzing a Python repository.

Produce a high level architecture summary.

Include:

System overview
Main modules
Execution flow
Dependencies
Design risks

File summaries:
{text}
"""

    return ask_llm(prompt)


# =====================================================
# MERMAID GRAPH
# =====================================================

def generate_mermaid(graph):

    lines = ["graph TD"]

    for src in graph:

        for dst in graph[src]:

            src_name = os.path.basename(src)
            dst_name = dst.split(".")[0]

            lines.append(f"{src_name} --> {dst_name}")

    return "\n".join(lines)


# =====================================================
# MAIN PIPELINE
# =====================================================

def main():

    files = collect_python_files(REPO_PATH)

    print(f"Found {len(files)} Python files")

    progress = load_json(PROGRESS_FILE, {"processed_files": []})
    chunk_cache = load_json(CHUNK_CACHE_FILE, {})
    file_cache = load_json(FILE_CACHE_FILE, {})

    file_summaries = []

    import_graph = build_import_graph(files)
    cycles = find_cycles(import_graph)

    defined, called = collect_function_usage(files)
    dead_functions = defined - called

    for file in tqdm(files):

        if file in progress["processed_files"]:
            if file in file_cache:
                file_summaries.append(file_cache[file])
            continue

        print("Processing:", file)

        chunks = extract_chunks(file)

        if not chunks:
            continue

        summaries = summarize_chunks(chunks, chunk_cache)

        file_summary = summarize_file(file, summaries)

        entry = f"\nFILE: {file}\n{file_summary}"

        file_cache[file] = entry
        save_json(FILE_CACHE_FILE, file_cache)

        progress["processed_files"].append(file)
        save_json(PROGRESS_FILE, progress)

        file_summaries.append(entry)

    print("\nGenerating architecture summary...")

    architecture = summarize_architecture(file_summaries)

    with open(f"{OUTPUT_DIR}/file_summaries.md", "w") as f:
        f.write("\n\n".join(file_summaries))

    with open(f"{OUTPUT_DIR}/architecture.md", "w") as f:
        f.write(architecture)

    with open(f"{OUTPUT_DIR}/dead_functions.txt", "w") as f:
        f.write("\n".join(dead_functions))

    with open(f"{OUTPUT_DIR}/circular_imports.txt", "w") as f:
        for a, b in cycles:
            f.write(f"{a} -> {b}\n")

    with open(f"{OUTPUT_DIR}/dependency_graph.mmd", "w") as f:
        f.write(generate_mermaid(import_graph))

    print("\nAnalysis complete.")
    print("Results in repo_analysis/")


if __name__ == "__main__":
    main()


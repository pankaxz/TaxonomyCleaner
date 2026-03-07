import ast
import os
from pathlib import Path

REPO_PATH = "/mnt/workspace/Backups/TaxonomyCleaner-master/CanonicalData"
OUTPUT_FILE = "chunks.json"


def extract_chunks(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        source = f.read()

    tree = ast.parse(source)
    lines = source.splitlines()

    chunks = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            start = node.lineno - 1
            end = node.end_lineno

            code = "\n".join(lines[start:end])

            chunk = {
                "file": str(file_path),
                "type": type(node).__name__,
                "name": node.name,
                "start_line": node.lineno,
                "end_line": node.end_lineno,
                "code": code
            }

            chunks.append(chunk)

    return chunks


def collect_chunks(repo_path):
    all_chunks = []

    for root, _, files in os.walk(repo_path):
        for file in files:
            if file.endswith(".py"):
                path = Path(root) / file
                try:
                    chunks = extract_chunks(path)
                    all_chunks.extend(chunks)
                except Exception:
                    pass

    return all_chunks


def main():
    chunks = collect_chunks(REPO_PATH)

    import json
    with open(OUTPUT_FILE, "w") as f:
        json.dump(chunks, f, indent=2)

    print(f"{len(chunks)} chunks extracted")


if __name__ == "__main__":
    main()

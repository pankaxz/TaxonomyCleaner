"""Shared low-level helpers for discovery pipeline processors."""

import json
import logging
import math
import os
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

NUM_WORKERS = os.cpu_count() or 4
# Skip multiprocessing overhead for small inputs
PARALLEL_THRESHOLD = 50
STATUS_FILENAME_RE = re.compile(r"[^a-z0-9_]+")


def resolve_jsonl_paths(path: str | Path, *, log_prefix: str) -> list[Path]:
    """Resolve a file or directory path into a sorted list of JSONL files."""
    p = Path(path)
    if p.is_dir():
        paths = sorted(p.glob("*.jsonl"))
        if not paths:
            logger.warning(f"{log_prefix}: no .jsonl files found in {p}")
        return paths
    if p.is_file():
        return [p]
    return []


def save_json(data: Any, path: str | Path) -> Path:
    """Write JSON to disk, creating parent dirs as needed."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"saved {p}")
    return p


def chunk_list(lst: list, n_chunks: int) -> list[list]:
    """Split a list into n roughly equal non-empty chunks."""
    if not lst:
        return []
    n_chunks = min(n_chunks, len(lst))
    chunk_size = math.ceil(len(lst) / n_chunks)
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]

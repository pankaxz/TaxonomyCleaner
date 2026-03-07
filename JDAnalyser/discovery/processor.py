"""Discovery queue processor — scans crawler JSONL for novel skills.

Accepts a single JSONL file **or** a directory of JSONL files.
All technical_skills from every record are always checked against the
canonical taxonomy — the scraper's extraction_quality.unmapped_skills is
informational only and never trusted as the sole source of candidates.

Uses all available CPU cores via ProcessPoolExecutor for:
  1. JSONL parsing + candidate extraction (chunked across workers)
  2. Taxonomy dedup matching (fuzzy matching parallelized across workers)
"""

import json
import logging
import math
import os
import re
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import cfg
from discovery.dedup import SkillDeduplicator

logger = logging.getLogger(__name__)

# Matches "[Group Name]" at end of a skill string
_GROUP_TAG_RE = re.compile(r"\s*\[([^\]]+)\]\s*$")
# Underscore-to-space for old-format tags like "Cloud_Platforms"
_UNDERSCORE_RE = re.compile(r"_")

_NUM_WORKERS = os.cpu_count() or 4
# Skip multiprocessing overhead for small inputs
_PARALLEL_THRESHOLD = 50
_STATUS_FILENAME_RE = re.compile(r"[^a-z0-9_]+")


def _resolve_jsonl_paths(path: str | Path) -> list[Path]:
    """Resolve a file or directory path into a sorted list of JSONL files."""
    p = Path(path)
    if p.is_dir():
        paths = sorted(p.glob("*.jsonl"))
        if not paths:
            logger.warning(f"discovery: no .jsonl files found in {p}")
        return paths
    if p.is_file():
        return [p]
    return []


def _save_json(data: Any, path: str | Path) -> Path:
    """Write JSON to disk, creating parent dirs as needed."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"saved {p}")
    return p


def _chunk_list(lst: list, n_chunks: int) -> list[list]:
    """Split a list into n roughly equal non-empty chunks."""
    if not lst:
        return []
    n_chunks = min(n_chunks, len(lst))
    chunk_size = math.ceil(len(lst) / n_chunks)
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


# ── Module-level functions (picklable for multiprocessing workers) ──────────


def _parse_skill_with_tag(raw_skill: str) -> tuple[str, Optional[str]]:
    """Extract skill name and optional group tag from a tagged skill string.

    Examples:
        "Python [Languages]"         -> ("Python", "Languages")
        "AWS [Cloud_Platforms]"       -> ("AWS", "Cloud Platforms")
        "Docker"                      -> ("Docker", None)
    """
    m = _GROUP_TAG_RE.search(raw_skill)
    if m:
        skill_name = raw_skill[: m.start()].strip()
        group_tag = m.group(1).strip()
        group_tag = _UNDERSCORE_RE.sub(" ", group_tag)
        return skill_name, group_tag
    return raw_skill.strip(), None


def _extract_candidates_from_record(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract skill candidates from a single JSONL record.

    Pulls from:
    1. extraction_quality.unmapped_skills (new crawler format)
    2. technical_skills with [Group] tags (both old and new formats)

    Returns list of {name, group_tag, source_url, scraped_date}.
    """
    candidates: list[dict[str, Any]] = []
    source_url = record.get("source_url", "")
    # Use scraped_at from record if available (Schema A/C), else None
    raw_ts = record.get("scraped_at", "")
    scraped_date = raw_ts[:10] if isinstance(raw_ts, str) and len(raw_ts) >= 10 else None
    # Per-record de-dup map: lowered skill name -> candidate dict.
    # This lets us enrich an existing candidate if a later source has a group tag.
    seen: dict[str, dict[str, Any]] = {}

    # Source 1: explicit unmapped_skills from extraction_quality
    eq = record.get("extraction_quality", {})
    unmapped = eq.get("unmapped_skills", []) if isinstance(eq, dict) else []
    for skill_name in unmapped or []:
        if not isinstance(skill_name, str):
            continue
        name = skill_name.strip()
        if not name:
            continue

        key = name.lower()
        if key in seen:
            continue

        candidate = {"name": name, "group_tag": None, "source_url": source_url, "scraped_date": scraped_date}
        candidates.append(candidate)
        seen[key] = candidate

    # Source 2: tagged technical_skills — extract group tag, treat all as candidates
    for raw_skill in record.get("technical_skills", []) or []:
        if not isinstance(raw_skill, str):
            continue
        name, group_tag = _parse_skill_with_tag(raw_skill)
        if not name:
            continue

        key = name.lower()
        if key not in seen:
            candidate = {"name": name, "group_tag": group_tag, "source_url": source_url, "scraped_date": scraped_date}
            candidates.append(candidate)
            seen[key] = candidate
            continue

        # If the first occurrence came from unmapped_skills (no tag),
        # enrich it with a later technical_skills tag.
        existing = seen[key]
        if not existing.get("group_tag") and group_tag:
            existing["group_tag"] = group_tag

    return candidates


def _parse_chunk(lines: List[str]) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Worker: parse a chunk of JSONL lines and extract skill candidates.

    Returns (candidates, record_count, parse_error_count, skipped_error_count).
    """
    candidates: list[dict[str, Any]] = []
    records = 0
    errors = 0
    skipped = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except Exception:
            errors += 1
            continue
        # Some JSONL schemas include an "is_error" flag set by the scraper
        # when LLM extraction failed for that job posting — skip those.
        if record.get("is_error"):
            skipped += 1
            continue
        records += 1
        candidates.extend(_extract_candidates_from_record(record))
    return candidates, records, errors, skipped


def _match_chunk(
    args: Tuple[List[str], float],
) -> Dict[str, Optional[Tuple[str, str, float]]]:
    """Worker: match a chunk of candidate names against the taxonomy.

    Each worker process loads the taxonomy independently (via fork COW)
    and runs 4-tier matching (exact → normalized → fuzzy → containment).
    """
    names, fuzzy_threshold = args
    return {name: SkillDeduplicator.find_match(name, fuzzy_threshold) for name in names}


class DiscoveryProcessor:
    """Scan crawler JSONL output and build/update the discovery queue."""

    _QUEUE_CACHE: Optional[Dict[str, dict]] = None

    @classmethod
    def invalidate_cache(cls) -> None:
        cls._QUEUE_CACHE = None

    @classmethod
    def _load_queue(cls) -> Dict[str, dict]:
        """Load existing discovery queue from disk, or return empty dict."""
        if cls._QUEUE_CACHE is not None:
            return cls._QUEUE_CACHE

        queue_path = cfg.get_abs_path("discovery.queue_path")
        if queue_path and Path(queue_path).exists():
            with open(queue_path, "r", encoding="utf-8") as f:
                cls._QUEUE_CACHE = json.load(f)
                logger.info(
                    f"discovery: loaded queue with {len(cls._QUEUE_CACHE)} entries"
                )
        else:
            cls._QUEUE_CACHE = {}
        return cls._QUEUE_CACHE

    @classmethod
    def _save_queue(cls, queue: Dict[str, dict], out_path: str | Path | None = None) -> Path:
        queue_path = out_path or cfg.get_abs_path("discovery.queue_path")
        cls._QUEUE_CACHE = queue
        return _save_json(queue, queue_path)

    @staticmethod
    def _status_output_dir(out_path: str | Path | None = None) -> Path:
        configured = cfg.get_abs_path("discovery.status_output_dir")
        if configured:
            return Path(configured)

        queue_path = Path(out_path or cfg.get_abs_path("discovery.queue_path"))
        return queue_path.parent / f"{queue_path.stem}_statuses"

    @classmethod
    def _save_queue_by_status(
        cls, queue: Dict[str, dict], out_path: str | Path | None = None
    ) -> Dict[str, int]:
        """Write one JSON file per queue status.

        Example outputs:
          - pending.json
          - ready_for_promotion.json
          - promoted.json
          - rejected.json
        """
        status_dir = cls._status_output_dir(out_path=out_path)
        status_dir.mkdir(parents=True, exist_ok=True)

        grouped: Dict[str, Dict[str, dict]] = {}
        for key, entry in queue.items():
            raw_status = str(entry.get("status", "unknown")).strip().lower()
            status = raw_status or "unknown"
            status = _STATUS_FILENAME_RE.sub("_", status).strip("_") or "unknown"
            grouped.setdefault(status, {})[key] = entry

        expected_files = {f"{status}.json" for status in grouped}
        for existing in status_dir.glob("*.json"):
            if existing.name not in expected_files:
                existing.unlink()

        counts: Dict[str, int] = {}
        for status, entries in grouped.items():
            _save_json(entries, status_dir / f"{status}.json")
            counts[status] = len(entries)

        logger.info(f"discovery: wrote status files to {status_dir} ({counts})")
        return counts

    @staticmethod
    def _parse_skill_with_tag(raw_skill: str) -> tuple[str, Optional[str]]:
        return _parse_skill_with_tag(raw_skill)

    @classmethod
    def _extract_candidates(cls, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        return _extract_candidates_from_record(record)

    @classmethod
    def process_jsonl(
        cls, path: str, *, parallel: bool = True, out_path: str | Path | None = None
    ) -> Dict[str, dict]:
        """Scan JSONL file(s) and update the discovery queue.

        Args:
            path: Path to a single .jsonl file or a directory containing
                .jsonl files (all files in the directory will be processed).
            parallel: If True (default), use ProcessPoolExecutor for parsing
                and matching. If False, run everything single-process.
            out_path: Write the queue to this file instead of the default.
        """
        jsonl_paths = _resolve_jsonl_paths(path)
        if not jsonl_paths:
            logger.error(f"discovery: no JSONL files found at: {path}")
            return cls._load_queue()

        queue = cls._load_queue()
        threshold = cfg.get("discovery.promotion_threshold", 5)
        max_sources = cfg.get("discovery.max_sample_sources", 10)
        fuzzy_threshold = cfg.get("discovery.fuzzy_threshold", 0.85)
        fallback_date = date.today().isoformat()

        use_parallel = parallel  # caller can force single-process

        # ── Phase 1: Read all lines into memory ────────────────────────
        # Accepts a single file or a directory of .jsonl files.
        all_lines: List[str] = []
        for jp in jsonl_paths:
            with open(jp, "r", encoding="utf-8") as f:
                all_lines.extend(f.readlines())
            logger.info(f"discovery: read {jp.name}")

        # ── Phase 2: Parse JSONL & extract candidates ──────────────────
        all_candidates: List[Dict[str, Any]] = []
        record_count = 0
        error_count = 0
        skipped_count = 0

        if use_parallel and len(all_lines) >= _PARALLEL_THRESHOLD:
            chunks = _chunk_list(all_lines, _NUM_WORKERS)
            with ProcessPoolExecutor(max_workers=_NUM_WORKERS) as pool:
                for candidates, records, errors, skipped in pool.map(
                    _parse_chunk, chunks
                ):
                    all_candidates.extend(candidates)
                    record_count += records
                    error_count += errors
                    skipped_count += skipped
        else:
            candidates, records, errors, skipped = _parse_chunk(all_lines)
            all_candidates.extend(candidates)
            record_count += records
            error_count += errors
            skipped_count += skipped

        if error_count > 0:
            logger.warning(f"discovery: {error_count} JSON parse errors")
        if skipped_count > 0:
            logger.info(f"discovery: skipped {skipped_count} is_error records")

        mode = f"parallel, workers={_NUM_WORKERS}" if use_parallel else "sequential"
        logger.info(
            f"discovery: scanned {record_count} records, "
            f"extracted {len(all_candidates)} raw candidates "
            f"({mode})"
        )

        # ── Phase 3: Deduplicate against taxonomy ──────────────────────
        unique_names = list({c["name"] for c in all_candidates})

        if use_parallel and len(unique_names) >= _PARALLEL_THRESHOLD:
            name_chunks = _chunk_list(unique_names, _NUM_WORKERS)
            chunk_args = [(chunk, fuzzy_threshold) for chunk in name_chunks]
            matches: Dict[str, Optional[Tuple[str, str, float]]] = {}
            with ProcessPoolExecutor(max_workers=_NUM_WORKERS) as pool:
                for chunk_result in pool.map(_match_chunk, chunk_args):
                    matches.update(chunk_result)
        else:
            matches = SkillDeduplicator.find_match_batch(
                unique_names, fuzzy_threshold
            )

        # ── Phase 4: Update queue (sequential — fast dict ops) ─────────
        name_to_candidates: Dict[str, List[Dict]] = {}
        for c in all_candidates:
            name_to_candidates.setdefault(c["name"], []).append(c)

        novel_count = 0
        matched_count = 0

        for name in unique_names:
            match = matches.get(name)
            if match is not None:
                matched_count += 1
                continue

            # Novel skill — update queue
            novel_count += 1
            key = name.lower().replace(" ", "_")
            entries = name_to_candidates[name]

            # Use actual scrape dates from records; fall back to today
            # if scraped_at was missing (Schema B has no scraped_at).
            candidate_dates = [
                c["scraped_date"] for c in entries if c.get("scraped_date")
            ]
            earliest = min(candidate_dates) if candidate_dates else fallback_date
            latest = max(candidate_dates) if candidate_dates else fallback_date

            if key in queue:
                entry = queue[key]
                entry["seen_count"] += len(entries)
                entry["first_seen"] = min(entry["first_seen"], earliest)
                entry["last_seen"] = max(entry["last_seen"], latest)
            else:
                entry = {
                    "display_name": name,
                    "seen_count": len(entries),
                    "first_seen": earliest,
                    "last_seen": latest,
                    "suggested_groups": {},
                    "llm_group_tags": {},
                    "sample_sources": [],
                    "status": "pending",
                }
                queue[key] = entry

            # Accumulate group tags
            for c in entries:
                tag = c.get("group_tag")
                if tag:
                    entry["suggested_groups"][tag] = (
                        entry["suggested_groups"].get(tag, 0) + 1
                    )
                    entry["llm_group_tags"][tag] = (
                        entry["llm_group_tags"].get(tag, 0) + 1
                    )

            # Accumulate sample sources (capped)
            for c in entries:
                url = c.get("source_url", "")
                if (
                    url
                    and url not in entry["sample_sources"]
                    and len(entry["sample_sources"]) < max_sources
                ):
                    entry["sample_sources"].append(url)

            # Auto-promote if threshold met
            if entry["status"] == "pending" and entry["seen_count"] >= threshold:
                entry["status"] = "ready_for_promotion"

        logger.info(
            f"discovery: {novel_count} novel skills, "
            f"{matched_count} already in taxonomy, "
            f"queue now has {len(queue)} entries"
        )

        cls._save_queue(queue, out_path=out_path)
        cls._save_queue_by_status(queue, out_path=out_path)
        return queue

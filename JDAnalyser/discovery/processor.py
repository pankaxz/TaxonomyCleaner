"""Discovery queue processor — scans crawler JSONL for novel skills,
soft skills, and action verbs.

Accepts a single JSONL file **or** a directory of JSONL files.
Candidates come from both extraction_quality.unmapped_skills AND
technical_skills. Within a single record, duplicates are merged and
tag-enriched. CanonicalDataCleaner handles dedup in later stages.

Uses all available CPU cores via ProcessPoolExecutor for:
  1. JSONL parsing + candidate extraction (chunked across workers)
  2. Taxonomy dedup matching (fuzzy matching parallelized across workers)

Additional processors:
  - SoftSkillProcessor: extracts soft_skills field, deduplicates against
    soft_skill_taxonomy.json, tracks novel soft skills.
  - VerbProcessor: extracts action verbs from raw_jd bullet points,
    deduplicates against verb-taxonomy.json, tracks occurrences.
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
from discovery.soft_skill_reader import SoftSkillReader
from discovery.verb_reader import VerbReader

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
    1. extraction_quality.unmapped_skills (primary source for discovery)
    2. technical_skills (tag enrichment only for overlapping skill names)

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

    # Parse technical skills once and keep one tag hint per skill.
    # These tags can enrich candidates that come from unmapped_skills.
    technical_tag_hints: dict[str, str] = {}
    for raw_skill in record.get("technical_skills", []) or []:
        if not isinstance(raw_skill, str):
            continue
        name, group_tag = _parse_skill_with_tag(raw_skill)
        if not name:
            continue
        key = name.lower()
        if group_tag and key not in technical_tag_hints:
            technical_tag_hints[key] = group_tag

    # Source 1: explicit unmapped_skills from extraction_quality.
    # This is the source of candidates we want to promote over time.
    eq = record.get("extraction_quality", {})
    unmapped = eq.get("unmapped_skills", []) if isinstance(eq, dict) else []
    for skill_name in unmapped or []:
        if not isinstance(skill_name, str):
            continue
        name, group_tag = _parse_skill_with_tag(skill_name)
        if not name:
            continue

        key = name.lower()
        resolved_group_tag = technical_tag_hints.get(key) or group_tag

        if key in seen:
            existing = seen[key]
            if not existing.get("group_tag") and resolved_group_tag:
                existing["group_tag"] = resolved_group_tag
            continue
        candidate = {
            "name": name,
            "group_tag": resolved_group_tag,
            "source_url": source_url,
            "scraped_date": scraped_date,
        }
        candidates.append(candidate)
        seen[key] = candidate

    # Source 2: technical_skills — also added as candidates.
    # Duplicates with unmapped_skills are merged (tag enriched).
    # CanonicalDataCleaner handles dedup in later stages.
    for raw_skill in record.get("technical_skills", []) or []:
        if not isinstance(raw_skill, str):
            continue
        name, group_tag = _parse_skill_with_tag(raw_skill)
        if not name:
            continue

        key = name.lower()
        if key in seen:
            existing = seen[key]
            if not existing.get("group_tag") and group_tag:
                existing["group_tag"] = group_tag
            continue
        candidate = {
            "name": name,
            "group_tag": group_tag,
            "source_url": source_url,
            "scraped_date": scraped_date,
        }
        candidates.append(candidate)
        seen[key] = candidate

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


# ══════════════════════════════════════════════════════════════════════════════
# Soft Skill Extraction
# ══════════════════════════════════════════════════════════════════════════════

# Module-level regex for soft skill dedup (mirrors SkillDeduplicator pattern)
_SOFT_STRIP_RE = re.compile(r"[^a-z0-9\s]")


def _strip_soft(text: str) -> str:
    text = text.lower()
    text = _SOFT_STRIP_RE.sub("", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_soft_skill_candidates_from_record(
    record: Dict[str, Any],
    max_chars: int = 60,
    max_words: int = 6,
) -> List[Dict[str, Any]]:
    """Extract soft skill candidates from the record's soft_skills field.

    Entries that look like full sentences (too long or too many words) are
    filtered out — they come from JDs that include prose rather than skill tags.
    """
    candidates: List[Dict[str, Any]] = []
    source_url = record.get("source_url", "")
    raw_ts = record.get("scraped_at", "")
    scraped_date = raw_ts[:10] if isinstance(raw_ts, str) and len(raw_ts) >= 10 else None

    seen: set[str] = set()
    for skill_name in record.get("soft_skills", []) or []:
        if not isinstance(skill_name, str):
            continue
        name = skill_name.strip()
        if not name:
            continue
        # Skip entries that are clearly full sentences, not skill names
        if len(name) > max_chars or len(name.split()) > max_words:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            {"name": name, "source_url": source_url, "scraped_date": scraped_date}
        )
    return candidates


def _parse_soft_skill_chunk(
    args: Tuple[List[str], int, int],
) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Worker: parse a chunk of JSONL lines and extract soft skill candidates.

    Args: (lines, max_chars, max_words)
    """
    lines, max_chars, max_words = args
    candidates: List[Dict[str, Any]] = []
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
        if record.get("is_error"):
            skipped += 1
            continue
        records += 1
        candidates.extend(
            _extract_soft_skill_candidates_from_record(record, max_chars, max_words)
        )
    return candidates, records, errors, skipped


def _find_soft_skill_match(
    name: str, fuzzy_threshold: float
) -> Optional[Tuple[str, str, float]]:
    """4-tier match of a soft skill candidate against the soft skill taxonomy.

    Returns (canonical, match_type, confidence) or None if novel.
    """
    alias_map = SoftSkillReader.get_alias_map()
    canonicals = SoftSkillReader.get_all_canonicals()

    normalized = name.lower()
    if normalized in alias_map:
        return (alias_map[normalized], "exact", 1.0)

    stripped = _strip_soft(name)
    for alias_key, canonical in alias_map.items():
        if _strip_soft(alias_key) == stripped:
            return (canonical, "normalized", 0.95)

    from difflib import SequenceMatcher

    best_ratio = 0.0
    best_canonical = None
    for alias_key, canonical in alias_map.items():
        ratio = SequenceMatcher(None, normalized, alias_key).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_canonical = canonical
    if best_ratio >= fuzzy_threshold and best_canonical:
        return (best_canonical, "fuzzy", round(best_ratio, 3))

    for canonical in canonicals:
        canon_lower = canonical.lower()
        if (
            len(canon_lower) >= 3
            and canon_lower in normalized
            and canon_lower != normalized
        ):
            return (canon_lower, "containment", 0.80)

    return None


def _match_soft_skill_chunk(
    args: Tuple[List[str], float],
) -> Dict[str, Optional[Tuple[str, str, float]]]:
    """Worker: match a chunk of soft skill names against the taxonomy."""
    names, fuzzy_threshold = args
    return {name: _find_soft_skill_match(name, fuzzy_threshold) for name in names}


class SoftSkillProcessor:
    """Scan crawler JSONL output and build/update the soft skills discovery queue.

    Novel soft skills (those not found in soft_skill_taxonomy.json) are added
    to the queue. Known soft skills are counted as 'matched' and skipped.
    """

    _QUEUE_CACHE: Optional[Dict[str, dict]] = None

    @classmethod
    def invalidate_cache(cls) -> None:
        cls._QUEUE_CACHE = None

    @classmethod
    def _load_queue(cls) -> Dict[str, dict]:
        if cls._QUEUE_CACHE is not None:
            return cls._QUEUE_CACHE

        queue_path = cfg.get_abs_path("soft_skill_taxonomy.queue_path")
        if queue_path and Path(queue_path).exists():
            with open(queue_path, "r", encoding="utf-8") as f:
                cls._QUEUE_CACHE = json.load(f)
            logger.info(
                f"soft_skills: loaded queue with {len(cls._QUEUE_CACHE)} entries"
            )
        else:
            cls._QUEUE_CACHE = {}
        return cls._QUEUE_CACHE

    @classmethod
    def _save_queue(
        cls, queue: Dict[str, dict], out_path: str | Path | None = None
    ) -> Path:
        queue_path = out_path or cfg.get_abs_path("soft_skill_taxonomy.queue_path")
        cls._QUEUE_CACHE = queue
        return _save_json(queue, queue_path)

    @classmethod
    def _save_queue_by_status(
        cls, queue: Dict[str, dict], out_path: str | Path | None = None
    ) -> Dict[str, int]:
        configured = cfg.get_abs_path("soft_skill_taxonomy.status_output_dir")
        if configured:
            status_dir = Path(configured)
        else:
            queue_path = Path(
                out_path or cfg.get_abs_path("soft_skill_taxonomy.queue_path")
            )
            status_dir = queue_path.parent / f"{queue_path.stem}_statuses"

        status_dir.mkdir(parents=True, exist_ok=True)
        grouped: Dict[str, Dict[str, dict]] = {}
        for key, entry in queue.items():
            raw_status = str(entry.get("status", "unknown")).strip().lower()
            status = _STATUS_FILENAME_RE.sub("_", raw_status).strip("_") or "unknown"
            grouped.setdefault(status, {})[key] = entry

        expected_files = {f"{s}.json" for s in grouped}
        for existing in status_dir.glob("*.json"):
            if existing.name not in expected_files:
                existing.unlink()

        counts: Dict[str, int] = {}
        for status, entries in grouped.items():
            _save_json(entries, status_dir / f"{status}.json")
            counts[status] = len(entries)

        logger.info(f"soft_skills: wrote status files to {status_dir} ({counts})")
        return counts

    @classmethod
    def process_jsonl(
        cls, path: str, *, parallel: bool = True, out_path: str | Path | None = None
    ) -> Dict[str, dict]:
        """Scan JSONL file(s) for soft skills and update the soft skills queue.

        Args:
            path: Path to a single .jsonl file or a directory.
            parallel: Use ProcessPoolExecutor when True.
            out_path: Override output queue path.
        """
        jsonl_paths = _resolve_jsonl_paths(path)
        if not jsonl_paths:
            logger.error(f"soft_skills: no JSONL files found at: {path}")
            return cls._load_queue()

        queue = cls._load_queue()
        threshold = cfg.get("discovery.promotion_threshold", 5)
        max_sources = cfg.get("discovery.max_sample_sources", 10)
        fuzzy_threshold = cfg.get("discovery.fuzzy_threshold", 0.85)
        max_chars = cfg.get("soft_skill_taxonomy.max_name_chars", 60)
        max_words = cfg.get("soft_skill_taxonomy.max_name_words", 6)
        fallback_date = date.today().isoformat()

        # ── Phase 1: Read lines ────────────────────────────────────────
        all_lines: List[str] = []
        for jp in jsonl_paths:
            with open(jp, "r", encoding="utf-8") as f:
                all_lines.extend(f.readlines())
            logger.info(f"soft_skills: read {jp.name}")

        # ── Phase 2: Parse & extract candidates ───────────────────────
        all_candidates: List[Dict[str, Any]] = []
        record_count = error_count = skipped_count = 0

        if parallel and len(all_lines) >= _PARALLEL_THRESHOLD:
            chunks = _chunk_list(all_lines, _NUM_WORKERS)
            chunk_args = [(chunk, max_chars, max_words) for chunk in chunks]
            with ProcessPoolExecutor(max_workers=_NUM_WORKERS) as pool:
                for candidates, records, errors, skipped in pool.map(
                    _parse_soft_skill_chunk, chunk_args
                ):
                    all_candidates.extend(candidates)
                    record_count += records
                    error_count += errors
                    skipped_count += skipped
        else:
            candidates, records, errors, skipped = _parse_soft_skill_chunk(
                (all_lines, max_chars, max_words)
            )
            all_candidates.extend(candidates)
            record_count += records
            error_count += errors
            skipped_count += skipped

        logger.info(
            f"soft_skills: scanned {record_count} records, "
            f"extracted {len(all_candidates)} raw candidates"
        )

        # ── Phase 3: Deduplicate against soft skill taxonomy ──────────
        unique_names = list({c["name"] for c in all_candidates})

        if parallel and len(unique_names) >= _PARALLEL_THRESHOLD:
            name_chunks = _chunk_list(unique_names, _NUM_WORKERS)
            chunk_args = [(chunk, fuzzy_threshold) for chunk in name_chunks]
            matches: Dict[str, Optional[Tuple[str, str, float]]] = {}
            with ProcessPoolExecutor(max_workers=_NUM_WORKERS) as pool:
                for chunk_result in pool.map(_match_soft_skill_chunk, chunk_args):
                    matches.update(chunk_result)
        else:
            matches = {
                name: _find_soft_skill_match(name, fuzzy_threshold)
                for name in unique_names
            }

        # ── Phase 4: Update queue ─────────────────────────────────────
        name_to_candidates: Dict[str, List[Dict]] = {}
        for c in all_candidates:
            name_to_candidates.setdefault(c["name"], []).append(c)

        novel_count = matched_count = 0
        group_map = SoftSkillReader.get_group_map()

        for name in unique_names:
            match = matches.get(name)
            if match is not None:
                matched_count += 1
                continue

            novel_count += 1
            key = name.lower().replace(" ", "_")
            entries = name_to_candidates[name]

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
                # Check if canonical form has a known group
                suggested_group = group_map.get(name.lower())
                entry = {
                    "display_name": name,
                    "seen_count": len(entries),
                    "first_seen": earliest,
                    "last_seen": latest,
                    "suggested_group": suggested_group,
                    "sample_sources": [],
                    "status": "pending",
                }
                queue[key] = entry

            for c in entries:
                url = c.get("source_url", "")
                if (
                    url
                    and url not in entry["sample_sources"]
                    and len(entry["sample_sources"]) < max_sources
                ):
                    entry["sample_sources"].append(url)

            if entry["status"] == "pending" and entry["seen_count"] >= threshold:
                entry["status"] = "ready_for_promotion"

        logger.info(
            f"soft_skills: {novel_count} novel, "
            f"{matched_count} already in taxonomy, "
            f"queue now has {len(queue)} entries"
        )

        cls._save_queue(queue, out_path=out_path)
        cls._save_queue_by_status(queue, out_path=out_path)
        return queue


# ══════════════════════════════════════════════════════════════════════════════
# Action Verb Extraction
# ══════════════════════════════════════════════════════════════════════════════

# Bullet-point line patterns in raw JD text
_BULLET_LINE_RE = re.compile(r"^[-*•–\u2022\u2013\u2014]|\d+[.)]\s")
# Strip bullet markers to get the text body
_BULLET_STRIP_RE = re.compile(r"^[-*•–\u2022\u2013\u2014\s]+|\d+[.)]\s+")
# First word of a line
_FIRST_WORD_RE = re.compile(r"^([a-zA-Z]+)")


def _extract_verb_candidates_from_record(
    record: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Extract action verb candidates from the record's raw_jd field.

    Strategy:
    1. Scan bullet-point lines for imperative verbs at line start.
    2. Also scan all words in the text for known verb forms (whole-word match).

    Returns list of {name (base form), seniority, source_url, scraped_date}.
    """
    candidates: List[Dict[str, Any]] = []
    raw_jd = record.get("raw_jd", "") or ""
    source_url = record.get("source_url", "")
    raw_ts = record.get("scraped_at", "")
    scraped_date = raw_ts[:10] if isinstance(raw_ts, str) and len(raw_ts) >= 10 else None

    seniority_map = VerbReader.get_seniority_map()
    seen: set[str] = set()  # canonical base forms already added

    def _add_verb(base: str) -> None:
        if base in seen:
            return
        seen.add(base)
        seniority = seniority_map.get(base)
        candidates.append(
            {
                "name": base,
                "seniority": seniority,
                "source_url": source_url,
                "scraped_date": scraped_date,
            }
        )

    # Pass 1: bullet-point line starts — imperative verb extraction
    for line in raw_jd.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if _BULLET_LINE_RE.match(stripped):
            body = _BULLET_STRIP_RE.sub("", stripped)
            m = _FIRST_WORD_RE.match(body)
            if m:
                word = m.group(1)
                base = VerbReader.resolve_form(word)
                if base:
                    _add_verb(base)
                else:
                    # Novel verb candidate — record as-is (lowercased)
                    key = word.lower()
                    if len(key) >= 3 and key not in seen:
                        seen.add(key)
                        candidates.append(
                            {
                                "name": key,
                                "seniority": None,
                                "source_url": source_url,
                                "scraped_date": scraped_date,
                            }
                        )

    # Pass 2: whole-word scan for known verb forms anywhere in the text
    all_forms = VerbReader.get_all_forms()
    for word in re.findall(r"\b[a-zA-Z]+\b", raw_jd):
        wl = word.lower()
        if wl in all_forms:
            base = VerbReader.resolve_form(wl)
            if base:
                _add_verb(base)

    return candidates


def _parse_verb_chunk(
    lines: List[str],
) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Worker: parse a chunk of JSONL lines and extract verb candidates."""
    candidates: List[Dict[str, Any]] = []
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
        if record.get("is_error"):
            skipped += 1
            continue
        records += 1
        candidates.extend(_extract_verb_candidates_from_record(record))
    return candidates, records, errors, skipped


class VerbProcessor:
    """Scan crawler JSONL output and build/update the verb occurrences queue.

    Known verbs (matched against verb-taxonomy.json) are tracked with their
    seniority level and occurrence counts. Novel verbs (extracted from bullet
    points but not in the taxonomy) are tracked separately for review.
    """

    _QUEUE_CACHE: Optional[Dict[str, dict]] = None

    @classmethod
    def invalidate_cache(cls) -> None:
        cls._QUEUE_CACHE = None

    @classmethod
    def _load_queue(cls) -> Dict[str, dict]:
        if cls._QUEUE_CACHE is not None:
            return cls._QUEUE_CACHE

        queue_path = cfg.get_abs_path("verb_taxonomy.queue_path")
        if queue_path and Path(queue_path).exists():
            with open(queue_path, "r", encoding="utf-8") as f:
                cls._QUEUE_CACHE = json.load(f)
            logger.info(
                f"verbs: loaded queue with {len(cls._QUEUE_CACHE)} entries"
            )
        else:
            cls._QUEUE_CACHE = {}
        return cls._QUEUE_CACHE

    @classmethod
    def _save_queue(
        cls, queue: Dict[str, dict], out_path: str | Path | None = None
    ) -> Path:
        queue_path = out_path or cfg.get_abs_path("verb_taxonomy.queue_path")
        cls._QUEUE_CACHE = queue
        return _save_json(queue, queue_path)

    @classmethod
    def _save_queue_by_status(
        cls, queue: Dict[str, dict], out_path: str | Path | None = None
    ) -> Dict[str, int]:
        configured = cfg.get_abs_path("verb_taxonomy.status_output_dir")
        if configured:
            status_dir = Path(configured)
        else:
            queue_path = Path(
                out_path or cfg.get_abs_path("verb_taxonomy.queue_path")
            )
            status_dir = queue_path.parent / f"{queue_path.stem}_statuses"

        status_dir.mkdir(parents=True, exist_ok=True)
        grouped: Dict[str, Dict[str, dict]] = {}
        for key, entry in queue.items():
            raw_status = str(entry.get("status", "unknown")).strip().lower()
            status = _STATUS_FILENAME_RE.sub("_", raw_status).strip("_") or "unknown"
            grouped.setdefault(status, {})[key] = entry

        expected_files = {f"{s}.json" for s in grouped}
        for existing in status_dir.glob("*.json"):
            if existing.name not in expected_files:
                existing.unlink()

        counts: Dict[str, int] = {}
        for status, entries in grouped.items():
            _save_json(entries, status_dir / f"{status}.json")
            counts[status] = len(entries)

        logger.info(f"verbs: wrote status files to {status_dir} ({counts})")
        return counts

    @classmethod
    def process_jsonl(
        cls, path: str, *, parallel: bool = True, out_path: str | Path | None = None
    ) -> Dict[str, dict]:
        """Scan JSONL file(s) for action verbs and update the verb queue.

        Each entry in the queue represents one verb (canonical base form) and
        tracks:
          - seen_count: total occurrences across all records
          - seniority: level from taxonomy (or None for novel verbs)
          - status: 'known' for taxonomy verbs, 'novel' for unrecognised ones
          - sample_sources: up to N source URLs

        Args:
            path: Path to a single .jsonl file or a directory.
            parallel: Use ProcessPoolExecutor when True.
            out_path: Override output queue path.
        """
        jsonl_paths = _resolve_jsonl_paths(path)
        if not jsonl_paths:
            logger.error(f"verbs: no JSONL files found at: {path}")
            return cls._load_queue()

        queue = cls._load_queue()
        max_sources = cfg.get("discovery.max_sample_sources", 10)
        fallback_date = date.today().isoformat()

        # ── Phase 1: Read lines ────────────────────────────────────────
        all_lines: List[str] = []
        for jp in jsonl_paths:
            with open(jp, "r", encoding="utf-8") as f:
                all_lines.extend(f.readlines())
            logger.info(f"verbs: read {jp.name}")

        # ── Phase 2: Parse & extract verb candidates ───────────────────
        all_candidates: List[Dict[str, Any]] = []
        record_count = error_count = skipped_count = 0

        if parallel and len(all_lines) >= _PARALLEL_THRESHOLD:
            chunks = _chunk_list(all_lines, _NUM_WORKERS)
            with ProcessPoolExecutor(max_workers=_NUM_WORKERS) as pool:
                for candidates, records, errors, skipped in pool.map(
                    _parse_verb_chunk, chunks
                ):
                    all_candidates.extend(candidates)
                    record_count += records
                    error_count += errors
                    skipped_count += skipped
        else:
            candidates, records, errors, skipped = _parse_verb_chunk(all_lines)
            all_candidates.extend(candidates)
            record_count += records
            error_count += errors
            skipped_count += skipped

        logger.info(
            f"verbs: scanned {record_count} records, "
            f"extracted {len(all_candidates)} raw candidates"
        )

        # ── Phase 3: Update queue (all verbs tracked, novel flagged) ───
        name_to_candidates: Dict[str, List[Dict]] = {}
        for c in all_candidates:
            name_to_candidates.setdefault(c["name"], []).append(c)

        known_count = novel_count = 0
        seniority_map = VerbReader.get_seniority_map()

        for name, entries in name_to_candidates.items():
            candidate_dates = [
                c["scraped_date"] for c in entries if c.get("scraped_date")
            ]
            earliest = min(candidate_dates) if candidate_dates else fallback_date
            latest = max(candidate_dates) if candidate_dates else fallback_date

            # Seniority from taxonomy; None means novel
            seniority = seniority_map.get(name.lower()) or (
                entries[0].get("seniority") if entries else None
            )
            is_known = seniority is not None

            if is_known:
                known_count += 1
            else:
                novel_count += 1

            key = name.lower().replace(" ", "_")
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
                    "seniority": seniority,
                    "sample_sources": [],
                    "status": "known" if is_known else "novel",
                }
                queue[key] = entry

            for c in entries:
                url = c.get("source_url", "")
                if (
                    url
                    and url not in entry["sample_sources"]
                    and len(entry["sample_sources"]) < max_sources
                ):
                    entry["sample_sources"].append(url)

        logger.info(
            f"verbs: {known_count} known (taxonomy), "
            f"{novel_count} novel, "
            f"queue now has {len(queue)} entries"
        )

        cls._save_queue(queue, out_path=out_path)
        cls._save_queue_by_status(queue, out_path=out_path)
        return queue

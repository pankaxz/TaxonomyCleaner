"""Soft-skill discovery processor.

Maintains the soft-skill queue independently from technical skills and verbs.
"""

import json
import logging
import re
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import cfg
from discovery.common.pipeline_common import (
    NUM_WORKERS,
    PARALLEL_THRESHOLD,
    STATUS_FILENAME_RE,
    chunk_list,
    resolve_jsonl_paths,
    save_json,
)
from discovery.soft_skills.reader import SoftSkillReader

logger = logging.getLogger(__name__)

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
        return save_json(queue, queue_path)

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
            status = STATUS_FILENAME_RE.sub("_", raw_status).strip("_") or "unknown"
            grouped.setdefault(status, {})[key] = entry

        expected_files = {f"{s}.json" for s in grouped}
        for existing in status_dir.glob("*.json"):
            if existing.name not in expected_files:
                existing.unlink()

        counts: Dict[str, int] = {}
        for status, entries in grouped.items():
            save_json(entries, status_dir / f"{status}.json")
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
        jsonl_paths = resolve_jsonl_paths(path, log_prefix="soft_skills")
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

        if parallel and len(all_lines) >= PARALLEL_THRESHOLD:
            chunks = chunk_list(all_lines, NUM_WORKERS)
            chunk_args = [(chunk, max_chars, max_words) for chunk in chunks]
            with ProcessPoolExecutor(max_workers=NUM_WORKERS) as pool:
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

        if parallel and len(unique_names) >= PARALLEL_THRESHOLD:
            name_chunks = chunk_list(unique_names, NUM_WORKERS)
            chunk_args = [(chunk, fuzzy_threshold) for chunk in name_chunks]
            matches: Dict[str, Optional[Tuple[str, str, float]]] = {}
            with ProcessPoolExecutor(max_workers=NUM_WORKERS) as pool:
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

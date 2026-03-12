"""Verb discovery processor.

Maintains verb occurrences independently from technical and soft-skill queues.
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
from discovery.VerbAnalysis.reader import VerbReader

logger = logging.getLogger(__name__)

import spacy

_nlp = None

def get_nlp():
    global _nlp
    if _nlp is None:
        _nlp = spacy.load("en_core_web_sm", disable=["ner", "textcat"])
    return _nlp


def _extract_verb_candidates_from_record(
    record: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Extract action verb candidates from the record's raw_jd field using spaCy.

    Extracts verbs and their associated direct/prepositional objects to form context strings.
    Returns list of {name (base form), seniority, source_url, scraped_date, [context_sample]}.
    """
    candidates: List[Dict[str, Any]] = []
    raw_jd = record.get("raw_jd", "") or ""
    source_url = record.get("source_url", "")
    raw_ts = record.get("scraped_at", "")
    scraped_date = raw_ts[:10] if isinstance(raw_ts, str) and len(raw_ts) >= 10 else None

    seniority_map = VerbReader.get_seniority_map()
    seen: set[str] = set()

    def _add_verb(base: str, context: Optional[str] = None) -> None:
        if base in seen:
            return
        seen.add(base)
        seniority = seniority_map.get(base)
        
        is_known = seniority is not None
        
        candidate = {
            "name": base,
            "seniority": seniority,
            "source_url": source_url,
            "scraped_date": scraped_date,
        }
        
        if not is_known and context:
            candidate["context_sample"] = context
            
        candidates.append(candidate)

    if not raw_jd:
        return candidates

    nlp = get_nlp()
    # Process text, chunking if too long to prevent spacy memory issues, though usually JDs are small enough
    doc = nlp(raw_jd)
    
    IGNORE_VERBS = {"be", "is", "are", "was", "were", "been", "being", "have", "has", "had", "do", "does", "did"}
    
    for token in doc:
        if token.pos_ == "VERB":
            lemma = token.lemma_.lower()
            if lemma in IGNORE_VERBS:
                continue
                
            context_string = None
            objects = []
            
            for child in token.children:
                if child.dep_ in ("dobj", "pobj"):
                    obj_phrase = " ".join([t.text for t in child.subtree]).strip()
                    objects.append(obj_phrase)
                elif child.dep_ == "prep":
                    for p_child in child.children:
                        if p_child.dep_ == "pobj":
                            prep_phrase = " ".join([t.text for t in child.subtree]).strip()
                            objects.append(prep_phrase)
                            
            if objects:
                context_string = f"{token.text} {objects[0]}"
            
            base = VerbReader.resolve_form(lemma)
            if base:
                _add_verb(base, context=None)
            else:
                if len(lemma) >= 3:
                    _add_verb(lemma, context=context_string)

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
        return save_json(queue, queue_path)

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
        jsonl_paths = resolve_jsonl_paths(path, log_prefix="verbs")
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

        if parallel and len(all_lines) >= PARALLEL_THRESHOLD:
            chunks = chunk_list(all_lines, NUM_WORKERS)
            with ProcessPoolExecutor(max_workers=NUM_WORKERS) as pool:
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

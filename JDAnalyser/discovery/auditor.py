"""Audit report — traces every skill candidate back to its source JD and JSONL file.

Read-only pass: does not modify the discovery queue or any existing state.
Reuses taxonomy matching from the discovery pipeline for consistency.
"""

import csv
import json
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import cfg
from discovery.dedup import SkillDeduplicator
from discovery.processor import (
    _chunk_list,
    _match_chunk,
    _NUM_WORKERS,
    _PARALLEL_THRESHOLD,
    _parse_skill_with_tag,
    _resolve_jsonl_paths,
    _save_json,
)

logger = logging.getLogger(__name__)


class DiscoveryAuditor:
    """Read-only audit: traces every skill candidate to its source JD and JSONL file."""

    @classmethod
    def audit(cls, path: str | None = None) -> dict:
        """Run full audit over JSONL file(s).

        Args:
            path: File or directory. Defaults to configured crawler.input_dir.

        Returns:
            The complete audit report dict.
        """
        if path is None:
            path = cfg.get_abs_path("crawler.input_dir")

        jsonl_paths = _resolve_jsonl_paths(path)
        if not jsonl_paths:
            logger.error(f"audit: no JSONL files found at: {path}")
            return {}

        # ── Phase 1: Scan JSONL files, build occurrence map ────────────
        skill_occurrences, skill_display_names, total_records, skipped = (
            cls._scan_files(jsonl_paths)
        )
        logger.info(
            f"audit: {total_records} records, "
            f"{len(skill_occurrences)} unique skills, "
            f"{skipped} error records skipped"
        )

        # ── Phase 2: Match against taxonomy ────────────────────────────
        fuzzy_threshold = cfg.get("discovery.fuzzy_threshold", 0.85)
        matches = cls._match_skills(
            skill_occurrences, skill_display_names, fuzzy_threshold
        )
        matched_count = sum(1 for m in matches.values() if m is not None)
        novel_count = len(matches) - matched_count
        logger.info(f"audit: {matched_count} taxonomy matches, {novel_count} novel")

        # ── Phase 3: Assemble and write report ─────────────────────────
        skills_list = cls._build_skills_list(
            skill_occurrences, skill_display_names, matches
        )

        report = {
            "meta": {
                "generated_at": datetime.now().isoformat(),
                "jsonl_files": [p.name for p in jsonl_paths],
                "total_records_scanned": total_records,
                "total_unique_skills": len(skill_occurrences),
                "taxonomy_matched": matched_count,
                "novel": novel_count,
                "skipped_error_records": skipped,
            },
            "skills": skills_list,
        }

        out_dir = Path(
            cfg.get_abs_path("discovery.audit_dir") or "data/discovery/audit"
        )
        report_path = out_dir / "audit_report.json"
        csv_path = out_dir / "audit_summary.csv"

        _save_json(report, report_path)
        cls._write_csv_summary(skills_list, csv_path)

        logger.info(f"audit: report → {report_path}")
        logger.info(f"audit: summary → {csv_path}")
        return report

    # ── Phase 1 internals ──────────────────────────────────────────────

    @classmethod
    def _scan_files(
        cls, jsonl_paths: list[Path]
    ) -> tuple[
        Dict[str, List[Dict[str, Any]]],
        Dict[str, str],
        int,
        int,
    ]:
        """Read every JSONL file and build per-skill occurrence lists.

        Returns:
            (skill_occurrences, skill_display_names, total_records, skipped)
        """
        skill_occurrences: Dict[str, List[Dict[str, Any]]] = {}
        skill_display_names: Dict[str, str] = {}
        total_records = 0
        skipped = 0

        for jp in jsonl_paths:
            fname = jp.name
            with open(jp, "r", encoding="utf-8") as f:
                for line_num, raw_line in enumerate(f, start=1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except Exception:
                        continue

                    # Skip records the scraper flagged as LLM extraction failures
                    if record.get("is_error"):
                        skipped += 1
                        continue

                    total_records += 1
                    cls._extract_occurrences(
                        record,
                        fname,
                        line_num,
                        skill_occurrences,
                        skill_display_names,
                    )

            logger.info(f"audit: scanned {fname}")

        return skill_occurrences, skill_display_names, total_records, skipped

    @staticmethod
    def _extract_occurrences(
        record: Dict[str, Any],
        jsonl_file: str,
        line_number: int,
        skill_occurrences: Dict[str, List[Dict[str, Any]]],
        skill_display_names: Dict[str, str],
    ) -> None:
        """Extract skill occurrences from one record into the shared maps."""
        title = record.get("title", "")
        source_url = record.get("source_url", "")

        # Build lowered set of unmapped skill names for cross-reference
        eq = record.get("extraction_quality", {})
        unmapped_raw = eq.get("unmapped_skills", []) if isinstance(eq, dict) else []
        unmapped_lower: set[str] = set()
        for s in unmapped_raw or []:
            if isinstance(s, str) and s.strip():
                unmapped_lower.add(s.strip().lower())

        # Avoid double-counting within one record
        seen_in_record: set[str] = set()

        # All technical_skills — always checked, never trust scraper mapping
        for raw_skill in record.get("technical_skills", []) or []:
            if not isinstance(raw_skill, str):
                continue
            name, _tag = _parse_skill_with_tag(raw_skill)
            if not name:
                continue
            key = name.lower()
            if key in seen_in_record:
                continue
            seen_in_record.add(key)

            skill_display_names.setdefault(key, name)
            skill_occurrences.setdefault(key, []).append(
                {
                    "jsonl_file": jsonl_file,
                    "line_number": line_number,
                    "job_title": title,
                    "source_url": source_url,
                    "raw_skill_string": raw_skill,
                    "from_unmapped": key in unmapped_lower,
                }
            )

        # Unmapped skills not already covered by technical_skills
        for skill_name in unmapped_raw or []:
            if not isinstance(skill_name, str):
                continue
            name = skill_name.strip()
            if not name:
                continue
            key = name.lower()
            if key in seen_in_record:
                continue
            seen_in_record.add(key)

            skill_display_names.setdefault(key, name)
            skill_occurrences.setdefault(key, []).append(
                {
                    "jsonl_file": jsonl_file,
                    "line_number": line_number,
                    "job_title": title,
                    "source_url": source_url,
                    "raw_skill_string": name,
                    "from_unmapped": True,
                }
            )

    # ── Phase 2 internals ──────────────────────────────────────────────

    @classmethod
    def _match_skills(
        cls,
        skill_occurrences: Dict[str, list],
        skill_display_names: Dict[str, str],
        fuzzy_threshold: float,
    ) -> Dict[str, Optional[Tuple[str, str, float]]]:
        """Match every unique skill against the taxonomy (parallel)."""
        all_display_names = list(
            {skill_display_names[k] for k in skill_occurrences}
        )

        if len(all_display_names) >= _PARALLEL_THRESHOLD:
            name_chunks = _chunk_list(all_display_names, _NUM_WORKERS)
            chunk_args = [(chunk, fuzzy_threshold) for chunk in name_chunks]
            matches_by_name: Dict[str, Optional[Tuple[str, str, float]]] = {}
            with ProcessPoolExecutor(max_workers=_NUM_WORKERS) as pool:
                for chunk_result in pool.map(_match_chunk, chunk_args):
                    matches_by_name.update(chunk_result)
        else:
            matches_by_name = SkillDeduplicator.find_match_batch(
                all_display_names, fuzzy_threshold
            )

        return {
            key: matches_by_name.get(skill_display_names[key])
            for key in skill_occurrences
        }

    # ── Phase 3 internals ──────────────────────────────────────────────

    @staticmethod
    def _build_skills_list(
        skill_occurrences: Dict[str, List[Dict[str, Any]]],
        skill_display_names: Dict[str, str],
        matches: Dict[str, Optional[Tuple[str, str, float]]],
    ) -> List[dict]:
        """Build the skills list sorted by total count descending."""
        skills_list = []
        for key in sorted(
            skill_occurrences,
            key=lambda k: len(skill_occurrences[k]),
            reverse=True,
        ):
            occurrences = skill_occurrences[key]
            match = matches.get(key)

            taxonomy_match = None
            if match:
                taxonomy_match = {
                    "canonical": match[0],
                    "match_type": match[1],
                    "confidence": match[2],
                }

            skills_list.append(
                {
                    "skill_name": skill_display_names[key],
                    "queue_key": key.replace(" ", "_"),
                    "total_count": len(occurrences),
                    "taxonomy_match": taxonomy_match,
                    "occurrences": occurrences,
                }
            )

        return skills_list

    @staticmethod
    def _write_csv_summary(skills: List[dict], path: Path) -> None:
        """Write flat CSV for quick spreadsheet review."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "skill_name",
                    "total_count",
                    "taxonomy_match",
                    "match_type",
                    "confidence",
                    "top_job_titles",
                    "jsonl_files",
                ]
            )
            for skill in skills:
                match = skill["taxonomy_match"]

                # Top 3 unique job titles for context
                titles: list[str] = []
                seen_titles: set[str] = set()
                for occ in skill["occurrences"]:
                    t = occ["job_title"]
                    if t and t not in seen_titles:
                        seen_titles.add(t)
                        titles.append(t)
                    if len(titles) == 3:
                        break

                files = sorted(
                    {occ["jsonl_file"] for occ in skill["occurrences"]}
                )

                writer.writerow(
                    [
                        skill["skill_name"],
                        skill["total_count"],
                        match["canonical"] if match else "",
                        match["match_type"] if match else "",
                        match["confidence"] if match else "",
                        " | ".join(titles),
                        " | ".join(files),
                    ]
                )
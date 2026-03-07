"""Skill deduplication against the existing taxonomy."""

import logging
import re
from difflib import SequenceMatcher
from typing import Dict, Optional, Tuple

from config import cfg
from discovery.taxonomy import TaxonomyReader

logger = logging.getLogger(__name__)


class SkillDeduplicator:
    """Check candidates against existing taxonomy using 4-tier matching.

    Tiers (in priority order):
        1. Exact      (confidence 1.0)  — case-insensitive alias map lookup
        2. Normalized (0.95)            — strip punctuation, collapse whitespace
        3. Fuzzy      (>=threshold)     — difflib SequenceMatcher ratio
        4. Containment (0.80)           — canonical is substring of candidate
    """

    _CACHE: Dict[str, Optional[dict]] = {}
    _STRIP_RE = re.compile(r"[^a-z0-9\s#+.]")

    @classmethod
    def invalidate_cache(cls) -> None:
        cls._CACHE.clear()

    @classmethod
    def _strip_punctuation(cls, text: str) -> str:
        """Aggressive normalize: lowercase, strip punctuation except #/+/."""
        text = text.lower()
        text = cls._STRIP_RE.sub("", text)
        return re.sub(r"\s+", " ", text).strip()

    @classmethod
    def find_match(
        cls,
        candidate: str,
        fuzzy_threshold: float | None = None,
    ) -> Optional[Tuple[str, str, float]]:
        """Find a taxonomy match for a candidate skill.

        Returns:
            (canonical, match_type, confidence) or None if novel.
        """
        if fuzzy_threshold is None:
            fuzzy_threshold = cfg.get("discovery.fuzzy_threshold", 0.85)

        cache_key = candidate.lower()
        if cache_key in cls._CACHE:
            cached = cls._CACHE[cache_key]
            if cached is None:
                return None
            return (cached["canonical"], cached["match_type"], cached["confidence"])

        result = cls._match_internal(candidate, fuzzy_threshold)
        cls._CACHE[cache_key] = (
            {"canonical": result[0], "match_type": result[1], "confidence": result[2]}
            if result
            else None
        )
        return result

    @classmethod
    def _match_internal(
        cls,
        candidate: str,
        fuzzy_threshold: float,
    ) -> Optional[Tuple[str, str, float]]:
        alias_map = TaxonomyReader.get_alias_map()
        canonicals = TaxonomyReader.get_all_canonicals()
        group_names = TaxonomyReader.get_group_names()

        # Tier 0: Group-name exact match (case-insensitive)
        normalized = candidate.lower()
        if normalized in group_names:
            return (normalized, "group_exact", 1.0)

        # Tier 1: Exact canonical/alias match (case-insensitive)
        if normalized in alias_map:
            return (alias_map[normalized], "exact", 1.0)

        # Tier 2: Stripped-punctuation match
        stripped = cls._strip_punctuation(candidate)
        for alias_key, canonical in alias_map.items():
            if cls._strip_punctuation(alias_key) == stripped:
                return (canonical, "normalized", 0.95)

        # Tier 3: Fuzzy match (SequenceMatcher)
        best_ratio = 0.0
        best_canonical = None
        for alias_key, canonical in alias_map.items():
            ratio = SequenceMatcher(None, normalized, alias_key).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_canonical = canonical
        if best_ratio >= fuzzy_threshold and best_canonical:
            return (best_canonical, "fuzzy", round(best_ratio, 3))

        # Tier 4: Containment — canonical is a substring of candidate
        for canonical in canonicals:
            canon_lower = canonical.lower()
            if (
                len(canon_lower) >= 3
                and canon_lower in normalized
                and canon_lower != normalized
            ):
                return (canon_lower, "containment", 0.80)

        return None

    @classmethod
    def find_match_batch(
        cls,
        candidates: list[str],
        fuzzy_threshold: float | None = None,
    ) -> Dict[str, Optional[Tuple[str, str, float]]]:
        """Match a batch of candidates. Returns {candidate: match_or_None}."""
        return {c: cls.find_match(c, fuzzy_threshold) for c in candidates}

"""Read-only access to the verb taxonomy (verb-taxonomy.json).

Format:
    { "seniority_level": { "canonical_verb": ["alias1", "alias2"], ... }, ... }

Seniority levels: junior, mid, senior, executive
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set

from config import cfg

logger = logging.getLogger(__name__)

_DEFAULT_PATH = Path(__file__).parent.parent / "input" / "Taxonomy" / "verb-taxonomy.json"


class VerbReader:
    """Read-only access to verb-taxonomy.json.

    The 'group' concept maps to seniority level (junior/mid/senior/executive).
    """

    _TAXONOMY: Optional[Dict] = None
    _ALIAS_MAP: Optional[Dict[str, str]] = None   # lowered alias/canonical -> lowered canonical
    _SENIORITY_MAP: Optional[Dict[str, str]] = None  # lowered canonical -> seniority level
    _ALL_FORMS: Optional[Set[str]] = None  # canonical + aliases + common verb forms
    _CANONICALS: Optional[List[str]] = None

    @classmethod
    def invalidate(cls) -> None:
        cls._TAXONOMY = None
        cls._ALIAS_MAP = None
        cls._SENIORITY_MAP = None
        cls._ALL_FORMS = None
        cls._CANONICALS = None

    @classmethod
    def _load(cls) -> Dict:
        if cls._TAXONOMY is not None:
            return cls._TAXONOMY

        path_str = cfg.get_abs_path("verb_taxonomy.path")
        path = Path(path_str) if path_str else _DEFAULT_PATH
        try:
            with open(path, "r", encoding="utf-8") as f:
                cls._TAXONOMY = json.load(f)
            n = sum(len(v) for v in cls._TAXONOMY.values())
            logger.info(f"verb_reader: loaded {n} verbs from {path}")
        except Exception as e:
            logger.error(f"verb_reader: failed to load {path}: {e}")
            cls._TAXONOMY = {}
        return cls._TAXONOMY

    @classmethod
    def get_alias_map(cls) -> Dict[str, str]:
        """Returns {lowered_alias_or_canonical: lowered_canonical}."""
        if cls._ALIAS_MAP is not None:
            return cls._ALIAS_MAP

        alias_map: Dict[str, str] = {}
        for _level, verbs in cls._load().items():
            for canonical, aliases in verbs.items():
                lc = canonical.lower()
                alias_map[lc] = lc
                for alias in aliases:
                    alias_map[alias.lower()] = lc
        cls._ALIAS_MAP = alias_map
        return alias_map

    @classmethod
    def get_seniority_map(cls) -> Dict[str, str]:
        """Returns {lowered_canonical: seniority_level}."""
        if cls._SENIORITY_MAP is not None:
            return cls._SENIORITY_MAP

        seniority_map: Dict[str, str] = {}
        for level, verbs in cls._load().items():
            for canonical in verbs:
                seniority_map[canonical.lower()] = level
        cls._SENIORITY_MAP = seniority_map
        return seniority_map

    @classmethod
    def get_all_canonicals(cls) -> List[str]:
        if cls._CANONICALS is not None:
            return cls._CANONICALS

        cls._CANONICALS = [
            canonical
            for verbs in cls._load().values()
            for canonical in verbs
        ]
        return cls._CANONICALS

    @classmethod
    def get_all_forms(cls) -> Set[str]:
        """All canonical verbs + aliases + common conjugated forms (-s, -ing, -ed, -d).

        Used for whole-word scanning of raw JD text.
        """
        if cls._ALL_FORMS is not None:
            return cls._ALL_FORMS

        forms: Set[str] = set()
        alias_map = cls.get_alias_map()
        for word in alias_map:
            forms.add(word)
            forms.add(word + "s")
            forms.add(word + "d")
            forms.add(word + "ed")
            if word.endswith("e"):
                forms.add(word[:-1] + "ing")
            else:
                forms.add(word + "ing")
        cls._ALL_FORMS = forms
        return forms

    @classmethod
    def resolve_form(cls, word: str) -> Optional[str]:
        """Resolve a conjugated verb form to its canonical base, or None.

        Tries: exact, strip-s, strip-d/ed, strip-ing (with/without 'e' reinsert).
        """
        alias_map = cls.get_alias_map()
        w = word.lower()
        if w in alias_map:
            return alias_map[w]
        # strip trailing 's' (leads/lead)
        if w.endswith("s") and len(w) > 3 and w[:-1] in alias_map:
            return alias_map[w[:-1]]
        # strip 'ed' (architected/architect)
        if w.endswith("ed") and len(w) > 4:
            stem = w[:-2]
            if stem in alias_map:
                return alias_map[stem]
            # double-consonant: collaborated -> collaborate
            if stem + "e" in alias_map:
                return alias_map[stem + "e"]
        # strip 'd' (standardized/standardize)
        if w.endswith("d") and len(w) > 3 and w[:-1] in alias_map:
            return alias_map[w[:-1]]
        # strip 'ing' (architecting/architect)
        if w.endswith("ing") and len(w) > 5:
            stem = w[:-3]
            if stem in alias_map:
                return alias_map[stem]
            # 'e' was dropped: driving -> drive
            if stem + "e" in alias_map:
                return alias_map[stem + "e"]
        return None
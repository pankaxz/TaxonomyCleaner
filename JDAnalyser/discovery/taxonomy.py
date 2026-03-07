"""Lightweight taxonomy reader — reads DataFactory's canonical_data.json directly."""

import json
import logging
from typing import Dict, List, Optional

from config import cfg

logger = logging.getLogger(__name__)


class TaxonomyReader:
    """Read-only access to the canonical taxonomy.

    Loads canonical_data.json once and builds alias/group lookup maps.
    No dependency on DataFactory code — just reads the JSON file.
    """

    _TAXONOMY: Optional[Dict] = None
    _ALIAS_MAP: Optional[Dict[str, str]] = None
    _GROUP_MAP: Optional[Dict[str, str]] = None
    _GROUP_NAMES: Optional[set[str]] = None
    _CANONICALS: Optional[List[str]] = None

    @classmethod
    def invalidate(cls) -> None:
        cls._TAXONOMY = None
        cls._ALIAS_MAP = None
        cls._GROUP_MAP = None
        cls._GROUP_NAMES = None
        cls._CANONICALS = None

    @classmethod
    def _load(cls) -> Dict:
        if cls._TAXONOMY is not None:
            return cls._TAXONOMY

        path = cfg.get_abs_path("taxonomy.canonical_data")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cls._TAXONOMY = json.load(f)
            n = sum(len(v) for v in cls._TAXONOMY.values())
            logger.info(f"taxonomy: loaded {n} skills from {path}")
        except Exception as e:
            logger.error(f"taxonomy: failed to load {path}: {e}")
            cls._TAXONOMY = {}
        return cls._TAXONOMY

    @classmethod
    def get_alias_map(cls) -> Dict[str, str]:
        """Returns {lowered_alias_or_canonical: lowered_canonical}."""
        if cls._ALIAS_MAP is not None:
            return cls._ALIAS_MAP

        taxonomy = cls._load()
        alias_map: Dict[str, str] = {}
        for _group, skills in taxonomy.items():
            for canonical, aliases in skills.items():
                lc = canonical.lower()
                alias_map[lc] = lc
                for alias in aliases:
                    alias_map[alias.lower()] = lc
        cls._ALIAS_MAP = alias_map
        return alias_map

    @classmethod
    def get_group_map(cls) -> Dict[str, str]:
        """Returns {lowered_canonical: group_name}."""
        if cls._GROUP_MAP is not None:
            return cls._GROUP_MAP

        taxonomy = cls._load()
        group_map: Dict[str, str] = {}
        for group, skills in taxonomy.items():
            for canonical in skills:
                group_map[canonical.lower()] = group
        cls._GROUP_MAP = group_map
        return group_map

    @classmethod
    def get_group_names(cls) -> set[str]:
        """Returns lowered group names from taxonomy keys."""
        if cls._GROUP_NAMES is not None:
            return cls._GROUP_NAMES

        taxonomy = cls._load()
        cls._GROUP_NAMES = {str(group).lower() for group in taxonomy.keys()}
        return cls._GROUP_NAMES

    @classmethod
    def get_all_canonicals(cls) -> List[str]:
        if cls._CANONICALS is not None:
            return cls._CANONICALS

        taxonomy = cls._load()
        cls._CANONICALS = [
            canonical for skills in taxonomy.values() for canonical in skills
        ]
        return cls._CANONICALS

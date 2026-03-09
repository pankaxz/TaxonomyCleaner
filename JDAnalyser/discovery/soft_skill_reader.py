"""Read-only access to the soft skill taxonomy (soft_skill_taxonomy.json).

Format:
    { "GroupName": { "canonical_skill": ["alias1", "alias2"], ... }, ... }
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from config import cfg

logger = logging.getLogger(__name__)

_DEFAULT_PATH = Path(__file__).parent.parent / "input" / "Taxonomy" / "soft_skill_taxonomy.json"


class SoftSkillReader:
    """Read-only access to soft_skill_taxonomy.json.

    Builds alias and group lookup maps on first access.
    """

    _TAXONOMY: Optional[Dict] = None
    _ALIAS_MAP: Optional[Dict[str, str]] = None   # lowered alias/canonical -> lowered canonical
    _GROUP_MAP: Optional[Dict[str, str]] = None   # lowered canonical -> group name
    _CANONICALS: Optional[List[str]] = None

    @classmethod
    def invalidate(cls) -> None:
        cls._TAXONOMY = None
        cls._ALIAS_MAP = None
        cls._GROUP_MAP = None
        cls._CANONICALS = None

    @classmethod
    def _load(cls) -> Dict:
        if cls._TAXONOMY is not None:
            return cls._TAXONOMY

        path_str = cfg.get_abs_path("soft_skill_taxonomy.path")
        path = Path(path_str) if path_str else _DEFAULT_PATH
        try:
            with open(path, "r", encoding="utf-8") as f:
                cls._TAXONOMY = json.load(f)
            n = sum(len(v) for v in cls._TAXONOMY.values())
            logger.info(f"soft_skill_reader: loaded {n} skills from {path}")
        except Exception as e:
            logger.error(f"soft_skill_reader: failed to load {path}: {e}")
            cls._TAXONOMY = {}
        return cls._TAXONOMY

    @classmethod
    def get_alias_map(cls) -> Dict[str, str]:
        """Returns {lowered_alias_or_canonical: lowered_canonical}."""
        if cls._ALIAS_MAP is not None:
            return cls._ALIAS_MAP

        alias_map: Dict[str, str] = {}
        for _group, skills in cls._load().items():
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

        group_map: Dict[str, str] = {}
        for group, skills in cls._load().items():
            for canonical in skills:
                group_map[canonical.lower()] = group
        cls._GROUP_MAP = group_map
        return group_map

    @classmethod
    def get_all_canonicals(cls) -> List[str]:
        if cls._CANONICALS is not None:
            return cls._CANONICALS

        cls._CANONICALS = [
            canonical
            for skills in cls._load().values()
            for canonical in skills
        ]
        return cls._CANONICALS
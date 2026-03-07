"""
Stage 0 Rules
=============

This module contains hard-coded configuration and blocking rules for the deterministic pre-cleaning stage.
It serves as a "safety valve" or "manual override" system for the automated cleanup process.

Primary responsibilities:
1.  Defining "Hard Block" pairs: Specific Canonical-Alias checks that are known to be incorrect
    but might slip through other heuristics (e.g., phonetically similar but distinct libraries).
"""

from __future__ import annotations
import os
import json
from typing import Set, Tuple
from ...shared.utilities import normalize_term

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../../.."))
HARD_BLOCK_PAIRS_FILE = os.path.join(PROJECT_ROOT, "Input/hard_block_alias_pairs.json")


def _load_hard_block_pairs() -> Set[Tuple[str, str]]:
    """Loads and normalizes the hard block configuration."""
    block_pairs: Set[Tuple[str, str]] = set()
    if not os.path.exists(HARD_BLOCK_PAIRS_FILE):
        return block_pairs

    try:
        with open(HARD_BLOCK_PAIRS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                for canonical, alias in data.items():
                    # Normalize both sides to ensure strict matching against the pipeline's normalized terms
                    norm_canonical = normalize_term(str(canonical))
                    norm_alias = normalize_term(str(alias))
                    block_pairs.add((norm_canonical, norm_alias))
    except Exception as e:
        print(f"Warning: Failed to load hard block pairs from {HARD_BLOCK_PAIRS_FILE}: {e}")

    return block_pairs


HARD_BLOCK_ALIAS_PAIRS = _load_hard_block_pairs()


def is_hard_blocked_alias(canonical_normalized: str, alias_normalized: str) -> bool:
    """
    Check if a specific (Canonical, Alias) pair is explicitly forbidden.

    This function is called during the alias validation loop in `stage.py`.
    If it returns True, the alias is dropped immediately without further processing,
    and a specific finding (L1-008) is generated.

    Args:
        canonical_normalized (str): The normalized string of the canonical term.
        alias_normalized (str): The normalized string of the potential alias.

    Returns:
        bool: True if the pair is in the blocklist, False otherwise.
    """
    return (canonical_normalized, alias_normalized) in HARD_BLOCK_ALIAS_PAIRS

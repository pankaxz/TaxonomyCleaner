from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Set
from typing import Tuple

# -----------------------------------------------------------------------------
# CONSTANTS & PATTERNS
# -----------------------------------------------------------------------------

# Prefixes that indicate a term might be too generic or context-dependent
# (e.g., "Advanced Java" vs just "Java"). Used in atomicity checks.
CONTEXTUAL_PREFIXES = (
    "core ",
    "advanced ",
    "basic ",
    "modern ",
    "enterprise ",
    "backend ",
    "frontend ",
)

# Regex to detect version numbers or version-like tokens (v1, 2.0, 2024, 64bit).
# These are often metadata attached to a skill rather than the skill itself.
VERSION_PATTERN = re.compile(
    r"\b(v\d+|\d+\.\d+|\d{4}|\d+\s*bit|es\d+|python\s*\d+)\b",
    re.IGNORECASE,
)

# Regex to detect the word "and" as a holistic term separator.
AND_PATTERN = re.compile(r"\band\b", re.IGNORECASE)

# Regex to capture content inside parentheses at the end of a string.
# Example: "Java (Programming Language)" -> captures "Programming Language"
PARENS_PATTERN = re.compile(r"\(([^)]+)\)")


# -----------------------------------------------------------------------------
# STRING NORMALIZATION
# -----------------------------------------------------------------------------

def normalize_term(value: str) -> str:
    """
    Standardizes a term string for comparison or storage.

    Performs the following operations:
    1. Trims leading/trailing whitespace.
    2. Converts to lowercase.
    3. Collapses multiple internal spaces into a single space.

    Args:
        value (str): The raw string to normalize.

    Returns:
        str: The normalized string.
    """
    stripped = value.strip().lower()
    normalized = re.sub(r"\s+", " ", stripped)
    return normalized


def normalize_for_key(value: str) -> str:
    """
    Alias for normalize_term, specifically intended for dictionary keys.
    
    This function exists to make intent explicit when a string is being
    prepared for use as a map key.

    Args:
        value (str): The raw string key.

    Returns:
        str: The normalized key string.
    """
    return normalize_term(value)


# -----------------------------------------------------------------------------
# FILE I/O & HASHING
# -----------------------------------------------------------------------------

def stable_hash_file(path: str) -> str:
    """
    Computes a SHA-256 hash of a file's contents in a memory-efficient way.

    Reads the file in 8KB chunks so large files do not consume excessive RAM.
    This hash is used to detect changes in input files (e.g., for caching results).

    Args:
        path (str): The absolute path to the file.

    Returns:
        str: The hexadecimal SHA-256 digest string.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(8192)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def load_json_file(path: str) -> Any:
    """
    Loads and parses a JSON file from disk.

    Args:
        path (str): The file path to read.

    Returns:
        Any: The parsed JSON data (dict, list, etc.).
    """
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data


def write_json(path: str, payload: Any) -> None:
    """
    Writes data to a JSON file with pretty-printing.

    Ensures the target directory exists before writing.
    Sorts keys to ensure deterministic output (important for diffing).

    Args:
        path (str): The target file path.
        payload (Any): The JSON-serializable data to write.
    """
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")  # Ensure file ends with a newline


def write_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    """
    Writes a list of dictionaries to a JSON Lines (JSONL) file.

    Each item is written as a separate line of valid JSON.
    Useful for large datasets or streaming.

    Args:
        path (str): The target file path.
        rows (Iterable[Dict[str, Any]]): The sequence of row objects to write.
    """
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)

    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            encoded = json.dumps(row, sort_keys=True)
            handle.write(encoded)
            handle.write("\n")


# -----------------------------------------------------------------------------
# TAXONOMY UTILITIES
# -----------------------------------------------------------------------------

def flatten_canonicals(store: Dict[str, Dict[str, List[str]]]) -> List[str]:
    """
    Extracts a flat list of all canonical terms from the hierarchical store.

    The store structure is Group -> Canonical -> [Aliases].
    This function ignores groups and aliases, returning only the unique canonical keys.

    Args:
        store (Dict[str, Dict[str, List[str]]]): The hierarchical taxonomy store.

    Returns:
        List[str]: A sorted list of all canonical terms.
    """
    terms: List[str] = []

    for group in sorted(store):
        canonicals = store[group]
        for canonical in sorted(canonicals):
            terms.append(canonical)

    return terms


def find_parenthetical_split(value: str) -> Tuple[str, str] | None:
    """
    Suggests a split for terms formatted like "Base (Alias)".

    Example:
        Input: "Python (Programming Language)"
        Output: ("Python", "Programming Language")

    This is used to automatically decompose complex terms into a cleaner canonical
    and a useful alias.

    Args:
        value (str): The term string to analyze.

    Returns:
        Tuple[str, str] | None: 
            - (Base Name, Content Inside Parens) if a valid split is found.
            - None if the pattern doesn't match or the resulting parts are empty.
    """
    match = PARENS_PATTERN.search(value)
    if not match:
        return None

    alias = match.group(1).strip()
    # Remove the parenthetical part from the original string to get the base term
    base = PARENS_PATTERN.sub("", value).strip()

    if not base:
        return None
    if not alias:
        return None
    return base, alias


def contains_atomicity_violation(value: str) -> List[str]:
    """
    Checks if a term violates "atomicity" rules (i.e., is it a compound or unclean term?).

    A term is considered non-atomic if it contains:
    - Separators like "/" or "," or " and "
    - Parentheses (indicating metadata or disambiguation)
    - Contextual prefixes (e.g., "Advanced")
    - Version numbers

    Args:
        value (str): The term to check.

    Returns:
        List[str]: A list of reason codes for why the term is not atomic. 
                   Empty list implies the term is atomic.
    """
    reasons: List[str] = []
    normalized = normalize_term(value)

    if "/" in value:
        reasons.append("slash")
    if "," in value:
        reasons.append("comma")
    if "(" in value or ")" in value:
        reasons.append("parentheses")
    if AND_PATTERN.search(normalized):
        reasons.append("and")

    for prefix in CONTEXTUAL_PREFIXES:
        if normalized.startswith(prefix):
            reasons.append("contextual_prefix")
            break

    if VERSION_PATTERN.search(normalized):
        reasons.append("version_like")

    return reasons


def contains_version_token(value: str) -> bool:
    """
    Checks if a string contains what looks like a version number.

    Wraps the VERSION_PATTERN regex check.

    Args:
        value (str): The string/alias to check.

    Returns:
        bool: True if a version token is found, False otherwise.
    """
    normalized = normalize_term(value)
    match = VERSION_PATTERN.search(normalized)
    return bool(match)


def explicit_split_tokens(original_term: str) -> Set[str]:
    """
    Splits a compound term into its constituent parts based on common separators.

    Separators include: "/", ",", "+", "&", and the word "and".
    Parentheses are treated as spaces during this split.

    Example:
        Input: "C++/C#"
        Output: {"c++", "c#"}

    Args:
        original_term (str): The complex term string.

    Returns:
        Set[str]: A set of normalized, individual token strings found in the term.
    """
    lowered = normalize_term(original_term)
    lowered = lowered.replace("(", " ")
    lowered = lowered.replace(")", " ")

    # Split on slash, comma, plus, ampersand, or the word "and"
    pieces = re.split(r"[\/,+&]|\band\b", lowered)
    tokens: Set[str] = set()

    for piece in pieces:
        normalized_piece = normalize_term(piece)
        if normalized_piece:
            tokens.add(normalized_piece)

    return tokens


# -----------------------------------------------------------------------------
# STRING SIMILARITY & SEARCH
# -----------------------------------------------------------------------------

def build_inverted_index(terms: Iterable[str]) -> Dict[str, Set[str]]:
    """
    Builds a character n-gram inverted index for a collection of terms.

    This index maps n-grams (substrings of length N) to the set of Terms that contain them.
    It enables fast candidate retrieval for similarity searches.

    Args:
        terms (Iterable[str]): A collection of terms to index.

    Returns:
        Dict[str, Set[str]]: A dictionary where key=n-gram, value=Set of terms.
    """
    index: Dict[str, Set[str]] = defaultdict(set)

    for term in terms:
        normalized = normalize_term(term)
        grams = char_ngrams(normalized)

        for gram in grams:
            index[gram].add(term)

    return index


def char_ngrams(term: str, n: int = 3) -> Set[str]:
    """
    Generates a set of character n-grams for a given string.

    This function replaces spaces with underscores ('_') to capture word boundaries
    distinctly. Ideally suited for short text similarity (like skill names).

    Example N=3: "data" -> {"dat", "ata"}

    Args:
        term (str): The input string.
        n (int): The length of the n-grams (default 3).

    Returns:
        Set[str]: A set of unique n-gram substrings.
    """
    compact = term.replace(" ", "_")
    
    # If the term is shorter than N, return it as a single gram
    if len(compact) < n:
        return {compact}

    grams: Set[str] = set()
    limit = len(compact) - n + 1
    for index in range(limit):
        grams.add(compact[index : index + n])

    return grams


def cosine_similarity_sparse(left_vector: Dict[str, float], right_vector: Dict[str, float]) -> float:
    """
    Computes cosine similarity between two sparse vectors.

    Cosine Similarity = (A . B) / (||A|| * ||B||)
    
    The vectors are represented as dictionaries where keys are dimensions (e.g., n-grams)
    and values are weights (e.g., 1.0).

    Args:
        left_vector (Dict[str, float]): The first vector.
        right_vector (Dict[str, float]): The second vector.

    Returns:
        float: A similarity score between 0.0 (orthogonal) and 1.0 (identical).
               Returns 0.0 if either vector is empty or has zero magnitude.
    """
    if not left_vector:
        return 0.0
    if not right_vector:
        return 0.0

    # Calculate Dot Product
    dot_product = 0.0
    for token, left_value in left_vector.items():
        right_value = right_vector.get(token, 0.0)
        dot_product += left_value * right_value

    if dot_product == 0.0:
        return 0.0

    # Calculate Magnitude of Left Vector
    norm_left = 0.0
    for value in left_vector.values():
        norm_left += value * value
    norm_left = norm_left ** 0.5

    # Calculate Magnitude of Right Vector
    norm_right = 0.0
    for value in right_vector.values():
        norm_right += value * value
    norm_right = norm_right ** 0.5

    if norm_left == 0.0:
        return 0.0
    if norm_right == 0.0:
        return 0.0

    similarity = dot_product / (norm_left * norm_right)
    return similarity


def ngram_vector(term: str) -> Dict[str, float]:
    """
    Converts a string into a sparse n-gram vector.

    Used to prepare terms for cosine similarity comparison.
    Each unique n-gram in the term gets a weight of 1.0.

    Args:
        term (str): The input string.

    Returns:
        Dict[str, float]: A sparse vector representation (gram -> 1.0).
    """
    normalized_term = normalize_term(term)
    grams = char_ngrams(normalized_term)

    vector: Dict[str, float] = {}
    for gram in grams:
        vector[gram] = 1.0

    return vector

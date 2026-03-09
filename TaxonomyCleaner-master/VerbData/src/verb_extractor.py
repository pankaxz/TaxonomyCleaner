import re
from typing import Dict, Set


class VerbExtractor:
    @staticmethod
    def _build_scan_regex() -> re.Pattern:
        parts = []


    def extract_verbs(record : Dict) -> Dict[str, Set[str]]:
        """Extract verbs from a JD record's raw_jd field."""
        return VerbExtractor.extract_from_text(record.get("raw_jd", ""))

"""Global configuration loader. Exposes the `cfg` singleton."""

import os

import yaml


class Config:
    def __init__(self, config_path: str = "config/settings.yaml") -> None:
        self._base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # If running from JDAnalyser/ directory, _base is JDAnalyser/
        # If the yaml isn't found relative to _base, try relative to this file's dir
        full_path = os.path.join(self._base, config_path)
        if not os.path.exists(full_path):
            full_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "settings.yaml"
            )
        with open(full_path, "r") as f:
            self._data: dict = yaml.safe_load(f)

    def get(self, key: str, default=None):
        """Dot-notation key access, e.g. 'discovery.queue_path'."""
        keys = key.split(".")
        node = self._data
        for k in keys:
            if not isinstance(node, dict):
                return default
            node = node.get(k, default)
        return node

    def get_abs_path(self, key: str) -> str | None:
        """Return an absolute path for a config key, or None if not set.

        If the stored value is already absolute, return as-is.
        Otherwise, resolve relative to the project root (JDAnalyser/).
        """
        rel = self.get(key)
        if rel is None:
            return None
        if os.path.isabs(rel):
            return rel
        return os.path.join(self._base, rel)


cfg = Config()

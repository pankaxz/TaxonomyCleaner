from __future__ import annotations

from .runner.pipeline_runner import build_artifact_paths
from .runner.pipeline_runner import load_atomicity_exceptions
from .runner.pipeline_runner import main
from .runner.pipeline_runner import parse_args
from .runner.pipeline_runner import run_pipeline

__all__ = [
    "build_artifact_paths",
    "load_atomicity_exceptions",
    "main",
    "parse_args",
    "run_pipeline",
]


if __name__ == "__main__":
    raise SystemExit(main())

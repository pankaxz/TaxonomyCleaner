# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataFactoryServices is a monorepo containing two Python services that manage the skill taxonomy lifecycle for the CareerNavigator platform. Skills flow through: **discovery (JDAnalyser) → validation (CanonicalDataCleaner) → consumption (DataFactory)**.

### Subprojects

| Service | Purpose | Entry Point |
|---------|---------|-------------|
| **CanonicalDataCleaner** | 6-stage pipeline to validate & clean the canonical skill taxonomy | `cd CanonicalDataCleaner && python3 -m src.pipeline.run` |
| **JDAnalyser** | Discover novel skills from crawler output, deduplicate, and promote through human review | `cd JDAnalyser && python main.py` |

Each subproject has its own `CLAUDE.md` (CanonicalDataCleaner) or inline documentation. Refer to those for stage-level details.

## Commands

### CanonicalDataCleaner

```bash
cd CanonicalDataCleaner

# Full pipeline (heuristic mode — no external model servers)
python3 -m src.pipeline.run --llm-provider heuristic --embedding-provider heuristic --out Output/test

# Run a single stage
python3 -m src.pipeline.run --stage stage0 --out Output/stage0

# Resume from prior artifacts
python3 -m src.pipeline.run --stage stage3 --out Output/new-run --resume-from Output/prior-run

# Tests
python3 -m pytest tests/
```

### JDAnalyser

```bash
cd JDAnalyser

# Scan crawler JSONL and update discovery queue
python main.py --discover /path/to/builtin_structured_jobs.jsonl

# Generate review file for promotion-ready skills
python main.py --review

# Apply approved promotions to canonical_data.json
python main.py --apply-review

# Tests
pytest tests/

# Lint & format
ruff check discovery/ config/ tests/
ruff format discovery/ config/ tests/
```

## Architecture

### Data Flow

```
DataCrawler (JSONL)
      |
      v
  JDAnalyser
  (discover -> human review -> promote)
      |
      v
CanonicalDataCleaner/Input/canonical_data.json
      |
      v
  CanonicalDataCleaner
  (6 stages: preclean -> embedding -> clustering -> arbitration -> classification -> reporting)
      |
      v
  Validated Taxonomy + Advisory Artifacts
      |
      v
  DataFactory (consumes taxonomy for JD processing)
```

### CanonicalDataCleaner — Structure

```
CanonicalDataCleaner/
  src/pipeline/
    run.py                  # Entry point
    runner/pipeline_runner.py  # Orchestrator (DAG/serial/resume)
    clients/model_clients.py  # LLM & embedding client abstraction
    shared/models.py        # StageResult, Finding
    shared/utilities.py     # normalize_term, cosine_similarity, etc.
    stages/
      stage0_deterministic_preclean/  # Schema, collisions, atomicity
      stage1_embedding_similarity/    # Pairwise similarity edges
      stage2_conflict_clustering/     # Connected-component clustering
      stage3_semantic_arbitration/    # LLM-based cluster arbitration
      stage4_abstraction_classification/  # LLM ontology classification
      stage5_graph_validation/        # Graph safety checks
      stage6_diff_reporting/          # Final aggregated artifacts
  Input/canonical_data.json   # Source of truth (never mutated by pipeline)
  artifacts/                  # Output directory (gitignored)
```

**Key design rules:**
- The pipeline **never writes back** to `Input/canonical_data.json` — all outputs are advisory
- Each stage returns a `StageResult` with `findings: List[Finding]` and a `payload` dict
- Stages 3 & 4 use LLMs (DeepSeek) but have heuristic/file-backed fallbacks
- DAG mode runs Stages 3 & 4 concurrently; serial mode runs 0→1→2→3→4→5→6
- Resume verifies SHA-256 source hash before reusing prior artifacts

### JDAnalyser — Structure

```
JDAnalyser/
  main.py                 # CLI: --discover, --review, --apply-review
  config/
    __init__.py           # Config class (dot-notation access to settings.yaml)
    settings.yaml         # Paths, thresholds, logging config
  discovery/
    processor.py          # DiscoveryProcessor: scan JSONL, deduplicate, queue
    dedup.py              # SkillDeduplicator: 4-tier matching (exact/normalized/fuzzy/containment)
    promoter.py           # PromotionManager: generate review, apply approvals
    taxonomy.py           # TaxonomyReader: read-only canonical_data.json access
  tests/test_discovery.py
  data/discovery/         # Queue and review files (gitignored)
```

**Key design rules:**
- 4-tier dedup: exact (1.0) → normalized (0.95) → fuzzy via SequenceMatcher (≥0.85) → containment (0.80)
- Discovery queue entries auto-promote to `ready_for_promotion` at `seen_count >= threshold` (default 5)
- Review actions: `approve`, `alias_of:<Canonical>`, `reject`
- Static utility classes with `_CACHE` dicts for lazy-loaded data

## Integration Points

- **JDAnalyser reads** the canonical taxonomy from `CanonicalDataCleaner/Input/canonical_data.json` (configured via `JDAnalyser/config/settings.yaml` → `taxonomy.canonical_data`)
- **JDAnalyser writes** promoted skills back to that same file (only after human review)
- **CanonicalDataCleaner** then validates the updated taxonomy before it flows to DataFactory
- Both services are independent Python projects — no shared code or imports between them

## Key Patterns

- Python 3.10+ required for both subprojects
- Ruff rules: E, F, I (errors, pyflakes, isort)
- All JSON output uses sorted keys for deterministic diffs
- Config singletons: `cfg.get("dotted.key")` pattern in both projects
- Tests use pytest with isolated fixtures and temp directories
- Data files are gitignored; only `.gitkeep` files are tracked
- No hardcoded paths — all paths come from config/CLI args

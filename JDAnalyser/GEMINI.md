# JDAnalyser - Gemini CLI Instructions

This file contains foundational mandates for Gemini CLI when working in the `JDAnalyser` project. These instructions take absolute precedence over general workflows.

## Role & Purpose
JDAnalyser is a helper service for Career Navigator's DataFactory. It processes raw Job Description (JD) scrapes (JSONL), discovers new skills not yet in the taxonomy, deduplicates them, assigns groups via LLM agents, and produces candidates for taxonomy promotion.

## Scope & Boundaries
**CRITICAL: DO NOT MODIFY TAXONOMY DIRECTLY.**
- **Taxonomy (READ ONLY):** `input/Taxonomy/canonical_data.json` is a SYMLINK to `/mnt/workspace/CareerNavigator/DataFactory/data/input/taxonomy/canonical_data.json`.
  - **NEVER** break this symlink.
  - **NEVER** edit this file from JDAnalyser.
  - All taxonomy changes go through `TaxonomyCleaner`.
- **In Scope:**
  - Processing raw JD JSONL files (`discovery/processor.py`).
  - Deduplicating discovered skills (exact, SBERT, semantic).
  - Auditing and reviewing discovered items.
  - Assigning skill groups via LLM agents (`agents/group_assigner.py`).
  - Promoting validated discoveries as taxonomy candidates.
- **Out of Scope:**
  - Building the skill graph (handled by DataFactory).
  - Cleaning the taxonomy (handled by TaxonomyCleaner).
  - Anomaly detection on the graph (handled by NLPAnalysis).

## Pipeline Flow
1. **Input:** `input/Builtin/*.jsonl` (raw scraped JD data).
2. **Extraction:** `discovery/processor.py` parses JSONL and extracts skill candidates.
3. **Deduplication:**
   - `discovery/dedup.py` (exact/fuzzy dedup)
   - `agents/sbert_dedup.py` (SBERT embedding-based)
   - `agents/semantic_dedup.py` (LLM-based semantic with checkpointing)
4. **Validation:** `discovery/auditor.py` validates and audits discoveries.
5. **Grouping:** `agents/group_assigner.py` assigns skill group/category using LLMs.
6. **Promotion:** `discovery/promoter.py` promotes to taxonomy candidates.
7. **Output:** 
   - `data/discovery/discovery_queue.json` (skills pending review)
   - `data/agents/group_assignments.json` (group assignment results)

## Directory Structure & Important Files
- `input/Builtin/`: Raw JD JSONL files (timestamped format: `builtin_structured_jobs_YYYY-MM-DD_HHMMSS.jsonl`).
- `input/Taxonomy/`: Symlink to canonical taxonomy data.
- `data/`: Contains outputs, statuses, and agent checkpoints (e.g., `semantic_dedup.checkpoint.json`).
- `main.py`: Entry point for the pipeline.
- `config/settings.yaml`: Configuration file.

## Coding Conventions
- **Language:** Python 3.12.
- **Testing:** Tests reside in the `tests/` directory and should be run using `pytest`. Always update or add tests when modifying pipeline logic.
- **Resumability:** Ensure LLM agents with long-running tasks use checkpointing (e.g., semantic dedup saving progress).
- **Style:** Adhere to existing Python formatting. The project uses `ruff` for linting.

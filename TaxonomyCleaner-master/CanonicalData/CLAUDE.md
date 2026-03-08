# TaxonomyCleaner / CanonicalData — Taxonomy Maintenance Pipeline

## What This Is
The gatekeeper for Career Navigator's canonical skill taxonomy. Runs a
multi-stage pipeline that cleans, deduplicates, resolves conflicts, and
validates canonical_data.json before it's consumed by DataFactory and JDAnalyser.

## This Project's Scope
✅ Cleaning and rewriting canonical entries (Stage 0)
✅ Similarity computation and edge detection (Stage 1)
✅ Conflict cluster resolution and alias dedup (Stage 2)
✅ Final validation checks (Stage 3-4)
✅ Audit tooling (hard blocks, redundant tokens)
✅ Maintaining atomicity exceptions and hard block pairs

❌ NOT discovering new skills (that's JDAnalyser)
❌ NOT building the skill graph (that's DataFactory)
❌ NOT anomaly detection (that's NLPAnalysis)

## Pipeline Stages
```
Input/canonical_data.json          ← raw/current taxonomy
Input/atomicity_exceptions.json    ← skills exempt from splitting
Input/hard_block_alias_pairs.json  ← pairs that must NOT be merged
    ↓
Stage 0: Clean & Rewrite
  - Validate entries, find issues (stage0_findings.json)
  - Generate rewrite plan (stage0_rewrite_plan.json)
  - Apply rewrites, produce cleaned store
  - Detect suffix redundancy candidates
    ↓
Stage 1: Similarity Analysis
  - Compute similarity edges between entries (stage1_similarity_edges.json)
  - Set merge/split thresholds (stage1_thresholds.json)
  - HTTP checks for external validation
    ↓
Stage 2: Conflict Resolution
  - Identify conflict clusters (stage2_conflict_clusters.json)
  - Resolve alias collisions
  - Produce alias-canonical advisories (stage1_alias_canonical_advisories.json)
    ↓
Stage 3-4: Final Checks
  - Validation passes
  - Output cleaned canonical_data.json
```

Entry point: `main.py`
Pipeline runner: `src/pipeline/run.py`

## Relationship to Other Projects

### TaxonomyCleaner → DataFactory
- **canonical_data.json** is THE output. When this pipeline completes successfully,
  the cleaned file lands at DataFactory/data/input/taxonomy/canonical_data.json
- DataFactory's Taxonomy.py loads it for skill resolution during graph building
- JDAnalyser symlinks to the same file

### JDAnalyser → TaxonomyCleaner
- JDAnalyser's discovery_queue.json contains new skill candidates
- After human review, approved candidates get added to Input/canonical_data.json
- Then this pipeline runs to clean and validate the additions

### NLPAnalysis → TaxonomyCleaner
- NLPAnalysis findings (group_outliers.json) may trigger taxonomy cleanup
- Group review lists inform which categories need restructuring

## Key Files
- `main.py` — entry point
- `src/pipeline/run.py` — pipeline orchestrator
- `src/pipeline/stages/` — individual stage implementations
- `src/pipeline/shared/` — shared utilities across stages
- `src/pipeline/clients/` — LLM client wrappers
- `src/agents/audit_hard_blocks_agent.py` — audits hard block rules
- `scripts/find_redundant_tokens.py` — finds redundant token patterns
- `scripts/keyword_duplicate_report.py` — reports keyword duplicates
- `scripts/resolve_duplicate_keywords.py` — resolves keyword duplication
- `scripts/run_stage0_iterative.py` — iterative Stage 0 execution
- `verify_alias_atomicity.py` — verifies aliases respect atomicity rules

## Data Directory
```
Input/
├── canonical_data.json          ← current taxonomy (input to pipeline)
├── canonical_data.final.json    ← finalized version
├── atomicity_exceptions.json    ← skills exempt from atomicity splitting
└── hard_block_alias_pairs.json  ← pairs that must never be merged

Output/
├── stage0/                      ← cleaning results
├── stage1/                      ← similarity analysis
├── stage2/                      ← conflict resolution
└── stage3/                      ← final validation

artifacts/
├── audit_results/               ← audit logs, hard blocks, findings
├── stage0-4/                    ← intermediate artifacts per stage
└── flow_mermaid.mmd             ← pipeline flow diagram

repo_analysis/                   ← codebase analysis (architecture, deps)
```

## Conventions
- Python 3.12
- LLM agents for semantic analysis (local Qwen + Claude API)
- Checkpointing: stages save intermediate results for resumability
- Tests in tests/test_pipeline.py
- Hard blocks: pairs in hard_block_alias_pairs.json are NEVER merged
  regardless of similarity score
- Atomicity exceptions: entries in atomicity_exceptions.json are NOT
  split even if they appear compound (e.g., "Machine Learning" stays whole)
